import json

import torch
from transformers import T5ForConditionalGeneration, T5Config, LogitsProcessor

from modeling.models import TorchModel
from modeling.utils import DEVICE, create_masked_tensor


class CorrectItemsLogitsProcessor(LogitsProcessor):
    # index is identical for every instantiation — parse it once, not per eval batch
    _index_cache = {}

    def __init__(self, num_codebooks, codebook_size, index_path, num_beams, visited_items, visited_lengths=None):
        self.num_codebooks = num_codebooks
        self.codebook_size = codebook_size
        self.num_beams = num_beams

        key = (index_path, num_codebooks)
        if key not in self._index_cache:
            with open(index_path, 'r') as f:
                mapping = json.load(f)
            semantic_ids = [mapping[str(i)] for i in range(len(mapping))]
            assert all(len(s) == num_codebooks for s in semantic_ids), 'All semantic ids must have the same length'
            self._index_cache[key] = semantic_ids
        semantic_ids = self._index_cache[key]
        num_items = len(semantic_ids)

        self.index_semantic_ids = torch.tensor(semantic_ids, dtype=torch.long, device=DEVICE)  # (num_items, semantic_ids)
        self.index_semantic_ids += torch.arange(num_codebooks, device=DEVICE)[None] * codebook_size  # (num_items, semantic_ids)

        batch_size, num_rated = visited_items.shape
        base = self.index_semantic_ids[None].expand(batch_size, -1, -1).contiguous()  # (batch, num_items, semantic_ids)

        index = visited_items[..., None].tile(dims=[1, 1, num_codebooks])  # (batch, num_rated, semantic_ids)

        # Bug-fix: the visited tensor is zero-padded, but 0 is a real item id, so padding
        # spuriously masked item 0 for every short sequence. Route padding rows to a dummy
        # final column (in-range for a num_items+1 buffer) which is dropped afterwards, so
        # padding no longer touches item 0. torch scatter *raises* on out-of-range index,
        # hence the +1 buffer trick instead of an out-of-range sentinel.
        if visited_lengths is not None:
            pad = torch.arange(num_rated, device=DEVICE)[None, :] >= visited_lengths[:, None]  # (batch, num_rated)
            pad = pad[..., None].tile(dims=[1, 1, num_codebooks])
            index = index.masked_fill(pad, num_items)  # dummy col num_items (in-range for buffer)

        buff = torch.zeros(batch_size, num_items + 1, num_codebooks, dtype=torch.long, device=DEVICE)
        buff[:, :num_items, :] = base
        buff = torch.scatter(input=buff, dim=1, index=index, src=torch.zeros_like(index))  # zero out visited items' SIDs
        self.index_semantic_ids = buff[:, :num_items, :]  # (batch, num_items, semantic_ids)
    
    def __call__(self, input_ids: torch.LongTensor, scores: torch.FloatTensor) -> torch.FloatTensor:
        next_sid_codebook_num = (torch.minimum((input_ids[:, -1].max() // self.codebook_size), torch.as_tensor(self.num_codebooks - 1)).item() + 1) % self.num_codebooks
        a = torch.tile(self.index_semantic_ids[:, None, :, next_sid_codebook_num], dims=[1, self.num_beams, 1])  # (batch_size, num_beams, num_items)
        a = a.reshape(a.shape[0] * a.shape[1], a.shape[2])  # (batch_size * num_beams, num_items)

        if next_sid_codebook_num != 0:
            b = torch.tile(self.index_semantic_ids[:, None :, :next_sid_codebook_num], dims=[1, self.num_beams, 1, 1])  # (batch_size, num_beams, num_items, sid_len)
            b = b.reshape(b.shape[0] * b.shape[1], b.shape[2], b.shape[3])  # (batch_size * num_beams, num_items, sid_len)

            current_prefixes = input_ids[:, -next_sid_codebook_num:]  # (batch_size * num_beams, sid_len)
            possible_next_items_mask = (
                torch.eq(current_prefixes[:, None, :], b).long().sum(dim=-1) == next_sid_codebook_num
            )  # (batch_size * num_beams, num_items)
            a[~possible_next_items_mask] = (next_sid_codebook_num + 1) * self.codebook_size

        scores_mask = torch.zeros_like(scores).bool()  # (batch_size * num_beams, num_items)
        scores_mask = torch.scatter_add(
            input=scores_mask,
            dim=-1,
            index=a,
            src=torch.ones_like(a).bool()
        )
        
        scores[:, :next_sid_codebook_num * self.codebook_size] = -torch.inf
        scores[:, (next_sid_codebook_num + 1) * self.codebook_size:] = -torch.inf
        scores[~(scores_mask.bool())] = -torch.inf
        
        return scores



class TigerModel(TorchModel):
    def __init__(
            self,
            embedding_dim,
            codebook_size,
            sem_id_len,
            num_positions,
            user_ids_count,
            num_heads,
            num_encoder_layers,
            num_decoder_layers,
            dim_feedforward,
            num_beams=100,
            num_return_sequences=20,
            d_kv=64,
            layer_norm_eps=1e-6,
            activation='relu',
            dropout=0.1,
            initializer_range=0.02,
            logits_processor=None
    ):
        super().__init__()
        self._embedding_dim = embedding_dim
        self._codebook_size = codebook_size
        self._num_positions = num_positions
        self._num_heads = num_heads
        self._num_encoder_layers = num_encoder_layers
        self._num_decoder_layers = num_decoder_layers
        self._dim_feedforward = dim_feedforward
        self._num_beams = num_beams
        self._num_return_sequences = num_return_sequences
        self._d_kv = d_kv
        self._layer_norm_eps = layer_norm_eps
        self._activation = activation
        self._dropout = dropout
        self._sem_id_len = sem_id_len
        self.user_ids_count = user_ids_count
        self.logits_processor = logits_processor

        unified_vocab_size = codebook_size * self._sem_id_len + self.user_ids_count + 10  # 10 for utilities
        self.config = T5Config(
            vocab_size=unified_vocab_size,
            d_model=self._embedding_dim,
            d_kv=self._d_kv,
            d_ff=self._dim_feedforward,
            num_layers=self._num_encoder_layers,
            num_decoder_layers=self._num_decoder_layers,
            num_heads=self._num_heads,
            dropout_rate=self._dropout,
            is_encoder_decoder=True,
            use_cache=False,
            pad_token_id=unified_vocab_size - 1,
            eos_token_id=unified_vocab_size - 2,
            decoder_start_token_id=unified_vocab_size - 3,
            layer_norm_epsilon=self._layer_norm_eps,
            feed_forward_proj=self._activation,
            tie_word_embeddings=False
        )
        self.model = T5ForConditionalGeneration(config=self.config)
        self._init_weights(initializer_range)

    def forward(self, inputs):
        all_sample_events = inputs['semantic_item.ids']  # (all_batch_events)
        all_sample_lengths = inputs['semantic_item.length']  # (batch_size)
        offsets = (
            torch.arange(
                start=0,
                end=all_sample_events.shape[0],
                device=all_sample_events.device,
                dtype=torch.long
            ) % self._sem_id_len
        ) * self._codebook_size
        all_sample_events = all_sample_events + offsets

        batch_size = all_sample_lengths.shape[0]

        input_semantic_ids, attention_mask = create_masked_tensor(
            data=all_sample_events,
            lengths=all_sample_lengths,
            is_right_aligned=True
        )

        input_semantic_ids[~attention_mask] = self.config.pad_token_id
        input_semantic_ids = torch.cat([
            input_semantic_ids,
            self._sem_id_len * self._codebook_size + inputs['hashed_user.ids'][:, None],
        ], dim=-1)
        attention_mask = torch.cat([
            attention_mask,
            torch.ones(batch_size, 1, device=attention_mask.device, dtype=attention_mask.dtype)
        ], dim=-1)

        if self.training:
            positive_sample_events = inputs['semantic_labels.ids']  # (batch_size * sem_id_len)
            positive_sample_lengths = inputs['semantic_labels.length']  # (batch_size)
            offsets = (
                torch.arange(
                    start=0,
                    end=positive_sample_events.shape[0],
                    device=positive_sample_events.device,
                    dtype=torch.long
                ) % self._sem_id_len
            ) * self._codebook_size
            positive_sample_events = positive_sample_events + offsets

            target_semantic_ids, _ = create_masked_tensor(
                data=positive_sample_events,
                lengths=positive_sample_lengths,
                is_right_aligned=True
            )
            target_semantic_ids = torch.cat([
                torch.ones(
                    batch_size, 1,
                    dtype=torch.long,
                    device=target_semantic_ids.device
                ) * self.config.decoder_start_token_id,
                target_semantic_ids
            ], dim=-1)

            decoder_input_ids = target_semantic_ids[:, :-1].contiguous()
            labels = target_semantic_ids[:, 1:].contiguous()

            model_output = self.model(
                input_ids=input_semantic_ids,
                attention_mask=attention_mask,
                decoder_input_ids=decoder_input_ids,
                labels=labels
            )

            return model_output
        else:
            visited_batch, _ = create_masked_tensor(
                data=inputs['visited.ids'],
                lengths=inputs['visited.length'],
            )

            output = self.model.generate(
                input_ids=input_semantic_ids,
                attention_mask=attention_mask,
                num_beams=self._num_beams,
                num_return_sequences=self._num_return_sequences,
                max_length=self._sem_id_len + 1,
                decoder_start_token_id=self.config.decoder_start_token_id,
                eos_token_id=self.config.eos_token_id,
                pad_token_id=self.config.pad_token_id,
                do_sample=False,
                early_stopping=False,
                logits_processor=[self.logits_processor(visited_items=visited_batch, visited_lengths=inputs['visited.length'])] if self.logits_processor is not None else [],
            )
            return {
                'predictions': output[:, 1:].reshape(-1, self._num_return_sequences, self._sem_id_len)
            }
