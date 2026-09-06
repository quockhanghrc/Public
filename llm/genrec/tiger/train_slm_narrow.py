"""SLM-TIGER narrow-head (expN1): Qwen2.5-0.5B (fp32, frozen) + LoRA (attn only) + NarrowSIDHead.

Single-shot 4-slot decode; pad-slot collate so train == infer; mid-run evals.
Run:
    python train_slm_narrow.py --params configs/expN1_slm_narrow_s512.json
    MODAL_SMOKE=1 python train_slm_narrow.py --params configs/_smoke_expN1.json
"""
import argparse
import json
import os
import time
import numpy as np
import torch
from torch.utils.data import DataLoader, Dataset
from transformers import AutoModelForCausalLM, AutoTokenizer
from peft import LoraConfig, get_peft_model, TaskType

from modeling.models.slm_tiger import ItemicTokenizer
from modeling.models.slm_tiger_narrow import NarrowSIDHead
from modeling.utils import DEVICE, fix_random_seed

os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True")
SMOKE = os.environ.get("MODAL_SMOKE") == "1"


class NarrowOutput:
    def __init__(self, logits):
        self.logits = logits
        self.loss = None

    def __contains__(self, key):
        return hasattr(self, key)


class NarrowTrainer(torch.nn.Module):
    def __init__(self, slm_id, num_codebooks=4, codebook_size=256, lora_r=32, lora_alpha=64, lora_dropout=0.1):
        super().__init__()
        self.num_codebooks = num_codebooks
        self.codebook_size = codebook_size

        self._tok = AutoTokenizer.from_pretrained(slm_id)
        if self._tok.pad_token is None:
            self._tok.pad_token = self._tok.eos_token

        base = AutoModelForCausalLM.from_pretrained(slm_id, torch_dtype=torch.float32)
        self.itemic = ItemicTokenizer(self._tok, num_codebooks, codebook_size)
        self.itemic.add_tokens()
        base.resize_token_embeddings(self.itemic.new_vocab_size())

        with torch.no_grad():
            torch.manual_seed(42)
            e_in = base.get_input_embeddings().weight
            e_out = base.get_output_embeddings().weight
            std_in = e_in[:self.itemic.base].std().item() or 1e-3
            std_out = e_out[:self.itemic.base].std().item() or 1e-3
            e_in[self.itemic.base:] = torch.randn(
                self.itemic.new_vocab_size() - self.itemic.base, e_in.shape[1], device=e_in.device, dtype=e_in.dtype
            ) * std_in
            e_out[self.itemic.base:] = torch.randn(
                self.itemic.new_vocab_size() - self.itemic.base, e_out.shape[1], device=e_out.device, dtype=e_out.dtype
            ) * std_out



        lora_config = LoraConfig(
            r=lora_r,
            lora_alpha=lora_alpha,
            target_modules=["q_proj", "k_proj", "v_proj", "o_proj"],
            modules_to_save=["embed_tokens"],
            lora_dropout=lora_dropout,
            bias="none",
            task_type=TaskType.CAUSAL_LM,
        )
        self.model = get_peft_model(base, lora_config)
        self.model.get_base_model().get_input_embeddings().weight.requires_grad = True

        hidden = self.model.config.hidden_size
        self.narrow_head = NarrowSIDHead(hidden=hidden, num_codebooks=num_codebooks, codebook_size=codebook_size)

    def get_trainable_params(self):
        return [p for p in self.model.parameters() if p.requires_grad] + [
            p for p in self.narrow_head.parameters() if p.requires_grad
        ]

    def forward(self, input_ids, attention_mask, labels=None):
        outputs = self.model.base_model(
            input_ids=input_ids, attention_mask=attention_mask, output_hidden_states=True
        )
        hidden_states = outputs.hidden_states[-1][:, -self.num_codebooks:, :].to(torch.float32)
        logits = self.narrow_head(hidden_states)  # (B, 4, 1024)

        loss = None
        if labels is not None:
            ls = labels[:, -self.num_codebooks:].contiguous()  # (B, 4)
            # Per-codebook loss: each of the 4 codebook levels gets its own 256-way CE.
            # This is a STRONGER gradient signal than flat 1024-way CE because each
            # level's correct code only competes with 255 others (not 1023). It also
            # matches eval's per-level beam search scoring.
            loss_fct = torch.nn.CrossEntropyLoss(ignore_index=-100, label_smoothing=0.2)
            per_level_losses = []
            for lvl in range(self.num_codebooks):
                lo = lvl * self.codebook_size
                hi = lo + self.codebook_size
                lvl_logits = logits[:, lvl, lo:hi]  # (B, 256)
                # Map labels from lvl*256+c format to [0, 255], preserve -100 for ignore_index
                lvl_labels = ls[:, lvl] - lvl * self.codebook_size
                lvl_labels = torch.where(ls[:, lvl] >= 0, lvl_labels, ls[:, lvl])
                per_level_losses.append(loss_fct(lvl_logits, lvl_labels))
            loss = sum(per_level_losses) / self.num_codebooks

        return {"loss": loss, "logits": logits}

    def generate(self, input_ids, trie, num_beams=20, num_return_sequences=20, pad_token_id=None):
        base = self.itemic.base
        cs = self.itemic.codebook_size
        nc = self.itemic.num_codebooks

        slot_id = pad_token_id if pad_token_id is not None else base
        query = torch.full((input_ids.shape[0], nc), slot_id, dtype=torch.long, device=input_ids.device)
        full_ids = torch.cat([input_ids, query], dim=1)
        full_am = torch.ones_like(full_ids)

        with torch.no_grad():
            outputs = self.model.base_model(input_ids=full_ids, attention_mask=full_am, output_hidden_states=True)
            hidden = outputs.hidden_states[-1][:, -nc:, :].to(torch.float32)
            logits = self.narrow_head(hidden)[0]  # (4, 1024)

        beam = [(0.0, [])]
        for lvl in range(nc):
            new_beam = []
            lo, hi = lvl * cs, lvl * cs + cs
            lw = logits[lvl, lo:hi]
            for sc, codes in beam:
                allowed = trie.get(tuple(codes))
                if not allowed:
                    allowed = set(range(cs))
                for c in allowed:
                    if 0 <= c < cs:
                        new_beam.append((sc + float(lw[c]), codes + [c]))
            new_beam.sort(key=lambda x: x[0], reverse=True)
            beam = new_beam[:num_beams]

        beam = sorted(beam, key=lambda x: x[0], reverse=True)[:num_return_sequences]
        # Return list of raw-code tuples: [(c0, c1, c2, c3), ...]
        return [tuple(codes) for _, codes in beam]


def load_config():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", required=True)
    args = ap.parse_args()
    with open(args.params) as f:
        return json.load(f)


def build_samples(inter, index, ratings, kind, max_len):
    samples = []
    for uid_str, items in inter.items():
        user_ratings = ratings.get(uid_str, {})
        rat_pairs = []
        for iid in items:
            codes = tuple(index[str(iid)])
            rat = int(user_ratings.get(str(iid), 5))  # default 5 if missing
            rat_pairs.append((codes, rat))
        if kind == "train":
            for t in range(1, len(rat_pairs) - 2):
                history = rat_pairs[:t][-max_len:]
                target = rat_pairs[t][0]  # target = codes only (no rating)
                samples.append((history, target))
        elif kind == "val":
            history = rat_pairs[:-2][-max_len:]
            target = rat_pairs[-2][0]
            samples.append((history, target))
        else:  # test
            history = rat_pairs[:-1][-max_len:]
            target = rat_pairs[-1][0]
            samples.append((history, target))
    return samples


class SftDataset(Dataset):
    def __init__(self, samples, is_train=False, drop_prob=0.2):
        self._samples = samples
        self.is_train = is_train
        self.drop_prob = drop_prob

    def __len__(self):
        return len(self._samples)

    def __getitem__(self, i):
        history, target = self._samples[i]
        # Regularizer: Sequence/Item Dropout during training
        if self.is_train and len(history) > 1 and self.drop_prob > 0:
            kept = [item for item in history if np.random.rand() > self.drop_prob]
            history = kept if len(kept) > 0 else [history[-1]]
        return history, target


def collate_narrow(itemic, tok):
    def _collate(batch):
        nc = itemic.num_codebooks
        cs = itemic.codebook_size
        pad_id = tok.pad_token_id if getattr(tok, "pad_token_id", None) is not None else 0
        seqs = []
        for history, target in batch:
            seq = []
            for codes, rating in history:
                seq.extend(itemic.encode_codes([codes]))  # 4 SID tokens
                seq.append(itemic.rating_token_id(rating)) # 1 rating token
            seq.extend([pad_id] * nc)  # pad slots for narrow head
            seqs.append(seq)
        max_len = max(len(s) for s in seqs)
        inp_ids, attn, labels = [], [], []
        for (h, t), seq in zip(batch, seqs):
            pad = max_len - len(seq)
            L = len(seq)
            inp_ids.append([pad_id] * pad + seq)
            attn.append([0] * pad + [1] * L)
            # Labels: history positions (including rating tokens) -> -100, target -> composed ID
            lab = ([-100] * (L - nc)
                   + [l * cs + int(t[l]) for l in range(nc)])
            labels.append([-100] * pad + lab)
        return {
            "input_ids": torch.tensor(inp_ids, dtype=torch.long),
            "attention_mask": torch.tensor(attn, dtype=torch.long),
            "labels": torch.tensor(labels, dtype=torch.long),
        }
    return _collate


def main():
    config = load_config()
    fix_random_seed(42)

    if SMOKE:
        config["train_steps_num"] = config.get("train_steps_num", 60)
        config["eval_subset"] = config.get("eval_subset", 20)
        print("[SMOKE MODE] reduced steps/subset", flush=True)

    dataset_cfg = config["dataset"]
    ds_cfg = config.get("dataloader", {})
    model_cfg = config["model"]

    num_codebooks = dataset_cfg.get("num_codebooks", 4)
    codebook_size = model_cfg.get("codebook_size", 256)
    max_len = dataset_cfg.get("max_sequence_length", 20)
    num_beams = model_cfg.get("num_beams", 20)
    top_k = model_cfg.get("top_k", 20)
    batch_size = ds_cfg.get("train_batch_size", 8)
    lr = config["optimizer"]["lr"]

    train_steps = config.get("train_steps_num")
    eval_subset = config.get("eval_subset", 300)
    eval_step = config.get("eval_step", 1000)
    eval_mid_subset = config.get("eval_mid_subset", 50)
    log_steps = config.get("log_steps", 50)

    with open(dataset_cfg["inter_json_path"]) as f:
        inter = json.load(f)
    with open(dataset_cfg["index_json_path"]) as f:
        index = json.load(f)
    ratings_path = dataset_cfg.get("ratings_json_path", "data/ratings.json")
    with open(ratings_path) as f:
        ratings = json.load(f)

    model = NarrowTrainer(
        slm_id=config["slm_id"],
        num_codebooks=num_codebooks,
        codebook_size=codebook_size,
        lora_r=model_cfg.get("lora_r", 32),
        lora_alpha=model_cfg.get("lora_alpha", 64),
    ).to(DEVICE)
    tok = model._tok
    itemic = model.itemic

    trainable = sum(p.numel() for p in model.get_trainable_params())
    total = sum(p.numel() for p in model.parameters())
    print(f"Model: {config['slm_id']} | trainable {trainable:,}/{total:,} ({100*trainable/total:.1f}%)", flush=True)

    train_samples = build_samples(inter, index, ratings, "train", max_len)
    val_samples = build_samples(inter, index, ratings, "val", max_len)
    test_samples = build_samples(inter, index, ratings, "test", max_len)

    train_loader = DataLoader(
        SftDataset(train_samples, is_train=True, drop_prob=0.2),
        batch_size=batch_size,
        shuffle=True,
        collate_fn=collate_narrow(itemic, tok),
        drop_last=True,
    )
    print(f"samples: train={len(train_samples)} val={len(val_samples)} test={len(test_samples)}", flush=True)

    opt = torch.optim.AdamW(model.get_trainable_params(), lr=lr, weight_decay=0.1)

    # Fast validation loss helper (no beam search, ~100x faster than evaluate)
    def compute_val_loss(val_samples, n):
        model.eval()
        n = min(n, len(val_samples))
        val_batch_size = min(32, n)
        val_loader = DataLoader(
            SftDataset(val_samples[:n]),
            batch_size=val_batch_size, shuffle=False,
            collate_fn=collate_narrow(itemic, tok),
        )
        total_loss = 0.0
        num_batches = 0
        val_steps = min(2, len(val_loader))  # use at most 2 batches to avoid OOM
        with torch.no_grad():
            for i, batch in enumerate(val_loader):
                if i >= val_steps:
                    break
                b = {k: v.to(DEVICE) for k, v in batch.items()}
                out = model(**b)
                total_loss += out["loss"].item()
                num_batches += 1
        return total_loss / max(num_batches, 1)

    # Build catalogue trie ONCE so beam search is constrained to real items
    trie = {}
    for cs_str in index.values():
        codes = tuple(int(c) for c in cs_str)
        for lvl in range(num_codebooks):
            p = codes[:lvl]
            if p not in trie:
                trie[p] = set()
            trie[p].add(codes[lvl])
    print(f"Catalogue trie: {len(trie):,} prefix nodes across {len(index):,} items.", flush=True)

    def evaluate(prefix, samples, n):
        model.eval()
        sub = samples[:n]
        n_users = len(sub)
        rec_hits = {5: 0.0, 10: 0.0, 20: 0.0}
        ndcg_hits = {5: 0.0, 10: 0.0, 20: 0.0}

        with torch.no_grad():
            for history, target in sub:
                seq = []
                for codes, rating in history:
                    seq.extend(itemic.encode_codes([codes]))
                    seq.append(itemic.rating_token_id(rating))
                hid = torch.tensor([seq], dtype=torch.long, device=DEVICE)
                candidate_tuples = model.generate(
                    input_ids=hid,
                    trie=trie,
                    num_beams=num_beams,
                    num_return_sequences=top_k,
                    pad_token_id=tok.pad_token_id,
                )
                gt_tuple = tuple(int(x) for x in target)
                for k in [5, 10, 20]:
                    topk = candidate_tuples[:k]
                    if gt_tuple in topk:
                        rank = topk.index(gt_tuple) + 1
                        rec_hits[k] += 1.0
                        ndcg_hits[k] += 1.0 / np.log2(rank + 1)

        line = " ".join(
            f"recall@{k} {rec_hits[k]/n_users:.6f} ndcg@{k} {ndcg_hits[k]/n_users:.6f}"
            for k in [5, 10, 20]
        )
        print(f"[eval:{prefix}] ({n_users} users) " + line, flush=True)

    step, epoch, sw = 0, 0, time.time()
    best_val_loss, no_improve = float("inf"), 0
    early_stop_patience = 5
    val_check_n = min(64, len(val_samples))

    while train_steps is None or step < train_steps:
        model.train()
        for batch in train_loader:
            b = {k: v.to(DEVICE) for k, v in batch.items()}
            out = model(**b)
            loss = out["loss"]

            if not torch.isfinite(loss):
                print(f"[narrow-lora] epoch {epoch} step {step} NON-FINITE loss, skipping", flush=True)
                opt.zero_grad()
                step += 1
                continue

            opt.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.get_trainable_params(), 1.0)
            opt.step()
            step += 1

            if step % log_steps == 0:
                print(f"[narrow-lora] epoch {epoch} step {step}/{train_steps} loss {loss.item():.4f} | {time.time()-sw:.0f}s", flush=True)

            if step > 0 and eval_step > 0 and step % eval_step == 0:
                # Fast val loss (no beam search)
                vl = compute_val_loss(val_samples, val_check_n)
                print(f"[narrow-lora] step {step} val_loss {vl:.4f} (train_loss {loss.item():.4f}) best {best_val_loss:.4f} no_improve {no_improve}/{early_stop_patience}", flush=True)
                if vl < best_val_loss - 1e-4:
                    best_val_loss = vl
                    no_improve = 0
                else:
                    no_improve += 1

                # Full beam-search eval every eval_step
                evaluate("val-mid", val_samples, eval_mid_subset)
                model.train()

                if no_improve >= early_stop_patience:
                    print(f"[narrow-lora] EARLY STOP at step {step}: val_loss {vl:.4f} not improving for {early_stop_patience} checks", flush=True)
                    break

            if train_steps is not None and step >= train_steps:
                break
        if no_improve >= early_stop_patience:
            break
        epoch += 1

    ckpt_dir = f"checkpoints/{config['experiment_name']}_narrow"
    os.makedirs(ckpt_dir, exist_ok=True)
    model.model.save_pretrained(ckpt_dir)
    tok.save_pretrained(ckpt_dir)
    torch.save(model.narrow_head.state_dict(), os.path.join(ckpt_dir, "narrow_head.pt"))
    print(f"Checkpoint saved to {ckpt_dir}", flush=True)

    print("Final evaluation...", flush=True)
    if val_samples:
        evaluate("val", val_samples, min(eval_subset, len(val_samples)))
        model.train()
    if test_samples:
        evaluate("test", test_samples, min(eval_subset, len(test_samples)))


if __name__ == "__main__":
    main()