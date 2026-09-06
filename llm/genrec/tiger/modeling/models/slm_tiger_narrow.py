"""Narrow SID-only output head for SLM-TIGER.

The standard Qwen LM head has 150,000+ output classes (mostly English tokens).
For generative retrieval, we don't want that surface: we want to FORCE the model
to only output SID codes. This module implements a tiny 2-layer transformer
decoder + 1,024-way (4 codebooks × 256 codes) classifier that replaces the
LM head entirely.
"""
import torch
import torch.nn as nn


class NarrowSIDHead(nn.Module):
    """A 2-layer transformer decoder + 1,024-way classification head for SID codes.

    Inputs:  hidden states from Qwen (batch, seq, hidden)
    Outputs: logits over (4 codebooks * 256 codes) = 1024 (batch, seq, 4*256)

    The 1,024 logits are flat-indexed as:
      [codebook_0_code_0, codebook_0_code_1, ..., codebook_0_code_255,
       codebook_1_code_0, ..., codebook_3_code_255]
    A downstream TrieLogitsProcessor reshapes this per-position to the
    correct codebook level.  Since the head only has 1,024 outputs, the
    model mathematically CANNOT emit anything other than SID codes.
    """

    def __init__(
        self,
        hidden: int,
        num_codebooks: int = 4,
        codebook_size: int = 256,
        n_layers: int = 2,
        n_heads: int = 4,
        dropout: float = 0.1,
    ):
        super().__init__()
        self.num_codebooks = num_codebooks
        self.codebook_size = codebook_size
        self.out_dim = num_codebooks * codebook_size

        # learnable positional embeddings for the 4 SID slots
        self.pos = nn.Parameter(torch.zeros(1, num_codebooks, hidden))
        nn.init.trunc_normal_(self.pos, std=0.02)

        decoder_layer = nn.TransformerDecoderLayer(
            d_model=hidden,
            nhead=n_heads,
            dim_feedforward=hidden * 4,
            dropout=dropout,
            batch_first=True,
            activation="gelu",
            norm_first=True,
        )
        self.decoder = nn.TransformerDecoder(decoder_layer, num_layers=n_layers)

        self.out_proj = nn.Linear(hidden, self.out_dim)
        # small init so initial loss looks like uniform over 1024 classes
        nn.init.normal_(self.out_proj.weight, std=0.02)
        nn.init.zeros_(self.out_proj.bias)

    def forward(self, hidden_states: torch.Tensor) -> torch.Tensor:
        """hidden_states: (B, T, H) -> logits: (B, T, 4*256)

        We use the last num_codebooks positions of the sequence as the
        4 SID "slots".  Causal mask ensures position i cannot attend to j>i.
        The decoded (B, 4, 1024) slot logits are broadcast back across the
        sequence so the head's output keeps the input time dimension T —
        non-slot positions are placeholders (only the last num_codebooks
        positions of the output are meaningful for the SID code).
        """
        b, t, h = hidden_states.shape
        sid_inputs = hidden_states[:, -self.num_codebooks:, :] + self.pos   # (B, 4, H)
        causal = torch.triu(
            torch.ones(self.num_codebooks, self.num_codebooks,
                       device=hidden_states.device), 1).bool()
        decoded = self.decoder(sid_inputs, sid_inputs, tgt_mask=causal)        # (B, 4, H)
        slot_logits = self.out_proj(decoded)                                   # (B, 4, 1024)
        # broadcast SID logits across the full sequence length so output is (B, T, 1024)
        # (T may equal num_codebooks; repeat handles both cases safely)
        t_repeat = max(1, t // self.num_codebooks)
        if t % self.num_codebooks == 0:
            return slot_logits.repeat(1, t_repeat, 1)
        # t < num_codebooks is the only other case: pad by tiling
        reps = -(-t // self.num_codebooks)   # ceil division
        tiled = slot_logits.repeat(1, reps, 1)[:, :t, :]
        return tiled