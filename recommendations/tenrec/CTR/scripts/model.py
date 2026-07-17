"""
CTR model: embedding branches + history attention + MLP head.

Architecture (matches the agreed design):
  item_id        -> Embedding(64)            [SHARED with hist_1..hist_10]
  video_category -> Embedding(16)
  gender         -> Embedding(4)
  age            -> Embedding(8)
  follow/like/share -> concat -> Linear -> 16d   (engagement vector)
  watching_times -> z-scored -> Linear -> 8d     (watch vector)
  hist_1..hist_10   -> SHARED item embed -> Attention(query=item_embed) -> 64d
  concat(180) -> LayerNorm -> MLP[256,128,64] (Dice + BN + Dropout) -> Linear(1) -> Sigmoid

Embedding-model notes:
  * ONE shared nn.Embedding for item_id and all hist_* halves parameters and
    lets the history sequence teach the candidate embedding.
  * padding_idx=0 in that shared embedding so masked history (hist==0) is
    mapped to a zero vector and never receives gradient.
"""

import torch
import torch.nn as nn

import config


class Dice(nn.Module):
    """Data-adaptive activation (from DIN/DIEN).

    y = p * x + (1 - p) * alpha * x,  p = sigmoid( (x - mean) / sqrt(var + eps) )
    Per-feature running statistics are updated in training mode only.
    var is clamped to >= 0 to avoid sqrt(negative) NaNs that arise from
    float32 precision on near-constant features.
    """

    def __init__(self, dim: int, eps: float = 1e-8):
        super().__init__()
        self.eps = eps
        self.alpha = nn.Parameter(torch.zeros(1))
        self.register_buffer("running_mean", torch.zeros(dim))
        self.register_buffer("running_var", torch.ones(dim))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if self.training:
            mean = x.mean(dim=0)
            var = x.var(dim=0, unbiased=False)
            var = torch.clamp(var, min=0.0)
            # EMA update of running stats (skip if batch produced NaNs)
            if torch.isfinite(mean).all() and torch.isfinite(var).all():
                self.running_mean.mul_(0.9).add_(mean * 0.1)
                self.running_var.mul_(0.9).add_(var * 0.1)
            m, v = mean, var
        else:
            m, v = self.running_mean, self.running_var
        v = torch.clamp(v, min=0.0)
        x_norm = (x - m) / torch.sqrt(v + self.eps)
        p = torch.sigmoid(x_norm)
        return p * x + (1 - p) * self.alpha * x


class HistoryAttention(nn.Module):
    """Multi-head attention over the history items.

    Query  : candidate item embedding (B, D)
    Key/Val: history item embeddings (B, L, D), with padding mask (hist==0)
    Output : (B, D) interest vector, averaged over heads.
    """

    def __init__(self, embed_dim: int, num_heads: int, dropout: float = 0.1):
        super().__init__()
        self.attn = nn.MultiheadAttention(
            embed_dim, num_heads, dropout=dropout, batch_first=True
        )
        self.proj = nn.Linear(embed_dim, embed_dim)

    def forward(self, cand_emb: torch.Tensor, hist_emb: torch.Tensor,
                hist_mask: torch.Tensor) -> torch.Tensor:
        # cand_emb: (B, D) -> (B, 1, D) as query
        query = cand_emb.unsqueeze(1)
        # MultiheadAttention expects key_padding_mask True for positions to ignore
        key_padding_mask = ~hist_mask  # (B, L)
        # Rows whose entire history is padding would make softmax(-inf) -> NaN,
        # and that NaN leaks into the shared item_emb gradients on backward.
        # Unmask at least one (padding) position so softmax is well-defined.
        all_padding = ~hist_mask.all(dim=1)  # True where the whole history is padding
        if all_padding.any():
            key_padding_mask = key_padding_mask.clone()
            key_padding_mask[all_padding, 0] = False
        attn_out, _ = self.attn(
            query, hist_emb, hist_emb, key_padding_mask=key_padding_mask
        )
        # (B, 1, D) -> (B, D)
        out = self.proj(attn_out.squeeze(1))
        # For all-padding rows the attention is meaningless; fall back to the
        # candidate embedding (forward value only; backward is now clean).
        out = torch.where(all_padding.unsqueeze(1), cand_emb, out)
        return out


class CTRModel(nn.Module):
    def __init__(self):
        super().__init__()

        # Shared item embedding (candidate + history). padding_idx=0.
        self.item_emb = nn.Embedding(
            config.ITEM_CARD, config.EMBED_DIMS["item_id"], padding_idx=0
        )
        self.cat_emb = nn.Embedding(
            config.VIDEO_CATEGORY_CARD, config.EMBED_DIMS["video_category"], padding_idx=0
        )
        self.gender_emb = nn.Embedding(
            config.GENDER_CARD, config.EMBED_DIMS["gender"], padding_idx=0
        )
        self.age_emb = nn.Embedding(
            config.AGE_CARD, config.EMBED_DIMS["age"], padding_idx=0
        )

        # Engagement dense branch
        self.engagement = nn.Sequential(
            nn.Linear(config.ENGAGEMENT_IN, config.ENGAGEMENT_OUT),
            Dice(config.ENGAGEMENT_OUT),
        )
        # Watch dense branch
        self.watch = nn.Sequential(
            nn.Linear(1, config.WATCH_OUT),
            Dice(config.WATCH_OUT),
        )

        # History attention
        self.history_attn = HistoryAttention(
            config.EMBED_DIMS["item_id"], config.ATTN_HEADS, config.ATTN_DROPOUT
        )

        # Final concat -> LayerNorm -> MLP head
        self.norm = nn.LayerNorm(config.FINAL_DIM)
        layers = []
        in_dim = config.FINAL_DIM
        for h in config.MLP_DIMS:
            layers += [
                nn.Linear(in_dim, h),
                nn.BatchNorm1d(h),
                Dice(h),
                nn.Dropout(config.DROPOUT),
            ]
            in_dim = h
        self.mlp = nn.Sequential(*layers)
        self.head = nn.Linear(in_dim, 1)

    def forward(self, batch: dict) -> torch.Tensor:
        item_id = batch["item_id"]
        hist = batch["hist"]

        cand_emb = self.item_emb(item_id)                       # (B, 64)
        hist_emb = self.item_emb(hist)                          # (B, L, 64) shared

        cat_emb = self.cat_emb(batch["video_category"])         # (B, 16)
        gender_emb = self.gender_emb(batch["gender"])           # (B, 4)
        age_emb = self.age_emb(batch["age"])                    # (B, 8)

        eng = self.engagement(
            torch.stack([batch["follow"], batch["like"], batch["share"]], dim=1)
        )                                                       # (B, 16)
        watch = self.watch(batch["watching_times"].unsqueeze(1))  # (B, 8)

        interest = self.history_attn(cand_emb, hist_emb, batch["hist_mask"])  # (B, 64)

        fused = torch.cat(
            [cand_emb, cat_emb, gender_emb, age_emb, eng, watch, interest], dim=1
        )                                                       # (B, 180)
        fused = self.norm(fused)
        self._last_fused = fused.detach()
        x = self.mlp(fused)
        logit = self.head(x).squeeze(1)                         # (B,)
        return logit  # raw logits; apply sigmoid at inference time


def build_model(device: str = "cpu") -> CTRModel:
    model = CTRModel().to(device)
    return model


if __name__ == "__main__":
    from dataset import load_stats, get_dataloader

    stats = load_stats()
    dl = get_dataloader("train", stats, shuffle_files=True)
    model = build_model("cpu")
    n_params = sum(p.numel() for p in model.parameters())
    print(f"Model params: {n_params:,}")
    for batch in dl:
        out = model(batch)
        print("output shape:", tuple(out.shape), "mean:", float(out.mean()))
        break
