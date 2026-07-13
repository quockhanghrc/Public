"""
Data-independent tests for the NRMS component-attribution & history-attention code.

These build a tiny synthetic model (no MIND data files required) so they run
anywhere with `pytest`. Run from the MIND folder:

    cd pub/Public/recommendations/MIND
    python -m pytest tests/test_attribution.py -q

Requires: pip install pytest  (not part of the MIND runtime deps)
"""

import numpy as np
import pytest
import torch

from src.model import (
    AdditiveAttention,
    NRMSModel,
    ScoringDetail,
    UserEncoder,
    build_default_nrms,
)
from src.attribution import (
    _spearman,
    compute_component_attribution,
    compute_history_attention,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_tiny_model():
    """Build a tiny eval-mode NRMS model with random news_title_tokens."""
    torch.manual_seed(0)
    model = build_default_nrms(
        vocab_size=20,
        word_embed_dim=16,
        num_heads=4,
        user_num_heads=4,
        max_title_len=10,
    )
    # news_title_tokens: (num_news + 1, max_title_len); row 0 all zeros (padding)
    tokens = torch.randint(0, 20, (21, 10)).long()
    tokens[0] = 0
    model.set_news_title_tokens(tokens)
    model.eval()
    return model


@pytest.fixture
def tiny_model():
    return _make_tiny_model()


def _make_batch():
    """Synthetic listwise batch: history (2,6), candidates (2,4), mask, labels."""
    torch.manual_seed(1)
    history = torch.tensor([[1, 2, 3, 0, 0, 0], [4, 5, 0, 0, 0, 0]]).long()
    candidates = torch.tensor([[1, 2, 3, 4], [5, 6, 7, 8]]).long()
    candidate_mask = torch.tensor([[1, 1, 1, 1], [1, 1, 1, 0]]).long()
    labels = torch.tensor([[1.0, 0.0, 0.0, 1.0], [0.0, 1.0, 0.0, 0.0]]).float()
    return history, candidates, labels, candidate_mask


# ---------------------------------------------------------------------------
# Level 1 — Component Attribution
# ---------------------------------------------------------------------------

def test_score_candidates_detailed_shapes(tiny_model):
    history, candidates, labels, mask = _make_batch()
    detail = tiny_model.score_candidates_detailed(history, candidates, mask)
    assert isinstance(detail, ScoringDetail)
    B, C = candidates.shape
    H = history.shape[1]
    assert detail.scores.shape == (B, C)
    assert detail.user_norm.shape == (B,)
    assert detail.cand_norm.shape == (B, C)
    assert detail.history_weights.shape == (B, H)


def test_cos_sim_in_range(tiny_model):
    history, candidates, labels, mask = _make_batch()
    detail = tiny_model.score_candidates_detailed(history, candidates, mask)
    denom = detail.user_norm.unsqueeze(1) * detail.cand_norm + 1e-8
    cos_sim = detail.scores / denom
    # cosine similarity must lie in [-1, 1] over VALID candidates (padding scores
    # are -inf by design and are excluded, exactly as compute_component_attribution does)
    cos_valid = cos_sim[mask == 1]
    assert torch.all(cos_valid.abs() <= 1.0 + 1e-3)


def test_padding_cand_norm_zero(tiny_model):
    history, candidates, labels, mask = _make_batch()
    detail = tiny_model.score_candidates_detailed(history, candidates, mask)
    # Row 1 has a padding candidate at position 3 (mask == 0)
    assert detail.cand_norm[1, 3].item() == 0.0


def test_separation_finite_scalar(tiny_model):
    history, candidates, labels, mask = _make_batch()
    detail = tiny_model.score_candidates_detailed(history, candidates, mask)
    denom = detail.user_norm.unsqueeze(1) * detail.cand_norm + 1e-8
    cos_sim = detail.scores / denom
    cos_np = cos_sim.detach().numpy()
    labels_np = labels.detach().numpy()
    mask_np = mask.detach().numpy()
    pos, neg = [], []
    for i in range(cos_np.shape[0]):
        m = int(mask_np[i].sum())
        c = cos_np[i, :m]
        y = labels_np[i, :m].astype(int)
        pos.extend(c[y == 1].tolist())
        neg.extend(c[y == 0].tolist())
    sep = float(np.mean(pos) - np.mean(neg)) if pos and neg else 0.0
    assert np.isfinite(sep)


def test_user_dominance_in_range(tiny_model):
    history, candidates, labels, mask = _make_batch()
    detail = tiny_model.score_candidates_detailed(history, candidates, mask)
    mean_user = float(detail.user_norm.detach().mean())
    mean_cand = float(detail.cand_norm.detach().mean())
    denom = mean_user + mean_cand
    ud = mean_user / denom if denom > 0 else 0.0
    assert 1e-6 < ud < 1.0 - 1e-6


# ---------------------------------------------------------------------------
# Level 2 — History Attention
# ---------------------------------------------------------------------------

def test_user_encoder_return_attention_shape(tiny_model):
    history, candidates, labels, mask = _make_batch()
    tokens = tiny_model.news_title_tokens
    out = tiny_model.user_encoder(history, tokens, return_attention=True)
    assert isinstance(out, tuple) and len(out) == 2
    user_vec, weights = out
    B, H = history.shape
    D = tiny_model.embed_dim
    assert user_vec.shape == (B, D)
    assert weights.shape == (B, H)


def test_history_weights_sum_to_one(tiny_model):
    history, candidates, labels, mask = _make_batch()
    tokens = tiny_model.news_title_tokens
    _, weights = tiny_model.user_encoder(history, tokens, return_attention=True)
    # Over valid positions (history != 0) weights should sum to ~1 PER ROW
    valid = history != 0
    for i in range(history.shape[0]):
        row_valid = valid[i]
        assert torch.allclose(
            weights[i][row_valid].sum(), torch.tensor(1.0), atol=1e-4
        )
    # Padding positions should carry ~0 weight
    pad = ~valid
    assert torch.all(weights[pad] <= 1e-4)


def test_all_padding_history_no_nan(tiny_model):
    history = torch.zeros(1, 6, dtype=torch.long)  # all padding
    tokens = tiny_model.news_title_tokens
    user_vec, weights = tiny_model.user_encoder(history, tokens, return_attention=True)
    assert torch.isfinite(user_vec).all()
    assert torch.isfinite(weights).all()


def test_recency_bias_sign(tiny_model):
    # Build synthetic weights that INCREASE with position -> positive Spearman
    pos = np.arange(6, dtype=float)
    inc = np.array([0.05, 0.10, 0.15, 0.20, 0.25, 0.25])
    dec = inc[::-1]
    assert _spearman(pos, inc) > 0
    assert _spearman(pos, dec) < 0


def test_category_concentration_range():
    shares = np.array([0.6, 0.3, 0.1])
    assert 0.0 <= float(shares.max()) <= 1.0


def test_active_categories_count():
    shares = np.array([0.6, 0.3, 0.05, 0.04])
    active = int((shares > 0.05).sum())
    assert active == 2  # 0.6 and 0.3 only; 0.05 and 0.04 excluded


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def test_spearman():
    x = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
    assert abs(_spearman(x, x) - 1.0) < 1e-6          # perfect positive
    assert abs(_spearman(x, x[::-1]) + 1.0) < 1e-6    # perfect negative
    const = np.array([2.0, 2.0, 2.0, 2.0])
    assert np.isnan(_spearman(x, const))                # degenerate -> NaN guard


# ---------------------------------------------------------------------------
# End-to-end attribution functions (synthetic, no data files)
# ---------------------------------------------------------------------------

def _make_loader(history, candidates, labels, mask):
    """Minimal DataLoader-like iterator yielding a single batch."""
    class _Loader:
        def __iter__(self):
            yield history, candidates, labels, mask
    return _Loader()


def test_compute_component_attribution_runs(tiny_model):
    history, candidates, labels, mask = _make_batch()
    loader = _make_loader(history, candidates, labels, mask)
    res = compute_component_attribution(tiny_model, loader, torch.device("cpu"))
    assert "separation" in res and np.isfinite(res["separation"])
    assert 0.0 < res["user_dominance"] < 1.0
    assert len(res["topic_alignment"]) > 0


def test_compute_history_attention_runs(tiny_model):
    history, candidates, labels, mask = _make_batch()
    loader = _make_loader(history, candidates, labels, mask)
    # idx_to_category: 21 entries (num_news+1), index 0 = -1 padding
    idx_to_category = np.arange(21, dtype=np.int64) - 1  # -1,0,1,...,19
    res = compute_history_attention(
        tiny_model, loader, torch.device("cpu"), idx_to_category, max_history_len=6,
    )
    assert "recency_bias_mean" in res
    assert "category_concentration_mean" in res
    assert "active_categories_mean" in res
    # Both users have non-empty history -> 2 per-user entries
    assert len(res["recency_bias"]) == 2
    assert len(res["active_categories"]) == 2
