"""
Component attribution & history-attention analysis for NRMS (MIND).

Two levels of interpretability, computed at EVALUATION time only (read-only, no
impact on training):

Level 1 — Component Attribution
  Decompose each (user, candidate) dot-product score into magnitude and alignment:
    cos_sim = score / (user_norm * cand_norm + 1e-8)
  Per-prediction breakdown: user_strength, candidate_strength, topic_alignment.
  Aggregates:
    separation     = mean(pos alignment) - mean(neg alignment)
    user_dominance = mean(user_norm) / (mean(user_norm) + mean(cand_norm))

Level 2 — History Attention Distribution
  Use the user-history additive-attention weights (returned by
  NRMSModel.score_candidates_detailed) to characterize what the user encoder attends
  to. Per user (impression):
    recency_bias        = Spearman(position_index, attention_weight) over valid items
    category_concentration = attention share of the single top category
    active_categories  = # of categories with attention share > 5%
  Reported as population averages across all users.

No new third-party dependency: Spearman is implemented with numpy rank correlation
(scipy is not in the MIND dependency set).
"""

from typing import Dict, List, Optional

import numpy as np
import torch


# ---------------------------------------------------------------------------
# Spearman rank correlation (numpy-only, no scipy)
# ---------------------------------------------------------------------------

def _spearman(x: np.ndarray, y: np.ndarray) -> float:
    """
    Spearman rank correlation between two 1-D arrays.

    Returns np.nan if either array is constant (degenerate — no rank variance) or
    if fewer than 2 points are provided, so callers can guard without crashing.
    """
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    if x.size < 2 or y.size < 2 or x.size != y.size:
        return float("nan")

    # Rank each array (average ties). Constant input -> all ranks equal -> 0 variance.
    x_rank = _rank(x)
    y_rank = _rank(y)
    return float(_pearson(x_rank, y_rank))


def _rank(a: np.ndarray) -> np.ndarray:
    """Average-rank transform of a 1-D array (handles ties)."""
    # argsort of argsort yields ranks (1-based); average over tied positions.
    order = a.argsort()
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(1, a.size + 1)
    # Average ties
    _, inv, counts = np.unique(a, return_inverse=True, return_counts=True)
    sums = np.zeros(inv.max() + 1)
    np.add.at(sums, inv, ranks)
    avg = sums / counts
    return avg[inv]


def _pearson(x: np.ndarray, y: np.ndarray) -> float:
    """Pearson correlation; returns np.nan if either array has zero variance."""
    xm = x - x.mean()
    ym = y - y.mean()
    denom = np.sqrt((xm ** 2).sum()) * np.sqrt((ym ** 2).sum())
    if denom == 0 or not np.isfinite(denom):
        return float("nan")
    return float((xm * ym).sum() / denom)


# ---------------------------------------------------------------------------
# Level 1 — Component Attribution
# ---------------------------------------------------------------------------

def compute_component_attribution(
    model,
    loader,
    device: torch.device,
    candidate_mask_key: str = "candidate_mask",
) -> Dict[str, object]:
    """
    Compute Level-1 component-attribution metrics over a (listwise) eval loader.

    Args:
        model: NRMSModel (must have score_candidates_detailed + news_title_tokens).
        loader: DataLoader yielding (history, candidates, labels, candidate_mask).
        device: torch device.
        candidate_mask_key: unused placeholder for API symmetry; kept for clarity.

    Returns:
        dict with scalar aggregates and per-prediction arrays:
          separation, user_dominance (floats)
          user_strength, candidate_strength, topic_alignment (np.ndarray, valid only)
          pos_alignment, neg_alignment (np.ndarray)
    """
    model.eval()
    user_norms: List[float] = []
    cand_norms: List[float] = []
    cos_all: List[float] = []
    cos_pos: List[float] = []
    cos_neg: List[float] = []

    with torch.no_grad():
        for history, candidates, labels, candidate_mask in loader:
            history = history.to(device)
            candidates = candidates.to(device)
            labels = labels.to(device)
            candidate_mask = candidate_mask.to(device)

            detail = model.score_candidates_detailed(history, candidates, candidate_mask)
            scores = detail.scores  # (B, C) possibly -inf at padding
            user_norm = detail.user_norm  # (B,)
            cand_norm = detail.cand_norm  # (B, C)

            # Cosine similarity (guard padding: cand_norm already 0 there)
            denom = user_norm.unsqueeze(1) * cand_norm + 1e-8
            cos_sim = scores / denom  # (B, C)

            # Move to numpy for aggregation
            cos_np = cos_sim.detach().cpu().numpy()
            user_np = user_norm.detach().cpu().numpy()
            cand_np = cand_norm.detach().cpu().numpy()
            labels_np = labels.detach().cpu().numpy()
            mask_np = candidate_mask.detach().cpu().numpy()

            B, C = cos_np.shape
            for i in range(B):
                m = int(mask_np[i].sum())
                if m == 0:
                    continue
                c = cos_np[i, :m]
                u = user_np[i]
                cn = cand_np[i, :m]
                y = labels_np[i, :m].astype(int)

                user_norms.append(float(u))
                cand_norms.extend(cn.tolist())
                cos_all.extend(c.tolist())
                pos = c[y == 1]
                neg = c[y == 0]
                if pos.size:
                    cos_pos.extend(pos.tolist())
                if neg.size:
                    cos_neg.extend(neg.tolist())

    user_norms = np.array(user_norms, dtype=float)
    cand_norms = np.array(cand_norms, dtype=float)
    cos_all = np.array(cos_all, dtype=float)
    cos_pos = np.array(cos_pos, dtype=float)
    cos_neg = np.array(cos_neg, dtype=float)

    # separation = mean(pos alignment) - mean(neg alignment)
    if cos_pos.size and cos_neg.size:
        separation = float(cos_pos.mean() - cos_neg.mean())
    else:
        separation = 0.0  # guard: no positives or no negatives in the split

    # user_dominance = mean(user_norm) / (mean(user_norm) + mean(cand_norm))
    mean_user = float(user_norms.mean()) if user_norms.size else 0.0
    mean_cand = float(cand_norms.mean()) if cand_norms.size else 0.0
    denom = mean_user + mean_cand
    user_dominance = float(mean_user / denom) if denom > 0 else 0.0

    return {
        "separation": separation,
        "user_dominance": user_dominance,
        "user_strength": user_norms,
        "candidate_strength": cand_norms,
        "topic_alignment": cos_all,
        "pos_alignment": cos_pos,
        "neg_alignment": cos_neg,
    }


# ---------------------------------------------------------------------------
# Level 2 — History Attention Distribution
# ---------------------------------------------------------------------------

def compute_history_attention(
    model,
    loader,
    device: torch.device,
    idx_to_category: np.ndarray,
    max_history_len: int,
) -> Dict[str, object]:
    """
    Compute Level-2 history-attention metrics over a (listwise) eval loader.

    Args:
        model: NRMSModel (score_candidates_detailed returns history_weights).
        loader: DataLoader yielding (history, candidates, labels, candidate_mask).
        device: torch device.
        idx_to_category: (num_news + 1,) int64 array; index 0 = -1 (padding).
        max_history_len: max history length (for position indexing).

    Returns:
        dict with population-average scalars and per-user arrays:
          recency_bias_mean, category_concentration_mean, active_categories_mean
          recency_bias, category_concentration, active_categories (np.ndarray, per user)
    """
    model.eval()
    recency_list: List[float] = []
    concentration_list: List[float] = []
    active_list: List[int] = []

    position_index = np.arange(max_history_len, dtype=float)  # 0..H-1

    with torch.no_grad():
        for history, candidates, labels, candidate_mask in loader:
            history = history.to(device)
            candidates = candidates.to(device)
            candidate_mask = candidate_mask.to(device)

            detail = model.score_candidates_detailed(history, candidates, candidate_mask)
            history_weights = detail.history_weights.detach().cpu().numpy()  # (B, H)
            history_np = history.detach().cpu().numpy()  # (B, H) news indices

            B, H = history_weights.shape
            for i in range(B):
                w = history_weights[i]  # (H,)
                idx = history_np[i]  # (H,) news indices (0 = padding)
                valid = idx != 0
                if not valid.any():
                    # All-padding history: skip (no meaningful attention to analyze).
                    continue
                w_valid = w[valid]
                # Normalize over valid positions (guard against tiny float drift).
                s = w_valid.sum()
                if s <= 0 or not np.isfinite(s):
                    continue
                w_valid = w_valid / s

                # --- recency_bias: Spearman(position, weight) over valid items ---
                pos_valid = position_index[valid]
                rb = _spearman(pos_valid, w_valid)
                if np.isfinite(rb):
                    recency_list.append(float(rb))

                # --- category concentration / active categories ---
                cats = idx_to_category[idx[valid]]  # category id per valid item
                # Sum attention weight per category
                unique_cats = np.unique(cats)
                shares = np.array(
                    [w_valid[cats == c].sum() for c in unique_cats], dtype=float
                )
                if shares.size == 0:
                    continue
                concentration_list.append(float(shares.max()))
                active_list.append(int((shares > 0.05).sum()))

    recency_arr = np.array(recency_list, dtype=float)
    concentration_arr = np.array(concentration_list, dtype=float)
    active_arr = np.array(active_list, dtype=int)

    return {
        "recency_bias_mean": float(recency_arr.mean()) if recency_arr.size else float("nan"),
        "category_concentration_mean": float(concentration_arr.mean()) if concentration_arr.size else 0.0,
        "active_categories_mean": float(active_arr.mean()) if active_arr.size else 0.0,
        "recency_bias": recency_arr,
        "category_concentration": concentration_arr,
        "active_categories": active_arr,
    }
