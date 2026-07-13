"""
Report generation for NRMS training.
Saves plots and metric summaries to the checkpoint directory.
"""

import os
from typing import Dict, List, Optional

import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import numpy as np
from sklearn.metrics import roc_curve, auc


def _save_learning_curve(
    history: Dict[str, List[float]],
    save_dir: str,
):
    """Plot AUC, MRR, nDCG over epochs."""
    epochs = range(1, len(history.get("epoch", [])) + 1)
    if not epochs:
        return

    fig, axes = plt.subplots(1, 3, figsize=(16, 4))

    # AUC
    ax = axes[0]
    for prefix, color, marker in [("train", "steelblue", "o"), ("dev", "coral", "s")]:
        key = f"{prefix}_auc"
        if key in history and len(history[key]) > 0:
            vals = history[key]
            ax.plot(epochs[:len(vals)], vals, color=color, marker=marker,
                    label=f"{prefix.upper()} AUC", linewidth=2)
    ax.set_xlabel("Epoch")
    ax.set_ylabel("AUC")
    ax.set_title("AUC over Epochs")
    ax.legend()
    ax.grid(True, alpha=0.3)

    # MRR
    ax = axes[1]
    for prefix, color, marker in [("train", "steelblue", "o"), ("dev", "coral", "s")]:
        key = f"{prefix}_mrr"
        if key in history and len(history[key]) > 0:
            vals = history[key]
            ax.plot(epochs[:len(vals)], vals, color=color, marker=marker,
                    label=f"{prefix.upper()} MRR", linewidth=2)
    ax.set_xlabel("Epoch")
    ax.set_ylabel("MRR")
    ax.set_title("MRR over Epochs")
    ax.legend()
    ax.grid(True, alpha=0.3)

    # nDCG@5, nDCG@10
    ax = axes[2]
    for metric, color, marker in [("dev_ndcg@5", "seagreen", "o"), ("dev_ndcg@10", "orchid", "s")]:
        if metric in history and len(history[metric]) > 0:
            vals = history[metric]
            label = metric.replace("_", " ").upper()
            ax.plot(epochs[:len(vals)], vals, color=color, marker=marker,
                    label=label, linewidth=2)
    ax.set_xlabel("Epoch")
    ax.set_ylabel("nDCG")
    ax.set_title("nDCG over Epochs")
    ax.legend()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    path = os.path.join(save_dir, "learning_curves.png")
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved learning curves -> {path}")


def _save_combined_roc(
    final_eval_results: Dict[str, Dict[str, np.ndarray]],
    save_dir: str,
):
    """
    Combine ROC curves for ALL splits into a single figure with one subplot per split.
    Splits with <2 unique labels are skipped (cannot compute ROC).
    """
    # Only include splits that have both classes
    valid = {
        name: d for name, d in final_eval_results.items()
        if len(np.unique(d["labels"])) >= 2
    }
    if not valid:
        return

    n = len(valid)
    ncols = min(3, n)
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(6 * ncols, 5 * nrows), squeeze=False)
    axes = axes.flatten()

    for ax, (split_name, data) in zip(axes, valid.items()):
        labels = data["labels"]
        scores = data["scores"]
        fpr, tpr, _ = roc_curve(labels, scores)
        roc_auc = auc(fpr, tpr)
        ax.plot(fpr, tpr, color="darkorange", lw=2,
                label=f"ROC (AUC = {roc_auc:.4f})")
        ax.plot([0, 1], [0, 1], color="gray", linestyle="--", lw=1)
        ax.set_xlim([0.0, 1.0])
        ax.set_ylim([0.0, 1.05])
        ax.set_xlabel("False Positive Rate")
        ax.set_ylabel("True Positive Rate")
        ax.set_title(f"ROC — {split_name.upper()}")
        ax.legend(loc="lower right")
        ax.grid(True, alpha=0.3)

    # Hide any unused subplot axes
    for ax in axes[n:]:
        ax.set_visible(False)

    plt.tight_layout()
    path = os.path.join(save_dir, "roc_curves_combined.png")
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved combined ROC curves -> {path}")


def _save_combined_score_distribution(
    final_eval_results: Dict[str, Dict[str, np.ndarray]],
    save_dir: str,
):
    """
    Combine score distributions for ALL splits into a single figure with one
    subplot per split (positives vs negatives overlaid).
    """
    if not final_eval_results:
        return

    n = len(final_eval_results)
    ncols = min(3, n)
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(7 * ncols, 4 * nrows), squeeze=False)
    axes = axes.flatten()

    for ax, (split_name, data) in zip(axes, final_eval_results.items()):
        labels = data["labels"]
        scores = data["scores"]
        pos_scores = scores[labels == 1]
        neg_scores = scores[labels == 0]
        if len(neg_scores) > 0:
            ax.hist(neg_scores, bins=50, alpha=0.6, color="steelblue",
                    label=f"Negatives (n={len(neg_scores)})", density=True)
        if len(pos_scores) > 0:
            ax.hist(pos_scores, bins=50, alpha=0.6, color="coral",
                    label=f"Positives (n={len(pos_scores)})", density=True)
        ax.set_xlabel("Predicted Score")
        ax.set_ylabel("Density")
        ax.set_title(f"Score Dist — {split_name.upper()}")
        ax.legend()
        ax.grid(True, alpha=0.3)

    # Hide any unused subplot axes
    for ax in axes[n:]:
        ax.set_visible(False)

    plt.tight_layout()
    path = os.path.join(save_dir, "score_dist_combined.png")
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved combined score distributions -> {path}")


def generate_report(
    epoch_history: Dict[str, List[float]],
    final_eval_results: Dict[str, Dict[str, np.ndarray]],
    save_dir: str,
    attribution_results: Optional[Dict[str, Dict[str, object]]] = None,
    attribution_model: object = None,
    attribution_loader: object = None,
):
    """
    Generate all report plots and save them to save_dir.

    Args:
        epoch_history: Dict mapping metric name -> list of values per epoch.
                       Should include 'epoch' and prefixed metrics.
        final_eval_results: Dict mapping split name -> {"labels": np.array,
                            "scores": np.array} for final epoch ROC/dist plots.
        save_dir: Directory to save plots.
        attribution_results: Optional dict mapping split name -> {"component": {...},
                            "history": {...}} from src.attribution. When provided,
                            attribution plots are also generated.
        attribution_model: Optional NRMSModel (needed for the per-user history
                            attention profile plot, which re-runs score_candidates_detailed
                            on a single batch).
        attribution_loader: Optional eval DataLoader (a single batch is drawn from it
                            for the per-user history attention profile plot).
    """
    os.makedirs(save_dir, exist_ok=True)

    print("\n--- Generating Reports ---")

    if epoch_history:
        _save_learning_curve(epoch_history, save_dir)

    # Combined figures: one subplot per split, all splits in a single image each.
    _save_combined_roc(final_eval_results, save_dir)
    _save_combined_score_distribution(final_eval_results, save_dir)

    if attribution_results:
        _save_attribution_plots(
            attribution_results, save_dir,
            model=attribution_model, loader=attribution_loader,
        )

    print("--- Reports Complete ---\n")


def _save_attribution_plots(
    attribution_results: Dict[str, Dict[str, object]],
    save_dir: str,
    model: object = None,
    loader: object = None,
):
    """
    Plot the Level-1 (component attribution) and Level-2 (history attention)
    distributions for each split. One combined figure per level, with one subplot
    per split. Also renders the two detailed attribution figures:
      - component breakdown stacked bars (per prediction)
      - per-user history attention profile (step + cumulative)
    """
    splits = list(attribution_results.keys())
    if not splits:
        return
    splits = list(attribution_results.keys())
    if not splits:
        return

    # ---- Level 1: topic alignment (pos vs neg) + strength distributions ----
    ncols = min(3, len(splits))
    nrows = (len(splits) + ncols - 1) // ncols

    fig, axes = plt.subplots(nrows, ncols, figsize=(7 * ncols, 5 * nrows), squeeze=False)
    axes = axes.flatten()
    for ax, split in zip(axes, splits):
        comp = attribution_results[split].get("component", {})
        pos = np.asarray(comp.get("pos_alignment", []), dtype=float)
        neg = np.asarray(comp.get("neg_alignment", []), dtype=float)
        if len(neg) > 0:
            ax.hist(neg, bins=50, alpha=0.6, color="steelblue",
                     label=f"Neg (n={len(neg)})", density=True)
        if len(pos) > 0:
            ax.hist(pos, bins=50, alpha=0.6, color="coral",
                     label=f"Pos (n={len(pos)})", density=True)
        ax.set_xlabel("Topic alignment (cosine sim)")
        ax.set_ylabel("Density")
        ax.set_title(f"L1 Alignment — {split.upper()}")
        if ax.get_legend_handles_labels()[1]:
            ax.legend()
        ax.grid(True, alpha=0.3)
    for ax in axes[len(splits):]:
        ax.set_visible(False)
    plt.tight_layout()
    path = os.path.join(save_dir, "attribution_alignment.png")
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved attribution alignment plots -> {path}")

    fig, axes = plt.subplots(nrows, ncols, figsize=(7 * ncols, 5 * nrows), squeeze=False)
    axes = axes.flatten()
    for ax, split in zip(axes, splits):
        comp = attribution_results[split].get("component", {})
        us = np.asarray(comp.get("user_strength", []), dtype=float)
        cs = np.asarray(comp.get("candidate_strength", []), dtype=float)
        if len(cs) > 0:
            ax.hist(cs, bins=50, alpha=0.6, color="seagreen",
                     label=f"Cand (n={len(cs)})", density=True)
        if len(us) > 0:
            ax.hist(us, bins=50, alpha=0.6, color="orchid",
                     label=f"User (n={len(us)})", density=True)
        ax.set_xlabel("L2 norm (vector strength)")
        ax.set_ylabel("Density")
        ax.set_title(f"L1 Strength — {split.upper()}")
        if ax.get_legend_handles_labels()[1]:
            ax.legend()
        ax.grid(True, alpha=0.3)
    for ax in axes[len(splits):]:
        ax.set_visible(False)
    plt.tight_layout()
    path = os.path.join(save_dir, "attribution_strength.png")
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved attribution strength plots -> {path}")

    # ---- Level 2: history attention distributions ----
    fig, axes = plt.subplots(nrows, ncols, figsize=(7 * ncols, 5 * nrows), squeeze=False)
    axes = axes.flatten()
    for ax, split in zip(axes, splits):
        hist = attribution_results[split].get("history", {})
        rb = np.asarray(hist.get("recency_bias", []), dtype=float)
        if len(rb) > 0:
            ax.hist(rb, bins=50, alpha=0.7, color="darkorange", density=True)
        ax.set_xlabel("Recency bias (Spearman pos↔weight)")
        ax.set_ylabel("Density")
        ax.set_title(f"L2 Recency — {split.upper()}")
        if ax.get_legend_handles_labels()[1]:
            ax.legend()
        ax.grid(True, alpha=0.3)
    for ax in axes[len(splits):]:
        ax.set_visible(False)
    plt.tight_layout()
    path = os.path.join(save_dir, "attribution_recency.png")
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved attribution recency plots -> {path}")

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    for split in splits:
        hist = attribution_results[split].get("history", {})
        cc = np.asarray(hist.get("category_concentration", []), dtype=float)
        ac = np.asarray(hist.get("active_categories", []), dtype=float)
        if len(cc) > 0:
            axes[0].hist(cc, bins=30, alpha=0.5, label=f"{split.upper()} (n={len(cc)})", density=True)
        if len(ac) > 0:
            axes[1].hist(ac, bins=max(2, int(ac.max()) + 1), alpha=0.5,
                         label=f"{split.upper()} (n={len(ac)})", density=True)
    axes[0].set_xlabel("Category concentration (top-category attention share)")
    axes[0].set_ylabel("Density")
    axes[0].set_title("L2 Category Concentration")
    if axes[0].get_legend_handles_labels()[1]:
        axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    axes[1].set_xlabel("Active categories (>5% attention share)")
    axes[1].set_ylabel("Density")
    axes[1].set_title("L2 Active Categories")
    if axes[1].get_legend_handles_labels()[1]:
        axes[1].legend()
    axes[1].grid(True, alpha=0.3)
    plt.tight_layout()
    path = os.path.join(save_dir, "attribution_history.png")
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved attribution history plots -> {path}")

    # ---- Level 1: per-prediction component breakdown (stacked bars) ----
    _save_component_attribution_breakdown(attribution_results, save_dir)

    # ---- Level 2: per-user history attention profile (needs model + a batch) ----
    if model is not None and loader is not None:
        _save_per_user_history_attention(model, loader, save_dir)


def _save_component_attribution_breakdown(
    attribution_results: Dict[str, Dict[str, object]],
    save_dir: str,
):
    """
    Level-1 stacked bar chart: for each prediction sample, decompose the score into
    the relative contribution of (a) user-encoder magnitude, (b) candidate-encoder
    magnitude, and (c) their alignment (match). Shows which encoder dominates.

    Purple = user_strength / total   (user encoder influence)
    Green  = candidate_strength / total (news encoder influence)
    Gold   = |topic_alignment| / total (their match)
    """
    splits = list(attribution_results.keys())
    if not splits:
        return

    ncols = min(3, len(splits))
    nrows = (len(splits) + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(8 * ncols, 5 * nrows), squeeze=False)
    axes = axes.flatten()

    for ax, split in zip(axes, splits):
        comp = attribution_results[split].get("component", {})
        us = np.asarray(comp.get("user_strength", []), dtype=float)
        cs = np.asarray(comp.get("candidate_strength", []), dtype=float)
        ta = np.asarray(comp.get("topic_alignment", []), dtype=float)
        if cs.size == 0:
            ax.set_title(f"L1 Breakdown — {split.upper()} (no data)")
            ax.set_visible(True)
            continue
        # `user_strength` is per-USER while `candidate_strength` / `topic_alignment`
        # are per-(user, candidate) pair. Broadcast the per-user value across that
        # user's candidates so all three arrays share the per-prediction (pair) axis.
        if us.size > 0 and us.size != cs.size:
            # Each user contributes len(cs)/len(us) pairs on average; repeat uniformly.
            reps = max(1, cs.size // us.size) if us.size else 1
            us = np.repeat(us, reps)[:cs.size]
            if us.size < cs.size:
                us = np.pad(us, (0, cs.size - us.size), constant_values=0.0)
        # Per-prediction total = user + candidate + |alignment| (all non-negative)
        total = us + cs + np.abs(ta)
        # Guard zero totals (avoid div-by-zero) -> treat as all-zero contribution
        safe = np.where(total > 0, total, 1.0)
        u_rel = us / safe
        c_rel = cs / safe
        a_rel = np.abs(ta) / safe

        n = u_rel.shape[0]
        x = np.arange(n)
        # Stacked bars (each prediction sums to 1)
        ax.bar(x, u_rel, color="purple", label="user_strength / total")
        ax.bar(x, c_rel, bottom=u_rel, color="seagreen", label="candidate_strength / total")
        ax.bar(x, a_rel, bottom=u_rel + c_rel, color="gold",
               label="|topic_alignment| / total")
        ax.set_xlabel("Prediction sample index (user,candidate pair)")
        ax.set_ylabel("Relative contribution")
        ax.set_ylim(0, 1.0)
        ax.set_title(f"L1 Breakdown — {split.upper()} (n={n})")
        if ax.get_legend_handles_labels()[1]:
            ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    for ax in axes[len(splits):]:
        ax.set_visible(False)
    plt.tight_layout()
    path = os.path.join(save_dir, "component_breakdown.png")
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved component breakdown -> {path}")


def _save_per_user_history_attention(
    model,
    loader,
    save_dir: str,
    n_users: int = 5,
):
    """
    Level-2 per-user history attention profile. Draws ONE batch from `loader`, runs
    model.score_candidates_detailed() to obtain the user-history attention weights,
    and plots up to `n_users` example users:

      - Bar chart  = step attention at each history position
      - Red line    = cumulative attention (rises from 0 to 1)

    Reveals recency bias (bars skewed to recent positions) and attention spread.
    """
    try:
        batch = next(iter(loader))
    except StopIteration:
        return
    history, candidates, labels, candidate_mask = batch

    import torch as _torch
    device = next(model.parameters()).device if hasattr(model, "parameters") else _torch.device("cpu")
    history = history.to(device)
    candidates = candidates.to(device)
    candidate_mask = candidate_mask.to(device)

    model.eval()
    with _torch.no_grad():
        detail = model.score_candidates_detailed(history, candidates, candidate_mask)
    weights = detail.history_weights.detach().cpu().numpy()  # (B, H)
    history_np = history.detach().cpu().numpy()              # (B, H) news indices

    B, H = weights.shape
    # Pick up to n_users users with a non-empty history (at least one valid item)
    valid_rows = [i for i in range(B) if int((history_np[i] != 0).sum()) > 0]
    chosen = valid_rows[:n_users]
    if not chosen:
        return

    n = len(chosen)
    ncols = min(3, n)
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(5 * ncols, 4 * nrows), squeeze=False)
    axes = axes.flatten()

    for ax, i in zip(axes, chosen):
        w = weights[i]                       # (H,)
        idx = history_np[i]                  # (H,) news indices (0 = padding)
        valid = idx != 0
        pos = np.arange(H)
        w_valid = w[valid]
        s = w_valid.sum()
        if s > 0 and np.isfinite(s):
            w_valid = w_valid / s            # normalize over valid positions
        cum = np.cumsum(w_valid)
        # Plot only valid positions (pad positions carry ~0 weight anyway)
        ax.bar(pos[valid], w_valid, color="steelblue", alpha=0.8,
               label="step attention")
        ax.plot(pos[valid], cum, color="red", marker="o", linewidth=2,
                label="cumulative")
        ax.set_xlabel("History position (0 = oldest)")
        ax.set_ylabel("Attention weight")
        ax.set_title(f"User {i} (hist len={int(valid.sum())})")
        ax.set_ylim(0, 1.0)
        if ax.get_legend_handles_labels()[1]:
            ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)

    for ax in axes[n:]:
        ax.set_visible(False)
    plt.tight_layout()
    path = os.path.join(save_dir, "attribution_per_user_history.png")
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved per-user history attention -> {path}")