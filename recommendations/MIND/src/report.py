"""
Report generation for NRMS training.
Saves plots and metric summaries to the checkpoint directory.
"""

import os
from typing import Dict, List

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
    print(f"  Saved learning curves → {path}")


def _save_roc_curve(
    all_labels: np.ndarray,
    all_scores: np.ndarray,
    split_name: str,
    save_dir: str,
):
    """Plot ROC curve for a single split."""
    if len(np.unique(all_labels)) < 2:
        return
    fpr, tpr, _ = roc_curve(all_labels, all_scores)
    roc_auc = auc(fpr, tpr)

    fig, ax = plt.subplots(figsize=(6, 5))
    ax.plot(fpr, tpr, color="darkorange", lw=2,
            label=f"ROC (AUC = {roc_auc:.4f})")
    ax.plot([0, 1], [0, 1], color="gray", linestyle="--", lw=1)
    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.05])
    ax.set_xlabel("False Positive Rate")
    ax.set_ylabel("True Positive Rate")
    ax.set_title(f"ROC Curve — {split_name.upper()}")
    ax.legend(loc="lower right")
    ax.grid(True, alpha=0.3)

    path = os.path.join(save_dir, f"roc_curve_{split_name}.png")
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved ROC curve → {path}")


def _save_score_distribution(
    labels: np.ndarray,
    scores: np.ndarray,
    split_name: str,
    save_dir: str,
):
    """Plot score histograms for positives vs negatives."""
    pos_scores = scores[labels == 1]
    neg_scores = scores[labels == 0]

    fig, ax = plt.subplots(figsize=(7, 4))
    if len(neg_scores) > 0:
        ax.hist(neg_scores, bins=50, alpha=0.6, color="steelblue",
                label=f"Negatives (n={len(neg_scores)})", density=True)
    if len(pos_scores) > 0:
        ax.hist(pos_scores, bins=50, alpha=0.6, color="coral",
                label=f"Positives (n={len(pos_scores)})", density=True)
    ax.set_xlabel("Predicted Score")
    ax.set_ylabel("Density")
    ax.set_title(f"Score Distribution — {split_name.upper()}")
    ax.legend()
    ax.grid(True, alpha=0.3)

    path = os.path.join(save_dir, f"score_dist_{split_name}.png")
    fig.savefig(path, dpi=100, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved score distribution → {path}")


def generate_report(
    epoch_history: Dict[str, List[float]],
    final_eval_results: Dict[str, Dict[str, np.ndarray]],
    save_dir: str,
):
    """
    Generate all report plots and save them to save_dir.

    Args:
        epoch_history: Dict mapping metric name -> list of values per epoch.
                       Should include 'epoch' and prefixed metrics.
        final_eval_results: Dict mapping split name -> {"labels": np.array,
                            "scores": np.array} for final epoch ROC/dist plots.
        save_dir: Directory to save plots.
    """
    os.makedirs(save_dir, exist_ok=True)

    print("\n--- Generating Reports ---")

    if epoch_history:
        _save_learning_curve(epoch_history, save_dir)

    for split_name, data in final_eval_results.items():
        labels = data["labels"]
        scores = data["scores"]
        _save_roc_curve(labels, scores, split_name, save_dir)
        _save_score_distribution(labels, scores, split_name, save_dir)

    print("--- Reports Complete ---\n")