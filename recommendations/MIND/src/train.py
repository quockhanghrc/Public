"""
Training and evaluation utilities for NRMS model.

Supports CPU and GPU (with mixed precision for GPU).
"""

import os
import time
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

import numpy as np
import torch
import torch.nn as nn
from sklearn.metrics import roc_auc_score
from torch.utils.data import DataLoader
from torch.utils.data.dataset import Dataset

from src.data import collate_fn, eval_collate_fn
from src.model import NRMSModel


def train_one_epoch(
    model: NRMSModel,
    loader: DataLoader,
    optimizer: torch.optim.Optimizer,
    device: torch.device,
    criterion: nn.Module,
    epoch: int,
    grad_clip: float = 1.0,
    use_amp: bool = False,
) -> float:
    """
    Train the model for one epoch.

    Returns:
        Average loss for the epoch.
    """
    model.train()
    total_loss = 0.0
    num_batches = 0

    scaler = torch.amp.GradScaler("cuda", enabled=use_amp)

    for batch_idx, (history, candidates, labels) in enumerate(loader):
        history = history.to(device)
        candidates = candidates.to(device)
        labels = labels.to(device)

        optimizer.zero_grad()

        with torch.amp.autocast("cuda", enabled=use_amp):
            logits = model(history, candidates)
            loss = criterion(logits, labels)

        scaler.scale(loss).backward()
        scaler.unscale_(optimizer)
        torch.nn.utils.clip_grad_norm_(model.parameters(), grad_clip)
        scaler.step(optimizer)
        scaler.update()

        total_loss += loss.item()
        num_batches += 1

        if batch_idx % 100 == 0 and batch_idx > 0:
            print(
                f"  Epoch {epoch} | Batch {batch_idx}/{len(loader)} "
                f"| Loss: {total_loss / num_batches:.4f}"
            )

    return total_loss / max(num_batches, 1)


def dcg_at_k(relevances: List[float], k: int) -> float:
    """Compute DCG@k given a list of binary relevances sorted by rank."""
    relevances = relevances[:k]
    if not relevances:
        return 0.0
    return sum((2**r - 1) / np.log2(i + 2) for i, r in enumerate(relevances))


def ndcg_at_k(y_true: List[int], y_score: List[float], k: int) -> float:
    """Compute nDCG@k for a single query."""
    paired = sorted(zip(y_score, y_true), key=lambda x: -x[0])
    sorted_labels = [p[1] for p in paired]
    ideal_labels = sorted(y_true, reverse=True)
    dcg = dcg_at_k(sorted_labels, k)
    idcg = dcg_at_k(ideal_labels, k)
    return dcg / idcg if idcg > 0 else 0.0


@torch.no_grad()
def evaluate(
    model: NRMSModel,
    loader: DataLoader,
    device: torch.device,
    criterion: nn.Module,
    name: str = "dev",
    return_raw: bool = False,
) -> Dict[str, float]:
    """
    Evaluate the model on a dataset with per-impression metrics.

    Expects loader to yield (gids, history, candidates, labels) from eval_collate_fn.

    Args:
        return_raw: If True, returns a tuple (metrics, labels, scores) for report plots.

    Returns:
        Dictionary of metric name -> value. If return_raw, also (np.ndarray, np.ndarray).
    """
    model.eval()
    total_loss = 0.0
    num_batches = 0
    all_labels: List[int] = []
    all_scores: List[float] = []

    # Per-impression grouping
    group_scores: Dict[int, List[float]] = defaultdict(list)
    group_labels: Dict[int, List[int]] = defaultdict(list)

    for gids, history, candidates, labels in loader:
        history = history.to(device)
        candidates = candidates.to(device)
        labels = labels.to(device)

        logits = model(history, candidates)
        loss = criterion(logits, labels)

        total_loss += loss.item()
        num_batches += 1

        scores = torch.sigmoid(logits).cpu().numpy()
        labels_np = labels.cpu().numpy()
        gids_np = gids.cpu().numpy()

        all_scores.extend(scores.tolist())
        all_labels.extend(labels_np.tolist())

        for gid, sc, lb in zip(gids_np, scores, labels_np):
            group_scores[gid].append(float(sc))
            group_labels[gid].append(int(lb))

    avg_loss = total_loss / max(num_batches, 1)

    # Global AUC
    all_labels_arr = np.array(all_labels)
    all_scores_arr = np.array(all_scores)
    if len(np.unique(all_labels_arr)) < 2:
        global_auc = 0.5
    else:
        global_auc = roc_auc_score(all_labels_arr, all_scores_arr)

    # Per-impression metrics
    impression_aucs, mrrs, ndcg5s, ndcg10s = [], [], [], []
    for gid in group_scores:
        y_true = group_labels[gid]
        y_score = group_scores[gid]
        unique = np.unique(y_true)
        if len(unique) < 2:
            continue

        try:
            impression_aucs.append(roc_auc_score(y_true, y_score))
        except Exception:
            pass

        # MRR: reciprocal rank of first positive
        paired = sorted(zip(y_score, y_true), key=lambda x: -x[0])
        for rank, (_, lbl) in enumerate(paired, start=1):
            if lbl == 1:
                mrrs.append(1.0 / rank)
                break

        # nDCG
        ndcg5s.append(ndcg_at_k(y_true, y_score, 5))
        ndcg10s.append(ndcg_at_k(y_true, y_score, 10))

    avg_impression_auc = float(np.mean(impression_aucs)) if impression_aucs else 0.5
    avg_mrr = float(np.mean(mrrs)) if mrrs else 0.0
    avg_ndcg5 = float(np.mean(ndcg5s)) if ndcg5s else 0.0
    avg_ndcg10 = float(np.mean(ndcg10s)) if ndcg10s else 0.0

    metrics = {
        f"{name}_loss": avg_loss,
        f"{name}_auc": global_auc,
        f"{name}_impression_auc": avg_impression_auc,
        f"{name}_mrr": avg_mrr,
        f"{name}_ndcg@5": avg_ndcg5,
        f"{name}_ndcg@10": avg_ndcg10,
    }
    if return_raw:
        return metrics, all_labels_arr, all_scores_arr
    return metrics


def get_device() -> torch.device:
    """Auto-detect device: CUDA if available, else CPU."""
    if torch.cuda.is_available():
        device = torch.device("cuda")
        print(f"Using device: CUDA ({torch.cuda.get_device_name(0)})")
    else:
        device = torch.device("cpu")
        print("Using device: CPU")
    return device


def save_checkpoint(
    model: NRMSModel,
    optimizer: torch.optim.Optimizer,
    epoch: int,
    metrics: Dict[str, float],
    path: str,
):
    """Save a model checkpoint."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    torch.save(
        {
            "epoch": epoch,
            "model_state_dict": model.state_dict(),
            "optimizer_state_dict": optimizer.state_dict(),
            "metrics": metrics,
        },
        path,
    )
    print(f"Checkpoint saved to {path}")


def load_checkpoint(
    model: NRMSModel,
    path: str,
    device: torch.device,
) -> Tuple[int, Dict[str, float]]:
    """Load a model checkpoint. Returns (epoch, metrics)."""
    checkpoint = torch.load(path, map_location=device)
    model.load_state_dict(checkpoint["model_state_dict"])
    epoch = checkpoint.get("epoch", 0)
    metrics = checkpoint.get("metrics", {})
    print(f"Checkpoint loaded from {path} (epoch {epoch})")
    return epoch, metrics