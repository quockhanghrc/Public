"""
Utility helpers for the Tenrec CTR training pipeline.

These functions estimate training volume without touching the data:
  * estimate_steps_per_epoch  -> how many batches (steps) make up one full pass
                                 over the train split for a given batch size.
  * estimate_steps_coverage  -> when training is capped with --max-steps, how
                                 much of the whole training data those steps
                                 actually sweep through (fraction / # epochs).
  * estimate_epoch_coverage  -> combined view across `epochs` of training,
                                 accounting for an optional per-epoch step cap.

All estimates are pure arithmetic on row counts, so they are cheap and safe to
call before any dataloader is built.
"""

import math
from typing import Optional


def estimate_steps_per_epoch(
    n_train_rows: int, batch_size: int, drop_last: bool = False
) -> int:
    """Number of batches (steps) in one full pass over the train split.

    Args:
        n_train_rows: total rows in the train split.
        batch_size:   batch size used by the dataloader.
        drop_last:    if True, drop the final partial batch (floor); if False,
                      keep it (ceil). The streaming IterableDataset keeps the
                      partial tail, so the default is ceil.

    Returns:
        Integer step count for one epoch.
    """
    if batch_size <= 0:
        raise ValueError(f"batch_size must be > 0, got {batch_size}")
    if n_train_rows <= 0:
        return 0
    if drop_last:
        return n_train_rows // batch_size
    return math.ceil(n_train_rows / batch_size)


def estimate_steps_coverage(
    max_steps: int, n_train_rows: int, batch_size: int
) -> dict:
    """Estimate how much of the whole training data `max_steps` covers.

    When training is capped with --max-steps (smoke tests), this reports the
    fraction of the full train split those steps sweep through, plus the
    implied number of full epochs.

    Returns a dict with:
        steps_per_epoch, max_steps, covered_rows, n_train_rows,
        fraction, percent, full_epochs, full_pass (bool)
    """
    steps_per_epoch = estimate_steps_per_epoch(n_train_rows, batch_size)
    if steps_per_epoch == 0:
        return {
            "steps_per_epoch": 0,
            "max_steps": max_steps,
            "covered_rows": 0,
            "n_train_rows": n_train_rows,
            "fraction": 0.0,
            "percent": 0.0,
            "full_epochs": 0.0,
            "full_pass": False,
        }
    effective_steps = min(max_steps, steps_per_epoch)
    covered_rows = min(effective_steps * batch_size, n_train_rows)
    fraction = covered_rows / n_train_rows
    return {
        "steps_per_epoch": steps_per_epoch,
        "max_steps": max_steps,
        "covered_rows": covered_rows,
        "n_train_rows": n_train_rows,
        "fraction": fraction,
        "percent": fraction * 100.0,
        "full_epochs": max_steps / steps_per_epoch,
        "full_pass": max_steps >= steps_per_epoch,
    }


def estimate_epoch_coverage(
    epochs: int,
    max_steps: Optional[int],
    n_train_rows: int,
    batch_size: int,
) -> dict:
    """Combined view: total steps and data coverage across `epochs` of training.

    If `max_steps` is None, a full pass per epoch is assumed.

    Returns a dict with:
        epochs, steps_per_epoch, max_steps_per_epoch, total_steps,
        covered_rows, n_train_rows, fraction_per_epoch, percent_per_epoch,
        full_pass_per_epoch (bool)
    """
    steps_per_epoch = estimate_steps_per_epoch(n_train_rows, batch_size)
    if max_steps is None:
        total_steps = steps_per_epoch * epochs
        covered_rows = n_train_rows * epochs
        fraction = 1.0 if epochs >= 1 else 0.0
        full_pass = True
    else:
        effective_steps = min(max_steps, steps_per_epoch)
        total_steps = effective_steps * epochs
        covered_rows = min(total_steps * batch_size, n_train_rows * epochs)
        fraction = (covered_rows / (n_train_rows * epochs)) if epochs else 0.0
        full_pass = max_steps >= steps_per_epoch
    return {
        "epochs": epochs,
        "steps_per_epoch": steps_per_epoch,
        "max_steps_per_epoch": max_steps,
        "total_steps": total_steps,
        "covered_rows": covered_rows,
        "n_train_rows": n_train_rows,
        "fraction_per_epoch": fraction,
        "percent_per_epoch": fraction * 100.0,
        "full_pass_per_epoch": full_pass,
    }


def n_train_rows_from_stats(stats: dict, train_frac: float = 1.0) -> int:
    """Resolve the train-split row count from a stats.json dict.

    Prefers the explicit `split_counts.train` field written by split_data.py;
    falls back to `n_rows * train_frac` when that field is absent.
    """
    split_counts = stats.get("split_counts") or {}
    if "train" in split_counts:
        return int(split_counts["train"])
    n_rows = int(stats.get("n_rows", 0))
    return int(n_rows * train_frac)
