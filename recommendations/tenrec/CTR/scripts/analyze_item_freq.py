"""
Profile item_id frequency on the TRAIN split to choose ITEM_HASH_BUCKETS.

Reads data/split/train/ctr_part_*.parquet, computes a bincount of item_id
(excluding the padding id 0), and reports:
  - unique items seen
  - long-tail distribution (top-10% threshold, items seen < 10 times)
  - predicted collision rate for candidate bucket counts

Usage:
    python scripts/analyze_item_freq.py
"""

import glob
import os

import numpy as np
import pyarrow.parquet as pq

from config import SPLIT_DIR

TRAIN_DIR = SPLIT_DIR / "train"


def load_train_item_ids():
    files = sorted(glob.glob(str(TRAIN_DIR / "ctr_part_*.parquet")))
    if not files:
        raise SystemExit(f"No train parquet files found in {TRAIN_DIR}")
    chunks = []
    for f in files:
        col = pq.read_table(f, columns=["item_id"]).column("item_id")
        chunks.append(col.to_numpy(zero_copy_only=False))
    return np.concatenate(chunks)


def analyze_item_frequencies(item_ids):
    # padding id is 0 -> exclude from frequency analysis
    item_ids = item_ids[item_ids > 0]

    item_counts = np.bincount(item_ids)
    freq_items = item_counts[item_counts > 0]  # non-padding, seen >= 1
    total_items = int(freq_items.size)

    print(f"Train rows (non-padding item_id): {item_ids.size:,}")
    print(f"Unique items seen: {total_items:,}")
    print(f"Max item_id value: {int(item_ids.max()):,}")
    top10 = np.percentile(freq_items, 90)
    print(f"Items in top 10% (count > {top10:.0f}): "
          f"{int((freq_items > top10).sum()):,}")
    print(f"Items appearing < 10 times: {int((freq_items < 10).sum()):,} "
          f"({100*(freq_items < 10).mean():.1f}% of unique items)")
    print(f"Median appearances per item: {int(np.median(freq_items)):,}")
    print(f"Mean appearances per item: {freq_items.mean():.1f}")

    print("\nPredicted collision rate (single-hash approx 1 - exp(-n/buckets)):")
    for buckets in [250_000, 500_000, 1_000_000, 2_000_000]:
        collision_rate = 1 - np.exp(-total_items / buckets)
        print(f"  Buckets: {buckets:,} -> {collision_rate:.2%}")


if __name__ == "__main__":
    ids = load_train_item_ids()
    analyze_item_frequencies(ids)
