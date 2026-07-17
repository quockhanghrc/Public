"""
Split the raw CTR parquet shards into train/val/test by USER-LEVEL HASH.

Why user-level hash: a single user must live entirely in one split, otherwise
the same user's rows leak across train/val/test and inflate offline metrics.
We assign each user to a split using a stable hash of user_id, so the split is
reproducible and streaming-friendly (no need to hold the whole dataset).

The script streams shards one at a time and writes buffered output, so peak RAM
stays bounded regardless of dataset size.

Outputs:
  data/split/train/ctr_part_*.parquet
  data/split/val/ctr_part_*.parquet
  data/split/test/ctr_part_*.parquet
  data/split/stats.json   (watching_times mean/std, pos rate, split sizes)

Idempotent: if data/split already exists and is non-empty, it is skipped unless
--force is passed.
"""

import argparse
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import config


def user_bucket(user_id: int, train_frac: float, val_frac: float) -> str:
    """Assign a user to train/val/test via stable hash in [0, 100)."""
    # Python's hash() is randomized per process; use a stable hash instead.
    h = (user_id * 2654435761) % (2 ** 32)
    pct = h % 100
    if pct < int(round(train_frac * 100)):
        return "train"
    if pct < int(round((train_frac + val_frac) * 100)):
        return "val"
    return "test"


def main():
    p = argparse.ArgumentParser(description="User-level hash split of CTR parquet shards.")
    p.add_argument("--train-frac", type=float, default=config.TRAIN_FRAC)
    p.add_argument("--val-frac", type=float, default=config.VAL_FRAC)
    p.add_argument("--test-frac", type=float, default=config.TEST_FRAC)
    p.add_argument("--rows-per-file", type=int, default=2_000_000,
                   help="Rows per output parquet shard.")
    p.add_argument("--force", action="store_true",
                   help="Re-split even if data/split already exists.")
    args = p.parse_args()

    assert abs(args.train_frac + args.val_frac + args.test_frac - 1.0) < 1e-6, \
        "train/val/test fractions must sum to 1.0"

    if config.SPLIT_DIR.exists() and any(config.SPLIT_DIR.iterdir()) and not args.force:
        print(f"{config.SPLIT_DIR} already populated; skipping (use --force to redo).")
        return

    for split in ("train", "val", "test"):
        (config.SPLIT_DIR / split).mkdir(parents=True, exist_ok=True)

    shards = sorted(config.PARQUET_DIR.glob("ctr_part_*.parquet"))
    if not shards:
        raise FileNotFoundError(f"No shards found in {config.PARQUET_DIR}")

    buffers = {s: [] for s in ("train", "val", "test")}
    buffered_rows = {s: 0 for s in ("train", "val", "test")}
    file_index = {s: 0 for s in ("train", "val", "test")}
    split_counts = {s: 0 for s in ("train", "val", "test")}

    # Stats accumulators (streaming, numerically stable enough for our scale)
    wt_sum = 0.0
    wt_sumsq = 0.0
    wt_n = 0
    pos_sum = 0
    total = 0
    seen_users = set()

    print(f"Splitting {len(shards)} shards by user-level hash "
          f"(train={args.train_frac}, val={args.val_frac}, test={args.test_frac}) ...")

    for shard in shards:
        pf = pq.ParquetFile(shard)
        for batch in pf.iter_batches(batch_size=500_000):
            df = batch.to_pandas()
            buckets = df["user_id"].map(
                lambda u: user_bucket(int(u), args.train_frac, args.val_frac)
            )
            for split in ("train", "val", "test"):
                part = df[buckets == split]
                if len(part) == 0:
                    continue
                buffers[split].append(part)
                buffered_rows[split] += len(part)
                split_counts[split] += len(part)

                # stats
                wt = part["watching_times"].to_numpy(dtype="float64")
                wt_sum += wt.sum()
                wt_sumsq += (wt ** 2).sum()
                wt_n += wt.size
                pos_sum += int(part["click"].sum())
                total += len(part)
                seen_users.update(part["user_id"].unique().tolist())

                if buffered_rows[split] >= args.rows_per_file:
                    _flush(config.SPLIT_DIR, split, buffers, file_index)
                    buffered_rows[split] = 0

    for split in ("train", "val", "test"):
        if buffers[split]:
            _flush(config.SPLIT_DIR, split, buffers, file_index)

    wt_mean = wt_sum / wt_n if wt_n else 0.0
    wt_std = float(np.sqrt(max(wt_sumsq / wt_n - wt_mean ** 2, 1e-8))) if wt_n else 1.0
    stats = {
        "watching_times_mean": float(wt_mean),
        "watching_times_std": float(wt_std),
        "pos_rate": float(pos_sum / total) if total else 0.0,
        "n_rows": int(total),
        "n_users": int(len(seen_users)),
        "split_counts": {s: int(split_counts[s]) for s in split_counts},
        "train_frac": args.train_frac,
        "val_frac": args.val_frac,
        "test_frac": args.test_frac,
    }
    with open(config.STATS_PATH, "w") as f:
        json.dump(stats, f, indent=2)

    print("Done.")
    print(f"  rows: train={split_counts['train']:,} "
          f"val={split_counts['val']:,} test={split_counts['test']:,}")
    print(f"  users: {len(seen_users):,}  pos_rate={stats['pos_rate']:.4f}")
    print(f"  watching_times mean={wt_mean:.4f} std={wt_std:.4f}")
    print(f"  stats -> {config.STATS_PATH}")


def _flush(split_dir: Path, split: str, buffers: dict, file_index: dict):
    out_df = pd.concat(buffers[split], ignore_index=True)
    out_path = split_dir / split / f"ctr_part_{file_index[split]:04d}.parquet"
    out_df.to_parquet(out_path, engine="pyarrow", index=False)
    print(f"  wrote {out_path}  ({len(out_df):,} rows)")
    buffers[split].clear()
    file_index[split] += 1


if __name__ == "__main__":
    main()
