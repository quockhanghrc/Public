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
  data/split/stats.json   (pos rate, split sizes, and the train/val/test
                          frac ratios used)

  NOTE: follow / like / share / watching_times are DROPPED from the written
  parquet (and from stats). They are post-click engagement signals, so using
  them as features is target leakage.

Idempotent: if data/split already exists and is non-empty, it is skipped unless
--force is passed. With --force, any stale ctr_part_*.parquet files from a
previous split ratio are removed first (retrying on Windows file locks) so a
smaller split (e.g. 0.8->0.6 train) does not leave outdated files behind.
"""

import argparse
import json
import os
import time
from pathlib import Path

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


def load_existing_stats() -> dict:
    """Read the already-written stats.json (used when split is skipped)."""
    if config.STATS_PATH.exists():
        with open(config.STATS_PATH) as f:
            return json.load(f)
    return {}


def _safe_unlink(path: Path, retries: int = 5, delay: float = 0.5):
    """Delete a stale split file, retrying on Windows file-lock errors.

    On Windows a parquet may still be held by OneDrive sync or a lingering
    mmap handle, so we retry with backoff. If it still fails we raise a clear
    error: the dataset globs ALL ctr_part_*.parquet in a split dir, so leaving
    a stale file behind would silently mix old+new split data during training.
    """
    last = None
    for attempt in range(retries):
        try:
            path.unlink()
            return
        except PermissionError as e:
            last = e
            if attempt < retries - 1:
                time.sleep(delay)
    raise PermissionError(
        f"Could not remove stale split file {path} (still locked by another "
        f"process, e.g. OneDrive sync). Close the file / pause OneDrive and "
        f"retry, or delete data/split/ manually before re-splitting."
    ) from last



def run_split(train_frac: float, val_frac: float, test_frac: float,
               rows_per_file: int = 2_000_000, force: bool = False) -> dict:
    """Perform the user-level hash split and write data/split/ + stats.json.

    Returns the stats dict that was written (so callers can reload it).

    Idempotent: if data/split already exists and is non-empty, it is skipped
    unless force=True.
    """
    assert abs(train_frac + val_frac + test_frac - 1.0) < 1e-6, \
        "train/val/test fractions must sum to 1.0"

    if config.SPLIT_DIR.exists() and any(config.SPLIT_DIR.iterdir()) and not force:
        print(f"{config.SPLIT_DIR} already populated; skipping (use force=True to redo).")
        return load_existing_stats()

    for split in ("train", "val", "test"):
        split_dir = config.SPLIT_DIR / split
        split_dir.mkdir(parents=True, exist_ok=True)
        if force:
            # Remove stale part files from any previous split ratio so a
            # smaller split (e.g. 0.8->0.6 train) doesn't leave outdated files.
            old = sorted(split_dir.glob("ctr_part_*.parquet"))
            for f in old:
                _safe_unlink(f)
            if old:
                print(f"  removed {len(old)} stale file(s) from {split_dir}")

    shards = sorted(config.PARQUET_DIR.glob("ctr_part_*.parquet"))
    if not shards:
        raise FileNotFoundError(f"No shards found in {config.PARQUET_DIR}")

    buffers = {s: [] for s in ("train", "val", "test")}
    buffered_rows = {s: 0 for s in ("train", "val", "test")}
    file_index = {s: 0 for s in ("train", "val", "test")}
    split_counts = {s: 0 for s in ("train", "val", "test")}

    # Stats accumulators (streaming, numerically stable enough for our scale)
    pos_sum = 0
    total = 0
    seen_users = set()

    print(f"Splitting {len(shards)} shards by user-level hash "
          f"(train={train_frac}, val={val_frac}, test={test_frac}) ...")

    for shard in shards:
        pf = pq.ParquetFile(shard)
        for batch in pf.iter_batches(batch_size=500_000):
            df = batch.to_pandas()
            buckets = df["user_id"].map(
                lambda u: user_bucket(int(u), train_frac, val_frac)
            )
            for split in ("train", "val", "test"):
                part = df[buckets == split]
                if len(part) == 0:
                    continue
                buffers[split].append(part)
                buffered_rows[split] += len(part)
                split_counts[split] += len(part)

                # stats (watching_times excluded: not a feature anymore)
                pos_sum += int(part["click"].sum())
                total += len(part)
                seen_users.update(part["user_id"].unique().tolist())

                if buffered_rows[split] >= rows_per_file:
                    _flush(config.SPLIT_DIR, split, buffers, file_index)
                    buffered_rows[split] = 0

    for split in ("train", "val", "test"):
        if buffers[split]:
            _flush(config.SPLIT_DIR, split, buffers, file_index)

    stats = {
        "pos_rate": float(pos_sum / total) if total else 0.0,
        "n_rows": int(total),
        "n_users": int(len(seen_users)),
        "split_counts": {s: int(split_counts[s]) for s in split_counts},
        "train_frac": train_frac,
        "val_frac": val_frac,
        "test_frac": test_frac,
    }
    with open(config.STATS_PATH, "w") as f:
        json.dump(stats, f, indent=2)

    print("Done.")
    print(f"  rows: train={split_counts['train']:,} "
          f"val={split_counts['val']:,} test={split_counts['test']:,}")
    print(f"  users: {len(seen_users):,}  pos_rate={stats['pos_rate']:.4f}")
    print(f"  stats -> {config.STATS_PATH}")
    return stats


def _flush(split_dir: Path, split: str, buffers: dict, file_index: dict):
    out_df = pd.concat(buffers[split], ignore_index=True)
    # Drop post-click engagement columns (target leakage): follow/like/share/
    # watching_times. Keep everything else (incl. click = the label).
    # It MTL so no need filter-out any label
    #leak_cols = [c for c in ("follow", "like", "share", "watching_times") if c in out_df.columns]
    leak_cols =[]
    if leak_cols:
        out_df = out_df.drop(columns=leak_cols)
    out_path = split_dir / split / f"ctr_part_{file_index[split]:04d}.parquet"
    out_df.to_parquet(out_path, engine="pyarrow", index=False)
    print(f"  wrote {out_path}  ({len(out_df):,} rows)")
    buffers[split].clear()
    file_index[split] += 1


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
    run_split(args.train_frac, args.val_frac, args.test_frac,
              rows_per_file=args.rows_per_file, force=args.force)


if __name__ == "__main__":
    main()
