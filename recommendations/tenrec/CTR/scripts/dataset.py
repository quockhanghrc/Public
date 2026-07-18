"""
Streaming dataset for the CTR model.

Uses pyarrow's `ParquetFile.iter_batches` so each split is read in small
row-groups and never fully loaded into RAM. A PyTorch IterableDataset wraps
this and yields feature dicts ready for the model.

Feature dict produced per batch:
  item_id        : (B,)        int64   candidate item (0 reserved as padding)
  video_category : (B,)        int64   nullable -> filled with 0
  gender         : (B,)        int64
  age            : (B,)        int64
  follow         : (B,)        float32
  like           : (B,)        float32
  share          : (B,)        float32
  watching_times : (B,)        float32 (z-scored via stats.json)
  hist           : (B, HIST_LEN) int64  history items, 0 = padding
  hist_mask      : (B, HIST_LEN) bool   True where hist != 0
  click          : (B,)        float32  target
"""

from pathlib import Path

import json
import numpy as np
import pyarrow.parquet as pq
import torch
from torch.utils.data import DataLoader, IterableDataset, get_worker_info

import config


class CtrIterableDataset(IterableDataset):
    def __init__(self, split_dir: Path, stats: dict, shuffle_files: bool = False,
                 epoch: int = 0):
        super().__init__()
        self.files = sorted(Path(split_dir).glob("ctr_part_*.parquet"))
        self.stats = stats
        self.shuffle_files = shuffle_files
        self.epoch = epoch
        self.wt_mean = float(stats.get("watching_times_mean", 0.0))
        self.wt_std = float(stats.get("watching_times_std", 1.0)) or 1.0

    def __iter__(self):
        files = list(self.files)
        if self.shuffle_files:
            # Local shuffle of file order; within-file order is preserved for
            # streaming. Good enough for SGD without loading everything.
            # Seed varies per epoch (config.SEED + epoch) so SGD sees a
            # different file order each pass instead of the same sequence.
            rng = np.random.default_rng(config.SEED + self.epoch)
            rng.shuffle(files)

        # Split files across dataloader workers so each row is yielded exactly
        # once per epoch. Without this, every worker replays the full dataset
        # and each row is duplicated num_workers times.
        wi = get_worker_info()
        if wi is not None:
            files = files[wi.id::wi.num_workers]

        for f in files:
            pf = pq.ParquetFile(f)
            for batch in pf.iter_batches(batch_size=config.BATCH_SIZE):
                yield self._to_features(batch.to_pandas())

    def _to_features(self, df):
        # Nullable ints -> fill; video_category may be null.
        item_id = df["item_id"].to_numpy(dtype="int64")
        video_category = df["video_category"].fillna(0).to_numpy(dtype="int64")
        gender = df["gender"].to_numpy(dtype="int64")
        age = df["age"].to_numpy(dtype="int64")

        follow = df["follow"].fillna(0).to_numpy(dtype="float32")
        like = df["like"].fillna(0).to_numpy(dtype="float32")
        share = df["share"].fillna(0).to_numpy(dtype="float32")

        wt = df["watching_times"].to_numpy(dtype="float32")
        wt = (wt - self.wt_mean) / self.wt_std

        hist = np.stack(
            [df[f"hist_{i}"].fillna(0).to_numpy(dtype="int64") for i in range(1, config.HIST_LEN + 1)],
            axis=1,
        )
        hist_mask = hist != 0

        click = df["click"].fillna(0).to_numpy(dtype="float32")

        return {
            "item_id": torch.as_tensor(item_id),
            "video_category": torch.as_tensor(video_category),
            "gender": torch.as_tensor(gender),
            "age": torch.as_tensor(age),
            "follow": torch.as_tensor(follow),
            "like": torch.as_tensor(like),
            "share": torch.as_tensor(share),
            "watching_times": torch.as_tensor(wt),
            "hist": torch.as_tensor(hist),
            "hist_mask": torch.as_tensor(hist_mask),
            "click": torch.as_tensor(click),
        }


def load_stats():
    with open(config.STATS_PATH) as f:
        return json.load(f)


def get_dataloader(split: str, stats: dict, shuffle_files: bool = False,
                  epoch: int = 0):
    split_dir = config.SPLIT_DIR / split
    if not split_dir.exists():
        raise FileNotFoundError(
            f"Split dir {split_dir} not found. Run scripts/split_data.py first."
        )
    ds = CtrIterableDataset(split_dir, stats, shuffle_files=shuffle_files,
                            epoch=epoch)
    return DataLoader(
        ds,
        batch_size=None,  # IterableDataset already yields full batches
        num_workers=config.NUM_WORKERS,
        pin_memory=True,
        drop_last=False,
    )


if __name__ == "__main__":
    s = load_stats()
    dl = get_dataloader("train", s, shuffle_files=True)
    for i, batch in enumerate(dl):
        print({k: (tuple(v.shape) if hasattr(v, "shape") else v) for k, v in batch.items()})
        if i >= 2:
            break
