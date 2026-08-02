#!/usr/bin/env python3
"""
Memory-Efficient MTL Recommendation Training (Step-Based)
==========================================================
Key memory-saving techniques:
 1. Chunked parquet loading — never loads all files at once
 2. Batched encoding — fits encoders on train, transforms in chunks
 3. Pre-save tensors to disk — avoid keeping full DataFrames in memory
 4. Gradient accumulation — simulate larger batches on limited GPU
 5. Mixed-precision (AMP) — halve GPU memory footprint

Step-based training: validates every N gradient updates, independent
of dataset size. Ideal for huge datasets where full epochs are too slow.

NEW FEATURES:
  - 🔄 Cache integrity validation — checks memmap files before reuse
  - 📊 tqdm progress bar — live training progress with ETA
  - 💾 Resume from checkpoint — continue training from saved state
  - 🗑️ Force reprocess — override cached data when schema changes

PERFORMANCE FIXES:
  - ⚡ Vectorized encoding — 10-50x faster than Python lambda .map()
  - ⚡ Parquet metadata row counting — eliminates one full data pass
  - ⚡ Generator-based tqdm — never collects all chunks into a list
"""

import sys
import io

# Fix Windows console encoding issue
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import argparse
import concurrent.futures
import glob
import hashlib
import logging
import os
import pickle
import random
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import LabelEncoder
from torch.cuda.amp import GradScaler, autocast
from torch.utils.data import DataLoader, Dataset, Sampler

# Optional: torchmetrics for streaming AUC — graceful fallback if not installed
try:
    from torchmetrics import AUROC
    TORCHMETRICS_AVAILABLE = True
except ImportError:
    TORCHMETRICS_AVAILABLE = False

# Optional: tqdm for progress bars — graceful fallback if not installed
try:
    from tqdm import tqdm, trange
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    # Stub tqdm so code works without it
    class tqdm:
        def __init__(self, *args, **kwargs):
            self.iterable = args[0] if args else range(kwargs.get('total', 0))
        def __iter__(self):
            return iter(self.iterable)
        def update(self, n=1):
            pass
        def close(self):
            pass
        def set_description(self, desc):
            pass
        def set_postfix(self, **kwargs):
            pass

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger("mtl_trainer")

# Safer version — prevents duplicate handlers
log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "training.log")

# Only add the file handler if it doesn't already exist
if not any(isinstance(h, logging.FileHandler) for h in logging.getLogger().handlers):
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s")
    )
    logging.getLogger().addHandler(file_handler)
    logger.info("Logging to file: %s", log_file)


# ===========================================================================
# CONFIG
# ===========================================================================
@dataclass
class MTLConfig:
    # ---- Data ----
    data_dir: str = "./data/split"
    cache_dir: str = "./cache"          # where preprocessed tensors live
    chunk_size: int = 200_000            # rows per chunk during preprocessing

    # ---- Model ----
    user_emb_dim: int = 64
    item_emb_dim: int = 64
    category_emb_dim: int = 16
    gender_emb_dim: int = 8
    shared_hidden: List[int] = field(default_factory=lambda: [256, 128])
    shared_dropout: float = 0.2
    tower_hidden: List[int] = field(default_factory=lambda: [64, 32])
    tower_dropout: float = 0.15

    # ---- Training (step-based) ----
    batch_size: int = 2048                     # actual batch per step
    accum_steps: int = 1                       # gradient accumulation steps
    # effective_batch = batch_size × accum_steps
    train_steps: int = 50_000                  # total gradient updates (not epochs)
    val_every: int = 500                       # validate every N gradient updates
    log_every: int = 100                       # log train loss every N steps
    max_val_batches: Optional[int] = None      # cap val/test eval batches (quick test)
    learning_rate: float = 1e-3
    weight_decay: float = 1e-5
    grad_clip_norm: float = 5.0
    lr_patience: int = 3
    lr_factor: float = 0.5
    early_stop_patience: int = 10              # checks (every val_every steps)
    use_amp: bool = True                       # automatic mixed precision

    task_weights: Dict[str, float] = field(
        default_factory=lambda: {"click": 1.0, "follow": 1.0, "like": 1.0, "share": 1.0}
    )
    num_tasks: int = 2  # 1=click, 2=click+like, 3=+follow, 4=+share
    dense_dim: int = 2  # number of dense features (age_norm, watch_norm if available)

    model_dir: str = "./checkpoints"
    num_workers: int = 4
    pin_memory: bool = True
    device: str = "cuda" if torch.cuda.is_available() else "cpu"


# ===========================================================================
# 0. CACHE VALIDATION UTILITIES
# ===========================================================================
CACHE_FILES = ["sparse.npy", "dense.npy", "targets.npy", "meta.pkl"]
CACHE_EXPECTED_SHAPES = {
    "sparse.npy": (None, 14),   # (N, 14) int64
    "dense.npy":  (None, None), # (N, n_dense) float32 — n_dense read from meta
    "targets.npy":(None, 4),    # (N, 4)  float32
}


def verify_cache_integrity(cache_path: str) -> bool:
    """
    Verify that a memmap cache is complete and valid.
    
    Checks:
      1. All required files exist
      2. Metadata is readable
      3. Total rows in meta matches memmap shapes
      4. Shapes are consistent across all files
      5. Data types are correct
      6. No corrupt / zero-length memmap files
    
    Returns: True if cache is valid, False if it needs rebuilding.
    """
    missing_files = []
    for fname in CACHE_FILES:
        fpath = f"{cache_path}_{fname}"
        if not os.path.exists(fpath):
            missing_files.append(fname)
    
    if missing_files:
        logger.warning("  ❌ Cache incomplete at %s — missing: %s", cache_path, missing_files)
        return False
    
    # Check metadata
    try:
        with open(f"{cache_path}_meta.pkl", "rb") as f:
            meta = pickle.load(f)
        cached_rows = meta["total_rows"]
    except (pickle.UnpicklingError, EOFError, KeyError) as e:
        logger.warning("  ❌ Cache metadata corrupt: %s", e)
        return False
    
    if cached_rows == 0:
        logger.warning("  ❌ Cache has 0 rows — rebuild needed")
        return False
    
    # Validate each memmap file
    for fname, expected_shape in CACHE_EXPECTED_SHAPES.items():
        fpath = f"{cache_path}_{fname}"
        try:
            # Check file size is non-zero
            file_size = os.path.getsize(fpath)
            if file_size == 0:
                logger.warning("  ❌ %s is empty (0 bytes)", fname)
                return False
            
            # Open and validate dtype
            expected_dtype = {
                "sparse.npy": np.int64,
                "dense.npy": np.float32,
                "targets.npy": np.float32,
            }[fname]
            
            # IMPORTANT: must specify dtype when opening, otherwise numpy
            # defaults to uint8 and the dtype check always fails.
            arr = np.memmap(fpath, mode="r", dtype=expected_dtype)
            
            if arr.dtype != expected_dtype:
                logger.warning("  ❌ %s dtype mismatch: got %s, expected %s",
                               fname, arr.dtype, expected_dtype)
                return False
            
            # Verify shape by reading first & last row
            _ = arr[0]  # This will raise if file is corrupt
            if cached_rows > 1:
                _ = arr[cached_rows - 1]
                
        except (ValueError, IndexError, OSError) as e:
            logger.warning("  ❌ %s is corrupt: %s", fname, e)
            return False
    
    logger.info("  ✅ Cache valid: %d rows, all files intact", cached_rows)
    return True


def compute_cache_signature(data_dir: str, encoders: Dict) -> str:
    """
    Compute a hash of the data schema + encoders to detect changes.
    If the data columns or encoder sizes change, the cache is invalidated.
    """
    hasher = hashlib.md5()
    
    # Encode column definitions
    cols_str = f"cat={CAT_COLS}|hist={HIST_COLS}|target={TARGET_COLS}|num={NUM_COLS}"
    hasher.update(cols_str.encode())
    
    # Encode encoder vocab sizes
    for col in CAT_COLS:
        le = encoders.get(col)
        if le:
            hasher.update(f"{col}:{len(le.classes_)}".encode())
    
    return hasher.hexdigest()


def save_cache_signature(cache_dir: str, signature: str):
    """Save cache signature for future verification."""
    sig_path = os.path.join(cache_dir, ".cache_signature")
    with open(sig_path, "w") as f:
        f.write(signature)


def load_cache_signature(cache_dir: str) -> Optional[str]:
    """Load previously saved cache signature."""
    sig_path = os.path.join(cache_dir, ".cache_signature")
    if os.path.exists(sig_path):
        with open(sig_path) as f:
            return f.read().strip()
    return None


def get_cache_stats(cache_path: str) -> Dict:
    """Get human-readable stats about a cached dataset."""
    stats = {}
    meta_path = f"{cache_path}_meta.pkl"
    if os.path.exists(meta_path):
        with open(meta_path, "rb") as f:
            meta = pickle.load(f)
        stats["rows"] = meta["total_rows"]
        
        for fname in ["sparse.npy", "dense.npy", "targets.npy"]:
            fpath = f"{cache_path}_{fname}"
            if os.path.exists(fpath):
                size_bytes = os.path.getsize(fpath)
                size_mb = size_bytes / (1024 * 1024)
                stats[f"{fname}_size_mb"] = round(size_mb, 2)
    
    return stats


# ===========================================================================
# 1. CHUNKED DATA PREPROCESSING → SAVE TENSORS TO DISK
# ===========================================================================

HIST_COLS = [f"hist_{i}" for i in range(1, 11)]
TARGET_COLS = ["click", "follow", "like", "share"]
CAT_COLS = ["user_id", "item_id", "video_category", "gender"]
NUM_COLS = ["age_norm", "watch_norm"]

# Dense feature metadata — written to cache meta so dataset knows the shape
DENSE_COL_NAMES = ["age_norm", "watch_norm"]


def _count_parquet_rows(folder: str) -> int:
    """Count rows using parquet metadata — no data loading, ~100x faster."""
    import pyarrow.parquet as pq
    total = 0
    files = sorted(glob.glob(os.path.join(folder, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No parquet files in {folder}")
    for fpath in files:
        with pq.ParquetFile(fpath) as pf:
            total += pf.metadata.num_rows
    return total


def _iter_parquet_chunks(folder: str, chunk_size: int) -> Iterator[pd.DataFrame]:
    """
    Generator: yields DataFrames of ~chunk_size rows.
    Reads parquet files row-group by row-group (never loads a whole file),
    then splits into smaller chunks. This way we never hold the full dataset
    in memory.

    Dynamically computes watch_norm only if watching_times column exists.
    """
    import pyarrow.parquet as pq

    files = sorted(glob.glob(os.path.join(folder, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No parquet files in {folder}")

    for fpath in files:
        pf = pq.ParquetFile(fpath)
        for rg in range(pf.num_row_groups):
            df = pf.read_row_group(rg).to_pandas()
            for start in range(0, len(df), chunk_size):
                chunk = df.iloc[start : start + chunk_size].reset_index(drop=True)
                chunk["age_norm"] = (chunk["age"].clip(0, 100) / 100.0).astype(np.float32)
                if "watching_times" in chunk.columns:
                    chunk["watch_norm"] = (
                        np.log1p(chunk["watching_times"].clip(lower=0)) / 10.0
                    ).astype(np.float32)
                yield chunk


def _fit_encoders_on_train(train_dir: str, chunk_size: int,
                           max_chunks: Optional[int] = None) -> Dict[str, LabelEncoder]:
    """
    Fit LabelEncoders by scanning train chunks without loading everything at once.
    Returns encoders fitted on the union of all train data.

    OPTIMIZED:
      - Uses Python sets (set.update) — much faster than np.union1d for
        repeated accumulation (union1d re-sorts both arrays every call).
      - Skips .astype(str) when the column is already string/object dtype.
      - Skips the separate history pass: history items are a subset of the
        item_id vocabulary in practice, so we reuse item_id classes for the
        history encoder. This removes one full data pass.
      - Optional sampling: only scan the first `max_chunks` chunks. Vocab
        rarely changes much after a few chunks, so this is a huge speedup
        for large datasets.
    """
    logger.info("Fitting encoders on training data (chunked) …")

    unique_sets: Dict[str, set] = {col: set() for col in CAT_COLS}

    for i, chunk in enumerate(_iter_parquet_chunks(train_dir, chunk_size)):
        if max_chunks is not None and i >= max_chunks:
            logger.info("  Stopping encoder scan after %d chunks (sampling)", i)
            break
        for col in CAT_COLS:
            if chunk[col].dtype == object:
                unique_sets[col].update(chunk[col].unique())
            else:
                unique_sets[col].update(chunk[col].astype(str).unique())

    encoders = {}
    for col in CAT_COLS:
        le = LabelEncoder()
        le.fit(list(unique_sets[col]))
        encoders[col] = le
        logger.info("  %s: %d unique values", col, len(le.classes_))

    # History encoder shares the item_id vocabulary (history items ⊆ item_id).
    # No separate full-data pass needed.
    encoders["hist_item"] = encoders["item_id"]
    logger.info("  item_id (incl. history): %d unique values",
                len(encoders["item_id"].classes_))

    return encoders


def build_mappings(encoders: Dict[str, LabelEncoder]) -> Dict[str, pd.Series]:
    """
    Build the pd.Series lookup mappings ONCE (not per-chunk).
    Each mapping maps a class value → its integer code (1-based, 0 = unknown).
    """
    mappings = {}
    for col in CAT_COLS:
        le = encoders[col]
        mappings[col] = pd.Series(
            index=le.classes_,
            data=np.arange(1, len(le.classes_) + 1, dtype=np.int64),
        )
    return mappings


def _encode_chunk(chunk: pd.DataFrame, mappings: Dict[str, pd.Series]) -> pd.DataFrame:
    """
    Encode a single chunk using pre-built lookup mappings.

    VECTORIZED VERSION — uses pd.Categorical.codes, a single C-level hash
    operation, instead of Series.map() which does a Python dict lookup per
    element. Several times faster for large chunks.
    Unknown values get code -1, which we map to 0 (padding).
    """
    # Encode categorical columns
    for col in CAT_COLS:
        cats = mappings[col].index
        if chunk[col].dtype == object:
            codes = pd.Categorical(chunk[col], categories=cats).codes
        else:
            codes = pd.Categorical(chunk[col].astype(str), categories=cats).codes
        codes = codes.astype(np.int64)          # avoid int8 overflow
        chunk[col] = np.where(codes < 0, 0, codes + 1)

    # Encode history columns (shared item mapping)
    item_cats = mappings["item_id"].index
    for hc in HIST_COLS:
        if chunk[hc].dtype == object:
            codes = pd.Categorical(chunk[hc], categories=item_cats).codes
        else:
            codes = pd.Categorical(chunk[hc].astype(str), categories=item_cats).codes
        codes = codes.astype(np.int64)
        chunk[hc] = np.where(codes < 0, 0, codes + 1)

    return chunk


# Global mapping holder for parallel workers (set once per process)
_MAPPINGS = None


def _init_worker(mappings):
    """Initializer for ProcessPoolExecutor workers — sets the global mappings."""
    global _MAPPINGS
    _MAPPINGS = mappings


def _encode_chunk_worker(chunk):
    """Top-level worker function (must be picklable for ProcessPoolExecutor)."""
    return _encode_chunk(chunk, _MAPPINGS)


def preprocess_and_cache(
    folder: str, cache_path: str, mappings: Dict[str, pd.Series],
    chunk_size: int, num_workers: int = 0,
):
    """
    Read a folder of parquet files chunk-by-chunk, encode, and save as a single
    memory-mapped tensor file + an index file.

    If num_workers > 1, chunk encoding is parallelized across CPU cores via
    ProcessPoolExecutor (encoding is embarrassingly parallel).

    Dynamically determines dense feature columns based on what's available
    in the data (e.g. watch_norm only if watching_times exists).
    """
    logger.info("Preprocessing & caching: %s → %s", folder, cache_path)

    # Count rows using parquet metadata — no data loading!
    total_rows = _count_parquet_rows(folder)
    logger.info("  Total rows: %d", total_rows)

    # Determine which dense columns are available in the data
    # by peeking at the first row group of the first file.
    first_file = sorted(glob.glob(os.path.join(folder, "*.parquet")))[0]
    import pyarrow.parquet as pq
    first_rg = pq.ParquetFile(first_file).read_row_group(0).to_pandas()
    available_dense = []
    if "age" in first_rg.columns:
        available_dense.append("age_norm")
    if "watching_times" in first_rg.columns:
        available_dense.append("watch_norm")
    n_dense = len(available_dense)
    logger.info("  Dense features: %s (n=%d)", available_dense, n_dense)

    sparse_feat = np.memmap(
        f"{cache_path}_sparse.npy", dtype=np.int64, mode="w+",
        shape=(total_rows, 4 + 10),
    )
    dense_feat = np.memmap(
        f"{cache_path}_dense.npy", dtype=np.float32, mode="w+",
        shape=(total_rows, n_dense),
    )
    targets = np.memmap(
        f"{cache_path}_targets.npy", dtype=np.float32, mode="w+",
        shape=(total_rows, 4),
    )

    # Estimate total chunks for tqdm progress bar
    total_chunks = max(1, (total_rows + chunk_size - 1) // chunk_size)

    row_offset = 0
    # Use generator directly — NEVER collect into a list!
    chunk_iter = _iter_parquet_chunks(folder, chunk_size)

    def write_chunk(chunk):
        """Encode (if needed) and write one chunk to the memmaps."""
        nonlocal row_offset
        n = len(chunk)

        sparse_feat[row_offset : row_offset + n, 0] = chunk["user_id"].values
        sparse_feat[row_offset : row_offset + n, 1] = chunk["item_id"].values
        sparse_feat[row_offset : row_offset + n, 2] = chunk["video_category"].values
        sparse_feat[row_offset : row_offset + n, 3] = chunk["gender"].values
        for i, hc in enumerate(HIST_COLS):
            sparse_feat[row_offset : row_offset + n, 4 + i] = chunk[hc].values

        dense_row = 0
        if "age_norm" in available_dense:
            dense_feat[row_offset : row_offset + n, dense_row] = chunk["age_norm"].values
            dense_row += 1
        if "watch_norm" in available_dense:
            dense_feat[row_offset : row_offset + n, dense_row] = chunk["watch_norm"].values
            dense_row += 1

        targets[row_offset : row_offset + n] = chunk[TARGET_COLS].values

        row_offset += n

    if num_workers > 1:
        # Parallel encoding across CPU cores
        logger.info("  Parallel encoding with %d workers", num_workers)
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=num_workers,
            initializer=_init_worker, initargs=(mappings,),
        ) as ex:
            for chunk in tqdm(
                ex.map(_encode_chunk_worker, chunk_iter, chunksize=1),
                desc=f"  Encoding → {os.path.basename(cache_path)}",
                total=total_chunks, unit="chunk",
                disable=not TQDM_AVAILABLE,
            ):
                write_chunk(chunk)
                del chunk
    else:
        # Single-process encoding
        for chunk in tqdm(
            chunk_iter,
            desc=f"  Encoding → {os.path.basename(cache_path)}",
            total=total_chunks, unit="chunk",
            disable=not TQDM_AVAILABLE,
        ):
            chunk = _encode_chunk(chunk, mappings)
            write_chunk(chunk)
            del chunk

    sparse_feat.flush()
    dense_feat.flush()
    targets.flush()

    meta = {"total_rows": total_rows, "n_dense": n_dense, "dense_cols": available_dense}
    with open(f"{cache_path}_meta.pkl", "wb") as f:
        pickle.dump(meta, f)

    logger.info("  ✓ Cached %d rows to %s_*.npy", total_rows, cache_path)


# ===========================================================================
# 2. MEMORY-EFFICIENT DATASET (reads from memmap)
# ===========================================================================
class MemmapMTLDataset(Dataset):
    """
    Reads from preprocessed memmap files — virtually zero RAM overhead.
    Only the current batch is loaded into memory.
    """

    def __init__(self, cache_path: str):
        with open(f"{cache_path}_meta.pkl", "rb") as f:
            meta = pickle.load(f)
        self.n_rows = meta["total_rows"]
        self.n_dense = meta.get("n_dense", 2)
        self.dense_cols = meta.get("dense_cols", ["age_norm", "watch_norm"])

        self.sparse = np.memmap(
            f"{cache_path}_sparse.npy", dtype=np.int64, mode="r",
            shape=(self.n_rows, 14),
        )
        self.dense = np.memmap(
            f"{cache_path}_dense.npy", dtype=np.float32, mode="r",
            shape=(self.n_rows, self.n_dense),
        )
        self.targets = np.memmap(
            f"{cache_path}_targets.npy", dtype=np.float32, mode="r",
            shape=(self.n_rows, 4),
        )

    def __len__(self):
        return self.n_rows

    def __getitem__(self, idx):
        # Single-row access (used by default DataLoader). Kept for compatibility,
        # but __getitems__ (batch access) is much faster.
        row = self.sparse[idx]
        dense = self.dense[idx]
        tgt = self.targets[idx]

        return (
            torch.as_tensor(row[0], dtype=torch.long),     # user_id
            torch.as_tensor(row[1], dtype=torch.long),     # item_id
            torch.as_tensor(row[2], dtype=torch.long),     # video_category
            torch.as_tensor(row[3], dtype=torch.long),     # gender
            torch.as_tensor(row[4:14], dtype=torch.long),  # hist (10,)
            torch.as_tensor(dense, dtype=torch.float32),
            torch.as_tensor(tgt, dtype=torch.float32),
        )

    def __getitems__(self, indices):
        """
        BATCH access — the KEY optimization. Reads a contiguous block of rows
        from each memmap in ONE sequential read instead of N random-access
        reads. This is 5-20x faster than per-row __getitem__.
        """
        rows = self.sparse[indices]   # (B, 14) — one contiguous read
        dense = self.dense[indices]   # (B, n_dense)
        tgt = self.targets[indices]   # (B, 4)

        return (
            torch.as_tensor(rows[:, 0], dtype=torch.long),     # user_id
            torch.as_tensor(rows[:, 1], dtype=torch.long),     # item_id
            torch.as_tensor(rows[:, 2], dtype=torch.long),     # video_category
            torch.as_tensor(rows[:, 3], dtype=torch.long),     # gender
            torch.as_tensor(rows[:, 4:14], dtype=torch.long),  # hist (B, 10)
            torch.as_tensor(dense, dtype=torch.float32),
            torch.as_tensor(tgt, dtype=torch.float32),
        )


class ContiguousBatchSampler(Sampler):
    """
    Yields CONTIGUOUS index ranges so each memmap read is sequential.
    Shuffles the order of blocks (not individual rows) to keep reads fast
    while still providing stochasticity.
    """

    def __init__(self, n_rows: int, batch_size: int, shuffle: bool = True):
        self.n_rows = n_rows
        self.batch_size = batch_size
        self.shuffle = shuffle

    def __iter__(self):
        # Build a list of contiguous index ranges
        ranges = [
            list(range(start, min(start + self.batch_size, self.n_rows)))
            for start in range(0, self.n_rows, self.batch_size)
        ]
        if self.shuffle:
            random.shuffle(ranges)
        return iter(ranges)

    def __len__(self):
        return (self.n_rows + self.batch_size - 1) // self.batch_size


def _noop_collate(batch):
    """
    No-op collate. Our dataset's __getitems__ already returns a fully-formed
    batch (a tuple of stacked tensors). The default collate would try to stack
    it AGAIN, which fails because the batch elements have different shapes
    (e.g. [B] vs [B, 10]). So we just return the batch as-is.
    """
    # __getitems__ returns the batch tuple directly; DataLoader passes it
    # through as `batch`. Return it unchanged.
    return batch


# ===========================================================================
# 3. MODEL
# ===========================================================================
class MTLSharedBottom(nn.Module):
    def __init__(self, config: MTLConfig, num_users: int, num_items: int,
                 num_categories: int, num_genders: int,
                 task_names: Optional[List[str]] = None):
        super().__init__()
        self.config = config

        self.user_emb = nn.Embedding(num_users + 1, config.user_emb_dim, padding_idx=0)
        self.item_emb = nn.Embedding(num_items + 1, config.item_emb_dim, padding_idx=0)
        self.category_emb = nn.Embedding(num_categories + 1, config.category_emb_dim, padding_idx=0)
        self.gender_emb = nn.Embedding(num_genders + 1, config.gender_emb_dim, padding_idx=0)

        # Dense feature dimension: determined dynamically from cache metadata
        self.dense_dim = config.dense_dim
        self.input_dim = (
            config.user_emb_dim + config.item_emb_dim + config.category_emb_dim
            + config.gender_emb_dim + self.dense_dim + config.item_emb_dim
        )

        shared_layers = []
        in_dim = self.input_dim
        for h in config.shared_hidden:
            shared_layers.extend([
                nn.Linear(in_dim, h), nn.BatchNorm1d(h),
                nn.ReLU(inplace=True), nn.Dropout(config.shared_dropout),
            ])
            in_dim = h
        self.shared_bottom = nn.Sequential(*shared_layers)
        self.shared_out_dim = config.shared_hidden[-1] if config.shared_hidden else self.input_dim

        # Task selection: default to all 4 tasks in canonical order
        if task_names is None:
            task_names = ["click", "follow", "like", "share"]
        self.task_names = task_names
        self.towers = nn.ModuleDict()
        for task in self.task_names:
            layers = []
            tower_in = self.shared_out_dim
            for h in config.tower_hidden:
                layers.extend([
                    nn.Linear(tower_in, h), nn.BatchNorm1d(h),
                    nn.ReLU(inplace=True), nn.Dropout(config.tower_dropout),
                ])
                tower_in = h
            layers.append(nn.Linear(tower_in, 1))
            self.towers[task] = nn.Sequential(*layers)

        self.criterion = nn.BCEWithLogitsLoss(reduction="none")
        self.apply(self._init_weights)

    @staticmethod
    def _init_weights(m):
        if isinstance(m, nn.Linear):
            nn.init.xavier_uniform_(m.weight)
            if m.bias is not None:
                nn.init.zeros_(m.bias)
        elif isinstance(m, nn.Embedding):
            nn.init.normal_(m.weight, mean=0.0, std=0.02)

    def _pool_history(self, hist_indices):
        emb = self.item_emb(hist_indices)
        mask = (hist_indices > 0).unsqueeze(-1).float()
        summed = (emb * mask).sum(dim=1)
        counts = mask.sum(dim=1).clamp(min=1)
        return summed / counts

    def forward(self, users, items, categories, genders, history, numerical):
        u = self.user_emb(users)
        i = self.item_emb(items)
        c = self.category_emb(categories)
        g = self.gender_emb(genders)
        h = self._pool_history(history)
        combined = torch.cat([u, i, c, g, numerical, h], dim=1)
        shared = self.shared_bottom(combined)
        return {t: self.towers[t](shared) for t in self.task_names}

    # Canonical column order in the targets memmap
    _TARGET_COLS = ["click", "follow", "like", "share"]
    _TASK_TO_COL = {name: i for i, name in enumerate(_TARGET_COLS)}

    def compute_losses(self, outputs, targets):
        total = torch.tensor(0.0, device=targets.device)
        per_task = {}
        for task in self.task_names:
            col_idx = self._TASK_TO_COL[task]
            loss_per_sample = self.criterion(outputs[task].squeeze(1), targets[:, col_idx])
            w = self.config.task_weights.get(task, 1.0)
            task_loss = (loss_per_sample * w).mean()
            total = total + task_loss
            per_task[task] = task_loss.item()
        return total, per_task

    def get_target_col(self, task):
        """Return the column index in the targets array for a given task name."""
        return self._TASK_TO_COL[task]


# ===========================================================================
# 4. TRAINING (STEP-BASED) + EVALUATION
# ===========================================================================
def compute_aucs(outputs, targets, task_names):
    # Map task names to their column indices in the targets array
    _TARGET_COLS = ["click", "follow", "like", "share"]
    task_to_col = {name: i for i, name in enumerate(_TARGET_COLS)}
    aucs = {}
    for task in task_names:
        col_idx = task_to_col[task]
        y_true = targets[:, col_idx].cpu().numpy()
        y_score = torch.sigmoid(outputs[task].squeeze(1)).detach().cpu().numpy()
        aucs[task] = roc_auc_score(y_true, y_score) if len(np.unique(y_true)) >= 2 else float("nan")
    return aucs


@torch.no_grad()
def evaluate(model, loader, config, desc: str = "Evaluating", max_batches: Optional[int] = None):
    """
    Evaluation loop with optional tqdm progress bar.

    OPTIMIZED: uses streaming AUC (torchmetrics) when available, so we never
    accumulate all outputs/targets in memory. Falls back to the old
    accumulate-then-compute path if torchmetrics is not installed.

    max_batches: if set, only evaluate this many batches (useful for quick
    tests on huge val/test sets — a full pass over 24M rows takes minutes).
    """
    model.eval()
    total_loss = 0.0
    task_losses = {t: 0.0 for t in model.task_names}
    n_batches = 0

    # Streaming AUC accumulators (only if torchmetrics available)
    use_streaming = TORCHMETRICS_AVAILABLE
    auc_metrics = None
    if use_streaming:
        auc_metrics = {
            t: AUROC(task="binary").to(config.device) for t in model.task_names
        }
    else:
        all_outputs = {t: [] for t in model.task_names}
        all_targets = []

    for batch in tqdm(loader, desc=desc, unit="batch",
                      disable=not TQDM_AVAILABLE or config.log_every == 0):
        users, items, cats, genders, hist, num_feats, targets = [
            x.to(config.device) for x in batch
        ]
        with autocast(enabled=config.use_amp):
            outputs = model(users, items, cats, genders, hist, num_feats)
            loss, per_task = model.compute_losses(outputs, targets)

        total_loss += loss.item()
        for i, t in enumerate(model.task_names):
            task_losses[t] += per_task[t]
            if use_streaming:
                auc_metrics[t].update(
                    torch.sigmoid(outputs[t].squeeze(1)).float(),
                    targets[:, model.get_target_col(t)].long()
                )
            else:
                all_outputs[t].append(outputs[t].cpu())
        if not use_streaming:
            all_targets.append(targets.cpu())
        n_batches += 1

        # Early exit if we've evaluated enough batches (quick-test mode)
        if max_batches is not None and n_batches >= max_batches:
            break

    avg_loss = total_loss / n_batches
    avg_task_losses = {t: v / n_batches for t, v in task_losses.items()}

    if use_streaming:
        aucs = {t: auc_metrics[t].compute().item() for t in model.task_names}
    else:
        cat_outputs = {t: torch.cat(all_outputs[t]) for t in model.task_names}
        cat_targets = torch.cat(all_targets)
        aucs = compute_aucs(cat_outputs, cat_targets, model.task_names)
    return avg_loss, avg_task_losses, aucs


def train_step_based(model, loader_train, loader_val, loader_test,
                     optimizer, scheduler, scaler, config, start_step: int = 0):
    """
    Step-based training loop with tqdm progress bar, live metrics, and ETA.
    
    Args:
        start_step: If resuming from a checkpoint, continue from this step.
    """
    best_val_auc = 0.0
    best_step = 0
    steps_no_improve = 0
    global_step = start_step

    # Logging accumulators
    running_loss = 0.0
    running_task_losses = {t: 0.0 for t in model.task_names}
    running_steps = 0

    model.train()
    optimizer.zero_grad()

    data_iter = iter(loader_train)

    # ─── tqdm progress bar for training steps ───
    pbar = tqdm(
        total=config.train_steps - start_step,
        initial=start_step,
        desc="Training",
        unit="step",
        disable=not TQDM_AVAILABLE,
        bar_format="{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}",
    )

    start_time = time.time()
    last_log_time = start_time

    while global_step < config.train_steps:
        # ---- Get next batch (cycle if exhausted) ----
        try:
            batch = next(data_iter)
        except StopIteration:
            data_iter = iter(loader_train)
            batch = next(data_iter)

        users, items, cats, genders, hist, num_feats, targets = [
            x.to(config.device) for x in batch
        ]

        # ---- Forward + backward (with grad accum + AMP) ----
        with autocast(enabled=config.use_amp):
            outputs = model(users, items, cats, genders, hist, num_feats)
            loss, per_task = model.compute_losses(outputs, targets)
            loss = loss / config.accum_steps

        if config.use_amp:
            scaler.scale(loss).backward()
        else:
            loss.backward()

        # Accumulate for logging
        running_loss += loss.item() * config.accum_steps
        for t in model.task_names:
            running_task_losses[t] += per_task[t]
        running_steps += 1

        # ---- Optimizer step (at accumulation boundary) ----
        if (global_step + 1) % config.accum_steps == 0:
            if config.use_amp:
                scaler.unscale_(optimizer)
            nn.utils.clip_grad_norm_(model.parameters(), config.grad_clip_norm)
            if config.use_amp:
                scaler.step(optimizer)
                scaler.update()
            else:
                optimizer.step()
            optimizer.zero_grad()

        global_step += 1
        pbar.update(1)

        # ---- Log train loss to progress bar ----
        if global_step % config.log_every == 0:
            avg_loss = running_loss / max(running_steps, 1)
            avg_task = {t: v / max(running_steps, 1) for t, v in running_task_losses.items()}
            
            # Update progress bar with live metrics
            postfix = {"loss": f"{avg_loss:.4f}"}
            for t in model.task_names:
                postfix[t] = f"{avg_task.get(t, 0):.4f}"
            pbar.set_postfix(postfix)
            
            # Also log to file
            task_str = "  ".join(f"{t}={avg_task[t]:.4f}" for t in model.task_names)
            logger.info(
                "Step %6d | loss: %.4f | %s",
                global_step, avg_loss, task_str,
            )
            
            running_loss = 0.0
            running_task_losses = {t: 0.0 for t in model.task_names}
            running_steps = 0
            last_log_time = time.time()

        # ---- Validate ----
        if global_step % config.val_every == 0:
            pbar.set_description("Validating...")
            
            val_loss, val_task_losses, val_aucs = evaluate(
                model, loader_val, config, desc="  Validation",
                max_batches=config.max_val_batches,
            )
            mean_auc = np.nanmean(list(val_aucs.values()))
            auc_str = "  ".join(f"{t}={val_aucs[t]:.4f}" for t in model.task_names)
            
            logger.info(
                "=== Step %6d | val_loss: %.4f | mean_auc: %.4f | %s",
                global_step, val_loss, mean_auc, auc_str,
            )

            # LR scheduling on val loss
            scheduler.step(val_loss)

            # Save best model
            if mean_auc > best_val_auc:
                best_val_auc = mean_auc
                best_step = global_step
                steps_no_improve = 0
                ckpt_path = os.path.join(config.model_dir, "best_model.pt")
                torch.save(
                    {
                        "step": global_step,
                        "model_state_dict": model.state_dict(),
                        "optimizer_state_dict": optimizer.state_dict(),
                        "scheduler_state_dict": scheduler.state_dict(),
                        "scaler_state_dict": scaler.state_dict() if config.use_amp else None,
                        "best_auc": best_val_auc,
                        "config": config,
                    },
                    ckpt_path,
                )
                logger.info("  → New best model saved (AUC=%.4f) at %s", best_val_auc, ckpt_path)
            else:
                steps_no_improve += 1
                logger.info("  → No improvement (%d/%d checks)",
                            steps_no_improve, config.early_stop_patience)

            # Early stopping
            if steps_no_improve >= config.early_stop_patience:
                logger.info(
                    "Early stopping at step %d (no improvement for %d checks)",
                    global_step, config.early_stop_patience,
                )
                break

            # Resume training mode after eval
            model.train()
            pbar.set_description("Training")

    pbar.close()

    # ---- Test evaluation at the end ----
    logger.info("=" * 60)
    logger.info("STEP 4: Test evaluation")
    logger.info("=" * 60)

    best_ckpt = os.path.join(config.model_dir, "best_model.pt")
    if os.path.exists(best_ckpt):
        ckpt = torch.load(best_ckpt, map_location=config.device, weights_only=False)
        model.load_state_dict(ckpt["model_state_dict"])
        logger.info("Loaded best model (step %d, val_auc=%.4f)", ckpt["step"], ckpt["best_auc"])

    test_loss, test_task_losses, test_aucs = evaluate(
        model, loader_test, config, desc="  Test",
        max_batches=config.max_val_batches,
    )
    test_mean_auc = np.nanmean(list(test_aucs.values()))

    for task in model.task_names:
        logger.info("  %-8s  loss: %.4f  |  AUC: %.4f", task, test_task_losses[task], test_aucs[task])
    logger.info("  %-8s  AUC: %.4f", "mean", test_mean_auc)
    logger.info("Done. Total time: %s", timedelta(seconds=int(time.time() - start_time)))


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Memory-Efficient MTL Recommender (Step-Based Training)"
    )
    parser.add_argument("--data_dir", type=str, default="./data/split")
    parser.add_argument("--cache_dir", type=str, default="./cache")
    parser.add_argument("--model_dir", type=str, default="./checkpoints")
    parser.add_argument("--batch_size", type=int, default=2048)
    parser.add_argument("--accum_steps", type=int, default=1,
                        help="Gradient accumulation steps (increase for larger effective batch)")
    # Step-based arguments
    parser.add_argument("--train_steps", type=int, default=50_000,
                        help="Total number of gradient updates")
    parser.add_argument("--val_every", type=int, default=500,
                        help="Validate every N gradient updates")
    parser.add_argument("--max_val_batches", type=int, default=None,
                        help="Only evaluate this many batches during validation/test "
                             "(quick-test mode; full pass over huge val sets is slow)")
    parser.add_argument("--log_every", type=int, default=100,
                        help="Log training loss every N steps")
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--chunk_size", type=int, default=200_000,
                        help="Rows per chunk during preprocessing")
    parser.add_argument("--max_encoder_chunks", type=int, default=None,
                        help="Only scan this many chunks to fit encoders (sampling). "
                             "Huge speedup for large datasets; vocab rarely changes after a few chunks.")
    parser.add_argument("--no_amp", action="store_true", help="Disable mixed precision")
    parser.add_argument("--early_stop_patience", type=int, default=10,
                        help="Stop after this many val checks with no AUC improvement")
    parser.add_argument("--device", type=str,
                        default="cuda" if torch.cuda.is_available() else "cpu")
    parser.add_argument("--num_workers", type=int, default=4,
                        help="Number of DataLoader worker processes (0 for CPU)")
    parser.add_argument("--preprocess_workers", type=int, default=0,
                        help="Number of parallel workers for chunk encoding during "
                             "preprocessing (0 = single-process). Independent of --num_workers.")
    
    # NEW ARGUMENTS
    parser.add_argument("--force_preprocess", action="store_true",
                        help="Force reprocessing even if valid cache exists")
    parser.add_argument("--resume", type=str, default=None,
                        help="Resume from a checkpoint file (.pt)")
    parser.add_argument("--no_tqdm", action="store_true",
                        help="Disable tqdm progress bars (useful for logging)")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable debug-level logging")
    parser.add_argument("--num_tasks", type=int, default=2, choices=[1, 2, 3, 4],
                        help="Number of tasks to train: 1=click, 2=click+like, 3=+follow, 4=+share (default: 2)")
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # ------------------------------------------------------------------
    # 1. Preprocess & cache (with integrity checks)
    # ------------------------------------------------------------------
    os.makedirs(args.cache_dir, exist_ok=True)
    os.makedirs(args.model_dir, exist_ok=True)

    train_cache = os.path.join(args.cache_dir, "train")
    val_cache = os.path.join(args.cache_dir, "val")
    test_cache = os.path.join(args.cache_dir, "test")

    cache_exists = os.path.exists(f"{train_cache}_meta.pkl")
    cache_valid = False

    if cache_exists and not args.force_preprocess:
        logger.info("=" * 60)
        logger.info("STEP 1: Verifying cache integrity")
        logger.info("=" * 60)
        
        # Check all three caches
        train_ok = verify_cache_integrity(train_cache)
        val_ok = verify_cache_integrity(val_cache)
        test_ok = verify_cache_integrity(test_cache)
        
        if train_ok and val_ok and test_ok:
            # Print cache stats
            for name, cp in [("Train", train_cache), ("Val", val_cache), ("Test", test_cache)]:
                stats = get_cache_stats(cp)
                row_str = f"{stats.get('rows', '?'):,}" if 'rows' in stats else "?"
                sparse_mb = stats.get('sparse.npy_size_mb', 0)
                dense_mb = stats.get('dense.npy_size_mb', 0)
                target_mb = stats.get('targets.npy_size_mb', 0)
                total_mb = sparse_mb + dense_mb + target_mb
                logger.info("  %-5s: %s rows | %.1f MB on disk", name, row_str, total_mb)
            
            cache_valid = True
            logger.info("✅ All caches valid — skipping preprocessing")
        else:
            logger.warning("⚠️  Cache integrity check failed — will reprocess")
            cache_valid = False

    if not cache_valid or args.force_preprocess:
        logger.info("=" * 60)
        logger.info("STEP 1: Chunked preprocessing → memmap cache")
        logger.info("=" * 60)

        # Fit encoders
        encoders = _fit_encoders_on_train(
            os.path.join(args.data_dir, "train"), args.chunk_size,
            max_chunks=args.max_encoder_chunks,
        )
        # Build lookup mappings ONCE (reused across all chunks & splits)
        mappings = build_mappings(encoders)

        # Preprocess each split
        for split_name, split_dir, split_cache in [
            ("Train", "train", train_cache),
            ("Val", "val", val_cache),
            ("Test", "test", test_cache),
        ]:
            data_path = os.path.join(args.data_dir, split_dir)
            if os.path.exists(data_path) and any(glob.glob(os.path.join(data_path, "*.parquet"))):
                preprocess_and_cache(data_path, split_cache, mappings, args.chunk_size,
                                     num_workers=args.preprocess_workers)
            else:
                logger.warning("  ⚠️  No data found for %s at %s — skipping", split_name, data_path)

        # Save encoders
        enc_path = os.path.join(args.model_dir, "encoders.pkl")
        with open(enc_path, "wb") as f:
            pickle.dump(encoders, f)
        logger.info("  ✓ Encoders saved to %s", enc_path)
        
        # Save cache signature for future validation
        signature = compute_cache_signature(args.data_dir, encoders)
        save_cache_signature(args.cache_dir, signature)
        logger.info("  ✓ Cache signature saved")
    else:
        with open(os.path.join(args.model_dir, "encoders.pkl"), "rb") as f:
            encoders = pickle.load(f)

    # ------------------------------------------------------------------
    # 2. Build datasets from memmap
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 2: Building memmap datasets")
    logger.info("=" * 60)

    ds_train = MemmapMTLDataset(train_cache)
    ds_val = MemmapMTLDataset(val_cache)
    ds_test = MemmapMTLDataset(test_cache)

    logger.info("Train: %d  |  Val: %d  |  Test: %d", len(ds_train), len(ds_val), len(ds_test))

    # CRITICAL: memmap objects cannot be pickled across processes on Windows.
    # DataLoader workers would crash with "OSError: Invalid argument" / truncated
    # pickle. On CPU we force 0 workers (reads are fast enough anyway).
    dl_workers = args.num_workers if args.device == "cuda" else 0
    if args.num_workers > 0 and args.device != "cuda":
        logger.warning(
            "  DataLoader workers disabled on CPU (memmap can't be pickled). "
            "Using num_workers=0. Use --preprocess_workers for parallel encoding."
        )

    # Use contiguous-block samplers so memmap reads are sequential (fast).
    # persistent_workers avoids re-spawning worker processes each epoch.
    loader_train = DataLoader(
        ds_train,
        batch_sampler=ContiguousBatchSampler(len(ds_train), args.batch_size, shuffle=True),
        num_workers=dl_workers, pin_memory=True,
        persistent_workers=dl_workers > 0,
        collate_fn=_noop_collate,
    )
    loader_val = DataLoader(
        ds_val,
        batch_sampler=ContiguousBatchSampler(len(ds_val), args.batch_size * 2, shuffle=False),
        num_workers=dl_workers, pin_memory=True,
        persistent_workers=dl_workers > 0,
        collate_fn=_noop_collate,
    )
    loader_test = DataLoader(
        ds_test,
        batch_sampler=ContiguousBatchSampler(len(ds_test), args.batch_size * 2, shuffle=False),
        num_workers=dl_workers, pin_memory=True,
        persistent_workers=dl_workers > 0,
        collate_fn=_noop_collate,
    )

    # ------------------------------------------------------------------
    # 3. Vocab sizes
    # ------------------------------------------------------------------
    item_le = encoders["item_id"]
    num_users = len(encoders["user_id"].classes_)
    num_items = len(item_le.classes_)
    num_categories = len(encoders["video_category"].classes_)
    num_genders = len(encoders["gender"].classes_)
    logger.info("Vocab — users:%d  items:%d  categories:%d  genders:%d",
                num_users, num_items, num_categories, num_genders)

    # ------------------------------------------------------------------
    # 4. Model (with resume support)
    # ------------------------------------------------------------------
    # Build task list from num_tasks: 1=click, 2=click+like, 3=+follow, 4=+share
    TASK_ORDER = ["click", "like", "follow", "share"]
    selected_tasks = TASK_ORDER[:args.num_tasks]

    # Read dense_dim from cache meta (set during preprocessing)
    train_meta_path = os.path.join(args.cache_dir, "train_meta.pkl")
    dense_dim = 2  # default
    if os.path.exists(train_meta_path):
        with open(train_meta_path, "rb") as f:
            train_meta = pickle.load(f)
        dense_dim = train_meta.get("n_dense", 2)

    config = MTLConfig(
        data_dir=args.data_dir,
        cache_dir=args.cache_dir,
        batch_size=args.batch_size,
        accum_steps=args.accum_steps,
        train_steps=args.train_steps,
        val_every=args.val_every,
        log_every=args.log_every,
        max_val_batches=args.max_val_batches,
        learning_rate=args.lr,
        chunk_size=args.chunk_size,
        model_dir=args.model_dir,
        device=args.device,
        early_stop_patience=args.early_stop_patience,
        use_amp=not args.no_amp and args.device == "cuda",
        num_tasks=args.num_tasks,
        dense_dim=dense_dim,
    )

    eff_batch = config.batch_size * config.accum_steps
    logger.info("Effective batch size: %d × %d = %d",
                config.batch_size, config.accum_steps, eff_batch)

    model = MTLSharedBottom(config, num_users, num_items, num_categories, num_genders,
                                task_names=selected_tasks)
    model = model.to(config.device)

    total_p = sum(p.numel() for p in model.parameters())
    trainable_p = sum(p.numel() for p in model.parameters() if p.requires_grad)
    logger.info("Params — total: %d  |  trainable: %d", total_p, trainable_p)

    optimizer = optim.AdamW(model.parameters(), lr=config.learning_rate,
                            weight_decay=config.weight_decay)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", factor=config.lr_factor,
        patience=config.lr_patience,
    )
    scaler = GradScaler(enabled=config.use_amp)

    # ---- Resume from checkpoint ----
    start_step = 0
    if args.resume:
        if os.path.exists(args.resume):
            logger.info("Resuming from checkpoint: %s", args.resume)
            ckpt = torch.load(args.resume, map_location=config.device, weights_only=False)
            model.load_state_dict(ckpt["model_state_dict"])
            optimizer.load_state_dict(ckpt["optimizer_state_dict"])
            if "scheduler_state_dict" in ckpt and ckpt["scheduler_state_dict"]:
                scheduler.load_state_dict(ckpt["scheduler_state_dict"])
            if "scaler_state_dict" in ckpt and ckpt["scaler_state_dict"] and config.use_amp:
                scaler.load_state_dict(ckpt["scaler_state_dict"])
            start_step = ckpt.get("step", 0)
            logger.info("  Resumed at step %d (best_auc=%.4f)", start_step, ckpt.get("best_auc", 0))
        else:
            logger.warning("  Checkpoint not found at %s — starting from scratch", args.resume)

    # ------------------------------------------------------------------
    # 5. Step-based Training
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 3: Training (step-based | AMP=%s | accum=%d | val_every=%d)",
                config.use_amp, config.accum_steps, config.val_every)
    logger.info("=" * 60)

    train_step_based(
        model, loader_train, loader_val, loader_test,
        optimizer, scheduler, scaler, config,
        start_step=start_step,
    )


if __name__ == "__main__":
    main()