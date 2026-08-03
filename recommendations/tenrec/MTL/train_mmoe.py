#!/usr/bin/env python3
"""
MMoE Multi-Task Recommender (Gen 3 Multi-Gated Mixture of Experts)
===================================================================

Architecture:
  Embeddings → Concat → K Expert Towers → Per-Task Gates → Per-Task Towers → Scores

Features:
  - Reuses existing memmap cache (cache/{train,val,test}_*.npy) by default
  - Independent parquet→cache preprocessing path (rerun if needed)
  - Multi-head attention history pooling (matches shared-bottom ATTN_PROJ_DIM)
  - All 4 tasks (click/follow/like/share), controlled by --num-tasks
  - watching_times kept as dense feature only when column exists (dynamic n_dense)
  - Expert analysis: task affinity (gradient norms), feature utilization
  - Gate analysis: cosine similarity matrix between task gates
  - Step-based training with validation, early stopping, checkpointing
  - AMP mixed-precision, gradient clipping, ReduceLROnPlateau

Device support:
  - CUDA (NVIDIA GPU) — full AMP, pin_memory, multi-worker DataLoader
  - DirectML (AMD/Intel GPU on Windows via `torch-directml`) — no AMP, workers + pin_memory
  - CPU — fallback, no AMP, single-threaded DataLoader

Usage:
  python train_mmoe.py                          # train with defaults (auto device)
  python train_mmoe.py --device cuda            # force CUDA
  python train_mmoe.py --device directml        # force DirectML (Windows, needs torch-directml)
  python train_mmoe.py --device cpu             # force CPU
  python train_mmoe.py --num-tasks 2            # click + like only
  python train_mmoe.py --num-experts 8          # 8 experts instead of 4
  python train_mmoe.py --force-preprocess       # rebuild cache from parquet
  python train_mmoe.py --resume checkpoints/best_model.pt

Small-scale local testing (DirectML example):
  python train_mmoe.py --device directml --batch-size 512 --train-steps 5000 --val-every 250 --num-workers 2
"""

import sys
import io

# Fix Windows console encoding issue
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import argparse
import glob
import hashlib
import json
import logging
import os
import pickle
import random
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

# Optional: tqdm for progress bars — graceful fallback if not installed
try:
    from tqdm import tqdm, trange
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
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

# Optional: matplotlib for diagnostics plots — graceful fallback if not installed
try:
    import matplotlib
    matplotlib.use("Agg")  # non-interactive backend
    import matplotlib.pyplot as plt
    import seaborn as sns
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    plt = None
    sns = None

# ---------------------------------------------------------------------------
# Device resolution helpers
# ---------------------------------------------------------------------------
def resolve_device(device_arg: str) -> torch.device:
    """
    Resolve a device string to a torch.device.

    Args:
        device_arg: One of "auto", "cuda", "directml", "cpu"

    Returns:
        torch.device instance

    Raises:
        RuntimeError: if requested device is unavailable
    """
    if device_arg == "auto":
        # Priority: CUDA > DirectML > CPU
        if torch.cuda.is_available():
            return torch.device("cuda")
        try:
            import torch_directml
            return torch_directml.device()
        except ImportError:
            return torch.device("cpu")

    if device_arg == "cuda":
        if not torch.cuda.is_available():
            raise RuntimeError("CUDA requested but not available")
        return torch.device("cuda")

    if device_arg == "directml":
        try:
            import torch_directml
            return torch_directml.device()
        except ImportError:
            raise RuntimeError(
                "DirectML requested but torch-directml not installed. "
                "Install with: pip install torch-directml"
            )

    if device_arg == "cpu":
        return torch.device("cpu")

    raise ValueError(f"Unknown device: {device_arg}. Choose from: auto, cuda, directml, cpu")


def is_cuda(device: torch.device) -> bool:
    """Check if device is CUDA."""
    return device.type == "cuda"


def is_directml(device: torch.device) -> bool:
    """Check if device is DirectML (privateuseone)."""
    return device.type == "privateuseone"


def supports_amp(device: torch.device) -> bool:
    """Check if device supports AMP (CUDA only)."""
    return is_cuda(device)


def supports_pin_memory(device: torch.device) -> bool:
    """Check if device benefits from pin_memory (CUDA only)."""
    return is_cuda(device)


def supports_workers(device: torch.device) -> bool:
    """Check if device benefits from DataLoader workers (CUDA and DirectML).

    Disabled on Windows: DataLoader workers use the 'spawn' start method,
    which pickles the dataset (including np.memmap) through a pipe to each
    worker. This fails on Windows with OSError/MemoryError. Use num_workers=0.
    """
    if os.name == "nt":
        return False
    return is_cuda(device) or is_directml(device)


def clip_grad_norm_sparse(parameters, max_norm: float, norm_type: float = 2.0):
    """Gradient clipping that supports sparse gradients (from sparse embeddings).

    `nn.utils.clip_grad_norm_` fails on sparse grads because it applies an
    in-place `.mul_()` which sparse tensors do not support. Here we:
      - compute the norm on dense views (`.to_dense()` for sparse grads)
      - scale sparse grads by creating a new coalesced tensor (no in-place op)
    """
    parameters = [p for p in parameters if p.grad is not None]
    if not parameters:
        return 0.0

    device = parameters[0].grad.device
    total_norm = torch.norm(
        torch.stack([
            torch.norm(
                p.grad.detach().to_dense() if p.grad.is_sparse else p.grad.detach(),
                norm_type,
            ).to(device)
            for p in parameters
        ]),
        norm_type,
    )

    clip_coef = max_norm / (total_norm + 1e-6)
    if clip_coef < 1:
        for p in parameters:
            if p.grad.is_sparse:
                # Scalar multiplication on a sparse tensor returns a new sparse
                # tensor (no in-place op) — safe to reassign to p.grad.
                p.grad = p.grad.coalesce() * clip_coef
            else:
                p.grad.detach().mul_(clip_coef)
    return total_norm


# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger("mmoe_trainer")

log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "training.log")
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
class MMoEConfig:
    # ---- Data ----
    data_dir: str = "./data/split"
    cache_dir: str = "./cache"
    chunk_size: int = 200_000

    # ---- Feature columns (single source of truth) ----
    cat_cols: List[str] = field(default_factory=lambda: ["user_id", "item_id", "video_category", "gender"])
    num_cols: List[str] = field(default_factory=lambda: ["age_norm", "watch_norm"])
    hist_cols: List[str] = field(default_factory=lambda: [f"hist_{i}" for i in range(1, 11)])

    # ---- Model ----
    num_experts: int = 4
    expert_hidden: List[int] = field(default_factory=lambda: [256, 128])
    expert_dropout: float = 0.2
    # Gate: input_dim → num_experts (softmax)
    gate_hidden: List[int] = field(default_factory=lambda: [64])
    gate_dropout: float = 0.1
    # Per-task tower
    tower_hidden: List[int] = field(default_factory=lambda: [64, 32])
    tower_dropout: float = 0.15

    # Embedding dimensions (sized from config.py conventions)
    user_emb_dim: int = 64
    item_emb_dim: int = 64
    category_emb_dim: int = 4
    gender_emb_dim: int = 2
    # Sparse embeddings: use SparseAdam (saves memory, but incompatible with AMP)
    sparse_embeddings: bool = True
    # Attention history pooling
    attn_heads: int = 4
    attn_proj_dim: int = 64
    attn_dropout: float = 0.1

    # Dense features (derived from num_cols)
    dense_dim: int = field(init=False)

    def __post_init__(self):
        self.dense_dim = len(self.num_cols)

    # ---- Training (step-based) ----
    batch_size: int = 2048
    accum_steps: int = 1
    train_steps: int = 50_000
    val_every: int = 500
    log_every: int = 100
    max_val_batches: int = 20
    learning_rate: float = 1e-3
    weight_decay: float = 1e-5
    grad_clip_norm: float = 5.0
    lr_patience: int = 3
    lr_factor: float = 0.5
    early_stop_patience: int = 10
    use_amp: bool = True

    # ---- Gate/Expert regularization ----
    gate_temperature: float = 1
    gate_entropy_coef: float = 0.01
    expert_diversity_coef: float = 0.01

    task_weights: Dict[str, float] = field(
        default_factory=lambda: {"click": 1.0, "follow": 1.0, "like": 1.5, "share": 1.0}
    )
    num_tasks: int = 4  # 1=click, 2=+like, 3=+follow, 4=+share

    model_dir: str = "./checkpoints"
    runs_dir: str = "./runs"
    num_workers: int = 4
    pin_memory: bool = True
    device: torch.device = field(default_factory=lambda: torch.device("cpu"))

    # ---- Diagnostics ----
    diag_every: int = 500          # run expert/gate diagnostics every N val checks
    diag_max_batches: int = 200    # cap batches for diagnostics (speed)


# ===========================================================================
# 0. CACHE / PREPROCESSING (independent of shared-bottom)
# ===========================================================================
# Module-level column definitions (single source of truth)
CAT_COLS = [#"user_id",
             "item_id", "video_category", "gender"]
NUM_COLS = ["age_norm", "watch_norm"]
HIST_COLS = [f"hist_{i}" for i in range(1, 11)]
TARGET_COLS = ["click", "follow", "like", "share"]


def _count_parquet_rows(folder: str) -> int:
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
    import pyarrow.parquet as pq
    files = sorted(glob.glob(os.path.join(folder, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No parquet files in {folder}")
    for fpath in files:
        pf = pq.ParquetFile(fpath)
        for rg in range(pf.num_row_groups):
            df = pf.read_row_group(rg).to_pandas()
            # Normalize dense features dynamically from NUM_COLS
            if "age_norm" in NUM_COLS and "age" in df.columns:
                df["age_norm"] = (df["age"].clip(0, 100) / 100.0).astype(np.float32)
            if "watch_norm" in NUM_COLS and "watching_times" in df.columns:
                df["watch_norm"] = (
                    np.log1p(df["watching_times"].clip(lower=0)) / 10.0
                ).astype(np.float32)
            # Extensible: add more normalization rules here for other dense features
            for start in range(0, len(df), chunk_size):
                yield df.iloc[start : start + chunk_size].reset_index(drop=True)


def _detect_dense_cols(folder: str) -> List[str]:
    """Peek at first row group to detect which dense columns exist."""
    import pyarrow.parquet as pq
    first_file = sorted(glob.glob(os.path.join(folder, "*.parquet")))[0]
    first_rg = pq.ParquetFile(first_file).read_row_group(0).to_pandas()
    available = []
    for col in NUM_COLS:
        if col == "age_norm" and "age" in first_rg.columns:
            available.append("age_norm")
        elif col == "watch_norm" and "watching_times" in first_rg.columns:
            available.append("watch_norm")
        # Extensible: add more conditions here for other dense features
    return available


def _fit_encoders_on_train(train_dir: str, chunk_size: int,
                           max_chunks: Optional[int] = None) -> Dict[str, LabelEncoder]:
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
    encoders["hist_item"] = encoders["item_id"]
    logger.info("  item_id (incl. history): %d unique values",
                len(encoders["item_id"].classes_))
    return encoders


def _build_mappings(encoders: Dict[str, LabelEncoder]) -> Dict[str, pd.Series]:
    mappings = {}
    for col in CAT_COLS:
        le = encoders[col]
        mappings[col] = pd.Series(
            index=le.classes_,
            data=np.arange(1, len(le.classes_) + 1, dtype=np.int64),
        )
    return mappings


def _encode_chunk(chunk: pd.DataFrame, mappings: Dict[str, pd.Series]) -> pd.DataFrame:
    for col in CAT_COLS:
        cats = mappings[col].index
        if chunk[col].dtype == object:
            codes = pd.Categorical(chunk[col], categories=cats).codes
        else:
            codes = pd.Categorical(chunk[col].astype(str), categories=cats).codes
        chunk[col] = np.where(codes < 0, 0, codes.astype(np.int64) + 1)
    item_cats = mappings["item_id"].index
    for hc in HIST_COLS:
        if chunk[hc].dtype == object:
            codes = pd.Categorical(chunk[hc], categories=item_cats).codes
        else:
            codes = pd.Categorical(chunk[hc].astype(str), categories=item_cats).codes
        chunk[hc] = np.where(codes < 0, 0, codes.astype(np.int64) + 1)
    return chunk


def preprocess_and_cache(
    folder: str, cache_path: str, mappings: Dict[str, pd.Series],
    chunk_size: int, num_workers: int = 0,
    cat_cols: Optional[List[str]] = None,
    num_cols: Optional[List[str]] = None,
    hist_cols: Optional[List[str]] = None,
):
    """Chunked parquet → memmap cache (independent of shared-bottom)."""
    cat_cols = cat_cols or CAT_COLS
    num_cols = num_cols or NUM_COLS
    hist_cols = hist_cols or HIST_COLS
    
    logger.info("Preprocessing & caching: %s → %s", folder, cache_path)
    total_rows = _count_parquet_rows(folder)
    logger.info("  Total rows: %d", total_rows)

    available_dense = _detect_dense_cols(folder)
    n_dense = len(available_dense)
    logger.info("  Dense features: %s (n=%d)", available_dense, n_dense)

    n_sparse = len(cat_cols) + len(hist_cols)
    sparse_feat = np.memmap(
        f"{cache_path}_sparse.npy", dtype=np.int64, mode="w+",
        shape=(total_rows, n_sparse),
    )
    dense_feat = np.memmap(
        f"{cache_path}_dense.npy", dtype=np.float32, mode="w+",
        shape=(total_rows, n_dense),
    )
    targets = np.memmap(
        f"{cache_path}_targets.npy", dtype=np.float32, mode="w+",
        shape=(total_rows, 4),
    )

    total_chunks = max(1, (total_rows + chunk_size - 1) // chunk_size)
    row_offset = 0

    for chunk in tqdm(
        _iter_parquet_chunks(folder, chunk_size),
        desc=f"  Encoding → {os.path.basename(cache_path)}",
        total=total_chunks, unit="chunk",
        disable=not TQDM_AVAILABLE,
    ):
        chunk = _encode_chunk(chunk, mappings)
        n = len(chunk)

        # Write categorical columns
        for i, col in enumerate(cat_cols):
            sparse_feat[row_offset : row_offset + n, i] = chunk[col].values
        # Write history columns
        for i, hc in enumerate(hist_cols):
            sparse_feat[row_offset : row_offset + n, len(cat_cols) + i] = chunk[hc].values

        dense_row = 0
        for col in available_dense:
            dense_feat[row_offset : row_offset + n, dense_row] = chunk[col].values
            dense_row += 1

        targets[row_offset : row_offset + n] = chunk[TARGET_COLS].values
        row_offset += n
        del chunk

    sparse_feat.flush()
    dense_feat.flush()
    targets.flush()

    meta = {
        "total_rows": total_rows,
        "n_dense": n_dense,
        "dense_cols": available_dense,
        "cat_cols": cat_cols,
        "num_cols": num_cols,
        "hist_cols": hist_cols,
    }
    with open(f"{cache_path}_meta.pkl", "wb") as f:
        pickle.dump(meta, f)
    logger.info("  ✓ Cached %d rows to %s_*.npy", total_rows, cache_path)


def verify_cache_integrity(cache_path: str, cat_cols: Optional[List[str]] = None,
                            num_cols: Optional[List[str]] = None,
                            hist_cols: Optional[List[str]] = None) -> bool:
    """Verify memmap cache is complete and valid, and column lists match."""
    cat_cols = cat_cols or CAT_COLS
    num_cols = num_cols or NUM_COLS
    hist_cols = hist_cols or HIST_COLS
    
    CACHE_FILES = ["sparse.npy", "dense.npy", "targets.npy", "meta.pkl"]
    for fname in CACHE_FILES:
        if not os.path.exists(f"{cache_path}_{fname}"):
            logger.warning("  ❌ Cache incomplete at %s — missing: %s", cache_path, fname)
            return False
    try:
        with open(f"{cache_path}_meta.pkl", "rb") as f:
            meta = pickle.load(f)
        cached_rows = meta["total_rows"]
        if cached_rows == 0:
            return False
        for fname, exp_dtype in [("sparse.npy", np.int64), ("dense.npy", np.float32),
                                  ("targets.npy", np.float32)]:
            fpath = f"{cache_path}_{fname}"
            if os.path.getsize(fpath) == 0:
                return False
            arr = np.memmap(fpath, mode="r", dtype=exp_dtype)
            _ = arr[0]
            if cached_rows > 1:
                _ = arr[cached_rows - 1]
        
        # Verify column lists match
        if meta.get("cat_cols") != cat_cols:
            logger.warning("  ❌ Cache cat_cols mismatch: stored=%s, current=%s", meta.get("cat_cols"), cat_cols)
            return False
        if meta.get("num_cols") != num_cols:
            logger.warning("  ❌ Cache num_cols mismatch: stored=%s, current=%s", meta.get("num_cols"), num_cols)
            return False
        if meta.get("hist_cols") != hist_cols:
            logger.warning("  ❌ Cache hist_cols mismatch: stored=%s, current=%s", meta.get("hist_cols"), hist_cols)
            return False
            
    except Exception as e:
        logger.warning("  ❌ Cache corrupt: %s", e)
        return False
    logger.info("  ✅ Cache valid: %d rows", cached_rows)
    return True


# ===========================================================================
# 1. DATASET
# ===========================================================================
class MMoEDataset(Dataset):
    """Reads from preprocessed memmap files — virtually zero RAM overhead.
    
    Returns structured tensors:
      - sparse: (n_cat + n_hist,) int64
      - numerical: (n_dense,) float32
      - targets: (n_tasks,) float32
    """

    def __init__(self, cache_path: str):
        with open(f"{cache_path}_meta.pkl", "rb") as f:
            meta = pickle.load(f)
        self.n_rows = meta["total_rows"]
        self.n_dense = meta.get("n_dense", 2)
        self.dense_cols = meta.get("dense_cols", ["age_norm", "watch_norm"])
        self.cat_cols = meta.get("cat_cols", CAT_COLS)
        self.hist_cols = meta.get("hist_cols", HIST_COLS)
        self.n_cat = len(self.cat_cols)
        self.n_hist = len(self.hist_cols)

        self.sparse = np.memmap(
            f"{cache_path}_sparse.npy", dtype=np.int64, mode="r",
            shape=(self.n_rows, self.n_cat + self.n_hist),
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
        sparse = self.sparse[idx]
        dense = self.dense[idx]
        tgt = self.targets[idx]
        return (
            torch.as_tensor(sparse, dtype=torch.long),
            torch.as_tensor(dense, dtype=torch.float32),
            torch.as_tensor(tgt, dtype=torch.float32),
        )

    def __getitems__(self, indices):
        sparse = self.sparse[indices]
        dense = self.dense[indices]
        tgt = self.targets[indices]
        return (
            torch.as_tensor(sparse, dtype=torch.long),
            torch.as_tensor(dense, dtype=torch.float32),
            torch.as_tensor(tgt, dtype=torch.float32),
        )


class ContiguousBatchSampler(Sampler):
    def __init__(self, n_rows: int, batch_size: int, shuffle: bool = True):
        self.n_rows = n_rows
        self.batch_size = batch_size
        self.shuffle = shuffle

    def __iter__(self):
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
    return batch


# ===========================================================================
# 2. MMoE MODEL
# ===========================================================================
class HistoryPool(nn.Module):
    """
    History item embedding pooling.
    Uses mean-pool over non-padding positions (matching shared-bottom).
    Set use_attention=True to enable multi-head attention pooling.
    """

    def __init__(self, item_emb_dim: int, proj_dim: int, num_heads: int = 4,
                 dropout: float = 0.1, use_attention: bool = False):
        super().__init__()
        self.use_attention = use_attention
        if use_attention:
            self.proj = nn.Linear(item_emb_dim, proj_dim)
            self.attn = nn.MultiheadAttention(proj_dim, num_heads,
                                              dropout=dropout, batch_first=True)
            self.norm = nn.LayerNorm(proj_dim)
        else:
            # Mean-pool path: project to proj_dim then average
            self.proj = nn.Linear(item_emb_dim, proj_dim)

    def forward(self, hist_indices: torch.Tensor, item_emb: nn.Embedding) -> torch.Tensor:
        # hist_indices: (B, H)  — 0 = padding
        emb = item_emb(hist_indices)          # (B, H, item_emb_dim)
        mask = (hist_indices > 0).unsqueeze(-1).float()  # (B, H, 1)
        lengths = mask.sum(dim=1).clamp(min=1)           # (B, 1)

        projected = self.proj(emb)                       # (B, H, proj_dim)

        if self.use_attention:
            # CRITICAL: never feed attention an all-masked row. Softmax over
            # all -inf → 0/0 → NaN, which also poisons the *gradients* of
            # in_proj/out_proj even if we mask the output afterward.
            valid = (hist_indices > 0)                        # (B, H) True = real history
            row_has_history = valid.any(dim=1, keepdim=True)  # (B, 1)
            # Only mask positions that are padding in rows that DO have history.
            # Fully-padding rows get an all-False mask (fully valid).
            key_padding_mask = valid.logical_not() & row_has_history  # (B, H)

            # For fully-padding rows, substitute a valid index (1 = real item)
            # so attention is well-defined and gradients stay finite. For rows
            # that DO have history, these are masked positions → ignored anyway.
            attn_input = hist_indices.masked_fill(valid.logical_not(), 1)
            proj_input = self.proj(item_emb(attn_input))       # (B, H, proj_dim)

            attn_out, _ = self.attn(proj_input, proj_input, proj_input,
                                    key_padding_mask=key_padding_mask)  # (B, H, proj_dim)
            # Zero out fully-padding rows entirely (they have no real history),
            # then mean-pool over valid positions.
            attn_out = attn_out * mask
            pooled = attn_out.sum(dim=1) / lengths
            return self.norm(pooled)
        else:
            # Mean-pool over non-padding positions
            pooled = (projected * mask).sum(dim=1) / lengths  # (B, proj_dim)
            return pooled


class ExpertTower(nn.Module):
    """A single expert: MLP that maps input_dim → expert_out_dim."""

    def __init__(self, input_dim: int, hidden_dims: List[int], out_dim: int, dropout: float):
        super().__init__()
        layers = []
        in_dim = input_dim
        for h in hidden_dims:
            layers.extend([
                nn.Linear(in_dim, h), nn.BatchNorm1d(h),
                nn.ReLU(inplace=True), nn.Dropout(dropout),
            ])
            in_dim = h
        layers.append(nn.Linear(in_dim, out_dim))
        self.net = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


class GateNetwork(nn.Module):
    """Per-task gate: input → softmax over experts."""

    def __init__(self, input_dim: int, hidden_dims: List[int], num_experts: int, dropout: float, temperature: float = 1.0):
        super().__init__()
        self.temperature = temperature
        layers = []
        in_dim = input_dim
        for h in hidden_dims:
            layers.extend([
                nn.Linear(in_dim, h), nn.BatchNorm1d(h),
                nn.ReLU(inplace=True), nn.Dropout(dropout),
            ])
            in_dim = h
        layers.append(nn.Linear(in_dim, num_experts))
        self.net = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return torch.softmax(self.net(x) / self.temperature, dim=-1)  # (B, num_experts)


class TaskTower(nn.Module):
    """Per-task mini-DNN: expert_out_dim → 1 logit."""

    def __init__(self, input_dim: int, hidden_dims: List[int], dropout: float):
        super().__init__()
        layers = []
        in_dim = input_dim
        for h in hidden_dims:
            layers.extend([
                nn.Linear(in_dim, h), nn.BatchNorm1d(h),
                nn.ReLU(inplace=True), nn.Dropout(dropout),
            ])
            in_dim = h
        layers.append(nn.Linear(in_dim, 1))
        self.net = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)  # (B, 1)


class MTLMMoE(nn.Module):
    """
    Multi-Gated Mixture of Experts for Multi-Task Learning.

    Architecture:
      Embeddings → Concat → [K Expert Towers] → [Per-Task Gates] → [Per-Task Towers] → Scores
    """

    def __init__(self, config: MMoEConfig, vocab_sizes: Dict[str, int],
                 task_names: Optional[List[str]] = None):
        super().__init__()
        self.config = config
        self.cat_cols = config.cat_cols
        self.num_cols = config.num_cols
        self.hist_cols = config.hist_cols

        # Embeddings - dynamically built from cat_cols
        self.embeddings = nn.ModuleDict()
        emb_dims = {}
        for col in self.cat_cols:
            if col == "user_id":
                dim = config.user_emb_dim
            elif col == "item_id":
                dim = config.item_emb_dim
            elif col == "video_category":
                dim = config.category_emb_dim
            elif col == "gender":
                dim = config.gender_emb_dim
            else:
                dim = config.user_emb_dim  # default fallback
            self.embeddings[col] = nn.Embedding(vocab_sizes[col] + 1, dim, padding_idx=0,
                                                sparse=config.sparse_embeddings)
            emb_dims[col] = dim

        # History pooling (mean-pool by default; attention via use_attention flag)
        self.attn_pool = HistoryPool(
            item_emb_dim=config.item_emb_dim,
            proj_dim=config.attn_proj_dim,
            num_heads=config.attn_heads,
            dropout=config.attn_dropout,
            use_attention=False,  # set True to enable attention pooling
        )

        # Input dim = sum of all embedding dims + dense_dim + attn_proj_dim
        self.input_dim = sum(emb_dims.values()) + config.dense_dim + config.attn_proj_dim

        # K expert towers (all share the same input → output dim)
        expert_out_dim = config.expert_hidden[-1] if config.expert_hidden else self.input_dim
        self.experts = nn.ModuleList([
            ExpertTower(
                self.input_dim, config.expert_hidden, expert_out_dim, config.expert_dropout
            )
            for _ in range(config.num_experts)
        ])

        # Per-task gate networks
        self.task_names = task_names or ["click", "like", "follow", "share"]
        self.gates = nn.ModuleDict({
            task: GateNetwork(
                self.input_dim, config.gate_hidden, config.num_experts, config.gate_dropout,
                temperature=config.gate_temperature
            )
            for task in self.task_names
        })

        # Per-task towers
        self.towers = nn.ModuleDict()
        for task in self.task_names:
            self.towers[task] = TaskTower(
                expert_out_dim, config.tower_hidden, config.tower_dropout
            )

        self.criterion = nn.BCEWithLogitsLoss(reduction="none")
        self.expert_out_dim = expert_out_dim
        self.apply(self._init_weights)

    @staticmethod
    def _init_weights(m):
        if isinstance(m, nn.Linear):
            nn.init.xavier_uniform_(m.weight)
            if m.bias is not None:
                nn.init.zeros_(m.bias)
        elif isinstance(m, nn.Embedding):
            nn.init.normal_(m.weight, mean=0.0, std=0.02)

    def forward(self, sparse: torch.Tensor, numerical: torch.Tensor):
        # sparse: (B, n_cat + n_hist)
        # numerical: (B, n_dense)
        B = sparse.size(0)
        n_cat = len(self.cat_cols)
        
        # Split sparse into categorical and history
        cat_indices = sparse[:, :n_cat]      # (B, n_cat)
        hist_indices = sparse[:, n_cat:]     # (B, n_hist)

        # Embed each categorical feature
        emb_list = []
        for i, col in enumerate(self.cat_cols):
            emb = self.embeddings[col](cat_indices[:, i])  # (B, emb_dim)
            emb_list.append(emb)
        
        # History pooling using item_emb
        interest = self.attn_pool(hist_indices, self.embeddings["item_id"])  # (B, attn_proj_dim)

        # Concatenate all features
        combined = torch.cat(emb_list + [numerical, interest], dim=1)  # (B, input_dim)

        # Expert outputs: list of (B, expert_out_dim)
        expert_outputs = [expert(combined) for expert in self.experts]
        expert_stack = torch.stack(expert_outputs, dim=1)               # (B, K, expert_out_dim)

        # Per-task gate → weighted sum of experts → task tower → logit
        outputs = {}
        gate_weights = {}
        for task in self.task_names:
            gate_w = self.gates[task](combined)                         # (B, K)
            gate_weights[task] = gate_w
            # Weighted sum: (B, K) @ (B, K, expert_out_dim) → (B, expert_out_dim)
            consolidated = torch.bmm(gate_w.unsqueeze(1), expert_stack).squeeze(1)
            outputs[task] = self.towers[task](consolidated)             # (B, 1)

        # Expert diversity regularization: penalize similar expert outputs
        if self.config.expert_diversity_coef > 0.0:
            # expert_stack: (B, K, D) - compute pairwise cosine similarity between experts
            # Normalize expert outputs
            expert_norm = expert_stack / (expert_stack.norm(dim=-1, keepdim=True) + 1e-12)
            # Pairwise cosine similarity: (B, K, K)
            cos_sim = torch.bmm(expert_norm, expert_norm.transpose(1, 2))
            # Penalize off-diagonal elements (similarity between different experts)
            mask = ~torch.eye(self.config.num_experts, dtype=torch.bool, device=cos_sim.device)
            diversity_loss = cos_sim[:, mask].mean()
            # We'll add this to the loss in compute_losses by storing it
            self._diversity_loss = diversity_loss
        else:
            self._diversity_loss = None

        return outputs, gate_weights

    def compute_losses(self, outputs, targets, gate_weights=None):
        total = torch.tensor(0.0, device=targets.device)
        per_task = {}
        for task in self.task_names:
            col_idx = self._task_to_col(task)
            loss_per_sample = self.criterion(outputs[task].squeeze(1), targets[:, col_idx])
            w = self.config.task_weights.get(task, 1.0)
            task_loss = (loss_per_sample * w).mean()
            total = total + task_loss
            per_task[task] = task_loss.item()

        # Gate entropy regularization: encourage peaked (low entropy) gates
        if gate_weights is not None and self.config.gate_entropy_coef > 0.0:
            eps = 1e-12
            for task in self.task_names:
                gate_w = gate_weights[task]  # (B, K)
                entropy = -(gate_w * (gate_w + eps).log()).sum(dim=-1).mean()
                total = total + self.config.gate_entropy_coef * entropy

        # Expert diversity regularization
        if self._diversity_loss is not None and self.config.expert_diversity_coef > 0.0:
            total = total + self.config.expert_diversity_coef * self._diversity_loss

        return total, per_task

    @staticmethod
    def _task_to_col(task: str) -> int:
        return {"click": 0, "follow": 1, "like": 2, "share": 3}[task]

    def get_target_col(self, task: str) -> int:
        return self._task_to_col(task)


# ===========================================================================
# 3. DIAGNOSTICS
# ===========================================================================
def compute_expert_diagnostics(model: MTLMMoE, loader, config,
                                max_batches: Optional[int] = None,
                                device: torch.device = torch.device("cpu")) -> Dict:
    """
    Expert analysis diagnostics:
      - Per-expert gradient norms (do they learn?)
      - Per-expert task affinity (which tasks each expert is good at)
      - Per-expert feature utilization (mean |gradient| per input feature)
    """
    model.eval()
    K = config.num_experts
    expert_task_grads = {e: {t: [] for t in model.task_names} for e in range(K)}
    expert_feature_grads = {e: [] for e in range(K)}
    expert_param_norms = {e: [] for e in range(K)}

    # Enable gradients for backward pass (model.eval() keeps BN/Dropout in eval mode)
    for batch_idx, batch in enumerate(loader):
            if max_batches is not None and batch_idx >= max_batches:
                break
            sparse, num_feats, targets = [x.to(device) for x in batch]

            # Forward with gradient tracking for expert analysis
            B = sparse.size(0)
            n_cat = len(model.cat_cols)
            cat_indices = sparse[:, :n_cat]
            hist_indices = sparse[:, n_cat:]
            
            emb_list = []
            for i, col in enumerate(model.cat_cols):
                emb = model.embeddings[col](cat_indices[:, i])
                emb_list.append(emb)
            interest = model.attn_pool(hist_indices, model.embeddings["item_id"])
            combined = torch.cat(emb_list + [num_feats, interest], dim=1)

            # Expert outputs
            expert_outputs = [expert(combined) for expert in model.experts]
            # Retain grad on non-leaf tensors so we can access .grad after backward()
            for eo in expert_outputs:
                eo.retain_grad()
            expert_stack = torch.stack(expert_outputs, dim=1)  # (B, K, out_dim)

            # Per-task: compute loss, backprop through experts, record grad norms
            for task in model.task_names:
                gate_w = model.gates[task](combined)
                consolidated = torch.bmm(gate_w.unsqueeze(1), expert_stack).squeeze(1)
                logits = model.towers[task](consolidated)
                col_idx = model.get_target_col(task)
                loss = model.criterion(logits.squeeze(1), targets[:, col_idx]).mean()

                # Zero grads, then backward to get per-expert gradients
                model.zero_grad()
                loss.backward(retain_graph=True)

                for e_idx, expert in enumerate(model.experts):
                    # Gradient norm of expert's output w.r.t. this task loss
                    expert_out = expert_outputs[e_idx]
                    if expert_out.grad is not None:
                        grad_norm = expert_out.grad.norm(2).item()
                        expert_task_grads[e_idx][task].append(grad_norm)

                    # Feature utilization: gradient of loss w.r.t. combined input
                    # flowing through this expert's first layer
                    if expert.net[0].weight.grad is not None:
                        feat_grad = expert.net[0].weight.grad.abs().mean(dim=1)  # (in_dim,)
                        expert_feature_grads[e_idx].append(feat_grad.cpu())

                    # Parameter norm
                    param_norm = sum(p.grad.norm(2).item() ** 2
                                     for p in expert.parameters() if p.grad is not None) ** 0.5
                    expert_param_norms[e_idx].append(param_norm)

            # Clear gradients between batches
            model.zero_grad()

    # Aggregate diagnostics
    diagnostics = {"expert_task_grads": {}, "expert_feature_grads": {},
                   "expert_param_norms": {}, "num_batches": batch_idx + 1}

    for e_idx in range(K):
        # Task affinity: mean gradient norm per task (higher = expert specializes in that task)
        task_affinity = {}
        for task in model.task_names:
            vals = expert_task_grads[e_idx][task]
            task_affinity[task] = float(np.mean(vals)) if vals else 0.0
        diagnostics["expert_task_grads"][e_idx] = task_affinity

        # Feature utilization: mean |gradient| per input feature across experts
        feat_grads = expert_feature_grads[e_idx]
        if feat_grads:
            stacked = torch.stack(feat_grads, dim=0)  # (n_samples, in_dim)
            diagnostics["expert_feature_grads"][e_idx] = stacked.mean(dim=0).tolist()
        else:
            diagnostics["expert_feature_grads"][e_idx] = [0.0] * model.input_dim

        # Parameter norms
        norms = expert_param_norms[e_idx]
        diagnostics["expert_param_norms"][e_idx] = float(np.mean(norms)) if norms else 0.0

    return diagnostics


def compute_gate_diagnostics(model: MTLMMoE, loader, config,
                              max_batches: Optional[int] = None,
                              device: torch.device = torch.device("cpu")) -> Dict:
    """
    Gate weight analysis: cosine similarity between per-task gate weight vectors.
    - Too high (>0.9): gates are nearly identical → no task differentiation
    - Too low (<0.1): tasks are nearly orthogonal → defeats MTL purpose
    Also computes per-task gate entropy (softmax distribution over experts).
    """
    model.eval()
    gate_vectors = {t: [] for t in model.task_names}

    with torch.no_grad():
        for batch_idx, batch in enumerate(loader):
            if max_batches is not None and batch_idx >= max_batches:
                break
            sparse, num_feats, _ = [
                x.to(device) for x in batch
            ]
            B = sparse.size(0)
            n_cat = len(model.cat_cols)
            cat_indices = sparse[:, :n_cat]
            hist_indices = sparse[:, n_cat:]
            
            emb_list = []
            for i, col in enumerate(model.cat_cols):
                emb = model.embeddings[col](cat_indices[:, i])
                emb_list.append(emb)
            interest = model.attn_pool(hist_indices, model.embeddings["item_id"])
            combined = torch.cat(emb_list + [num_feats, interest], dim=1)

            for task in model.task_names:
                gate_w = model.gates[task](combined)  # (B, K)
                gate_vectors[task].append(gate_w.cpu())

    # Compute mean gate vector per task, then pairwise cosine similarity
    mean_gates = {}
    gate_entropy = {}
    gate_distribution = {}
    for task in model.task_names:
        if gate_vectors[task]:
            stacked = torch.cat(gate_vectors[task], dim=0)  # (N, K)
            mean_gates[task] = stacked.mean(dim=0)           # (K,)
            # Entropy of the mean gate distribution (softmax already applied)
            mean_gate = mean_gates[task]
            eps = 1e-12
            entropy = -(mean_gate * (mean_gate + eps).log()).sum().item()
            gate_entropy[task] = float(entropy)
            gate_distribution[task] = mean_gate.tolist()
        else:
            mean_gates[task] = torch.zeros(config.num_experts)
            gate_entropy[task] = 0.0
            gate_distribution[task] = [0.0] * config.num_experts

    # Pairwise cosine similarity
    task_list = model.task_names
    cos_sim = {}
    for i, t1 in enumerate(task_list):
        cos_sim[t1] = {}
        for j, t2 in enumerate(task_list):
            v1 = mean_gates[t1]
            v2 = mean_gates[t2]
            denom = v1.norm(2) * v2.norm(2)
            sim = (v1 @ v2) / denom if denom > 0 else 0.0
            cos_sim[t1][t2] = float(sim)

    # Flag problematic pairs
    warnings = []
    for i, t1 in enumerate(task_list):
        for j, t2 in enumerate(task_list):
            if i < j:
                sim = cos_sim[t1][t2]
                if sim > 0.9:
                    warnings.append(
                        f"  ⚠️  Gate similarity {t1}↔{t2} = {sim:.3f} (>0.9): "
                        f"gates are nearly identical — tasks may not need separate gates"
                    )
                elif sim < 0.1:
                    warnings.append(
                        f"  ⚠️  Gate similarity {t1}↔{t2} = {sim:.3f} (<0.1): "
                        f"tasks are nearly orthogonal — MTL may not be helping"
                    )

    return {
        "cosine_similarity": cos_sim,
        "mean_gates": {t: v.tolist() for t, v in mean_gates.items()},
        "gate_entropy": gate_entropy,
        "gate_distribution": gate_distribution,
        "warnings": warnings,
        "num_batches": batch_idx + 1
    }


def save_diagnostics(diagnostics: Dict, gate_diag: Dict, run_dir: str, step: int):
    """Save diagnostic results to disk as JSON + summary text + plots + markdown report."""
    os.makedirs(run_dir, exist_ok=True)

    # Save JSON (extended with gate entropy/distribution)
    diag_path = os.path.join(run_dir, f"diagnostics_step{step}.json")
    with open(diag_path, "w") as f:
        json.dump({
            "step": step,
            "expert_task_grads": diagnostics["expert_task_grads"],
            "expert_feature_grads": diagnostics["expert_feature_grads"],
            "expert_param_norms": diagnostics["expert_param_norms"],
            "gate_cosine_similarity": gate_diag["cosine_similarity"],
            "gate_mean_vectors": gate_diag["mean_gates"],
            "gate_entropy": gate_diag["gate_entropy"],
            "gate_distribution": gate_diag["gate_distribution"],
            "gate_warnings": gate_diag["warnings"],
        }, f, indent=2, default=str)
    logger.info("  Diagnostics saved to %s", diag_path)

    # Print summary
    logger.info("  === Expert Task Affinity (gradient norms) ===")
    for e_idx in sorted(diagnostics["expert_task_grads"]):
        affinity = diagnostics["expert_task_grads"][e_idx]
        sorted_tasks = sorted(affinity.items(), key=lambda x: x[1], reverse=True)
        top = sorted_tasks[0] if sorted_tasks else ("none", 0.0)
        logger.info("    Expert %d: strongest task=%s (grad_norm=%.4f) | %s",
                     e_idx, top[0], top[1],
                     ", ".join(f"{t}={v:.4f}" for t, v in sorted_tasks[:3]))

    logger.info("  === Gate Cosine Similarity ===")
    for t1 in gate_diag["cosine_similarity"]:
        row = ", ".join(f"{t2}={gate_diag['cosine_similarity'][t1][t2]:.3f}"
                        for t2 in gate_diag["cosine_similarity"][t1])
        logger.info("    %s: %s", t1, row)

    logger.info("  === Gate Entropy ===")
    for task, ent in gate_diag["gate_entropy"].items():
        logger.info("    %s: %.4f", task, ent)

    for w in gate_diag["warnings"]:
        logger.info(w)

    # Generate plots if matplotlib available
    if MATPLOTLIB_AVAILABLE:
        _save_diagnostic_plots(diagnostics, gate_diag, run_dir, step)

    # Generate markdown report
    _save_diagnostic_report(diagnostics, gate_diag, run_dir, step)


def _save_diagnostic_plots(diagnostics: Dict, gate_diag: Dict, run_dir: str, step: int):
    """Generate and save heatmap plots for diagnostics."""
    try:
        task_names = list(diagnostics["expert_task_grads"][0].keys()) if diagnostics["expert_task_grads"] else []
        num_experts = len(diagnostics["expert_task_grads"])

        if not task_names or num_experts == 0:
            return

        # 1. Expert-Task Affinity Heatmap
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        # Expert-task affinity matrix
        affinity_matrix = np.zeros((num_experts, len(task_names)))
        for e_idx in range(num_experts):
            for t_idx, task in enumerate(task_names):
                affinity_matrix[e_idx, t_idx] = diagnostics["expert_task_grads"][e_idx].get(task, 0.0)

        im1 = axes[0].imshow(affinity_matrix, aspect='auto', cmap='viridis')
        axes[0].set_title(f'Expert-Task Affinity (Grad Norm) - Step {step}')
        axes[0].set_xlabel('Task')
        axes[0].set_ylabel('Expert')
        axes[0].set_xticks(range(len(task_names)))
        axes[0].set_xticklabels(task_names)
        axes[0].set_yticks(range(num_experts))
        axes[0].set_yticklabels([f'Expert {i}' for i in range(num_experts)])
        plt.colorbar(im1, ax=axes[0], label='Mean Gradient Norm')

        # Annotate values
        for e_idx in range(num_experts):
            for t_idx in range(len(task_names)):
                val = affinity_matrix[e_idx, t_idx]
                axes[0].text(t_idx, e_idx, f'{val:.3f}', ha='center', va='center',
                            color='white' if val > affinity_matrix.max() / 2 else 'black', fontsize=8)

        # 2. Gate Cosine Similarity Heatmap
        task_list = list(gate_diag["cosine_similarity"].keys())
        cos_matrix = np.zeros((len(task_list), len(task_list)))
        for i, t1 in enumerate(task_list):
            for j, t2 in enumerate(task_list):
                cos_matrix[i, j] = gate_diag["cosine_similarity"][t1][t2]

        im2 = axes[1].imshow(cos_matrix, aspect='auto', cmap='RdBu_r', vmin=-1, vmax=1)
        axes[1].set_title(f'Gate Cosine Similarity - Step {step}')
        axes[1].set_xlabel('Task')
        axes[1].set_ylabel('Task')
        axes[1].set_xticks(range(len(task_list)))
        axes[1].set_xticklabels(task_list)
        axes[1].set_yticks(range(len(task_list)))
        axes[1].set_yticklabels(task_list)
        plt.colorbar(im2, ax=axes[1], label='Cosine Similarity')

        # Annotate values
        for i in range(len(task_list)):
            for j in range(len(task_list)):
                val = cos_matrix[i, j]
                axes[1].text(j, i, f'{val:.3f}', ha='center', va='center',
                            color='white' if abs(val) > 0.5 else 'black', fontsize=8)

        plt.tight_layout()
        plot_path = os.path.join(run_dir, f"diagnostics_step{step}_heatmaps.png")
        plt.savefig(plot_path, dpi=150, bbox_inches='tight')
        plt.close()
        logger.info("  Heatmaps saved to %s", plot_path)

        # 3. Gate Distribution Bar Plot (per task)
        fig, axes = plt.subplots(len(task_list), 1, figsize=(8, 3 * len(task_list)), squeeze=False)
        for t_idx, task in enumerate(task_list):
            dist = gate_diag["gate_distribution"][task]
            ent = gate_diag["gate_entropy"][task]
            axes[t_idx, 0].bar(range(len(dist)), dist, color='steelblue', alpha=0.7)
            axes[t_idx, 0].set_title(f'Gate Distribution: {task} (Entropy: {ent:.4f})')
            axes[t_idx, 0].set_xlabel('Expert')
            axes[t_idx, 0].set_ylabel('Weight')
            axes[t_idx, 0].set_xticks(range(len(dist)))
            axes[t_idx, 0].set_xticklabels([f'E{i}' for i in range(len(dist))])
            axes[t_idx, 0].set_ylim(0, max(dist) * 1.2 if max(dist) > 0 else 1.0)

        plt.tight_layout()
        dist_plot_path = os.path.join(run_dir, f"diagnostics_step{step}_gate_dist.png")
        plt.savefig(dist_plot_path, dpi=150, bbox_inches='tight')
        plt.close()
        logger.info("  Gate distribution plot saved to %s", dist_plot_path)

    except Exception as e:
        logger.warning("  Failed to generate plots: %s", e)


def _save_diagnostic_report(diagnostics: Dict, gate_diag: Dict, run_dir: str, step: int):
    """Generate a markdown summary report for diagnostics."""
    try:
        report_path = os.path.join(run_dir, f"diagnostics_step{step}_report.md")
        task_names = list(diagnostics["expert_task_grads"][0].keys()) if diagnostics["expert_task_grads"] else []
        num_experts = len(diagnostics["expert_task_grads"])

        with open(report_path, "w") as f:
            f.write(f"# MMoE Diagnostics Report - Step {step}\n\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Expert Task Affinity
            f.write("## Expert-Task Affinity (Mean Gradient Norm)\n\n")
            f.write("| Expert | " + " | ".join(task_names) + " |\n")
            f.write("|" + "---|" * (len(task_names) + 1) + "\n")
            for e_idx in range(num_experts):
                row = f"| Expert {e_idx} |"
                for task in task_names:
                    val = diagnostics["expert_task_grads"][e_idx].get(task, 0.0)
                    row += f" {val:.4f} |"
                f.write(row + "\n")
            f.write("\n")

            # Expert Feature Utilization (top 5 features per expert)
            f.write("## Expert Feature Utilization (Top 5 Features by Mean |Grad|)\n\n")
            for e_idx in range(num_experts):
                feat_grads = diagnostics["expert_feature_grads"][e_idx]
                if feat_grads:
                    top5_idx = np.argsort(feat_grads)[-5:][::-1]
                    f.write(f"**Expert {e_idx}**: ")
                    f.write(", ".join(f"feat[{i}]={feat_grads[i]:.4f}" for i in top5_idx))
                    f.write("\n\n")

            # Expert Parameter Norms
            f.write("## Expert Parameter Gradient Norms\n\n")
            for e_idx in range(num_experts):
                norm = diagnostics["expert_param_norms"][e_idx]
                f.write(f"- Expert {e_idx}: {norm:.4f}\n")
            f.write("\n")

            # Gate Cosine Similarity
            f.write("## Gate Cosine Similarity Matrix\n\n")
            task_list = list(gate_diag["cosine_similarity"].keys())
            f.write("| Task | " + " | ".join(task_list) + " |\n")
            f.write("|" + "---|" * (len(task_list) + 1) + "\n")
            for t1 in task_list:
                row = f"| {t1} |"
                for t2 in task_list:
                    row += f" {gate_diag['cosine_similarity'][t1][t2]:.3f} |"
                f.write(row + "\n")
            f.write("\n")

            # Gate Entropy & Distribution
            f.write("## Gate Entropy & Distribution\n\n")
            f.write("| Task | Entropy | Distribution (per expert) |\n")
            f.write("|---|---|---|\n")
            for task in task_list:
                ent = gate_diag["gate_entropy"][task]
                dist = gate_diag["gate_distribution"][task]
                dist_str = ", ".join(f"E{i}={v:.3f}" for i, v in enumerate(dist))
                f.write(f"| {task} | {ent:.4f} | {dist_str} |\n")
            f.write("\n")

            # Warnings
            if gate_diag["warnings"]:
                f.write("## Warnings\n\n")
                for w in gate_diag["warnings"]:
                    # Replace unicode chars for Windows compatibility
                    w_safe = w.replace("⚠️", "[WARNING]").replace("→", "->").replace("↔", "<->")
                    f.write(f"- {w_safe}\n")
                f.write("\n")

            f.write("---\n")
            f.write(f"*Report generated at step {step} with {gate_diag['num_batches']} batches.*\n")

        logger.info("  Markdown report saved to %s", report_path)

    except Exception as e:
        logger.warning("  Failed to generate markdown report: %s", e)


# ===========================================================================
# 4. EVALUATION
# ===========================================================================
def compute_aucs(outputs, targets, task_names):
    aucs = {}
    for task in task_names:
        col_idx = {"click": 0, "follow": 1, "like": 2, "share": 3}[task]
        y_true = targets[:, col_idx].cpu().numpy()
        y_score = torch.sigmoid(outputs[task].squeeze(1)).detach().cpu().numpy()
        # Guard against NaN/Inf predictions from numerical instability —
        # treat that task's AUC as invalid rather than crashing.
        if len(np.unique(y_true)) < 2:
            aucs[task] = float("nan")
        elif not np.isfinite(y_score).all():
            logger.warning("  AUC %s: non-finite predictions (%d/%d NaN/Inf) → nan",
                           task, int(np.logical_not(np.isfinite(y_score)).sum()), len(y_score))
            aucs[task] = float("nan")
        else:
            aucs[task] = roc_auc_score(y_true, y_score)
    return aucs


@torch.no_grad()
def evaluate(model, loader, config, desc: str = "Evaluating",
             max_batches: Optional[int] = None):
    model.eval()
    total_loss = 0.0
    task_losses = {t: 0.0 for t in model.task_names}
    n_batches = 0

    all_outputs = {t: [] for t in model.task_names}
    all_targets = []

    for batch in tqdm(loader, desc=desc, unit="batch",
                      disable=not TQDM_AVAILABLE):
        sparse, num_feats, targets = [
            x.to(config.device) for x in batch
        ]
        with autocast(enabled=config.use_amp):
            outputs, gate_weights = model(sparse, num_feats)
            loss, per_task = model.compute_losses(outputs, targets, gate_weights)

        total_loss += loss.item()
        for t in model.task_names:
            task_losses[t] += per_task[t]
            all_outputs[t].append(outputs[t].cpu())
        all_targets.append(targets.cpu())
        n_batches += 1

        if max_batches is not None and n_batches >= max_batches:
            break

    avg_loss = total_loss / n_batches
    avg_task_losses = {t: v / n_batches for t, v in task_losses.items()}
    cat_outputs = {t: torch.cat(all_outputs[t]) for t in model.task_names}
    cat_targets = torch.cat(all_targets)
    aucs = compute_aucs(cat_outputs, cat_targets, model.task_names)
    return avg_loss, avg_task_losses, aucs


# ===========================================================================
# 5. TRAINING LOOP
# ===========================================================================
def train_step_based(model, loader_train, loader_val, loader_test,
                     optimizer_dense, optimizer_sparse, scheduler, scaler,
                     config, start_step: int = 0):
    best_val_auc = 0.0
    best_step = 0
    steps_no_improve = 0
    global_step = start_step

    running_loss = 0.0
    running_task_losses = {t: 0.0 for t in model.task_names}
    running_steps = 0

    model.train()
    optimizer_dense.zero_grad()
    optimizer_sparse.zero_grad()

    data_iter = iter(loader_train)

    pbar = tqdm(
        total=config.train_steps - start_step,
        initial=start_step,
        desc="Training",
        unit="step",
        disable=not TQDM_AVAILABLE,
        bar_format="{l_bar}{bar:30}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}] {postfix}",
    )

    start_time = time.time()

    while global_step < config.train_steps:
        try:
            batch = next(data_iter)
        except StopIteration:
            data_iter = iter(loader_train)
            batch = next(data_iter)

        sparse, num_feats, targets = [
            x.to(config.device) for x in batch
        ]

        with autocast(enabled=config.use_amp):
            outputs, gate_weights = model(sparse, num_feats)
            loss, per_task = model.compute_losses(outputs, targets, gate_weights)
            loss = loss / config.accum_steps

        if config.use_amp:
            scaler.scale(loss).backward()
        else:
            loss.backward()

        running_loss += loss.item() * config.accum_steps
        for t in model.task_names:
            running_task_losses[t] += per_task[t]
        running_steps += 1

        if (global_step + 1) % config.accum_steps == 0:
            if config.use_amp:
                scaler.unscale_(optimizer_dense)
            # Sparse-safe clipping (sparse embeddings produce sparse grads)
            clip_grad_norm_sparse(model.parameters(), config.grad_clip_norm)
            if config.use_amp:
                scaler.step(optimizer_dense)
                scaler.update()
            else:
                optimizer_dense.step()
            optimizer_sparse.step()
            optimizer_dense.zero_grad()
            optimizer_sparse.zero_grad()

        global_step += 1
        pbar.update(1)

        if global_step % config.log_every == 0:
            avg_loss = running_loss / max(running_steps, 1)
            avg_task = {t: v / max(running_steps, 1) for t, v in running_task_losses.items()}
            postfix = {"loss": f"{avg_loss:.4f}"}
            for t in model.task_names:
                postfix[t] = f"{avg_task.get(t, 0):.4f}"
            pbar.set_postfix(postfix)

            task_str = "  ".join(f"{t}={avg_task[t]:.4f}" for t in model.task_names)
            logger.info("Step %6d | loss: %.4f | %s", global_step, avg_loss, task_str)

            running_loss = 0.0
            running_task_losses = {t: 0.0 for t in model.task_names}
            running_steps = 0

        # Validate
        if global_step % config.val_every == 0:
            pbar.set_description("Validating...")

            val_loss, val_task_losses, val_aucs = evaluate(
                model, loader_val, config, desc="  Validation",
                max_batches=config.max_val_batches,
            )
            mean_auc = np.nanmean(list(val_aucs.values()))
            auc_str = "  ".join(f"{t}={val_aucs[t]:.4f}" for t in model.task_names)
            logger.info("=== Step %6d | val_loss: %.4f | mean_auc: %.4f | %s",
                        global_step, val_loss, mean_auc, auc_str)

            scheduler.step(val_loss)

            if mean_auc > best_val_auc:
                best_val_auc = mean_auc
                best_step = global_step
                steps_no_improve = 0
                ckpt_path = os.path.join(config.model_dir, "best_model.pt")
                torch.save({
                    "step": global_step,
                    "model_state_dict": model.state_dict(),
                    "optimizer_sparse_state_dict": optimizer_sparse.state_dict(),
                    "optimizer_dense_state_dict": optimizer_dense.state_dict(),
                    "scheduler_state_dict": scheduler.state_dict(),
                    "scaler_state_dict": scaler.state_dict() if config.use_amp else None,
                    "best_auc": best_val_auc,
                    "config": config,
                }, ckpt_path)
                logger.info("  → New best model saved (AUC=%.4f) at %s", best_val_auc, ckpt_path)
            else:
                steps_no_improve += 1
                logger.info("  → No improvement (%d/%d checks)",
                            steps_no_improve, config.early_stop_patience)

            # Run diagnostics periodically
            if global_step % config.diag_every == 0:
                logger.info("  --- Running diagnostics ---")
                diag = compute_expert_diagnostics(
                    model, loader_val, config,
                    max_batches=config.diag_max_batches, device=config.device,
                )
                gate_diag = compute_gate_diagnostics(
                    model, loader_val, config,
                    max_batches=config.diag_max_batches, device=config.device,
                )
                save_diagnostics(diag, gate_diag, config.runs_dir, global_step)

            if steps_no_improve >= config.early_stop_patience:
                logger.info("Early stopping at step %d (no improvement for %d checks)",
                            global_step, config.early_stop_patience)
                break

            model.train()
            pbar.set_description("Training")

    pbar.close()

    # Final test evaluation
    logger.info("=" * 60)
    logger.info("FINAL: Test evaluation")
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

    # Final diagnostics on test set
    logger.info("  --- Final diagnostics (test set) ---")
    diag = compute_expert_diagnostics(
        model, loader_test, config,
        max_batches=config.diag_max_batches, device=config.device,
    )
    gate_diag = compute_gate_diagnostics(
        model, loader_test, config,
        max_batches=config.diag_max_batches, device=config.device,
    )
    save_diagnostics(diag, gate_diag, config.runs_dir, global_step)


# ===========================================================================
# 6. MAIN
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(description="MMoE Multi-Task Recommender")
    parser.add_argument("--data-dir", type=str, default="./data/split")
    parser.add_argument("--cache-dir", type=str, default="./cache")
    parser.add_argument("--model-dir", type=str, default="./checkpoints")
    parser.add_argument("--runs-dir", type=str, default="./runs")
    parser.add_argument("--chunk-size", type=int, default=200_000)
    parser.add_argument("--batch-size", type=int, default=2048)
    parser.add_argument("--accum-steps", type=int, default=2)
    parser.add_argument("--train-steps", type=int, default=50_000)
    parser.add_argument("--val-every", type=int, default=500)
    parser.add_argument("--log-every", type=int, default=200)
    parser.add_argument("--max-val-batches", type=int, default=None)
    parser.add_argument("--diag-every", type=int, default=500)
    parser.add_argument("--diag-max-batches", type=int, default=400)
    parser.add_argument("--num-experts", type=int, default=4)
    parser.add_argument("--no-sparse-embeddings", action="store_true",
                        help="Use dense embeddings (allows AMP; sparse=True needs SparseAdam)")
    parser.add_argument("--num-tasks", type=int, default=4, choices=[1, 2, 3, 4])
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=1e-5)
    parser.add_argument("--grad-clip-norm", type=float, default=5.0)
    parser.add_argument("--early-stop-patience", type=int, default=10)
    parser.add_argument("--no-amp", action="store_true")
    parser.add_argument("--force-preprocess", action="store_true",
                        help="Force rebuild cache from parquet")
    parser.add_argument("--resume", type=str, default=None,
                        help="Resume from checkpoint .pt file")
    parser.add_argument("--device", type=str, choices=["auto", "cuda", "directml", "cpu"],
                        default="auto", help="Device to use: auto (cuda>directml>cpu), cuda, directml, cpu")
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument("--preprocess-workers", type=int, default=0,
                        help="Parallel workers for chunk encoding during preprocessing")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Resolve device
    device = resolve_device(args.device)
    logger.info("Using device: %s", device)

    # Set seed
    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)
    if is_cuda(device):
        torch.cuda.manual_seed_all(args.seed)

    os.makedirs(args.model_dir, exist_ok=True)
    os.makedirs(args.runs_dir, exist_ok=True)

    # Run name based on timestamp
    run_name = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(args.runs_dir, run_name)
    os.makedirs(run_dir, exist_ok=True)

    # Save config
    with open(os.path.join(run_dir, "config.json"), "w") as f:
        json.dump({k: v for k, v in vars(args).items() if not k.startswith("_")}, f, indent=2, default=str)
    logger.info("Run directory: %s", run_dir)

    # ------------------------------------------------------------------
    # 0. Build config (must precede preprocessing — used for column lists)
    # ------------------------------------------------------------------
    # Use module-level column lists as the single source of truth
    cat_cols = CAT_COLS
    num_cols = NUM_COLS
    hist_cols = HIST_COLS

    # Sparse embeddings (sparse=True) are incompatible with AMP (autocast +
    # GradScaler) — they produce sparse gradients, which AMP cannot handle.
    # Disable AMP whenever sparse embeddings are enabled.
    use_sparse_emb = not args.no_sparse_embeddings

    config = MMoEConfig(
        data_dir=args.data_dir,
        cache_dir=args.cache_dir,
        chunk_size=args.chunk_size,
        batch_size=args.batch_size,
        accum_steps=args.accum_steps,
        train_steps=args.train_steps,
        val_every=args.val_every,
        log_every=args.log_every,
        max_val_batches=args.max_val_batches,
        learning_rate=args.lr,
        weight_decay=args.weight_decay,
        grad_clip_norm=args.grad_clip_norm,
        early_stop_patience=args.early_stop_patience,
        use_amp=not args.no_amp and supports_amp(device) and not use_sparse_emb,
        num_tasks=args.num_tasks,
        num_experts=args.num_experts,
        sparse_embeddings=use_sparse_emb,
        model_dir=args.model_dir,
        runs_dir=run_dir,
        device=device,
        diag_every=args.diag_every,
        diag_max_batches=args.diag_max_batches,
        cat_cols=cat_cols,
        num_cols=num_cols,
        hist_cols=hist_cols,
    )
    logger.info("Features — cat: %s | num: %s | hist: %d",
                config.cat_cols, config.num_cols, len(config.hist_cols))

    # ------------------------------------------------------------------
    # 1. Preprocess & cache
    # ------------------------------------------------------------------
    train_cache = os.path.join(args.cache_dir, "train")
    val_cache = os.path.join(args.cache_dir, "val")
    test_cache = os.path.join(args.cache_dir, "test")

    cache_exists = os.path.exists(f"{train_cache}_meta.pkl")
    cache_valid = False

    if cache_exists and not args.force_preprocess:
        logger.info("Verifying cache integrity …")
        train_ok = verify_cache_integrity(train_cache, cat_cols=config.cat_cols, num_cols=config.num_cols, hist_cols=config.hist_cols)
        val_ok = verify_cache_integrity(val_cache, cat_cols=config.cat_cols, num_cols=config.num_cols, hist_cols=config.hist_cols)
        test_ok = verify_cache_integrity(test_cache, cat_cols=config.cat_cols, num_cols=config.num_cols, hist_cols=config.hist_cols)
        if train_ok and val_ok and test_ok:
            cache_valid = True
            logger.info("✅ All caches valid — skipping preprocessing")
        else:
            logger.warning("⚠️  Cache integrity check failed — will reprocess")

    if not cache_valid or args.force_preprocess:
        logger.info("Preprocessing data → cache …")
        encoders = _fit_encoders_on_train(
            os.path.join(args.data_dir, "train"), args.chunk_size,
        )
        mappings = _build_mappings(encoders)

        for split_name, split_dir, split_cache in [
            ("Train", "train", train_cache),
            ("Val", "val", val_cache),
            ("Test", "test", test_cache),
        ]:
            data_path = os.path.join(args.data_dir, split_dir)
            if os.path.exists(data_path) and any(glob.glob(os.path.join(data_path, "*.parquet"))):
                preprocess_and_cache(data_path, split_cache, mappings, args.chunk_size,
                                       num_workers=args.preprocess_workers,
                                       cat_cols=config.cat_cols,
                                       num_cols=config.num_cols,
                                       hist_cols=config.hist_cols)
            else:
                logger.warning("  ⚠️  No data found for %s at %s — skipping", split_name, data_path)

        with open(os.path.join(args.model_dir, "encoders.pkl"), "wb") as f:
            pickle.dump(encoders, f)
        logger.info("  ✓ Encoders saved")
    else:
        with open(os.path.join(args.model_dir, "encoders.pkl"), "rb") as f:
            encoders = pickle.load(f)

    # ------------------------------------------------------------------
    # 2. Build datasets & loaders
    # ------------------------------------------------------------------
    ds_train = MMoEDataset(train_cache)
    ds_val = MMoEDataset(val_cache)
    ds_test = MMoEDataset(test_cache)
    logger.info("Train: %d  |  Val: %d  |  Test: %d", len(ds_train), len(ds_val), len(ds_test))

    dl_workers = args.num_workers if supports_workers(device) else 0
    if args.num_workers > 0 and not supports_workers(device):
        logger.warning("DataLoader workers disabled on this device — using num_workers=0")

    pin_mem = supports_pin_memory(device)

    loader_train = DataLoader(
        ds_train,
        batch_sampler=ContiguousBatchSampler(len(ds_train), args.batch_size, shuffle=True),
        num_workers=dl_workers, pin_memory=pin_mem,
        persistent_workers=dl_workers > 0,
        collate_fn=_noop_collate,
    )
    loader_val = DataLoader(
        ds_val,
        batch_sampler=ContiguousBatchSampler(len(ds_val), args.batch_size * 2, shuffle=False),
        num_workers=dl_workers, pin_memory=pin_mem,
        persistent_workers=dl_workers > 0,
        collate_fn=_noop_collate,
    )
    loader_test = DataLoader(
        ds_test,
        batch_sampler=ContiguousBatchSampler(len(ds_test), args.batch_size * 2, shuffle=False),
        num_workers=dl_workers, pin_memory=pin_mem,
        persistent_workers=dl_workers > 0,
        collate_fn=_noop_collate,
    )

    # ------------------------------------------------------------------
    # 3. Vocab sizes (dynamic from cat_cols)
    # ------------------------------------------------------------------
    vocab_sizes = {}
    for col in config.cat_cols:
        vocab_sizes[col] = len(encoders[col].classes_)
    logger.info("Vocab — %s",
                ", ".join(f"{col}:{vocab_sizes[col]}" for col in config.cat_cols))

    # ------------------------------------------------------------------
    # 4. Task list
    # ------------------------------------------------------------------
    # Order: 1=click, 2=+like, 3=+follow, 4=+share
    TASK_ORDER = ["click", "like", "follow", "share"]
    selected_tasks = TASK_ORDER[:args.num_tasks]

    # Detect dense_dim from cache meta
    train_meta_path = os.path.join(args.cache_dir, "train_meta.pkl")
    if os.path.exists(train_meta_path):
        with open(train_meta_path, "rb") as f:
            train_meta = pickle.load(f)
        config.dense_dim = train_meta.get("n_dense", len(config.num_cols))

    eff_batch = config.batch_size * config.accum_steps
    logger.info("Effective batch size: %d × %d = %d", config.batch_size, config.accum_steps, eff_batch)
    logger.info("Tasks: %s  |  Experts: %d", selected_tasks, config.num_experts)

    model = MTLMMoE(config, vocab_sizes, task_names=selected_tasks)
    model = model.to(config.device)

    total_p = sum(p.numel() for p in model.parameters())
    trainable_p = sum(p.numel() for p in model.parameters() if p.requires_grad)
    logger.info("Params — total: %d  |  trainable: %d", total_p, trainable_p)

    # ------------------------------------------------------------------
    # 5. Optimizers — dual setup for sparse embeddings
    # ------------------------------------------------------------------
    # AdamW does NOT support sparse gradients (RuntimeError). Split params:
    #   - SparseAdam  → the giant sparse embedding table
    #   - AdamW       → the dense NN layers (experts, gates, towers, attn_pool)
    emb_params = list(model.embeddings.parameters())
    dense_params = [p for n, p in model.named_parameters() if not n.startswith("embeddings.")]

    optimizer_sparse = optim.SparseAdam(emb_params, lr=config.learning_rate)
    optimizer_dense = optim.AdamW(dense_params, lr=config.learning_rate, weight_decay=config.weight_decay)

    # Scheduler drives the dense optimizer (sparse embeddings have no LR schedule)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(
        optimizer_dense, mode="min", factor=config.lr_factor,
        patience=config.lr_patience,
    )
    scaler = GradScaler(enabled=config.use_amp)

    # ------------------------------------------------------------------
    # 6. Resume from checkpoint if requested
    # ------------------------------------------------------------------
    start_step = 0
    if args.resume:
        if os.path.exists(args.resume):
            ckpt = torch.load(args.resume, map_location=config.device, weights_only=False)
            model.load_state_dict(ckpt["model_state_dict"])
            if ckpt.get("optimizer_sparse_state_dict"):
                optimizer_sparse.load_state_dict(ckpt["optimizer_sparse_state_dict"])
            if ckpt.get("optimizer_dense_state_dict"):
                optimizer_dense.load_state_dict(ckpt["optimizer_dense_state_dict"])
            if config.use_amp and ckpt.get("scaler_state_dict"):
                scaler.load_state_dict(ckpt["scaler_state_dict"])
            start_step = ckpt.get("step", 0)
            logger.info("Resumed from step %d (best_auc=%.4f)", start_step, ckpt.get("best_auc", 0.0))
        else:
            logger.warning("Checkpoint not found at %s — starting from scratch", args.resume)

    # ------------------------------------------------------------------
    # 7. Train
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STARTING TRAINING")
    logger.info("=" * 60)

    train_step_based(
        model, loader_train, loader_val, loader_test,
        optimizer_dense, optimizer_sparse, scheduler, scaler, config,
        start_step=start_step,
    )

    logger.info("Training complete. Run directory: %s", run_dir)


if __name__ == "__main__":
    main()