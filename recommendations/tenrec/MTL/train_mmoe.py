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

Usage:
  python train_mmoe.py                          # train with defaults
  python train_mmoe.py --num-tasks 2            # click + like only
  python train_mmoe.py --num-experts 8          # 8 experts instead of 4
  python train_mmoe.py --force-preprocess       # rebuild cache from parquet
  python train_mmoe.py --resume checkpoints/best_model.pt
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
    category_emb_dim: int = 16
    gender_emb_dim: int = 8
    # Attention history pooling
    attn_heads: int = 4
    attn_proj_dim: int = 64
    attn_dropout: float = 0.1

    # Dense features (detected dynamically from data)
    dense_dim: int = 2  # age_norm + watch_norm (if available)

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
    gate_temperature: float = 0.5
    gate_entropy_coef: float = 0.01
    expert_diversity_coef: float = 0.0

    task_weights: Dict[str, float] = field(
        default_factory=lambda: {"click": 1.0, "follow": 1.0, "like": 1.0, "share": 1.0}
    )
    num_tasks: int = 4  # 1=click, 2=+like, 3=+follow, 4=+share

    model_dir: str = "./checkpoints"
    runs_dir: str = "./runs"
    num_workers: int = 4
    pin_memory: bool = True
    device: str = "cuda" if torch.cuda.is_available() else "cpu"

    # ---- Diagnostics ----
    diag_every: int = 500          # run expert/gate diagnostics every N val checks
    diag_max_batches: int = 200    # cap batches for diagnostics (speed)


# ===========================================================================
# 0. CACHE / PREPROCESSING (independent of shared-bottom)
# ===========================================================================
HIST_COLS = [f"hist_{i}" for i in range(1, 11)]
TARGET_COLS = ["click", "follow", "like", "share"]
CAT_COLS = ["user_id", "item_id", "video_category", "gender"]
NUM_COLS = ["age_norm", "watch_norm"]


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
            # Normalize dense features
            df["age_norm"] = (df["age"].clip(0, 100) / 100.0).astype(np.float32)
            if "watching_times" in df.columns:
                df["watch_norm"] = (
                    np.log1p(df["watching_times"].clip(lower=0)) / 10.0
                ).astype(np.float32)
            for start in range(0, len(df), chunk_size):
                yield df.iloc[start : start + chunk_size].reset_index(drop=True)


def _detect_dense_cols(folder: str) -> List[str]:
    """Peek at first row group to detect which dense columns exist."""
    import pyarrow.parquet as pq
    first_file = sorted(glob.glob(os.path.join(folder, "*.parquet")))[0]
    first_rg = pq.ParquetFile(first_file).read_row_group(0).to_pandas()
    available = []
    if "age" in first_rg.columns:
        available.append("age_norm")
    if "watching_times" in first_rg.columns:
        available.append("watch_norm")
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
):
    """Chunked parquet → memmap cache (independent of shared-bottom)."""
    logger.info("Preprocessing & caching: %s → %s", folder, cache_path)
    total_rows = _count_parquet_rows(folder)
    logger.info("  Total rows: %d", total_rows)

    available_dense = _detect_dense_cols(folder)
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
        del chunk

    sparse_feat.flush()
    dense_feat.flush()
    targets.flush()

    meta = {"total_rows": total_rows, "n_dense": n_dense, "dense_cols": available_dense}
    with open(f"{cache_path}_meta.pkl", "wb") as f:
        pickle.dump(meta, f)
    logger.info("  ✓ Cached %d rows to %s_*.npy", total_rows, cache_path)


def verify_cache_integrity(cache_path: str) -> bool:
    """Verify memmap cache is complete and valid."""
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
    except Exception as e:
        logger.warning("  ❌ Cache corrupt: %s", e)
        return False
    logger.info("  ✅ Cache valid: %d rows", cached_rows)
    return True


# ===========================================================================
# 1. DATASET
# ===========================================================================
class MMoEDataset(Dataset):
    """Reads from preprocessed memmap files — virtually zero RAM overhead."""

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
        row = self.sparse[idx]
        dense = self.dense[idx]
        tgt = self.targets[idx]
        return (
            torch.as_tensor(row[0], dtype=torch.long),
            torch.as_tensor(row[1], dtype=torch.long),
            torch.as_tensor(row[2], dtype=torch.long),
            torch.as_tensor(row[3], dtype=torch.long),
            torch.as_tensor(row[4:14], dtype=torch.long),
            torch.as_tensor(dense, dtype=torch.float32),
            torch.as_tensor(tgt, dtype=torch.float32),
        )

    def __getitems__(self, indices):
        rows = self.sparse[indices]
        dense = self.dense[indices]
        tgt = self.targets[indices]
        return (
            torch.as_tensor(rows[:, 0], dtype=torch.long),
            torch.as_tensor(rows[:, 1], dtype=torch.long),
            torch.as_tensor(rows[:, 2], dtype=torch.long),
            torch.as_tensor(rows[:, 3], dtype=torch.long),
            torch.as_tensor(rows[:, 4:14], dtype=torch.long),
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
            # Build 2D padding mask: (H, H)
            padding = (hist_indices == 0)                # (B, H)
            any_padding = padding.any(dim=0)             # (H,)
            attn_mask = any_padding.unsqueeze(1) | any_padding.unsqueeze(0)  # (H, H)
            attn_mask = attn_mask.float().masked_fill(attn_mask, float('-inf'))
            attn_out, _ = self.attn(projected, projected, projected,
                                    attn_mask=attn_mask)  # (B, H, proj_dim)
            pooled = (attn_out * mask).sum(dim=1) / lengths
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

    def __init__(self, config: MMoEConfig, num_users: int, num_items: int,
                 num_categories: int, num_genders: int,
                 task_names: Optional[List[str]] = None):
        super().__init__()
        self.config = config

        # Embeddings
        self.user_emb = nn.Embedding(num_users + 1, config.user_emb_dim, padding_idx=0)
        self.item_emb = nn.Embedding(num_items + 1, config.item_emb_dim, padding_idx=0)
        self.category_emb = nn.Embedding(num_categories + 1, config.category_emb_dim, padding_idx=0)
        self.gender_emb = nn.Embedding(num_genders + 1, config.gender_emb_dim, padding_idx=0)

        # History pooling (mean-pool by default; attention via use_attention flag)
        self.attn_pool = HistoryPool(
            item_emb_dim=config.item_emb_dim,
            proj_dim=config.attn_proj_dim,
            num_heads=config.attn_heads,
            dropout=config.attn_dropout,
            use_attention=False,  # set True to enable attention pooling
        )

        # Input dim = user_emb + item_emb + category_emb + gender_emb + dense + interest_vec
        self.input_dim = (
            config.user_emb_dim + config.item_emb_dim + config.category_emb_dim
            + config.gender_emb_dim + config.dense_dim + config.attn_proj_dim
        )

        # K expert towers (all share the same input → output dim)
        expert_out_dim = config.expert_hidden[-1] if config.expert_hidden else self.input_dim
        self.experts = nn.ModuleList([
            ExpertTower(
                self.input_dim, config.expert_hidden, expert_out_dim, config.expert_dropout
            )
            for _ in range(config.num_experts)
        ])

        # Per-task gate networks
        self.task_names = task_names or ["click", "follow", "like", "share"]
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

    def forward(self, users, items, categories, genders, history, numerical):
        # Embeddings
        u = self.user_emb(users)                                        # (B, user_emb_dim)
        i = self.item_emb(items)                                        # (B, item_emb_dim)
        c = self.category_emb(categories)                               # (B, cat_emb_dim)
        g = self.gender_emb(genders)                                    # (B, gender_emb_dim)
        interest = self.attn_pool(history, self.item_emb)               # (B, attn_proj_dim)

        # Concatenate all features
        combined = torch.cat([u, i, c, g, numerical, interest], dim=1)  # (B, input_dim)

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
                                device: str = "cpu") -> Dict:
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
            users, items, cats, genders, hist, num_feats, targets = [
                x.to(device) for x in batch
            ]

            # Forward with gradient tracking for expert analysis
            u = model.user_emb(users)
            i = model.item_emb(items)
            c = model.category_emb(cats)
            g = model.gender_emb(genders)
            interest = model.attn_pool(hist, model.item_emb)
            combined = torch.cat([u, i, c, g, num_feats, interest], dim=1)

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
                              device: str = "cpu") -> Dict:
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
            users, items, cats, genders, hist, num_feats, _ = [
                x.to(device) for x in batch
            ]
            u = model.user_emb(users)
            i = model.item_emb(items)
            c = model.category_emb(cats)
            g = model.gender_emb(genders)
            interest = model.attn_pool(hist, model.item_emb)
            combined = torch.cat([u, i, c, g, num_feats, interest], dim=1)

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
        aucs[task] = roc_auc_score(y_true, y_score) if len(np.unique(y_true)) >= 2 else float("nan")
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
        users, items, cats, genders, hist, num_feats, targets = [
            x.to(config.device) for x in batch
        ]
        with autocast(enabled=config.use_amp):
            outputs, gate_weights = model(users, items, cats, genders, hist, num_feats)
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
                     optimizer, scheduler, scaler, config, start_step: int = 0):
    best_val_auc = 0.0
    best_step = 0
    steps_no_improve = 0
    global_step = start_step

    running_loss = 0.0
    running_task_losses = {t: 0.0 for t in model.task_names}
    running_steps = 0

    model.train()
    optimizer.zero_grad()

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

        users, items, cats, genders, hist, num_feats, targets = [
            x.to(config.device) for x in batch
        ]

        with autocast(enabled=config.use_amp):
            outputs, gate_weights = model(users, items, cats, genders, hist, num_feats)
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
                    "optimizer_state_dict": optimizer.state_dict(),
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
    parser.add_argument("--accum-steps", type=int, default=1)
    parser.add_argument("--train-steps", type=int, default=50_000)
    parser.add_argument("--val-every", type=int, default=500)
    parser.add_argument("--log-every", type=int, default=100)
    parser.add_argument("--max-val-batches", type=int, default=None)
    parser.add_argument("--diag-every", type=int, default=500)
    parser.add_argument("--diag-max-batches", type=int, default=200)
    parser.add_argument("--num-experts", type=int, default=4)
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
    parser.add_argument("--device", type=str,
                        default="cuda" if torch.cuda.is_available() else "cpu")
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument("--preprocess-workers", type=int, default=0,
                        help="Parallel workers for chunk encoding during preprocessing")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--verbose", action="store_true")

    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Set seed
    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)
    if torch.cuda.is_available():
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
    # 1. Preprocess & cache
    # ------------------------------------------------------------------
    train_cache = os.path.join(args.cache_dir, "train")
    val_cache = os.path.join(args.cache_dir, "val")
    test_cache = os.path.join(args.cache_dir, "test")

    cache_exists = os.path.exists(f"{train_cache}_meta.pkl")
    cache_valid = False

    if cache_exists and not args.force_preprocess:
        logger.info("Verifying cache integrity …")
        train_ok = verify_cache_integrity(train_cache)
        val_ok = verify_cache_integrity(val_cache)
        test_ok = verify_cache_integrity(test_cache)
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
                                     num_workers=args.preprocess_workers)
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

    dl_workers = args.num_workers if args.device == "cuda" else 0
    if args.num_workers > 0 and args.device != "cuda":
        logger.warning("DataLoader workers disabled on CPU — using num_workers=0")

    pin_mem = (args.device == "cuda")

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
    # 3. Vocab sizes
    # ------------------------------------------------------------------
    num_users = len(encoders["user_id"].classes_)
    num_items = len(encoders["item_id"].classes_)
    num_categories = len(encoders["video_category"].classes_)
    num_genders = len(encoders["gender"].classes_)
    logger.info("Vocab — users:%d  items:%d  categories:%d  genders:%d",
                num_users, num_items, num_categories, num_genders)

    # ------------------------------------------------------------------
    # 4. Task list
    # ------------------------------------------------------------------
    TASK_ORDER = ["click", "follow", "like", "share"]
    selected_tasks = TASK_ORDER[:args.num_tasks]

    # Detect dense_dim from cache meta
    train_meta_path = os.path.join(args.cache_dir, "train_meta.pkl")
    dense_dim = 2
    if os.path.exists(train_meta_path):
        with open(train_meta_path, "rb") as f:
            train_meta = pickle.load(f)
        dense_dim = train_meta.get("n_dense", 2)

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
        use_amp=not args.no_amp and args.device == "cuda",
        num_tasks=args.num_tasks,
        num_experts=args.num_experts,
        dense_dim=dense_dim,
        model_dir=args.model_dir,
        runs_dir=run_dir,
        device=args.device,
        diag_every=args.diag_every,
        diag_max_batches=args.diag_max_batches,
    )

    eff_batch = config.batch_size * config.accum_steps
    logger.info("Effective batch size: %d × %d = %d", config.batch_size, config.accum_steps, eff_batch)
    logger.info("Tasks: %s  |  Experts: %d", selected_tasks, config.num_experts)

    model = MTLMMoE(config, num_users, num_items, num_categories, num_genders,
                    task_names=selected_tasks)
    model = model.to(config.device)

    total_p = sum(p.numel() for p in model.parameters())
    trainable_p = sum(p.numel() for p in model.parameters() if p.requires_grad)
    logger.info("Params — total: %d  |  trainable: %d", total_p, trainable_p)

    optimizer = optim.AdamW(model.parameters(), lr=config.learning_rate, weight_decay=config.weight_decay)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", factor=config.lr_factor,
        patience=config.lr_patience,
    )
    scaler = GradScaler(enabled=config.use_amp)

    # ------------------------------------------------------------------
    # 5. Resume from checkpoint if requested
    # ------------------------------------------------------------------
    start_step = 0
    if args.resume:
        if os.path.exists(args.resume):
            ckpt = torch.load(args.resume, map_location=config.device, weights_only=False)
            model.load_state_dict(ckpt["model_state_dict"])
            optimizer.load_state_dict(ckpt["optimizer_state_dict"])
            if config.use_amp and ckpt.get("scaler_state_dict"):
                scaler.load_state_dict(ckpt["scaler_state_dict"])
            start_step = ckpt.get("step", 0)
            logger.info("Resumed from step %d (best_auc=%.4f)", start_step, ckpt.get("best_auc", 0.0))
        else:
            logger.warning("Checkpoint not found at %s — starting from scratch", args.resume)

    # ------------------------------------------------------------------
    # 6. Train
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STARTING TRAINING")
    logger.info("=" * 60)

    train_step_based(
        model, loader_train, loader_val, loader_test,
        optimizer, scheduler, scaler, config, start_step=start_step,
    )

    logger.info("Training complete. Run directory: %s", run_dir)


if __name__ == "__main__":
    main()