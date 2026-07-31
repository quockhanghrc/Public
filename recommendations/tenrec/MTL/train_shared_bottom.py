#!/usr/bin/env python3
"""
Memory-Efficient MTL Recommendation Training
============================================
Key memory-saving techniques:
 1. Chunked parquet loading — never loads all files at once
 2. Batched encoding — fits encoders on train, transforms in chunks
 3. Pre-save tensors to disk — avoid keeping full DataFrames in memory
 4. Gradient accumulation — simulate larger batches on limited GPU
 5. Mixed-precision (AMP) — halve GPU memory footprint
"""

import argparse
import glob
import logging
import os
import pickle
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import LabelEncoder
from torch.cuda.amp import GradScaler, autocast
from torch.utils.data import DataLoader, Dataset

# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger("mtl_trainer")


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

    # ---- Training ----
    batch_size: int = 2048                     # actual batch per step
    accum_steps: int = 1                       # gradient accumulation steps
    # effective_batch = batch_size × accum_steps
    epochs: int = 30
    learning_rate: float = 1e-3
    weight_decay: float = 1e-5
    grad_clip_norm: float = 5.0
    lr_patience: int = 3
    lr_factor: float = 0.5
    early_stop_patience: int = 6
    use_amp: bool = True                       # automatic mixed precision

    task_weights: Dict[str, float] = field(
        default_factory=lambda: {"click": 1.0, "follow": 1.0, "like": 1.0, "share": 1.0}
    )

    model_dir: str = "./checkpoints"
    num_workers: int = 4
    pin_memory: bool = True
    device: str = "cuda" if torch.cuda.is_available() else "cpu"


# ===========================================================================
# 1. CHUNKED DATA PREPROCESSING → SAVE TENSORS TO DISK
# ===========================================================================

HIST_COLS = [f"hist_{i}" for i in range(1, 11)]
TARGET_COLS = ["click", "follow", "like", "share"]
CAT_COLS = ["user_id", "item_id", "video_category", "gender"]
NUM_COLS = ["age_norm", "watch_norm"]


def _iter_parquet_chunks(folder: str, chunk_size: int) -> Iterator[pd.DataFrame]:
    """
    Generator: yields DataFrames of ~chunk_size rows.
    Reads parquet files one at a time and splits into smaller chunks.
    This way we never hold the full dataset in memory.
    """
    files = sorted(glob.glob(os.path.join(folder, "*.parquet")))
    if not files:
        raise FileNotFoundError(f"No parquet files in {folder}")

    buffer = []
    buffer_rows = 0

    for fpath in files:
        # Read one file (still potentially large, but typically manageable)
        df = pd.read_parquet(fpath)
        # Split into chunks if needed
        for start in range(0, len(df), chunk_size):
            chunk = df.iloc[start : start + chunk_size].reset_index(drop=True)
            # Add derived numerical columns immediately to save memory
            chunk["age_norm"] = (chunk["age"].clip(0, 100) / 100.0).astype(np.float32)
            chunk["watch_norm"] = (
                np.log1p(chunk["watching_times"].clip(lower=0)) / 10.0
            ).astype(np.float32)
            yield chunk


def _fit_encoders_on_train(train_dir: str, chunk_size: int) -> Dict[str, LabelEncoder]:
    """
    Fit LabelEncoders by scanning train chunks without loading everything at once.
    Returns encoders fitted on the union of all train data.
    """
    logger.info("Fitting encoders on training data (chunked) …")

    # Collect all unique string values per column
    unique_sets: Dict[str, set] = {col: set() for col in CAT_COLS}
    item_values: set = set()

    for chunk in _iter_parquet_chunks(train_dir, chunk_size):
        for col in CAT_COLS:
            unique_sets[col].update(chunk[col].astype(str).unique())
        for hc in HIST_COLS:
            item_values.update(chunk[hc].astype(str).unique())

    # Build encoders
    encoders = {}
    for col in CAT_COLS:
        le = LabelEncoder()
        le.fit(list(unique_sets[col]))
        encoders[col] = le
        logger.info("  %s: %d unique values", col, len(le.classes_))

    # History encoder (shared with item_id vocabulary)
    item_le = encoders["item_id"]
    all_item_vals = set(item_le.classes_) | item_values
    item_le_full = LabelEncoder()
    item_le_full.fit(list(all_item_vals))
    encoders["item_id"] = item_le_full
    encoders["hist_item"] = item_le_full
    logger.info("  item_id (incl. history): %d unique values", len(item_le_full.classes_))

    return encoders


def _encode_chunk(chunk: pd.DataFrame, encoders: Dict[str, LabelEncoder]) -> pd.DataFrame:
    """Encode a single chunk using pre-fitted encoders."""
    item_le = encoders["item_id"]
    item_map = {cls: idx + 1 for idx, cls in enumerate(item_le.classes_)}

    for col in CAT_COLS:
        le = encoders[col]
        known = set(le.classes_)
        chunk[col] = (
            chunk[col]
            .astype(str)
            .map(lambda x: le.transform([x])[0] + 1 if x in known else 0)
            .astype(np.int64)
        )

    for hc in HIST_COLS:
        chunk[hc] = (
            chunk[hc]
            .astype(str)
            .map(lambda x: item_map.get(x, 0))
            .fillna(0)
            .astype(np.int64)
        )

    return chunk


def preprocess_and_cache(
    folder: str, cache_path: str, encoders: Dict[str, LabelEncoder], chunk_size: int
):
    """
    Read a folder of parquet files chunk-by-chunk, encode, and save as a single
    memory-mapped tensor file + an index file.

    Saved format:
        {cache_path}_features.npy   — memmap  (N, 4 + 10 + 2)  int64 + float32
        {cache_path}_targets.npy    — memmap  (N, 4)           float32
    """
    logger.info("Preprocessing & caching: %s → %s", folder, cache_path)

    # First pass: count total rows
    total_rows = 0
    for chunk in _iter_parquet_chunks(folder, chunk_size):
        total_rows += len(chunk)
    logger.info("  Total rows: %d", total_rows)

    # Create memmap arrays
    # Features layout:
    #   [0]: user_id (int64), [1]: item_id (int64),
    #   [2]: video_category (int64), [3]: gender (int64),
    #   [4:14]: hist_1 … hist_10 (int64),
    #   [14]: age_norm (float32), [15]: watch_norm (float32)
    #
    # We'll use separate arrays for int64 and float32 to keep types simple.
    sparse_feat = np.memmap(
        f"{cache_path}_sparse.npy", dtype=np.int64, mode="w+",
        shape=(total_rows, 4 + 10),  # 4 cat + 10 hist
    )
    dense_feat = np.memmap(
        f"{cache_path}_dense.npy", dtype=np.float32, mode="w+",
        shape=(total_rows, 2),       # age_norm, watch_norm
    )
    targets = np.memmap(
        f"{cache_path}_targets.npy", dtype=np.float32, mode="w+",
        shape=(total_rows, 4),
    )

    row_offset = 0
    for chunk in _iter_parquet_chunks(folder, chunk_size):
        chunk = _encode_chunk(chunk, encoders)
        n = len(chunk)

        sparse_feat[row_offset : row_offset + n, 0] = chunk["user_id"].values
        sparse_feat[row_offset : row_offset + n, 1] = chunk["item_id"].values
        sparse_feat[row_offset : row_offset + n, 2] = chunk["video_category"].values
        sparse_feat[row_offset : row_offset + n, 3] = chunk["gender"].values
        for i, hc in enumerate(HIST_COLS):
            sparse_feat[row_offset : row_offset + n, 4 + i] = chunk[hc].values

        dense_feat[row_offset : row_offset + n, 0] = chunk["age_norm"].values
        dense_feat[row_offset : row_offset + n, 1] = chunk["watch_norm"].values

        targets[row_offset : row_offset + n] = chunk[TARGET_COLS].values

        row_offset += n

    # Flush to disk
    sparse_feat.flush()
    dense_feat.flush()
    targets.flush()

    # Save metadata
    meta = {"total_rows": total_rows}
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

        # Open in read mode
        self.sparse = np.memmap(
            f"{cache_path}_sparse.npy", dtype=np.int64, mode="r",
            shape=(self.n_rows, 14),
        )
        self.dense = np.memmap(
            f"{cache_path}_dense.npy", dtype=np.float32, mode="r",
            shape=(self.n_rows, 2),
        )
        self.targets = np.memmap(
            f"{cache_path}_targets.npy", dtype=np.float32, mode="r",
            shape=(self.n_rows, 4),
        )

        # Pre-load as torch tensors?  No — we index into memmap arrays on __getitem__.
        # The OS page cache handles repeated access efficiently.

    def __len__(self):
        return self.n_rows

    def __getitem__(self, idx):
        row = self.sparse[idx]       # (14,) int64
        dense = self.dense[idx]      # (2,)  float32
        tgt = self.targets[idx]      # (4,)  float32

        return (
            torch.as_tensor(row[0], dtype=torch.long),     # user_id
            torch.as_tensor(row[1], dtype=torch.long),     # item_id
            torch.as_tensor(row[2], dtype=torch.long),     # video_category
            torch.as_tensor(row[3], dtype=torch.long),     # gender
            torch.as_tensor(row[4:14], dtype=torch.long),  # hist (10,)
            torch.as_tensor(dense, dtype=torch.float32),
            torch.as_tensor(tgt, dtype=torch.float32),
        )


# ===========================================================================
# 3. MODEL (same as before)
# ===========================================================================
class MTLSharedBottom(nn.Module):
    def __init__(self, config: MTLConfig, num_users: int, num_items: int,
                 num_categories: int, num_genders: int):
        super().__init__()
        self.config = config

        self.user_emb = nn.Embedding(num_users + 1, config.user_emb_dim, padding_idx=0)
        self.item_emb = nn.Embedding(num_items + 1, config.item_emb_dim, padding_idx=0)
        self.category_emb = nn.Embedding(num_categories + 1, config.category_emb_dim, padding_idx=0)
        self.gender_emb = nn.Embedding(num_genders + 1, config.gender_emb_dim, padding_idx=0)

        self.input_dim = (
            config.user_emb_dim + config.item_emb_dim + config.category_emb_dim
            + config.gender_emb_dim + 2 + config.item_emb_dim
        )

        # Shared bottom
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

        # Task towers
        self.task_names = ["click", "follow", "like", "share"]
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

    def compute_losses(self, outputs, targets):
        total = torch.tensor(0.0, device=targets.device)
        per_task = {}
        for i, task in enumerate(self.task_names):
            loss_per_sample = self.criterion(outputs[task].squeeze(1), targets[:, i])
            w = self.config.task_weights.get(task, 1.0)
            task_loss = (loss_per_sample * w).mean()
            total = total + task_loss
            per_task[task] = task_loss.item()
        return total, per_task


# ===========================================================================
# 4. TRAINING WITH GRADIENT ACCUMULATION + AMP
# ===========================================================================
def compute_aucs(outputs, targets):
    task_order = ["click", "follow", "like", "share"]
    aucs = {}
    for i, task in enumerate(task_order):
        y_true = targets[:, i].cpu().numpy()
        y_score = torch.sigmoid(outputs[task].squeeze(1)).detach().cpu().numpy()
        aucs[task] = roc_auc_score(y_true, y_score) if len(np.unique(y_true)) >= 2 else float("nan")
    return aucs


def train_one_epoch(model, loader, optimizer, scaler, config):
    """
    Gradient accumulation: weights are updated every `accum_steps` batches.
    AMP: forward/backward in mixed precision.
    """
    model.train()
    total_loss = 0.0
    task_losses = {t: 0.0 for t in model.task_names}
    n_batches = 0
    optimizer.zero_grad()

    for batch_idx, batch in enumerate(loader):
        users, items, cats, genders, hist, num_feats, targets = [
            x.to(config.device) for x in batch
        ]

        # ---- Mixed-precision forward ----
        with autocast(enabled=config.use_amp):
            outputs = model(users, items, cats, genders, hist, num_feats)
            loss, per_task = model.compute_losses(outputs, targets)
            # Scale loss by accumulation steps so gradients average correctly
            loss = loss / config.accum_steps

        # ---- Mixed-precision backward ----
        if config.use_amp:
            scaler.scale(loss).backward()
        else:
            loss.backward()

        total_loss += loss.item() * config.accum_steps  # undo scaling for logging
        for t in model.task_names:
            task_losses[t] += per_task[t]
        n_batches += 1

        # ---- Step only after accumulation ----
        if (batch_idx + 1) % config.accum_steps == 0:
            if config.use_amp:
                scaler.unscale_(optimizer)
            nn.utils.clip_grad_norm_(model.parameters(), config.grad_clip_norm)
            if config.use_amp:
                scaler.step(optimizer)
                scaler.update()
            else:
                optimizer.step()
            optimizer.zero_grad()

    # Handle leftover accumulated gradients
    if n_batches % config.accum_steps != 0:
        if config.use_amp:
            scaler.unscale_(optimizer)
        nn.utils.clip_grad_norm_(model.parameters(), config.grad_clip_norm)
        if config.use_amp:
            scaler.step(optimizer)
            scaler.update()
        else:
            optimizer.step()
        optimizer.zero_grad()

    return total_loss / n_batches, {t: v / n_batches for t, v in task_losses.items()}


@torch.no_grad()
def evaluate(model, loader, config):
    model.eval()
    total_loss = 0.0
    task_losses = {t: 0.0 for t in model.task_names}
    all_outputs = {t: [] for t in model.task_names}
    all_targets = []
    n_batches = 0

    for batch in loader:
        users, items, cats, genders, hist, num_feats, targets = [
            x.to(config.device) for x in batch
        ]
        with autocast(enabled=config.use_amp):
            outputs = model(users, items, cats, genders, hist, num_feats)
            loss, per_task = model.compute_losses(outputs, targets)

        total_loss += loss.item()
        for t in model.task_names:
            task_losses[t] += per_task[t]
            all_outputs[t].append(outputs[t].cpu())
        all_targets.append(targets.cpu())
        n_batches += 1

    avg_loss = total_loss / n_batches
    avg_task_losses = {t: v / n_batches for t, v in task_losses.items()}
    cat_outputs = {t: torch.cat(all_outputs[t]) for t in model.task_names}
    cat_targets = torch.cat(all_targets)
    aucs = compute_aucs(cat_outputs, cat_targets)
    return avg_loss, avg_task_losses, aucs


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=str, default="./data/split")
    parser.add_argument("--cache_dir", type=str, default="./cache")
    parser.add_argument("--model_dir", type=str, default="./checkpoints")
    parser.add_argument("--batch_size", type=int, default=2048)
    parser.add_argument("--accum_steps", type=int, default=1,
 help="Gradient accumulation steps (increase for larger effective batch)")
    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--chunk_size", type=int, default=200_000,
 help="Rows per chunk during preprocessing")
    parser.add_argument("--no_amp", action="store_true", help="Disable mixed precision")
    parser.add_argument("--device", type=str,
                        default="cuda" if torch.cuda.is_available() else "cpu")
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # 1. Preprocess & cache (do this once — skip if cache exists)
    # ------------------------------------------------------------------
    os.makedirs(args.cache_dir, exist_ok=True)
    train_cache = os.path.join(args.cache_dir, "train")
    val_cache = os.path.join(args.cache_dir, "val")
    test_cache = os.path.join(args.cache_dir, "test")

    if not os.path.exists(f"{train_cache}_meta.pkl"):
        logger.info("=" * 60)
        logger.info("STEP 1: Chunked preprocessing → memmap cache")
        logger.info("=" * 60)

        encoders = _fit_encoders_on_train(
            os.path.join(args.data_dir, "train"), args.chunk_size
        )

        preprocess_and_cache(
            os.path.join(args.data_dir, "train"), train_cache, encoders, args.chunk_size
        )
        preprocess_and_cache(
            os.path.join(args.data_dir, "val"), val_cache, encoders, args.chunk_size
        )
        preprocess_and_cache(
            os.path.join(args.data_dir, "test"), test_cache, encoders, args.chunk_size
        )

        # Save encoders
        with open(os.path.join(args.model_dir, "encoders.pkl"), "wb") as f:
            pickle.dump(encoders, f)
    else:
        logger.info("✓ Found cached preprocessed data — skipping preprocessing")
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

    loader_train = DataLoader(ds_train, batch_size=args.batch_size, shuffle=True,
                              num_workers=4, pin_memory=True, drop_last=False)
    loader_val = DataLoader(ds_val, batch_size=args.batch_size * 2, shuffle=False,
                            num_workers=4, pin_memory=True)
    loader_test = DataLoader(ds_test, batch_size=args.batch_size * 2, shuffle=False,
                             num_workers=4, pin_memory=True)

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
    # 4. Model
    # ------------------------------------------------------------------
    config = MTLConfig(
        data_dir=args.data_dir,
        cache_dir=args.cache_dir,
        batch_size=args.batch_size,
        accum_steps=args.accum_steps,
        epochs=args.epochs,
        learning_rate=args.lr,
        chunk_size=args.chunk_size,
        model_dir=args.model_dir,
        device=args.device,
        use_amp=not args.no_amp and args.device == "cuda",
    )

    eff_batch = config.batch_size * config.accum_steps
    logger.info("Effective batch size: %d × %d = %d",
                config.batch_size, config.accum_steps, eff_batch)

    model = MTLSharedBottom(config, num_users, num_items, num_categories, num_genders)
    model = model.to(config.device)

    total_p = sum(p.numel() for p in model.parameters())
    trainable_p = sum(p.numel() for p in model.parameters() if p.requires_grad)
    logger.info("Params — total: %d  |  trainable: %d", total_p, trainable_p)

    optimizer = optim.AdamW(model.parameters(), lr=config.learning_rate,
                            weight_decay=config.weight_decay)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", factor=config.lr_factor,
        patience=config.lr_patience, verbose=True,
    )
    scaler = GradScaler(enabled=config.use_amp)

    # ------------------------------------------------------------------
    # 5. Training
    # ------------------------------------------------------------------
    os.makedirs(config.model_dir, exist_ok=True)
    best_val_auc = 0.0
    best_epoch = 0
    epochs_no_improve = 0

    logger.info("=" * 60)
    logger.info("STEP 3: Training (AMP=%s, accum_steps=%d)",
                config.use_amp, config.accum_steps)
    logger.info("=" * 60)

    for epoch in range(1, config.epochs + 1):
        train_loss, train_task_losses = train_one_epoch(
            model, loader_train, optimizer, scaler, config
        )
        val_loss, val_task_losses, val_aucs = evaluate(model, loader_val, config)

        mean_auc = np.nanmean(list(val_aucs.values()))
        auc_str = "  ".join(f"{t}={val_aucs[t]:.4f}" for t in model.task_names)
        logger.info(
            "Epoch %2d | train_loss: %.4f | val_loss: %.4f | mean_auc: %.4f | %s",
            epoch, train_loss, val_loss, mean_auc, auc_str,
        )

        scheduler.step(val_loss)

        if mean_auc > best_val_auc:
            best_val_auc = mean_auc
            best_epoch = epoch
            epochs_no_improve = 0
            torch.save(
                {
                    "epoch": epoch,
                    "model_state_dict": model.state_dict(),
                    "optimizer_state_dict": optimizer.state_dict(),
                    "best_auc": best_val_auc,
                },
                os.path.join(config.model_dir, "best_model.pt"),
            )
        else:
            epochs_no_improve += 1

        if epochs_no_improve >= config.early_stop_patience:
            logger.info("Early stopping at epoch %d", epoch)
            break

    # ------------------------------------------------------------------
    # 6. Test evaluation
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 4: Test evaluation")
    logger.info("=" * 60)

    best_ckpt = os.path.join(config.model_dir, "best_model.pt")
    if os.path.exists(best_ckpt):
        ckpt = torch.load(best_ckpt, map_location=config.device)
        model.load_state_dict(ckpt["model_state_dict"])
        logger.info("Loaded best model (epoch %d, val_auc=%.4f)", ckpt["epoch"], ckpt["best_auc"])

    test_loss, test_task_losses, test_aucs = evaluate(model, loader_test, config)
    test_mean_auc = np.nanmean(list(test_aucs.values()))

    for task in model.task_names:
        logger.info("  %-8s  loss: %.4f  |  AUC: %.4f", task, test_task_losses[task], test_aucs[task])
    logger.info("  %-8s  AUC: %.4f", "mean", test_mean_auc)
    logger.info("Done.")


if __name__ == "__main__":
    main()