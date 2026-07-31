#!/usr/bin/env python3
"""
Multi-Task Learning (MTL) Recommendation Model — Shared-Bottom Architecture
============================================================================
Targets: click, follow, like, share (all binary)

Data layout expected:
    data_dir/
    ├── train/
    │   ├── part_001.parquet
    │   ├── part_002.parquet
    │   └── ...
    ├── val/
    │   └── ...
    └── test/
        └── ...

Features: user_id, item_id, video_category, watching_times, gender, age,
          hist_1 … hist_10 (historical item IDs)

Architecture (Shared-Bottom):
    1. Sparse features → Embedding layers
    2. History items → item embedding + masked mean-pool
    3. All vectors + numerical features → concatenated
    4. Shared Bottom MLP → common pattern extraction
    5. Four independent Task Towers → binary logit per task
    6. Combined BCE loss → shared + task-specific weight updates
"""

import argparse
import glob
import logging
import os
import pickle
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
from sklearn.metrics import roc_auc_score
from sklearn.preprocessing import LabelEncoder
from torch.utils.data import DataLoader, Dataset

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger("mtl_trainer")


# ===========================================================================
# CONFIGURATION
# ===========================================================================
@dataclass
class MTLConfig:
    """Hyperparameters & paths for the MTL training pipeline."""

    # ---- Data ----
    data_dir: str = "./data/split"  # expects train/ val/ test/ subfolders inside
    num_workers: int = 4
    pin_memory: bool = True

    # ---- Embedding dimensions ----
    user_emb_dim: int = 64
    item_emb_dim: int = 64
    category_emb_dim: int = 16
    gender_emb_dim: int = 8

    # ---- Shared Bottom ----
    shared_hidden: List[int] = field(default_factory=lambda: [256, 128])
    shared_dropout: float = 0.2

    # ---- Task Towers ----
    tower_hidden: List[int] = field(default_factory=lambda: [64, 32])
    tower_dropout: float = 0.15

    # ---- Training ----
    batch_size: int = 2048
    epochs: int = 30
    learning_rate: float = 1e-3
    weight_decay: float = 1e-5
    grad_clip_norm: float = 5.0
    lr_patience: int = 3
    lr_factor: float = 0.5
    early_stop_patience: int = 6

    # ---- Task loss weights (can emphasise harder / rarer tasks) ----
    task_weights: Dict[str, float] = field(
        default_factory=lambda: {"click": 1.0, "follow": 1.0, "like": 1.0, "share": 1.0}
    )

    # ---- Output ----
    model_dir: str = "./checkpoints"
    device: str = "cuda" if torch.cuda.is_available() else "cpu"


# ===========================================================================
# DATA LOADING & PREPROCESSING
# ===========================================================================
def load_parquet_folder(folder_path: str) -> pd.DataFrame:
    """Load and concatenate all *.parquet files inside *folder_path*."""
    pattern = os.path.join(folder_path, "*.parquet")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {folder_path}")

    logger.info("  Loading %d parquet file(s) from %s …", len(files), folder_path)
    dfs = []
    for f in files:
        dfs.append(pd.read_parquet(f))
        logger.debug("    ✓ %s  (%d rows)", os.path.basename(f), len(dfs[-1]))

    df = pd.concat(dfs, ignore_index=True)
    logger.info("  → %d total rows", len(df))
    return df


def preprocess_features(
    df_train: pd.DataFrame, df_val: pd.DataFrame, df_test: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """
    Fit encoders on the **training set only**, then transform all three splits.

    This avoids data leakage — the model never sees val/test category indices
    that weren't present at train time.

    Returns transformed DataFrames and a dict of fitted encoders.
    """
    encoders: Dict[str, LabelEncoder] = {}
    cat_cols = ["user_id", "item_id", "video_category", "gender"]
    hist_cols = [f"hist_{i}" for i in range(1, 11)]

    # ---- Encode categorical features (fit on train only) ----
    for col in cat_cols:
        le = LabelEncoder()
        le.fit(df_train[col].astype(str))
        # offset by 1 so that 0 is reserved for "unknown / padding"
        for df_split, name in [(df_train, "train"), (df_val, "val"), (df_test, "test")]:
            df_split[col] = (
                df_split[col]
                .astype(str)
                .map(lambda x: x if x in le.classes_ else None)  # unseen → None
                .fillna(le.classes_[0]) ) # fallback to first class )
            df_split[col] = le.transform(df_split[col]) + 1
        encoders[col] = le
        logger.info("  %s: %d unique values (+1 pad offset)", col, len(le.classes_))

    # ---- History columns → reuse item_id vocabulary (fit on train item_id) ----
    item_le = encoders["item_id"]
    item_class_map = {cls: idx + 1 for idx, cls in enumerate(item_le.classes_)}

    for hc in hist_cols:
        for df_split, name in [(df_train, "train"), (df_val, "val"), (df_test, "test")]:
            df_split[hc] = (
                df_split[hc]
                .astype(str)
                .map(item_class_map)
                .fillna(0)                        # 0 = padding / unknown
                .astype(np.int64)
            )
    encoders["hist_item"] = item_le # reference to same encoder

    # ---- Numerical features ----
    for df_split in [df_train, df_val, df_test]:
        df_split["age_norm"] = (df_split["age"].clip(0, 100) / 100.0).astype(np.float32)
        df_split["watch_norm"] = (
            np.log1p(df_split["watching_times"].clip(lower=0)) / 10.0
        ).astype(np.float32)

    return df_train, df_val, df_test, encoders


# ===========================================================================
# PyTorch DATASET
# ===========================================================================
class MTLRecommendationDataset(Dataset):
    """Torch Dataset that wraps a single preprocessed DataFrame split."""

    TARGET_COLS = ["click", "follow", "like", "share"]
    FEAT_COLS = ["user_id", "item_id", "video_category", "gender"]
    HIST_COLS = [f"hist_{i}" for i in range(1, 11)]
    NUM_COLS = ["age_norm", "watch_norm"]

    def __init__(self, df: pd.DataFrame):
        # Sparse features
        self.users = torch.as_tensor(df["user_id"].to_numpy(), dtype=torch.long)
        self.items = torch.as_tensor(df["item_id"].to_numpy(), dtype=torch.long)
        self.categories = torch.as_tensor(df["video_category"].to_numpy(), dtype=torch.long)
        self.genders = torch.as_tensor(df["gender"].to_numpy(), dtype=torch.long)

        # History sequence (N, 10)
        self.history = torch.as_tensor(
            df[self.HIST_COLS].to_numpy(), dtype=torch.long
        )

        # Numerical features  (N, 2)
        self.numerical = torch.as_tensor(
            df[self.NUM_COLS].to_numpy(), dtype=torch.float32
        )

        # Targets  (N, 4) — order: click, follow, like, share
        self.targets = torch.as_tensor(
            df[self.TARGET_COLS].to_numpy(), dtype=torch.float32
        )

    def __len__(self) -> int:
        return len(self.users)

    def __getitem__(self, idx: int):
        return (
            self.users[idx],
            self.items[idx],
            self.categories[idx],
            self.genders[idx],
            self.history[idx],
            self.numerical[idx],
            self.targets[idx],
        )


# ===========================================================================
# SHARED-BOTTOM MTL MODEL
# ===========================================================================
class MTLSharedBottom(nn.Module):
    """
    Shared-Bottom Multi-Task Learning model.

    Flow    ----
    user_id, item_id, category, gender →  Embedding
    hist_1 … hist_10                    →  item-embedding → masked mean-pool
    All embeddings + numerical features →  concatenate
    Shared Bottom MLP                   →  shared representation
    Four independent Towers             →  1 logit each    """

    def __init__(self, config: MTLConfig, num_users: int, num_items: int,
                 num_categories: int, num_genders: int):
        super().__init__()
        self.config = config

        # ---- Embedding layers ----
        self.user_emb = nn.Embedding(num_users + 1, config.user_emb_dim, padding_idx=0)
        self.item_emb = nn.Embedding(num_items + 1, config.item_emb_dim, padding_idx=0)
        self.category_emb = nn.Embedding(num_categories + 1, config.category_emb_dim, padding_idx=0)
        self.gender_emb = nn.Embedding(num_genders + 1, config.gender_emb_dim, padding_idx=0)

        # Total input width to shared bottom
        self.input_dim = (
            config.user_emb_dim
            + config.item_emb_dim          # target item
            + config.category_emb_dim
            + config.gender_emb_dim
            + 2                             # age_norm, watch_norm
            + config.item_emb_dim           # history pooled vector
        )

        # ---- Shared Bottom MLP ----
        shared_layers = []
        in_dim = self.input_dim
        for h in config.shared_hidden:
            shared_layers.extend([
                nn.Linear(in_dim, h),
                nn.BatchNorm1d(h),
                nn.ReLU(inplace=True),
                nn.Dropout(config.shared_dropout),
            ])
            in_dim = h
        self.shared_bottom = nn.Sequential(*shared_layers)
        self.shared_out_dim = config.shared_hidden[-1] if config.shared_hidden else self.input_dim

        # ---- Task-specific Towers ----
        self.task_names = ["click", "follow", "like", "share"]
        self.towers = nn.ModuleDict()
        for task in self.task_names:
            tower_layers = []
            tower_in = self.shared_out_dim
            for h in config.tower_hidden:
                tower_layers.extend([
                    nn.Linear(tower_in, h),
                    nn.BatchNorm1d(h),
                    nn.ReLU(inplace=True),
                    nn.Dropout(config.tower_dropout),
                ])
                tower_in = h
            tower_layers.append(nn.Linear(tower_in, 1))  # single logit
            self.towers[task] = nn.Sequential(*tower_layers)

        # Loss function (reduction='none' so we can weight per task)
        self.criterion = nn.BCEWithLogitsLoss(reduction="none")

        # Weight initialisation
        self.apply(self._init_weights)

    @staticmethod
    def _init_weights(m):
        if isinstance(m, nn.Linear):
            nn.init.xavier_uniform_(m.weight)
            if m.bias is not None:
                nn.init.zeros_(m.bias)
        elif isinstance(m, nn.Embedding):
            nn.init.normal_(m.weight, mean=0.0, std=0.02)

    def _pool_history(self, hist_indices: torch.Tensor) -> torch.Tensor:
        """
        Masked mean-pool over historical item embeddings.
        Args:
            hist_indices: (batch, 10)  —  0 indicates padding / unknown.
        Returns:
            pooled: (batch, item_emb_dim)
        """
        emb = self.item_emb(hist_indices)               # (B, 10, E)
        mask = (hist_indices > 0).unsqueeze(-1).float() # (B, 10, 1)
        summed = (emb * mask).sum(dim=1)                 # (B, E)
        counts = mask.sum(dim=1).clamp(min=1)            # (B, 1)  avoid div-by-zero
        return summed / counts

    def forward(self, users, items, categories, genders, history, numerical):
        """
        Returns:
            dict: {"click": logits, "follow": logits, "like": logits, "share": logits}
                  Each logits shape is (batch, 1).
        """
        # Embeddings
        u = self.user_emb(users)          # (B, Eu)
        i = self.item_emb(items)          # (B, Ei)
        c = self.category_emb(categories) # (B, Ec)
        g = self.gender_emb(genders)      # (B, Eg)
        h = self._pool_history(history)   # (B, Ei)

        # Concatenate all feature vectors
        combined = torch.cat([u, i, c, g, numerical, h], dim=1)  # (B, input_dim)

        # Shared bottom
        shared = self.shared_bottom(combined)  # (B, shared_out_dim)

        # Task towers
        outputs = {}
        for task in self.task_names:
            outputs[task] = self.towers[task](shared)  # (B, 1)

        return outputs

    def compute_losses(self, outputs: dict, targets: torch.Tensor) -> Tuple[torch.Tensor, dict]:
        """
        Args:
            outputs: dict of task → (B, 1) logits.
            targets: (B, 4)  —  order [click, follow, like, share].
        Returns:
            total_loss (scalar tensor), per_task_loss dict (python floats).
        """
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
# TRAINING & EVALUATION HELPERS
# ===========================================================================
def compute_aucs(outputs: dict, targets: torch.Tensor) -> Dict[str, float]:
    """Compute ROC-AUC per task (numpy)."""
    task_order = ["click", "follow", "like", "share"]
    aucs = {}
    for i, task in enumerate(task_order):
        y_true = targets[:, i].cpu().numpy()
        y_score = torch.sigmoid(outputs[task].squeeze(1)).detach().cpu().numpy()
        if len(np.unique(y_true)) < 2:
            aucs[task] = float("nan")
        else:
            aucs[task] = roc_auc_score(y_true, y_score)
    return aucs


def train_one_epoch(model, loader, optimizer, device):
    """Return average total loss and per-task losses (as floats)."""
    model.train()
    total_loss = 0.0
    task_losses = {t: 0.0 for t in model.task_names}
    n_batches = 0

    for batch in loader:
        users, items, cats, genders, hist, num_feats, targets = [
            x.to(device) for x in batch
        ]

        optimizer.zero_grad()
        outputs = model(users, items, cats, genders, hist, num_feats)
        loss, per_task = model.compute_losses(outputs, targets)
        loss.backward()

        nn.utils.clip_grad_norm_(model.parameters(), model.config.grad_clip_norm)
        optimizer.step()

        total_loss += loss.item()
        for t in model.task_names:
            task_losses[t] += per_task[t]
        n_batches += 1

    return total_loss / n_batches, {t: v / n_batches for t, v in task_losses.items()}


@torch.no_grad()
def evaluate(model, loader, device):
    """Return avg total loss, per-task avg losses, and per-task AUCs."""
    model.eval()
    total_loss = 0.0
    task_losses = {t: 0.0 for t in model.task_names}
    all_outputs: Dict[str, List[torch.Tensor]] = {t: [] for t in model.task_names}
    all_targets: List[torch.Tensor] = []
    n_batches = 0

    for batch in loader:
        users, items, cats, genders, hist, num_feats, targets = [
            x.to(device) for x in batch
        ]

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

    # Concatenate all batches for AUC computation
    cat_outputs = {t: torch.cat(all_outputs[t]) for t in model.task_names}
    cat_targets = torch.cat(all_targets)
    aucs = compute_aucs(cat_outputs, cat_targets)

    return avg_loss, avg_task_losses, aucs


def save_checkpoint(model, optimizer, epoch, best_auc, path: str):
    torch.save(
        {
            "epoch": epoch,
            "model_state_dict": model.state_dict(),
            "optimizer_state_dict": optimizer.state_dict(),
            "best_auc": best_auc,
            "config": model.config,
        },
        path,
    )
    logger.info("  ✓ Checkpoint saved → %s", path)


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(
        description="MTL Shared-Bottom Training with train/val/test folders"
    )
    parser.add_argument(
        "--data_dir", type=str, default="./data/split",
        help="Root folder containing train/, val/, test/ subfolders with parquet files",
    )
    parser.add_argument("--model_dir", type=str, default="./checkpoints")
    parser.add_argument("--batch_size", type=int, default=2048)
    parser.add_argument("--epochs", type=int, default=30)
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument(
        "--device", type=str,
        default="cuda" if torch.cuda.is_available() else "cpu",
    )
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # 1. Load data from train/ val/ test/ folders
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 1: Loading data from train/ val/ test/ folders")
    logger.info("=" * 60)

    train_dir = os.path.join(args.data_dir, "train")
    val_dir = os.path.join(args.data_dir, "val")
    test_dir = os.path.join(args.data_dir, "test")

    for folder, name in [(train_dir, "train"), (val_dir, "val"), (test_dir, "test")]:
        if not os.path.isdir(folder):
            raise FileNotFoundError(f"{name.capitalize()} folder not found: {folder}")

    df_train_raw = load_parquet_folder(train_dir)
    df_val_raw = load_parquet_folder(val_dir)
    df_test_raw = load_parquet_folder(test_dir)

    logger.info("Total — train: %d | val: %d | test: %d",
                len(df_train_raw), len(df_val_raw), len(df_test_raw))

    # ------------------------------------------------------------------
    # 2. Preprocess (fit encoders on train only)
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 2: Preprocessing (encoding, normalisation)")
    logger.info("=" * 60)

    df_train, df_val, df_test, encoders = preprocess_features(
        df_train_raw, df_val_raw, df_test_raw
    )

    # ------------------------------------------------------------------
    # 3. Determine vocabulary sizes (from train set)
    # ------------------------------------------------------------------
    num_users = int(df_train["user_id"].max())
    num_items = int(df_train["item_id"].max())
    num_categories = int(df_train["video_category"].max())
    num_genders = int(df_train["gender"].max())
    logger.info(
        "Vocab sizes — users: %d | items: %d | categories: %d | genders: %d",
        num_users, num_items, num_categories, num_genders,
    )

    # ------------------------------------------------------------------
    # 4. Build PyTorch datasets & data loaders
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 3: Building datasets & data loaders")
    logger.info("=" * 60)

    ds_train = MTLRecommendationDataset(df_train)
    ds_val = MTLRecommendationDataset(df_val)
    ds_test = MTLRecommendationDataset(df_test)

    loader_train = DataLoader(
        ds_train,
        batch_size=args.batch_size,
        shuffle=True,
        num_workers=4,
        pin_memory=True,
        drop_last=False,
    )
    loader_val = DataLoader(
        ds_val,
        batch_size=args.batch_size * 2, # larger batch for val — no gradients
        shuffle=False,
        num_workers=4,
        pin_memory=True,
    )
    loader_test = DataLoader(
        ds_test,
        batch_size=args.batch_size * 2,
        shuffle=False,
        num_workers=4,
        pin_memory=True,
    )

    # ------------------------------------------------------------------
    # 5. Build model, optimizer, scheduler
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 4: Building model")
    logger.info("=" * 60)

    config = MTLConfig(
        data_dir=args.data_dir,
        batch_size=args.batch_size,
        epochs=args.epochs,
        learning_rate=args.lr,
        model_dir=args.model_dir,
        device=args.device,
    )
    model = MTLSharedBottom(config, num_users, num_items, num_categories, num_genders)
    model = model.to(config.device)

    total_params = sum(p.numel() for p in model.parameters())
    trainable_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    logger.info("Total params: %d  |  Trainable: %d", total_params, trainable_params)

    optimizer = optim.AdamW(
        model.parameters(),
        lr=config.learning_rate,
        weight_decay=config.weight_decay,
    )
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(
        optimizer,
        mode="min",
        factor=config.lr_factor,
        patience=config.lr_patience,
        verbose=True,
    )

    # ------------------------------------------------------------------
    # 6. Training loop (val after each epoch)
    # ------------------------------------------------------------------
    os.makedirs(config.model_dir, exist_ok=True)
    best_val_auc = 0.0
    best_epoch = 0
    epochs_no_improve = 0

    logger.info("=" * 60)
    logger.info("STEP 5: Training loop")
    logger.info("=" * 60)

    for epoch in range(1, config.epochs + 1):
        # ----- Train -----
        train_loss, train_task_losses = train_one_epoch(
            model, loader_train, optimizer, config.device
        )

        # ----- Validate -----
        val_loss, val_task_losses, val_aucs = evaluate(
            model, loader_val, config.device
        )

        mean_auc = np.nanmean(list(val_aucs.values()))

        # Pretty-print
        auc_str = "  ".join(f"{t}={val_aucs[t]:.4f}" for t in model.task_names)
        logger.info(
            "Epoch %2d | train_loss: %.4f | val_loss: %.4f | mean_auc: %.4f | %s",
            epoch, train_loss, val_loss, mean_auc, auc_str,
        )

        # Learning-rate scheduling
        scheduler.step(val_loss)

        # Best model checkpointing
        if mean_auc > best_val_auc:
            best_val_auc = mean_auc
            best_epoch = epoch
            epochs_no_improve = 0
            save_checkpoint(
                model, optimizer, epoch, best_val_auc,
                os.path.join(config.model_dir, "best_model.pt"),
            )
        else:
            epochs_no_improve += 1

        # Early stopping
        if epochs_no_improve >= config.early_stop_patience:
            logger.info(
                "Early stopping triggered after %d epochs without improvement.",
                config.early_stop_patience,
            )
            break

    # ------------------------------------------------------------------
    # 7. Final evaluation on test set
    # ------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("STEP 6: Final evaluation on TEST set")
    logger.info("=" * 60)

    # Reload the best checkpoint
    best_ckpt = os.path.join(config.model_dir, "best_model.pt")
    if os.path.exists(best_ckpt):
        checkpoint = torch.load(best_ckpt, map_location=config.device)
        model.load_state_dict(checkpoint["model_state_dict"])
        logger.info("Loaded best model from epoch %d (val mean_auc: %.4f)",
 checkpoint["epoch"], checkpoint["best_auc"])
    else:
        logger.warning("No checkpoint found — evaluating with current model.")

    test_loss, test_task_losses, test_aucs = evaluate(
        model, loader_test, config.device
    )
    test_mean_auc = np.nanmean(list(test_aucs.values()))

    logger.info("─── Test Results ───")
    for task in model.task_names:
        logger.info("  %-8s  loss: %.4f  |  AUC: %.4f",
 task, test_task_losses[task], test_aucs[task])
    logger.info("  %-8s  AUC: %.4f", "mean", test_mean_auc)

    # ------------------------------------------------------------------
    # 8. Save encoders & final summary
    # ------------------------------------------------------------------
    with open(os.path.join(config.model_dir, "encoders.pkl"), "wb") as f:
        pickle.dump(encoders, f)
    logger.info("Encoders saved → %s", os.path.join(config.model_dir, "encoders.pkl"))

    logger.info("=" * 60)
    logger.info("Training complete.")
    logger.info("Best validation mean AUC: %.4f  (epoch %d)", best_val_auc, best_epoch)
    logger.info("Test mean AUC: %.4f", test_mean_auc)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()