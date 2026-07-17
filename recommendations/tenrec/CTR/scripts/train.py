"""
Training entry point for the Tenrec CTR model.

Run isolation:
  Each run writes to runs/{run_name}_{unix_timestamp}/ containing:
    tensorboard/   TensorBoard event files
    metrics.json   per-epoch train/val/test metrics
    plots/         auc.png, prauc.png, loss.png
    ctr_best.pt    best checkpoint (by val AUC)

Low-RAM design:
  Data is streamed via CtrIterableDataset (pyarrow iter_batches). AUC/PR-AUC
  are computed incrementally in batches using roc_auc_score / average_precision_score
  on accumulated predictions, so we never hold the full dataset in memory.

Imbalance handling:
  FocalLoss (BCE focal) with gamma/alpha, plus a pos_weight fallback computed
  from the data pos_rate. We report AUC and PR-AUC (not accuracy) because the
  click label is imbalanced (~27% positive).
"""

import argparse
import json
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import torch
import torch.nn as nn
from torch.utils.tensorboard import SummaryWriter
from tqdm import tqdm
import config
from dataset import load_stats, get_dataloader
from model import build_model


class FocalLoss(nn.Module):
    """Binary focal loss. pt = p if y==1 else 1-p.
    FL = -alpha * (1-pt)^gamma * log(pt)
    """

    def __init__(self, gamma: float = 2.0, alpha: float = 0.75, pos_weight: float = 1.0):
        super().__init__()
        self.gamma = gamma
        self.alpha = alpha
        self.pos_weight = pos_weight
        self.bce = nn.BCEWithLogitsLoss(pos_weight=torch.tensor([pos_weight]),
                                        reduction="none")

    def forward(self, logits: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        bce = self.bce(logits, targets)
        pt = torch.exp(-bce)
        focal = self.alpha * (1 - pt) ** self.gamma * bce
        # for negative samples use (1-alpha) weighting
        focal = torch.where(targets == 1, focal, (1 - self.alpha) * focal)
        return focal.mean()


class EarlyStopping:
    def __init__(self, patience: int = 3, monitor: str = "val_auc", higher_better: bool = True):
        self.patience = patience
        self.monitor = monitor
        self.higher_better = higher_better
        self.best = -np.inf if higher_better else np.inf
        self.counter = 0
        self.best_epoch = 0

    def step(self, metrics: dict) -> bool:
        val = metrics[self.monitor]
        improved = (val > self.best) if self.higher_better else (val < self.best)
        if improved:
            self.best = val
            self.best_epoch = metrics["epoch"]
            self.counter = 0
        else:
            self.counter += 1
        return self.counter >= self.patience


class StreamingMetrics:
    """Memory-bounded AUC / PR-AUC / loss via score-histogram binning.

    Only O(n_bins) aggregates are kept, so this works on the full 96M-row
    train split without ever loading it into RAM. AUC uses the standard
    histogram approximation of the Mann-Whitney statistic; PR-AUC (average
    precision) is approximated trapezoidally over bin thresholds.
    """

    def __init__(self, n_bins: int = 1000):
        self.n_bins = n_bins
        self.pos = np.zeros(n_bins, dtype=np.float64)
        self.neg = np.zeros(n_bins, dtype=np.float64)
        self.total_pos = 0.0
        self.total_neg = 0.0
        self.running_loss = 0.0
        self.n = 0

    def update(self, logits_np: np.ndarray, labels_np: np.ndarray, loss_val=None):
        logits_np = np.asarray(logits_np, dtype=np.float64)
        labels_np = np.asarray(labels_np, dtype=np.float64)
        # Drop non-finite logits (defensive; should not happen after the
        # all-padding history fix in model.py).
        finite = np.isfinite(logits_np)
        logits_np = logits_np[finite]
        labels_np = labels_np[finite]
        if logits_np.size == 0:
            return
        scores = 1.0 / (1.0 + np.exp(-logits_np))  # sigmoid -> [0, 1]
        bins = np.minimum((scores * self.n_bins).astype(np.int64), self.n_bins - 1)
        for b, y in zip(bins, labels_np):
            if y >= 0.5:
                self.pos[b] += 1.0
                self.total_pos += 1.0
            else:
                self.neg[b] += 1.0
                self.total_neg += 1.0
        self.n += labels_np.size
        if loss_val is not None:
            self.running_loss += float(loss_val) * labels_np.size

    def compute(self):
        P, N = self.total_pos, self.total_neg
        if P == 0 or N == 0:
            return {"auc": float("nan"), "prauc": float("nan"),
                    "loss": self.running_loss / max(self.n, 1)}
        cum_neg = np.cumsum(self.neg)
        neg_before = np.zeros_like(cum_neg)
        neg_before[1:] = cum_neg[:-1]
        auc = np.sum(self.pos * (neg_before + self.neg / 2.0)) / (P * N)
        order = np.arange(self.n_bins)[::-1]  # high score -> low score
        tp = np.cumsum(self.pos[order])
        fp = np.cumsum(self.neg[order])
        prec = tp / np.maximum(tp + fp, 1.0)
        rec = tp / P
        drec = np.diff(rec, prepend=0.0)
        prauc = np.sum(prec * drec)
        return {"auc": float(auc), "prauc": float(prauc),
                "loss": self.running_loss / max(self.n, 1)}


@torch.no_grad()
def evaluate(model, dataloader, device, max_steps: int = None, desc: str = "eval"):
    model.eval()
    sm = StreamingMetrics()
    loss_fn = nn.BCEWithLogitsLoss()
    pbar = tqdm(dataloader, desc=desc, total=max_steps, leave=False)
    for i, batch in enumerate(pbar):
        if max_steps is not None and i >= max_steps:
            break
        labels = batch["click"].to(device)
        feats = {k: v.to(device) for k, v in batch.items() if k != "click"}
        logits = model(feats)  # (B,) raw logits
        loss = loss_fn(logits, labels)
        sm.update(logits.detach().cpu().numpy(), labels.detach().cpu().numpy(),
                  float(loss.item()))
        pbar.set_postfix(loss=f"{sm.compute()['loss']:.4f}")
    return sm.compute()


def train_one_epoch(model, dataloader, optimizer, criterion, device, max_steps: int = None):
    model.train()
    sm = StreamingMetrics()
    pbar = tqdm(dataloader, desc="train", total=max_steps, leave=False)
    for i, batch in enumerate(pbar):
        if max_steps is not None and i >= max_steps:
            break
        labels = batch["click"].to(device)
        feats = {k: v.to(device) for k, v in batch.items() if k != "click"}
        logits = model(feats)  # (B,) raw logits
        loss = criterion(logits, labels)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()
        sm.update(logits.detach().cpu().numpy(), labels.detach().cpu().numpy(),
                  float(loss.item()))
        pbar.set_postfix(loss=f"{sm.compute()['loss']:.4f}")
    return sm.compute()


def plot_metrics(metrics_history, run_dir: Path):
    epochs = [m["epoch"] for m in metrics_history]
    plots_dir = run_dir / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    def _plot(key, title, fname):
        plt.figure(figsize=(8, 5))
        for split in ("train", "val", "test"):
            vals = [m[split][key] for m in metrics_history]
            plt.plot(epochs, vals, marker="o", label=split)
        plt.title(title)
        plt.xlabel("epoch")
        plt.ylabel(key)
        plt.legend()
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(plots_dir / fname, dpi=120)
        plt.close()

    _plot("auc", "AUC: train / val / test", "auc.png")
    _plot("prauc", "PR-AUC: train / val / test", "prauc.png")
    _plot("loss", "Loss: train / val / test", "loss.png")
    print(f"  plots -> {plots_dir}")


def main():
    p = argparse.ArgumentParser(description="Train Tenrec CTR model.")
    p.add_argument("--run-name", default=config.DEFAULT_RUN_NAME)
    p.add_argument("--train-frac", type=float, default=config.TRAIN_FRAC)
    p.add_argument("--val-frac", type=float, default=config.VAL_FRAC)
    p.add_argument("--test-frac", type=float, default=config.TEST_FRAC)
    p.add_argument("--batch-size", type=int, default=config.BATCH_SIZE)
    p.add_argument("--epochs", type=int, default=config.EPOCHS)
    p.add_argument("--lr", type=float, default=config.LR)
    p.add_argument("--weight-decay", type=float, default=config.WEIGHT_DECAY)
    p.add_argument("--gamma", type=float, default=config.FOCAL_GAMMA)
    p.add_argument("--alpha", type=float, default=config.FOCAL_ALPHA)
    p.add_argument("--device", default="cpu")
    p.add_argument("--num-workers", type=int, default=config.NUM_WORKERS)
    p.add_argument("--max-steps", type=int, default=None,
                   help="Limit batches per epoch (smoke test). None = full pass.")
    args = p.parse_args()

    config.BATCH_SIZE = args.batch_size
    config.NUM_WORKERS = args.num_workers

    # Run directory: runs/{name}_{unix_ts}
    ts = int(time.time())
    run_dir = config.RUNS_DIR / f"{args.run_name}_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "tensorboard").mkdir(parents=True, exist_ok=True)
    print(f"Run dir: {run_dir}")

    stats = load_stats()
    pos_rate = stats.get("pos_rate", 0.5)
    pos_weight = (1.0 - pos_rate) / max(pos_rate, 1e-6)  # weight positives higher

    device = args.device
    model = build_model(device)
    n_params = sum(pp.numel() for pp in model.parameters())
    print(f"Model params: {n_params:,}")

    criterion = FocalLoss(gamma=args.gamma, alpha=args.alpha, pos_weight=pos_weight)
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=args.lr, weight_decay=args.weight_decay
    )
    writer = SummaryWriter(log_dir=str(run_dir / "tensorboard"))
    early_stop = EarlyStopping(patience=config.ES_PATIENCE, monitor=config.ES_MONITOR)

    train_dl = get_dataloader("train", stats, shuffle_files=True)
    val_dl = get_dataloader("val", stats)
    test_dl = get_dataloader("test", stats)

    metrics_history = []
    best_val_auc = -np.inf
    best_state = None

    for epoch in range(1, args.epochs + 1):
        t0 = time.time()
        train_m = train_one_epoch(model, train_dl, optimizer, criterion, device, args.max_steps)
        val_m = evaluate(model, val_dl, device, args.max_steps)
        test_m = evaluate(model, test_dl, device, args.max_steps)
        dt = time.time() - t0

        record = {
            "epoch": epoch,
            "train": train_m,
            "val": val_m,
            "test": test_m,
            "lr": args.lr,
            "seconds": round(dt, 1),
        }
        metrics_history.append(record)

        # TensorBoard
        writer.add_scalar("AUC/train", train_m["auc"], epoch)
        writer.add_scalar("AUC/val", val_m["auc"], epoch)
        writer.add_scalar("AUC/test", test_m["auc"], epoch)
        writer.add_scalar("PR-AUC/train", train_m["prauc"], epoch)
        writer.add_scalar("PR-AUC/val", val_m["prauc"], epoch)
        writer.add_scalar("PR-AUC/test", test_m["prauc"], epoch)
        writer.add_scalar("Loss/train", train_m["loss"], epoch)
        writer.add_scalar("Loss/val", val_m["loss"], epoch)
        writer.add_scalar("Loss/test", test_m["loss"], epoch)

        print(f"Epoch {epoch:02d} [{dt:5.1f}s] "
              f"train(auc={train_m['auc']:.4f},loss={train_m['loss']:.4f}) "
              f"val(auc={val_m['auc']:.4f},prauc={val_m['prauc']:.4f}) "
              f"test(auc={test_m['auc']:.4f},prauc={test_m['prauc']:.4f})")

        if val_m["auc"] > best_val_auc:
            best_val_auc = val_m["auc"]
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
            torch.save(best_state, run_dir / "ctr_best.pt")
            print(f"  -> saved best checkpoint (val_auc={best_val_auc:.4f})")

        if early_stop.step({"epoch": epoch, "val_auc": val_m["auc"]}):
            print(f"Early stopping at epoch {epoch} (no val_auc improvement "
                  f"for {config.ES_PATIENCE} epochs).")
            break

    # Finalize
    with open(run_dir / "metrics.json", "w") as f:
        json.dump(metrics_history, f, indent=2)
    plot_metrics(metrics_history, run_dir)
    writer.close()

    # Best test metrics (reload best checkpoint for honest test report)
    if best_state is not None:
        model.load_state_dict(best_state)
        best_test = evaluate(model, test_dl, device)
        print("\n=== Final summary (best val AUC checkpoint) ===")
        print(f"  best val_auc epoch : {early_stop.best_epoch}")
        print(f"  best val_auc       : {best_val_auc:.4f}")
        print(f"  test auc           : {best_test['auc']:.4f}")
        print(f"  test pr-auc        : {best_test['prauc']:.4f}")
        print(f"  artifacts in       : {run_dir}")


if __name__ == "__main__":
    main()
