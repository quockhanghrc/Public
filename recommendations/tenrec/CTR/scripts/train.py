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

Commands (run from the CTR/ directory):
  # Full training (10 epochs, full data, 4 dataloader workers)
  python scripts/train.py --run-name exp01 --epochs 10

  # Quick smoke test (1 epoch, 200 batches, single worker)
  python scripts/train.py --run-name smoke --epochs 1 --max-steps 200 --num-workers 0

  # Train on a few steps but evaluate on (near) full val/test data
  # (a value >= steps/epoch means a full pass over that split)
  python scripts/train.py --run-name smoke --epochs 1 --max-steps 200 \
      --eval-steps 100000 --test-steps 100000 --num-workers 0

  # Override hyperparameters
  python scripts/train.py --run-name tune --epochs 10 --batch-size 8192 \
      --lr 3e-4 --gamma 1.5 --alpha 0.5 --weight-decay 1e-4

  # Force CPU / pick device
  python scripts/train.py --run-name exp01 --device cpu

  # Re-split when requested frac ratios differ from the stored split
  python scripts/train.py --run-name exp02 --train-frac 0.6 --val-frac 0.2 \
      --test-frac 0.2 --auto-resplit

Split-ratio behavior (--train-frac / --val-frac / --test-frac):
  The split is computed once by scripts/split_data.py and recorded in
  data/split/stats.json. train.py reads that stored split and will NOT silently
  re-split. If the requested frac ratios differ from the stored ones:
    * with --auto-resplit  -> re-splits (force) and reloads stats, then trains
    * without --auto-resplit -> prints the exact split_data.py command and exits
      with code 1 (so you never train on the wrong split or lie in run_note.json)
  If stats.json is missing, the split is bootstrapped automatically on first run.

Useful flags:
  --run-name      name prefix for the output dir (default: exp)
  --epochs        number of training epochs (default: 10)
  --batch-size    batch size (default: 4096)
  --lr            learning rate (default: 1e-3)
  --weight-decay  AdamW weight decay (default: 1e-5)
  --gamma         focal loss gamma (default: 2.0)
  --alpha         focal loss alpha (default: 0.75)
  --num-workers   dataloader workers (default: 4; use 0 on Windows to avoid spawn issues)
  --max-steps     cap training batches per epoch (default: None = full pass; handy for smoke tests)
  --eval-steps    cap validation batches per eval (default: None = same as --max-steps;
                  set a value >= steps/epoch for a full val pass)
  --test-steps    cap test batches (default: None = same as --eval-steps)
  --device        cpu or cuda (default: cpu)
  --train-frac / --val-frac / --test-frac  split fractions (defaults: 0.8/0.1/0.1)
  --auto-resplit  if requested frac ratios differ from the stored split, re-split
                  automatically instead of erroring out

Outputs land in runs/{run_name}_{timestamp}/ (tensorboard/, metrics.json,
plots/*.png, ctr_best.pt). Launch TensorBoard with:
  tensorboard --logdir runs/{run_name}_{timestamp}/tensorboard
"""

import os

# Silence TensorFlow's noisy CUDA-loader warnings (we train on CPU with
# PyTorch; TF is only pulled in transitively by tensorboard). Must be set
# before tensorboard is imported below.
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")

import argparse
import json
import sys
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
import split_data
from utils import (
    estimate_steps_per_epoch,
    estimate_steps_coverage,
    estimate_epoch_coverage,
    n_train_rows_from_stats,
)


def ensure_split(args) -> dict:
    """Make sure data/split/ matches the requested frac ratios.

    - If stats.json is missing, bootstrap the split from raw parquet.
    - If stored fracs match the requested ones (within 1e-6), return stats (no-op).
    - If they differ:
        * with --auto-resplit: re-split (force) and reload stats.
        * otherwise: print the exact command to run and exit(1) so we never
          silently train on the wrong split (and never lie in run_note.json).

    Returns the (possibly freshly written) stats dict.
    """
    try:
        stats = load_stats()
    except FileNotFoundError:
        # First run: bootstrap the split from raw parquet.
        print("No data/split/stats.json found -> bootstrapping split "
              f"({args.train_frac}/{args.val_frac}/{args.test_frac}) ...")
        return split_data.run_split(
            args.train_frac, args.val_frac, args.test_frac, force=True
        )

    stored = (
        stats.get("train_frac", config.TRAIN_FRAC),
        stats.get("val_frac", config.VAL_FRAC),
        stats.get("test_frac", config.TEST_FRAC),
    )
    requested = (args.train_frac, args.val_frac, args.test_frac)
    if all(abs(s - r) < 1e-6 for s, r in zip(stored, requested)):
        return stats  # no-op: split already matches

    s_str = "/".join(f"{x:g}" for x in stored)
    r_str = "/".join(f"{x:g}" for x in requested)
    if args.auto_resplit:
        print(f"Stored split {s_str} != requested {r_str} -> re-splitting "
              f"(--auto-resplit) ...")
        return split_data.run_split(
            args.train_frac, args.val_frac, args.test_frac, force=True
        )

    cmd = (f"python scripts/split_data.py --train-frac {args.train_frac:g} "
           f"--val-frac {args.val_frac:g} --test-frac {args.test_frac:g} --force")
    print("ERROR: requested split ratios do not match the stored split.")
    print(f"  stored   : {s_str}")
    print(f"  requested: {r_str}")
    print(f"  To apply the new ratios, run:\n    {cmd}")
    print("  (or rerun train.py with --auto-resplit to do it automatically)")
    sys.exit(1)


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


def _print_coverage(split_name, steps, n_rows, batch_size, flag_name):
    """Print how much of a split's data `steps` covers (for eval/test)."""
    if steps is None:
        print(f"  {split_name} eval: full pass ({n_rows:,} rows)")
        return
    cov = estimate_steps_coverage(steps, n_rows, batch_size)
    print(f"  {split_name} eval ({flag_name} {steps:,}): "
          f"covers {cov['percent']:.2f}% of {split_name} data "
          f"({cov['covered_rows']:,} rows)")


def summarize_model_params(model) -> dict:
    """Break model parameters down by top-level layer and their share of total.

    Returns {"total": int, "by_layer": [{"layer", "params", "percent"}, ...]}
    sorted by descending param count.
    """
    groups = {}
    for name, p in model.named_parameters():
        key = name.split(".")[0] if "." in name else name
        groups[key] = groups.get(key, 0) + int(p.numel())
    total = sum(groups.values())
    by_layer = [
        {"layer": k, "params": v, "percent": (v / total * 100.0) if total else 0.0}
        for k, v in sorted(groups.items(), key=lambda kv: -kv[1])
    ]
    return {"total": int(total), "by_layer": by_layer}


def build_run_note(model, args, stats, metrics_history, best_val_auc, best_epoch, best_test):
    """Assemble a self-contained JSON note for the run:
      * model_parameters  -> total + per-layer breakdown (params & %)
      * training_params    -> all hyper-parameters used
      * metrics_history    -> per-epoch train/val/test metrics
      * final_summary      -> best val AUC epoch + train/val/test auc/loss
                              (+ honest test re-eval on the best checkpoint)
    """
    param_summary = summarize_model_params(model)

    pos_rate = stats.get("pos_rate", 0.5)
    training_params = {
        "run_name": args.run_name,
        "epochs": args.epochs,
        "batch_size": args.batch_size,
        "lr": args.lr,
        "weight_decay": args.weight_decay,
        "gamma": args.gamma,
        "alpha": args.alpha,
        "device": args.device,
        "num_workers": args.num_workers,
        "max_steps": args.max_steps,
        "eval_steps": args.eval_steps,
        "test_steps": args.test_steps,
        "train_frac": stats.get("train_frac", args.train_frac),
        "val_frac": stats.get("val_frac", args.val_frac),
        "test_frac": stats.get("test_frac", args.test_frac),
        "pos_rate": pos_rate,
        "pos_weight": (1.0 - pos_rate) / max(pos_rate, 1e-6),
    }

    best_record = None
    if best_epoch and 1 <= best_epoch <= len(metrics_history):
        best_record = metrics_history[best_epoch - 1]

    final_summary = {
        "best_val_auc_epoch": best_epoch,
        "best_val_auc": best_val_auc,
    }
    if best_record is not None:
        for split in ("train", "val", "test"):
            final_summary[split] = {
                k: best_record[split][k] for k in ("auc", "prauc", "loss")
            }
    if best_test is not None:
        final_summary["test_on_best_checkpoint"] = {
            "auc": best_test["auc"], "prauc": best_test["prauc"], "loss": best_test["loss"]
        }

    return {
        "model_parameters": param_summary,
        "training_params": training_params,
        "metrics_history": metrics_history,
        "final_summary": final_summary,
    }


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
                   help="Limit training batches per epoch (smoke test). None = full pass.")
    p.add_argument("--eval-steps", type=int, default=None,
                   help="Limit validation batches per eval. None = same as --max-steps.")
    p.add_argument("--test-steps", type=int, default=None,
                   help="Limit test batches. None = same as --eval-steps.")
    p.add_argument("--auto-resplit", action="store_true",
                   help="If requested frac ratios differ from the stored split, "
                        "re-split automatically instead of erroring out.")
    args = p.parse_args()

    config.BATCH_SIZE = args.batch_size
    config.NUM_WORKERS = args.num_workers

    # Run directory: runs/{name}_{unix_ts}
    ts = int(time.time())
    run_dir = config.RUNS_DIR / f"{args.run_name}_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "tensorboard").mkdir(parents=True, exist_ok=True)
    print(f"Run dir: {run_dir}")

    stats = ensure_split(args)
    pos_rate = stats.get("pos_rate", 0.5)
    pos_weight = (1.0 - pos_rate) / max(pos_rate, 1e-6)  # weight positives higher

    # --- Training-volume estimate (pure arithmetic, no data touched) ---
    n_train = n_train_rows_from_stats(stats, args.train_frac)
    steps_per_epoch = estimate_steps_per_epoch(n_train, args.batch_size)
    print(f"Train rows: {n_train:,} | batch size: {args.batch_size:,} "
          f"| steps/epoch: {steps_per_epoch:,}")
    if args.max_steps is not None:
        cov = estimate_steps_coverage(args.max_steps, n_train, args.batch_size)
        print(f"  --max-steps {args.max_steps:,} covers "
              f"{cov['percent']:.2f}% of train data "
              f"({cov['covered_rows']:,} rows, "
              f"~{cov['full_epochs']:.3f} epochs/full pass)")
    else:
        cov = estimate_epoch_coverage(args.epochs, None, n_train, args.batch_size)
        print(f"  full pass per epoch ({args.epochs} epochs) -> "
              f"{cov['total_steps']:,} total steps")

    # Eval / test step caps are separate from training so you can evaluate
    # on more (or less) data than you trained on.
    eval_steps = args.eval_steps if args.eval_steps is not None else args.max_steps
    test_steps = args.test_steps if args.test_steps is not None else eval_steps
    split_counts = stats.get("split_counts") or {}
    n_val = int(split_counts.get("val", 0))
    n_test = int(split_counts.get("test", 0))
    if n_val:
        _print_coverage("val", eval_steps, n_val, args.batch_size, "--eval-steps")
    if n_test:
        _print_coverage("test", test_steps, n_test, args.batch_size, "--test-steps")
    # ---------------------------------------------------------------------

    device = args.device
    model = build_model(device)
    n_params = sum(pp.numel() for pp in model.parameters())
    print(f"Model params: {n_params:,}")
    param_summary = summarize_model_params(model)
    print("Model param breakdown (by layer):")
    for layer in param_summary["by_layer"]:
        print(f"  {layer['layer']:<14} {layer['params']:>14,}  ({layer['percent']:5.2f}%)")

    criterion = FocalLoss(gamma=args.gamma, alpha=args.alpha, pos_weight=pos_weight)
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=args.lr, weight_decay=args.weight_decay
    )
    writer = SummaryWriter(log_dir=str(run_dir / "tensorboard"))
    early_stop = EarlyStopping(patience=config.ES_PATIENCE, monitor=config.ES_MONITOR)

    val_dl = get_dataloader("val", stats)
    test_dl = get_dataloader("test", stats)

    metrics_history = []
    best_val_auc = -np.inf
    best_state = None

    for epoch in range(1, args.epochs + 1):
        t0 = time.time()
        # Rebuild the train loader each epoch so the file-order shuffle seed
        # varies (config.SEED + epoch) -> different SGD order per pass.
        train_dl = get_dataloader("train", stats, shuffle_files=True, epoch=epoch)
        train_m = train_one_epoch(model, train_dl, optimizer, criterion, device, args.max_steps)
        val_m = evaluate(model, val_dl, device, eval_steps)
        test_m = evaluate(model, test_dl, device, test_steps)
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

    # Best test metrics (reload best checkpoint for honest test report).
    # Reuse test_steps so the final eval matches the per-epoch test coverage.
    best_test = None
    if best_state is not None:
        model.load_state_dict(best_state)
        best_test = evaluate(model, test_dl, device, test_steps)
        print("\n=== Final summary (best val AUC checkpoint) ===")
        print(f"  best val_auc epoch : {early_stop.best_epoch}")
        print(f"  best val_auc       : {best_val_auc:.4f}")
        print(f"  test auc           : {best_test['auc']:.4f}")
        print(f"  test pr-auc        : {best_test['prauc']:.4f}")
        print(f"  artifacts in       : {run_dir}")

    # --- Run note: model params breakdown + training params + metrics ---
    note = build_run_note(
        model=model, args=args, stats=stats,
        metrics_history=metrics_history,
        best_val_auc=best_val_auc,
        best_epoch=early_stop.best_epoch,
        best_test=best_test,
    )
    with open(run_dir / "run_note.json", "w") as f:
        json.dump(note, f, indent=2)
    print(f"  run note           : {run_dir / 'run_note.json'}")
    # -------------------------------------------------------------------


if __name__ == "__main__":
    main()
