"""
Lightning entry point for the Tenrec CTR model.

Wraps CTRModel (scripts/model.py) in a LightningModule and the streaming
CtrIterableDataset (scripts/dataset.py) in a LightningDataModule, driven by
pytorch_lightning.Trainer. Lightning's Trainer natively provides the
continuous step-based training + periodic validation + early stopping +
best-checkpoint behavior you asked for:

  * max_steps          -> train continuously for N steps (or until --epochs cap)
  * val_check_interval -> pause training every X steps for a validation check
  * EarlyStopping      -> stop after P consecutive non-improving val_auc checks
  * ModelCheckpoint    -> save the best checkpoint by val_auc

The original scripts/train.py (manual epoch loop) is kept untouched as a
fallback. Reporting (run_note.json + PNG plots + TensorBoard) reuses the same
helpers as train.py so the two entry points stay consistent.

Run from the CTR/ directory:
  # install once
  d:\Anaconda\python.exe -m pip install -r requirements.txt

  # continuous training: validate every 2000 steps on a 5000-batch val subset,
  # early-stop after 5 non-improving checks, full test each check.
  python scripts/train_lightning.py --run-name lt --num-workers 0 \
      --max-steps 6000 --eval-every 2000 --eval-steps 5000 --patience 5

  # hash item embedding, longer run
  python scripts/train_lightning.py --run-name lt-hash --num-workers 0 \
      --item-embed-method hash --max-steps 20000 --eval-every 2000 \
      --eval-steps 5000 --patience 5
"""

import os

# Silence TensorFlow's noisy CUDA-loader warnings (we train on CPU with
# PyTorch; TF is only pulled in transitively by tensorboard). Must be set
# before tensorboard is imported.
os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "3")

import argparse
import json
import time
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn

try:
    import pytorch_lightning as pl
    from pytorch_lightning.callbacks import EarlyStopping, ModelCheckpoint
    from pytorch_lightning.loggers import TensorBoardLogger
except ImportError:
    raise SystemExit(
        "PyTorch Lightning is not installed. Run: "
        "d:\\Anaconda\\python.exe -m pip install -r requirements.txt"
    )

import config
from dataset import load_stats, get_dataloader
from model import CTRModel

# Reuse the manual-loop helpers (FocalLoss, StreamingMetrics, reporting) so the
# two entry points stay consistent and don't drift.
from train import (
    FocalLoss,
    StreamingMetrics,
    ensure_split,
    plot_metrics,
    build_run_note,
)


class CTRModule(pl.LightningModule):
    """Lightning wrapper around CTRModel.

    Training uses FocalLoss (+ optional DIEN aux loss). Validation/test compute
    a stable AUC/PR-AUC via the same histogram-binning StreamingMetrics used by
    train.py, so the reported numbers are directly comparable.
    """

    def __init__(self, item_embed_method, lr, weight_decay, gamma, alpha,
                 pos_weight, aux_loss_weight, eval_steps=None, test_steps=None,
                 run_dir=None):
        super().__init__()
        # config.AUX_LOSS_WEIGHT gates the aux GRU encoder inside CTRModel, so it
        # must be set BEFORE constructing the model.
        config.AUX_LOSS_WEIGHT = aux_loss_weight
        self.model = CTRModel(item_embed_method=item_embed_method)
        self.criterion = FocalLoss(gamma=gamma, alpha=alpha, pos_weight=pos_weight)
        self.save_hyperparameters(ignore=["run_dir"])
        self.run_dir = run_dir

        # Streaming AUC accumulators (reset per window/check).
        self.train_sm = StreamingMetrics()
        self.val_sm = StreamingMetrics()

        self._check_idx = 0
        self.best_val_auc = -np.inf
        self.best_epoch = 0
        self.best_test = None
        self.metrics_history = []
        self._epoch_t0 = time.time()

        # Injected by main(): a callable that returns a fresh test DataLoader.
        self.test_dataloader_fn = None

    def forward(self, batch, return_aux=False):
        return self.model(batch, return_aux=return_aux)

    def training_step(self, batch, idx):
        labels = batch["click"].to(self.device)
        feats = {k: v.to(self.device) for k, v in batch.items() if k != "click"}
        out = self(feats, return_aux=True)
        logits = out[0] if isinstance(out, tuple) else out
        loss = self.criterion(logits, labels)
        if isinstance(out, tuple) and len(out) > 1 and out[1] is not None:
            loss = loss + self.hparams.aux_loss_weight * out[1]
        self.train_sm.update(
            logits.detach().cpu().numpy(), labels.detach().cpu().numpy(),
            float(loss.item()),
        )
        self.log("train_loss", loss, prog_bar=True, on_step=True, on_epoch=True)
        return loss

    def on_train_epoch_start(self):
        self._epoch_t0 = time.time()

    def on_validation_epoch_start(self):
        self.val_sm = StreamingMetrics()

    def validation_step(self, batch, idx):
        labels = batch["click"].to(self.device)
        feats = {k: v.to(self.device) for k, v in batch.items() if k != "click"}
        logits = self(feats)
        loss = nn.functional.binary_cross_entropy_with_logits(logits, labels)
        self.val_sm.update(
            logits.detach().cpu().numpy(), labels.detach().cpu().numpy(),
            float(loss.item()),
        )
        self.log("val_loss", loss, prog_bar=True, on_step=False, on_epoch=True)

    def on_validation_epoch_end(self):
        val_m = self.val_sm.compute()
        self.log("val_auc", val_m["auc"], prog_bar=True, on_step=False, on_epoch=True)
        self.log("val_prauc", val_m["prauc"], on_step=False, on_epoch=True)

        # Honest test pass on the CURRENT model. When this is the best check,
        # the current weights ARE the best checkpoint, so this is the honest
        # test-on-best evaluation. Capped by --test-steps for cost control.
        test_m = self._run_test()

        self._check_idx += 1
        train_m = self.train_sm.compute()
        self.train_sm = StreamingMetrics()  # reset window for the next check

        record = {
            "epoch": self._check_idx,
            "train": train_m,
            "val": val_m,
            "test": test_m,
            "lr": self.hparams.lr,
            "seconds": round(time.time() - self._epoch_t0, 1),
        }
        self.metrics_history.append(record)

        improved = val_m["auc"] > self.best_val_auc
        if improved:
            self.best_val_auc = val_m["auc"]
            self.best_epoch = self._check_idx
            self.best_test = test_m
            if self.run_dir is not None:
                state = {k: v.detach().cpu().clone()
                         for k, v in self.model.state_dict().items()}
                torch.save(state, self.run_dir / "ctr_best.pt")
            self.print(f"  -> saved best checkpoint (val_auc={self.best_val_auc:.4f})")

        self.print(
            f"Check {self._check_idx:02d} "
            f"train(auc={train_m['auc']:.4f},loss={train_m['loss']:.4f}) "
            f"val(auc={val_m['auc']:.4f},prauc={val_m['prauc']:.4f}) "
            f"test(auc={test_m['auc']:.4f},prauc={test_m['prauc']:.4f})"
        )

    @torch.no_grad()
    def _run_test(self):
        self.model.eval()
        sm = StreamingMetrics()
        loss_fn = nn.BCEWithLogitsLoss()
        limit = self.hparams.test_steps
        dl = self.test_dataloader_fn() if self.test_dataloader_fn is not None \
            else None
        if dl is None:
            return sm.compute()
        for i, batch in enumerate(dl):
            if limit is not None and i >= limit:
                break
            labels = batch["click"].to(self.device)
            feats = {k: v.to(self.device) for k, v in batch.items() if k != "click"}
            logits = self(feats)
            loss = loss_fn(logits, labels)
            sm.update(logits.detach().cpu().numpy(), labels.detach().cpu().numpy(),
                      float(loss.item()))
        return sm.compute()

    def configure_optimizers(self):
        return torch.optim.AdamW(
            self.model.parameters(),
            lr=self.hparams.lr,
            weight_decay=self.hparams.weight_decay,
        )


class CTRDataModule(pl.LightningDataModule):
    """Streaming train/val/test loaders for the CTR pipeline.

    CtrIterableDataset already yields full batches (DataLoader batch_size=None),
    so Lightning receives pre-batched tensors. The train loader reshuffles its
    file order per epoch (epoch=self.trainer.current_epoch), matching the
    original train.py SGD-order behavior.
    """

    def __init__(self, batch_size, num_workers, eval_steps=None, test_steps=None):
        super().__init__()
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.eval_steps = eval_steps
        self.test_steps = test_steps
        self.stats = load_stats()

    def train_dataloader(self):
        epoch = self.trainer.current_epoch if self.trainer is not None else 0
        return get_dataloader("train", self.stats, shuffle_files=True, epoch=epoch)

    def val_dataloader(self):
        return get_dataloader("val", self.stats)

    def test_dataloader(self):
        return get_dataloader("test", self.stats)


def main():
    p = argparse.ArgumentParser(description="Train Tenrec CTR model (PyTorch Lightning).")
    p.add_argument("--run-name", default=config.DEFAULT_RUN_NAME)
    p.add_argument("--train-frac", type=float, default=config.TRAIN_FRAC)
    p.add_argument("--val-frac", type=float, default=config.VAL_FRAC)
    p.add_argument("--test-frac", type=float, default=config.TEST_FRAC)
    p.add_argument("--batch-size", type=int, default=config.BATCH_SIZE)
    p.add_argument("--lr", type=float, default=config.LR)
    p.add_argument("--weight-decay", type=float, default=config.WEIGHT_DECAY)
    p.add_argument("--gamma", type=float, default=config.FOCAL_GAMMA)
    p.add_argument("--alpha", type=float, default=config.FOCAL_ALPHA)
    p.add_argument("--device", default="cpu")
    p.add_argument("--num-workers", type=int, default=0,
                   help="Dataloader workers. Default 0: on CPU/Windows, >0 uses "
                        "spawn which re-imports the module graph per worker and "
                        "stalls (and can crash). 0 is fastest for this pipeline.")
    p.add_argument("--item-embed-method", choices=("standard", "hash", "qr"),
                   default=config.ITEM_EMBED_METHOD,
                   help="Item embedding: 'standard' = nn.Embedding(ITEM_CARD,16) "
                        "(full table, ~62M params); 'hash' = memory-bounded "
                        "HashEmbedding (ITEM_HASH_BUCKETS rows, 64-dim rep).")
    p.add_argument("--aux-loss-weight", type=float, default=config.AUX_LOSS_WEIGHT,
                   help="DIEN-style next-item auxiliary loss weight. 0 = disabled.")
    # Lightning-specific continuous-training controls
    p.add_argument("--max-steps", type=int, default=None,
                   help="Max training steps (continuous). None -> capped by --epochs.")
    p.add_argument("--eval-every", type=int, default=2000,
                   help="Validation check every N training steps (val_check_interval).")
    p.add_argument("--eval-steps", type=int, default=None,
                   help="Cap validation batches per check. None = full val pass.")
    p.add_argument("--test-steps", type=int, default=None,
                   help="Cap test batches per check. None = full test pass.")
    p.add_argument("--patience", type=int, default=5,
                   help="EarlyStopping patience: consecutive non-improving val_auc checks.")
    p.add_argument("--epochs", type=int, default=config.EPOCHS,
                   help="Hard safety cap on epochs (max_epochs) when --max-steps is None.")
    p.add_argument("--auto-resplit", action="store_true",
                   help="If requested frac ratios differ from the stored split, "
                        "re-split automatically instead of erroring out.")
    args = p.parse_args()

    # Must be set BEFORE building the model / dataloaders (dataset + model read
    # these at construction time).
    config.BATCH_SIZE = args.batch_size
    config.NUM_WORKERS = args.num_workers
    config.AUX_LOSS_WEIGHT = args.aux_loss_weight

    ts = int(time.time())
    logger = TensorBoardLogger(
        save_dir=str(config.RUNS_DIR), name=f"{args.run_name}_{ts}", version=""
    )
    run_dir = Path(logger.log_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    print(f"Run dir: {run_dir}")

    stats = ensure_split(args)
    pos_rate = stats.get("pos_rate", 0.5)
    pos_weight = (1.0 - pos_rate) / max(pos_rate, 1e-6)  # weight positives higher

    model = CTRModule(
        item_embed_method=args.item_embed_method,
        lr=args.lr,
        weight_decay=args.weight_decay,
        gamma=args.gamma,
        alpha=args.alpha,
        pos_weight=pos_weight,
        aux_loss_weight=args.aux_loss_weight,
        eval_steps=args.eval_steps,
        test_steps=args.test_steps,
        run_dir=run_dir,
    )
    model.run_dir = run_dir
    dm = CTRDataModule(args.batch_size, args.num_workers, args.eval_steps, args.test_steps)
    # Inject a fresh-test-loader factory so the module can run a test pass each
    # validation check (LightningModule has no test_dataloader of its own).
    model.test_dataloader_fn = dm.test_dataloader

    early_stop = EarlyStopping(monitor="val_auc", mode="max",
                               patience=args.patience, verbose=True)
    ckpt = ModelCheckpoint(dirpath=str(run_dir), filename="ctr_best",
                           monitor="val_auc", mode="max", save_top_k=1,
                           auto_insert_metric_name=False)
    if args.max_steps is None:
        max_steps, max_epochs = -1, args.epochs
    else:
        max_steps, max_epochs = args.max_steps, -1

    trainer = pl.Trainer(
        max_steps=max_steps,
        max_epochs=max_epochs,
        val_check_interval=args.eval_every,
        limit_val_batches=args.eval_steps if args.eval_steps is not None else 1.0,
        callbacks=[early_stop, ckpt],
        logger=logger,
        accelerator=args.device,
        devices=1,
        num_sanity_val_steps=0,
        default_root_dir=str(run_dir),
        enable_progress_bar=True,
    )

    trainer.fit(model, dm)

    # Finalize reporting (reuse the manual-loop helpers for a consistent schema).
    with open(run_dir / "metrics.json", "w") as f:
        json.dump(model.metrics_history, f, indent=2)
    plot_metrics(model.metrics_history, run_dir)

    note = build_run_note(
        model=model.model,
        args=args,
        stats=stats,
        metrics_history=model.metrics_history,
        best_val_auc=model.best_val_auc,
        best_epoch=model.best_epoch,
        best_test=model.best_test,
    )
    with open(run_dir / "run_note.json", "w") as f:
        json.dump(note, f, indent=2)
    print(f"  run note           : {run_dir / 'run_note.json'}")
    print(f"  artifacts in       : {run_dir}")


if __name__ == "__main__":
    main()
