"""
Main entry point for NRMS training on the MIND dataset.

Usage:
    # Smoke test (1k impressions, ~1 min on CPU):
    python main.py --epochs 1 --max_train_impressions 1000 --max_dev_impressions 500

    # Small test (10k impressions, ~5 min on CPU):
    python main.py --epochs 3 --max_train_impressions 10000 --max_dev_impressions 2000

    # Full training on CPU:
    python main.py --epochs 5

    # Full training on GPU (omit --max_* to use all data):
    python main.py --epochs 10 --batch_size 512 --embed_dim 300 --use_amp

    # Custom paths:
    python main.py --train_behaviors MINDsmall_train/behaviors.tsv \
                   --dev_behaviors MINDsmall_dev/behaviors.tsv
"""

import argparse
import os
import random
import time
from typing import Dict, List

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

from src.data import collate_fn, eval_collate_fn, prepare_data
from src.model import build_default_nrms
from src.report import generate_report
from src.train import (
    evaluate,
    get_device,
    save_checkpoint,
    train_one_epoch,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Train NRMS on MIND dataset")

    # Data paths
    parser.add_argument("--train_behaviors", type=str,
                        default="MINDsmall_train/behaviors.tsv",
                        help="Path to training behaviors.tsv")
    parser.add_argument("--train_news", type=str,
                        default="MINDsmall_train/news.tsv",
                        help="Path to training news.tsv")
    parser.add_argument("--dev_behaviors", type=str,
                        default="MINDsmall_dev/behaviors.tsv",
                        help="Path to dev behaviors.tsv")
    parser.add_argument("--dev_news", type=str,
                        default="MINDsmall_dev/news.tsv",
                        help="Path to dev news.tsv")

    # Data processing
    parser.add_argument("--max_history_len", type=int, default=30,
                        help="Max clicked news in user history")
    parser.add_argument("--max_title_len", type=int, default=20,
                        help="Max title words")
    parser.add_argument("--neg_samples", type=int, default=10,
                        help="Max negative samples per impression row. Increase for more data.")
    parser.add_argument("--max_train_impressions", type=int, default=None,
                        help="Limit to N impression rows for fast testing")
    parser.add_argument("--max_dev_impressions", type=int, default=None,
                        help="Limit to N dev impression rows for fast eval")
    parser.add_argument("--min_word_freq", type=int, default=2,
                        help="Minimum word frequency in vocabulary")

    # Model hyperparameters
    parser.add_argument("--embed_dim", type=int, default=100,
                        help="Word/news/user embedding dimension")
    parser.add_argument("--num_heads", type=int, default=20,
                        help="Multi-head attention heads (must divide embed_dim)")
    parser.add_argument("--dropout", type=float, default=0.2,
                        help="Dropout rate")

    # Training
    parser.add_argument("--epochs", type=int, default=5,
                        help="Number of training epochs")
    parser.add_argument("--batch_size", type=int, default=128,
                        help="Training batch size")
    parser.add_argument("--eval_batch_size", type=int, default=256,
                        help="Evaluation batch size")
    parser.add_argument("--lr", type=float, default=1e-3,
                        help="Learning rate")
    parser.add_argument("--grad_clip", type=float, default=1.0,
                        help="Gradient clipping norm")
    parser.add_argument("--use_amp", action="store_true",
                        help="Enable mixed precision (GPU only)")

    # Checkpointing
    parser.add_argument("--checkpoint_dir", type=str, default="checkpoints",
                        help="Directory to save checkpoints")
    parser.add_argument("--save_every", type=int, default=1,
                        help="Save checkpoint every N epochs")

    # Reproducibility
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed")

    return parser.parse_args()


def set_seed(seed: int):
    """Set all random seeds for reproducibility."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def main():
    args = parse_args()
    set_seed(args.seed)
    device = get_device()

    print("\n" + "=" * 60)
    print("NRMS Training — MIND Dataset")
    print("=" * 60)
    print(f"Config: embed_dim={args.embed_dim}, num_heads={args.num_heads}, "
          f"batch={args.batch_size}, lr={args.lr}, epochs={args.epochs}")
    print(f"Device: {device}")
    if args.use_amp:
        print("Mixed precision: ENABLED")
    print("=" * 60)

    # Resolve data paths relative to script location
    script_dir = os.path.dirname(os.path.abspath(__file__))

    def resolve(path: str) -> str:
        if os.path.isfile(path):
            return path
        candidate = os.path.join(script_dir, path)
        if os.path.isfile(candidate):
            return candidate
        return path

    train_behaviors = resolve(args.train_behaviors)
    train_news = resolve(args.train_news)
    dev_behaviors = resolve(args.dev_behaviors)
    dev_news = resolve(args.dev_news)

    # ---- Load data ----
    print("\n[1/4] Loading data...")
    train_dataset, dev_dataset, train_eval_dataset, word_vocab, news_title_tokens, num_news = prepare_data(
        train_behaviors_path=train_behaviors,
        train_news_path=train_news,
        dev_behaviors_path=dev_behaviors,
        dev_news_path=dev_news,
        max_history_len=args.max_history_len,
        max_title_len=args.max_title_len,
        neg_samples=args.neg_samples,
        min_word_freq=args.min_word_freq,
        max_train_impressions=args.max_train_impressions,
        max_dev_impressions=args.max_dev_impressions,
        seed=args.seed,
    )

    train_loader = DataLoader(
        train_dataset,
        batch_size=args.batch_size,
        shuffle=True,
        collate_fn=collate_fn,
        num_workers=0,
    )
    # Dev loader uses eval_collate_fn to preserve impression grouping for metrics
    dev_loader = DataLoader(
        dev_dataset,
        batch_size=args.eval_batch_size,
        shuffle=False,
        collate_fn=eval_collate_fn,
        num_workers=0,
    )
    # Train eval loader (no shuffling, eval collate)
    train_eval_loader = DataLoader(
        train_eval_dataset,
        batch_size=args.eval_batch_size,
        shuffle=False,
        collate_fn=eval_collate_fn,
        num_workers=0,
    )

    # ---- Build model ----
    print("\n[2/4] Building NRMS model...")
    model = build_default_nrms(
        vocab_size=len(word_vocab),
        word_embed_dim=args.embed_dim,
        num_heads=args.num_heads,
        max_title_len=args.max_title_len,
        dropout=args.dropout,
    )
    model = model.to(device)
    news_title_tokens = news_title_tokens.to(device)
    model.set_news_title_tokens(news_title_tokens)
    print(f"Model parameters: {sum(p.numel() for p in model.parameters()):,}")

    # ---- Training setup ----
    print("\n[3/4] Setting up training...")
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr)
    scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=3, gamma=0.5)
    criterion = nn.BCEWithLogitsLoss()

    # ---- Train ----
    print("\n[4/4] Training...")
    print("-" * 60)
    best_auc = 0.0

    # Track metrics over epochs for report plots
    epoch_history: Dict[str, List[float]] = {"epoch": []}

    for epoch in range(1, args.epochs + 1):
        epoch_start = time.time()

        train_loss = train_one_epoch(
            model=model,
            loader=train_loader,
            optimizer=optimizer,
            device=device,
            criterion=criterion,
            epoch=epoch,
            grad_clip=args.grad_clip,
            use_amp=args.use_amp and device.type == "cuda",
        )

        scheduler.step()

        # Evaluate on both train and dev
        train_metrics = evaluate(model, train_eval_loader, device, criterion, name="train")
        dev_metrics = evaluate(model, dev_loader, device, criterion, name="dev")
        epoch_time = time.time() - epoch_start

        # Track history
        epoch_history["epoch"].append(epoch)
        for k, v in {**train_metrics, **dev_metrics}.items():
            epoch_history.setdefault(k, []).append(v)

        print(
            f"Epoch {epoch:2d}/{args.epochs} | "
            f"Train Loss: {train_metrics['train_loss']:.4f} | "
            f"Dev Loss: {dev_metrics['dev_loss']:.4f} | "
            f"Train AUC: {train_metrics['train_auc']:.4f} | "
            f"Dev AUC: {dev_metrics['dev_auc']:.4f} | "
            f"Dev IAUC: {dev_metrics['dev_impression_auc']:.4f} | "
            f"Dev MRR: {dev_metrics['dev_mrr']:.4f} | "
            f"Dev n@5: {dev_metrics['dev_ndcg@5']:.4f} | "
            f"Time: {epoch_time:.1f}s"
        )

        # Combine metrics for checkpointing
        all_metrics = {**train_metrics, **dev_metrics}

        # Checkpoint
        if epoch % args.save_every == 0 or dev_metrics["dev_auc"] > best_auc:
            if dev_metrics["dev_auc"] > best_auc:
                best_auc = dev_metrics["dev_auc"]
                best_path = os.path.join(args.checkpoint_dir, "best_model.pt")
                save_checkpoint(model, optimizer, epoch, all_metrics, best_path)

            if epoch % args.save_every == 0:
                ckpt_path = os.path.join(
                    args.checkpoint_dir, f"checkpoint_epoch_{epoch}.pt"
                )
                save_checkpoint(model, optimizer, epoch, all_metrics, ckpt_path)

    # ---- Generate report plots ----
    print("\n[5/4] Generating reports...")
    _, train_labels, train_scores = evaluate(
        model, train_eval_loader, device, criterion, name="train", return_raw=True,
    )
    _, dev_labels, dev_scores = evaluate(
        model, dev_loader, device, criterion, name="dev", return_raw=True,
    )
    final_eval_results = {
        "train": {"labels": train_labels, "scores": train_scores},
        "dev": {"labels": dev_labels, "scores": dev_scores},
    }
    generate_report(epoch_history, final_eval_results, args.checkpoint_dir)

    # ---- Final summary ----
    print("\n" + "=" * 60)
    print("Training complete!")
    print(f"Best Dev AUC:         {best_auc:.4f}")
    print(f"Final Dev MRR:        {dev_metrics['dev_mrr']:.4f}")
    print(f"Final Dev nDCG@5:     {dev_metrics['dev_ndcg@5']:.4f}")
    print(f"Final Dev nDCG@10:    {dev_metrics['dev_ndcg@10']:.4f}")
    print(f"Checkpoints saved to: {os.path.abspath(args.checkpoint_dir)}")
    print("=" * 60)


if __name__ == "__main__":
    main()