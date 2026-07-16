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

    # Early stopping (stop if validation loss doesn't improve):
    python main.py --epochs 20 --early_stopping_patience 5 --early_stopping_min_delta 0.001
"""

import argparse
import json
import os
import random
import time
from typing import Dict, List

# Suppress the HuggingFace tokenizers "forked after parallelism" warning. The HF
# tokenizer is used ONCE, upfront, in load_hf_embeddings() to build the embedding
# matrix -- BEFORE the training loop forks DataLoader workers (num_workers > 0).
# The forked workers only use the simple whitespace vocab tokenizer, never the HF
# one, so disabling tokenizers' internal thread pool is safe and removes the
# deadlock warning noise (Linux/Modal fork start method only).
os.environ["TOKENIZERS_PARALLELISM"] = "false"

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader

from src.data import collate_fn, eval_collate_fn, impression_collate_fn, prepare_data
from src.model import build_default_nrms
from src.embeddings import load_hf_embeddings, apply_hf_to_model
from src.report import generate_report
from src.attribution import compute_component_attribution, compute_history_attention
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
    parser.add_argument("--max_train_impressions", type=int, default=None,
                        help="Limit to N impression rows for fast testing")
    parser.add_argument("--max_dev_impressions", type=int, default=None,
                        help="Limit to N dev impression rows for fast eval")
    parser.add_argument("--min_word_freq", type=int, default=2,
                        help="Minimum word frequency in vocabulary")
    parser.add_argument("--neg_samples", type=int, default=None,
                        help="Negatives sampled per positive in TRAIN only "
                             "(NRMS/MIND protocol, K=4). None = keep all candidates "
                             "(no sampling, current default).")
    parser.add_argument("--train_mode", type=str, default="listwise",
                        choices=["pointwise", "listwise", "listwise_hn"],
                        help="pointwise = current (BCE, one candidate at a time); "
                             "listwise = per-impression ranking loss (NRMS softmax-CE); "
                             "listwise_hn = listwise with HARD NEGATIVES mined by a "
                             "Dense/MiniLM retriever (industry-aligned retraining).")
    parser.add_argument("--max_candidates", type=int, default=50,
                        help="Max candidates per impression (listwise mode; truncate/pad).")
    parser.add_argument("--mine_num_hn", type=int, default=4,
                        help="Hard negatives mined per impression when "
                             "--train_mode listwise_hn (default 4, NRMS/MIND K).")
    parser.add_argument("--mine_model", type=str,
                        default="sentence-transformers/all-MiniLM-L6-v2",
                        help="HuggingFace model used to mine hard negatives in "
                             "listwise_hn mode (DenseRetriever).")
    parser.add_argument("--mine_cache_dir", type=str, default="cache",
                        help="Cache folder for the hard-negative mining model.")
    parser.add_argument("--mine_max_news", type=int, default=None,
                        help="Cap the hard-negative mining corpus size (smoke tests). "
                             "None = use all news (full run).")
    parser.add_argument("--in_time_val_frac", type=float, default=0.0,
                        help="Fraction of train USERS held out as in-time validation "
                             "(user-disjoint from training). 0.0 = no in-time split "
                             "(current dev set is the only validation).")
    parser.add_argument("--in_time_val_seed", type=int, default=None,
                        help="Seed for the user-disjoint in-time split. None = use --seed.")

    # Model hyperparameters
    parser.add_argument("--embed_dim", type=int, default=50,
                        help="Word/news/user embedding dimension (no bottleneck). "
                             "Overridden automatically when --use_hf_embeddings is set "
                             "(taken from the HF model's hidden size).")
    parser.add_argument("--num_heads", type=int, default=5,
                        help="Multi-head attention heads for the NEWS encoder "
                             "(working_dim / num_heads; 50/5 = 10 dims/head).")
    parser.add_argument("--bottleneck_dim", type=int, default=None,
                        help="If set, compress word_embed_dim -> this dim via a linear "
                             "projection BEFORE the transformer blocks (forces the "
                             "attention/FFN to do compression work). None = no bottleneck.")
    parser.add_argument("--user_num_heads", type=int, default=5,
                        help="Multi-head attention heads for the USER encoder "
                             "(embed_dim / num_heads; 50/5 = 10 dims/head). "
                             "Capped internally to >= ~6 dims/head.")
    parser.add_argument("--use_hf_embeddings", action="store_true",
                        help="Initialize word embeddings from a HuggingFace model "
                             "(--embed_model). Embedding dim is auto-detected.")
    parser.add_argument("--embed_model", type=str, default="sentence-transformers/all-MiniLM-L6-v2",
                        help="HuggingFace model id for --use_hf_embeddings. Swap freely, "
                             "e.g. bert-base-uncased, roberta-base, distilbert-base-uncased, "
                             "google/electra-base-discriminator, "
                             "sentence-transformers/all-MiniLM-L6-v2.")
    parser.add_argument("--freeze_embeddings", action="store_true",
                        help="Freeze word embeddings (pretrained only, no fine-tune).")
    parser.add_argument("--hf_cache", type=str, default="cache",
                        help="Folder for HuggingFace model cache (cache-first).")
    parser.add_argument("--hf_pool", type=str, default="mean",
                        choices=["mean", "first"],
                        help="How to pool subword pieces of a multi-piece word.")
    parser.add_argument("--dropout", type=float, default=0.2,
                        help="Dropout rate")
    parser.add_argument("--category_mode", type=str, default="none",
                        choices=["none", "concat", "cross"],
                        help="How to use MIND category/subcategory. 'none' = ignore "
                             "(default, backward compatible). 'concat' = Option 1: append "
                             "small category+subcategory embeddings to each title-word vector. "
                             "'cross' = Option 2: category embedding is a query that "
                             "cross-attends over the title words.")
    parser.add_argument("--cat_embed_dim", type=int, default=8,
                        help="Dimension of the category embedding (concat/cross modes).")
    parser.add_argument("--subcat_embed_dim", type=int, default=8,
                        help="Dimension of the subcategory embedding (concat/cross modes).")

    # Training
    parser.add_argument("--epochs", type=int, default=5,
                        help="Number of training epochs")
    parser.add_argument("--steps_per_epoch", type=int, default=None,
                        help="Batches trained per epoch. None = use all data in the "
                             "loader (default). If set, train that many steps then move "
                             "to the next epoch (loader reshuffles each epoch).")
    parser.add_argument("--batch_size", type=int, default=128,
                        help="Training batch size")
    parser.add_argument("--eval_batch_size", type=int, default=256,
                        help="Evaluation batch size")
    parser.add_argument("--lr", type=float, default=5e-4,
                        help="Learning rate")
    parser.add_argument("--grad_clip", type=float, default=1.0,
                        help="Gradient clipping norm")
    parser.add_argument("--use_amp", action="store_true",
                        help="Enable mixed precision (GPU only)")
    parser.add_argument("--num_workers", type=int, default=None,
                        help="DataLoader worker processes for parallel data loading/"
                             "collation. None = auto = min(os.cpu_count(), 4). More "
                             "workers use more CPU cores + RAM to prefetch/collate and "
                             "feed the GPU faster. Safe on Windows (main.py uses the "
                             "if __name__ == '__main__' guard).")
    parser.add_argument("--pos_weight", type=float, default=None,
                        help="BCE pos_weight to counter class imbalance "
                             "(e.g. neg/pos ratio). None = disabled (current default).")

    # Checkpointing
    parser.add_argument("--checkpoint_dir", type=str, default="checkpoints",
                        help="Base directory under which per-run folders are created")
    parser.add_argument("--run_name", type=str, default=None,
                        help="Name for this run's subfolder (default: runs_<unixtimestamp>). "
                             "Each run gets its own folder so previous files are never overwritten.")
    parser.add_argument("--save_every", type=int, default=1,
                        help="Save checkpoint every N epochs")

    # Early stopping
    parser.add_argument("--early_stopping_patience", type=int, default=3,
                        help="Number of epochs to wait for improvement before early stopping")
    parser.add_argument("--early_stopping_min_delta", type=float, default=0.0,
                        help="Minimum change in validation loss to qualify as an improvement")

    # Reproducibility
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed")

    # Attribution / interpretability (computed at FINAL eval only, read-only)
    parser.add_argument("--attribution", dest="attribution", action="store_true",
                        default=True,
                        help="Compute component-attribution + history-attention metrics "
                             "at final evaluation (default: on).")
    parser.add_argument("--no_attribution", dest="attribution", action="store_false",
                        help="Disable attribution metrics at final evaluation.")
    parser.add_argument("--attribution_splits", type=str, default="dev,intime",
                        help="Comma-separated eval splits to run attribution on "
                             "(must match loader names: dev, intime). Default: dev,intime.")

    # Pipeline phase (used by run_nrms_mind.py to run each part with its own GPU
    # policy; "all" keeps the original single-process behavior for local runs).
    parser.add_argument("--phase", type=str, default="all",
                        choices=["all", "train", "eval", "report"],
                        help="Which pipeline phase to run. 'all' = train+eval+report "
                             "in one process (default, backward compatible). "
                             "'train' / 'eval' / 'report' run a single phase "
                             "(used by the Modal split-run entrypoint).")

    return parser.parse_args()


def set_seed(seed: int):
    """Set all random seeds for reproducibility."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def main():
    """Thin CLI wrapper around the split pipeline phases.

    `--phase all` (default) reproduces the original single-process behavior
    (train + eval + report). `--phase train|eval|report` runs a single phase,
    which is what `run_nrms_mind.py` uses to assign a GPU only to training.
    """
    args = parse_args()
    if args.num_workers is None:
        args.num_workers = min(os.cpu_count() or 1, 4)
    set_seed(args.seed)

    from src.common import prepare_run
    from src.train_run import run_train
    from src.eval_run import run_evaluate
    from src.report_run import run_report

    state = prepare_run(args)

    if args.phase in ("all", "train"):
        run_train(state)
    if args.phase in ("all", "eval"):
        run_evaluate(state)
    if args.phase in ("all", "report"):
        run_report(state)

    if args.phase == "all":
        print("\n" + "=" * 60)
        print("Training complete!")
        print(f"Checkpoints saved to: {os.path.abspath(args.checkpoint_dir)}")
        print("=" * 60)


if __name__ == "__main__":
    main()
