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
                        choices=["pointwise", "listwise"],
                        help="pointwise = current (BCE, one candidate at a time); "
                             "listwise = per-impression ranking loss (NRMS softmax-CE).")
    parser.add_argument("--max_candidates", type=int, default=50,
                        help="Max candidates per impression (listwise mode; truncate/pad).")
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
    # Auto-resolve DataLoader workers: use up to 4 cores (Modal allocates 4 vCPU;
    # caps RAM pressure on smaller local laptops). Safe on Windows via __main__ guard.
    if args.num_workers is None:
        args.num_workers = min(os.cpu_count() or 1, 4)
    set_seed(args.seed)
    device = get_device()

    print("\n" + "=" * 60)
    print("NRMS Training — MIND Dataset")
    print("=" * 60)
    print(f"Config: embed_dim={args.embed_dim}, num_heads={args.num_heads}, "
          f"batch={args.batch_size}, lr={args.lr}, epochs={args.epochs}"
          + (f", hf_model={args.embed_model}" if args.use_hf_embeddings else ""))
    print(f"Device: {device}")
    if args.use_amp:
        print("Mixed precision: ENABLED")
    print("=" * 60)

    # ---- Per-run checkpoint folder (never overwrite previous runs) ----
    # Each run gets its own timestamped subfolder under --checkpoint_dir so that
    # checkpoints and report artifacts from earlier runs are preserved.
    if args.run_name:
        run_name = args.run_name
    else:
        run_name = f"runs_{int(time.time())}"
    args.checkpoint_dir = os.path.join(args.checkpoint_dir, run_name)
    os.makedirs(args.checkpoint_dir, exist_ok=True)
    print(f"Run folder: {os.path.abspath(args.checkpoint_dir)}")

    # Resolve data paths relative to script location.
    # Tries, in order:
    #   1. the path as given (relative to cwd)
    #   2. <script_dir>/<path>            (e.g. MIND/MINDsmall_train/...)
    #   3. <script_dir>/data/<path>       (local layout: MIND/data/MINDsmall_train/...)
    #   4. /data/<path>                   (Modal Volume mount: /data/MINDsmall_train/...)
    # This keeps the documented defaults (MINDsmall_train/...) valid for both the
    # local folder (data/MINDsmall_train) and the Modal GPU run (/data/MINDsmall_train).
    script_dir = os.path.dirname(os.path.abspath(__file__))

    def resolve(path: str) -> str:
        candidates = [
            path,
            os.path.join(script_dir, path),
            os.path.join(script_dir, "data", path),
            os.path.join("/data", path),
        ]
        for cand in candidates:
            if os.path.isfile(cand):
                return cand
        return path

    train_behaviors = resolve(args.train_behaviors)
    train_news = resolve(args.train_news)
    dev_behaviors = resolve(args.dev_behaviors)
    dev_news = resolve(args.dev_news)

    # ---- Load data ----
    print("\n[1/5] Loading data...")
    train_dataset, in_time_val_dataset, out_of_time_val_dataset, word_vocab, news_title_tokens, num_news, news_id_to_idx, idx_to_category, idx_to_subcategory, num_categories, num_subcategories = prepare_data(
        train_behaviors_path=train_behaviors,
        train_news_path=train_news,
        dev_behaviors_path=dev_behaviors,
        dev_news_path=dev_news,
        max_history_len=args.max_history_len,
        max_title_len=args.max_title_len,
        min_word_freq=args.min_word_freq,
        max_train_impressions=args.max_train_impressions,
        max_dev_impressions=args.max_dev_impressions,
        neg_samples=args.neg_samples,
        in_time_val_frac=args.in_time_val_frac,
        in_time_val_seed=args.in_time_val_seed,
        train_mode=args.train_mode,
        max_candidates=args.max_candidates,
        seed=args.seed,
    )

    train_loader = DataLoader(
        train_dataset,
        batch_size=args.batch_size,
        shuffle=True,
        collate_fn=impression_collate_fn if args.train_mode == "listwise" else collate_fn,
        num_workers=args.num_workers,
    )
    # In-time validation loader (user-disjoint from train; per-impression collate)
    in_time_val_loader = DataLoader(
        in_time_val_dataset,
        batch_size=args.eval_batch_size,
        shuffle=False,
        collate_fn=impression_collate_fn,
        num_workers=args.num_workers,
    )
    # Out-of-time (dev) loader uses per-impression collate for vectorized scoring
    out_of_time_val_loader = DataLoader(
        out_of_time_val_dataset,
        batch_size=args.eval_batch_size,
        shuffle=False,
        collate_fn=impression_collate_fn,
        num_workers=args.num_workers,
    )

    # ---- Optional HuggingFace pretrained embeddings (cache-first) ----
    # Load BEFORE building the model so the embedding dim is auto-detected.
    hf_matrix = None
    if args.use_hf_embeddings:
        print("\n[2/5] Loading HuggingFace pretrained embeddings...")
        hf_matrix, hf_found, hf_dim = load_hf_embeddings(
            word_vocab,
            model_name=args.embed_model,
            cache_dir=args.hf_cache,
            device=str(device),
            pool=args.hf_pool,
        )
        # Auto-override the embedding dim to match the HF model
        args.embed_dim = hf_dim
        print(f"  Embedding dim auto-set to {args.embed_dim} (from {args.embed_model}).")

    # num_heads must divide the WORKING dim (bottleneck if set, else embed_dim),
    # since that is the dimension the transformer blocks actually operate on.
    # Pick the largest divisor of the working dim that is <= the requested heads.
    def _fit_heads(requested: int, dim: int) -> int:
        requested = max(1, min(requested, dim))
        while requested > 1 and dim % requested != 0:
            requested -= 1
        return requested
    working_dim = args.bottleneck_dim if args.bottleneck_dim else args.embed_dim
    # When category_mode is enabled, the category+subcategory signal is concatenated
    # to the working dim before the transformer blocks, so heads must divide the
    # AUGMENTED dim (not just the word/working dim).
    if args.category_mode != "none":
        working_dim = working_dim + args.cat_embed_dim + args.subcat_embed_dim
    new_nh = _fit_heads(args.num_heads, working_dim)
    new_unh = _fit_heads(args.user_num_heads, working_dim)
    if new_nh != args.num_heads or new_unh != args.user_num_heads:
        print(f"  Heads adjusted to divide working_dim={working_dim}: "
              f"news {args.num_heads}->{new_nh}, user {args.user_num_heads}->{new_unh}")
    args.num_heads = new_nh
    args.user_num_heads = new_unh

    # ---- Build model ----
    print("\n[3/5] Building NRMS model...")
    model = build_default_nrms(
        vocab_size=len(word_vocab),
        word_embed_dim=args.embed_dim,
        num_heads=args.num_heads,
        user_num_heads=args.user_num_heads,
        max_title_len=args.max_title_len,
        dropout=args.dropout,
        bottleneck_dim=args.bottleneck_dim,
        category_mode=args.category_mode,
        num_categories=num_categories,
        num_subcategories=num_subcategories,
        cat_embed_dim=args.cat_embed_dim,
        subcat_embed_dim=args.subcat_embed_dim,
    )
    model = model.to(device)
    news_title_tokens = news_title_tokens.to(device)
    model.set_news_title_tokens(news_title_tokens)
    # Category / subcategory buffers (only used when category_mode != "none")
    if args.category_mode != "none":
        idx_to_category_t = torch.as_tensor(idx_to_category, dtype=torch.long, device=device)
        idx_to_subcategory_t = torch.as_tensor(idx_to_subcategory, dtype=torch.long, device=device)
        model.set_news_category_tokens(idx_to_category_t, idx_to_subcategory_t)
        print(f"  Category mode: {args.category_mode} "
              f"(cat_embed={args.cat_embed_dim}, subcat_embed={args.subcat_embed_dim}, "
              f"news_embed_dim={model.embed_dim})")

    # Parameter breakdown by component
    def _count(mod) -> int:
        return sum(p.numel() for p in mod.parameters())
    total_params = _count(model)
    ne = model.news_encoder
    ue = model.user_encoder
    # News encoder sub-breakdown
    ne_word_emb = _count(ne.word_embedding)
    ne_blocks = _count(ne.blocks) if hasattr(ne, "blocks") else 0
    ne_pool = _count(ne.attention_pooling)
    ne_other = _count(ne) - ne_word_emb - ne_blocks - ne_pool
    # User encoder: count ONLY its own layers (exclude the shared news_encoder, which
    # is already reported above and is de-duplicated in model.parameters()).
    ue_blocks = _count(ue.blocks) if hasattr(ue, "blocks") else 0
    ue_pool = _count(ue.attention_pooling)
    ue_own = ue_blocks + ue_pool
    print(f"Model parameters: {total_params:,}")
    print(f"  News encoder:        {_count(ne):,}  (word_emb={ne_word_emb:,}, "
          f"transformer_blocks={ne_blocks:,}, "
          f"pooling={ne_pool:,}, other={ne_other:,})")
    print(f"  User encoder (own): {ue_own:,}  (transformer_blocks={ue_blocks:,}, "
          f"pooling={ue_pool:,})  [shares News encoder]")

    # ---- Attribution scalars extractor (for run_config.json) ----
    # Flattens the per-split attribution dicts into JSON-serializable scalars
    # (arrays are dropped; only the population-average numbers are persisted).
    def _attribution_scalars(results: Dict[str, object]) -> Dict[str, object]:
        if not results:
            return {}
        out: Dict[str, object] = {}
        for split, d in results.items():
            comp = d.get("component", {})
            hist = d.get("history", {})
            out[split] = {
                "separation": float(comp.get("separation", float("nan"))),
                "user_dominance": float(comp.get("user_dominance", float("nan"))),
                "recency_bias_mean": float(hist.get("recency_bias_mean", float("nan"))),
                "category_concentration_mean": float(hist.get("category_concentration_mean", float("nan"))),
                "active_categories_mean": float(hist.get("active_categories_mean", float("nan"))),
            }
        return out

    # ---- Write run_config.json (captures ALL args + resolved dims + param counts) ----
    # Written right after build so it exists even if training crashes later.
    def _write_run_config(final_metrics=None, attribution_metrics=None):
        working_dim = args.bottleneck_dim if args.bottleneck_dim else args.embed_dim
        cfg = {
            "run_name": run_name,
            "run_folder": os.path.abspath(args.checkpoint_dir),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "device": str(device),
            # All CLI args (includes defaults for anything not passed)
            "args": vars(args),
            # Resolved/derived values (what the model actually used)
            "resolved_embed_dim": args.embed_dim,
            "resolved_bottleneck_dim": args.bottleneck_dim,
            "resolved_working_dim": working_dim,
            "resolved_num_heads": args.num_heads,
            "resolved_user_num_heads": args.user_num_heads,
            "hf_model": args.embed_model if args.use_hf_embeddings else None,
            "hf_dim": hf_dim if args.use_hf_embeddings else None,
            "total_params": total_params,
            "param_breakdown": {
                "news_encoder_total": _count(ne),
                "news_word_emb": ne_word_emb,
                "news_transformer_blocks": ne_blocks,
                "news_pooling": ne_pool,
                "news_other": ne_other,
                "user_encoder_own": ue_own,
                "user_transformer_blocks": ue_blocks,
                "user_pooling": ue_pool,
            },
        }
        if final_metrics is not None:
            cfg["final_metrics"] = final_metrics
        if attribution_metrics is not None:
            cfg["attribution_metrics"] = attribution_metrics
        cfg_path = os.path.join(args.checkpoint_dir, "run_config.json")
        with open(cfg_path, "w") as f:
            json.dump(cfg, f, indent=2, default=str)
        print(f"Run config written to {cfg_path}")

    _write_run_config()

    # ---- Apply HuggingFace embeddings (after build, before training) ----
    if hf_matrix is not None:
        apply_hf_to_model(
            model,
            hf_matrix,
            device=device,
            freeze=args.freeze_embeddings,
        )

    # ---- Training setup ----
    print("\n[4/5] Setting up training...")
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=1e-4)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=args.epochs, eta_min=0)
    # Class weighting: pos_weight=None -> standard BCE (default). Otherwise counter
    # the click imbalance (e.g. neg/pos ratio). Applied to training loss only.
    if args.pos_weight is not None:
        criterion = nn.BCEWithLogitsLoss(
            pos_weight=torch.tensor(float(args.pos_weight), device=device)
        )
        print(f"Using BCE pos_weight={args.pos_weight}")
    else:
        criterion = nn.BCEWithLogitsLoss()

    # Early stopping initialization (keyed to in-time validation loss)
    best_in_time_loss = float('inf')
    epochs_no_improve = 0
    best_model_state = None

    # ---- Train ----
    print("\n[5/5] Training...")
    print("-" * 60)
    # Headline metric is impression-level AUC (MIND leaderboard standard).
    # best_auc tracks IN-TIME impression_auc (user-disjoint held-out); out-of-time
    # (dev) is reported as the final generalization number.
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
            max_steps=args.steps_per_epoch,
            train_mode=args.train_mode,
        )

        scheduler.step()

        # Evaluate on in-time (user-disjoint) and out-of-time (dev) validation
        in_time_metrics = evaluate(model, in_time_val_loader, device, criterion, name="intime")
        out_time_metrics = evaluate(model, out_of_time_val_loader, device, criterion, name="dev")
        epoch_time = time.time() - epoch_start

        # Track history
        epoch_history["epoch"].append(epoch)
        for k, v in {**in_time_metrics, **out_time_metrics}.items():
            epoch_history.setdefault(k, []).append(v)

        print(
            f"Epoch {epoch:2d}/{args.epochs} | "
            f"Train Loss: {train_loss:.4f} | "
            f"InTime IAUC: {in_time_metrics['intime_impression_auc']:.4f} | "  # headline
            f"InTime Loss: {in_time_metrics['intime_loss']:.4f} | "
            f"OutTime IAUC: {out_time_metrics['dev_impression_auc']:.4f} | "
            f"OutTime AUC: {out_time_metrics['dev_auc']:.4f} | "
            f"OutTime MRR: {out_time_metrics['dev_mrr']:.4f} | "
            f"Time: {epoch_time:.1f}s"
        )

        # Combine metrics for checkpointing
        all_metrics = {**in_time_metrics, **out_time_metrics}

        # Checkpoint — best model selected on IN-TIME impression-level AUC
        if epoch % args.save_every == 0 or in_time_metrics["intime_impression_auc"] > best_auc:
            if in_time_metrics["intime_impression_auc"] > best_auc:
                best_auc = in_time_metrics["intime_impression_auc"]
                best_path = os.path.join(args.checkpoint_dir, "best_model.pt")
                save_checkpoint(model, optimizer, epoch, all_metrics, best_path)

            if epoch % args.save_every == 0:
                ckpt_path = os.path.join(
                    args.checkpoint_dir, f"checkpoint_epoch_{epoch}.pt"
                )
                save_checkpoint(model, optimizer, epoch, all_metrics, ckpt_path)

        # Early stopping on in-time validation loss
        if in_time_metrics["intime_loss"] < best_in_time_loss - args.early_stopping_min_delta:
            best_in_time_loss = in_time_metrics["intime_loss"]
            epochs_no_improve = 0
            best_model_state = model.state_dict()
        else:
            epochs_no_improve += 1
        if epochs_no_improve >= args.early_stopping_patience:
            print(f"Early stopping at epoch {epoch}")
            break

    # Restore best model for final evaluation and reporting
    if best_model_state is not None:
        model.load_state_dict(best_model_state)
        print(f"\nRestored best model from epoch {epoch - epochs_no_improve} with in-time loss: {best_in_time_loss:.4f}")

    # ---- Generate report plots ----
    print("\n[6/5] Generating reports...")
    _, in_time_labels, in_time_scores = evaluate(
        model, in_time_val_loader, device, criterion, name="intime", return_raw=True,
    )
    _, out_time_labels, out_time_scores = evaluate(
        model, out_of_time_val_loader, device, criterion, name="dev", return_raw=True,
    )
    final_eval_results = {
        "intime": {"labels": in_time_labels, "scores": in_time_scores},
        "dev": {"labels": out_time_labels, "scores": out_time_scores},
    }

    # ---- Attribution / interpretability (final eval only, read-only) ----
    attribution_results: Dict[str, object] = {}
    if args.attribution:
        print("\n[7/5] Computing attribution metrics...")
        requested_splits = [s.strip() for s in args.attribution_splits.split(",") if s.strip()]
        split_loaders = {"dev": out_of_time_val_loader, "intime": in_time_val_loader}
        for split in requested_splits:
            if split not in split_loaders:
                print(f"  [warn] unknown attribution split '{split}'; skipping "
                      f"(known: {list(split_loaders)})")
                continue
            loader = split_loaders[split]
            # Skip empty splits (e.g. intime when --in_time_val_frac is 0) to
            # avoid meaningless 0.0 / NaN scalars.
            try:
                loader_len = len(loader)
            except TypeError:
                loader_len = None
            if loader_len == 0:
                print(f"  [{split}] skipped (no samples in this split)")
                continue
            print(f"  [{split}] component attribution...")
            comp = compute_component_attribution(model, loader, device)
            print(f"  [{split}] history attention...")
            hist = compute_history_attention(
                model, loader, device, idx_to_category, args.max_history_len,
            )
            attribution_results[split] = {"component": comp, "history": hist}
            # Print a concise summary block (arrays omitted; scalars only).
            print(f"    separation        = {comp['separation']:.4f}")
            print(f"    user_dominance   = {comp['user_dominance']:.4f}")
            print(f"    recency_bias     = {hist['recency_bias_mean']:.4f}")
            print(f"    category_conc.   = {hist['category_concentration_mean']:.4f}")
            print(f"    active_categories= {hist['active_categories_mean']:.4f}")

    generate_report(
        epoch_history, final_eval_results, args.checkpoint_dir,
        attribution_results=attribution_results if attribution_results else None,
        attribution_model=model if attribution_results else None,
        attribution_loader=out_of_time_val_loader if attribution_results else None,
    )

    # ---- Final summary ----
    print("\n" + "=" * 60)
    print("Training complete!")
    if best_model_state is not None:
        print(f"Best In-Time Loss:    {best_in_time_loss:.4f} (epoch {epoch - epochs_no_improve})")
    print(f"Best In-Time IAUC:    {best_auc:.4f}  (impression-level AUC, model-selection headline)")
    print(f"Final Out-Time IAUC:  {out_time_metrics['dev_impression_auc']:.4f}  (generalization)")
    print(f"Final Out-Time AUC:   {out_time_metrics['dev_auc']:.4f}  (global, secondary)")
    print(f"Final Out-Time MRR:   {out_time_metrics['dev_mrr']:.4f}")
    print(f"Final Out-Time nDCG@5:  {out_time_metrics['dev_ndcg@5']:.4f}")
    print(f"Final Out-Time nDCG@10: {out_time_metrics['dev_ndcg@10']:.4f}")
    print(f"Checkpoints saved to: {os.path.abspath(args.checkpoint_dir)}")
    print("=" * 60)

    # Re-write run_config.json with final metrics appended.
    _write_run_config(
        final_metrics={
            "best_in_time_loss": best_in_time_loss if best_model_state is not None else None,
            "best_in_time_auc": best_auc,
            "final_out_time_iauc": out_time_metrics["dev_impression_auc"],
            "final_out_time_auc": out_time_metrics["dev_auc"],
            "final_out_time_mrr": out_time_metrics["dev_mrr"],
            "final_out_time_ndcg@5": out_time_metrics["dev_ndcg@5"],
            "final_out_time_ndcg@10": out_time_metrics["dev_ndcg@10"],
        },
        attribution_metrics=_attribution_scalars(attribution_results),
    )


if __name__ == "__main__":
    main()