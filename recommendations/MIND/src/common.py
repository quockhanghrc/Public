"""
Shared setup for the NRMS pipeline.

`prepare_run()` performs the data loading, model building, optional HuggingFace
embedding injection, and optimizer/scheduler construction that is common to the
train, evaluate, and report phases. It returns a `RunState` dataclass so the
individual phase modules (`src/train_run.py`, `src/eval_run.py`,
`src/report_run.py`) can stay small and focused.

This lets `run_nrms_mind.py` run each phase as a separate Modal function with its
own GPU policy (training on GPU, evaluation/report on CPU) while reusing the exact
same setup logic.
"""

import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import torch
import torch.nn as nn
from torch.utils.data import DataLoader

from src.data import collate_fn, impression_collate_fn, prepare_data
from src.model import build_default_nrms
from src.embeddings import load_hf_embeddings, apply_hf_to_model
from src.train import get_device, save_checkpoint


@dataclass
class RunState:
    """Bundled state produced by `prepare_run` and consumed by the phase modules."""

    args: Any
    run_name: str
    checkpoint_dir: str
    device: torch.device
    model: nn.Module
    optimizer: torch.optim.Optimizer
    scheduler: Any
    criterion: nn.Module
    train_loader: DataLoader
    in_time_val_loader: DataLoader
    out_of_time_val_loader: DataLoader
    idx_to_category: Any
    # Populated by the training phase (best-model selection / early stopping).
    best_in_time_loss: float = float("inf")
    epochs_no_improve: int = 0
    best_model_state: Optional[dict] = None
    best_auc: float = 0.0
    # Populated by the evaluate phase, consumed by the report phase.
    final_eval_results: Dict[str, Any] = field(default_factory=dict)
    attribution_results: Dict[str, Any] = field(default_factory=dict)
    # Final headline metrics (filled by evaluate phase for run_config).
    final_metrics: Dict[str, Any] = field(default_factory=dict)


def _resolve_data_path(script_dir: str, path: str) -> str:
    """Resolve a data path against cwd, script dir, data/ subdir, or /data (Modal)."""
    candidates = [
        path,
        os.path.join(script_dir, path),
        os.path.join(script_dir, "data", path),
        os.path.join("/data", path),
        # Modal Volume layout: data was uploaded under a `data/` prefix, so the
        # files actually live at /data/data/<path> (e.g. /data/data/MINDsmall_train).
        os.path.join("/data", "data", path),
    ]
    for cand in candidates:
        if os.path.isfile(cand):
            return cand
    return path


def _fit_heads(requested: int, dim: int) -> int:
    """Largest divisor of `dim` that is <= `requested` (>= 1)."""
    requested = max(1, min(requested, dim))
    while requested > 1 and dim % requested != 0:
        requested -= 1
    return requested


def prepare_run(args) -> RunState:
    """
    Perform all shared setup and return a `RunState`.

    Args:
        args: the parsed argparse Namespace (from main.parse_args()).
    """
    # Auto-resolve DataLoader workers: up to 4 cores (Modal allocates 4 vCPU).
    if args.num_workers is None:
        args.num_workers = min(os.cpu_count() or 1, 4)
    # set_seed is called by the caller (main / phase entrypoint) before this.

    device = get_device()

    print("\n" + "=" * 60)
    print("NRMS — MIND Dataset")
    print("=" * 60)
    print(f"Config: embed_dim={args.embed_dim}, num_heads={args.num_heads}, "
          f"batch={args.batch_size}, lr={args.lr}, epochs={args.epochs}"
          + (f", hf_model={args.embed_model}" if args.use_hf_embeddings else ""))
    print(f"Device: {device}")
    if args.use_amp:
        print("Mixed precision: ENABLED")
    print("=" * 60)

    # ---- Per-run checkpoint folder (never overwrite previous runs) ----
    if args.run_name:
        run_name = args.run_name
    else:
        run_name = f"runs_{int(time.time())}"
    args.checkpoint_dir = os.path.join(args.checkpoint_dir, run_name)
    os.makedirs(args.checkpoint_dir, exist_ok=True)
    print(f"Run folder: {os.path.abspath(args.checkpoint_dir)}")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    # __file__ here is src/common.py, so the project root is one level up.
    project_dir = os.path.dirname(script_dir)

    train_behaviors = _resolve_data_path(project_dir, args.train_behaviors)
    train_news = _resolve_data_path(project_dir, args.train_news)
    dev_behaviors = _resolve_data_path(project_dir, args.dev_behaviors)
    dev_news = _resolve_data_path(project_dir, args.dev_news)

    # ---- Load data ----
    print("\n[1/5] Loading data...")
    (train_dataset, in_time_val_dataset, out_of_time_val_dataset, word_vocab,
     news_title_tokens, num_news, news_id_to_idx, idx_to_category,
     idx_to_subcategory, num_categories, num_subcategories) = prepare_data(
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
        mine_num_hn=args.mine_num_hn,
        mine_model=args.mine_model,
        mine_cache_dir=args.mine_cache_dir,
        mine_max_news=args.mine_max_news,
        seed=args.seed,
    )

    train_loader = DataLoader(
        train_dataset,
        batch_size=args.batch_size,
        shuffle=True,
        collate_fn=impression_collate_fn if args.train_mode in ("listwise", "listwise_hn") else collate_fn,
        num_workers=args.num_workers,
    )
    in_time_val_loader = DataLoader(
        in_time_val_dataset,
        batch_size=args.eval_batch_size,
        shuffle=False,
        collate_fn=impression_collate_fn,
        num_workers=args.num_workers,
    )
    out_of_time_val_loader = DataLoader(
        out_of_time_val_dataset,
        batch_size=args.eval_batch_size,
        shuffle=False,
        collate_fn=impression_collate_fn,
        num_workers=args.num_workers,
    )

    # ---- Optional HuggingFace pretrained embeddings (cache-first) ----
    hf_matrix = None
    hf_dim = None
    if args.use_hf_embeddings:
        print("\n[2/5] Loading HuggingFace pretrained embeddings...")
        hf_matrix, hf_found, hf_dim = load_hf_embeddings(
            word_vocab,
            model_name=args.embed_model,
            cache_dir=args.hf_cache,
            device=str(device),
            pool=args.hf_pool,
        )
        args.embed_dim = hf_dim
        print(f"  Embedding dim auto-set to {args.embed_dim} (from {args.embed_model}).")

    # num_heads must divide the WORKING dim (bottleneck if set, else embed_dim).
    working_dim = args.bottleneck_dim if args.bottleneck_dim else args.embed_dim
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
    if args.category_mode != "none":
        idx_to_category_t = torch.as_tensor(idx_to_category, dtype=torch.long, device=device)
        idx_to_subcategory_t = torch.as_tensor(idx_to_subcategory, dtype=torch.long, device=device)
        model.set_news_category_tokens(idx_to_category_t, idx_to_subcategory_t)
        print(f"  Category mode: {args.category_mode} "
              f"(cat_embed={args.cat_embed_dim}, subcat_embed={args.subcat_embed_dim}, "
              f"news_embed_dim={model.embed_dim})")

    # Parameter breakdown (for run_config.json)
    def _count(mod) -> int:
        return sum(p.numel() for p in mod.parameters())
    total_params = _count(model)
    ne = model.news_encoder
    ue = model.user_encoder
    ne_word_emb = _count(ne.word_embedding)
    ne_blocks = _count(ne.blocks) if hasattr(ne, "blocks") else 0
    ne_pool = _count(ne.attention_pooling)
    ne_other = _count(ne) - ne_word_emb - ne_blocks - ne_pool
    ue_blocks = _count(ue.blocks) if hasattr(ue, "blocks") else 0
    ue_pool = _count(ue.attention_pooling)
    ue_own = ue_blocks + ue_pool
    print(f"Model parameters: {total_params:,}")
    print(f"  News encoder:        {_count(ne):,}  (word_emb={ne_word_emb:,}, "
          f"transformer_blocks={ne_blocks:,}, pooling={ne_pool:,}, other={ne_other:,})")
    print(f"  User encoder (own): {ue_own:,}  (transformer_blocks={ue_blocks:,}, "
          f"pooling={ue_pool:,})  [shares News encoder]")

    # ---- Apply HuggingFace embeddings (after build, before training) ----
    if hf_matrix is not None:
        apply_hf_to_model(model, hf_matrix, device=device, freeze=args.freeze_embeddings)

    # ---- Training setup ----
    print("\n[4/5] Setting up training...")
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=1e-4)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=args.epochs, eta_min=0)
    if args.pos_weight is not None:
        criterion = nn.BCEWithLogitsLoss(
            pos_weight=torch.tensor(float(args.pos_weight), device=device)
        )
        print(f"Using BCE pos_weight={args.pos_weight}")
    else:
        criterion = nn.BCEWithLogitsLoss()

    state = RunState(
        args=args,
        run_name=run_name,
        checkpoint_dir=args.checkpoint_dir,
        device=device,
        model=model,
        optimizer=optimizer,
        scheduler=scheduler,
        criterion=criterion,
        train_loader=train_loader,
        in_time_val_loader=in_time_val_loader,
        out_of_time_val_loader=out_of_time_val_loader,
        idx_to_category=idx_to_category,
    )
    # Stash param-breakdown + hf info on the state for run_config writing.
    state._param_breakdown = {
        "total_params": total_params,
        "news_encoder_total": _count(ne),
        "news_word_emb": ne_word_emb,
        "news_transformer_blocks": ne_blocks,
        "news_pooling": ne_pool,
        "news_other": ne_other,
        "user_encoder_own": ue_own,
        "user_transformer_blocks": ue_blocks,
        "user_pooling": ue_pool,
    }
    state._hf_dim = hf_dim
    return state
