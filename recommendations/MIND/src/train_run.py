"""
Training phase for the NRMS pipeline.

`run_train(state)` executes the epoch loop, per-epoch evaluation for early
stopping / best-model selection, and checkpoint saving. This is the ONLY phase
that needs a GPU, so `run_nrms_mind.py` runs it via `with_options(gpu="L4")`.
"""

import json
import os
import time
from typing import Any, Dict, List

import torch

from src.train import evaluate, save_checkpoint, train_one_epoch


def _write_run_config(state, final_metrics=None, attribution_metrics=None):
    """Write run_config.json capturing args + resolved dims + param counts."""
    args = state.args
    working_dim = args.bottleneck_dim if args.bottleneck_dim else args.embed_dim
    cfg = {
        "run_name": state.run_name,
        "run_folder": os.path.abspath(state.checkpoint_dir),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "device": str(state.device),
        "args": vars(args),
        "resolved_embed_dim": args.embed_dim,
        "resolved_bottleneck_dim": args.bottleneck_dim,
        "resolved_working_dim": working_dim,
        "resolved_num_heads": args.num_heads,
        "resolved_user_num_heads": args.user_num_heads,
        "hf_model": args.embed_model if args.use_hf_embeddings else None,
        "hf_dim": state._hf_dim if args.use_hf_embeddings else None,
        "total_params": state._param_breakdown["total_params"],
        "param_breakdown": state._param_breakdown,
    }
    if final_metrics is not None:
        cfg["final_metrics"] = final_metrics
    if attribution_metrics is not None:
        cfg["attribution_metrics"] = attribution_metrics
    cfg_path = os.path.join(state.checkpoint_dir, "run_config.json")
    with open(cfg_path, "w") as f:
        json.dump(cfg, f, indent=2, default=str)
    print(f"Run config written to {cfg_path}")


def run_train(state) -> None:
    """Run the training loop and save checkpoints. GPU is expected (caller's job)."""
    args = state.args
    model = state.model
    device = state.device
    criterion = state.criterion

    # Write run_config.json up front so it exists even if training crashes later.
    _write_run_config(state)

    best_auc = 0.0
    best_in_time_loss = float("inf")
    epochs_no_improve = 0
    best_model_state = None

    epoch_history: Dict[str, List[float]] = {"epoch": []}

    print("\n[5/5] Training...")
    print("-" * 60)
    for epoch in range(1, args.epochs + 1):
        epoch_start = time.time()

        train_loss = train_one_epoch(
            model=model,
            loader=state.train_loader,
            optimizer=state.optimizer,
            device=device,
            criterion=criterion,
            epoch=epoch,
            grad_clip=args.grad_clip,
            use_amp=args.use_amp and device.type == "cuda",
            max_steps=args.steps_per_epoch,
            train_mode=args.train_mode,
        )

        state.scheduler.step()

        in_time_metrics = evaluate(model, state.in_time_val_loader, device, criterion, name="intime")
        out_time_metrics = evaluate(model, state.out_of_time_val_loader, device, criterion, name="dev")
        epoch_time = time.time() - epoch_start

        epoch_history["epoch"].append(epoch)
        for k, v in {**in_time_metrics, **out_time_metrics}.items():
            epoch_history.setdefault(k, []).append(v)

        print(
            f"Epoch {epoch:2d}/{args.epochs} | "
            f"Train Loss: {train_loss:.4f} | "
            f"InTime IAUC: {in_time_metrics['intime_impression_auc']:.4f} | "
            f"InTime Loss: {in_time_metrics['intime_loss']:.4f} | "
            f"OutTime IAUC: {out_time_metrics['dev_impression_auc']:.4f} | "
            f"OutTime AUC: {out_time_metrics['dev_auc']:.4f} | "
            f"OutTime MRR: {out_time_metrics['dev_mrr']:.4f} | "
            f"Time: {epoch_time:.1f}s"
        )

        all_metrics = {**in_time_metrics, **out_time_metrics}

        if epoch % args.save_every == 0 or in_time_metrics["intime_impression_auc"] > best_auc:
            if in_time_metrics["intime_impression_auc"] > best_auc:
                best_auc = in_time_metrics["intime_impression_auc"]
                best_path = os.path.join(state.checkpoint_dir, "best_model.pt")
                save_checkpoint(model, state.optimizer, epoch, all_metrics, best_path)

            if epoch % args.save_every == 0:
                ckpt_path = os.path.join(state.checkpoint_dir, f"checkpoint_epoch_{epoch}.pt")
                save_checkpoint(model, state.optimizer, epoch, all_metrics, ckpt_path)

        if in_time_metrics["intime_loss"] < best_in_time_loss - args.early_stopping_min_delta:
            best_in_time_loss = in_time_metrics["intime_loss"]
            epochs_no_improve = 0
            best_model_state = model.state_dict()
        else:
            epochs_no_improve += 1
        if epochs_no_improve >= args.early_stopping_patience:
            print(f"Early stopping at epoch {epoch}")
            break

    # Persist best-model selection state so the evaluate phase can restore it.
    state.best_auc = best_auc
    state.best_in_time_loss = best_in_time_loss
    state.best_model_state = best_model_state
    state.epochs_no_improve = epochs_no_improve
    state._epoch_history = epoch_history

    # Save a small JSON of training summary for the report/evaluate phases.
    summary = {
        "best_auc": best_auc,
        "best_in_time_loss": best_in_time_loss,
        "epoch_history": epoch_history,
    }
    with open(os.path.join(state.checkpoint_dir, "train_summary.json"), "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"Training summary written to {os.path.join(state.checkpoint_dir, 'train_summary.json')}")
