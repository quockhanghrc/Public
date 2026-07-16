"""
Evaluation + attribution phase for the NRMS pipeline.

`run_evaluate(state)` loads the best checkpoint saved by `run_train`, runs the
final (return_raw) evaluation on the dev / in-time splits, and computes the
attribution metrics. This phase is CPU-friendly, so `run_nrms_mind.py` runs it
without a GPU. Results are stored back on `state` for the report phase.
"""

import os
import json
from typing import Any, Dict

import torch

from src.train import evaluate, load_checkpoint
from src.attribution import compute_component_attribution, compute_history_attention


def _attribution_scalars(results: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten per-split attribution dicts into JSON-serializable scalars."""
    if not results:
        return {}
    out: Dict[str, Any] = {}
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


def run_evaluate(state) -> Dict[str, Any]:
    """
    Load best checkpoint, run final evaluation + attribution. CPU is fine.

    Returns the `final_metrics` dict (headline numbers) for run_config.json.
    Populates `state.final_eval_results`, `state.attribution_results`,
    `state.final_metrics`.
    """
    args = state.args
    model = state.model
    device = state.device
    criterion = state.criterion

    # Restore the best model (selected on in-time IAUC during training).
    best_path = os.path.join(state.checkpoint_dir, "best_model.pt")
    if os.path.isfile(best_path):
        epoch, metrics = load_checkpoint(model, best_path, device)
        print(f"\nRestored best model from {best_path} (epoch {epoch})")
        state.best_in_time_loss = metrics.get("best_in_time_loss", state.best_in_time_loss)
    else:
        print(f"\n[warn] {best_path} not found; evaluating current model weights.")

    print("\n[6/5] Final evaluation...")
    _, in_time_labels, in_time_scores = evaluate(
        model, state.in_time_val_loader, device, criterion, name="intime", return_raw=True,
    )
    _, out_time_labels, out_time_scores = evaluate(
        model, state.out_of_time_val_loader, device, criterion, name="dev", return_raw=True,
    )
    state.final_eval_results = {
        "intime": {"labels": in_time_labels, "scores": in_time_scores},
        "dev": {"labels": out_time_labels, "scores": out_time_scores},
    }

    # ---- Attribution / interpretability (final eval only, read-only) ----
    attribution_results: Dict[str, Any] = {}
    if args.attribution:
        print("\n[7/5] Computing attribution metrics...")
        requested_splits = [s.strip() for s in args.attribution_splits.split(",") if s.strip()]
        split_loaders = {"dev": state.out_of_time_val_loader, "intime": state.in_time_val_loader}
        for split in requested_splits:
            if split not in split_loaders:
                print(f"  [warn] unknown attribution split '{split}'; skipping "
                      f"(known: {list(split_loaders)})")
                continue
            loader = split_loaders[split]
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
                model, loader, device, state.idx_to_category, args.max_history_len,
            )
            attribution_results[split] = {"component": comp, "history": hist}
            print(f"    separation        = {comp['separation']:.4f}")
            print(f"    user_dominance   = {comp['user_dominance']:.4f}")
            print(f"    recency_bias     = {hist['recency_bias_mean']:.4f}")
            print(f"    category_conc.   = {hist['category_concentration_mean']:.4f}")
            print(f"    active_categories= {hist['active_categories_mean']:.4f}")

    state.attribution_results = attribution_results

    # Persist eval + attribution results to disk so the (separate-process) report
    # phase can reload them. final_eval_results holds raw label/score arrays; the
    # attribution dict holds numpy scalars that don't survive JSON, so we persist
    # the scalars via _attribution_scalars and the raw arrays via NPZ.
    import numpy as np
    eval_arrays = {}
    for split, d in state.final_eval_results.items():
        eval_arrays[f"{split}_labels"] = np.asarray(d["labels"])
        eval_arrays[f"{split}_scores"] = np.asarray(d["scores"])
    np.savez(os.path.join(state.checkpoint_dir, "eval_results.npz"), **eval_arrays)
    # Save attribution scalars (JSON-safe) for the report phase to reload.
    if attribution_results:
        with open(os.path.join(state.checkpoint_dir, "attribution_results.json"), "w") as f:
            json.dump(_attribution_scalars(attribution_results), f, indent=2, default=str)
    print(f"Eval/attribution results persisted to {state.checkpoint_dir}")

    # Headline final metrics (dev generalization numbers).
    out_time_metrics = evaluate(model, state.out_of_time_val_loader, device, criterion, name="dev")
    final_metrics = {
        "best_in_time_loss": state.best_in_time_loss,
        "best_in_time_auc": state.best_auc,
        "final_out_time_iauc": out_time_metrics["dev_impression_auc"],
        "final_out_time_auc": out_time_metrics["dev_auc"],
        "final_out_time_mrr": out_time_metrics["dev_mrr"],
        "final_out_time_ndcg@5": out_time_metrics["dev_ndcg@5"],
        "final_out_time_ndcg@10": out_time_metrics["dev_ndcg@10"],
    }
    state.final_metrics = final_metrics

    print("\n" + "=" * 60)
    print("Evaluation complete!")
    print(f"Best In-Time IAUC:    {state.best_auc:.4f}  (impression-level AUC, model-selection headline)")
    print(f"Final Out-Time IAUC:  {final_metrics['final_out_time_iauc']:.4f}  (generalization)")
    print(f"Final Out-Time AUC:   {final_metrics['final_out_time_auc']:.4f}")
    print(f"Final Out-Time MRR:   {final_metrics['final_out_time_mrr']:.4f}")
    print(f"Final Out-Time nDCG@5:  {final_metrics['final_out_time_ndcg@5']:.4f}")
    print(f"Final Out-Time nDCG@10: {final_metrics['final_out_time_ndcg@10']:.4f}")
    print("=" * 60)

    return final_metrics
