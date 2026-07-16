"""
Reporting phase for the NRMS pipeline.

`run_report(state)` generates the report plots (learning curves, ROC, score
distributions, attribution plots) from the evaluation + attribution results
produced by `run_evaluate`. This is matplotlib-only work, so it runs on CPU.
"""

import os
from typing import Any

from src.report import generate_report


def run_report(state) -> None:
    """Generate report plots from persisted eval/attribution results.

    The report phase runs in a SEPARATE process (Modal CPU phase), so it cannot
    read the in-memory `state.final_eval_results` / `state.attribution_results`
    populated by the eval phase. Instead it reloads them from disk:
      - eval_results.npz        (label/score arrays per split)
      - attribution_results.json (JSON-safe attribution scalars)
      - train_summary.json      (epoch history written by the train phase)
    """
    import json
    import numpy as np

    ckpt = state.checkpoint_dir
    print("\n[8/5] Generating reports...")

    # Reload epoch history from the train phase's summary.
    epoch_history = {"epoch": []}
    summary_path = os.path.join(ckpt, "train_summary.json")
    if os.path.isfile(summary_path):
        with open(summary_path) as f:
            epoch_history = json.load(f).get("epoch_history", {"epoch": []})

    # Reload final eval label/score arrays.
    final_eval_results: Dict[str, Any] = {}
    npz_path = os.path.join(ckpt, "eval_results.npz")
    if os.path.isfile(npz_path):
        data = np.load(npz_path)
        splits = {k.rsplit("_", 1)[0] for k in data.files if k.endswith("_labels")}
        for split in splits:
            final_eval_results[split] = {
                "labels": data[f"{split}_labels"],
                "scores": data[f"{split}_scores"],
            }
    else:
        print(f"  [warn] {npz_path} not found; ROC/score plots will be skipped.")

    # Reload attribution scalars (the raw attribution dict is not JSON-serializable,
    # so the report phase only gets the scalars — sufficient for the bar plots).
    attribution_results: Dict[str, Any] = {}
    attr_path = os.path.join(ckpt, "attribution_results.json")
    if os.path.isfile(attr_path):
        with open(attr_path) as f:
            scalars = json.load(f)
        # Re-hydrate into the {split: {"component": {...}, "history": {...}}} shape
        # that generate_report expects (only the scalar fields are present).
        for split, s in scalars.items():
            attribution_results[split] = {
                "component": {
                    "separation": s["separation"],
                    "user_dominance": s["user_dominance"],
                },
                "history": {
                    "recency_bias_mean": s["recency_bias_mean"],
                    "category_concentration_mean": s["category_concentration_mean"],
                    "active_categories_mean": s["active_categories_mean"],
                },
            }

    # Stash the reloaded results back on state so the run_config update below
    # (and any future in-process consumer) sees them.
    state.final_eval_results = final_eval_results
    state.attribution_results = attribution_results

    generate_report(
        epoch_history,
        final_eval_results,
        ckpt,
        attribution_results=attribution_results if attribution_results else None,
        attribution_model=state.model if attribution_results else None,
        attribution_loader=state.out_of_time_val_loader if attribution_results else None,
    )

    # Re-write run_config.json with final + attribution metrics appended.
    cfg_path = os.path.join(state.checkpoint_dir, "run_config.json")
    if os.path.isfile(cfg_path):
        import json
        with open(cfg_path) as f:
            cfg = json.load(f)
        cfg["final_metrics"] = state.final_metrics
        # Flatten attribution scalars.
        if state.attribution_results:
            out = {}
            for split, d in state.attribution_results.items():
                comp = d.get("component", {})
                hist = d.get("history", {})
                out[split] = {
                    "separation": float(comp.get("separation", float("nan"))),
                    "user_dominance": float(comp.get("user_dominance", float("nan"))),
                    "recency_bias_mean": float(hist.get("recency_bias_mean", float("nan"))),
                    "category_concentration_mean": float(hist.get("category_concentration_mean", float("nan"))),
                    "active_categories_mean": float(hist.get("active_categories_mean", float("nan"))),
                }
            cfg["attribution_metrics"] = out
        with open(cfg_path, "w") as f:
            json.dump(cfg, f, indent=2, default=str)
        print(f"Run config updated at {cfg_path}")

    print("--- Reports Complete ---")
