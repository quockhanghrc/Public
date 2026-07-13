# Sample Commands — NRMS MIND training

Two entry points:
- **Local** (`main.py`): runs on your machine (CPU or CUDA). Flags use **underscores**
  (`--use_hf_embeddings`, `--bottleneck_dim`).
- **Modal GPU** (`run_nrms_mind.py`): runs on Modal's T4. Flags use **hyphens**
  (`--use-hf-embeddings`, `--bottleneck-dim`) because Modal's CLI auto-converts
  underscores to hyphens. The script forwards them to the same `main.py` inside the
  container, so behavior is identical.

> Secrets live in `secrets_local.py` (git-ignored). Fill it in once before Modal runs.
> Checkpoints + `run_config.json` land in `checkpoints/<run_name>/` (local) or are
> uploaded to the Volume and pulled back with `modal volume get`.

---

## A. Local (`main.py`)

### A1. Baseline — random word embeddings, no bottleneck (fast CPU smoke)
```bash
python main.py --epochs 1 --max_train_impressions 200 --max_dev_impressions 100
```

### A2. Baseline — random embeddings, no bottleneck, full-ish data, 5 epochs
```bash
python main.py --epochs 5 --max_train_impressions 5000 --max_dev_impressions 2000
```

### A3. Projection bottleneck (compress 50-dim → 64-dim), no HF
```bash
python main.py --epochs 3 --bottleneck_dim 64 --max_train_impressions 2000 --max_dev_impressions 1000
```

### A4. HuggingFace embeddings (MiniLM-L6-v2, 384-dim), NO bottleneck
```bash
python main.py --epochs 3 --use_hf_embeddings --max_train_impressions 2000 --max_dev_impressions 1000
```

### A5. HuggingFace embeddings + bottleneck (384 → 64) — recommended for compression
```bash
python main.py --epochs 3 --use_hf_embeddings --bottleneck_dim 64 --max_train_impressions 2000 --max_dev_impressions 1000
```

### A6. With negative sampling (NRMS-standard K=4) to shrink the dataset
```bash
python main.py --epochs 3 --use_hf_embeddings --bottleneck_dim 64 --neg_samples 4 --max_train_impressions 5000
```

### A7. With in-time validation + early stopping (needs --in_time_val_frac > 0)
```bash
python main.py --epochs 20 --use_hf_embeddings --bottleneck_dim 64 --neg_samples 4 \
  --in_time_val_frac 0.1 --early_stopping_patience 3 --early_stopping_min_delta 0.001
```

### A8. Freeze the pretrained embeddings (train only the projection + transformers)
```bash
python main.py --epochs 3 --use_hf_embeddings --bottleneck_dim 64 --freeze_embeddings
```

### A9. Listwise training objective (per-impression softmax) instead of pointwise
```bash
python main.py --epochs 3 --use_hf_embeddings --bottleneck_dim 64 --train_mode listwise
```

---

## B. Modal GPU (`run_nrms_mind.py`)

> Use `--detach` is NOT needed: the script already uses `train.spawn()` so the run
> survives your terminal closing / laptop powering off. Monitor with
> `modal app logs <app-id>` (printed at launch) or `modal app list`.

### B1. Baseline — random embeddings, no bottleneck (quick GPU smoke)
```bash
modal run run_nrms_mind.py --run-name t_baseline --epochs 1 --max-train-impressions 200 --max-dev-impressions 100
```

### B2. Projection bottleneck, no HF
```bash
modal run run_nrms_mind.py --run-name t_bn64 --epochs 3 --bottleneck-dim 64 --max-train-impressions 5000 --max-dev-impressions 2000
```

### B3. HuggingFace embeddings, NO bottleneck
```bash
modal run run_nrms_mind.py --run-name t_hf --epochs 3 --use-hf-embeddings --max-train-impressions 5000 --max-dev-impressions 2000
```

### B4. HuggingFace embeddings + bottleneck (384 → 64) — recommended
```bash
modal run run_nrms_mind.py --run-name t_hf_bn64 --epochs 3 --use-hf-embeddings --bottleneck-dim 64 --neg-samples 4
```

### B5. Full run with in-time validation + early stopping
```bash
modal run run_nrms_mind.py --run-name exp_full --epochs 20 --use-hf-embeddings --bottleneck-dim 64 \
  --neg-samples 4 --in-time-val-frac 0.1 --early-stopping-patience 3
```

### B6. Freeze pretrained embeddings
```bash
modal run run_nrms_mind.py --run-name t_frozen --epochs 3 --use-hf-embeddings --bottleneck-dim 64 --freeze-embeddings
```

---

## C. Pull results back from Modal

```bash
# Create the destination dir FIRST (modal volume get merges into one file otherwise)
mkdir -p downloads
modal volume get nrms-mind-vol /checkpoints/t_hf_bn64 ./downloads
# -> downloads/t_hf_bn64/{best_model.pt, checkpoint_epoch_*.pt, *_curves.png, run_config.json}
```

Inspect `run_config.json` in the run folder — it records every arg (with defaults), the
resolved dims (`resolved_embed_dim`, `resolved_bottleneck_dim`, `resolved_working_dim`,
`resolved_num_heads`), `total_params`, and `final_metrics`.

---

## D. One-time Volume data upload (MINDsmall train/dev)

```bash
modal volume put nrms-mind-vol MINDsmall_train /data/MINDsmall_train
modal volume put nrms-mind-vol MINDsmall_dev   /data/MINDsmall_dev
```

---

## Notes
- Local flags use underscores; Modal flags use hyphens (Modal auto-converts).
- `--neg_samples` / `--neg-samples`: ignored in `listwise` mode (all candidates kept).
- The HF model downloads once into the Volume's `/data/model_cache` and is reused by
  later runs (no re-download).
- `run_config.json` is written twice per run: right after the model is built (survives
  crashes) and again at the end with `final_metrics`.
