"""
Small-scale smoke test for hard-negative retraining of NRMS.

Trains a tiny NRMS with --train_mode listwise_hn (hard negatives mined by the
Dense/MiniLM retriever) on a small slice, then runs the retrieval benchmark to
confirm the pipeline executes end-to-end without OOM. The full run (no caps, GPU)
is intended for a big system.
"""
import os
import sys
import json

import pandas as pd
import torch

sys.path.insert(0, '.')

from src.data import prepare_data  # noqa: E402
from src.model import build_default_nrms  # noqa: E402
from src.train import get_device, save_checkpoint  # noqa: E402
from src.common import prepare_run  # noqa: E402
from src.train_run import run_train  # noqa: E402
from src.retrieval_eval import run_benchmark, results_to_dataframe  # noqa: E402

DATA_TRAIN = 'data/MINDsmall_train'
DATA_DEV = 'data/MINDsmall_dev'
ENTITY_VEC = os.path.join(DATA_TRAIN, 'entity_embedding.vec')

# --- Smoke-test scale (small). Set to None for the full run on a big system. ---
MAX_TRAIN_IMPRESSIONS = 200
MAX_DEV_IMPRESSIONS = 100
EPOCHS = 1
MINE_NUM_HN = 4
K = 50

# Build a minimal argparse-like namespace for prepare_run (reuses the real pipeline).
import argparse
args = argparse.Namespace(
    train_behaviors=os.path.join(DATA_TRAIN, 'behaviors.tsv'),
    train_news=os.path.join(DATA_TRAIN, 'news.tsv'),
    dev_behaviors=os.path.join(DATA_DEV, 'behaviors.tsv'),
    dev_news=os.path.join(DATA_DEV, 'news.tsv'),
    max_history_len=30,
    max_title_len=20,
    min_word_freq=2,
    max_train_impressions=MAX_TRAIN_IMPRESSIONS,
    max_dev_impressions=MAX_DEV_IMPRESSIONS,
    neg_samples=None,
    in_time_val_frac=0.0,
    in_time_val_seed=42,
    train_mode='listwise_hn',
    max_candidates=K,
    mine_num_hn=MINE_NUM_HN,
    mine_model='sentence-transformers/all-MiniLM-L6-v2',
    mine_cache_dir='cache',
    mine_max_news=2000,
    embed_dim=50,
    num_heads=5,
    user_num_heads=5,
    use_hf_embeddings=False,
    freeze_embeddings=False,
    hf_pool='mean',
    hf_cache='cache',
    bottleneck_dim=None,
    dropout=0.2,
    category_mode='none',
    cat_embed_dim=8,
    subcat_embed_dim=8,
    epochs=EPOCHS,
    steps_per_epoch=None,
    batch_size=32,
    eval_batch_size=16,
    lr=5e-4,
    grad_clip=1.0,
    use_amp=False,
    num_workers=0,
    pos_weight=None,
    checkpoint_dir='checkpoints_hn_smoke',
    run_name='hn_smoke',
    save_every=1,
    early_stopping_patience=3,
    early_stopping_min_delta=0.0,
    seed=42,
    attribution=False,
    attribution_splits='dev',
    phase='all',
)

device = get_device()
print("Device:", device)

# prepare_run builds data + model + optimizer (reuses the real pipeline).
state = prepare_run(args)
print(f"Mined training impressions: {len(state.train_loader.dataset)}")

# Train one epoch (CPU, tiny).
run_train(state)
print("Training complete (smoke).")

# Benchmark the trained hard-negative model through the retrieval stage.
results = run_benchmark(
    train_news_path=os.path.join(DATA_TRAIN, 'news.tsv'),
    dev_news_path=os.path.join(DATA_DEV, 'news.tsv'),
    dev_behaviors_path=os.path.join(DATA_DEV, 'behaviors.tsv'),
    entity_vec_path=ENTITY_VEC,
    model=state.model,
    device=device,
    criterion=state.criterion,
    k=K,
    max_impressions=30,
    eval_batch_size=8,
    use_tfidf=True,
    use_dense=True,
    use_entity=True,
    max_news=2000,
)
df = results_to_dataframe(results)
pd.set_option('display.width', 220)
pd.set_option('display.max_columns', 30)
print("\n=== HARD-NEGATIVE SMOKE TEST (max_news=2000, max_impressions=30) ===")
print(df.to_string())
