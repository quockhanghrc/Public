"""
Small-scale smoke test for the two-stage retrieval benchmark.

Runs all three retrievers (TF-IDF / Dense / Entity) on a tiny corpus slice
and a handful of dev impressions to verify the pipeline executes end-to-end
without OOM. The full-scale run (full 65k corpus) is intended for a big system.
"""
import os
import sys
import json

import pandas as pd
import torch

sys.path.insert(0, '.')

from src.data import prepare_data  # noqa: E402
from src.model import build_default_nrms  # noqa: E402
from src.train import load_checkpoint, get_device  # noqa: E402
from src.retrieval_eval import run_benchmark, results_to_dataframe  # noqa: E402

DATA_TRAIN = 'data/MINDsmall_train'
DATA_DEV = 'data/MINDsmall_dev'
ENTITY_VEC = os.path.join(DATA_TRAIN, 'entity_embedding.vec')
CKPT = 'checkpoints/runs_1784137259/best_model.pt'
CFG = 'checkpoints/runs_1784137259/run_config.json'

with open(CFG) as f:
    a = json.load(f)['args']
device = get_device()
(train_ds, in_time_ds, dev_ds, vocab, ntt, num_news, n2i, i2c, i2s, nc, ns) = prepare_data(
    train_behaviors_path=os.path.join(DATA_TRAIN, 'behaviors.tsv'),
    train_news_path=os.path.join(DATA_TRAIN, 'news.tsv'),
    dev_behaviors_path=os.path.join(DATA_DEV, 'behaviors.tsv'),
    dev_news_path=os.path.join(DATA_DEV, 'news.tsv'),
    max_history_len=a['max_history_len'],
    max_title_len=a['max_title_len'],
    min_word_freq=a['min_word_freq'],
    max_train_impressions=a['max_train_impressions'],
    max_dev_impressions=a['max_dev_impressions'],
    train_mode=a['train_mode'],
    max_candidates=a['max_candidates'],
    seed=a['seed'],
)
model = build_default_nrms(
    vocab_size=len(vocab),
    word_embed_dim=a['embed_dim'],
    num_heads=a['num_heads'],
    user_num_heads=a['user_num_heads'],
    max_title_len=a['max_title_len'],
    dropout=a['dropout'],
    category_mode=a['category_mode'],
    num_categories=nc,
    num_subcategories=ns,
    cat_embed_dim=a['cat_embed_dim'],
    subcat_embed_dim=a['subcat_embed_dim'],
)
model.set_news_title_tokens(ntt)
load_checkpoint(model, CKPT, device)
model.to(device)
model.eval()
criterion = torch.nn.BCEWithLogitsLoss()

# --- Smoke-test scale: tiny corpus + few impressions ---
MAX_NEWS = 2000
MAX_IMPRESSIONS = 30

results = run_benchmark(
    train_news_path=os.path.join(DATA_TRAIN, 'news.tsv'),
    dev_news_path=os.path.join(DATA_DEV, 'news.tsv'),
    dev_behaviors_path=os.path.join(DATA_DEV, 'behaviors.tsv'),
    entity_vec_path=ENTITY_VEC,
    model=model,
    device=device,
    criterion=criterion,
    k=50,
    max_impressions=MAX_IMPRESSIONS,
    eval_batch_size=8,
    use_tfidf=True,
    use_dense=True,
    use_entity=True,
    max_news=MAX_NEWS,
)
df = results_to_dataframe(results)
pd.set_option('display.width', 220)
pd.set_option('display.max_columns', 30)
print("\n=== SMOKE TEST RESULTS (max_news=%d, max_impressions=%d) ===" % (MAX_NEWS, MAX_IMPRESSIONS))
print(df.to_string())
