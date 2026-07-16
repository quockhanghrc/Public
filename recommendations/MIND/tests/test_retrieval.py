"""
Smoke test for the retrieval stage (src/retrieval.py) + end-to-end reranking.

Validates:
  - Each retriever builds an index over the full corpus and returns <= K unique ids.
  - EntityRetriever loads entity_embedding.vec and produces non-trivial vectors.
  - The end-to-end path (retrieval -> NRMS rerank via src/train.py::evaluate)
    runs without error and returns the expected metric keys, using a real checkpoint.

NOTE: We do NOT assert Recall@K > 0 on a tiny dev slice. MIND impressions are
randomly-sampled candidate sets, so content-based retrieval from history has a low
ceiling on small slices; recall is a benchmark quantity, not a unit-test invariant.

Run:
  python -m pytest tests/test_retrieval.py -q
  # or directly:
  python tests/test_retrieval.py
"""

import os
import sys

import numpy as np
import pandas as pd
import torch

# Make repo root importable when run directly.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from src.data import load_behaviors, load_news  # noqa: E402
from src.retrieval import (  # noqa: E402
    DenseRetriever,
    EntityRetriever,
    TfidfRetriever,
)
from src.retrieval_eval import run_benchmark, results_to_dataframe  # noqa: E402
from src.train import load_checkpoint, get_device  # noqa: E402
from src.model import build_default_nrms  # noqa: E402


HERE = os.path.dirname(os.path.abspath(__file__))
DATA_TRAIN = os.path.join(ROOT, "data", "MINDsmall_train")
DATA_DEV = os.path.join(ROOT, "data", "MINDsmall_dev")
ENTITY_VEC = os.path.join(DATA_TRAIN, "entity_embedding.vec")
CHECKPOINT = os.path.join(ROOT, "checkpoints", "runs_1784137259", "best_model.pt")
RUN_CONFIG = os.path.join(ROOT, "checkpoints", "runs_1784137259", "run_config.json")


def _load_corpus():
    train_news = load_news(os.path.join(DATA_TRAIN, "news.tsv"))
    dev_news = load_news(os.path.join(DATA_DEV, "news.tsv"))
    all_news = pd.concat([train_news, dev_news], ignore_index=True).drop_duplicates(
        subset=["news_id"]
    ).reset_index(drop=True)
    return all_news


def test_tfidf_returns_k():
    news = _load_corpus()
    r = TfidfRetriever()
    r.build_index(news)
    hist = news["title"].fillna("").head(3).tolist()
    out = r.retrieve(hist, k=10)
    assert len(out) <= 10
    assert all(isinstance(x, str) for x in out)
    assert len(set(out)) == len(out), "retrieved ids should be unique"


def test_entity_retriever_loads_and_retrieves():
    news = _load_corpus()
    r = EntityRetriever(entity_vec_path=ENTITY_VEC)
    r.build_index(news)
    nonzero = (np.linalg.norm(r.corpus_matrix, axis=1) > 0).sum()
    assert nonzero > 0, "expected some news with entity vectors"
    sample_id = str(news["news_id"].iloc[0])
    q = r.history_entity_vector([sample_id])
    assert q.shape[0] == r.dim
    out = r.retrieve_entity_impression(
        history_entity_vec=q,
        clicked_news_ids=[],
        shown_news_ids=news["news_id"].astype(str).head(5).tolist(),
        shown_labels=[1, 0, 0, 0, 0],
        k=10,
    )
    assert len(out["candidates"]) <= 10


def _load_model():
    import json
    from src.data import prepare_data
    with open(RUN_CONFIG) as f:
        cfg = json.load(f)
    a = cfg["args"]
    device = get_device()
    # Build the model through the real data pipeline so vocab size, embed dim, and
    # the news_title_tokens buffer all match the checkpoint's state_dict.
    (train_ds, in_time_ds, dev_ds, vocab, news_title_tokens, num_news,
     news_id_to_idx, idx_to_cat, idx_to_sub, num_cat, num_sub) = prepare_data(
        train_behaviors_path=os.path.join(DATA_TRAIN, "behaviors.tsv"),
        train_news_path=os.path.join(DATA_TRAIN, "news.tsv"),
        dev_behaviors_path=os.path.join(DATA_DEV, "behaviors.tsv"),
        dev_news_path=os.path.join(DATA_DEV, "news.tsv"),
        max_history_len=a["max_history_len"],
        max_title_len=a["max_title_len"],
        min_word_freq=a["min_word_freq"],
        max_train_impressions=a["max_train_impressions"],
        max_dev_impressions=a["max_dev_impressions"],
        train_mode=a["train_mode"],
        max_candidates=a["max_candidates"],
        seed=a["seed"],
    )
    model = build_default_nrms(
        vocab_size=len(vocab),
        word_embed_dim=a["embed_dim"],
        num_heads=a["num_heads"],
        user_num_heads=a["user_num_heads"],
        max_title_len=a["max_title_len"],
        dropout=a["dropout"],
        category_mode=a["category_mode"],
        num_categories=num_cat,
        num_subcategories=num_sub,
        cat_embed_dim=a["cat_embed_dim"],
        subcat_embed_dim=a["subcat_embed_dim"],
    )
    model.set_news_title_tokens(news_title_tokens)
    load_checkpoint(model, CHECKPOINT, device)
    model.to(device)
    model.eval()
    return model, device


def test_end_to_end_benchmark_runs():
    """Full retrieval -> NRMS rerank path runs and returns metric keys."""
    if not (os.path.isfile(CHECKPOINT) and os.path.isfile(RUN_CONFIG)):
        import pytest
        pytest.skip("trained checkpoint not found")
    model, device = _load_model()
    criterion = torch.nn.BCEWithLogitsLoss()

    # Use TF-IDF only for a fast smoke (dense needs transformers; entity is cheap).
    results = run_benchmark(
        train_news_path=os.path.join(DATA_TRAIN, "news.tsv"),
        dev_news_path=os.path.join(DATA_DEV, "news.tsv"),
        dev_behaviors_path=os.path.join(DATA_DEV, "behaviors.tsv"),
        entity_vec_path=ENTITY_VEC,
        model=model,
        device=device,
        criterion=criterion,
        k=20,
        max_impressions=30,
        use_tfidf=True,
        use_dense=False,
        use_entity=False,
    )
    assert len(results) == 1
    res = results[0]
    assert res.method == "tfidf"
    for key in ("retrieved_auc", "retrieved_impression_auc",
                "retrieved_mrr", "retrieved_ndcg@5", "retrieved_ndcg@10"):
        assert key in res.endtoend, f"missing metric {key}"
    df = results_to_dataframe(results)
    assert "recall@k" in df.columns


if __name__ == "__main__":
    test_tfidf_returns_k()
    test_entity_retriever_loads_and_retrieves()
    test_end_to_end_benchmark_runs()
    print("All retrieval smoke tests passed.")
