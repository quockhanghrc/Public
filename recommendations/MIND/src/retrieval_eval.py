"""
Benchmark harness: retrieval stage -> NRMS reranking (end-to-end).

For each retriever (TF-IDF / Dense / Entity) we:
  1. Index the FULL news corpus (train+dev combined so dev news are retrievable).
  2. For every dev impression, build a retrieved candidate set from the user's
     clicked HISTORY (optionally force-including the clicked positives).
  3. Score the retrieved set with the trained NRMS reranker via the EXISTING
     src/train.py::evaluate (impression AUC / MRR / nDCG@5 / nDCG@10).
  4. Also compute standalone retrieval quality: Recall@K and hit-rate
     (fraction of impressions with >=1 clicked news in the retrieved top-K).

Reuses src/train.py::evaluate and src/data.py helpers — no duplicated metrics.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
from torch.utils.data import DataLoader

from src.data import (
    build_eval_impression_samples,
    impression_collate_fn,
    load_behaviors,
    load_news,
)
from src.retrieval import (
    DenseRetriever,
    EntityRetriever,
    Retriever,
    TfidfRetriever,
)
from src.train import evaluate


# ---------------------------------------------------------------------------
# Impression parsing helpers (mirror src/data.py flatten logic)
# ---------------------------------------------------------------------------

def _parse_history(row) -> List[str]:
    raw = row.get("history", "")
    if pd.notna(raw) and isinstance(raw, str) and raw.strip():
        return raw.strip().split()
    return []


def _parse_impression(row) -> Tuple[List[str], List[int]]:
    """Return (shown_news_ids, labels) for one impression row."""
    raw = str(row["impressions"])
    nids, labels = [], []
    for item in raw.strip().split():
        parts = item.rsplit("-", 1)
        if len(parts) != 2:
            continue
        nid, lbl = parts
        nids.append(nid)
        labels.append(1 if lbl == "1" else 0)
    return nids, labels


def _history_texts(news_lookup: Dict[str, str], history_ids: List[str]) -> List[str]:
    return [news_lookup[n] for n in history_ids if n in news_lookup]


# ---------------------------------------------------------------------------
# Per-retriever evaluation
# ---------------------------------------------------------------------------

@dataclass
class RetrievalResult:
    method: str
    k: int
    # end-to-end (NRMS reranking on retrieved set)
    endtoend: Dict[str, float] = field(default_factory=dict)
    # standalone retrieval quality
    recall_at_k: float = 0.0
    hit_rate: float = 0.0
    avg_candidates: float = 0.0


def evaluate_retriever(
    retriever: Retriever,
    method_name: str,
    dev_behavior: pd.DataFrame,
    news_lookup: Dict[str, str],       # news_id -> "title abstract" text
    news_id_to_idx: Dict[str, int],    # for building NRMS candidate tensors
    model: torch.nn.Module,
    device: torch.device,
    criterion: torch.nn.Module,
    k: int = 50,
    force_include_clicks: bool = True,
    max_impressions: Optional[int] = None,
    eval_batch_size: int = 16,
    entity_retriever: bool = False,
    history_news_ids_per_row: Optional[List[List[str]]] = None,
) -> RetrievalResult:
    """
    Run one retriever end-to-end on the dev impressions.

    Returns a RetrievalResult with both reranking metrics (via train.py::evaluate)
    and standalone retrieval metrics (Recall@K, hit-rate).
    """
    rows = dev_behavior
    if max_impressions is not None:
        rows = dev_behavior.iloc[:max_impressions]

    # Accumulators for standalone retrieval metrics
    n_impr = 0
    n_hit = 0
    sum_recall = 0.0
    cand_counts = []

    # Build retrieved-impression samples for the NRMS reranker
    retrieved_samples = []  # (history_indices, cand_idx_list, label_list)

    for bidx in range(len(rows)):
        row = rows.iloc[bidx]
        history_ids = _parse_history(row)
        shown_ids, shown_labels = _parse_impression(row)
        clicked_ids = [nid for nid, l in zip(shown_ids, shown_labels) if l == 1]
        if not clicked_ids:
            # No positive in this impression -> skip (can't measure recall/rerank fairly)
            continue

        hist_texts = _history_texts(news_lookup, history_ids)

        if entity_retriever and isinstance(retriever, EntityRetriever):
            hvec = retriever.history_entity_vector(history_ids)
            out = retriever.retrieve_entity_impression(
                history_entity_vec=hvec,
                clicked_news_ids=clicked_ids,
                shown_news_ids=shown_ids,
                shown_labels=shown_labels,
                k=k,
                force_include_clicks=force_include_clicks,
                history_news_ids=history_ids,
            )
        else:
            out = retriever.retrieve_impression(
                history_texts=hist_texts,
                clicked_news_ids=clicked_ids,
                shown_news_ids=shown_ids,
                shown_labels=shown_labels,
                k=k,
                force_include_clicks=force_include_clicks,
                history_news_ids=history_ids,
            )

        retrieved_set = out["retrieved_set"]
        # Standalone metrics
        n_impr += 1
        hit = len(retrieved_set & set(clicked_ids)) > 0
        n_hit += int(hit)
        sum_recall += len(retrieved_set & set(clicked_ids)) / len(clicked_ids)
        cand_counts.append(len(out["candidates"]))

        # Map to NRMS indices (skip candidates not in news_id_to_idx)
        hist_idx = [news_id_to_idx[n] for n in history_ids if n in news_id_to_idx]
        cand_idx = [news_id_to_idx[n] for n in out["candidates"] if n in news_id_to_idx]
        cand_lbl = [
            l for n, l in zip(out["candidates"], out["labels"]) if n in news_id_to_idx
        ]
        if not cand_idx:
            continue
        retrieved_samples.append((hist_idx, cand_idx, cand_lbl))  # noqa

    # ---- End-to-end: rerank retrieved set with NRMS via existing evaluate ----
    from src.data import ImpressionMINDDataset
    dataset = ImpressionMINDDataset(retrieved_samples)
    loader = DataLoader(
        dataset, batch_size=eval_batch_size, shuffle=False,
        collate_fn=impression_collate_fn,
    )
    metrics = evaluate(
        model, loader, device, criterion, name="retrieved", return_raw=False,
    )

    result = RetrievalResult(method=method_name, k=k)
    result.endtoend = metrics
    result.recall_at_k = sum_recall / max(1, n_impr)
    result.hit_rate = n_hit / max(1, n_impr)
    result.avg_candidates = float(np.mean(cand_counts)) if cand_counts else 0.0
    return result


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_benchmark(
    train_news_path: str,
    dev_news_path: str,
    dev_behaviors_path: str,
    entity_vec_path: str,
    model: torch.nn.Module,
    device: torch.device,
    criterion: torch.nn.Module,
    k: int = 50,
    force_include_clicks: bool = True,
    max_impressions: Optional[int] = None,
    eval_batch_size: int = 16,
    use_tfidf: bool = True,
    use_dense: bool = True,
    use_entity: bool = True,
    dense_model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
    dense_cache_dir: str = "cache",
    max_news: Optional[int] = None,
) -> List[RetrievalResult]:
    """
    Build the full-corpus index once, then benchmark all enabled retrievers.

    `max_news` caps the indexed corpus size (head of the combined frame) for
    small-scale smoke tests; the full run on a big system leaves it None.

    Returns a list of RetrievalResult (one per method).
    """
    # 1. Load + combine news (full corpus = train + dev)
    train_news = load_news(train_news_path)
    dev_news = load_news(dev_news_path)
    all_news = pd.concat([train_news, dev_news], ignore_index=True).drop_duplicates(
        subset=["news_id"]
    ).reset_index(drop=True)
    if max_news is not None:
        all_news = all_news.head(max_news).reset_index(drop=True)
    dev_behavior = load_behaviors(dev_behaviors_path, nrows=max_impressions)

    # news_id -> "title abstract" text (for TF-IDF / Dense queries)
    news_lookup = {
        str(r["news_id"]): f"{r.get('title','')} {r.get('abstract','')}"
        for _, r in all_news.iterrows()
    }
    # news_id -> NRMS index (1-based, matching src/data.build_news_title_tokens)
    news_id_to_idx = {str(nid): i + 1 for i, nid in enumerate(all_news["news_id"])}

    results: List[RetrievalResult] = []

    # 2a. TF-IDF
    if use_tfidf:
        print("\n[Retrieval] TF-IDF ...")
        r = TfidfRetriever()
        r.build_index(all_news)
        res = evaluate_retriever(
            r, "tfidf", dev_behavior, news_lookup, news_id_to_idx, model, device,
            criterion, k=k, force_include_clicks=force_include_clicks,
            max_impressions=max_impressions, eval_batch_size=eval_batch_size,
        )
        results.append(res)
        _print_result(res)

    # 2b. Dense (MiniLM)
    if use_dense:
        print("\n[Retrieval] Dense (MiniLM) ...")
        r = DenseRetriever(model_name=dense_model_name, cache_dir=dense_cache_dir)
        r.build_index(all_news)
        res = evaluate_retriever(
            r, "dense", dev_behavior, news_lookup, news_id_to_idx, model, device,
            criterion, k=k, force_include_clicks=force_include_clicks,
            max_impressions=max_impressions, eval_batch_size=eval_batch_size,
        )
        results.append(res)
        _print_result(res)

    # 2c. Entity-KG
    if use_entity:
        print("\n[Retrieval] Entity-KG ...")
        r = EntityRetriever(entity_vec_path=entity_vec_path)
        r.build_index(all_news)
        res = evaluate_retriever(
            r, "entity", dev_behavior, news_lookup, news_id_to_idx, model, device,
            criterion, k=k, force_include_clicks=force_include_clicks,
            max_impressions=max_impressions, eval_batch_size=eval_batch_size,
            entity_retriever=True,
        )
        results.append(res)
        _print_result(res)

    return results


def _print_result(res: RetrievalResult):
    print(f"  method={res.method}  k={res.k}")
    print(f"    Recall@{res.k}={res.recall_at_k:.4f}  hit_rate={res.hit_rate:.4f}  "
          f"avg_cands={res.avg_candidates:.1f}")
    for key in ("retrieved_auc", "retrieved_impression_auc", "retrieved_mrr",
                "retrieved_ndcg@5", "retrieved_ndcg@10"):
        if key in res.endtoend:
            print(f"    {key}={res.endtoend[key]:.4f}")


def results_to_dataframe(results: List[RetrievalResult]) -> pd.DataFrame:
    """Flatten RetrievalResult list into a comparison DataFrame."""
    rows = []
    for r in results:
        row = {
            "method": r.method,
            "k": r.k,
            "recall@k": r.recall_at_k,
            "hit_rate": r.hit_rate,
            "avg_candidates": r.avg_candidates,
        }
        for kk, vv in r.endtoend.items():
            row[kk] = vv
        rows.append(row)
    return pd.DataFrame(rows)
