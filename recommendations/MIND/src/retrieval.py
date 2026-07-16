"""
Retrieval stage for the two-stage MIND recommender.

NRMS (in src/model.py) is the RERANKING stage: it consumes a fixed candidate
set (the impression) and ranks it. This module is the missing RETRIEVAL stage:
it indexes the full news corpus and, for each impression, retrieves the top-K
candidate news from the user's clicked history.

Three interchangeable retrievers share one interface (Retriever base class):
  - TfidfRetriever   : sparse TF-IDF over title + abstract (scikit-learn).
  - DenseRetriever   : dense MiniLM embeddings (reuses cache/ MiniLM) + sklearn ANN.
  - EntityRetriever  : knowledge-graph entity embeddings (entity_embedding.vec).

All retrievers expose:
  build_index(news_df)            -> index the corpus
  retrieve(history_titles, k)     -> list[str] of news_ids (top-K)
  retrieve_impression(imp, k, ...) -> dict with candidates/labels for eval

The query is the user's clicked HISTORY (standard MIND retrieval setup), not the
impression itself. The candidate pool is the FULL news corpus (train+dev combined
so dev news are retrievable).
"""

import json
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

class Retriever:
    """
    Abstract base for all retrieval methods.

    Subclasses must implement `_encode_corpus(news_df) -> np.ndarray` (returns a
    (num_news, dim) matrix aligned to `self.news_ids`) and `_encode_query(texts)
    -> np.ndarray` (returns a (dim,) vector for a list of history texts). The base
    class handles cosine similarity, top-K selection, and impression assembly.
    """

    def __init__(self):
        self.news_ids: List[str] = []
        self.corpus_matrix: Optional[np.ndarray] = None  # (num_news, dim)
        self._news_id_to_pos: Dict[str, int] = {}

    # --- to be implemented by subclasses ---
    def _encode_corpus(self, news_df: pd.DataFrame) -> np.ndarray:
        raise NotImplementedError

    def _encode_query(self, texts: List[str]) -> np.ndarray:
        """Return a single (dim,) query vector for a list of history texts."""
        raise NotImplementedError

    # --- shared machinery ---
    @staticmethod
    def _l2_normalize(mat: np.ndarray) -> np.ndarray:
        norms = np.linalg.norm(mat, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        return mat / norms

    def build_index(self, news_df: pd.DataFrame) -> "Retriever":
        """Index the full news corpus. `news_df` must have a 'news_id' column."""
        self.news_ids = news_df["news_id"].astype(str).tolist()
        self._news_id_to_pos = {nid: i for i, nid in enumerate(self.news_ids)}
        mat = self._encode_corpus(news_df)
        # Normalize for cosine; subclasses that prefer raw dot product can override.
        self.corpus_matrix = self._l2_normalize(np.asarray(mat, dtype=np.float32))
        return self

    def retrieve(self, history_texts: List[str], k: int = 50) -> List[str]:
        """
        Retrieve top-K news_ids for a single user given their clicked history texts.

        Args:
            history_texts: list of title/abstract strings from the user's history.
            k: number of candidates to return.

        Returns:
            list of news_id strings (length <= k), best first.
        """
        if self.corpus_matrix is None:
            raise RuntimeError("Call build_index() before retrieve().")
        if not history_texts:
            return []
        q = self._l2_normalize(self._encode_query(history_texts).reshape(1, -1))
        # cosine = dot product of L2-normalized vectors
        sims = (self.corpus_matrix @ q.T).ravel()  # (num_news,)
        # Exclude the user's own history items from the candidate set (no self-retrieval)
        # via subclass hook (base has no news_id mapping for history texts).
        exclude = self._excluded_positions(history_texts)
        if exclude:
            sims[np.array(sorted(exclude), dtype=int)] = -np.inf
        k_eff = min(k, len(sims))
        top_idx = np.argpartition(-sims, k_eff - 1)[:k_eff]
        top_idx = top_idx[np.argsort(-sims[top_idx])]
        return [self.news_ids[i] for i in top_idx]

    def _excluded_positions(self, history_texts: List[str]) -> List[int]:
        """
        Hook for subclasses to exclude the user's own history news from candidates.
        Default: no exclusion. DenseRetriever/EntityRetriever override this when they
        can map history texts back to news_ids.
        """
        return []

    def retrieve_impression(
        self,
        history_texts: List[str],
        clicked_news_ids: List[str],
        shown_news_ids: List[str],
        shown_labels: List[int],
        k: int = 50,
        force_include_clicks: bool = True,
        history_news_ids: Optional[List[str]] = None,
    ) -> Dict:
        """
        Build a retrieved candidate set for one impression, ready for NRMS reranking.

        Args:
            history_texts: title/abstract strings of the user's clicked history.
            clicked_news_ids: news_ids the user clicked in THIS impression (positives).
            shown_news_ids: all news_ids shown in THIS impression (for label lookup).
            shown_labels: 1/0 click labels aligned to shown_news_ids.
            k: retrieval cutoff.
            force_include_clicks: if True, union the retrieved top-K with the clicked
                positives so the reranker always has the positives to rank (fair eval).

        Returns:
            dict with keys:
              candidates: list[str] news_ids (retrieved, deduped, capped at k)
              labels:    list[int] 1/0 aligned to candidates
              retrieved_set: set[str] of retrieved ids (for Recall@K computation)
        """
        retrieved = self.retrieve(history_texts, k=k)
        retrieved_set = set(retrieved)

        # Map shown news -> label for label lookup
        shown_label_map = dict(zip(shown_news_ids, shown_labels))

        candidates = list(retrieved)
        if force_include_clicks:
            for nid in clicked_news_ids:
                if nid not in retrieved_set and nid not in candidates:
                    candidates.append(nid)
        # Cap at k (after force-include, trim least-relevant if over budget)
        if len(candidates) > k:
            candidates = candidates[:k]

        labels = [int(shown_label_map.get(nid, 0)) for nid in candidates]
        return {
            "candidates": candidates,
            "labels": labels,
            "retrieved_set": retrieved_set,
        }


# ---------------------------------------------------------------------------
# 1. TF-IDF retriever (sparse)
# ---------------------------------------------------------------------------

class TfidfRetriever(Retriever):
    """
    Sparse retrieval over title + abstract using TF-IDF + cosine similarity.
    No new dependencies (scikit-learn is already in requirements.txt).
    """

    def __init__(self, max_features: int = 20000):
        super().__init__()
        from sklearn.feature_extraction.text import TfidfVectorizer
        self.vectorizer = TfidfVectorizer(
            stop_words="english", max_features=max_features, ngram_range=(1, 2),
        )
        self._fitted = False

    def _corpus_text(self, news_df: pd.DataFrame) -> List[str]:
        titles = news_df["title"].fillna("").astype(str)
        abstracts = news_df["abstract"].fillna("").astype(str)
        return (titles + " " + abstracts).tolist()

    def _encode_corpus(self, news_df: pd.DataFrame) -> np.ndarray:
        texts = self._corpus_text(news_df)
        mat = self.vectorizer.fit_transform(texts)  # (num_news, vocab)
        self._fitted = True
        # Convert to dense float32 for the shared L2-normalize + dot product path.
        return np.asarray(mat.todense(), dtype=np.float32)

    def _encode_query(self, texts: List[str]) -> np.ndarray:
        if not self._fitted:
            raise RuntimeError("build_index() must run before retrieve().")
        q = self.vectorizer.transform([" ".join(texts)])
        return np.asarray(q.todense(), dtype=np.float32).ravel()


# ---------------------------------------------------------------------------
# 2. Dense retriever (MiniLM, reuses cache/)
# ---------------------------------------------------------------------------

class DenseRetriever(Retriever):
    """
    Dense retrieval: encode each news title+abstract with the MiniLM model already
    cached in cache/ (see src/embeddings.py for the loading pattern), then ANN via
    sklearn.neighbors.NearestNeighbors (no new dependency). The query vector is the
    mean-pool of the user's history news embeddings.
    """

    def __init__(
        self,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        cache_dir: str = "cache",
        pool: str = "mean",
        device: str = "cpu",
    ):
        super().__init__()
        self.model_name = model_name
        self.cache_dir = cache_dir
        self.pool = pool
        self.device = device
        self._model = None
        self._tokenizer = None
        # Map news_id -> position for history-exclusion during retrieval.
        self._id_to_pos: Dict[str, int] = {}

    def _load_model(self):
        if self._model is not None:
            return
        try:
            from transformers import AutoModel, AutoTokenizer
        except ImportError as _e:  # pragma: no cover
            raise ImportError(
                "HuggingFace `transformers` is required for DenseRetriever. "
                f"Install with: pip install -r requirements.txt (error: {_e})"
            )
        print(f"  [DenseRetriever] loading '{self.model_name}' (cache={self.cache_dir})")
        self._tokenizer = AutoTokenizer.from_pretrained(
            self.model_name, cache_dir=self.cache_dir
        )
        self._model = AutoModel.from_pretrained(
            self.model_name, cache_dir=self.cache_dir
        ).to(self.device)
        self._model.eval()

    def _embed_texts(self, texts: List[str], batch_size: int = 256) -> np.ndarray:
        self._load_model()
        import torch
        all_vecs = []
        for start in range(0, len(texts), batch_size):
            chunk = texts[start:start + batch_size]
            enc = self._tokenizer(
                chunk, padding=True, truncation=True, max_length=64,
                return_tensors="pt",
            )
            enc = {k: v.to(self.device) for k, v in enc.items()}
            with torch.no_grad():
                out = self._model(**enc).last_hidden_state  # (N, seq, dim)
            # Mean-pool over real tokens (ignore padding via attention mask)
            mask = enc["attention_mask"].unsqueeze(-1).float()  # (N, seq, 1)
            summed = (out * mask).sum(dim=1)
            counts = mask.sum(dim=1).clamp(min=1.0)
            vecs = summed.div(counts).cpu().numpy().astype(np.float32)
            all_vecs.append(vecs)
        return np.concatenate(all_vecs, axis=0)

    def _encode_corpus(self, news_df: pd.DataFrame) -> np.ndarray:
        self._id_to_pos = {nid: i for i, nid in enumerate(self.news_ids)}
        texts = self._corpus_text(news_df)
        # Cache the corpus texts so hard-negative mining can build history queries
        # from news_ids without re-reading the DataFrame.
        self._corpus_titles = texts
        return self._embed_texts(texts)

    def _corpus_text(self, news_df: pd.DataFrame) -> List[str]:
        titles = news_df["title"].fillna("").astype(str)
        abstracts = news_df["abstract"].fillna("").astype(str)
        return (titles + " " + abstracts).tolist()

    def _encode_query(self, texts: List[str]) -> np.ndarray:
        # Mean-pool of the history news embeddings (one vector per history item).
        embs = self._embed_texts(texts)  # (num_hist, dim)
        return embs.mean(axis=0)

    def _excluded_positions(self, history_texts: List[str]) -> List[int]:
        # history_texts are title+abstract strings; we cannot map back to news_id
        # here, so exclusion is handled at the impression level via retrieve_impression
        # override below.
        return []

    def retrieve_impression(
        self,
        history_texts: List[str],
        clicked_news_ids: List[str],
        shown_news_ids: List[str],
        shown_labels: List[int],
        k: int = 50,
        force_include_clicks: bool = True,
        history_news_ids: Optional[List[str]] = None,
    ) -> Dict:
        """
        Like base retrieve_impression but also excludes the user's own history news
        from the candidate set (no self-retrieval) when history_news_ids is provided.
        """
        retrieved = self.retrieve(history_texts, k=k)
        if history_news_ids:
            retrieved = [nid for nid in retrieved if nid not in set(history_news_ids)]
        retrieved_set = set(retrieved)
        shown_label_map = dict(zip(shown_news_ids, shown_labels))
        candidates = list(retrieved)
        if force_include_clicks:
            for nid in clicked_news_ids:
                if nid not in retrieved_set and nid not in candidates:
                    candidates.append(nid)
        if len(candidates) > k:
            candidates = candidates[:k]
        labels = [int(shown_label_map.get(nid, 0)) for nid in candidates]
        return {
            "candidates": candidates,
            "labels": labels,
            "retrieved_set": retrieved_set,
        }


# ---------------------------------------------------------------------------
# 3. Entity-KG retriever (entity_embedding.vec)
# ---------------------------------------------------------------------------

class EntityRetriever(Retriever):
    """
    Knowledge-graph retrieval. Each news article is represented by the mean of its
    title/abstract entity embeddings from entity_embedding.vec (WikidataId -> vec).
    The query is the mean of the user's history news entity vectors.
    """

    def __init__(self, entity_vec_path: str):
        super().__init__()
        self.entity_vec_path = entity_vec_path
        self.entity_vecs: Dict[str, np.ndarray] = {}
        self.dim: int = 0
        self._id_to_pos: Dict[str, int] = {}
        # Per-news entity vectors, aligned to self.news_ids after build_index.
        self.news_entity_vecs: Optional[np.ndarray] = None

    def _load_entity_vecs(self):
        if self.entity_vecs:
            return
        print(f"  [EntityRetriever] loading entity vectors from {self.entity_vec_path}")
        vecs: Dict[str, np.ndarray] = {}
        with open(self.entity_vec_path, "r", encoding="utf-8") as f:
            for line in f:
                # Robust split: handle tab OR whitespace, drop empty tokens.
                toks = [t for t in line.rstrip("\n").replace("\t", " ").split() if t]
                if len(toks) < 2:
                    continue
                eid = toks[0]
                try:
                    vals = np.asarray(
                        [float(x) for x in toks[1:]], dtype=np.float32
                    )
                except ValueError:
                    continue
                if vals.size == 0:
                    continue
                vecs[eid] = vals
        # Ensure consistent dim
        if vecs:
            self.dim = max(v.size for v in vecs.values())
            for k in vecs:
                if vecs[k].size < self.dim:
                    pad = np.zeros(self.dim, dtype=np.float32)
                    pad[: vecs[k].size] = vecs[k]
                    vecs[k] = pad
        self.entity_vecs = vecs
        print(f"  [EntityRetriever] loaded {len(vecs)} entity vectors (dim={self.dim})")

    @staticmethod
    def _parse_entities(json_str) -> List[str]:
        """Extract WikidataId list from a title/abstract_entities JSON column."""
        if not isinstance(json_str, str) or not json_str.strip():
            return []
        try:
            items = json.loads(json_str)
        except (json.JSONDecodeError, TypeError):
            return []
        if not isinstance(items, list):
            return []
        ids = []
        for it in items:
            if isinstance(it, dict) and it.get("WikidataId"):
                ids.append(it["WikidataId"])
        return ids

    def _news_entity_vector(self, row: pd.Series) -> np.ndarray:
        eids = self._parse_entities(row.get("title_entities")) + \
               self._parse_entities(row.get("abstract_entities"))
        vecs = [self.entity_vecs[e] for e in eids if e in self.entity_vecs]
        if not vecs:
            return np.zeros(self.dim, dtype=np.float32)
        return np.mean(vecs, axis=0).astype(np.float32)

    def _encode_corpus(self, news_df: pd.DataFrame) -> np.ndarray:
        self._load_entity_vecs()
        self._id_to_pos = {nid: i for i, nid in enumerate(self.news_ids)}
        mat = np.stack(
            [self._news_entity_vector(r) for _, r in news_df.iterrows()]
        ).astype(np.float32)
        self.news_entity_vecs = mat
        return mat

    def _encode_query(self, texts: List[str]) -> np.ndarray:
        # `texts` are not usable here; EntityRetriever queries via history news
        # entity vectors supplied at the impression level. Fall back to zero.
        return np.zeros(self.dim, dtype=np.float32)

    def retrieve_impression(
        self,
        history_texts: List[str],
        clicked_news_ids: List[str],
        shown_news_ids: List[str],
        shown_labels: List[int],
        k: int = 50,
        force_include_clicks: bool = True,
        history_news_ids: Optional[List[str]] = None,
    ) -> Dict:
        """
        Entity-based retrieval needs the history news entity vectors. We accept
        `history_entity_vec` (a precomputed (dim,) mean vector) via a separate
        helper; here we fall back to corpus mean-pool if not provided.
        """
        # This path is only used when a query vector is supplied externally.
        # The benchmark harness calls `retrieve_entity_impression` instead.
        retrieved = self.retrieve(history_texts, k=k)
        if history_news_ids:
            retrieved = [nid for nid in retrieved if nid not in set(history_news_ids)]
        retrieved_set = set(retrieved)
        shown_label_map = dict(zip(shown_news_ids, shown_labels))
        candidates = list(retrieved)
        if force_include_clicks:
            for nid in clicked_news_ids:
                if nid not in retrieved_set and nid not in candidates:
                    candidates.append(nid)
        if len(candidates) > k:
            candidates = candidates[:k]
        labels = [int(shown_label_map.get(nid, 0)) for nid in candidates]
        return {
            "candidates": candidates,
            "labels": labels,
            "retrieved_set": retrieved_set,
        }

    def retrieve_entity_impression(
        self,
        history_entity_vec: np.ndarray,
        clicked_news_ids: List[str],
        shown_news_ids: List[str],
        shown_labels: List[int],
        k: int = 50,
        force_include_clicks: bool = True,
        history_news_ids: Optional[List[str]] = None,
    ) -> Dict:
        """
        Retrieve using a precomputed history entity query vector (mean of history
        news entity vectors). This is the primary entry point for EntityRetriever.
        """
        if self.corpus_matrix is None:
            raise RuntimeError("Call build_index() before retrieval.")
        q = self._l2_normalize(
            np.asarray(history_entity_vec, dtype=np.float32).reshape(1, -1)
        )
        sims = (self.corpus_matrix @ q.T).ravel()
        if history_news_ids:
            excl = [self._id_to_pos[n] for n in history_news_ids if n in self._id_to_pos]
            if excl:
                sims[np.array(sorted(excl), dtype=int)] = -np.inf
        k_eff = min(k, len(sims))
        top_idx = np.argpartition(-sims, k_eff - 1)[:k_eff]
        top_idx = top_idx[np.argsort(-sims[top_idx])]
        retrieved = [self.news_ids[i] for i in top_idx]
        retrieved_set = set(retrieved)
        shown_label_map = dict(zip(shown_news_ids, shown_labels))
        candidates = list(retrieved)
        if force_include_clicks:
            for nid in clicked_news_ids:
                if nid not in retrieved_set and nid not in candidates:
                    candidates.append(nid)
        if len(candidates) > k:
            candidates = candidates[:k]
        labels = [int(shown_label_map.get(nid, 0)) for nid in candidates]
        return {
            "candidates": candidates,
            "labels": labels,
            "retrieved_set": retrieved_set,
        }

    def history_entity_vector(self, history_news_ids: List[str]) -> np.ndarray:
        """Mean-pool entity vectors of the user's history news (for the query)."""
        if self.news_entity_vecs is None:
            raise RuntimeError("Call build_index() first.")
        vecs = [
            self.news_entity_vecs[self._id_to_pos[n]]
            for n in history_news_ids if n in self._id_to_pos
        ]
        if not vecs:
            return np.zeros(self.dim, dtype=np.float32)
        return np.mean(vecs, axis=0).astype(np.float32)


# ---------------------------------------------------------------------------
# Hard-negative mining (industry-aligned retraining signal)
# ---------------------------------------------------------------------------

def mine_hard_negatives(
    retriever: "DenseRetriever",
    behaviors_df: pd.DataFrame,
    news_id_to_idx: Dict[str, int],
    max_history_len: int = 30,
    num_hn: int = 4,
    retrieve_k: Optional[int] = None,
) -> List[Tuple[List[int], List[int], List[int]]]:
    """
    Mine hard negatives for NRMS retraining using a trained retriever.

    For each impression we:
      1. Encode the user's clicked HISTORY with the retriever (mean-pooled query).
      2. Retrieve the top-(num_hn + shown) most similar news from the corpus.
      3. Drop any news already shown in this impression (clicked OR impression
         negatives) so the hard negatives are genuinely *unseen* but *confusable*.
      4. Keep the top `num_hn` remaining as hard negatives.

    This mirrors the dominant industry pattern (DPR / RocketQA / EMB): the ranker
    is trained on negatives that a strong retriever would surface, not on MIND's
    random negatives. It makes the retrieval -> reranking loop co-adapt.

    Args:
        retriever: a BUILT DenseRetriever (call build_index() first).
        behaviors_df: DataFrame with 'history' and 'impressions' columns.
        news_id_to_idx: news_id (str) -> 1-based int index (matches prepare_data).
        max_history_len: keep the most recent N history items.
        num_hn: number of hard negatives to mine per impression.
        retrieve_k: how many candidates to retrieve before filtering (default
            num_hn + 20 to leave room after dropping shown items).

    Returns:
        List of (history_indices, pos_idxs, hard_neg_idxs) tuples, one per
        impression that has at least one positive. Impressions with no positive
        or no usable history are skipped (consistent with flatten_impressions).
    """
    if retriever.corpus_matrix is None:
        raise RuntimeError("Call retriever.build_index() before mine_hard_negatives().")
    if retrieve_k is None:
        retrieve_k = num_hn + 20

    out: List[Tuple[List[int], List[int], List[int]]] = []

    for bidx in range(len(behaviors_df)):
        row = behaviors_df.iloc[bidx]

        # --- Parse history -> indices ---
        history_raw = row.get("history", "")
        if pd.notna(history_raw) and isinstance(history_raw, str) and history_raw.strip():
            history_ids = history_raw.strip().split()
        else:
            history_ids = []
        history_indices = []
        for nid in history_ids:
            if nid in news_id_to_idx:
                history_indices.append(news_id_to_idx[nid])
        history_indices = history_indices[-max_history_len:]

        # --- Parse impressions -> positives + shown set ---
        impressions_raw = str(row["impressions"])
        pos_idxs: List[int] = []
        shown_ids: List[str] = []
        for item in impressions_raw.strip().split():
            parts = item.rsplit("-", 1)
            if len(parts) != 2:
                continue
            nid, label_str = parts
            if nid not in news_id_to_idx:
                continue
            cidx = news_id_to_idx[nid]
            shown_ids.append(nid)
            if label_str == "1":
                pos_idxs.append(cidx)
        if not pos_idxs:
            continue  # skip impressions with no clicked positive

        # --- Build query from history texts (title+abstract) ---
        if not history_indices:
            continue  # no history -> cannot build a meaningful query
        # Map history indices back to news_ids to fetch their text.
        idx_to_news = {v: k for k, v in news_id_to_idx.items()}
        hist_texts = []
        for hidx in history_indices:
            hnid = idx_to_news.get(hidx)
            if hnid is not None and hnid in retriever._news_id_to_pos:
                pos = retriever._news_id_to_pos[hnid]
                title = (retriever._corpus_titles[pos]
                         if hasattr(retriever, "_corpus_titles") else "")
                hist_texts.append(title)
        if not hist_texts:
            continue

        # --- Retrieve top-(num_hn+shown) similar news ---
        retrieved = retriever.retrieve(hist_texts, k=retrieve_k)
        shown_set = set(shown_ids)
        hard_negs = [nid for nid in retrieved if nid not in shown_set]
        hard_neg_idxs = [
            news_id_to_idx[n] for n in hard_negs[:num_hn] if n in news_id_to_idx
        ]

        out.append((history_indices, pos_idxs, hard_neg_idxs))

    return out
