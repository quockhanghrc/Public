"""
Dataset and data loading utilities for the MIND dataset.
Flattens impression logs into (user_history, candidate_news, label) triplets.
"""

import random
import warnings
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
from torch.nn.utils.rnn import pad_sequence
from torch.utils.data import Dataset, DataLoader

from src.vocab import build_vocab, encode, vocab_size

# Column names for MIND TSV files
BEHAVIOR_COLS = ["impression_id", "user_id", "time", "history", "impressions"]
NEWS_COLS = [
    "news_id", "category", "subcategory", "title",
    "abstract", "url", "title_entities", "abstract_entities",
]


def load_behaviors(path: str, nrows: Optional[int] = None) -> pd.DataFrame:
    """Load behaviors.tsv or behaviors.parquet."""
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(
        path, sep="\t", header=None, names=BEHAVIOR_COLS, nrows=nrows,
    )


def load_news(path: str) -> pd.DataFrame:
    """Load news.tsv or news.parquet."""
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    return pd.read_csv(
        path, sep="\t", header=None, names=NEWS_COLS,
    )


def build_news_title_tokens(
    news_df: pd.DataFrame,
    vocab: dict,
    max_title_len: int,
) -> Tuple[torch.LongTensor, Dict[str, int]]:
    """
    Tokenize all news titles and build a mapping from news_id to index.

    Index 0 is RESERVED for padding only. All real news articles are shifted by +1
    (article i -> index i+1) so that index 0 never collides with a real article.
    This avoids the ambiguity where the history-padding sentinel (0) and a real
    news article at index 0 would be indistinguishable in UserEncoder's mask.

    Returns:
        news_title_tokens: (num_news + 1, max_title_len) LongTensor of token indices.
                            Row 0 is an all-zero padding row.
        news_id_to_idx: dict mapping news_id (str) -> int index (1-based, 0 reserved).
    """
    news_ids = news_df["news_id"].values
    # Reserve index 0 for padding; real articles start at index 1.
    news_id_to_idx = {nid: i + 1 for i, nid in enumerate(news_ids)}

    titles = news_df["title"].fillna("").values
    token_list = []
    for t in titles:
        token_list.append(encode(t, vocab, max_title_len, pad=True))

    news_title_tokens = torch.tensor(token_list, dtype=torch.long)
    # Prepend a zero row so a padding lookup (index 0) yields an all-zero,
    # fully-masked title vector.
    zero_row = torch.zeros(1, max_title_len, dtype=torch.long)
    news_title_tokens = torch.cat([zero_row, news_title_tokens], dim=0)
    return news_title_tokens, news_id_to_idx


def flatten_impressions(
    behaviors_df: pd.DataFrame,
    news_id_to_idx: Dict[str, int],
    max_history_len: int,
    neg_samples: Optional[int] = None,  # None = keep all candidates (no sampling)
) -> List[Tuple[List[int], int, float]]:
    """
    Flatten each behavior row into (history_indices, candidate_idx, label) triplets.

    Handles missing news IDs gracefully (skips them with a warning).

    Negative sampling (NRMS/MIND protocol): when `neg_samples` is set, for each
    impression we keep ALL positive candidates and randomly sample up to
    `neg_samples` negatives per positive. Impressions with no positive are skipped.
    When `neg_samples is None`, ALL candidates are kept (no downsampling) — this is
    the default and is also used for evaluation (see flatten_impressions_with_groups).

    Args:
        behaviors_df: DataFrame with history and impressions columns.
        news_id_to_idx: Mapping from news ID string to index.
        max_history_len: Max number of history items to keep (most recent).
        neg_samples: Negatives sampled per positive (int) or None to keep all.

    Returns:
        List of (history_indices, candidate_news_idx, label).
    """
    samples: List[Tuple[List[int], int, float]] = []
    missing_warned = False

    for bidx in range(len(behaviors_df)):
        row = behaviors_df.iloc[bidx]

        # --- Parse history ---
        history_raw = row.get("history", "")
        if pd.notna(history_raw) and isinstance(history_raw, str) and history_raw.strip():
            history_ids = history_raw.strip().split()
        else:
            history_ids = []

        history_indices = []
        for nid in history_ids:
            if nid in news_id_to_idx:
                history_indices.append(news_id_to_idx[nid])
            elif not missing_warned:
                warnings.warn(f"News ID {nid} not found in news data. Skipping.")
                missing_warned = True
        # Keep most recent history_len items
        history_indices = history_indices[-max_history_len:]

        # --- Parse impressions ---
        impressions_raw = str(row["impressions"])
        impression_items = impressions_raw.strip().split()

        # Collect positives and negatives for this impression
        pos_idxs: List[int] = []
        neg_idxs: List[int] = []
        for item in impression_items:
            parts = item.rsplit("-", 1)
            if len(parts) != 2:
                continue
            nid, label_str = parts
            if nid not in news_id_to_idx:
                continue
            candidate_idx = news_id_to_idx[nid]
            if label_str == "1":
                pos_idxs.append(candidate_idx)
            else:
                neg_idxs.append(candidate_idx)

        if neg_samples is None:
            # Keep all candidates (no downsampling)
            for cidx in pos_idxs:
                samples.append((history_indices, cidx, 1.0))
            for cidx in neg_idxs:
                samples.append((history_indices, cidx, 0.0))
        else:
            # Sample negatives per positive (skip impressions with no positive)
            if not pos_idxs:
                continue
            for pidx in pos_idxs:
                samples.append((history_indices, pidx, 1.0))
                if neg_idxs:
                    k = min(neg_samples, len(neg_idxs))
                    sampled = random.sample(neg_idxs, k)
                    for nidx in sampled:
                        samples.append((history_indices, nidx, 0.0))

    return samples


def flatten_impressions_with_groups(
    behaviors_df: pd.DataFrame,
    news_id_to_idx: Dict[str, int],
    max_history_len: int,
) -> Tuple[List[Tuple[int, List[int], int, float]], Dict[int, List[int]]]:
    """
    Flatten impressions but preserve impression grouping for evaluation.

    Returns:
        samples: List of (impression_idx, history_indices, candidate_idx, label).
        group_map: Dict mapping impression_idx -> list of sample indices within samples.
    """
    samples: List[Tuple[int, List[int], int, float]] = []
    group_map: Dict[int, List[int]] = {}
    missing_warned = False

    for bidx in range(len(behaviors_df)):
        row = behaviors_df.iloc[bidx]

        history_raw = row.get("history", "")
        if pd.notna(history_raw) and isinstance(history_raw, str) and history_raw.strip():
            history_ids = history_raw.strip().split()
        else:
            history_ids = []

        history_indices = []
        for nid in history_ids:
            if nid in news_id_to_idx:
                history_indices.append(news_id_to_idx[nid])
            elif not missing_warned:
                warnings.warn(f"News ID {nid} not found in news data. Skipping.")
                missing_warned = True
        history_indices = history_indices[-max_history_len:]

        impressions_raw = str(row["impressions"])
        impression_items = impressions_raw.strip().split()

        gid = bidx  # use row index as group ID
        group_map[gid] = []

        for item in impression_items:
            parts = item.rsplit("-", 1)
            if len(parts) != 2:
                continue
            nid, label_str = parts
            if nid not in news_id_to_idx:
                continue
            candidate_idx = news_id_to_idx[nid]
            label = 1.0 if label_str == "1" else 0.0
            group_map[gid].append(len(samples))
            samples.append((gid, history_indices, candidate_idx, label))

    return samples, group_map


class EvalMINDDataset(Dataset):
    """
    Dataset that preserves impression grouping for evaluation metrics.

    Returns (impression_id, history_indices, candidate_news_idx, label).
    """

    def __init__(
        self,
        samples: List[Tuple[int, List[int], int, float]],
    ):
        self.samples = samples

    def __len__(self) -> int:
        return len(self.samples)

    def __getitem__(self, idx: int) -> Tuple[int, List[int], int, float]:
        return self.samples[idx]


class MINDDataset(Dataset):
    """
    Dataset that returns (history_indices, candidate_news_idx, label) triplets.

    History indices are variable-length lists (padded in collate_fn).
    """

    def __init__(
        self,
        samples: List[Tuple[List[int], int, float]],
    ):
        self.samples = samples

    def __len__(self) -> int:
        return len(self.samples)

    def __getitem__(self, idx: int) -> Tuple[List[int], int, float]:
        return self.samples[idx]


class ImpressionMINDDataset(Dataset):
    """
    Dataset that keeps each IMPRESSION grouped: one sample = (history, candidate_list,
    label_list). Used for listwise (per-impression) training.
    """

    def __init__(
        self,
        samples: List[Tuple[List[int], List[int], List[float]]],
    ):
        self.samples = samples

    def __len__(self) -> int:
        return len(self.samples)

    def __getitem__(self, idx: int) -> Tuple[List[int], List[int], List[float]]:
        return self.samples[idx]


def collate_fn(
    batch: List[Tuple[List[int], int, float]],
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """
    Collate a batch of (history_indices, candidate_idx, label) into padded tensors.

    Returns:
        history_padded: (batch, max_hist_len) LongTensor.
        candidate: (batch,) LongTensor.
        label: (batch,) FloatTensor.
    """
    history_list, candidate_list, label_list = zip(*batch)

    # Convert each history to a tensor of indices
    history_tensors = [torch.tensor(h, dtype=torch.long) for h in history_list]
    history_padded = pad_sequence(
        history_tensors, batch_first=True, padding_value=0,
    )

    candidates = torch.tensor(candidate_list, dtype=torch.long)
    labels = torch.tensor(label_list, dtype=torch.float)

    return history_padded, candidates, labels


def impression_collate_fn(
    batch: List[Tuple[List[int], List[int], List[float]]],
    max_candidates: int = 50,
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    """
    Collate grouped impressions. Pads history and candidate lists.

    Returns:
        history_padded: (B, max_history_len) LongTensor.
        candidates: (B, max_candidates) LongTensor.
        labels: (B, max_candidates) FloatTensor.
        candidate_mask: (B, max_candidates) LongTensor (1=valid, 0=padding).
    """
    history_list, cand_list, label_list = zip(*batch)

    history_tensors = [torch.tensor(h, dtype=torch.long) for h in history_list]
    history_padded = pad_sequence(
        history_tensors, batch_first=True, padding_value=0,
    )

    padded_cands, padded_labels, masks = [], [], []
    for cands, labels in zip(cand_list, label_list):
        c = cands[:max_candidates]
        l = labels[:max_candidates]
        mask = [1] * len(c) + [0] * (max_candidates - len(c))
        c = c + [0] * (max_candidates - len(c))
        l = l + [0.0] * (max_candidates - len(l))
        padded_cands.append(torch.tensor(c, dtype=torch.long))
        padded_labels.append(torch.tensor(l, dtype=torch.float))
        masks.append(torch.tensor(mask, dtype=torch.long))

    candidates = torch.stack(padded_cands)      # (B, max_candidates)
    labels = torch.stack(padded_labels)        # (B, max_candidates)
    candidate_mask = torch.stack(masks)        # (B, max_candidates)
    return history_padded, candidates, labels, candidate_mask


def build_impression_samples(
    behaviors_df: pd.DataFrame,
    news_id_to_idx: Dict[str, int],
    max_history_len: int,
    max_candidates: int = 50,
) -> List[Tuple[List[int], List[int], List[float]]]:
    """
    Group each impression into (history_indices, candidate_idx_list, label_list).
    Used for listwise training. Keeps ALL candidates (negatives serve as the ranking
    negatives). Truncates to max_candidates.
    """
    samples: List[Tuple[List[int], List[int], List[float]]] = []
    missing_warned = False

    for bidx in range(len(behaviors_df)):
        row = behaviors_df.iloc[bidx]
        history_raw = row.get("history", "")
        if pd.notna(history_raw) and isinstance(history_raw, str) and history_raw.strip():
            history_ids = history_raw.strip().split()
        else:
            history_ids = []
        history_indices = []
        for nid in history_ids:
            if nid in news_id_to_idx:
                history_indices.append(news_id_to_idx[nid])
            elif not missing_warned:
                warnings.warn(f"News ID {nid} not found in news data. Skipping.")
                missing_warned = True
        history_indices = history_indices[-max_history_len:]

        impressions_raw = str(row["impressions"])
        cand_idxs, label_list = [], []
        for item in impressions_raw.strip().split():
            parts = item.rsplit("-", 1)
            if len(parts) != 2:
                continue
            nid, label_str = parts
            if nid not in news_id_to_idx:
                continue
            cand_idxs.append(news_id_to_idx[nid])
            label_list.append(1.0 if label_str == "1" else 0.0)
        if not cand_idxs:
            continue
        samples.append((history_indices, cand_idxs, label_list))

    return samples


def build_eval_impression_samples(
    behaviors_df: pd.DataFrame,
    news_id_to_idx: Dict[str, int],
    max_history_len: int,
) -> List[Tuple[List[int], List[int], List[float]]]:
    """
    Group each impression into (history_indices, candidate_idx_list, label_list) for
    EVALUATION. Like build_impression_samples but WITHOUT max_candidates truncation —
    keeps ALL candidates so per-impression metrics (AUC/MRR/nDCG) stay exact.
    """
    samples: List[Tuple[List[int], List[int], List[float]]] = []
    missing_warned = False

    for bidx in range(len(behaviors_df)):
        row = behaviors_df.iloc[bidx]
        history_raw = row.get("history", "")
        if pd.notna(history_raw) and isinstance(history_raw, str) and history_raw.strip():
            history_ids = history_raw.strip().split()
        else:
            history_ids = []
        history_indices = []
        for nid in history_ids:
            if nid in news_id_to_idx:
                history_indices.append(news_id_to_idx[nid])
            elif not missing_warned:
                warnings.warn(f"News ID {nid} not found in news data. Skipping.")
                missing_warned = True
        history_indices = history_indices[-max_history_len:]

        impressions_raw = str(row["impressions"])
        cand_idxs, label_list = [], []
        for item in impressions_raw.strip().split():
            parts = item.rsplit("-", 1)
            if len(parts) != 2:
                continue
            nid, label_str = parts
            if nid not in news_id_to_idx:
                continue
            cand_idxs.append(news_id_to_idx[nid])
            label_list.append(1.0 if label_str == "1" else 0.0)
        if not cand_idxs:
            continue
        samples.append((history_indices, cand_idxs, label_list))

    return samples


def eval_collate_fn(
    batch: List[Tuple[int, List[int], int, float]],
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]:
    """
    Collate for EvalMINDDataset: preserves impression_id.
    Returns (impression_ids, history_padded, candidates, labels).
    """
    gid_list, history_list, candidate_list, label_list = zip(*batch)

    history_tensors = [torch.tensor(h, dtype=torch.long) for h in history_list]
    history_padded = pad_sequence(
        history_tensors, batch_first=True, padding_value=0,
    )

    gids = torch.tensor(gid_list, dtype=torch.long)
    candidates = torch.tensor(candidate_list, dtype=torch.long)
    labels = torch.tensor(label_list, dtype=torch.float)

    return gids, history_padded, candidates, labels


def prepare_data(
    train_behaviors_path: str,
    train_news_path: str,
    dev_behaviors_path: str,
    dev_news_path: str,
    max_history_len: int = 30,
    max_title_len: int = 20,
    min_word_freq: int = 2,
    max_train_impressions: Optional[int] = None,
    max_dev_impressions: Optional[int] = None,
    neg_samples: Optional[int] = None,
    in_time_val_frac: float = 0.0,
    in_time_val_seed: int = 42,
    train_mode: str = "listwise",
    max_candidates: int = 50,
    seed: int = 42,
) -> Tuple:
    """
    End-to-end data preparation for NRMS training.

    Returns:
        train_dataset: MINDDataset (pointwise) or ImpressionMINDDataset (listwise).
        in_time_val_dataset: EvalMINDDataset for in-time validation (user-disjoint
                             from training; preserves impression grouping).
        out_of_time_val_dataset: EvalMINDDataset for out-of-time validation (dev set;
                             preserves impression grouping).
        vocab: word-to-index vocabulary (built from TRAIN titles only).
        news_title_tokens: (num_news + 1, max_title_len) tensor (row 0 = padding).
        num_news: total number of news articles (real articles; buffer has +1 row).
        news_id_to_idx: dict mapping news_id (str) -> 1-based int index.
        idx_to_category: (num_news + 1,) int64 array; idx_to_category[i] = integer
            category id for news index i (1-based), -1 for padding index 0.
            Used for history-attention attribution (category concentration, etc.).
        idx_to_subcategory: (num_news + 1,) int64 array; same layout as
            idx_to_category but for the finer-grained subcategory column.
        num_categories: number of distinct category ids (for the embedding table size).
        num_subcategories: number of distinct subcategory ids.
    """
    random.seed(seed)

    # 1. Load data (pass nrows for fast smoke tests — avoids loading full file)
    train_behavior = load_behaviors(train_behaviors_path, nrows=max_train_impressions)
    train_news = load_news(train_news_path)
    dev_behavior = load_behaviors(dev_behaviors_path, nrows=max_dev_impressions)
    dev_news = load_news(dev_news_path)

    # 1b. User-disjoint in-time validation split (no user in both train & in-time val).
    #     Split by user_id to avoid user-level leakage; news embeddings are shared
    #     across splits by design (not leakage).
    if in_time_val_frac and in_time_val_frac > 0.0:
        split_seed = in_time_val_seed if in_time_val_seed is not None else seed
        users = train_behavior["user_id"].dropna().unique().tolist()
        rng = random.Random(split_seed)
        rng.shuffle(users)
        n_val_users = int(len(users) * in_time_val_frac)
        val_users = set(users[:n_val_users])
        in_time_val_behavior = train_behavior[
            train_behavior["user_id"].isin(val_users)
        ].reset_index(drop=True)
        train_behavior = train_behavior[
            ~train_behavior["user_id"].isin(val_users)
        ].reset_index(drop=True)
        print(f"In-time val users: {len(val_users)} | train users: {len(users) - len(val_users)}")
    else:
        in_time_val_behavior = train_behavior.iloc[0:0].copy()  # empty; no in-time val

    # 2. Build vocabulary from TRAIN titles only (standard MIND protocol).
    #    Dev/test OOV words map to UNK — avoids leakage and keeps eval honest.
    all_titles = train_news["title"].fillna("").tolist()
    word_vocab = build_vocab(all_titles, min_freq=min_word_freq)
    print(f"Vocabulary size (train-only): {vocab_size(word_vocab)}")

    # 3. Build news title tokens + ID mapping (on combined news)
    all_news = pd.concat([train_news, dev_news], ignore_index=True).drop_duplicates(
        subset=["news_id"]
    ).reset_index(drop=True)

    news_title_tokens, news_id_to_idx = build_news_title_tokens(
        all_news, word_vocab, max_title_len,
    )
    # num_news = real article count. news_title_tokens has an extra padding row at
    # index 0, so its length is num_news + 1.
    num_news = len(news_id_to_idx)
    print(f"Total news articles: {num_news}")

    # 3b. Category / subcategory mappings for (a) history-attention attribution and
    # (b) optional category-aware news encoding. Factorize the combined-news columns
    # into integer ids, then build arrays indexed by news index (1-based, matching
    # news_id_to_idx) with index 0 = -1 (padding). Used by compute_history_attention()
    # and by CNNNewsEncoder when category_mode is enabled.
    cat_factorized, _ = pd.factorize(all_news["category"].fillna("").astype(str))
    subcat_factorized, _ = pd.factorize(all_news["subcategory"].fillna("").astype(str))
    idx_to_category = np.full(num_news + 1, -1, dtype=np.int64)  # index 0 = padding
    idx_to_subcategory = np.full(num_news + 1, -1, dtype=np.int64)
    # all_news row i corresponds to news index i+1 (see build_news_title_tokens shift)
    idx_to_category[1:] = cat_factorized.astype(np.int64)
    idx_to_subcategory[1:] = subcat_factorized.astype(np.int64)
    num_categories = int(pd.Series(cat_factorized).nunique())
    num_subcategories = int(pd.Series(subcat_factorized).nunique())

    # 4. Build training dataset (mode-dependent).
    #    pointwise: flatten to (history, candidate, label) triplets (current default).
    #    listwise: keep each impression grouped for per-impression ranking loss.
    if train_mode == "listwise":
        train_samples = build_impression_samples(
            train_behavior, news_id_to_idx, max_history_len,
            max_candidates=max_candidates,
        )
        train_dataset = ImpressionMINDDataset(train_samples)
        print(f"Training impressions (listwise): {len(train_samples)}")
    else:  # pointwise (default, current behavior)
        train_samples = flatten_impressions(
            train_behavior, news_id_to_idx, max_history_len, neg_samples=neg_samples,
        )
        train_dataset = MINDDataset(train_samples)
        if neg_samples is None:
            print(f"Training samples (all candidates): {len(train_samples)}")
        else:
            print(f"Training samples (neg_samples={neg_samples}): {len(train_samples)}")

    # 5. Build in-time-val impressions grouped (all candidates, eval). Scored per
    #    impression via score_candidates (user encoded once) — no redundant re-encoding.
    in_time_val_samples = build_eval_impression_samples(
        in_time_val_behavior, news_id_to_idx, max_history_len,
    )
    in_time_val_dataset = ImpressionMINDDataset(in_time_val_samples)
    print(f"In-time val impressions: {len(in_time_val_samples)}")

    # 6. Build dev (out-of-time) impressions grouped for per-impression eval
    out_of_time_val_samples = build_eval_impression_samples(
        dev_behavior, news_id_to_idx, max_history_len,
    )
    out_of_time_val_dataset = ImpressionMINDDataset(out_of_time_val_samples)
    print(f"Out-of-time (dev) impressions: {len(out_of_time_val_samples)}")

    return (train_dataset, in_time_val_dataset, out_of_time_val_dataset,
            word_vocab, news_title_tokens, num_news, news_id_to_idx,
            idx_to_category, idx_to_subcategory, num_categories, num_subcategories)