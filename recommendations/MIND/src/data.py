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

    Returns:
        news_title_tokens: (num_news, max_title_len) LongTensor of token indices.
        news_id_to_idx: dict mapping news_id (str) -> int index.
    """
    news_ids = news_df["news_id"].values
    news_id_to_idx = {nid: i for i, nid in enumerate(news_ids)}

    titles = news_df["title"].fillna("").values
    token_list = []
    for t in titles:
        token_list.append(encode(t, vocab, max_title_len, pad=True))

    news_title_tokens = torch.tensor(token_list, dtype=torch.long)
    return news_title_tokens, news_id_to_idx


def flatten_impressions(
    behaviors_df: pd.DataFrame,
    news_id_to_idx: Dict[str, int],
    max_history_len: int,
    neg_samples: Optional[int] = 10,
) -> List[Tuple[List[int], int, float]]:
    """
    Flatten each behavior row into (history_indices, candidate_idx, label) triplets.

    Handles missing news IDs gracefully (skips them with a warning).

    Args:
        behaviors_df: DataFrame with history and impressions columns.
        news_id_to_idx: Mapping from news ID string to index.
        max_history_len: Max number of history items to keep (most recent).
        neg_samples: Max negative samples per impression row. None = keep all.

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

        positives: List[Tuple[List[int], int, float]] = []
        negatives: List[Tuple[List[int], int, float]] = []

        for item in impression_items:
            parts = item.rsplit("-", 1)
            if len(parts) != 2:
                continue
            nid, label_str = parts
            if nid not in news_id_to_idx:
                continue
            candidate_idx = news_id_to_idx[nid]
            label = 1.0 if label_str == "1" else 0.0

            if label == 1.0:
                positives.append((history_indices, candidate_idx, label))
            else:
                negatives.append((history_indices, candidate_idx, label))

        # Downsample negatives
        if neg_samples is not None and len(negatives) > neg_samples:
            negatives = random.sample(negatives, neg_samples)

        samples.extend(positives)
        samples.extend(negatives)

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
    neg_samples: int = 10,
    min_word_freq: int = 2,
    max_train_impressions: Optional[int] = None,
    max_dev_impressions: Optional[int] = None,
    seed: int = 42,
) -> Tuple:
    """
    End-to-end data preparation for NRMS training.

    Returns:
        train_dataset: MINDDataset for training (shuffled batches).
        dev_dataset: EvalMINDDataset for validation (preserves impression grouping).
        train_eval_dataset: EvalMINDDataset of training data for per-impression eval.
        vocab: word-to-index vocabulary.
        news_title_tokens: (num_news, max_title_len) tensor.
        num_news: total number of news articles.
    """
    random.seed(seed)

    # 1. Load data (pass nrows for fast smoke tests — avoids loading full file)
    train_behavior = load_behaviors(train_behaviors_path, nrows=max_train_impressions)
    train_news = load_news(train_news_path)
    dev_behavior = load_behaviors(dev_behaviors_path, nrows=max_dev_impressions)
    dev_news = load_news(dev_news_path)

    # 2. Build vocabulary from ALL news titles (train + dev)
    all_titles = pd.concat(
        [train_news["title"], dev_news["title"]], ignore_index=True
    ).fillna("").tolist()
    word_vocab = build_vocab(all_titles, min_freq=min_word_freq)
    print(f"Vocabulary size: {vocab_size(word_vocab)}")

    # 3. Build news title tokens + ID mapping (on combined news)
    all_news = pd.concat([train_news, dev_news], ignore_index=True).drop_duplicates(
        subset=["news_id"]
    ).reset_index(drop=True)

    news_title_tokens, news_id_to_idx = build_news_title_tokens(
        all_news, word_vocab, max_title_len,
    )
    num_news = len(news_title_tokens)
    print(f"Total news articles: {num_news}")

    # 4. Flatten impressions into training samples (with neg downsampling)
    train_samples = flatten_impressions(
        train_behavior, news_id_to_idx, max_history_len, neg_samples,
    )
    train_dataset = MINDDataset(train_samples)
    print(f"Training samples (neg-sampled): {len(train_samples)}")

    # 5. Flatten training impressions WITH group info for per-impression eval
    train_eval_samples, _ = flatten_impressions_with_groups(
        train_behavior, news_id_to_idx, max_history_len,
    )
    train_eval_dataset = EvalMINDDataset(train_eval_samples)
    print(f"Train eval samples (all impressions): {len(train_eval_samples)}")

    # 6. Flatten dev impressions WITH group info for per-impression eval
    dev_samples, _ = flatten_impressions_with_groups(
        dev_behavior, news_id_to_idx, max_history_len,
    )
    dev_dataset = EvalMINDDataset(dev_samples)
    print(f"Dev samples: {len(dev_samples)}")

    return train_dataset, dev_dataset, train_eval_dataset, word_vocab, news_title_tokens, num_news