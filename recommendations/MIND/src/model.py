"""
NRMS (Neural News Recommendation with Multi-head Self-Attention) model.

Architecture:
  NewsEncoder: word embeddings → multi-head self-attention → additive attention pooling
  UserEncoder: look up news embeddings → multi-head self-attention → additive attention pooling
  NRMSModel: dot product between user and candidate news vectors

All dimensions are inferred dynamically — no hardcoded sizes.
BERTNewsEncoder placeholder is ready for future use.
"""

import math
from abc import ABC, abstractmethod
from typing import Optional

import torch
import torch.nn as nn
import torch.nn.functional as F


# ---------------------------------------------------------------------------
# Additive Attention
# ---------------------------------------------------------------------------

class AdditiveAttention(nn.Module):
    """
    Additive attention (also called Bahdanau attention).
    Computes a weighted sum of input vectors.

    Args:
        embed_dim: Input feature dimension.
    """

    def __init__(self, embed_dim: int):
        super().__init__()
        self.attention_projection = nn.Linear(embed_dim, embed_dim, bias=False)
        self.attention_query = nn.Linear(embed_dim, 1, bias=False)

    def forward(self, x: torch.Tensor, mask: Optional[torch.Tensor] = None) -> torch.Tensor:
        """
        Args:
            x: (batch, seq_len, embed_dim)
            mask: (batch, seq_len) — 1 for valid positions, 0 for padding.

        Returns:
            (batch, embed_dim) — weighted sum.
        """
        # (batch, seq_len, embed_dim) -> (batch, seq_len, embed_dim)
        attn_hidden = torch.tanh(self.attention_projection(x))
        # (batch, seq_len, 1)
        attn_scores = self.attention_query(attn_hidden).squeeze(-1)

        if mask is not None:
            attn_scores = attn_scores.masked_fill(mask == 0, float("-inf"))

        # Handle all-masked rows: replace -inf with 0 before softmax to avoid NaN
        if mask is not None:
            all_masked = (mask.sum(dim=-1) == 0)  # (batch,)
            if all_masked.any():
                attn_scores = attn_scores.clone()
                attn_scores[all_masked] = 0.0

        attn_weights = F.softmax(attn_scores, dim=-1)  # (batch, seq_len)
        # (batch, embed_dim) weighted sum
        weighted = torch.bmm(attn_weights.unsqueeze(1), x).squeeze(1)
        return weighted


# ---------------------------------------------------------------------------
# Multi-Head Self-Attention
# ---------------------------------------------------------------------------

class MultiHeadSelfAttention(nn.Module):
    """
    Multi-head scaled dot-product self-attention.

    Args:
        embed_dim: Input and output feature dimension.
        num_heads: Number of attention heads.
    """

    def __init__(self, embed_dim: int, num_heads: int = 20):
        super().__init__()
        assert embed_dim % num_heads == 0, \
            f"embed_dim ({embed_dim}) must be divisible by num_heads ({num_heads})"
        self.num_heads = num_heads
        self.head_dim = embed_dim // num_heads

        self.q_linear = nn.Linear(embed_dim, embed_dim, bias=False)
        self.k_linear = nn.Linear(embed_dim, embed_dim, bias=False)
        self.v_linear = nn.Linear(embed_dim, embed_dim, bias=False)
        self.out_linear = nn.Linear(embed_dim, embed_dim, bias=False)

    def forward(self, x: torch.Tensor, mask: Optional[torch.Tensor] = None) -> torch.Tensor:
        """
        Args:
            x: (batch, seq_len, embed_dim)
            mask: (batch, seq_len) — 1 for valid positions, 0 for padding.

        Returns:
            (batch, seq_len, embed_dim)
        """
        batch_size, seq_len, embed_dim = x.shape

        # Linear projections and reshape for multi-head
        Q = self.q_linear(x).view(batch_size, seq_len, self.num_heads, self.head_dim)
        K = self.k_linear(x).view(batch_size, seq_len, self.num_heads, self.head_dim)
        V = self.v_linear(x).view(batch_size, seq_len, self.num_heads, self.head_dim)

        # Transpose to (batch, num_heads, seq_len, head_dim)
        Q = Q.transpose(1, 2)
        K = K.transpose(1, 2)
        V = V.transpose(1, 2)

        # Scaled dot-product attention
        scores = torch.matmul(Q, K.transpose(-2, -1)) / math.sqrt(self.head_dim)
        # scores: (batch, num_heads, seq_len, seq_len)

        if mask is not None:
            # mask: (batch, seq_len) -> (batch, 1, 1, seq_len)
            mask_expanded = mask.unsqueeze(1).unsqueeze(2)
            scores = scores.masked_fill(mask_expanded == 0, float("-inf"))

            # Handle all-masked rows to avoid NaN in softmax
            all_masked_rows = (mask.sum(dim=-1) == 0)  # (batch,)
            if all_masked_rows.any():
                # Expand to (batch, num_heads, seq_len, seq_len)
                all_masked_expanded = all_masked_rows.unsqueeze(1).unsqueeze(2).unsqueeze(3)
                scores = scores.clone()
                scores = scores.masked_fill(all_masked_expanded, 0.0)

        attn_weights = F.softmax(scores, dim=-1)
        attn_output = torch.matmul(attn_weights, V)  # (batch, num_heads, seq_len, head_dim)

        # Concatenate heads
        attn_output = attn_output.transpose(1, 2).contiguous().view(
            batch_size, seq_len, embed_dim,
        )
        return self.out_linear(attn_output)


# ---------------------------------------------------------------------------
# News Encoder Base (abstract)
# ---------------------------------------------------------------------------

class NewsEncoderBase(ABC):
    """
    Abstract base class for news encoders.

    Subclasses MUST define `news_embed_dim` attribute (int) after __init__.
    This lets UserEncoder and NRMSModel infer dimensions dynamically.
    """

    @property
    @abstractmethod
    def news_embed_dim(self) -> int:
        """Output dimension of encoded news vectors."""
        ...

    @abstractmethod
    def forward(
        self,
        news_ids: torch.Tensor,
        news_title_tokens: torch.Tensor,
    ) -> torch.Tensor:
        """
        Args:
            news_ids: (batch,) — indices of news articles (not used by CNN encoder,
                       but used by BERT encoder to look up precomputed embeddings).
            news_title_tokens: (num_news, max_title_len) — title token indices.

        Returns:
            (batch, news_embed_dim) — encoded news vectors.
        """
        ...


# ---------------------------------------------------------------------------
# CNN-based News Encoder (NRMS paper style)
# ---------------------------------------------------------------------------

class CNNNewsEncoder(nn.Module, NewsEncoderBase):
    """
    NRMS-style news encoder using word embeddings + multi-head self-attention
    + additive attention pooling.

    This is the default encoder. To switch to BERT later, replace this class
    with BERTNewsEncoder — UserEncoder and NRMSModel need no changes.
    """

    def __init__(
        self,
        vocab_size: int,
        word_embed_dim: int = 100,
        num_heads: int = 20,
        max_title_len: int = 20,
        dropout: float = 0.2,
    ):
        super().__init__()
        self._news_embed_dim = word_embed_dim
        self.max_title_len = max_title_len

        self.word_embedding = nn.Embedding(vocab_size, word_embed_dim, padding_idx=0)
        self.self_attention = MultiHeadSelfAttention(word_embed_dim, num_heads)
        self.attention_pooling = AdditiveAttention(word_embed_dim)
        self.dropout = nn.Dropout(dropout)

    @property
    def news_embed_dim(self) -> int:
        return self._news_embed_dim

    def forward(
        self,
        news_ids: torch.Tensor,
        news_title_tokens: torch.Tensor,
    ) -> torch.Tensor:
        """
        Args:
            news_ids: (batch,) — indices into news_title_tokens.
            news_title_tokens: (num_news, max_title_len) — full title token matrix.

        Returns:
            (batch, word_embed_dim) — encoded news vectors.
        """
        # Look up title tokens for the given news IDs
        title_tokens = news_title_tokens[news_ids]  # (batch, max_title_len)

        # Create mask: 1 for non-pad tokens
        mask = (title_tokens != 0).long()  # (batch, max_title_len)

        # Word embeddings
        word_vecs = self.word_embedding(title_tokens)  # (batch, max_title_len, embed_dim)
        word_vecs = self.dropout(word_vecs)

        # Multi-head self-attention
        attn_out = self.self_attention(word_vecs, mask)  # (batch, max_title_len, embed_dim)

        # If all tokens are padding for some news, attn_out will be zeros — but mask ensures no NaN
        # Additive attention pooling
        news_vec = self.attention_pooling(attn_out, mask)  # (batch, embed_dim)

        return news_vec


# ---------------------------------------------------------------------------
# BERT-based News Encoder (placeholder)
# ---------------------------------------------------------------------------

class BERTNewsEncoder(nn.Module, NewsEncoderBase):
    """
    BERT-based news encoder using a pretrained transformer.

    To use:
        1. Precompute BERT embeddings for all news titles.
        2. Pass the precomputed embeddings matrix to this encoder.
        3. It simply looks them up — no forward BERT pass per batch.

    This design keeps training fast while using rich BERT representations.
    """

    def __init__(self, bert_embed_dim: int = 768, dropout: float = 0.2):
        super().__init__()
        self._news_embed_dim = bert_embed_dim
        self.dropout = nn.Dropout(dropout)

        # Projection layer (optional: reduce BERT dim to model dim)
        # self.projection = nn.Linear(bert_embed_dim, target_dim)

    @property
    def news_embed_dim(self) -> int:
        return self._news_embed_dim

    def set_precomputed_embeddings(self, embeddings: torch.Tensor):
        """
        Set the precomputed BERT embedding matrix.
        embeddings: (num_news, bert_embed_dim)
        """
        self.register_buffer("precomputed_embeddings", embeddings, persistent=True)

    def forward(
        self,
        news_ids: torch.Tensor,
        news_title_tokens: Optional[torch.Tensor] = None,
    ) -> torch.Tensor:
        """
        Args:
            news_ids: (batch,) — indices into precomputed embeddings.
            news_title_tokens: ignored (precomputed BERT embeddings used instead).

        Returns:
            (batch, bert_embed_dim) — news vectors.
        """
        news_vecs = self.precomputed_embeddings[news_ids]  # (batch, bert_embed_dim)
        news_vecs = self.dropout(news_vecs)
        return news_vecs


# ---------------------------------------------------------------------------
# User Encoder
# ---------------------------------------------------------------------------

class UserEncoder(nn.Module):
    """
    NRMS user encoder. Encodes a user's clicked news history into a single vector.

    Architecture:
        - Look up news embeddings for each clicked article via a shared NewsEncoder
        - Multi-head self-attention over the history sequence
        - Additive attention pooling -> user vector

    IMPORTANT: The news_embed_dim is inferred from the provided news_encoder.
    Works with any NewsEncoderBase subclass — no hardcoded dimensions.
    """

    def __init__(
        self,
        news_encoder: nn.Module,
        num_heads: int = 20,
        dropout: float = 0.2,
    ):
        super().__init__()
        self.news_encoder = news_encoder
        # Infer dimension from the news encoder
        embed_dim = news_encoder.news_embed_dim

        self.self_attention = MultiHeadSelfAttention(embed_dim, num_heads)
        self.attention_pooling = AdditiveAttention(embed_dim)
        self.dropout = nn.Dropout(dropout)

    def forward(
        self,
        history: torch.Tensor,
        news_title_tokens: torch.Tensor,
        history_mask: Optional[torch.Tensor] = None,
    ) -> torch.Tensor:
        """
        Args:
            history: (batch, max_history_len) — indices of news in user history.
            news_title_tokens: (num_news, max_title_len) — title token matrix.
            history_mask: (batch, max_history_len) — 1 for valid items, 0 for padding.
                          If None, inferred from zeros in history tensor.

        Returns:
            (batch, embed_dim) — user representation vector.
        """
        batch_size, max_history_len = history.shape

        # Infer mask from padding (0 = pad)
        if history_mask is None:
            history_mask = (history != 0).long()

        # Flatten history to get all news IDs, encode them, then reshape back
        flat_news_ids = history.view(-1)  # (batch * max_history_len)
        # Only encode non-padded news (pad idx = 0)
        is_pad = flat_news_ids == 0

        if is_pad.all():
            # Edge case: all history is padding; return zeros
            embed_dim = self.news_encoder.news_embed_dim
            return torch.zeros(batch_size, embed_dim, device=history.device)

        # Encode all unique news in this batch for efficiency
        unique_ids = torch.unique(flat_news_ids)
        unique_news_vecs = self.news_encoder(
            unique_ids, news_title_tokens,
        )  # (num_unique, embed_dim)

        # Create a mapping from unique IDs back to their positions
        mapping = {uid.item(): vec for uid, vec in zip(unique_ids, unique_news_vecs)}
        embed_dim = self.news_encoder.news_embed_dim

        # Build the history embedding tensor
        history_vectors = torch.stack([
            mapping[hid.item()] if hid.item() in mapping
            else torch.zeros(embed_dim, device=history.device)
            for hid in flat_news_ids
        ]).view(batch_size, max_history_len, embed_dim)

        history_vectors = self.dropout(history_vectors)

        # Multi-head self-attention over history
        attn_out = self.self_attention(history_vectors, history_mask)
        # (batch, max_history_len, embed_dim)

        # Additive attention pooling
        user_vec = self.attention_pooling(attn_out, history_mask)
        # (batch, embed_dim)

        return user_vec


# ---------------------------------------------------------------------------
# NRMS Model
# ---------------------------------------------------------------------------

class NRMSModel(nn.Module):
    """
    NRMS model: NewsEncoder + UserEncoder + dot-product scoring.

    Stores news_title_tokens as a persistent buffer so training code
    only needs to pass (history, candidates, labels) — no need to
    thread news_title_tokens through the DataLoader.

    Args:
        news_encoder: An instance of NewsEncoderBase (e.g., CNNNewsEncoder).
        user_encoder: An instance of UserEncoder (dimensions inferred from news_encoder).
    """

    def __init__(
        self,
        news_encoder: nn.Module,
        user_encoder: UserEncoder,
    ):
        super().__init__()
        self.news_encoder = news_encoder
        self.user_encoder = user_encoder
        self.embed_dim = news_encoder.news_embed_dim

        # Will be registered as a buffer by set_news_title_tokens()
        self._news_title_tokens: Optional[torch.Tensor] = None

    def set_news_title_tokens(self, tokens: torch.Tensor):
        """
        Register news title token matrix as a persistent buffer.
        Tokens: (num_news, max_title_len) LongTensor.
        """
        self.register_buffer("news_title_tokens", tokens, persistent=True)

    def forward(
        self,
        history: torch.Tensor,
        candidate_news: torch.Tensor,
        history_mask: Optional[torch.Tensor] = None,
    ) -> torch.Tensor:
        """
        Args:
            history: (batch, max_history_len) — user's clicked news indices.
            candidate_news: (batch,) — candidate news index for each sample.
            history_mask: Optional (batch, max_history_len).

        Returns:
            (batch,) — logits (before sigmoid).
        """
        news_tokens = self.news_title_tokens

        # Encode user
        user_vec = self.user_encoder(history, news_tokens, history_mask)
        # (batch, embed_dim)

        # Encode candidate news
        candidate_vec = self.news_encoder(candidate_news, news_tokens)
        # (batch, embed_dim)

        # Dot product -> logit
        logits = (user_vec * candidate_vec).sum(dim=-1)
        # (batch,)

        return logits

    def predict(
        self,
        history: torch.Tensor,
        candidates: torch.Tensor,
    ) -> torch.Tensor:
        """
        Score multiple candidates for a single user history.

        Args:
            history: (1, max_history_len) — single user.
            candidates: (num_candidates,) — candidate news indices.

        Returns:
            (num_candidates,) — scores.
        """
        self.eval()
        with torch.no_grad():
            news_tokens = self.news_title_tokens
            user_vec = self.user_encoder(history, news_tokens)
            user_vec_expanded = user_vec.expand(len(candidates), -1)
            candidate_vecs = self.news_encoder(candidates, news_tokens)
            scores = (user_vec_expanded * candidate_vecs).sum(dim=-1)
        return scores


def build_default_nrms(
    vocab_size: int,
    word_embed_dim: int = 100,
    num_heads: int = 20,
    max_title_len: int = 20,
    dropout: float = 0.2,
) -> NRMSModel:
    """
    Build a default NRMS model with CNNNewsEncoder.

    Args:
        vocab_size: Size of the word vocabulary.
        word_embed_dim: Word embedding dimension (also the news/user embedding dim).
        num_heads: Number of attention heads (must divide word_embed_dim).
        max_title_len: Max title token length.
        dropout: Dropout rate.

    Returns:
        NRMSModel instance ready for training.
    """
    news_encoder = CNNNewsEncoder(
        vocab_size=vocab_size,
        word_embed_dim=word_embed_dim,
        num_heads=num_heads,
        max_title_len=max_title_len,
        dropout=dropout,
    )
    user_encoder = UserEncoder(
        news_encoder=news_encoder,
        num_heads=num_heads,
        dropout=dropout,
    )
    model = NRMSModel(news_encoder=news_encoder, user_encoder=user_encoder)
    return model