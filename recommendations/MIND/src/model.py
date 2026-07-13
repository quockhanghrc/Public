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
from dataclasses import dataclass
from typing import Optional

import torch
import torch.nn as nn
import torch.nn.functional as F


@dataclass
class ScoringDetail:
    """
    Output of `NRMSModel.score_candidates_detailed`.

    Attributes:
        scores: (B, num_candidates) raw dot-product scores (padding -> -inf).
        user_norm: (B,) L2 norm of each user vector.
        cand_norm: (B, num_candidates) L2 norm of each candidate vector.
        history_weights: (B, max_history_len) additive-attention weights over the
            user's history (sum to 1 over valid positions; padding positions ~0).
    """
    scores: torch.Tensor
    user_norm: torch.Tensor
    cand_norm: torch.Tensor
    history_weights: torch.Tensor


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

    def forward(
        self,
        x: torch.Tensor,
        mask: Optional[torch.Tensor] = None,
        return_weights: bool = False,
    ) -> torch.Tensor:
        """
        Args:
            x: (batch, seq_len, embed_dim)
            mask: (batch, seq_len) — 1 for valid positions, 0 for padding.
            return_weights: If True, also return the (batch, seq_len) softmax
                attention weights (used for history-attention attribution).

        Returns:
            (batch, embed_dim) — weighted sum.
            OR (weighted, attn_weights) tuple when return_weights=True.
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
        if return_weights:
            return weighted, attn_weights
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
        news_categories: torch.Tensor = None,
        news_subcategories: torch.Tensor = None,
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
# Transformer Block (multi-head self-attention + FFN + residual + LayerNorm)
# ---------------------------------------------------------------------------

class TransformerBlock(nn.Module):
    """
    A single pre-norm-style transformer block:
        x -> attn -> LayerNorm(x + dropout(attn)) -> FFN -> LayerNorm(x + dropout(ffn))
    Reused by both the news encoder and the user encoder.
    """

    def __init__(self, embed_dim: int, num_heads: int, ffn_dim: int, dropout: float = 0.2):
        super().__init__()
        self.attn = MultiHeadSelfAttention(embed_dim, num_heads)
        self.attn_ln = nn.LayerNorm(embed_dim)
        self.ffn = nn.Sequential(
            nn.Linear(embed_dim, ffn_dim),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(ffn_dim, embed_dim),
        )
        self.ffn_ln = nn.LayerNorm(embed_dim)
        self.dropout = nn.Dropout(dropout)

    def forward(self, x: torch.Tensor, mask: Optional[torch.Tensor] = None) -> torch.Tensor:
        # Self-attention sub-layer with residual + LayerNorm
        a = self.attn(x, mask)
        x = self.attn_ln(x + self.dropout(a))
        # Feed-forward sub-layer with residual + LayerNorm
        f = self.ffn(x)
        x = self.ffn_ln(x + self.dropout(f))
        return x


# ---------------------------------------------------------------------------
# Category-Aware Attention (Option 2)
# ---------------------------------------------------------------------------

class CategoryAwareAttention(nn.Module):
    """
    Cross-attention: a category/subcategory embedding acts as a QUERY that attends
    over the title-word vectors, producing a single category-conditioned context
    vector. This lets the category dynamically weight which words matter (e.g.
    "stock" matters more for finance than sports).

    Args:
        d_model: Dimension of the word vectors being attended over.
        cat_dim: Dimension of the concatenated (category + subcategory) embedding.
    """

    def __init__(self, d_model: int, cat_dim: int):
        super().__init__()
        self.query_proj = nn.Linear(cat_dim, d_model, bias=False)

    def forward(
        self,
        word_vecs: torch.Tensor,
        cat_vec: torch.Tensor,
        mask: torch.Tensor,
    ) -> torch.Tensor:
        """
        Args:
            word_vecs: (batch, seq_len, d_model) — title-word vectors.
            cat_vec:   (batch, cat_dim) — concatenated category+subcategory embedding.
            mask:       (batch, seq_len) — 1 for valid words, 0 for padding.

        Returns:
            (batch, d_model) — category-conditioned context vector.
        """
        query = self.query_proj(cat_vec).unsqueeze(1)  # (B, 1, d_model)
        scores = torch.matmul(query, word_vecs.transpose(-2, -1)) / math.sqrt(word_vecs.size(-1))
        # (B, 1, seq_len)
        scores = scores.masked_fill(mask.unsqueeze(1) == 0, float("-inf"))
        attn = F.softmax(scores, dim=-1)
        cat_context = torch.matmul(attn, word_vecs).squeeze(1)  # (B, d_model)
        return cat_context


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
        word_embed_dim: int = 50,
        num_heads: int = 5,
        max_title_len: int = 20,
        dropout: float = 0.2,
        bottleneck_dim: int = None,
        category_mode: str = "none",
        num_categories: int = 0,
        num_subcategories: int = 0,
        cat_embed_dim: int = 8,
        subcat_embed_dim: int = 8,
    ):
        super().__init__()
        # Optional projection bottleneck: compress word_embed_dim -> bottleneck_dim
        # BEFORE the transformer blocks. This forces the attention/FFN layers to do
        # real compression work instead of passing the (e.g. 384-dim HF) features
        # straight through. When bottleneck_dim is None, the working dim == word_embed_dim
        # and no projection is applied (backward compatible).
        self.bottleneck_dim = bottleneck_dim
        self._news_embed_dim = bottleneck_dim if bottleneck_dim is not None else word_embed_dim
        self.max_title_len = max_title_len

        self.word_embedding = nn.Embedding(vocab_size, word_embed_dim, padding_idx=0)
        # Category / subcategory embeddings (Option 1 concat / Option 2 cross-attn).
        # category_mode: "none" (default, backward compatible) | "concat" | "cross"
        self.category_mode = category_mode
        self.cat_total = 0
        if category_mode in ("concat", "cross"):
            self.cat_embed = nn.Embedding(num_categories + 1, cat_embed_dim, padding_idx=0)
            self.subcat_embed = nn.Embedding(num_subcategories + 1, subcat_embed_dim, padding_idx=0)
            self.cat_total = cat_embed_dim + subcat_embed_dim
            # Working dim grows by the concatenated category signal.
            base_dim = bottleneck_dim if bottleneck_dim is not None else word_embed_dim
            self._news_embed_dim = base_dim + self.cat_total
            if category_mode == "cross":
                self.cat_attn = CategoryAwareAttention(self._news_embed_dim, self.cat_total)
        else:
            self.cat_embed = None
            self.subcat_embed = None
        # Input projection (after embedding lookup, before transformer blocks).
        if bottleneck_dim is not None:
            self.input_projection = nn.Linear(word_embed_dim, bottleneck_dim)
        else:
            self.input_projection = None
        # 2 transformer blocks (attention + FFN + residual + LayerNorm) at the WORKING dim
        self.blocks = nn.ModuleList([
            TransformerBlock(self._news_embed_dim, num_heads, ffn_dim=self._news_embed_dim * 4, dropout=dropout)
            for _ in range(2)
        ])
        self.attention_pooling = AdditiveAttention(self._news_embed_dim)
        self.dropout = nn.Dropout(dropout)

    @property
    def news_embed_dim(self) -> int:
        return self._news_embed_dim

    def forward(
        self,
        news_ids: torch.Tensor,
        news_title_tokens: torch.Tensor,
        news_categories: torch.Tensor = None,
        news_subcategories: torch.Tensor = None,
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
        word_vecs = self.word_embedding(title_tokens)  # (batch, max_title_len, word_embed_dim)
        word_vecs = self.dropout(word_vecs)

        # Optional projection bottleneck: compress word_embed_dim -> bottleneck_dim
        if self.input_projection is not None:
            word_vecs = self.input_projection(word_vecs)  # (batch, max_title_len, bottleneck_dim)

        # Category / subcategory conditioning (Options 1 & 2).
        cat_vec = None
        if self.category_mode in ("concat", "cross") and news_categories is not None:
            ce = self.cat_embed(news_categories[news_ids])        # (B, cat_embed_dim)
            se = self.subcat_embed(news_subcategories[news_ids])  # (B, subcat_embed_dim)
            cat_vec = torch.cat([ce, se], dim=-1)               # (B, cat_total)
            # Both modes broadcast the category signal across the title sequence and
            # concatenate it to word_vecs BEFORE the transformer blocks, because the
            # blocks are built for the category-augmented dim (base_dim + cat_total).
            cat_vec_seq = cat_vec.unsqueeze(1).expand(-1, word_vecs.size(1), -1)
            word_vecs = torch.cat([word_vecs, cat_vec_seq], dim=-1)  # (B, seq, working_dim+cat)

        # 2x transformer block (residual + LayerNorm inside each block)
        for blk in self.blocks:
            word_vecs = blk(word_vecs, mask)

        # Additive attention pooling (or category-guided cross-attention for Option 2).
        if self.category_mode == "cross" and cat_vec is not None:
            # Category query attends over the (category-augmented) word vectors.
            news_vec = self.cat_attn(word_vecs, cat_vec, mask)  # (batch, working_dim)
        else:
            news_vec = self.attention_pooling(word_vecs, mask)  # (batch, working_dim)

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
        num_heads: int = 5,
        dropout: float = 0.2,
    ):
        super().__init__()
        # IMPORTANT: store the shared news_encoder as a plain attribute (NOT a
        # registered submodule). NRMSModel already owns news_encoder, so registering
        # it here would make model.parameters() traverse it twice (double-counting
        # the word embedding and applying 2x gradient to it). We keep a reference
        # only for forward() and dimension inference.
        self.__dict__["news_encoder"] = news_encoder
        # Infer dimension from the news encoder (no bottleneck now = word_embed_dim)
        embed_dim = news_encoder.news_embed_dim

        # Cap heads so each head keeps >= ~6 dims — otherwise attention collapses
        # toward random. 50 / 5 = 10 dims/head (fine).
        num_heads = max(1, min(num_heads, embed_dim // 6))

        # 2 transformer blocks (same architecture as the news encoder)
        self.blocks = nn.ModuleList([
            TransformerBlock(embed_dim, num_heads, ffn_dim=embed_dim * 4, dropout=dropout)
            for _ in range(2)
        ])
        self.attention_pooling = AdditiveAttention(embed_dim)
        self.dropout = nn.Dropout(dropout)

    def forward(
        self,
        history: torch.Tensor,
        news_title_tokens: torch.Tensor,
        history_mask: Optional[torch.Tensor] = None,
        return_attention: bool = False,
    ) -> torch.Tensor:
        """
        Args:
            history: (batch, max_history_len) — indices of news in user history.
            news_title_tokens: (num_news, max_title_len) — title token matrix.
            history_mask: (batch, max_history_len) — 1 for valid items, 0 for padding.
                          If None, inferred from zeros in history tensor.
            return_attention: If True, also return the (batch, max_history_len)
                additive-attention weights over the history (used for attribution).

        Returns:
            (batch, embed_dim) — user representation vector.
            OR (user_vec, history_weights) tuple when return_attention=True.
        """
        batch_size, max_history_len = history.shape

        # Infer mask from padding (0 = pad)
        if history_mask is None:
            history_mask = (history != 0).long()

        embed_dim = self.news_encoder.news_embed_dim

        # Edge case: all history is padding; return zeros
        if int(history_mask.sum()) == 0:
            zero_vec = torch.zeros(batch_size, embed_dim, device=history.device)
            if return_attention:
                # Uniform-ish weights over padding (all zero) — no NaN. The
                # additive-attention guard already yields finite weights here.
                zero_weights = torch.zeros(batch_size, max_history_len, device=history.device)
                return zero_vec, zero_weights
            return zero_vec

        # Direct batched indexing of the flattened history (no per-element Python loop).
        flat_history = history.view(-1)  # (B * H,)
        history_vectors = self.news_encoder(
            flat_history, news_title_tokens,
            news_categories=news_categories, news_subcategories=news_subcategories,
        )
        history_vectors = history_vectors.view(batch_size, max_history_len, -1)
        # Zero out padding positions
        history_vectors = history_vectors * history_mask.unsqueeze(-1).float()

        history_vectors = self.dropout(history_vectors)

        # 2x transformer block (residual + LayerNorm inside each block)
        for blk in self.blocks:
            history_vectors = blk(history_vectors, history_mask)

        # Additive attention pooling (optionally return the weights for attribution)
        if return_attention:
            user_vec, history_weights = self.attention_pooling(
                history_vectors, history_mask, return_weights=True,
            )
            # (batch, embed_dim), (batch, max_history_len)
            return user_vec, history_weights

        user_vec = self.attention_pooling(history_vectors, history_mask)
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

        # Will be registered as buffers by set_news_title_tokens() / set_news_category_tokens()
        self._news_title_tokens: Optional[torch.Tensor] = None
        self._news_categories: Optional[torch.Tensor] = None
        self._news_subcategories: Optional[torch.Tensor] = None

    def set_news_title_tokens(self, tokens: torch.Tensor):
        """
        Register news title token matrix as a persistent buffer.
        Tokens: (num_news + 1, max_title_len) LongTensor, where row 0 is the
        reserved all-zero padding row (index 0 must never be a real article).
        """
        # Regression guard: index 0 is reserved for padding, so the buffer must
        # have at least one extra row beyond the max real article index, and the
        # padding row must be all zeros.
        assert tokens.shape[0] >= 1, "news_title_tokens must have a padding row at index 0"
        assert int(tokens[0].sum()) == 0, "news_title_tokens row 0 must be all zeros (padding)"
        self.register_buffer("news_title_tokens", tokens, persistent=True)

    def set_news_category_tokens(self, categories: torch.Tensor, subcategories: torch.Tensor):
        """
        Register category + subcategory integer-id arrays as persistent buffers, indexed
        by news index (1-based, matching news_title_tokens; index 0 = padding = -1).
        Used by CNNNewsEncoder when category_mode is 'concat' or 'cross'.
        """
        assert categories.shape[0] >= 1, "news_categories must have a padding row at index 0"
        assert subcategories.shape[0] >= 1, "news_subcategories must have a padding row at index 0"
        self.register_buffer("news_categories", categories.long(), persistent=True)
        self.register_buffer("news_subcategories", subcategories.long(), persistent=True)

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
        cat = getattr(self, "news_categories", None)
        sub = getattr(self, "news_subcategories", None)

        # Encode user
        user_vec = self.user_encoder(
            history, news_tokens, history_mask,
            news_categories=cat, news_subcategories=sub,
        )
        # (batch, embed_dim)

        # Encode candidate news
        candidate_vec = self.news_encoder(
            candidate_news, news_tokens,
            news_categories=cat, news_subcategories=sub,
        )
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
            cat = getattr(self, "news_categories", None)
            sub = getattr(self, "news_subcategories", None)
            user_vec = self.user_encoder(
                history, news_tokens,
                news_categories=cat, news_subcategories=sub,
            )
            user_vec_expanded = user_vec.expand(len(candidates), -1)
            candidate_vecs = self.news_encoder(
                candidates, news_tokens,
                news_categories=cat, news_subcategories=sub,
            )
            scores = (user_vec_expanded * candidate_vecs).sum(dim=-1)
        return scores

    def score_candidates(
        self,
        history: torch.Tensor,
        candidates: torch.Tensor,
        candidate_mask: Optional[torch.Tensor] = None,
    ) -> torch.Tensor:
        """
        Score ALL candidates for each user in one forward pass (listwise training).

        Args:
            history: (B, max_history_len) — user histories.
            candidates: (B, num_candidates) — candidate news indices per user.
            candidate_mask: (B, num_candidates) — 1 for valid, 0 for padding.

        Returns:
            (B, num_candidates) — scores for each candidate (padding -> -inf).
        """
        B, num_cand = candidates.shape
        news_tokens = self.news_title_tokens

        # Encode user once per impression
        user_vec = self.user_encoder(
            history, news_tokens,
            news_categories=cat, news_subcategories=sub,
        )  # (B, D)

        # Encode all candidates in one batched pass
        cat = getattr(self, "news_categories", None)
        sub = getattr(self, "news_subcategories", None)
        flat_candidates = candidates.view(-1)  # (B * num_cand)
        candidate_vecs = self.news_encoder(
            flat_candidates, news_tokens,
            news_categories=cat, news_subcategories=sub,
        )
        candidate_vecs = candidate_vecs.view(B, num_cand, -1)  # (B, num_cand, D)

        # Dot product per candidate: (B, D) * (B, num_cand, D) -> (B, num_cand)
        scores = (candidate_vecs * user_vec.unsqueeze(1)).sum(dim=-1)

        # Mask padding candidates so softmax ignores them
        if candidate_mask is not None:
            scores = scores.masked_fill(candidate_mask == 0, float("-inf"))

        return scores

    def score_candidates_detailed(
        self,
        history: torch.Tensor,
        candidates: torch.Tensor,
        candidate_mask: Optional[torch.Tensor] = None,
    ) -> "ScoringDetail":
        """
        Like `score_candidates` but also returns the per-impression L2 norms of the
        user and candidate vectors plus the user-history attention weights. Used for
        component-attribution and history-attention analysis at evaluation time.

        Args:
            history: (B, max_history_len) — user histories.
            candidates: (B, num_candidates) — candidate news indices per user.
            candidate_mask: (B, num_candidates) — 1 for valid, 0 for padding.

        Returns:
            ScoringDetail with:
              scores:        (B, num_candidates) raw dot-product scores (padding -> -inf)
              user_norm:     (B,)                 L2 norm of each user vector
              cand_norm:     (B, num_candidates) L2 norm of each candidate vector
              history_weights: (B, max_history_len) additive-attention weights over
                              the user's history (sum to 1 over valid positions)
        """
        B, num_cand = candidates.shape
        news_tokens = self.news_title_tokens

        # Encode user once per impression (also grab history attention weights)
        user_vec, history_weights = self.user_encoder(
            history, news_tokens, return_attention=True,
            news_categories=cat, news_subcategories=sub,
        )  # (B, D), (B, max_history_len)

        # Encode all candidates in one batched pass
        cat = getattr(self, "news_categories", None)
        sub = getattr(self, "news_subcategories", None)
        flat_candidates = candidates.view(-1)  # (B * num_cand)
        candidate_vecs = self.news_encoder(
            flat_candidates, news_tokens,
            news_categories=cat, news_subcategories=sub,
        )
        candidate_vecs = candidate_vecs.view(B, num_cand, -1)  # (B, num_cand, D)

        # Dot product per candidate: (B, D) * (B, num_cand, D) -> (B, num_cand)
        scores = (candidate_vecs * user_vec.unsqueeze(1)).sum(dim=-1)

        # L2 norms (for cosine decomposition)
        user_norm = user_vec.norm(dim=-1)  # (B,)
        cand_norm = candidate_vecs.norm(dim=-1)  # (B, num_cand)

        # Mask padding candidates so softmax ignores them AND their norms read as 0
        if candidate_mask is not None:
            scores = scores.masked_fill(candidate_mask == 0, float("-inf"))
            cand_norm = cand_norm * candidate_mask.float()

        return ScoringDetail(
            scores=scores,
            user_norm=user_norm,
            cand_norm=cand_norm,
            history_weights=history_weights,
        )

    def impression_cross_entropy(
        self,
        scores: torch.Tensor,
        labels: torch.Tensor,
        candidate_mask: torch.Tensor,
    ) -> torch.Tensor:
        """
        Masked softmax cross-entropy over candidates within each impression.

        Canonical NRMS listwise objective: positive candidate(s) should rank above
        negatives. Handles multiple positives by averaging their NLL.

        Args:
            scores: (B, num_candidates) — raw scores from score_candidates.
            labels: (B, num_candidates) — binary labels (1 = clicked).
            candidate_mask: (B, num_candidates) — 1 for valid candidates.

        Returns:
            Scalar loss (mean NLL over positive (impression, candidate) pairs).
        """
        masked_scores = scores.masked_fill(candidate_mask == 0, float("-inf"))
        log_probs = torch.log_softmax(masked_scores, dim=-1)  # (B, num_cand)
        # Zero out padding positions so (-inf * 0) doesn't produce NaN below.
        log_probs = log_probs.masked_fill(candidate_mask == 0, 0.0)

        pos_mask = (labels == 1) & (candidate_mask == 1)  # (B, num_cand)
        num_pos = pos_mask.sum()
        if num_pos == 0:
            return torch.tensor(0.0, device=scores.device, requires_grad=True)

        nll = -(log_probs * pos_mask).sum() / num_pos
        return nll


def build_default_nrms(
    vocab_size: int,
    word_embed_dim: int = 50,
    num_heads: int = 5,
    user_num_heads: int = 5,
    max_title_len: int = 20,
    dropout: float = 0.2,
    bottleneck_dim: int = None,
    category_mode: str = "none",
    num_categories: int = 0,
    num_subcategories: int = 0,
    cat_embed_dim: int = 8,
    subcat_embed_dim: int = 8,
) -> NRMSModel:
    """
    Build a default NRMS model with CNNNewsEncoder (transformer-block variant).

    Args:
        vocab_size: Size of the word vocabulary.
        word_embed_dim: Word embedding dimension (also the news/user output dim; no
            bottleneck). The GloVe loader matches this dim automatically.
        num_heads: Number of attention heads for the NEWS encoder (50/5 = 10 dims/head).
        user_num_heads: Number of attention heads for the USER encoder (50/5 = 10
            dims/head). UserEncoder also caps this internally to keep >= ~6 dims/head.
        max_title_len: Max title token length.
        dropout: Dropout rate.
        bottleneck_dim: If set, compress word_embed_dim -> bottleneck_dim before the
            transformer blocks (forces the attention/FFN to do compression work).
            None = no projection (current default behavior).

    Returns:
        NRMSModel instance ready for training.
    """
    news_encoder = CNNNewsEncoder(
        vocab_size=vocab_size,
        word_embed_dim=word_embed_dim,
        num_heads=num_heads,
        max_title_len=max_title_len,
        dropout=dropout,
        bottleneck_dim=bottleneck_dim,
        category_mode=category_mode,
        num_categories=num_categories,
        num_subcategories=num_subcategories,
        cat_embed_dim=cat_embed_dim,
        subcat_embed_dim=subcat_embed_dim,
    )
    user_encoder = UserEncoder(
        news_encoder=news_encoder,
        num_heads=user_num_heads,
        dropout=dropout,
    )
    model = NRMSModel(news_encoder=news_encoder, user_encoder=user_encoder)
    return model