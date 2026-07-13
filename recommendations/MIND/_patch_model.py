"""One-shot patch for src/model.py: extend CNNNewsEncoder with category_mode."""
p = "src/model.py"
s = open(p, encoding="utf-8").read()

# 1) __init__ signature
old_init_sig = '''    def __init__(
        self,
        vocab_size: int,
        word_embed_dim: int = 50,
        num_heads: int = 5,
        max_title_len: int = 20,
        dropout: float = 0.2,
        bottleneck_dim: int = None,
    ):'''
new_init_sig = '''    def __init__(
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
    ):'''
assert old_init_sig in s, "init sig not found"
s = s.replace(old_init_sig, new_init_sig)

# 2) embedding block
old_embed = '''        self.word_embedding = nn.Embedding(vocab_size, word_embed_dim, padding_idx=0)
        # Input projection (after embedding lookup, before transformer blocks).
        if bottleneck_dim is not None:
            self.input_projection = nn.Linear(word_embed_dim, bottleneck_dim)
        else:
            self.input_projection = None'''
new_embed = '''        self.word_embedding = nn.Embedding(vocab_size, word_embed_dim, padding_idx=0)
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
            self.input_projection = None'''
assert old_embed in s, "embed block not found"
s = s.replace(old_embed, new_embed)

# 3) forward signature
old_fwd_sig = '''    def forward(
        self,
        news_ids: torch.Tensor,
        news_title_tokens: torch.Tensor,
    ) -> torch.Tensor:'''
new_fwd_sig = '''    def forward(
        self,
        news_ids: torch.Tensor,
        news_title_tokens: torch.Tensor,
        news_categories: torch.Tensor = None,
        news_subcategories: torch.Tensor = None,
    ) -> torch.Tensor:'''
assert old_fwd_sig in s, "fwd sig not found"
s = s.replace(old_fwd_sig, new_fwd_sig)

# 4) projection -> category injection
old_proj = '''        # Optional projection bottleneck: compress word_embed_dim -> bottleneck_dim
        if self.input_projection is not None:
            word_vecs = self.input_projection(word_vecs)  # (batch, max_title_len, bottleneck_dim)

        # 2x transformer block (residual + LayerNorm inside each block)'''
new_proj = '''        # Optional projection bottleneck: compress word_embed_dim -> bottleneck_dim
        if self.input_projection is not None:
            word_vecs = self.input_projection(word_vecs)  # (batch, max_title_len, bottleneck_dim)

        # Category / subcategory conditioning (Options 1 & 2).
        cat_vec = None
        if self.category_mode in ("concat", "cross") and news_categories is not None:
            ce = self.cat_embed(news_categories[news_ids])        # (B, cat_embed_dim)
            se = self.subcat_embed(news_subcategories[news_ids])  # (B, subcat_embed_dim)
            cat_vec = torch.cat([ce, se], dim=-1)               # (B, cat_total)
            if self.category_mode == "concat":
                cat_vec = cat_vec.unsqueeze(1).expand(-1, word_vecs.size(1), -1)
                word_vecs = torch.cat([word_vecs, cat_vec], dim=-1)  # (B, seq, working_dim+cat)

        # 2x transformer block (residual + LayerNorm inside each block)'''
assert old_proj in s, "proj block not found"
s = s.replace(old_proj, new_proj)

# 5) pooling -> cross-attn override
old_pool = '''        # Additive attention pooling
        news_vec = self.attention_pooling(word_vecs, mask)  # (batch, word_embed_dim)

        return news_vec'''
new_pool = '''        # Additive attention pooling (or category-guided cross-attention for Option 2).
        if self.category_mode == "cross" and cat_vec is not None:
            # Category query attends over the (category-augmented) word vectors.
            news_vec = self.cat_attn(word_vecs, cat_vec, mask)  # (batch, working_dim)
        else:
            news_vec = self.attention_pooling(word_vecs, mask)  # (batch, working_dim)

        return news_vec'''
assert old_pool in s, "pool block not found"
s = s.replace(old_pool, new_pool)

open(p, "w", encoding="utf-8").write(s)
print("CNNNewsEncoder extended OK")
