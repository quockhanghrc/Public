"""One-shot patch for src/model.py: thread category buffers through NRMSModel + build_default_nrms."""
p = "src/model.py"
s = open(p, encoding="utf-8").read()

# A) NRMSModel.__init__: add category buffers
old_init = '''        self.news_encoder = news_encoder
        self.user_encoder = user_encoder
        self.embed_dim = news_encoder.news_embed_dim

        # Will be registered as a buffer by set_news_title_tokens()
        self._news_title_tokens: Optional[torch.Tensor] = None'''
new_init = '''        self.news_encoder = news_encoder
        self.user_encoder = user_encoder
        self.embed_dim = news_encoder.news_embed_dim

        # Will be registered as buffers by set_news_title_tokens() / set_news_category_tokens()
        self._news_title_tokens: Optional[torch.Tensor] = None
        self._news_categories: Optional[torch.Tensor] = None
        self._news_subcategories: Optional[torch.Tensor] = None'''
assert old_init in s, "NRMSModel init not found"
s = s.replace(old_init, new_init)

# B) Add set_news_category_tokens() right after set_news_title_tokens()
anchor = '''        assert int(tokens[0].sum()) == 0, "news_title_tokens row 0 must be all zeros (padding)"
        self.register_buffer("news_title_tokens", tokens, persistent=True)'''
new_method = '''        assert int(tokens[0].sum()) == 0, "news_title_tokens row 0 must be all zeros (padding)"
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
        self.register_buffer("news_subcategories", subcategories.long(), persistent=True)'''
assert anchor in s, "set_news_title_tokens anchor not found"
s = s.replace(anchor, new_method)

# C) forward(): pass category to news_encoder calls
old_fwd = '''        news_tokens = self.news_title_tokens

        # Encode user
        user_vec = self.user_encoder(history, news_tokens, history_mask)
        # (batch, embed_dim)

        # Encode candidate news
        candidate_vec = self.news_encoder(candidate_news, news_tokens)'''
new_fwd = '''        news_tokens = self.news_title_tokens
        cat = getattr(self, "news_categories", None)
        sub = getattr(self, "news_subcategories", None)

        # Encode user
        user_vec = self.user_encoder(history, news_tokens, history_mask)
        # (batch, embed_dim)

        # Encode candidate news
        candidate_vec = self.news_encoder(
            candidate_news, news_tokens,
            news_categories=cat, news_subcategories=sub,
        )'''
assert old_fwd in s, "forward news_encoder call not found"
s = s.replace(old_fwd, new_fwd)

# D) predict(): pass category
old_pred = '''            news_tokens = self.news_title_tokens
            user_vec = self.user_encoder(history, news_tokens)
            user_vec_expanded = user_vec.expand(len(candidates), -1)
            candidate_vecs = self.news_encoder(candidates, news_tokens)'''
new_pred = '''            news_tokens = self.news_title_tokens
            cat = getattr(self, "news_categories", None)
            sub = getattr(self, "news_subcategories", None)
            user_vec = self.user_encoder(history, news_tokens)
            user_vec_expanded = user_vec.expand(len(candidates), -1)
            candidate_vecs = self.news_encoder(
                candidates, news_tokens,
                news_categories=cat, news_subcategories=sub,
            )'''
assert old_pred in s, "predict news_encoder call not found"
s = s.replace(old_pred, new_pred)

# E) score_candidates(): pass category
old_sc = '''        # Encode user once per impression
        user_vec = self.user_encoder(history, news_tokens)  # (B, D)

        # Encode all candidates in one batched pass
        flat_candidates = candidates.view(-1)  # (B * num_cand)
        candidate_vecs = self.news_encoder(flat_candidates, news_tokens)'''
new_sc = '''        # Encode user once per impression
        user_vec = self.user_encoder(history, news_tokens)  # (B, D)

        # Encode all candidates in one batched pass
        cat = getattr(self, "news_categories", None)
        sub = getattr(self, "news_subcategories", None)
        flat_candidates = candidates.view(-1)  # (B * num_cand)
        candidate_vecs = self.news_encoder(
            flat_candidates, news_tokens,
            news_categories=cat, news_subcategories=sub,
        )'''
assert old_sc in s, "score_candidates news_encoder call not found"
s = s.replace(old_sc, new_sc)

# F) score_candidates_detailed(): pass category
old_scd = '''        # Encode user once per impression (also grab history attention weights)
        user_vec, history_weights = self.user_encoder(
            history, news_tokens, return_attention=True,
        )  # (B, D), (B, max_history_len)

        # Encode all candidates in one batched pass
        flat_candidates = candidates.view(-1)  # (B * num_cand)
        candidate_vecs = self.news_encoder(flat_candidates, news_tokens)'''
new_scd = '''        # Encode user once per impression (also grab history attention weights)
        user_vec, history_weights = self.user_encoder(
            history, news_tokens, return_attention=True,
        )  # (B, D), (B, max_history_len)

        # Encode all candidates in one batched pass
        cat = getattr(self, "news_categories", None)
        sub = getattr(self, "news_subcategories", None)
        flat_candidates = candidates.view(-1)  # (B * num_cand)
        candidate_vecs = self.news_encoder(
            flat_candidates, news_tokens,
            news_categories=cat, news_subcategories=sub,
        )'''
assert old_scd in s, "score_candidates_detailed news_encoder call not found"
s = s.replace(old_scd, new_scd)

# G) build_default_nrms: add params + forward to CNNNewsEncoder
old_bd = '''def build_default_nrms(
    vocab_size: int,
    word_embed_dim: int = 50,
    num_heads: int = 5,
    user_num_heads: int = 5,
    max_title_len: int = 20,
    dropout: float = 0.2,
    bottleneck_dim: int = None,
) -> NRMSModel:'''
new_bd = '''def build_default_nrms(
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
) -> NRMSModel:'''
assert old_bd in s, "build_default_nrms sig not found"
s = s.replace(old_bd, new_bd)

old_bd_call = '''    news_encoder = CNNNewsEncoder(
        vocab_size=vocab_size,
        word_embed_dim=word_embed_dim,
        num_heads=num_heads,
        max_title_len=max_title_len,
        dropout=dropout,
        bottleneck_dim=bottleneck_dim,
    )'''
new_bd_call = '''    news_encoder = CNNNewsEncoder(
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
    )'''
assert old_bd_call in s, "build_default_nrms CNNNewsEncoder call not found"
s = s.replace(old_bd_call, new_bd_call)

open(p, "w", encoding="utf-8").write(s)
print("NRMSModel + build_default_nrms patched OK")
