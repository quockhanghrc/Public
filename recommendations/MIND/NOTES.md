# Implementation Notes & Gotchas

## 1. NaN from All-Zero Attention Masks

**Issue**: When a user has an empty history (e.g., cold-start), the history mask is all zeros. Both `AdditiveAttention` and `MultiHeadSelfAttention` fill masked positions with `-inf` before softmax → softmax of all `-inf` produces `NaN`.

**Fix**: Before softmax, check if any row in the batch is fully masked, and replace its scores with zeros so softmax produces uniform weights instead of NaN.

```python
# In AdditiveAttention.forward
all_masked = (mask.sum(dim=-1) == 0)
if all_masked.any():
    attn_scores = attn_scores.clone()
    attn_scores[all_masked] = 0.0
```

Same pattern applied in `MultiHeadSelfAttention.forward`.

---

## 2. `news_title_tokens` Threading

**Issue**: The original model signature required passing `news_title_tokens` as a parameter through every `forward()` call, which meant the DataLoader / training loop had to carry it around awkwardly.

**Fix**: Register `news_title_tokens` as a persistent buffer inside `NRMSModel.set_news_title_tokens()`. Now the training loop only passes `(history, candidates)` — the buffer stays on the correct device automatically.

```python
model.set_news_title_tokens(news_title_tokens)  # called once after model.to(device)
```

---

## 3. AMP Deprecation Warnings (PyTorch 2.x)

**Issue**: `torch.cuda.amp.GradScaler()` and `torch.cuda.amp.autocast()` are deprecated in newer PyTorch. They trigger deprecation warnings.

**Fix**: Use `torch.amp.GradScaler("cuda", enabled=use_amp)` and `torch.amp.autocast("cuda", enabled=use_amp)`.

```python
scaler = torch.amp.GradScaler("cuda", enabled=use_amp and device.type == "cuda")
with torch.amp.autocast("cuda", enabled=use_amp):
    ...
```

---

## 4. CPU + GPU Compatibility

**Issue**: Training must work on both CPU and GPU. Using AMP on CPU is not supported (and unnecessary). Also, the full dev set (2.7M samples) is too large for CPU evaluation, making each epoch very slow.

**Solutions**:
- `get_device()` auto-detects CUDA vs CPU.
- AMP is only enabled when `device.type == "cuda"`.
- Batch size defaults: 128 (CPU), 512 (GPU).
- `neg_samples` controls data volume — lower for CPU quick tests, higher for GPU full runs.
- `num_workers=0` on all DataLoaders (Windows limitation with multiprocessing).

---

## 5. NewsEncoder Dimension Agnosticism (BERT-Ready)

**Issue**: The model needed to support different news encoders (CNN vs BERT) without hardcoding dimensions.

**Fix**: A `NewsEncoderBase` abstract class defines the interface. Both `CNNNewsEncoder` and `BERTNewsEncoder` (placeholder) expose `news_embed_dim`. `UserEncoder` infers its dimension from whichever encoder it receives.

To switch to BERT:
```python
# Instead of:
news_encoder = CNNNewsEncoder(vocab_size=V, word_embed_dim=100)

# Replace with:
news_encoder = BERTNewsEncoder(bert_embed_dim=768)
news_encoder.set_precomputed_embeddings(bert_embeddings)  # (num_news, 768)

# UserEncoder and NRMSModel adapt automatically — no changes needed.
```

---

## 6. Impression Parsing Edge Cases

**Issue**: The `impressions` column uses `{news_id}-{label}` format. Some rows may have malformed entries or missing news IDs.

**Fix**: `flatten_impressions()` uses `rsplit("-", 1)` to safely split on the last hyphen (handles hyphens in news IDs), and skips news IDs not found in the `news_id_to_idx` mapping with a single warning.

---

## 7. Vocabulary from Both Train & Dev

**Issue**: If vocabulary is built from training titles only, the dev set may contain out-of-vocabulary words, making evaluation inconsistent.

**Fix**: Build vocabulary from ALL news titles (train + dev combined) before any tokenization.

---

## 8. Random Seed for Reproducibility

**Issue**: Without setting seeds, results vary across runs.

**Fix**: `set_seed()` sets Python `random`, NumPy, and PyTorch seeds (including CUDA deterministic mode) at the start of `main()`.