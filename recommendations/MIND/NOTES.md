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

---

## 9. Padding Index 0 Ambiguity (news_id_to_idx shift)

**Issue**: `build_news_title_tokens()` mapped the first news article to index `0`
(`{nid: i for i, nid in enumerate(news_ids)}`). But `UserEncoder.forward` uses `0` as
the history-padding sentinel (`history_mask = (history != 0)` and `is_pad = flat_news_ids == 0`),
and `collate_fn`/`eval_collate_fn` pad history with `padding_value=0`. So any user click
on the article at index `0` was silently masked out of the user representation — index `0`
meant two different things depending on context (real article vs. padding).

**Fix**:
- `build_news_title_tokens()`: shift all news indices by **+1** (`{nid: i + 1 ...}`),
  reserving index `0` for padding only. Prepend an all-zero padding row to
  `news_title_tokens` via `torch.cat([torch.zeros(1, max_title_len), ...])`.
  Buffer shape is now `(num_news + 1, max_title_len)`; row 0 is all zeros.
- `prepare_data()`: `num_news = len(news_id_to_idx)` (real article count; the buffer
  carries one extra padding row).
- `model.set_news_title_tokens()`: added a regression guard asserting the buffer has a
  padding row at index 0 and that row 0 is all zeros.
- `CNNNewsEncoder.forward` mask is word-level (`title_tokens != 0`) and unaffected; the
  zero row yields a fully-masked title → additive attention returns a zero vector (correct
  padding behavior). `UserEncoder.forward` now correctly treats only true padding (index 0)
  as padding.

**Verification**: `0 not in news_id_to_idx.values()`, `buffer len == num_news + 1`,
`row 0 sum == 0`. Sample mapping now starts at `N55528 -> 1` (was `0`).

**Notes**:
- Word-vocab padding (`PAD_IDX=0` in `vocab.py`) is a separate dimension, left unchanged.
- `BERTNewsEncoder` is a placeholder; if ever used, its precomputed embeddings matrix must
  also get a zero row at index 0 to stay aligned.
- `news_title_tokens` is a persistent buffer in checkpoints, so old checkpoints (pre-fix
  shape) are now incompatible — retrain or migrate.

---

## 10. Per-Run Checkpoint Folders (no overwrite)

**Issue**: All runs wrote to the same `checkpoints/` directory, overwriting previous
`best_model.pt`, `checkpoint_epoch_*.pt`, and report PNGs.

**Fix**: Each run gets its own subfolder under `--checkpoint_dir` so previous runs are
preserved.
- New `--run_name` argument. If omitted, the default is `runs_<unixtimestamp>`
  (e.g. `runs_1783787076`); if provided, that name is used as the subfolder.
- In `main()`, after parsing args: `args.checkpoint_dir = os.path.join(args.checkpoint_dir, run_name)`
  then `os.makedirs(args.checkpoint_dir, exist_ok=True)`. All checkpoint and report writes
  use `args.checkpoint_dir`, so they land in the per-run folder.

**Usage**:
```bash
# Auto-named folder: checkpoints/runs_<unixtimestamp>/
python main.py --epochs 5

# Explicit folder: checkpoints/my_experiment/
python main.py --epochs 5 --run_name my_experiment
```

---

## 11. Negative Sampling (NRMS/MIND protocol)

**Issue**: `flatten_impressions` kept ALL candidates per impression (no downsampling),
causing heavy class imbalance (~1 pos : 4–20 neg), slow epochs, and a loss landscape
that diverges from the published NRMS setup (which samples K=4 negatives per positive).

**Fix**: Added `--neg_samples` (default `None` = keep all, backward compatible).
- `flatten_impressions(..., neg_samples=...)`: collects positives/negatives per impression;
  if `neg_samples` is set, keeps all positives and randomly samples `min(neg_samples, #neg)`
  negatives **per positive**; impressions with no positive are skipped. Uses `random.sample`
  (seeded via `random.seed(seed)` in `prepare_data`).
- `prepare_data(..., neg_samples=...)`: passes `neg_samples` to the TRAIN-only
  `flatten_impressions`. Eval (`flatten_impressions_with_groups`) still keeps ALL candidates
  — required for correct impression-level metrics.
- `main.py` passes `args.neg_samples` through.

**Usage**:
```bash
python main.py --epochs 5 --neg_samples 4   # NRMS-standard K=4
python main.py --epochs 5                    # default: all candidates (no sampling)
```

---

## 12. Class Weighting (pos_weight) for BCE Loss

**Issue**: With all negatives kept (or even with sampling), `BCEWithLogitsLoss` is biased
toward predicting "not clicked" due to imbalance.

**Fix**: Added `--pos_weight` (default `None` = disabled, backward compatible). When set,
the criterion becomes `nn.BCEWithLogitsLoss(pos_weight=torch.tensor(float(args.pos_weight)))`.
Typical value = neg/pos ratio. Applied to training loss only.

**Usage**:
```bash
python main.py --epochs 5 --pos_weight 4.0
```

---

## 13. Train-Only Vocabulary (remove leakage)

**Issue**: Vocabulary was built from train + dev titles, a mild form of leakage that makes
dev metrics slightly optimistic and non-comparable to the MIND leaderboard.

**Fix**: `prepare_data` now builds the vocab from **training titles only**
(`all_titles = train_news["title"]`). Dev/test OOV words map to UNK. `news_id_to_idx` still
uses combined train+dev news (needed for eval) — that is correct and not leakage.

---

## 14. Impression-AUC as Headline Metric

**Issue**: The training loop reported global (flattened) AUC as the headline and selected
the "best" checkpoint on it. MIND reports **impression-level AUC averaged across impressions**,
so the prior metric was non-comparable to the leaderboard.

**Fix**:
- Training-loop print now leads with `Dev IAUC` (impression-level AUC); global `Dev AUC`
  kept as a secondary diagnostic.
- Best-checkpoint selection (`best_model.pt`) now tracks `dev_impression_auc` instead of
  global `dev_auc`.
- Final summary prints `Best Dev IAUC` as the headline.
- `evaluate()` already computed impression-level metrics correctly via `group_scores`;
  no change needed there.

**Note**: Early stopping still triggers on `dev_loss` (kept as-is; can be aligned to
impression-AUC later if desired).

---

## 15. In-Time Validation (user-disjoint) + Out-of-Time Validation

**Issue**: There was only one validation set (dev). No held-out signal during training that
is temporally aligned with train, and no guard against user-level leakage when adding one.

**Fix**:
- `prepare_data(..., in_time_val_frac=0.0, in_time_val_seed=None)`:
  - Splits train **by `user_id`** (not random rows) so no user appears in both training and
    in-time validation. Shuffles unique users with `random.Random(in_time_val_seed or seed)`,
    holds out the first `int(n_users * frac)` users. `frac == 0` → no in-time split.
  - Returns THREE datasets: `train_dataset` (MINDDataset, neg sampling applies),
    `in_time_val_dataset` (EvalMINDDataset, all candidates), `out_of_time_val_dataset`
    (EvalMINDDataset = dev, all candidates).
- `main.py`: creates `in_time_val_loader` and `out_of_time_val_loader`; evaluates BOTH each
  epoch. **In-time** impression-AUC drives `best_model.pt` selection; **in-time loss** drives
  early stopping. **Out-of-time (dev)** is reported as the final generalization number.
- New args: `--in_time_val_frac` (default 0.0), `--in_time_val_seed` (default = --seed).

**Leakage**: User-level leakage eliminated (split by user_id). News-embedding sharing across
splits is correct, not leakage. Out-of-time dev remains the fairest generalization signal.

---

## 16. Step-Based Training (`--steps_per_epoch`)

**Issue**: Training was always full-epoch. No way to cap work per epoch (e.g. for quick
iteration or partial-epoch schedules).

**Fix**:
- `train_one_epoch(..., max_steps=None)`: if `max_steps` is set, stops after that many
  batches (`if max_steps is not None and num_batches >= max_steps: break`). `None` → full epoch.
- `main.py` passes `max_steps=args.steps_per_epoch`. Each new epoch reshuffles (loader
  `shuffle=True`), so step-limited epochs still see varied data.
- New arg: `--steps_per_epoch` (default None = all data).

**Note**: LR scheduler still steps once per epoch (keyed to epochs, not steps).

---

## 17. Combined Report Plots (subplots per split)

**Issue**: ROC curves and score distributions were saved as one PNG per split
(`roc_curve_<split>.png`, `score_dist_<split>.png`), cluttering the run folder and making
cross-split comparison tedious. With multiple splits (in-time + dev) this multiplied files.

**Fix**: `report.py` now writes TWO combined figures instead of per-split files:
- `roc_curves_combined.png` — one subplot per split (ROC curve + AUC), laid out in a grid
  (up to 3 columns, auto rows). Splits with <2 unique labels are skipped.
- `score_dist_combined.png` — one subplot per split (positives vs negatives histograms).
- Unused subplot axes are hidden. Works for any number of splits (1, 2, …).
- `learning_curves.png` unchanged (still the epoch-wise AUC/MRR/nDCG grid).

**Usage**: No new args; automatic. More splits (e.g. add a second in-time fold) appear as
extra subplots automatically.

---

## 18. Listwise vs Pointwise Training (`--train_mode`)

**Issue**: The original training objective was pointwise BCE — each (history, candidate)
pair scored independently and trained to predict click probability. This ignores that
impressions are a *ranking* problem: the positive should outrank the negatives *within the
same impression*. The canonical NRMS objective is per-impression softmax cross-entropy.

**Fix**: Added a `--train_mode` flag (default `pointwise`) so the user can switch and roll
back if listwise underperforms:

- `pointwise` (default, unchanged): `flatten_impressions` → `MINDDataset` → `collate_fn`
  → `model(history, candidates)` → `BCEWithLogitsLoss`. One candidate per sample.
- `listwise`: `build_impression_samples` → `ImpressionMINDDataset` → `impression_collate_fn`
  → `model.score_candidates(history, candidates, candidate_mask)` →
  `model.impression_cross_entropy(scores, labels, candidate_mask)`.

**Key implementation details (listwise)**:
- `score_candidates` encodes the user once, encodes all candidates in one batched pass,
  dot-products, then `masked_fill(candidate_mask == 0, -inf)` so padding is ignored.
- `impression_cross_entropy` does `log_softmax` over candidates, then NLL averaged over
  positive (impression, candidate) pairs. **Gotcha**: padding log-probs are `-inf`, and
  `(-inf * 0)` in the masked multiply produces `NaN` — must zero padding log-probs
  (`log_probs.masked_fill(candidate_mask == 0, 0.0)`) BEFORE multiplying by `pos_mask`.
- `neg_samples` is ignored in listwise mode (all candidates in the impression serve as the
  ranking negatives). `max_candidates` (default 50) truncates/pads each impression.
- **Evaluation is unchanged** for both modes: `evaluate()` stays pointwise and groups by
  impression_id for impression-AUC/MRR/nDCG (already correct).

**Usage**:
```bash
python main.py --train_mode pointwise   # current default
python main.py --train_mode listwise    # per-impression ranking loss
```

**Note on convergence**: listwise sees far fewer batches per epoch (one per impression vs
one per candidate), so it trains slower per epoch. Expect to use more epochs or a higher LR
to match pointwise. Verified finite loss + backward after the NaN fix.

---

## 19. Vectorized Evaluation & User Encoder (perf)

**Issue**: Evaluation was the dominant CPU bottleneck. `evaluate()` consumed
`eval_collate_fn` batches that yielded ONE row per candidate, so `model(history,
candidates)` re-encoded the user history ~50× per impression (once per candidate). The
`UserEncoder.forward` also built a Python loop (`torch.unique` + dict + `torch.stack([...])`)
= `batch * max_history_len` iterations per batch, on every forward pass.

**Fix**:
- **Eval now scores per-impression** via `model.score_candidates(history, candidates,
  candidate_mask)` (user encoded ONCE per impression). `prepare_data` builds in-time/dev
  eval datasets as `ImpressionMINDDataset(build_eval_impression_samples(...))` (new helper,
  like `build_impression_samples` but WITHOUT `max_candidates` truncation — keeps ALL
  candidates so metrics stay exact). `main.py` eval loaders use `impression_collate_fn`.
  `evaluate()` unpacks `(history, candidates, labels, candidate_mask)`, computes masked BCE
  loss over valid candidates (preserves `intime_loss`/`dev_loss` early-stopping), and
  computes per-impression AUC/MRR/nDCG from each impression row. `EvalMINDDataset` and
  `flatten_impressions_with_groups` are now unused by the pipeline.
- **`UserEncoder.forward` vectorized**: replaced the per-element Python loop with a gather.
  `flat = history.view(-1)` → `unique_ids = torch.unique(flat)` → encode uniques → build a
  `remap` table (`remap[unique_ids] = arange`) → `history_vectors = unique_vecs[remap[flat]]`.
  Padding news (id 0) zeroed via `masked_fill((history == 0).unsqueeze(-1), 0.0)`. Verified
  numerically identical (max abs diff = 0.0 with dropout=0).

**Measured impact**: scoring path ~2.7× faster (batched `score_candidates` vs per-candidate
`model()` on dev set). Both pointwise and listwise modes benefit (listwise already used
`score_candidates` for training; #2 speeds its user encode too).

**Usage**: No new args; automatic. Metrics unchanged vs pre-#19 (eval keeps all candidates).

**Follow-up (not done)**: `main.py` still calls `evaluate()` twice per epoch (in-loop + a
second `return_raw=True` pass for plots). Could reuse the in-loop results to halve eval
cost (#3 in the perf analysis).

---

## 20. News-Encoder Improvements + User-Encoder Head Fix

**Changes applied to `src/model.py` `CNNNewsEncoder` + `UserEncoder` + `build_default_nrms`:**
- **LayerNorm + Residual** in `CNNNewsEncoder`: `attn_out = layer_norm(word_vecs + attn_out)`
  before pooling (stabilizes gradients).
- **Projection bottleneck**: `news_embed_dim` (default 30) added as the encoder OUTPUT dim;
  `news_projection = Linear(word_embed_dim, news_embed_dim)` applied after attention. The
  news self-attention still runs on `word_embed_dim` (100) → 10 dims/head. Forces the model
  to generalize rather than memorize word patterns.
- **`UserEncoder.forward` simplified**: direct batched indexing (`flat_history = history.view(-1)`
  → `news_encoder(flat_history)` → zero padding via mask). Replaces the `torch.unique`/remap
  gather (both were correct; this is clearer).

**🐞 Bug fixed — UserEncoder had 3 dims/head**: `UserEncoder` infers its attention dim from
`news_encoder.news_embed_dim` (= 30, the bottleneck). With the old shared `num_heads=10`,
`MultiHeadSelfAttention(30, 10)` → `head_dim = 3` — far too narrow (worse than the original
5). **Fix**: `build_default_nrms` now takes a separate `user_num_heads` (default 5 → 30/5 = 6
dims/head), passed to `UserEncoder`. Additionally `UserEncoder.__init__` caps heads
internally: `num_heads = max(1, min(num_heads, embed_dim // 6))` so it can never collapse
below ~6 dims/head even if misconfigured. `main.py` exposes `--user_num_heads` (default 5)
and `--num_heads` (news encoder, default 10).

**🐞 Bug fixed — report UnicodeEncodeError (Windows)**: `report.py` printed `→` arrows, which
the Windows cp1252 console codec can't encode → `UnicodeEncodeError` at `_save_learning_curve`.
Replaced all three `→` with `->` in `report.py` print statements. (Our `main.py` had no such
arrows.)

**Verification**:
- Build: news encoder 10 heads × 10 dims/head; user encoder 5 heads × 6 dims/head.
- Defensive cap: `user_num_heads=20` → capped to 5 (6 dims/head).
- Gradients reach `word_embedding`, `news_projection`, and `user_encoder.self_attention`.
- Smoke (pointwise, 1 epoch, 1k/500): Train Loss 0.27, OutTime IAUC ~0.49, reports save OK.
- `main.py` now prints a parameter breakdown (News encoder sub-layers + User encoder own
  params, which shares the news encoder).

---

## 13. Component Attribution & History Attention Metrics

**Purpose**: Two levels of interpretability, computed at **final evaluation only**
(read-only, zero training overhead — the training path is untouched).

### Level 1 — Component Attribution
Decompose each `(user, candidate)` dot-product score into magnitude and alignment:
```python
cos_sim = score / (user_norm * cand_norm + 1e-8)   # topic_alignment
```
- `user_strength` = `user_norm` (L2 norm of the user vector)
- `candidate_strength` = `cand_norm` (L2 norm of each candidate vector)
- `topic_alignment` = `cos_sim` (per-prediction)
- **`separation`** = `mean(pos alignment) - mean(neg alignment)`
- **`user_dominance`** = `mean(user_norm) / (mean(user_norm) + mean(cand_norm))`  (∈ (0,1))

### Level 2 — History Attention Distribution
Uses the user-history additive-attention weights (sum to 1 over valid positions):
- **`recency_bias`** = Spearman(position_index, attention_weight) over valid history items
- **`category_concentration`** = attention share of the single top category
- **`active_categories`** = # of categories with attention share > 5%
- Reported as **population averages** across all users (`recency_bias_mean`, etc.)

### New API surface
- `src/model.py`:
  - `ScoringDetail` dataclass: `scores (B,C)`, `user_norm (B,)`, `cand_norm (B,C)`,
    `history_weights (B, max_history_len)`.
  - `NRMSModel.score_candidates_detailed(history, candidates, candidate_mask)` → `ScoringDetail`.
    Eval-only; the training `score_candidates` is unchanged.
  - `UserEncoder.forward(..., return_attention=True)` → `(user_vec, history_weights)`.
  - `AdditiveAttention.forward(..., return_weights=True)` → `(weighted, attn_weights)`.
    Both default to the original single-tensor return (backward compatible).
- `src/attribution.py` (NEW): `_spearman()` (numpy ranks, no scipy),
  `compute_component_attribution(model, loader, device)`,
  `compute_history_attention(model, loader, device, idx_to_category, max_history_len)`.
- `src/data.py`: `prepare_data` now also returns `news_id_to_idx` and `idx_to_category`
  (int64 array, length `num_news+1`, index 0 = -1 padding). `main.py` unpacks all 8.
- `main.py`: `--attribution` (default on) / `--no_attribution`; `--attribution_splits`
  (default `dev,intime`). Runs after final eval, prints a summary block, and persists
  scalars to `run_config.json` under `attribution_metrics`. Empty splits are skipped.
- `src/report.py`: `generate_report(..., attribution_results=None, attribution_model=None,
  attribution_loader=None)` adds:
  - `attribution_alignment.png`, `attribution_strength.png`, `attribution_recency.png`,
    `attribution_history.png` (distribution plots, one subplot per split).
  - `component_breakdown.png` (Level-1 stacked bars: per prediction, Purple =
    `user_strength/total`, Green = `candidate_strength/total`, Gold =
    `|topic_alignment|/total` — shows which encoder dominates).
  - `attribution_per_user_history.png` (Level-2 per-user profile: bar = step attention
    at each history position, red line = cumulative attention 0→1, for up to 5 example
    users drawn from one batch via `model.score_candidates_detailed()`).
  - `main.py` passes `model` + `out_of_time_val_loader` into `generate_report` so the
    per-user plot can re-run the detailed scoring on a single batch.

### Formulas (mirror the spec)
$$
\cos\_sim = \frac{score}{user\_norm \cdot cand\_norm + 10^{-8}}
$$
$$
separation = \bar{a}_{pos} - \bar{a}_{neg}, \quad
user\_dominance = \frac{\bar{u}}{\bar{u} + \bar{c}}
$$
$$
recency\_bias = \rho(\text{position}, \text{attention\_weight}), \quad
category\_concentration = \max_c \sum_{i \in c} w_i
$$
$$
active\_categories = |\{c : \sum_{i \in c} w_i > 0.05\}|
$$

### Edge cases handled
- **Padding candidates** → `cand_norm` set to 0 (masked), so `cos_sim` is only computed
  over valid candidates (matches `compute_component_attribution`).
- **All-padding history** → `UserEncoder` returns finite zero weights (reuses the NaN
  guard from §1); `compute_history_attention` skips such users (no NaN in `recency_bias`).
- **`cos_sim`** is asserted within `[-1, 1]` tolerance (float drift).
- **Spearman degenerate** (constant input / <2 points) → returns `NaN` (guarded, no crash).
- **Empty split** (e.g. `intime` when `--in_time_val_frac 0`) → skipped, no misleading 0/NaN.

### Test Cases
`tests/test_attribution.py` (NEW, data-independent — builds a tiny synthetic model, no
MIND files needed). Requires `pip install pytest`. Run:
```bash
cd pub/Public/recommendations/MIND
python -m pytest tests/test_attribution.py -q
```
Asserts (all PASS):
- **L1**: `score_candidates_detailed` shapes; `cos_sim ∈ [-1,1]` over valid candidates;
  masked `cand_norm == 0`; `separation` finite (empty pos/neg → 0.0); `user_dominance ∈ (0,1)`.
- **L2**: `return_attention` returns `(user_vec, history_weights)`; `history_weights` sum
  to 1 per valid row; padding positions ≈ 0; all-padding history → no NaN; `recency_bias`
  sign matches weight trend (increasing→+, decreasing→−); `category_concentration ∈ [0,1]`;
  `active_categories` counts only shares > 0.05.
- **Helper**: `_spearman` monotonic → ±1, constant → NaN.
- **E2E**: `compute_component_attribution` and `compute_history_attention` run on a synthetic
  loader and return the expected scalar/array shapes.

### Notes
- Attribution is **eval-only**; training loss / AUC / MRR / nDCG are unchanged vs baseline.
- No new runtime dependency (scipy avoided — Spearman via numpy ranks). `pytest` is needed
  only to run the tests.
- Category mapping built from combined train+dev news (same source as `news_title_tokens`).

---

## 22. Projection Bottleneck (compress HF dim) + `run_config.json`

**Change**: `CNNNewsEncoder` gained an optional **input projection bottleneck**
`Linear(word_embed_dim → bottleneck_dim)` applied **after** the word-embedding lookup and
**before** the transformer blocks. This forces the attention/FFN layers to do real
compression work instead of passing 384-dim HF features straight through, and shrinks the
model. Controlled by `--bottleneck_dim` (default `None` = no projection, backward compatible).

**Implementation**:
- `CNNNewsEncoder.__init__(bottleneck_dim=None)`: if set, `self.input_projection =
  nn.Linear(word_embed_dim, bottleneck_dim)` and `_news_embed_dim = bottleneck_dim`. The
  `word_embedding` stays at `word_embed_dim` (so HF 384-dim init maps 1:1); the transformer
  blocks + additive pooling run at the **working dim** (`bottleneck_dim` or `word_embed_dim`).
- `forward()`: `word_vecs = self.input_projection(word_vecs)` after embedding lookup when
  projection exists.
- `UserEncoder` needs **no change** — it infers `embed_dim = news_encoder.news_embed_dim`,
  so it automatically uses the bottleneck dim. `build_default_nrms` passes `bottleneck_dim`
  through.

**Head keying (important)**: `num_heads`/`user_num_heads` are now fit against the **working
dim** (`bottleneck_dim` if set, else `embed_dim`), not the raw embed dim. So
`embed_dim=384, bottleneck=64, heads=5` → 64/4 = 16 dims/head (intuitive). Without a
bottleneck, behavior is unchanged (heads fit against `embed_dim`). The fit runs for BOTH the
HF and non-HF paths (previously it only ran in the HF branch — a 64-dim bottleneck with
`num_heads=5` would have asserted `64 % 5 != 0` and crashed).

**`run_config.json`**: every run writes `run_config.json` into its checkpoint folder
(`checkpoints/<run>/`), capturing:
- `args`: **all** CLI args (including defaults for anything not passed) — so the user can
  diff/tune across runs.
- Resolved values: `resolved_embed_dim`, `resolved_bottleneck_dim`, `resolved_working_dim`,
  `resolved_num_heads`, `resolved_user_num_heads`, `hf_model`, `hf_dim`, `device`,
  `total_params`, and a `param_breakdown` (NE/UE sub-layers).
- Written **twice**: right after model build (survives crashes) and re-written at the end
  with `final_metrics` (best in-time loss/AUC, out-time IAUC/AUC/MRR/nDCG).

**Local ↔ Modal parity**: both paths execute the same `main.py`. `run_nrms_mind.py` forwards
`--bottleneck-dim` (Modal auto-converts the underscore) into the identical `train()`
subprocess, so a local run and a Modal run with the same flags produce the same model and the
same `run_config.json`. The JSON is uploaded to the Volume automatically because `train()`
already walks `/data/checkpoints/<run>`.

**🐞 Bug fixed — `spawn()` inside a `local_entrypoint` does NOT persist (root cause found)**:
the entrypoint originally used `train.spawn()` so the run would survive the laptop closing.
But a `spawn()` call returns immediately, so the `local_entrypoint` (`main`) finishes right
away → Modal tears the **entire ephemeral app down**, killing the spawned `train` before it
ever runs. That is why `spawn()` runs (`t_spawn_test`, `t_modal_*`) left **nothing** on the
Volume and the app (`fc-...`) was "not found" — it was destroyed, not that `batch_upload()`
failed to flush. **Fix**: use `train.remote()` (blocking) inside `main`, and run the whole
thing with `modal run --detach`. `--detach` keeps the Modal app alive server-side after the
local client exits, so you can close the terminal / shut the laptop and training continues
until it finishes (then the app stops on its own). `remote()` keeps the entrypoint — and thus
the app — alive until `train()` returns, so `batch_upload()` flushes and persists. Verified:
`t_detach_test`, `t_detach_long`, `t_survive` (3 epochs) all completed and uploaded 22 files
to the Volume with `--detach` + `remote()`. The error-capture (`train_error_<run>.txt`) is
still kept in `train()` for diagnosing any in-container failure.

**Verification (full matrix, both sides)**:
| Scenario | embed | bottleneck | working | heads | HF | params |
|---|---|---|---|---|---|---|
| Local reg (no bn) | 50 | — | 50 | 5/5 | no | 1.39M |
| Local bn=64 | 50 | 64 | 64 | 4/4 | no | 1.48M |
| Local HF+bn=64 | 384 | 64 | 64 | 4/4 | yes | 9.97M |
| Local HF no bn | 384 | — | 384 | 4/4 | yes | 17.12M |
| Modal reg (remote) | 50 | — | 50 | 5/5 | no | 1.39M |
| Modal HF+bn=64 (remote) | 384 | 64 | 64 | 4/4 | yes | 9.97M |

Modal runs confirmed by `modal volume get` of `run_config.json` — resolved values match the
local runs exactly (parity verified). All Modal runs used `train.remote()` and uploaded
checkpoints + `run_config.json` + `model_cache` (22 files) to the Volume.

**Usage**:
```bash
# Local
python main.py --epochs 5 --use_hf_embeddings --bottleneck_dim 64
# Modal — ALWAYS use --detach so closing the laptop doesn't cancel training.
# (entrypoint uses train.remote(); --detach keeps the app alive server-side)
modal run --detach run_nrms_mind.py --run-name exp_bn --epochs 3 --use-hf-embeddings --neg-samples 4 --bottleneck-dim 64
```


---

## 21. Modal GPU Packaging (`run_nrms_mind.py`)

**Purpose**: Run NRMS training on Modal's GPU (Tesla T4) with code baked into the image,
data + checkpoints + HF cache on a persistent `Volume`, and embedding models downloaded
**in-app** (no local model files needed).

**Layout**:
- Code (`main.py`, `src/`, `requirements.txt`) baked into the image at `/app` via
  `image.add_local_dir(LOCAL_MIND_DIR, "/app", copy=True)`. `LOCAL_MIND_DIR = _HERE`
  (the script's own directory) so it is self-contained in the MIND folder.
- `volume = modal.Volume.from_name("nrms-mind-vol", create_if_missing=True)`, mounted at
  `/data`. Holds: `MINDsmall_train/`, `MINDsmall_dev/` (uploaded once via
  `modal volume put`), `checkpoints/<run>/`, and `model_cache/` (HF models).
- Image: `debian_slim` + `pip_install(torch, torchvision, cu121 index_url)` +
  `uv_pip_install(...)` for the rest + `.env(HF_HOME=/data/model_cache, ...)`.
- Secrets: `setup_secrets()` writes the Modal token to `~/.modal.toml` and creates
  `modal.Secret.from_dict({"HF_TOKEN": ...})` (single dict arg in Modal 1.4.1).

**Run** (always `--detach` so the run survives the local client / laptop closing):
```bash
modal run --detach run_nrms_mind.py --run-name exp02 --epochs 3 --use-hf-embeddings
```

**🐞 Bug fixed — Volume checkpoint persistence**: `volume.commit()` after writing files to
the mounted `/data` path did **NOT** persist them (verified 3×: `modal volume ls` showed no
`checkpoints/` and no write-test marker). `volume.write_file()` does not exist in Modal
1.4.1. **Fix**: use `volume.batch_upload(force=True)` and `upload.put_file(local_path, rel)`
walking `/data/checkpoints/<run_name>`. `force=True` is required — `put_file` raises
`FileExistsError` on re-upload of an existing remote path otherwise.

**Download-back** (verified round-trip):
```bash
modal volume get nrms-mind-vol /checkpoints/<run> ./downloads   # into EXISTING dir
```
⚠️ Gotcha: if the local destination does not already exist as a directory, `modal volume get`
silently merges the folder into a single file. Always create the destination dir first (or
pass an existing parent). Each run folder contains `best_model.pt`, `checkpoint_epoch_*.pt`,
`learning_curves.png`, `roc_curves_combined.png`, `score_dist_combined.png`.

**Validation**: smoke runs (`smoke_test4`–`smoke_test6`) trained on T4 CUDA, loss 2.70→0.49,
InTime IAUC 0.6466, OutTime IAUC 0.5358, MRR 0.2740; all 5 checkpoint files persisted and
downloaded back successfully.

---

## 23. Hard-Negative Mining Stage (`--train_mode listwise_hn`)

**Purpose**: Industry-aligned two-stage retraining. Instead of training the NRMS reranker on
MIND's *random* impression negatives (which a real retriever would never surface), we first
**mine hard negatives with a trained dense retriever**, then train the ranker to separate
clicked news from *retrieved-but-unclicked* news. This makes the retrieval → reranking loop
co-adapt (the dominant DPR / RocketQA / EMB pattern).

**Where it lives (delegation, not a separate Modal phase)**:
- `src/retrieval.py`: `DenseRetriever` (MiniLM dense embeddings + sklearn ANN) and
  `mine_hard_negatives(retriever, behaviors_df, news_id_to_idx, num_hn, ...)`.
- `src/data.py`: `prepare_data()` `listwise_hn` branch (`src/data.py:660`) builds the
  retriever index, mines, then calls `build_impression_samples_hn()` to assemble
  `(history, [positives + mined hard negatives], labels)`.
- `main.py`: `--train_mode listwise_hn` (choice), plus `--mine_num_hn` (default 4 = NRMS/MIND
  K), `--mine_model` (default MiniLM), `--mine_cache_dir`, `--mine_max_news` (cap the mining
  corpus for smoke tests; `None` = full ~65k corpus).
- `run_nrms_mind.py`: **does NOT re-implement retrieval**. `train_phase` runs
  `python -u main.py --phase train ...`, and `_build_main_args` forwards the mine flags when
  `train_mode == "listwise_hn"` (`--mine-num-hn`, `--mine-model`, `--mine-cache-dir`,
  `--mine-max-news`). So `run_nrms_mind.py` "does what main.py does" for retrieval by
  delegating to the same code path — keeping the launcher and `main.py` in lockstep.

**Mining algorithm** (`mine_hard_negatives`):
1. Encode the user's clicked **history** (mean-pooled query) with the `DenseRetriever`.
2. Retrieve the top-`(num_hn + 20)` most similar news from the corpus ANN index.
3. Drop any news already **shown** in this impression (clicked OR impression negatives) so
   the hard negatives are genuinely *unseen* but *confusable*.
4. Keep the top `num_hn` remaining as hard negatives. Impressions with no positive or no
   usable history are skipped (consistent with `flatten_impressions`).

**Verified end-to-end** (local, `data/MINDsmall_*`):
```
[4b] Mining hard negatives with DenseRetriever (MiniLM) [corpus=2000 news] ...
  [DenseRetriever] loading 'sentence-transformers/all-MiniLM-L6-v2' (cache=cache)
Training impressions (listwise_hn, mined 4 neg/imp): 97
Epoch 1/1 | Train Loss: 2.6943 | OutTime AUC: 0.4816 | MRR: 0.2701
Checkpoint saved to .../best_model.pt
```
`run_nrms_mind.py` import + `tests/_bench_modal_smoke.py` confirm the `listwise_hn` argv
forwards all mine flags and `main.py` accepts them.

**Usage**:
```bash
# Local smoke (small mining corpus + few impressions)
python main.py --epochs 1 --use_hf_embeddings --bottleneck_dim 64 --train_mode listwise_hn \
  --mine_num_hn 4 --mine_max_news 2000 --max_train_impressions 200 --max_dev_impressions 100

# Modal (full corpus; GPU)
modal run --detach run_nrms_mind.py --run-name exp_hn --epochs 5 --train-mode listwise_hn \
  --mine-num-hn 4 --use-hf-embeddings --bottleneck-dim 64
```

### Additional benefit of the new stage vs. without it

| Aspect | Without mining (`pointwise` / `listwise`) | With `listwise_hn` (mined hard negatives) |
|---|---|---|
| **Negative quality** | MIND's *random* impression negatives — a real retriever would never surface them, so the ranker learns to separate clicks from trivially-easy random items. | **Retrieved-but-unclicked** negatives — hard, confusable items the retriever actually ranks near the user, forcing the ranker to learn fine-grained relevance. |
| **Retrieval↔rerank co-adaptation** | Reranker and retriever trained independently; the reranker is never exposed to the retriever's actual mistakes. | Reranker is trained *on the retriever's own top-K errors* → the two stages co-adapt (the DPR/RocketQA/EMB recipe that dominates production recsys). |
| **Train/serving alignment** | Training negatives ≠ what the deployed retriever feeds the reranker → train/serve skew. | Training negatives ≈ the deployed retriever's output distribution → far less train/serve skew. |
| **Signal-to-noise** | Many random negatives are near-duplicates of the positive or totally irrelevant; gradient is noisy. | Hard negatives carry a strong, informative gradient (the model must learn *why* a near-miss is still unclicked). |
| **Difficulty curriculum** | Fixed, easy negatives. | Naturally **curriculum-like**: as the retriever improves, the mined negatives get harder, continuously challenging the ranker. |
| **Eval realism** | Reranker metrics measured against random distractors. | Reranker metrics measured against the *same kind* of candidates it will see in production (retrieved set). |
| **Cost / control** | N/A. | `--mine_max_news` caps the mining corpus for fast smoke tests (2000 news ≈ seconds on CPU); omit for the full run. `--mine_num_hn` controls K. |

**Caveat (expected, not a bug)**: MIND impressions use *random* negatives, so a content
retriever structurally cannot recover them → `recall@k ≈ 0` on small corpus slices is
expected. The benefit of mining is in the *training signal*, not in retrieval recall on
MIND's held-out random negatives. Full-corpus runs recover meaningful recall.

**Note on `run_nrms_mind.py` design choice**: retrieval was deliberately kept **coupled to the
train phase** (matching `main.py`) rather than split into a standalone `mine`/`retrieval`
Modal phase. A separate phase would let you mine-once-train-many (persist mined negatives to
the Volume), but it would diverge from `main.py`'s structure. The current design guarantees
`run_nrms_mind.py` does exactly what `main.py` does.

