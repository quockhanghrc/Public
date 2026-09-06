# Ratings Integration — Results Grid (Amazon Beauty, S512)

Pipeline: Qwen2.5-0.5B (LoRA r=32, narrow-head) @ max_seq=512, RQ-KMeans @512 tokenizer (4-token SID space: 4×256 = 1,024 positions). Ratings experiment: expR1.

Baseline (expN1) and ratings variant (expR1) both: 12,000 steps, eval_subset=200, same backbone, same NarrowSIDHead + TrieLogitsProcessor (no model-output change). Ratings (`overall` 1–5 from Beauty_5.json) are interleaved as `<rating_1>`..`<rating_5>` tokens inside INPUT history ONLY — the model still predicts bare 4-token SIDs; no change to head/logits.

---

## Model grid — Ratings integration (eval_subset=200, T4)

| Exp | Model | Tokenizer (SID) | NDCG@5 | NDCG@10 | NDCG@20 | Recall@5 | Recall@10 | Recall@20 |
|---|---|---|---|---|---|---|---|---|
| **N1** (baseline) | **Qwen2.5-0.5B (LoRA r=32, narrow-head)** | **RQ-KMeans @512** | **0.008** | **0.009** | **0.011** | **0.010** | **0.012** | **0.020** |
|| R1 | Qwen2.5-0.5B (LoRA r=32, narrow-head) + ratings in INPUT history | RQ-KMeans @512 | ⏳ | ⏳ | ⏳ | ⏳ | ⏳ | ⏳ |

> Baseline expN1 metrics copied from `results_grid.md` (12k full-run, same backbone, same tokenizer, no ratings input, same 200-user eval subset where comparable). R1 smoke-test metrics (two 60-step runs, T4): run 1 loss 10.68→5.61, val_loss 5.95 at step 50; run 2 loss 10.20→5.82, val_loss 5.84 at step 50. Eval all zeros (expected with untrained model). Full 12k run launched and progressing.

---

## Rating distribution (dataset: Beauty_5.json → `data/ratings.json`)

Ratings file: `data/ratings.json` (from Beauty_5.json `overall`, 22,363 users, 198,502 interactions → ~199k ratings). Distribution over all ratings:

| Rating | Count | % |
|---|---|---|
| 1 | 10,526 | 5.3% |
| 2 | 11,456 | 5.8% |
| 3 | 22,248 | 11.2% |
| 4 | 39,741 | 20.0% |
| 5 | 114,531 | 57.7% |
| **Total** | **198,502** | **100.0%** |

Key observation: **58% of ratings are 5-star**. The ratings signal on the Beauty dataset is heavily right-skewed — most user-item pairs are already positive. Any NDCG/Recall lift from inserting ratings into INPUT history will have to overcome this weak-differentiation baseline; the ratings tokens are mostly `<rating_5>` and thus carry limited discriminative signal.

---

## Design constraint — ratings only in INPUT history

The expR1 modification (per `configs/expR1_slm_narrow_ratings_s512.json`) is **narrow** by design:

- Ratings (`<rating_1>` … `<rating_5>`) are interleaved in the INPUT sequence history (before the 4 SID target slots).
- The model's output space is **unchanged**: NarrowSIDHead still emits exactly 4 SID tokens (1,024 classes from 4×256 RQ-KMeans codebooks). No `<rating_*>` token is ever emitted by the model; no change to `TrieLogitsProcessor` or beam constraints.
- Train/eval loss is computed only over the bare 4-token SID target — ratings do not contribute to the loss; they act purely as contextual input features.

Consequence for analysis: if expR1 shows lift over N1 at `eval_subset=200`, the lift comes from the ratings tokens conditioning the hidden state before the SID decode — not from any change to the retrieval mechanism. If no lift appears, the weak 5-star skew (57.7% at 5) explains why: the input tokens are mostly identical (`<rating_5>`), giving the narrow head little extra information to separate good vs poor recommendations.

---

## Analysis framing

- **Baseline comparison:** expN1 (same backbone r=32, same tokenizer, same 12k steps, same 200-user eval subset, no ratings input). Direct head-to-head is fair; only variable is the interleaved ratings tokens in INPUT history.
- **Expected outcome range (given 58% 5-star skew):**
  - If ratings improve ranking even slightly, NDCG@5/10/20 and Recall@5/10/20 should tick up above the N1 baseline (0.008 / 0.009 / 0.011 NDCG; 0.010 / 0.012 / 0.020 Recall).
  - If ratings add noise or no signal (likely given the 5-star concentration), R1 should stay flat with N1 — or slightly below, since the longer input sequence (ratings tokens consume part of the 512-token budget, reducing available interaction history).
- **Secondary check:** compare per-user loss curves. Ratings interleaved in input may shift the training dynamics (more input tokens per step → lower effective batch diversity) even when the loss target (4 SID tokens) is unchanged.
- **Not comparable to full-grid A/B/C/C2:** expN1 / expR1 use a 600-user (full) or 200-user (subset) eval and a different backbone (Qwen2.5-0.5B + LoRA, not Sentence-T5). Cross-comparison with `results_grid.md` rows A–C is illustrative only.
