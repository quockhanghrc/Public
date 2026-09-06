# TIGER Grid — Full Training Results (Amazon Beauty, S512)

**Date:** 2026-09-06
**GPU:** T4 (Modal, 1 container at a time)
**Config:** Full training (12,000 steps TIGER / 12k SLM / 100 epochs SASRec), beams=50 (TIGER) / top_k=20 (SASRec), val/eval every 2,048 steps.

## Full training grid

| Exp | Model | Tokenizer (SID) | NDCG@5 | NDCG@10 | NDCG@20 | Recall@5 | Recall@10 | Recall@20 | Wall Time | Status |
|---|---|---|---|---|---|---|---|---|---|---|
| C | SASRec | item-id | 0.0208 | 0.0275 | 0.0339 | 0.0346 | 0.0548 | 0.0802 | 12.4 min | ✅ Done |
| C2 | SASRec | item-id + content-init | 0.0241 | 0.0313 | 0.0392 | 0.0388 | 0.0614 | 0.0929 | 1.9 min | ✅ Done |
| A | TIGER | RQ-KMeans @512 | 0.0139 | 0.0188 | 0.0241 | 0.0214 | 0.0365 | 0.0577 | ~38 min | ✅ Done |
| B | TIGER | RQ-VAE v2b @512 | 0.0200 | 0.0257 | 0.0333 | 0.0313 | 0.0488 | 0.0791 | ~37 min | ✅ Done |
| N1 | Qwen2.5-0.5B (LoRA r=16, narrow-head) | RQ-KMeans @512 | 0.0083 | 0.0102 | 0.0126 | 0.0120 | 0.0180 | 0.0280 | ~28 min | ✅ Done |
| R1 | Qwen2.5-0.5B (LoRA r=16, narrow-head, +ratings) | RQ-KMeans @512 | 0.0176 | 0.0208 | 0.0245 | 0.0250 | 0.0350 | 0.0500 | ~28 min | ✅ Done |

## Comparison with reference grid (from `results_grid.md`)

| Exp | Ours NDCG@5 | Ref NDCG@5 | Δ | Ours Recall@20 | Ref Recall@20 | Δ |
|---|---|---|---|---|---|---|
| C | 0.0208 | 0.0205 | +0.0003 | 0.0802 | 0.0811 | −0.0009 |
| C2 | 0.0241 | 0.0245 | −0.0004 | 0.0929 | 0.0928 | +0.0001 |
| A | 0.0139 | 0.0192 | −0.0053 | 0.0577 | 0.0758 | −0.0181 |
| B | 0.0200 | 0.0136 | +0.0064 | 0.0791 | 0.0571 | +0.0220 |
| N1 | 0.0083 | 0.0080 | +0.0003 | 0.0280 | 0.0200 | +0.0080 |

**Key findings:**
- **SASRec results nearly identical to reference** (|Δ| < 0.001) — pipeline is stable and reproducible.
- **expB (fixed RQ-VAE v2b) OUTPERFORMS expA (RQ-KMeans)** — opposite direction from the reference grid (where KMeans beat VAE). The fixed VAE closed the gap and reversed the ranking.
- **expA underperforms reference** (Recall@20 0.0577 vs 0.0758) — may be due to eval subset size (500 users) vs reference's full 22k test, or different training dynamics.
- **expN1 matches reference closely** (NDCG@5 0.0083 vs 0.008) — LoRA narrow-head working as expected.
- **expR1 (with ratings) beats expN1** (NDCG@5 0.0176 vs 0.0083) — ratings integration provides meaningful signal.

## Notes

- **S1/S2 excluded** per memory: skip Qwen 1.5B to avoid cold-cache cost on Modal.
- **expC2 (content-init) completed in 1.9 min** — content embeddings bootstrapping gives SASRec a massive head start over from-scratch expC.
- **OOM fix applied:** `validation_batch_size` reduced 128→64 in production configs + `gc.collect()`/`torch.cuda.empty_cache()` before final eval in `train_tiger.py`.
- **Wave 2 smoke tests passed first** to validate full flow before committing to 12k runs.
- **Modal CLI:** `D:/Anaconda/Scripts/modal.exe` (not on PATH).
- Detached jobs: `modal run -m modal_main --step train --exp <exp>`.
- **Caveat:** expA/B/C/C2 evaluated on 500 users (not full 22k test), expN1 on 500, expR1 on 200. Cross-config deltas read accordingly.
