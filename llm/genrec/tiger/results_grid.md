# TIGER Grid — Results (Amazon Beauty, S512)

Pipeline: Sentence-T5 @ max_seq=512 → tokenizer → recommender. All experiments: 12,000 steps, beams=50 (TIGER) / top_k=20, val/eval every 2,048, T4.

## Model grid (final full-test metrics, T4)
| Exp | Model | Tokenizer (SID) | NDCG@5 | NDCG@10 | NDCG@20 | Recall@5 | Recall@10 | Recall@20 |
|---|---|---|---|---|---|---|---|---|
| A | TIGER | RQ-KMeans @512 | 0.0192 | 0.0250 | 0.0320 | 0.0299 | 0.0480 | 0.0758 |
| B | TIGER | RQ-VAE v2b @512 | 0.0136 | 0.0183 | 0.0235 | 0.0219 | 0.0364 | 0.0571 |
| C | SASRec | item-id | 0.0205 | 0.0273 | 0.0340 | 0.0334 | 0.0547 | **0.0811** |
| C2 | SASRec | item-id + content-init | 0.0245 | 0.0319 | 0.0397 | 0.0389 | 0.0620 | **0.0928** |
| (baseline) | TIGER | RQ-KMeans @256 (beams100) | 0.0205 | 0.0273 | 0.0346 | 0.0333 | 0.0530 | 0.0803 |
|| **N1** | **Qwen2.5-0.5B (LoRA r=16, narrow-head)** | **RQ-KMeans @512** | **0.008** | **0.009** | **0.011** | **0.010** | **0.012** | **0.020** |

> **S1/S2 status:** both detached (spawned via deployed `tiger-t4::train`), training in parallel on 2 T4s. Loss confirmed descending after the SFT fixes (random SID init + label-causal-shift alignment). See notes below / `GRID_NOTES.md`. **Caveat (unchanged):** S1/S2 metrics are on a 600-user eval subset (beam eval too slow for full 22k test) → NOT directly comparable to expA's full-test Recall@20. Come back to fill the ⏳ cells when the runs land.

## Headline findings
- **SASRec (content-init) is the best run: Recall@20 0.093, NDCG@20 0.040** — classic ranking beats TIGER's generative retrieval here.
- **RQ-KMeans > RQ-VAE for TIGER** (Recall@20 0.076 vs 0.057) — confirms the tokenizer-quality recommendation even with the fixed RQ-VAE.
- max_seq 256→512 did not help TIGER (0.080 → 0.076).
- Caveats: TIGER beams=50 (baseline used 100); from-scratch T5 TIGER, not the reference's LLaMA-7B+full setup.

## Tokenizer quality (filled automatically from `results_tokenizer.csv`)
| tokenizer | rel-MSE | cosine | cov b1/b2/b3 | distinct 3-codes | collisions |
|---|---|---|---|---|---|
| RQ-KMeans | 0.060 | 0.970 | 1.0/1.0/1.0 | 10,802 | 18.7% |
| RQ-VAE v2b (promoted) | 0.084 | 0.957 | 1.0/1.0/1.0 | 11,840 | 3.9% |
| RQ-VAE v1 (degenerate, bak) | 0.153 | 0.920 | 0.04/0.13/0.12 | 859 | 99.2% |

## Notes
- expB uses the **fixed RQ-VAE (v2b)** → fair RQ-KMeans-vs-RQ-VAE comparison for TIGER.
- beams=50 vs the @256 baseline's 100 → cross-config recall deltas read accordingly.