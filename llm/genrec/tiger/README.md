# TIGER: Generative Retrieval (Sentence-T5 content + RQ-KMeans tokenization)

A rerunnable implementation of **TIGER** ("Recommender Systems with Generative Retrieval", NeurIPS'23)
for the Amazon **Beauty** 5-core dataset, ported from `ref/nonameuntitled_tiger/`.

All new code and artifacts live in this `tiger/` folder; `ref/` is the read-only backup.

## Pipeline

```
reviews_Beauty_5.json.gz ──┐
meta_Beauty.json.gz ───────┤ 00_decompress ─► data/Beauty_5.json + data/metadata.json
                            │
01_preprocess ─► data/inter.json            (user_id -> [item_ids], core-5, sequential ids)
              ─► data/item_text.json        (per-item "Title/Categories/Description" text)
02_embed_sentencet5 ─► data/content_embeddings.pkl   (Sentence-T5, 768-dim, cached model)
03_rq_kmeans    ─► data/index_rqkmeans.json          (3 RQ-KMeans codes + collision solver = 4-token SID)
04_verify       (cross-file sanity checks)

train_tiger.py / train_sasrec.py            (generative T5 encoder-decoder / SASRec baseline)
```

## Setup (once)

```bash
uv venv .venv --python 3.11
uv pip install --python .venv/Scripts/python.exe -r requirements.txt
```

Sentence-T5 is downloaded **once** into `cache/hf/` and reused (no re-download).

## Run offline stage

```bash
bash run_all.sh
```

## Train

```bash
# quick CPU end-to-end smoke (small subset, bounded steps)
.venv/Scripts/python.exe train_tiger.py --params configs/smoke_tiger_config.json

# full run (GPU strongly recommended; beam search num_beams=100)
.venv/Scripts/python.exe train_tiger.py --params configs/tiger_train_config.json

# SASRec baseline
.venv/Scripts/python.exe train_sasrec.py --params configs/sasrec_train_config.json
```

## Metrics

Ranking metrics at the **semantic-ID level**, matching the reference:

- **NDCG@5/10/20** — a hit requires all 4 SID tokens to match the ground-truth next item's SID.
- **H@5/10/20 (Hit Rate)** — with one ground-truth next item per user, H@k == Recall@k numerically.

## Notes & choices

- **Embedder:** `sentence-transformers/sentence-t5-base` (768-dim) instead of the reference's LLaMA-7B:
  fast on CPU, content-only, closer to the paper's T5 family. Absolute numbers will differ from the
  reference README (which used LLaMA-7B); compare against the SASRec baseline for direction.
- **Metadata:** `meta_Beauty.json.gz` covers all 12,101 items (title 12,094 / categories 12,101 /
  description 11,163); the ~7 items missing title or ~938 missing description get review-text fill-in.
- **RQ-KMeans:** 3 residual 256-cluster codebooks (`random_state=42+i`) + 1 random collision-solver
  code → 4-token SID. `num_codebooks=4`, `codebook_size=256` in configs.
- **user_ids_count=2000** is the murmur-hash *bucket* count (real user ids are hashed `% 2000`);
  it need not be >= the 22,363 users.
- **CPU vs GPU:** the full config (num_beams=100, top_k=20, ~22k users) is GPU-bound; use the
  `smoke_tiger_config.json` (small subset, beams=20, 20 steps) to validate the flow on CPU.

## Results — full run on Modal T4

Ran `configs/tiger_t4_config.json` (transformers 4.57.6, num_beams=100, 12,288 steps, eval every 2,048) on a Tesla T4. Final full-test metrics (see `results_t4.txt`):

| Metric | This run (S-T5 + RQ-KMeans) | Ref TIGER (RQ-VAE+LLaMA) | Ref SASRec |
|--------|-----------------------------|---------------------------|------------|
| NDCG@20 | 0.0346 | 0.0394 | - |
| Recall/H@20 | 0.0803 | 0.0880 | 0.0807 |

Reaches ~88% of reference NDCG@20 and ~91% of reference Recall@20; Recall@20 ≈ reference SASRec. Absolute numbers are **not** directly comparable to the reference (different tokenizer + embeddings) — compare directionally.