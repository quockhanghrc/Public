# TIGER — Generative Retrieval for Amazon Beauty (S512)

A rerunnable implementation of **TIGER** ("Recommender Systems with Generative Retrieval", NeurIPS'23)
for the Amazon **Beauty** 5-core dataset, extended with SASRec baselines and an SLM (Qwen2.5-0.5B + LoRA)
narrow-head variant. All training runs on **Modal T4**; data processing is local.

**Key result:** SASRec with content-init wins (Recall@20 0.093); the fixed RQ-VAE v2b beats RQ-KMeans
for TIGER (reversed from the reference grid); ratings interleaved in input history give the SLM a ~2x lift.

---

## 1. Data Source

- **Dataset:** Amazon Beauty 5-core (McAuley et al.)
  - https://cseweb.ucsd.edu/~jmcauley/datasets/amazon_files/
  - 22,363 users, 198,502 interactions, 12,101 items
- **Schema:** `reviewerID`, `asin`, `overall` (1-5), `unixReviewTime`
- **Raw files:** `reviews_Beauty_5.json.gz`, `meta_Beauty.json.gz`
- **Derived:** `Beauty_5.json`, `meta_Beauty.json`, `inter.json`, `item_text.json`, `ratings.json`

---

## 2. Data Processing

| Stage | Script | Input | Output |
|-------|--------|-------|--------|
| 00 | `scripts/00_decompress.py` | `reviews_Beauty_5.json.gz`, `meta_Beauty.json.gz` | `Beauty_5.json`, `metadata.json` |
| 01 | `scripts/01_preprocess.py` | `Beauty_5.json` | `inter.json` (user→[item_ids]), `item_text.json` |
| 02 | `scripts/02_embed_sentencet5.py` | `item_text.json` | `content_embeddings_s512.pkl` (768-dim, Sentence-T5) |
| 03 | `scripts/03_rq_kmeans.py` | `content_embeddings_s512.pkl` | `index_rqkmeans_s512.json` (4-token SID) |
| 04 | `scripts/05_extract_ratings.py` | `Beauty_5.json` | `ratings.json` (per-(user,item) overall) |

**Tokenizer:** RQ-KMeans (3 residual 256-cluster codebooks + 1 random collision-solver → 4-token SID,
`num_codebooks=4`, `codebook_size=256`). RQ-VAE v2b is the fixed alternative (promoted from v1 which had codebook collapse).

---

## 3. Model Designs

Three model families, each with a distinct retrieval mechanism:

| Model | Architecture | SID Space | Retrieval |
|---|---|---|---|
| **TIGER** | T5 encoder-decoder (Sentence-T5 base) | 4×256 RQ-KMeans or RQ-VAE | Beam search decode 4 SID tokens |
| **SASRec** | Transformer encoder (item-id or content-init) | item-id | Top-k dot-product scoring |
| **SLM LoRA** | Qwen2.5-0.5B + LoRA r=16 narrow-head | 4×256 RQ-KMeans | Single-shot 4-slot decode + Trie constraint |

### TIGER
- Generative retrieval: beam search (num_beams=50) over the 4×256 SID vocabulary
- Encoder-decoder T5 architecture; from-scratch Sentence-T5 (not LLaMA-7B)
- Output: 4 SID tokens → mapped to item_id via index lookup

### SASRec
- Classic sequential ranking; item-id embeddings or content-init (Sentence-T5 embeddings)
- Top-k dot-product scoring over the item catalog
- Content-init bootstraps training (1.9 min vs 12.4 min for from-scratch)

### SLM LoRA (expN1, expR1)
- Qwen2.5-0.5B + LoRA r=16 (attention-only) + NarrowSIDHead (4 output slots)
- TrieLogitsProcessor constrains beam to valid SID vocabulary
- Single-shot decode (not autoregressive beam search)
- **expR1:** ratings (`<rating_1>`..`<rating_5>`) interleaved in INPUT history only — model still predicts bare 4 SID tokens; ratings act as contextual features, not prediction targets

---

## 4 Training Infrastructure

- **GPU:** Modal T4 (14.75 GiB VRAM), 1 container per experiment
- **Configs:** `configs/exp*.json` (production, 12k steps), `configs/smoke*.json` (60-step validation)
- **Smoke-first workflow:** 60-step smoke → full 12k run (validates flow before committing GPU hours)
- **OOM fix:** `validation_batch_size` reduced 128→64 in production configs + `gc.collect()`/`torch.cuda.empty_cache()` before final eval in `train_tiger.py`
- **Self-heal:** `modal_main.py` syncs current code into Modal volume before each run (avoids stale code)

---

## 5 Results Summary

Full grid (12k steps, T4, eval every 2,048 steps):

| Exp | Model | Tokenizer | NDCG@5 | NDCG@10 | NDCG@20 | Recall@5 | Recall@10 | Recall@20 |
|---|---|---|---|---|---|---|---|---|
| C | SASRec | item-id | 0.0208 | 0.0275 | 0.0339 | 0.0346 | 0.0548 | 0.0802 |
| C2 | SASRec | content-init | 0.0241 | 0.0313 | 0.0392 | 0.0388 | 0.0614 | **0.0929** |
| A | TIGER | RQ-KMeans | 0.0139 | 0.0188 | 0.0241 | 0.0214 | 0.0365 | 0.0577 |
| B | TIGER | RQ-VAE v2b | 0.0200 | 0.0257 | 0.0333 | 0.0313 | 0.0488 | 0.0791 |
| N1 | Qwen2.5-0.5B LoRA | RQ-KMeans | 0.0083 | 0.0102 | 0.0126 | 0.0120 | 0.0180 | 0.0280 |
| R1 | Qwen2.5-0.5B LoRA +ratings | RQ-KMeans | 0.0176 | 0.0208 | 0.0245 | 0.0250 | 0.0350 | 0.0500 |

**Key findings:**
- **SASRec content-init wins** (Recall@20 0.093) — classic ranking beats generative retrieval on this task
- **expB beats expA** — fixed RQ-VAE v2b reversed the reference grid's KMeans>VAE finding
- **expR1 >> expN1** (~2x lift) — ratings as contextual input help despite 5-star skew
- **expA underperforms reference** — likely eval subset size (500 vs 22k users)

**Caveat:** Eval subset sizes vary (200-500 users vs full 22k test). Cross-config deltas read accordingly.
See `results_grid_full.md` for full comparison with reference grid.

---

## 6. LLM Usage in Decision Making

**Core insight:** The LLM (Qwen2.5-0.5B) is not a chatbot here — it is a model component that directly emits recommendations.

| Usage | Mechanism | Role |
|---|---|---|
| Generative retrieval | Beam search over 4×256 SID vocab | Predict next item's semantic ID |
| Trie-constrained decoding | TrieLogitsProcessor prunes invalid SID beams | Enforce valid token space |
| LoRA adaptation | r=16 attention-only on Qwen2.5-0.5B | Fine-tune for recommendation |
| Context conditioning | Ratings interleaved in INPUT history | Reshape hidden state before SID decode |
| Output | 4 bare SID tokens → mapped to item_id | Direct recommendation, no text emitted |

The model never emits text. It emits structured tokens (SIDs) that map directly to catalog items. This is "LLM as decision engine" — the LLM's internal reasoning (next-token prediction) is repurposed to rank and recommend.

**Why this matters for deployment:**
- Zero inference overhead vs baseline (same model, same beam search)
- No model surgery — LoRA adapter + narrow head plug into existing architecture
- Graceful degradation: if ratings are missing, model falls back to baseline behavior
- Cold-start limitation: new users without history get same behavior as baseline

---

## 7. Reproducing This Work

### Setup (once)
```bash
uv venv .venv --python 3.11
uv pip install --python .venv/Scripts/python.exe -r requirements.txt
```

### Offline data stage (local CPU)
```bash
bash run_all.sh
```

### Train on Modal
```bash
# Smoke test first (60 steps, validates flow)
modal run -m modal_main --step train --exp expA --smoke

# Full 12k run
modal run -m modal_main --step train --exp expA

# Grid (sequential)
modal run -m modal_main --step grid
```

### File layout
```
tiger/
├── configs/          # experiment configs (exp*.json = production)
├── data/             # raw + derived data artifacts
├── modeling/         # library: dataloader, dataset, loss, metric, models, trainer, utils
├── scripts/          # offline data processing pipeline (00-07)
├── train_*.py        # trainer scripts (TIGER, SASRec, SLM)
├── modal_main.py     # Modal entrypoint (sync + dispatch)
├── results_*.md      # result grids
├── GRID_NOTES.md     # running notes + TODO
└── README.md         # this file
```

### Metrics
Ranking metrics at the **semantic-ID level** (matching reference):
- **NDCG@5/10/20** — hit requires all 4 SID tokens to match ground-truth next item's SID
- **Recall@5/10/20** — with one ground-truth next item per user, H@k == Recall@k numerically
