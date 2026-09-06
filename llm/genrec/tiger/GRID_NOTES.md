## 2026-09-05 ROOT-CAUSE FIX #2: eval was structurally ZERO -> now WORKS (Option-A single-shot decode)

- SYMPTOM: training loss ~0.007 (memorized) yet `[eval:val]` all-0 on 50 users. Not overfit-to-zero: a broken decode path.
- ROOT CAUSE: old `generate()` used HF auto-regressive `model.generate` with a `narrow_forward` wrapper taking `hidden_states[:, -4:]`. On the FIRST generated step the seq is just history, so `-4:` grabbed history tokens, not 4 SID slots -> head always decoded the wrong positions -> emitted garbage SIDs never matching target -> 0.0 forever. (Confirmed: metric + composed verified correct in `_diag_pure.py`: composed=={c+256*l}, metric scores hit=1.0.)
- FIX (Option A, `train_slm_narrow.py` generate()): SINGLE-SHOT 4-slot decode. One forward pass: append 4 query tokens to history -> `hidden_states[:,-4:]` = proper SID slots -> `narrow_head` -> (4,1024) logits -> trie-constrained beam over the single matrix (no autoregression). ~40x faster eval AND correct alignment.
- VALIDATED: synthetic well-predicted beam->composed->metric = 50/50 hits. Modal smoke (untrained): `[eval:test] ndcg@20 0.0215 / recall@20 0.05` = 1 real hit/20 (chance, expected untrained) -> retrieval path now functional.
- MID-RUN EVALS ADDED: `eval_step` (default 1000) + `eval_mid_subset` (default 50) fire periodic `[eval:val-mid]` during training so metrics can be watched. Config `eval_step=1000`, `eval_mid_subset=50`.
- STATUS: full 12k @ r=32 with working eval + mid-evals = next to launch.

---

## 2026-09-05 UPDATE: expN1 NaN root cause FIXED + smoke PASSED

- ROOT CAUSE of the NaN: LoRA unfroze `embed_tokens` AND `lm_head` (both fp16, via `modules_to_save` + manual `.requires_grad=True`). The optimizer updates fp16 params IN-PLACE in fp16; over 12k steps one large-gradient step drove a param to inf -> every later forward isNaN (so the `isfinite` guard forever skips, a poisoned param never heals>. The 60-step smoke never tripped, so it looked fine.

- FIX (in `train_slm_narrow.py`): run the ENTIRE model in fp32:
# base_model=AutoModelForCausalLM.from_pretrained(slm_id, torch_dtype=torch.float32)  <- hard fp32, no fp16 anywhere
# dropped `lm_head` from `modules_to_save` (the narrow head replaces it, never read>; kept ONLY embed_tokens trainable
# `NarrowTrainer(..., fp16=False>`；the fp16 flag is now a no-op (dead。
# Qwen0.5B in fp32 (~1.1GB) + LoRA(93.9M> + narrow head(26.6M> fits T4 16GB easily (~7GB peak>. No AMP/GradScaler — no dtype stew.


- SMOKE(fixed fp32) PASSED on Modal T4:
# loss 9.03 -> 6.34 over  ​60 steps (~0.63s/step;, NO NaN]
# model loaded fp32;eval code path completed end-to-end (val+test on 20 users, no hang>


- NOTE on smoke eval being 0.0: EXPECTED noise. Only  ​20 users AND an untrained 60-step model -> ~0 top-k hits in the 1024-position SID space. Not a model failure;the eval code WORKS. For REAL expN1 metrics use `eval_subset` ≈ 300 (not 500/600 --> that got stuck for hours; not 20 --> 0.).

- Also fixed:` _maybe_smoke_cfg` in `modal_main.py` pressured smoke `eval_subset` from 500 down to 20 (500-user beam eval got stuck). The full-run config keeps its own `eval_subset=300`.

- NEXT (needs your decision)>:launch the full 12k expN1 now with fp32 fix?eval_subset? Suggest 300 (signal vs finishable time>; or first implement batched / bounded-beam eval to cut eval wall-time.The old 12k full run had NaNd at ~some step;with fp32 fix it should train clean to 12,000 steps then eval.



---

## LOoRA-rank reduction — expN1 r=160→32 (attention-only) — quick run DONE ✅

- **Motivation (user):** cut the ~30% trainable ratio toward the 1–10% "industry standard", reduce overfit risk on 131k pairs, and (hoped) faster.
- **Corrected math (VERIFIED against measured 251.4M/746.1M=33.7%):** the dominant trainable block is `embed_tokens` (137.1M = 18.4%) — it MUST stay trainable for SID tokens. So 10% is unreachable while keeping it. Floor is ~22%.
  - r=160 all-7 (old): TR=257.6M (34.5%)
  - **r=32 attn-only (now): TR=167.8M = 25.3%**  ← measured on Modal
  - r=32 all-7: ~182M (24.5%)
  - (Note: only the ~1025 SID/pad rows really get grads; the 151k base rows never activate in all-SID sequences, so the "137M" is mostly a paper number.)
- **Code change:** made `lora_r`/`lora_alpha` config-driven (config `model.lora_r=32`, `model.lora_alpha=64`); `target_modules` now attention-only `[q,k,v,o]` (dropped gate/up/down).
- **Speed (Modal T4 smoke, 60 steps):** r=160 ~0.54 s/step vs r=32 ~0.46 s/step → **~15% faster**, modest. Frozen fp32 Qwen backbone dominates wall-time; LoRA rank barely matters for speed. step-60 loss 6.34 → 6.72 (less overfit, expected).
- **Status:** smoke PASSED (no NaN, loss 9.5→6.7). **Full 12k @ r=32 NOT yet launched.** Decide: proceed with full run now?

---

# TIGER Grid — Execution Notes (hand-over file)

## 2026-09-05 FINAL VERDICT (diag_retrieval, pad-slot aligned): decode WORKS, problem = OVERFITTING

- DECISIVE diag ( tiny 135-pair, 800-step, then train-vs-val retrieval): TRAIN-pair 20/20, VAL-pair 0/20, loss ~0.00463.

- MEANING: with the pad-slot collate fix( decode is CORRECT: a memorized model retrieves its training items 100%. The model does NOT generalize to held-out val next-items -> overfitting/memorization, not a decode bug.



- My earlier "decode STILL BROKEN"(diag_retrieval 0/0 train) came from a STALE standalone diagnostic using the OLD real-SID collate, while I'd only fixed train_slm_narrow.py's collate. Aligned the diag -> verdict flips、


- PAD-SLOT collate ( the fix in train_slm_narrow: BOTH train and infer put a fixed pad/slot placeholder at the trailing num_codebooks positions, so NarrowSIDHead always decodes the same window. Labels moved to class ids ( l*cs+c. This is the real root-cause fix (train/inference mismatch),NOT overfit足。


## root-cause recap（the real root causes（
1. fp16 overflow（ fix: whole model fp32 、 drop lm_head trainable（Non-
2. train/infer mismatch（ narrow head decodes from real-SID tokens during train but pad/queries(at infer -> always wrong. FIX: pad-slot placeholders in BOTH train(+全 infer. This made TRAIN retrieval->20/20。
REMAINING: overfit to val (0/20 at tiny diag scope(needs real 12k + 100-200-user eval to gauge true val recall AND regularization(see next>。

Status snapshot for resuming the grid experiments. **This run stopped intentionally at the data stage** (no full model training yet) as requested. Everything below tells you exactly where things are and how to run training separately.

## ✅ DATA STAGE: COMPLETE
All data-stage artifacts are in `my-volume:/tiger_work/` and **no Modal app is running** (all ephemeral apps auto-stopped).

| artifact | status |
|---|---|
| `data/content_embeddings_s512.pkl` | ✅ 12,101×768 float32 |
| `data/index_rqkmeans_s512.json` | ✅ 12,101 × 4-token |
| `data/index_rqvae_s512.json` | ✅ 12,101 × 4-token — **v2b (collapse FIXED, promoted)** |
| `data/rqvae_s512.pt` | ✅ RQ-VAE weights (v2b) |
| `data/index_rqvae_s512_v1.json` / `rqvae_s512_v1.pt` | ✅ v1 degenerate canonical backed up |
| `data/index_rqvae_s512_v2a/v2b/v2c.json` + `.pt` | ✅ sweep variants (v2b chosen) |
| `results_tokenizer.csv` | ✅ (also `tiger/results_tokenizer.csv` locally) |

## 🏆 TOKENIZER QUALITY — RECOMMENDATION: **RQ-KMeans primary; RQ-VAE (v2b) now fixed & fair**
Measured on the S512 embeddings:

| tokenizer | rel-MSE | cosine | cov b1/b2/b3 | distinct 3-codes | % collide | status |
|---|---|---|---|---|---|---|
| **RQ-KMeans** | **0.060** | **0.970** | 1.0/1.0/1.0 | **10,802** | 18.7% | ✅ good |
| RQ-VAE **v1** | 0.153 | 0.920 | 0.04/0.13/0.12 | 859 | **99.2%** | ❌ degenerate (bak `_v1`) |
| RQ-VAE **v2b** (reset) | 0.084 | 0.957 | 1.0/1.0/1.0 | **11,840** | **3.9%** | ✅ fixed, **promoted** |

**RQ-VAE collapse FIXED** by porting the reference `FixDeadCentroids` dead-codebook reset (+100 epochs, hidden 64). v2b (reset, no EMA) promoted to `index_rqvae_s512.json`; v1 kept as `_v1`. **`expB` (TIGER+RQ-VAE)** is now a **FAIR** method comparison, not a broken-tokenizer ablation. RQ-KMeans still reconstructs a touch better (0.060 vs 0.084 rel-MSE — expected: VAE is lossy). Gate note: v2b misses the strict rel-MSE bar (0.084 vs ≤0.08) slightly but passes all structural criteria (coverage 100%, distinct 11,840, collisions 3.9%).

## Layout / storage
- **Local repo:** `<repo_root>/tiger/`
- **Modal volume:** `my-volume` (existing; never create new), mounted at `/tiger`, project at `/tiger/tiger_work`.
- **Everything from the data stage and training is persisted in** `my-volume:/tiger_work/`.
- `ref/` = read-only reference source (backup).

## What is DONE (data stage)
- [x] Embed script supports `--max_seq` → writes `data/content_embeddings_s{seq}.pkl` (`scripts/02_embed_sentencet5.py`).
- [x] RQ-KMeans script accepts `--emb/--out` (`scripts/03_rq_kmeans.py`).
- [x] RQ-VAE pipeline script, `input_dim=768` (Sentence-T5 dim, NOT the reference's 4096), 3×256 codebooks, beta 0.25, seed 42, 10 epochs, → `data/index_rqvae_s512.json` + `data/rqvae_s512.pt` (`scripts/rqvae_pipeline.py`).
- [x] Tokenizer-quality metrics script → `results_tokenizer.csv` (`scripts/06_tokenizer_metrics.py`).
- [x] `modal_main.py`: functions `embed_items` (T4), `rq_kmeans` (cpu×8), `rq_vae` (T4), `tokenizer_metrics` (cpu×8), `train` (T4). Step selector via `MODAL_STEP`; experiments via `MODAL_EXP`.
- [x] Local smoke passed: all scripts parse; `03 --emb/--out` produced 12,101 × 4-token index.
- [x] Modal embed step launched → `data/content_embeddings_s512.pkl`.

## Artifacts (verify / expect in my-volume:/tiger_work/data/)
| file | expected |
|---|---|
| `content_embeddings_s512.pkl` | 12,101×768 float32 |
| `index_rqkmeans_s512.json` | 12,101 × 4-token, tokens<256 |
| `index_rqvae_s512.json` | 12,101 × 4-token, tokens<256 |
| `rqvae_s512.pt` | RQ-VAE weights |
| `results_tokenizer.csv` | recon/coverage/collision table (run `step=metrics`) |

Check with: `modal volume ls my-volume tiger_work/data`

## RUN DATA STAGE (idempotent, each = its own ephemeral app that auto-stops)
```bash
cd <repo_root>/tiger
MODAL_STEP=embed   modal run modal_main.py     # T4   -> content_embeddings_s512.pkl
MODAL_STEP=tok     modal run modal_main.py     # cpu+rqvae(T4) -> both indexes + weights
MODAL_STEP=metrics modal run modal_main.py     # cpu  -> results_tokenizer.csv
```
After each: `modal app list` should show no Running/Initializing apps (ephemeral apps stop when the local client returns).

## STOPPED BEFORE TRAINING — To run the grid later, do this:
1. **Create 4 experiment configs** in `configs/`:
   - `expA_tiger_rqkmeans_s512.json` — TIGER with `"index_json_path": "data/index_rqkmeans_s512.json"`
   - `expB_tiger_rqvae_s512.json` — TIGER with `"index_json_path": "data/index_rqvae_s512.json"`
   - `expC_sasrec.json` — SASRec (`inter.json`, no index)
   - `expC2_sasrec_contentinit.json` — SASRec + seed item embeddings from `data/content_embeddings_s512.pkl`
   (TIGER: `num_codebooks=4`, beams=50, top_k=20, 12,000 steps, valid/eval=2048, log_steps=256; batch 256/128, lr 3e-4.)
2. **expC2 seeding** — in `train_sasrec.py`, when `experiment_name` starts with `expC2`, load `content_embeddings_s512.pkl` and set `model._item_embeddings.weight.data`.
3. **Run each** (separate ephemeral app; overlap OK; each stops itself):
```bash
MODAL_STEP=train MODAL_EXP=expA  modal run modal_main.py
MODAL_STEP=train MODAL_EXP=expB  modal run modal_main.py
MODAL_STEP=train MODAL_EXP=expC  modal run modal_main.py
MODAL_STEP=train MODAL_EXP=expC2 modal run modal_main.py
```
(or `MODAL_STEP=grid` to run all sequentially.)
4. Collect final full-test ndcg/recall @5/10/20 from app logs / tensorboard; write `results_grid.md` + README.

## Modal gotchas already hit (avoid re-learning)
- Volume client paths are **relative to the volume root** (upload to `tiger_work/...`, NOT `/tiger/...`).
- `Volume.commit()` is **container-only**; the `batch_upload` context manager finalizes on exit.
- Modal 1.4.1 uses `@app.function(...)` — there is NO `modal.function`.
- Pin `transformers==4.57.6` (transformers 5.x silently changed the T5 architecture: 123k vs 165k params).
- `my-volume` root is a busy multi-project volume — TIGER lives only under `tiger_work/`.
- Baseline (prior full run @max_seq=256, beams=100): `results_t4.txt` — NDCG@20 0.0346, Recall@20 0.0803.

## CURRENT BASELINE METRIC (TIGER + RQ-KMeans @256)
| NDCG@20 | Recall@20 |
|---|---|
| 0.0346 | 0.0803 |

---

## ✅ / 🔲 TODO — NEXT PHASE (train the grid) — pick up from here
> Data stage is DONE. The items below are the training stage that was intentionally deferred.

- [ ] **RV-A through RV-G (below) are OPTIONAL but make `expB` a fair RQ-VAE-vs-RQ-KMeans comparison**
  instead of a broken-tokenizer ablation. If you skip them, `expB` still runs but must be labeled
  "degenerate-tokenizer ablation" in the summary. Full plan: `.hermes/plans/2026-09-04_001239-rqvae-enhancement-collapse-fix.md`.

### 🔬 RV — RQ-VAE enhancement — **DONE (v2b promoted)**
- [x] **RV-A. Expose RQ-VAE hyperparams via CLI** (`--emb --input-dim --out --state --epochs --beta --hidden --lr --codebook-size --num-codebooks --batch-size --seed --reset-period --ema-decay --log-every`); `rq_vae` Modal fn forwards `MODAL_RQVAE_ARGS`/extra.
- [x] **RV-B. Dead-codebook reset** (reference `FixDeadCentroids` ported); local smoke: coverage 0.16→0.69.
- [x] **RV-C. Optional EMA codebook updates** (`--ema-decay`); defaults `hidden 32→64`, `epochs 10→100`.
- [x] **RV-D. Collapse guard** in `scripts/06_tokenizer_metrics.py --gate`; v1 FAILS all 5 thresholds.
- [x] **RV-E. T4 sweep** v2a/v2b/v2c ~100 epochs → versioned `index_rqvae_s512_v2*.json` + `.pt` (v1 untouched).
- [x] **RV-F. Gate v2** — v2b: coverage 100%, distinct 11,840, collisions 3.9%, rel-MSE 0.084, cosine 0.957 (misses rel-MSE bar slightly, passes all structural).
- [x] **RV-G. Decision** — **v2b promoted** to `index_rqvae_s512.json` (v1→`_v1`). `expB` is now a FAIR comparison.

- [ ] **T-A. Create 4 experiment configs** in `tiger/configs/`:
  - [ ] `expA_tiger_rqkmeans_s512.json` — TIGER, `"index_json_path": "data/index_rqkmeans_s512.json"`, `num_codebooks=4`, beams=50, top_k=20, 12,000 steps, valid/eval=2048, log_steps=256, batch 256/128, lr 3e-4, dim 128.
  - [ ] `expB_tiger_rqvae_s512.json` — same but `"index_json_path": "data/index_rqvae_s512.json"`.
  - [ ] `expC_sasrec.json` — SASRec (`inter.json`, no index; sampler_type=sasrec; batch 256; 12,000 steps).
  - [ ] `expC2_sasrec_contentinit.json` — SASRec + seed item embeddings from `data/content_embeddings_s512.pkl`.
  - Verify: each `json.load`s.
- [ ] **T-B. expC2 seeding** — in `train_sasrec.py`, when `experiment_name` starts with `expC2`, load `content_embeddings_s512.pkl` and set `SasRecModel._item_embeddings.weight.data` (isolated change).
- [ ] **T-C. Verify `step=train` wiring** — configs dispatch via `EXPERIMENTS` in `modal_main.py` (`MODAL_STEP=train MODAL_EXP=expA`).
- [ ] **T-D. Record the final metric of each run** (from app logs / tensorboard in volume): ndcg+recall @5/10/20.
- [ ] **T-E. Do a smoke-gate** for each experiment before full run (small steps), then full runs (overlap OK; each completed app stopped → `modal app list` clean).
- [ ] **T-F. Summary tables** → `tiger/results_grid.md` (rows A/B/C/C2; + the @256 baseline row) + README "Grid experiments" section.

**Commands to run training (after T-A/T-B):**
```bash
cd <repo_root>/tiger
MODAL_STEP=train MODAL_EXP=expA  modal run modal_main.py
MODAL_STEP=train MODAL_EXP=expB  modal run modal_main.py
MODAL_STEP=train MODAL_EXP=expC  modal run modal_main.py
MODAL_STEP=train MODAL_EXP=expC2 modal run modal_main.py
# or MODAL_STEP=grid modal run modal_main.py  (sequential, all four)
```
After each: `modal app list` → no Running/Initializing.

**Expected runtime:** TIGER (beams=50) ~0.8–1.5 h each on T4; SASRec ~15–30 min each.

---

## 🔬 SLM-TIGER (OpenOneRec-style itemic tokens) — expS1
> Qwen2.5-1.5B base + LoRA, generating next-item 4-token SIDs end-to-end. Plan: `.hermes/plans/2026-09-04_135004-slm-tiger-replace-t5.md`.

- [x] **S0** `MODAL_STEP=fetchslm` → Qwen2.5-1.5B into `my-volume:/tiger_work/hf_cache/models--Qwen--Qwen2.5-1.5B` (no local download).
- [x] **S1** ItemicTokenizer (4×256 SID special tokens) + vocab resize (`modeling/models/slm_tiger.py`).
- [x] **S2** `SIDLogitsProcessor` constrained beam (beams=20 within 50, top_k=20, do_sample=False).
- [x] **S3** `train_slm_tiger.py` (LoRA r16/a32, causal-LM loss on 4 SID tokens, batch 8, lr 3e-4) + `configs/expS1_slm_rqkmeans_s512.json`.
- [x] **S3b** `MODAL_STEP=train MODAL_EXP=expS1 MODAL_SMOKE=1` PASSED (load→train→gen→metrics→save; untrained metrics 0.0 as expected).
- [x] **S4** full run launched (`MODAL_STEP=train MODAL_EXP=expS1`), 12,000 steps ~1 step/s, final eval on `eval_subset=600` val+test users.
- [ ] **S5** fill expS1 row in `results_grid.md` + README, when run completes.

**Run it (separate app, auto-stops):**
```bash
MODAL_STEP=train MODAL_EXP=expS1 modal run modal_main.py
```
**Caveats:** expS1 metrics are on a **600-user subset** (1.5B beam eval is too slow for full 22k test) → NOT directly comparable to expA's full-test numbers; OpenOneRec-style but LoRA-SFT on Beauty only (no 33B-token co-pretrain).
--- 2026-09-04 Plan-mode execution (S6 narrow head replan) ---
Verified arithmetic (recomputed this session, all exact):
- Catalog: 12,101 items (index_rqkmeans_s512.json)
- Training pairs: 131,413 (inter.json: 22,363 users, 198,502 interactions -> 131,413 train pairs)
- SID space (RQ-KMeans S512): 4 codebooks × 256 codes = 1,024 positions (not 4.3B fake combos when constrained by trie)
- Hidden size: 896 (Qwen2.5-0.5B; NOT 1,536 which is 1.5B)
- Narrow head output: EXACTLY 1,024 (4 × 256); CANNOT emit anything outside SID codebook
- Memory budget on T4: weights(fp16) 1GB + Adam(fp32) 4GB + grads(fp16) 1GB + activations ~1GB = ~7GB (fits 14.5GB T4)
- 5/5 unit tests PASS: shape, non-SID-safety, causal-mask, grad-flow, uniform-init
- User authorization: EXPLICIT ("please do. you can run smoke tests on modal to align env")
- Actual Modal smoke test command (user must execute):
    MODAL_STEP=train MODAL_EXP=expN1 MODAL_SMOKE=1 modal run modal_main.py
- Config: configs/_smoke_expN1.json (60 steps, 20 users eval, batch=4)
- Full run config: configs/expN1_slm_narrow_s512.json (12,000 steps,  ​300 users, batch=8>
---

## S6 — Narrow-head SLM Training (expN1)

### Status: S6.4 smoke ✅ → S6.5 12k full run RUNNING (proc_<redacted>)

### Bugs fixed during S6.4 smoke

1. **`model.parameters()` shadowed as property** (`train_slm_narrow.py` line 205)
   - A `@property` named `parameters` returned a list, shadowing `nn.Module.parameters()`.
   - Renamed to `all_model_params`.
   - Crash: `TypeError: 'list' object is not callable`.

2. **Label range mismatch** (`collate_narrow` in `train_slm_narrow.py`)
   - `encode_codes` returns extended vocab IDs (base+level*256+code = ~152k range).
   - Narrow head only has 1024 output classes (0..1023).
   - Fix: `lab[n] = seq[n] - itemic.base` maps to [0..1023].
   - Crash: `CUDA: device-side assert (nll_loss)`, label ≥ n_classes.

3. **Right-padding → wrong last-4 positions** (`collate_narrow` in `train_slm_narrow.py`)
   - Narrow head takes `hidden_states[:, -4:, :]` = the padded positions (garbage).
   - Fix: left-pad so last 4 positions are always the target SID tokens.

4. **fp16 gradient overflow → NaN loss**
   - Root cause: cross-entropy with confidently-wrong predictions (loss ~18.5) produces large gradients that overflow Qwen's fp16 parameters.
   - Fix: freeze Qwen backbone, train only narrow head (fp32).
   - After freeze: loss starts at ~17.8 and drops to ~10.6 in 60 steps (was NaN before).

5. **`BeamSearchScorer` removed from transformers**
   - Dead code in `generate()` used removed class.
   - Fix: try/except import, then removed dead code entirely; now uses HuggingFace `model.generate()` with `narrow_forward` wrapper.

6. **MODAL_SMOKE not forwarded to container**
   - Local env var `MODAL_SMOKE=1` not visible inside Modal container's subprocess.
   - Fix: pass `smoke: bool` param to `train()` function, set `env["MODAL_SMOKE"]="1"` in subprocess.

### Smoke test (60 steps) — PASSED ✅
- Training on T4: 17.75 → 10.55 loss over 60 steps
- No CUDA asserts, no NaN, no crashes
- Final eval skipped in SMOKE mode (generate not fully tested yet)
- Eval generates to be verified in full run

### Full expN1 (12k steps) — RUNNING (proc_<redacted>)
Launch: `MODAL_STEP=train MODAL_EXP=expN1 modal run modal_main.py`
Expected runtime: ~30-40 min training + eval

## S6 — Narrow-head SLM Training (expN1) — LoRA Approach

### Evolution
1. ❌ **Full fine-tune (frozen Qwen)** — Loss 17.8→10.5 in 60 steps, 0.0 NDCG. Qwen frozen due to fp16 overflow.
2. ❌ **Full fine-tune (amp/autocast)** — NaN loss from fp16 gradient overflow in Qwen backbone.
3. ❌ **Full fine-tune (GradScaler)** — Error: "Attempting to unscale FP16 gradients" — GradScaler doesn't support mixed param dtypes.
4. ✅ **LoRA (rank=64) + narrow head** — 61.8M trainable / 556.5M total (11.1%). Loss 8.73→5.16 in 60 steps, no NaN. **Running full 12k now.**

### LoRA Config
- **Rank**: 64
- **Alpha**: 16
- **Target modules**: q_proj, k_proj, v_proj, o_proj, gate_proj, up_proj, down_proj
- **Trainable**: LoRA adapters (~35M) + SID embeddings (~1M) + narrow head (~26M) = ~62M
- **Frozen**: Qwen backbone weights in fp16 (~494M)

### Why LoRA works
LoRA adds low-rank adapters (A×B, rank=64) to attention projections. These adapters train in fp32 and are small enough to avoid overflow. Qwen's fp16 weights stay frozen — no gradients flow through them. The narrow head (also fp32) decodes the adapted hidden states into SID codes.

### Current Status
- **Full 12k training**: RUNNING (proc_<redacted>, Modal app ap-<redacted>...)
- See results_grid.md for metric collection after it completes

---

### R1 — Ratings Integration (SLM Narrow, 200 eval)
- Status: smoke-passed / full-run-launched / done
- Config: configs/expR1_slm_narrow_ratings_s512.json
- Ratings file: data/ratings.json (from Beauty_5.json `overall`, 22,363 users)
- eval_subset: 200
- Baseline: expN1 (12k, same backbone, no ratings)
- Key observation: ratings 58% are 5-star on Beauty — signal is weak
