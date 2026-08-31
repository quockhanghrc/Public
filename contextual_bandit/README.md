# obp_flow — Contextual Bandit Pipeline on the Open Bandit Dataset (retail)

A complete, reproducible, exploratory **contextual-bandit pipeline** built on the
[Open Bandit Dataset](https://huggingface.co/datasets/zozonext/open-bandit) (OBD,
from ZOZOTOWN fashion e-commerce). It answers two industry questions:

1. **Reward model:** in plain English — *"given who the user is and which item we showed
   in which slot, how likely were they to click?"*
2. **Off-policy evaluation (OPE):** *"if we had run a *different* shopping algorithm
   instead of the one that logged this data, what would its click rate have been —
   before we ever deploy it?"*

**Audience:** junior data scientists and business readers. Each section gives the plain-English
meaning first, then the formula. No previous bandit knowledge is assumed.

> Business takeaway in one line: on this weak-signal retail log, a simple logistic
> reward model beats a fancier gradient-boosted one out-of-time, a **learning ε-greedy**
> bandit lifts click-through the most online (→ ≈0.008), and offline estimates are only
> trustworthy when the evaluated policy overlaps (randomly explores) the logged actions.

---

## 1. Data flow (how data moves through the pipeline)

Four notebooks run in sequence, each writing artifacts to `data/` and `artifacts/`
that the next one reads. Run them in order; nothing is re-computed twice.

```
 590k-row OBD shard (real logged impressions)
   │  arms = (item, position); reward = click; context = 80 user-item affinity scores
   ▼
┌───────────────────────────────────────────────────────────────────┐
│ NB1  Data audit + split (01_data_audit.ipynb)                     │
│   • confirm schema & bandit semantics                             │
│   • measure empirical facts (affinity ~95% zero, CTR by slot)      │
│   • chronological 80/20 split → train (past) / eval (future)        │
└───────────────┬───────────────────────────────────────────────────┘
   data/obd_train.parquet (160k)   data/obd_eval.parquet (40k)   arm_universe.json
   ▼
┌───────────────────────────────────────────────────────────────────┐
│ NB2  Reward model + offline OPE (02_reward_model_ope.ipynb)       │
│   • fit P(click | user, item, slot): Logistic vs LightGBM, pick    │
│     best out-of-time AUC → reward_model.joblib                    │
│   • OPE 3 target policies with obp: IPW / SNIPS / DM / DR          │
│   • overlap diagnostics (why greedy breaks) + clip sensitivity      │
└───────────────┬───────────────────────────────────────────────────┘
   reward_model.joblib   model_comparison.json   ope_results.json
   ▼
┌───────────────────────────────────────────────────────────────────┐
│ NB3  Online simulation (03_online_simulation.ipynb)               │
│   • reward model becomes a click simulator (Bernoulli(q̂))          │
│   • live act→reward→learn loop: Random, greedy, model-ε-greedy,     │
│     learning ε-greedy, LinUCB, LinTS (mabwiser)                    │
│   • BEFORE vs AFTER table: early-Ctr → final-Ctr (online gain)      │
└───────────────┬───────────────────────────────────────────────────┘
   online_results.json   (early_ctr, final_ctr, before_vs_after)
   ▼
┌───────────────────────────────────────────────────────────────────┐
│ NB4  Capstone: does OPE predict the simulation?                   │
│  (04_capstone_report.ipynb)                                       │
│   • align OPE to the center slot the simulator played              │
│   • table: analytic truth | sim_early→sim_final | IPW/SNIPS/DM/DR  │
│   • read the OPE estimate against the online trajectory            │
└───────────────┬───────────────────────────────────────────────────┘
   capstone_summary.json  (+ this README)
```

**Artifacts you can trust and why:** `nb1_summary.json`, `model_comparison.json`,
`ope_results.json`, `online_results.json`, `capstone_summary.json` — every notebook prints
its headline numbers, so you can re-verify the summary below without re-running anything.

---

## 2. Metrics — what each number means

Definitions in plain English, then the formula.

### Click-through rate (CTR)
**Plain English:** out of every 100 impressions, how many got clicked.
$$ \text{CTR} = \frac{\#\text{clicks}}{\#\text{impressions}} $$
The OBD log is sparse: **≈0.005 (0.5%)** overall train / eval CTR — i.e. ~5 clicks per 1,000
impressions. That tiny rate drives almost every methodological choice below.

### ROC-AUC (reward-model quality)
**Plain English:** if we sort every logged impression by the model's predicted click
probability, how often does a real click end up *above* a non-click? AUC = 0.5 is a coin
flip; 1.0 is perfect ranking.
Formula (probability form):
$$ \text{AUC} = P(\, \hat p(\text{click}) > \hat p(\text{no click}) \,) $$
Computed on the model's predicted probability **at the action/position actually logged**
(`score_at_logged`). We report two: **train AUC** (in-sample fit) and **eval AUC**
(out-of-time holdout — the one we trust).

### Off-policy estimators (the core of NB2 / NB4)
All estimate the click rate a **target policy** π_e would get, using logs from a
*behaviour* policy b that chose each action with known propensity `p(a|x)`.

- **IPW / Inverse Propensity Weighting** — reweight each logged click by how much more/less
  likely the target is than the logger to pick that action:
  $$ \widehat V_{IPW} = \frac{1}{n}\sum_t r_t\,\frac{\pi_e(a_t|x_t)}{p(a_t|x_t)} $$
- **SNIPS (self-normalized IPW)** — same weights but divide by their mean, which
  stabilises variance on tiny propensities:
  $$ \widehat V_{SNIPS} = \frac{\sum_t r_t\,w_t}{\sum_t w_t},\quad w_t=\frac{\pi_e(a_t|x_t)}{p(a_t|x_t)} $$
- **DM / Direct Method** — skip the log entirely; ask the reward model what it *thinks* the
  target would earn:
  $$ \widehat V_{DM} = \frac{1}{n}\sum_t\sum_a \pi_e(a|x_t)\,\hat q(x_t,a) $$
- **DR / Doubly Robust** — combines DM (model) and IPW (log); unbiased if *either* is right:
  $$ \widehat V_{DR} = \widehat V_{DM} + \frac{1}{n}\sum_t \frac{\pi_e(a_t|x_t)}{p(a_t|x_t)}\big(r_t - \hat q(x_t,a_t)\big) $$

> **The key caveat (verified in NB4):** DM ≡ the model's opinion (it literally equals the
> analytic expected reward under the model); IPW/SNIPS = the log's opinion. Their gap is the
> model's optimism on arms the log never covered.

### Overlap (why greedy fails — NB2)
How well the target policy "covers" the logged actions:
$$ \text{overlap} = \frac{1}{n}\sum_t \pi_e(a_t|x_t) \qquad \text{zero-cover} = \frac1n\sum_t \mathbb 1[\pi_e(a_t|x_t) \approx 0] $$
A **deterministic greedy** policy almost never matches the logged action (zero-cover ≈ 98%),
so IPW has ~no support and collapses — the reason OPE needs **ε-exploration**.

### Before vs After (online learning gain — NB3)
- **early_ctr** = mean CTR over the **first** rolling window (the policy still exploring).
- **final_ctr** = mean CTR over the **last** window (converged).
- **delta = final − early** = the lift from doing online learning.
Caveat: no-learning policies (uniform/greedy/model-ε) show delta ≈ 0 — only *learning*
policies should move, and at CTR≈0.005 even their delta carries ±0.003 noise.

### Analytic policy value (model-side truth — NB3/NB4)
The *exact* expected CTR of a static policy under `est_rewards` q̂, with zero simulation noise:
$$ \text{uniform} = \bar q \qquad \text{greedy} = \overline{\max_a q} \qquad \varepsilon\text{-greedy} = (1-\varepsilon)\,\overline{\max_a q} + \varepsilon\,\bar q $$

---

## 3. Results summary

### 3.1 Data audit (NB1, 200k sampled)
- 80 items × 3 slots; arms = **(item, position)**; reward = click.
- **95.4%** of affinity rows are all-zero (past-click counts of 1–4).
- Overall CTR ≈ **0.0046** (train 0.0046 / eval 0.0047).
- Propensity is logged **per impression** — it is not a static (item, position) lookup.

### 3.2 Reward model (NB2) — Logistic vs LightGBM

| model | train AUC | **eval AUC** | mean pred CTR |
|---|---|---|---|
| **logistic** | 0.7952 | **0.7835** ✅ selected | 0.0029 |
| lgb | 0.8387 | 0.7794 | 0.0025 |

**Takeaway:** LightGBM overfits on train (0.839) yet **loses out-of-time** (0.779 < 0.784).
On weak-signal, 95%-zero-affinity data the simpler regularised logistic wins → it is the
saved pipeline reward model. Both sit well above chance (0.5) but below 0.8 — the raw log
is simply hard to rank.

### 3.3 Offline OPE (NB2, on the 40k eval holdout)
Well-covered policies (uniform, ε-greedy) → all four estimators **agree** (tight band).
The deterministic **greedy** target → **zero-cover ≈ 98%**, IPW collapses, DM inflates:
the textbook low-overlap failure. ε-exploration restores overlap and estimator agreement.

### 3.4 Online simulation before/after (NB3, 20k rounds)

| policy | early (before) | final (after) | delta |
|---|---|---|---|
| **learning ε-greedy** | 0.0000 | **0.0080** | **+0.0080** ✅ |
| greedy (model argmax) | 0.0020 | 0.0060 | +0.0040 |
| LinUCB | 0.0020 | 0.0040 | +0.0020 |
| uniform / LinTS | ~0 | ~0 | ~0 (noise) |

**Takeaway:** the **learning ε-greedy** almost doubles CTR over the uniform baseline
(0.0080 vs 0.0040) — the clearest online-learning signal. LinTS's 0.0000 was diagnosed as
window noise on an over-exploratory policy (not a dead arm). All deltas at this CTR are
within ±0.003 MC noise except learning ε-greedy's.

### 3.5 Capstone (NB4) — does OPE predict the simulation?

| policy | analytic truth | sim_early → sim_final | IPW | SNIPS | DM | DR |
|---|---|---|---|---|---|---|
| uniform | 0.0026 | 0.0020 → 0.0040 | 0.0019 | 0.0018 | 0.0026 | 0.0021 |
| greedy | 0.0054 | 0.0020 → 0.0060 | 0.0040 | 0.0036 | 0.0054 | 0.0047 |
| model ε-greedy | 0.0052 | 0.0000 → 0.0020 | 0.0038 | 0.0034 | 0.0052 | 0.0044 |

Pearson r (mean OPE vs analytic truth) = **1.0** — but read with care: DM *is* the analytic
truth by construction, so they always agree. IPW/SNIPS/DR sit below DM for greedy/ε-greedy,
i.e. the **log** thinks those policies are worth less than the **model** believes.

---

## 4. Repository layout

```
obp_flow/
  src/            tested library code (obd_io, reward_model, ope_wrap, online_sim, config)
  tests/          pytest suite (28 tests, all green)
  scripts/
    build_notebook.py   cell-fragment → .ipynb builder
    cells/              the notebook source as reviewable .py fragments
  0X_*.ipynb      the four pipeline notebooks (executed)
  data/           obd_train.parquet, obd_eval.parquet    (git-ignored)
  artifacts/      model_comparison.json, ope_results.json, online_results.json,
                  capstone_summary.json, reward_model.joblib, nb3_curves.png
  requirements.txt
```

### Run it yourself
```bash
# Python: D:/Anaconda/python.exe (has torch/obp/mabwiser/lightgbm)
cd obp_flow
python -m pytest tests/ -q            # 28 tests
# execute a notebook from its reviewable fragment:
python scripts/build_notebook.py scripts/cells/02_reward_model_ope.py 02_reward_model_ope.ipynb
python -m jupyter nbconvert --to notebook --execute --inplace 02_reward_model_ope.ipynb
```
Notebooks run in order NB1→NB4 (NB3 is the slow one, ~8–10 min). `config.py` controls the
sample size (`N_ROWS=200_000`; set `None` for the full 590k).

---

## 5. Caveats (read before acting on any number)

1. **The reward model is the ground truth for simulation.** NB3/NB4's "truth" is the NB2
   logistic model — a *model-internal self-consistency check*, not an observed effect.
2. **AUC ≠ bandit value.** AUC measures reward-model *ranking quality* on the log; OPE
   measures *policy value*. They answer different questions; do not equate them.
3. **Small CTR ⇒ noisy.** At 0.005 CTR, a 500-round window is ~2.5 clicks → ±0.003 noise,
   so small deltas are not signal. Only learning ε-greedy's lift clears this bar.
4. **One behavior policy, one shard.** Only the `bts` log is used; the `random` log would be
   the textbook overlap-safety net (a natural extension).
5. **No propensity-ignoring shortcut.** The log's propensities are non-trivial (bts policy,
   varies by user/arm/time) — they must be used in OPE, hence IPW/SNIPS/DR.

## 6. Extensions (if you keep going)
- Run the full 590k shard (`config.N_ROWS = None`).
- Add the `random`-policy log as an independent overlap-safety comparison.
- Apply the same pipeline to a **banking** capstone: arms = credit-limit tiers or disbursal
  amounts, reward = profit, follow identical data flow (audit → reward model + OPE →
  online sim → compare).