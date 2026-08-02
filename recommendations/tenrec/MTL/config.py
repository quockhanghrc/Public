"""
Central configuration for the Tenrec CTR training pipeline.

All paths are relative to the CTR folder root (the folder that contains
`scripts/`). The training code resolves them against the repo root so the
scripts can be launched from anywhere.

Embedding cardinalities: ITEM_CARD / USER_CARD are sized from the full TRAIN
split (item_id max = 3,864,696 over 72.2M rows; user_id max ~43.7K). We add
headroom and a dedicated padding index (0) for the shared item embedding so
masked history never pollutes gradients. Run scripts/analyze_item_freq.py to
re-profile item frequencies / hash-bucket collision rates.
"""

from pathlib import Path

# ----------------------------------------------------------------------------
# Paths
# ----------------------------------------------------------------------------
# scripts/config.py -> repo root = scripts/..
REPO_ROOT = Path(__file__).resolve().parent
PARQUET_DIR = REPO_ROOT / "data" / "parquet"          # raw shards from convert_csv_to_parquet.py
SPLIT_DIR = REPO_ROOT / "data" / "split"              # output of split_data.py
STATS_PATH = SPLIT_DIR / "stats.json"                 # normalization stats
RUNS_DIR = REPO_ROOT / "runs"                         # one subdir per training run

# ----------------------------------------------------------------------------
# Split ratios (user-level hash). Defaults; overridable via CLI.
# ----------------------------------------------------------------------------
TRAIN_FRAC = 0.6
VAL_FRAC = 0.2
TEST_FRAC = 0.2  # remainder

# ----------------------------------------------------------------------------
# Feature / model dimensions
# ----------------------------------------------------------------------------
# Categorical embedding cardinalities (num_embeddings). Sized from the full
# TRAIN split: max item_id = 3,864,696 (see analyze_item_freq.py). We set
# ITEM_CARD just above that max so nn.Embedding never indexes out of range
# (index 0 is the reserved padding idx). +headroom for unseen ids at inference.
ITEM_CARD = 3_865_000      # item_id (candidate + history share this embedding)
USER_CARD = 44_000         # user_id (kept for reference / future use)
VIDEO_CATEGORY_CARD = 4    # values seen: {0,1}; +headroom for nulls/unseen
GENDER_CARD = 4            # values seen: {0,1,2}
AGE_CARD = 10              # values seen: {0..7}

EMBED_DIMS = {
    "item_id": 16,          # shared with hist_1..hist_10 (standard embedding)
    "video_category": 8,
    "gender": 4,
    "age": 8,
}

# Item-embedding method selection (see --item-embed-method in train.py).
#   "standard": nn.Embedding(ITEM_CARD, EMBED_DIMS["item_id"], padding_idx=0)
#               -> 16-dim item rep, attention projects 16 -> ATTN_PROJ_DIM.
#   "hash":     HashEmbedding(ITEM_CARD, ITEM_HASH_EMBED_DIM, ITEM_HASH_BUCKETS,
#               ITEM_NUM_HASHES, ITEM_HASH_MODE) -> ITEM_HASH_EMBED_DIM-dim rep
#               (64), attention projects 64 -> ATTN_PROJ_DIM. Bounds item-embed
#               params to ITEM_HASH_BUCKETS * ITEM_HASH_EMBED_DIM regardless of
#               ITEM_CARD (the full 3.9M-row table would be ~62M params).
#   "qr":       QREmbedding(ITEM_CARD, ITEM_QR_EMBED_DIM, padding_idx=0) ->
#               ITEM_QR_EMBED_DIM-dim rep (64) with ZERO collisions. Maps each id
#               to a unique (quotient, remainder) pair of small tables
#               (divisor = int(sqrt(ITEM_CARD))), so memory stays tiny while every
#               id gets a distinct vector. Attention projects 64 -> ATTN_PROJ_DIM.
ITEM_EMBED_METHOD = "standard"  # "standard", "hash", or "qr"
ITEM_HASH_EMBED_DIM = 64        # dim of the hashed item table (output dim for "hash")
# Rows in the hashed table. Chosen from analyze_item_freq.py on the TRAIN split
# (72.2M rows): unique items = 1,912,866, max item_id = 3,864,696, and 74.8% of
# items appear < 10 times (median 2). Collision rate ~ 1 - exp(-n/buckets):
#   250K -> 99.95% | 500K -> 97.82% | 1M -> 85.23% | 2M -> 61.57%
# 500K is kept because it is the only count that preserves the hash method's
# purpose (~32M params vs 62.5M for "standard"); 1M already matches standard's
# param count and 2M exceeds it. With num_hashes=2 (sum) the effective collision
# is ~0.978^2 ~= 95.7%, but it mostly hits the long tail (rare items), so the
# smoke test still reached val_auc 0.7921 (>= standard 0.7849). Raise to 1M only
# if accuracy drops and you can spare the extra ~32M params.
ITEM_HASH_BUCKETS = 250_000     # rows in the hashed table
ITEM_NUM_HASHES = 2             # independent hash functions
ITEM_HASH_MODE = "sum"          # "sum" or "concat"

# Quotient-Remainder (QR) item embedding output dim. Split into two equal halves
# (split_dim = ITEM_QR_EMBED_DIM // 2) for the quotient and remainder tables, so
# ITEM_QR_EMBED_DIM MUST be even. For ITEM_CARD = 3,865,000: divisor = 1965,
# q_size = 1967, r_size = 1965 -> item-embed params = (1967+1965) * (64/2) = 125,824
# (vs standard 62.5M, hash ~32M). Unique per-id vector, zero collisions.
ITEM_QR_EMBED_DIM = 64          # even; output dim of the "qr" item rep

# ----------------------------------------------------------------------------
# Auxiliary (DIEN-style) next-item loss
# ----------------------------------------------------------------------------
# When AUX_LOSS_WEIGHT > 0, a small GRU "interest extractor" runs over the
# history sequence and a next-item prediction loss supervises the SHARED item
# embedding (DIEN idea: at step t predict hist[t+1] from the GRU hidden state,
# pushed away from in-batch negatives). This is OFF by default (weight 0) so
# existing standard/hash runs are bit-identical and pay zero overhead. The main
# task's attention path is untouched when disabled.
AUX_LOSS_WEIGHT = 0.0        # 0 = disabled; >0 blends aux loss into total
AUX_HIDDEN = 64               # GRU hidden size (can equal item_emb_dim)
AUX_NEG_SAMPLES = 1           # in-batch negatives per step (subsampled for CPU)
AUX_SEQ_ENCODER = "gru"       # sequence encoder type for the aux extractor

# NOTE: follow / like / share / watching_times were REMOVED as model inputs.
# They are post-click engagement signals (only observable AFTER the user
# clicks), so using them as features is target leakage. See model.py / dataset.py.

# History attention
HIST_LEN = 10
ATTN_HEADS = 4
ATTN_DROPOUT = 0.1
# Raw item embedding (EMBED_DIMS["item_id"]=16) is projected up to this dim
# BEFORE splitting into heads, then concatenated back to ATTN_PROJ_DIM.
# Must be divisible by ATTN_HEADS.
ATTN_PROJ_DIM = 64

# Active item-embedding output dim depends on the selected method:
#   "standard" -> EMBED_DIMS["item_id"] (16)
#   "hash"     -> ITEM_HASH_EMBED_DIM (64)
#   "qr"       -> ITEM_QR_EMBED_DIM (64)
# Used by model.py (item_emb_dim) and to size FINAL_DIM / LayerNorm.
def item_embed_dim(method=ITEM_EMBED_METHOD):
    if method == "hash":
        return ITEM_HASH_EMBED_DIM
    if method == "qr":
        return ITEM_QR_EMBED_DIM
    return EMBED_DIMS["item_id"]

# Final concat width = item_emb_dim + video_category(8) + gender(4) + age(8)
#   + interest(ATTN_PROJ_DIM=64)   [follow/like/share/watching_times removed: leakage]
#   standard -> 16+8+4+8+64 = 100
#   hash     -> 64+8+4+8+64 = 148
#   qr       -> 64+8+4+8+64 = 148
FINAL_DIM = (
    item_embed_dim()
    + EMBED_DIMS["video_category"]
    + EMBED_DIMS["gender"]
    + EMBED_DIMS["age"]
    + ATTN_PROJ_DIM  # interest_vec from projected attention
)

# MLP head
MLP_DIMS = [256, 128, 64]
DROPOUT = 0.1

# ----------------------------------------------------------------------------
# Training hyper-parameters (defaults; overridable via CLI)
# ----------------------------------------------------------------------------
BATCH_SIZE = 2048
EPOCHS = 10
LR = 1e-3
WEIGHT_DECAY = 1e-4
NUM_WORKERS = 4
SEED = 42

# Focal loss (imbalance handling). pos_weight fallback also computed from data.
FOCAL_GAMMA = 2.0
FOCAL_ALPHA = 0.75

# Early stopping
ES_PATIENCE = 3
ES_MONITOR = "val_auc"   # higher is better

# Default run name prefix (timestamp appended at runtime)
DEFAULT_RUN_NAME = "exp"
