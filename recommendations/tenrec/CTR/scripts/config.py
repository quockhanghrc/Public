"""
Central configuration for the Tenrec CTR training pipeline.

All paths are relative to the CTR folder root (the folder that contains
`scripts/`). The training code resolves them against the repo root so the
scripts can be launched from anywhere.

Embedding cardinalities are sized from a 3-shard sample of the data
(item_id max ~3.86M, user_id max ~43.7K). We add a +1/+2 headroom and a
dedicated padding index (0) for the shared item embedding so masked
history never pollutes gradients.
"""

from pathlib import Path

# ----------------------------------------------------------------------------
# Paths
# ----------------------------------------------------------------------------
# scripts/config.py -> repo root = scripts/..
REPO_ROOT = Path(__file__).resolve().parent.parent
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
# Categorical embedding cardinalities (num_embeddings). +2 headroom for unseen
# ids at inference; index 0 is reserved as the padding idx for item_id.
ITEM_CARD = 3_900_000      # item_id (candidate + history share this embedding)
USER_CARD = 44_000         # user_id (kept for reference / future use)
VIDEO_CATEGORY_CARD = 4    # values seen: {0,1}; +headroom for nulls/unseen
GENDER_CARD = 4            # values seen: {0,1,2}
AGE_CARD = 10              # values seen: {0..7}

EMBED_DIMS = {
    "item_id": 64,          # shared with hist_1..hist_10 (hashed, see ITEM_HASH_*)
    "video_category": 8,
    "gender": 4,
    "age": 8,
}

# Hashed item embedding (memory-bounded alternative to nn.Embedding(ITEM_CARD, dim)).
# The full 3.9M-row table is replaced by a small hash table of ITEM_HASH_BUCKETS
# rows; each item id is mapped through ITEM_NUM_HASHES independent hash functions
# and the looked-up rows are combined (sum/concat). This bounds item-embedding
# params to ITEM_HASH_BUCKETS * EMBED_DIMS["item_id"] regardless of ITEM_CARD.
#   - sum   : output dim = EMBED_DIMS["item_id"] (collisions add, like a bag of
#             embeddings; standard feature hashing).
#   - concat: output dim = EMBED_DIMS["item_id"] * ITEM_NUM_HASHES (more capacity,
#             more params; FINAL_DIM must be updated accordingly if used).
ITEM_HASH_BUCKETS = 500_000
ITEM_NUM_HASHES = 2
ITEM_HASH_MODE = "sum"  # "sum" or "concat"

# Engagement (follow/like/share) dense branch
ENGAGEMENT_IN = 3
ENGAGEMENT_OUT = 16

# Watching-times dense branch
WATCH_OUT = 8

# History attention
HIST_LEN = 10
ATTN_HEADS = 4
ATTN_DROPOUT = 0.1
# Raw item embedding (EMBED_DIMS["item_id"]=16) is projected up to this dim
# BEFORE splitting into heads, then concatenated back to ATTN_PROJ_DIM.
# Must be divisible by ATTN_HEADS.
ATTN_PROJ_DIM = 64

# Final concat width = item_id(16) + video_category(8) + gender(4) + age(8)
#   + engagement(16) + watch(8) + interest(ATTN_PROJ_DIM=64) = 124
FINAL_DIM = (
    EMBED_DIMS["item_id"]
    + EMBED_DIMS["video_category"]
    + EMBED_DIMS["gender"]
    + EMBED_DIMS["age"]
    + ENGAGEMENT_OUT
    + WATCH_OUT
    + ATTN_PROJ_DIM  # interest_vec from projected attention
)

# MLP head
MLP_DIMS = [256, 128, 64]
DROPOUT = 0.2

# ----------------------------------------------------------------------------
# Training hyper-parameters (defaults; overridable via CLI)
# ----------------------------------------------------------------------------
BATCH_SIZE = 4096
EPOCHS = 10
LR = 1e-3
WEIGHT_DECAY = 1e-5
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
