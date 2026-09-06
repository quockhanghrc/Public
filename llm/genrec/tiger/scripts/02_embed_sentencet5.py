"""Sentence-T5 item content embeddings -> data/content_embeddings.pkl.

Uses sentence-transformers/sentence-t5-base (768-dim). Model is downloaded ONCE
into tiger/cache/hf and reused on later runs (no re-download).
"""
import json
import os
import pickle
import sys

import numpy as np

# --- persistent local model cache (Task 0) ---
_CACHE = os.path.join(os.getcwd(), "cache", "hf")
os.environ.setdefault("HF_HOME", _CACHE)
os.environ.setdefault("TRANSFORMERS_CACHE", _CACHE)
os.environ.setdefault("HUGGINGFACE_HUB_CACHE", _CACHE)
os.environ.setdefault(
    "SENTENCE_TRANSFORMERS_HOME", os.path.join(_CACHE, "sentence_transformers")
)

from sentence_transformers import SentenceTransformer  # noqa: E402

MODEL = "sentence-transformers/sentence-t5-base"
EMB_DIM = 768
BATCH = 64
# max_seq override: pass `--max_seq 512` (default). Longer context may capture more content.
MAX_SEQ = int(sys.argv[sys.argv.index("--max_seq") + 1]) if "--max_seq" in sys.argv else 512
OUT_PATH = f"data/content_embeddings_s{MAX_SEQ}.pkl"  # e.g. content_embeddings_s512.pkl

texts = json.load(open("data/item_text.json", encoding="utf-8"))
ids = sorted(int(k) for k in texts)
strs = [texts[str(i)] for i in ids]
print(f"embedding {len(ids)} items with {MODEL} @ max_seq={MAX_SEQ} ...")

model = SentenceTransformer(
    MODEL, cache_folder=os.path.join(_CACHE, "sentence_transformers")
)
model.max_seq_length = MAX_SEQ  # version-agnostic truncation (encode(max_seq_length=) was removed in ST 6.x)
embeds = model.encode(
    strs,
    batch_size=BATCH,
    convert_to_numpy=True,
    normalize_embeddings=False,
    show_progress_bar=True,
)
embeds = embeds.astype(np.float32)
print("embedding shape:", embeds.shape)
assert embeds.shape == (len(ids), EMB_DIM), embeds.shape

with open(OUT_PATH, "wb") as f:
    pickle.dump(
        {"item_id": np.array(ids, dtype=np.int64), "embedding": embeds}, f
    )
print(f"wrote {OUT_PATH}", embeds.shape, embeds.dtype)