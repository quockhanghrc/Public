"""Cross-file artifact verification (Task 4): fail fast before training."""
import json
import pickle
import sys

import numpy as np
import murmurhash


def check(cond, msg):
    if not cond:
        print(f"[FAIL] {msg}")
        sys.exit(1)
    print(f"[ok]   {msg}")


def main():
    # --- inter.json ---
    inter = json.load(open("data/inter.json", encoding="utf-8"))
    lens = [len(v) for v in inter.values()]
    check(all(l >= 5 for l in lens), f"inter.json: all {len(inter)} users have >=5 items")
    all_items = sorted({i for v in inter.values() for i in v})
    check(
        all_items == list(range(len(all_items))),
        f"inter.json: item ids are 0-indexed & contiguous (max={all_items[-1] if all_items else None})",
    )
    n_items = len(all_items)

    # --- content_embeddings.pkl ---
    with open("data/content_embeddings.pkl", "rb") as f:
        data = pickle.load(f)
    emb_ids = np.asarray(data["item_id"])
    emb = np.asarray(data["embedding"])
    check(emb.ndim == 2 and emb.shape[1] == 768, f"embeddings shape {tuple(emb.shape)} (need N x 768)")
    check(emb.dtype == np.float32, f"embeddings dtype {emb.dtype} (need float32)")
    check(
        set(emb_ids.tolist()) == set(all_items),
        f"embeddings cover exactly the {n_items} inter items",
    )

    # --- index_rqkmeans.json ---
    index = json.load(open("data/index_rqkmeans.json", encoding="utf-8"))
    keys = sorted(int(k) for k in index)
    check(keys == list(range(len(keys))), f"index: keys 0..{len(keys)-1} contiguous")
    sids = [index[str(k)] for k in keys]
    check(all(len(s) == 4 for s in sids), "index: every item has a 4-token SID")
    check(all(0 <= x < 256 for s in sids for x in s), "index: all SID tokens in [0,256)")

    # --- user hashing lands in [0, user_ids_count) ---
    user_ids_count = 2000
    hashed = [murmurhash.hash(str(u)) % user_ids_count for u in inter.keys()]
    check(all(0 <= h < user_ids_count for h in hashed), "user hashing within [0, user_ids_count)")

    print("\nAll artifact checks passed.")


if __name__ == "__main__":
    main()