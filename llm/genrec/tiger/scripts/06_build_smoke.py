"""Build a small contiguous subset dataset + RQ-KMeans index for CPU smoke tests.

Pulls the FIRST N_USERS users from the full inter.json, remaps their items to
contiguous 0..K-1 ids, subsets the already-computed Sentence-T5 embeddings to
those items, and refits RQ-KMeans on the subset. Outputs go to data/smoke/.
"""
import json
import os
import pickle
import sys
from collections import defaultdict

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rqkmeans import RQKMeans  # noqa: E402  (reuse the exact RQ-KMeans class)

N_USERS = 60
NUM_CLUSTERS = 256
OUT_DIR = "data/smoke"

FULL_INTER = "data/inter.json"
FULL_EMB = "data/content_embeddings.pkl"


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    inter = json.load(open(FULL_INTER, encoding="utf-8"))

    # take first N_USERS users (each already >=5 items)
    user_ids = sorted(int(k) for k in inter.keys())[:N_USERS]
    subset_users = {str(u): inter[str(u)] for u in user_ids}

    # contiguous re-remap of items across selected users
    item_set = sorted({i for items in subset_users.values() for i in items})
    remap = {old: new for new, old in enumerate(item_set)}

    smoke_inter = {}
    for u, items in subset_users.items():
        smoke_inter[u] = [remap[i] for i in items]
    json.dump(
        smoke_inter,
        open(os.path.join(OUT_DIR, "inter.json"), "w"),
    )
    print(f"smoke users={len(smoke_inter)} items={len(item_set)}")

    # subset embeddings to these items, reordered to match remapped ids
    with open(FULL_EMB, "rb") as f:
        data = pickle.load(f)
    emb_ids = data["item_id"]
    emb = data["embedding"]
    pos = {int(i): p for p, i in enumerate(emb_ids)}
    X = np.stack([emb[pos[old]] for old in item_set], axis=0).astype(np.float32)
    print("subset X:", X.shape)

    rq = RQKMeans(num_clusters=NUM_CLUSTERS, num_codebooks=3, max_iter=300)
    rq.fit(X)
    clusters = rq.predict(X)

    inter2 = {}
    sem2 = defaultdict(list)
    for new_id, c in zip(range(len(item_set)), clusters):
        inter2[new_id] = c.tolist()
        sem2[tuple(c.tolist())].append(new_id)
    for sem, ids in sem2.items():
        assert len(ids) <= NUM_CLUSTERS
        solvers = np.random.permutation(NUM_CLUSTERS)[:len(ids)].tolist()
        for it, sv in zip(ids, solvers):
            inter2[it].append(sv)
    json.dump(inter2, open(os.path.join(OUT_DIR, "index_rqkmeans.json"), "w"))
    print("wrote smoke inter.json + index_rqkmeans.json")

    # sanity print a couple sequences
    k = sorted(smoke_inter)[0]
    print("sample user", k, ":", smoke_inter[k][-5:], "-> SID", inter2[smoke_inter[k][-1]])


if __name__ == "__main__":
    main()