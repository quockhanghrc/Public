"""RQ-KMeans tokenization -> data/index_rqkmeans.json.

Port of ref/nonameuntitled_tiger/notebooks/RQKmeansPipeline.ipynb:
3 residual KMeans codebooks (256 clusters each, subtract assigned centroid each round)
+ 1 collision-solver code (random 0-255 per colliding group) => 4-token SID per item.
"""
from collections import defaultdict
import json
import os
import pickle
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rqkmeans import RQKMeans

EMB_PATH = "data/content_embeddings.pkl"
OUT_PATH = "data/index_rqkmeans.json"
NUM_CLUSTERS = 256
NUM_CODEBOOKS = 3  # +1 collision solver appended below
MAX_ITER = 1000


def main():
    emb = EMB_PATH
    out = OUT_PATH
    if "--emb" in sys.argv:
        emb = sys.argv[sys.argv.index("--emb") + 1]
    if "--out" in sys.argv:
        out = sys.argv[sys.argv.index("--out") + 1]
    with open(emb, "rb") as f:
        data = pickle.load(f)
    item_ids = np.array(data["item_id"], dtype=np.int64)
    X = np.array(data["embedding"], dtype=np.float32)
    print(f"X: {X.shape} ({item_ids.shape[0]} items)")

    rq = RQKMeans(num_clusters=NUM_CLUSTERS, num_codebooks=NUM_CODEBOOKS, max_iter=MAX_ITER)
    print("fitting RQ-KMeans (may take a while on CPU)...")
    rq.fit(X)
    clusters = rq.predict(X)
    print("predicted clusters:", clusters.shape)

    inter = {}
    sem_2_ids = defaultdict(list)
    for idx, c in zip(item_ids, clusters):
        inter[int(idx)] = c.tolist()
        sem_2_ids[tuple(c.tolist())].append(int(idx))

    # collision solving: random 0-255 solvers per colliding group (requires <=256 per group)
    for semantics, ids in sem_2_ids.items():
        assert len(ids) <= NUM_CLUSTERS, f"too many collisions under one 3-code: {len(ids)}"
        solvers = np.random.permutation(NUM_CLUSTERS)[:len(ids)].tolist()
        for item_id, solver in zip(ids, solvers):
            inter[item_id].append(solver)

    with open(out, "w") as f:
        json.dump(inter, f)

    lens = set(len(v) for v in inter.values())
    print(f"wrote {out}: {len(inter)} items, all SID lengths: {lens}")


if __name__ == "__main__":
    main()