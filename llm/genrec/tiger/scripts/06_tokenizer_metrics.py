"""Tokenizer quality metrics: RQ-KMeans vs RQ-VAE (on S512 embeddings).

For each tokenizer computes:
  - reconstruction error (MSE, relative-MSE, cosine) decoding the SID back to the input emb
  - codebook coverage    (% of 256 codes used in each of the 3 codebooks)
  - semantic collisions  (3-code prefix groups: distinct count, max group, % items in group>1)

Writes results_tokenizer.csv and prints a table. Run from tiger/ cwd (volume paths).
"""
import csv
import json
import os
import pickle
import sys
from collections import defaultdict

import numpy as np
import torch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from rqkmeans import RQKMeans  # noqa: E402
# RQVAE class + hyperparams live in the pipeline module (its main() is guarded)
from rqvae_pipeline import RQVAE, INPUT_DIM, HIDDEN_DIM, BETA, DEVICE  # noqa: E402

EMB_PATH = "data/content_embeddings_s512.pkl"
KMEANS_INDEX = "data/index_rqkmeans_s512.json"
RVAE_INDEX = "data/index_rqvae_s512.json"
RVAE_CKPT = "data/rqvae_s512.pt"
CODEBOOK = 256
N_CB = 3


def load_emb():
    with open(EMB_PATH, "rb") as f:
        d = pickle.load(f)
    return np.array(d["item_id"], dtype=np.int64), np.array(d["embedding"], dtype=np.float32)


def metrics_from_codebook(codes3):
    """codes3: (N, 3) first-3 codes. Returns coverage + collision stats."""
    cov = []
    for c in range(N_CB):
        b = np.bincount(codes3[:, c], minlength=CODEBOOK)
        cov.append(round(float((b > 0).mean()), 3))
    groups = defaultdict(list)
    for item, c in enumerate(codes3):
        groups[tuple(c.tolist())].append(item)
    sizes = np.array([len(v) for v in groups.values()])
    frac_collide = float(np.mean([len(groups[tuple(c.tolist())]) > 1 for c in codes3]))
    return {
        "coverage_b1": cov[0], "coverage_b2": cov[1], "coverage_b3": cov[2],
        "distinct_3codes": int(len(groups)),
        "max_collision_group": int(sizes.max()) if len(sizes) else 0,
        "frac_collide_gt1": round(frac_collide, 4),
    }


def recon_metrics(X, Xhat):
    X = np.asarray(X, np.float32)
    Xhat = np.asarray(Xhat, np.float32)
    mse = float(np.mean((X - Xhat) ** 2))
    rel = mse / (float(np.var(X)) + 1e-8)
    cos = float(np.mean(np.sum(X * Xhat, 1) /
                        (np.linalg.norm(X, axis=1) * np.linalg.norm(Xhat, axis=1) + 1e-8)))
    return {"recon_mse": round(mse, 6), "rel_mse": round(rel, 6), "cosine_sim": round(cos, 4)}


def rqkmeans_metrics(X):
    rq = RQKMeans(num_clusters=CODEBOOK, num_codebooks=N_CB, max_iter=1000)  # seed 42 => matches saved index
    rq.fit(X)
    codes = rq.predict(X)  # (N, 3)
    with open(KMEANS_INDEX) as f:
        idx = json.load(f)
    idx_codes = np.array([idx[str(i)][:N_CB] for i in range(len(idx))], np.int64)
    assert np.array_equal(codes, idx_codes), "refit codes differ from saved index!"
    Xhat = np.zeros_like(X)
    for l in range(N_CB):
        Xhat = Xhat + rq.models[l].cluster_centers_[codes[:, l]]
    m = recon_metrics(X, Xhat)
    m.update(metrics_from_codebook(codes))
    return m


def rqvae_metrics(X):
    model = RQVAE(INPUT_DIM, HIDDEN_DIM, BETA, [CODEBOOK] * N_CB)
    model.load_state_dict(torch.load(RVAE_CKPT, map_location=DEVICE))
    model.to(DEVICE).eval()
    with torch.no_grad():
        Xhat = model({"embedding": torch.FloatTensor(X).to(DEVICE)})["embedding_hat"].cpu().numpy()
    with open(RVAE_INDEX) as f:
        idx = json.load(f)
    codes = np.array([idx[str(i)][:N_CB] for i in range(len(idx))], np.int64)
    m = recon_metrics(X, Xhat)
    m.update(metrics_from_codebook(codes))
    return m


def main():
    _, X = load_emb()
    print("embeddings:", X.shape)
    rows = {"rqkmeans": rqkmeans_metrics(X), "rqvae": rqvae_metrics(X)}
    cols = list(rows["rqkmeans"].keys())
    with open("results_tokenizer.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["tokenizer"] + cols)
        for name, m in rows.items():
            w.writerow([name] + [m[c] for c in cols])
    print("\nwrote results_tokenizer.csv\n")
    print(f"{'tokenizer':9s} " + " ".join(f"{c:>13s}" for c in cols))
    for name, m in rows.items():
        vals = " ".join(str(m[c]).rjust(13) for c in cols)
        print(f"{name:9s} {vals}")


if __name__ == "__main__":
    main()