"""Tokenizer quality metrics: RQ-KMeans vs RQ-VAE (on S512 embeddings) + collapse gate.

For each tokenizer computes:
  - reconstruction error (MSE, relative-MSE, cosine) decoding the SID back to the input emb
  - codebook coverage    (% of 256 codes used in each of the 3 codebooks)
  - semantic collisions  (3-code prefix groups: distinct count, max group, % items in group>1)

Usage (from tiger/ cwd):
  python scripts/06_tokenizer_metrics.py [--rqvae-index F] [--rqvae-ckpt F]
      [--rqvae-hidden N] [--gate] [--thresholds c,d,e,r,s] [--out F]
  --gate : also evaluate the RQ-VAE against acceptance thresholds and print PASS/FAIL.
"""
import argparse
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
from rqvae_pipeline import RQVAE, DEVICE  # noqa: E402

EMB_PATH = "data/content_embeddings_s512.pkl"
KMEANS_INDEX = "data/index_rqkmeans_s512.json"
CODEBOOK = 256
N_CB = 3

# acceptance threshold order: coverage_b1, distinct_3codes, frac_collide_gt1, rel_mse, cosine_sim
DEFAULT_THRESHOLDS = "0.85,9000,0.25,0.08,0.95"


def load_emb():
    with open(EMB_PATH, "rb") as f:
        d = pickle.load(f)
    return np.array(d["item_id"], dtype=np.int64), np.array(d["embedding"], dtype=np.float32)


def metrics_from_codebook(codes2d):
    """codes2d: (N, 3) first-3 codes. Returns coverage + collision stats."""
    cov = []
    for c in range(N_CB):
        b = np.bincount(codes2d[:, c], minlength=CODEBOOK)
        cov.append(round(float((b > 0).mean()), 3))
    groups = defaultdict(list)
    for item, c in enumerate(codes2d):
        groups[tuple(c.tolist())].append(item)
    sizes = np.array([len(v) for v in groups.values()])
    frac = float(np.mean([len(groups[tuple(c.tolist())]) > 1 for c in codes2d])) if len(sizes) else 0.0
    return {
        "coverage_b1": cov[0], "coverage_b2": cov[1], "coverage_b3": cov[2],
        "distinct_3codes": int(len(groups)),
        "max_collision_group": int(sizes.max()) if len(sizes) else 0,
        "frac_collide_gt1": round(frac, 4),
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


def rqvae_metrics(X, ckpt, index, hidden):
    model = RQVAE(768, hidden, 0.25, [CODEBOOK] * N_CB)
    model.load_state_dict(torch.load(ckpt, map_location=DEVICE))
    model.to(DEVICE).eval()
    with torch.no_grad():
        Xhat = model({"embedding": torch.FloatTensor(X).to(DEVICE)})["embedding_hat"].cpu().numpy()
    with open(index) as f:
        idx = json.load(f)
    keys = list(idx.keys())
    codes = np.array([idx[str(i)][:N_CB] for i in range(len(idx))], np.int64)
    m = recon_metrics(X, Xhat)
    m.update(metrics_from_codebook(codes))
    return m


def gate(m, threshold_vec):
    c1, distinct, frac, rel, cos = threshold_vec
    checks = {
        "coverage_b1 >= %.2f" % c1: m["coverage_b1"] >= c1,
        "distinct_3codes >= %d" % distinct: m["distinct_3codes"] >= distinct,
        "frac_collide_gt1 <= %.2f" % frac: m["frac_collide_gt1"] <= frac,
        "rel_mse <= %.2f" % rel: m["rel_mse"] <= rel,
        "cosine_sim >= %.2f" % cos: m["cosine_sim"] >= cos,
    }
    for name, ok in checks.items():
        got = None
        if "coverage_b1" in name: got = m["coverage_b1"]
        elif "distinct_3codes" in name: got = m["distinct_3codes"]
        elif "frac_collide" in name: got = m["frac_collide_gt1"]
        elif "rel_mse" in name: got = m["rel_mse"]
        elif "cosine_sim" in name: got = m["cosine_sim"]
        print(f"  {'PASS' if ok else 'FAIL'}  {name:28s}  got={got}")
    return all(checks.values())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rqvae-index", default="data/index_rqvae_s512.json")
    ap.add_argument("--rqvae-ckpt", default="data/rqvae_s512.pt")
    ap.add_argument("--rqvae-hidden", type=int, default=64)
    ap.add_argument("--gate", action="store_true")
    ap.add_argument("--thresholds", default=DEFAULT_THRESHOLDS)
    ap.add_argument("--out", default="results_tokenizer.csv")
    args = ap.parse_args()

    _, X = load_emb()
    print("embeddings:", X.shape)
    rows = {"rqkmeans": rqkmeans_metrics(X),
            "rqvae": rqvae_metrics(X, args.rqvae_ckpt, args.rqvae_index, args.rqvae_hidden)}
    cols = list(rows["rqkmeans"].keys())
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["tokenizer"] + cols)
        for name, m in rows.items():
            w.writerow([name] + [m[c] for c in cols])
    print("\nwrote", args.out)
    print(f"{'tokenizer':9s} " + " ".join(f"{c:>13s}" for c in cols))
    for name, m in rows.items():
        print(f"{name:9s} " + " ".join(str(m[c]).rjust(13) for c in cols))

    if args.gate:
        tv = [float(x) if i != 1 else int(x) for i, x in enumerate(args.thresholds.split(","))]
        print("\n--- RQ-VAE collapse gate ---")
        ok = gate(rows["rqvae"], tv)
        print("GATE:", "PASS" if ok else "FAIL")
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()