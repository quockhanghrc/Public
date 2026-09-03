"""Core-5 preprocessing of Amazon Beauty 5-core reviews -> inter.json + item_text.json.

Matches ref/nonameuntitled_tiger/notebooks/DatasetProcessing.ipynb cells 2-14
(reviews -> core-5 filter -> sequential id remap -> per-user sorted item lists),
plus builds item_text.json (content strings) from metadata for the embedding stage.
"""
import ast
import json
import re
from collections import defaultdict

import pandas as pd

REVIEWS_PATH = "data/Beauty_5.json"
META_PATH = "data/metadata.json"
INTER_OUT = "data/inter.json"
TEXT_OUT = "data/item_text.json"
THRESHOLD = 5  # core-5


def read_json_lines(path):
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def preprocess_meta(m):
    """Reference DatasetProcessing.preprocess string format."""
    title = (m.get("title") or "").strip()
    cats = m.get("categories") or []
    cat_str = ", ".join(cats[0]) if cats and cats[0] else ""
    desc = (m.get("description") or "").strip()
    # strip stray HTML entities
    desc = re.sub(r"&#\d+;", " ", desc)
    return f"Title: {title or 'None'}. Categories: {cat_str}. Description: {desc or 'None'}."


def _safe_parse_meta(line):
    """Parse a 2014 Amazon metadata record (python-literal syntax) WITHOUT executing it.

    ast.literal_eval only evaluates literals, so it cannot run arbitrary code — unlike the
    reference notebook's eval(). Falls back to json.loads for any records that are JSON.
    """
    try:
        return ast.literal_eval(line)
    except (ValueError, SyntaxError):
        return json.loads(line)


def main():
    reviews = read_json_lines(REVIEWS_PATH)  # ~200k dicts (ok in RAM)
    df = pd.DataFrame({
        "user_id": [r["reviewerID"] for r in reviews],
        "item_id": [r["asin"] for r in reviews],
        "ts": [r.get("unixReviewTime", 0) for r in reviews],
        "summary": [r.get("summary", "") or "" for r in reviews],
        "reviewText": [r.get("reviewText", "") or "" for r in reviews],
    })
    del reviews
    print(f"raw events: {len(df)}")

    # ---- core-5 iterative filtering (idempotent on an already-core-5 input) ----
    df = df[["user_id", "item_id", "ts", "summary", "reviewText"]]
    while True:
        user_counts = df["user_id"].value_counts()
        item_counts = df["item_id"].value_counts()
        good_users = set(user_counts[user_counts >= THRESHOLD].index)
        good_items = set(item_counts[item_counts >= THRESHOLD].index)
        n_before = len(df)
        df = df[df["user_id"].isin(good_users) & df["item_id"].isin(good_items)]
        if len(df) == n_before:
            break
    print(f"after core-{THRESHOLD}: {len(df)} events")

    # ---- sequential id remap (sorted for determinism) ----
    user_ids = sorted(df["user_id"].unique())
    item_ids = sorted(df["item_id"].unique())
    u_map = {u: i for i, u in enumerate(user_ids)}
    i_map = {i: j for j, i in enumerate(item_ids)}
    df["iuser"] = df["user_id"].map(u_map)
    df["iitem"] = df["item_id"].map(i_map)
    print(f"users={len(user_ids)} items={len(item_ids)}")

    # ---- per-user sorted (by time) item sequence ----
    df = df.sort_values(["iuser", "ts"]).reset_index(drop=True)
    inter = {
        int(u): [int(i) for i in grp["iitem"].tolist()]
        for u, grp in df.groupby("iuser", sort=True)
    }
    with open(INTER_OUT, "w") as f:
        json.dump(inter, f)
    assert all(len(v) >= THRESHOLD for v in inter.values()), "core-5 invariant broken"
    print(f"wrote {INTER_OUT}: {len(inter)} users")

    # ---- content text from metadata (with review-text fill for missing title/desc) ----
    meta = {}
    with open(META_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            m = _safe_parse_meta(line)  # literal_eval (safe); reference used eval()
            meta[m["asin"]] = m

    # aggregate review text per item (for fill-in and any item missing from meta)
    review_text_by_item = (
        df.assign(rev=lambda d: d["summary"] + ". " + d["reviewText"])
          .groupby("item_id")["rev"]
          .apply(lambda s: " ".join(str(x) for x in s[:3]))
          .to_dict()
    )

    texts = {}
    for iid in item_ids:  # iid is the ORIGINAL asin string
        new_id = i_map[iid]
        m = meta.get(iid, {})
        text = preprocess_meta(m)
        # fill-in: if title or description empty, append review text so content is non-trivial
        if not (m.get("title") or "").strip() or not (m.get("description") or "").strip():
            fill = review_text_by_item.get(iid, "")
            if fill.strip():
                text = text + " Reviews: " + fill.strip()
        texts[int(new_id)] = text

    with open(TEXT_OUT, "w") as f:
        json.dump(texts, f)
    print(f"wrote {TEXT_OUT}: {len(texts)} items")


if __name__ == "__main__":
    main()