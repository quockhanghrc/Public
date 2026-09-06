"""Extract per-(user,item) overall ratings from Amazon Beauty raw reviews.

Reads data/Beauty_5.json (one JSON object per line), applies core-5
iterative filtering, builds sequential id maps, and writes
data/ratings.json as {user_int: {item_int: rating_int}}.
"""
import json
import pandas as pd

REVIEWS_PATH = "data/Beauty_5.json"
RATINGS_OUT = "data/ratings.json"
THRESHOLD = 5


def read_json_lines(path):
    """Read a JSON-lines file into a list of dicts."""
    records = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def main():
    print("reading reviews...")
    reviews = read_json_lines(REVIEWS_PATH)
    print(f"raw reviews: {len(reviews)}")

    # Extract only the three fields we need: reviewerID, asin, overall
    df = pd.DataFrame({
        "user_id": [r["reviewerID"] for r in reviews],
        "item_id": [r["asin"] for r in reviews],
        "rating": [int(r["overall"]) for r in reviews],
    })
    del reviews  # free memory

    # ---- core-5 iterative filtering (same as 01_preprocess.py) ----
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

    # ---- sequential id maps (sorted for determinism) ----
    user_ids = sorted(df["user_id"].unique())
    item_ids = sorted(df["item_id"].unique())
    u_map = {u: i for i, u in enumerate(user_ids)}
    i_map = {i: j for j, i in enumerate(item_ids)}

    # ---- build {user_int: {item_int: rating_int}} ----
    ratings_dict = {}
    for _, row in df.iterrows():
        u = u_map[row["user_id"]]
        it = i_map[row["item_id"]]
        r = int(row["rating"])
        if u not in ratings_dict:
            ratings_dict[u] = {}
        ratings_dict[u][it] = r

    # ---- write output ----
    with open(RATINGS_OUT, "w") as f:
        json.dump(ratings_dict, f)

    print(f"wrote {RATINGS_OUT}")
    print(f"  users={len(user_ids)}  items={len(item_ids)}")
    print(f"  rating value counts:")
    print(df["rating"].value_counts().sort_index().to_string())


if __name__ == "__main__":
    main()