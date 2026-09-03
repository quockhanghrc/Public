"""Decompress the two Amazon Beauty 5-core gz files into JSON-lines."""
import gzip

JOBS = [
    ("data/reviews_Beauty_5.json.gz", "data/Beauty_5.json"),
    ("data/meta_Beauty.json.gz", "data/metadata.json"),
]

for src, dst in JOBS:
    with gzip.open(src, "rt", encoding="utf-8") as fin, open(dst, "w", encoding="utf-8") as fout:
        for i, line in enumerate(fin, 1):
            fout.write(line)
    print(f"decompressed {src} -> {dst} ({i} lines)")