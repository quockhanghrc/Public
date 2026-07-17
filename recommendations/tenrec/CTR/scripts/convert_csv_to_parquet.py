"""
Convert the large Tenrec CTR CSV (~10GB) into many smaller parquet files
in batches for smoother downstream processing.

Usage:
    python convert_csv_to_parquet.py

Tunable knobs (edit below or pass via CLI):
    --rows-per-file : number of rows per output parquet file
    --input         : path to the source CSV
    --output-dir    : directory to write parquet shards
"""

import argparse
import os

import pandas as pd


def parse_args():
    p = argparse.ArgumentParser(description="Batch-convert a large CSV into parquet shards.")
    p.add_argument(
        "--input",
        default=r"d:\OneDrive\python-code\Public\recommendations\tenrec\CTR\data\ctr_data_1M.csv",
    )
    p.add_argument(
        "--output-dir",
        default=r"d:\OneDrive\python-code\Public\recommendations\tenrec\CTR\data\parquet",
    )
    p.add_argument(
        "--rows-per-file",
        type=int,
        default=2_000_000,
        help="Number of rows written to each parquet shard.",
    )
    p.add_argument(
        "--chunksize",
        type=int,
        default=500_000,
        help="Internal read chunk size (rows read from CSV at a time).",
    )
    return p.parse_args()


def main():
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    # Explicit dtypes keep memory predictable and parquet schema stable.
    # Use pandas nullable integer types (capital I) so that the "\N" null
    # marker is preserved as <NA> instead of raising a cast error.
    dtype = {
        "user_id": "Int64",
        "item_id": "Int64",
        "click": "Int8",
        "follow": "Int8",
        "like": "Int8",
        "share": "Int8",
        "video_category": "Int32",
        "watching_times": "Int32",
        "gender": "Int8",
        "age": "Int8",
        "hist_1": "Int64",
        "hist_2": "Int64",
        "hist_3": "Int64",
        "hist_4": "Int64",
        "hist_5": "Int64",
        "hist_6": "Int64",
        "hist_7": "Int64",
        "hist_8": "Int64",
        "hist_9": "Int64",
        "hist_10": "Int64",
    }

    total_rows = 0
    file_index = 0
    buffer = []

    print(f"Reading {args.input} in chunks of {args.chunksize:,} rows ...")

    reader = pd.read_csv(
        args.input,
        dtype=dtype,
        chunksize=args.chunksize,
        na_values=["\\N"],
        keep_default_na=True,
    )

    for chunk in reader:
        buffer.append(chunk)
        buffered_rows = sum(len(c) for c in buffer)

        if buffered_rows >= args.rows_per_file:
            out_df = pd.concat(buffer, ignore_index=True)
            out_path = os.path.join(args.output_dir, f"ctr_part_{file_index:04d}.parquet")
            out_df.to_parquet(out_path, engine="pyarrow", index=False)
            total_rows += len(out_df)
            print(f"  wrote {out_path}  ({len(out_df):,} rows)")
            file_index += 1
            buffer = []

    # Flush remaining rows
    if buffer:
        out_df = pd.concat(buffer, ignore_index=True)
        out_path = os.path.join(args.output_dir, f"ctr_part_{file_index:04d}.parquet")
        out_df.to_parquet(out_path, engine="pyarrow", index=False)
        total_rows += len(out_df)
        print(f"  wrote {out_path}  ({len(out_df):,} rows)")
        file_index += 1

    print(f"Done. {file_index} parquet file(s), {total_rows:,} total rows.")


if __name__ == "__main__":
    main()
