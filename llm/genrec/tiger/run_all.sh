#!/usr/bin/env bash
# End-to-end artifact generation for TIGER (Sentence-T5 + RQ-KMeans) on Beauty.
# Runs only the data/tokenization stages (offline). Training is separate (see README).
set -euo pipefail

if [ ! -x ".venv/Scripts/python.exe" ]; then
  echo "No .venv found. Create it first: uv venv .venv --python 3.11 && uv pip install --python .venv/Scripts/python.exe -r requirements.txt"
  exit 1
fi

# Persistent local model cache so Sentence-T5 is not re-downloaded.
export HF_HOME="$PWD/cache/hf"
export TRANSFORMERS_CACHE="$PWD/cache/hf"
export HUGGINGFACE_HUB_CACHE="$PWD/cache/hf"
export SENTENCE_TRANSFORMERS_HOME="$PWD/cache/hf/sentence_transformers"

PY="$PWD/.venv/Scripts/python.exe"

echo "=== 00 decompress ===" ;        "$PY" scripts/00_decompress.py
echo "=== 01 preprocess ===" ;        "$PY" scripts/01_preprocess.py
echo "=== 02 Sentence-T5 embed ===" ; "$PY" scripts/02_embed_sentencet5.py
echo "=== 03 RQ-KMeans ===" ;         "$PY" scripts/03_rq_kmeans.py
echo "=== 04 verify ===" ;            "$PY" scripts/04_verify_artifacts.py

echo ""
echo "Artifacts ready. Train with:"
echo "  $PY train_tiger.py --params configs/tiger_train_config.json"
echo "Smoke-test (CPU): $PY train_tiger.py --params configs/smoke_tiger_config.json"