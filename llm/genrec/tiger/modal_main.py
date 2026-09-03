"""Modal T4 entrypoint for the TIGER grid pipeline.

Everything runs on Modal and persists to the EXISTING volume `my-volume` in the
`tiger_work/` subdir (busy volume root untouched). Includes a data stage
(embed -> rq_kmeans / rq_vae -> tokenizer metrics) and a training stage
(expA/B/C/C2). Each `modal run` invocation is one EPHEMERAL app that auto-stops
(and frees its GPU) when the local client returns — so completed apps never idle.

Run a single logical step via:
    modal run modal_main.py -- step embed|tok|metrics|train|grid
(plus `-- exp expA` for train/grid).
"""
import os
import subprocess

import modal

APP = modal.App("tiger-t4")
VOL = modal.Volume.from_name("my-volume")   # EXISTING volume — never create a new one
VOL_MOUNT = "/tiger"             # where my-volume is mounted (root holds user's other projects)
APP_DIR = "/tiger/tiger_work"    # TIGER subdir inside my-volume — keeps its busy root clean
HF_CACHE = f"{APP_DIR}/hf_cache"  # model cache lives in the volume so it's downloaded once

EXPERIMENTS = {
    "expA":  "configs/expA_tiger_rqkmeans_s512.json",
    "expB":  "configs/expB_tiger_rqvae_s512.json",
    "expC":  "configs/expC_sasrec.json",
    "expC2": "configs/expC2_sasrec_contentinit.json",
}

# Local files we do NOT upload (raw JSON sources, venv, caches, prior outputs).
EXCLUDE = {
    ".venv", "cache", "__pycache__",
    "data/Beauty_5.json", "data/metadata.json",
    "checkpoints", "tensorboard_logs", "modal_main.py",
}

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "torch", "transformers==4.57.6",   # 4.57.6 = the version validated locally
        "sentence-transformers", "scikit-learn", "pandas", "numpy", "murmurhash", "tensorboard",
    )
)


# ---------------------------------------------------------------- data stage
# max_containers=1 on every GPU function => only one T4 per function billed at a time.
@APP.function(image=image, gpu="T4", volumes={VOL_MOUNT: VOL},
               timeout=2 * 3600, memory=4096, max_containers=1)
def embed_items():
    import torch
    os.environ.setdefault("HF_HOME", HF_CACHE)                       # cache model into volume
    os.environ.setdefault("SENTENCE_TRANSFORMERS_HOME", f"{HF_CACHE}/st")
    print("cuda:", torch.cuda.is_available(), flush=True)
    r = subprocess.run(
        ["python", "scripts/02_embed_sentencet5.py", "--max_seq", "512"],
        cwd=APP_DIR)
    r.check_returncode()
    return "embedded"


@APP.function(image=image, volumes={VOL_MOUNT: VOL},
               timeout=1 * 3600, memory=8192, cpu=8, max_containers=1)
def rq_kmeans():
    r = subprocess.run(
        ["python", "scripts/03_rq_kmeans.py",
         "--emb", "data/content_embeddings_s512.pkl",
         "--out", "data/index_rqkmeans_s512.json"],
        cwd=APP_DIR)
    r.check_returncode()
    return "rqkmeans"


@APP.function(image=image, gpu="T4", volumes={VOL_MOUNT: VOL},
               timeout=1 * 3600, memory=4096, max_containers=1)
def rq_vae():
    import torch
    print("cuda:", torch.cuda.is_available(), flush=True)
    r = subprocess.run(["python", "scripts/07_rqvae_pipeline.py"], cwd=APP_DIR)
    r.check_returncode()
    return "rqvae"


@APP.function(image=image, volumes={VOL_MOUNT: VOL},
               timeout=1 * 3600, memory=8192, cpu=8, max_containers=1)
def tokenizer_metrics():
    r = subprocess.run(["python", "scripts/06_tokenizer_metrics.py"], cwd=APP_DIR)
    r.check_returncode()
    return "metrics"


# ------------------------------------------------------------ training stage
@APP.function(image=image, gpu="T4", volumes={VOL_MOUNT: VOL},
               timeout=4 * 3600, memory=4096, max_containers=1)
def train(params_path: str):
    import torch
    print("cuda:", torch.cuda.is_available(),
          torch.cuda.get_device_name(0) if torch.cuda.is_available() else "-", flush=True)
    r = subprocess.run(["python", "train_tiger.py", "--params", params_path], cwd=APP_DIR) \
        if "sasrec" not in params_path else \
        subprocess.run(["python", "train_sasrec.py", "--params", params_path], cwd=APP_DIR)
    r.check_returncode()
    return {"gpu_ok": torch.cuda.is_available()}


# ------------------------------------------------------------------- helpers
async def _prep():
    """Selectively upload the local tiger/ repo (code + small artifacts) into my-volume:/tiger_work/."""
    root = os.path.dirname(os.path.abspath(__file__))
    uploaded = 0
    async with VOL.batch_upload(force=True) as batch:
        for dirpath, _, files in os.walk(root):
            for f in files:
                lp = os.path.join(dirpath, f)
                rel = os.path.relpath(lp, root).replace("\\", "/")
                if any(rel == e or rel.startswith(e + "/") for e in EXCLUDE):
                    continue
                batch.put_file(lp, f"tiger_work/{rel}")  # relative to volume root => /tiger/tiger_work
                uploaded += 1
    print(f"uploaded {uploaded} files to my-volume:{APP_DIR}")
    # batch_upload's context manager finalizes on exit; Volume.commit() is container-only.


def stop_completed(step_name: str):
    """Best-effort: ensure the just-finished ephemeral app is stopped (it auto-stops on return)."""
    import subprocess as sp
    try:
        out = sp.run(["modal", "app", "list"], capture_output=True, text=True).stdout
        print(f"[stop] {step_name}: running/initializing apps remaining?\n{out or '(none listed)'}")
    except Exception as e:  # noqa
        print("[stop] could not query app list:", e)


@APP.local_entrypoint()
def main(step: str = "embed", exp: str = ""):
    # step = embed | tok | metrics | train | grid ; exp = expA/expB/expC/expC2 (for train/grid)
    import asyncio
    asyncio.run(_prep())
    if step == "embed":
        embed_items.remote()          # T4 -> data/content_embeddings_s512.pkl (my-volume)
    elif step == "tok":
        rq_kmeans.remote()            # cpu -> index_rqkmeans_s512.json
        rq_vae.remote()               # T4   -> index_rqvae_s512.json + weights
    elif step == "metrics":
        tokenizer_metrics.remote()    # cpu -> results_tokenizer.csv
    elif step == "train":
        assert exp in EXPERIMENTS, f"unknown exp {exp!r}; choose from {list(EXPERIMENTS)}"
        train.remote(EXPERIMENTS[exp])
    elif step == "grid":
        exps = [exp] if exp else list(EXPERIMENTS)
        for e in exps:
            train.remote(EXPERIMENTS[e])   # sequential; each completes then the app is done
    else:
        raise ValueError(f"unknown step {step!r}")
    stop_completed(step)
    print(f"DONE step={step} exp={exp}")