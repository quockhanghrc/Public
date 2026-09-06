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
app = APP          # modal deploy looks for an `app` (lowercase) object
application = APP  # ...or an `application` object in some versions
VOL = modal.Volume.from_name("my-volume")   # EXISTING volume — never create a new one
VOL_MOUNT = "/tiger"             # where my-volume is mounted (root holds user's other projects)
APP_DIR = "/tiger/tiger_work"    # TIGER subdir inside my-volume — keeps its busy root clean
HF_CACHE = f"{APP_DIR}/hf_cache"  # model cache lives in the volume so it's downloaded once

EXPERIMENTS = {
    "expA":  "configs/expA_tiger_rqkmeans_s512.json",
    "expB":  "configs/expB_tiger_rqvae_s512.json",
    "expC":  "configs/expC_sasrec.json",
    "expC2": "configs/expC2_sasrec_contentinit.json",
    "expS1": "configs/expS1_slm_rqkmeans_s512.json",
    "expS2": "configs/expS2_slm_rqvae_s512.json",
    "expN1": "configs/expN1_slm_narrow_s512.json",
    "expR1": "configs/expR1_slm_narrow_ratings_s512.json",
}

# Local files we do NOT upload (raw JSON sources, venv, caches, prior outputs).
EXCLUDE = {
    ".venv", "cache", "__pycache__",
    "data/Beauty_5.json", "data/metadata.json",
    "checkpoints", "tensorboard_logs", "modal_main.py",
    # Derived s512 outputs: computed on Modal and already in my-volume. NEVER re-upload
    # local copies (the local index/weights can be stale and would clobber the promoted v2b).
    "data/content_embeddings_s512.pkl",
    "data/index_rqkmeans_s512.json",
    "data/index_rqvae_s512.json", "data/index_rqvae_s512_v1.json",
    "data/index_rqvae_s512_v2a.json", "data/index_rqvae_s512_v2b.json", "data/index_rqvae_s512_v2c.json",
    "data/rqvae_s512.pt", "data/rqvae_s512_v1.pt",
    "data/rqvae_s512_v2a.pt", "data/rqvae_s512_v2b.pt", "data/rqvae_s512_v2c.pt",
}

image = (
    modal.Image.debian_slim(python_version="3.11")
    .pip_install(
        "torch", "transformers==4.57.6",   # 4.57.6 = the version validated locally
        "sentence-transformers==3.4.1", "scikit-learn", "pandas", "numpy", "murmurhash", "tensorboard",
        "peft", "accelerate",              # LoRA for the SLM backbone
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
        [sys.executable, "scripts/02_embed_sentencet5.py", "--max_seq", "512"],
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
               timeout=2 * 3600, memory=4096, max_containers=1)
def rq_vae(extra: str = ""):
    import torch
    print("cuda:", torch.cuda.is_available(), flush=True)
    args = (os.environ.get("MODAL_RQVAE_ARGS", "") + " " + extra).split()
    cmd = ["python", "scripts/rqvae_pipeline.py"] + args
    print("run:", " ".join(cmd), flush=True)
    r = subprocess.run(cmd, cwd=APP_DIR)
    r.check_returncode()
    return "rqvae:" + " ".join(args)


# RQ-VAE collapse-fix sweep: three variants, sequential on one T4, versioned outputs
# (never overwrite the v1 degenerate index). v2a no-reset / v2b reset / v2c reset+EMA.
SWEEP_VARIANTS = {
    "v2a": dict(reset=0, ema=0.0),
    "v2b": dict(reset=1, ema=0.0),
    "v2c": dict(reset=1, ema=0.9),
}


@APP.function(image=image, gpu="T4", volumes={VOL_MOUNT: VOL},
               timeout=3 * 3600, memory=4096, max_containers=1)
def rq_vae_sweep():
    import torch
    print("cuda:", torch.cuda.is_available(), flush=True)
    for name, cfg in SWEEP_VARIANTS.items():
        cmd = [
            "python", "scripts/rqvae_pipeline.py",
            "--reset-period", str(cfg["reset"]),
            "--ema-decay", str(cfg["ema"]),
            "--epochs", "100", "--hidden", "64",
            "--out", f"data/index_rqvae_s512_{name}.json",
            "--state", f"data/rqvae_s512_{name}.pt",
        ]
        print(">>> sweep", name, " ".join(cmd), flush=True)
        r = subprocess.run(cmd, cwd=APP_DIR)
        r.check_returncode()
    return "rqvaesweep done"


@APP.function(image=image, volumes={VOL_MOUNT: VOL},
               timeout=1 * 3600, memory=8192, cpu=8, max_containers=1)
def tokenizer_metrics():
    r = subprocess.run(["python", "scripts/06_tokenizer_metrics.py"], cwd=APP_DIR)
    r.check_returncode()
    return "metrics"


@APP.function(image=image, gpu="T4", volumes={VOL_MOUNT: VOL},
               timeout=2 * 3600, memory=4096, max_containers=1)
def fetch_slm(slm_id: str):
    """Download SLM weights straight from HuggingFace into my-volume (reused, no local download)."""
    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer
    os.environ.setdefault("HF_HOME", HF_CACHE)
    print("cuda:", torch.cuda.is_available(), "| slm:", slm_id, flush=True)
    tok = AutoTokenizer.from_pretrained(slm_id, cache_dir=HF_CACHE)
    m = AutoModelForCausalLM.from_pretrained(slm_id, torch_dtype=torch.float16, cache_dir=HF_CACHE)
    free, total = torch.cuda.mem_get_info()
    print(f"tokenizer vocab={len(tok)} model params={sum(p.numel() for p in m.parameters()):,} "
          f"vram_free={free/2**30:.1f}/{total/2**30:.1f} GiB", flush=True)
    return {"downloaded": slm_id}


# ------------------------------------------------------------ training stage
@APP.function(image=image, gpu="T4", volumes={VOL_MOUNT: VOL},
               timeout=4 * 3600, memory=4096, max_containers=2)   # 2 = run SLM expS1 & expS2 in parallel
def train(params_path: str, smoke: bool = False):
    import torch
    import shutil
    import json as _json
    # --- self-heal: copy CURRENT code/config into the volume so the subprocess never
    #     runs stale code. (modal volume put CLI is broken in 1.4.1 -> only sync via API
    #     from the baked image the deploy ships.) ---
    try:
        imgdir = os.path.dirname(os.path.abspath(__file__))
        for rel in ["train_slm_narrow.py", "train_slm_tiger.py",
                    "modeling/models/slm_tiger.py", "modeling/models/slm_tiger_narrow.py",
                    "modeling/metric/base.py", "modeling/metric/__init__.py",
                    "modeling/models/__init__.py", "modeling/utils/__init__.py", params_path]:
            src = os.path.join(imgdir, rel)
            dst = os.path.join(APP_DIR, rel)
            if os.path.isfile(src):
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                shutil.copy2(src, dst)
                print(f"[sync] {rel} -> volume", flush=True)
    except Exception as e:
        print("[sync-warn]", e, flush=True)

    cfg = _json.load(open(os.path.join(APP_DIR, params_path)))  # Modal fn cwd != APP_DIR; subprocess sets it
    if cfg.get("experiment_name") and cfg["experiment_name"].startswith(("expN", "expR")):
        script = "train_slm_narrow.py"
    elif cfg.get("slm_id"):                                        # SLM (Qwen) generative retrieval
        script = "train_slm_tiger.py"
    elif cfg["dataset"].get("sampler_type") == "sasrec":
        script = "train_sasrec.py"
    else:
        script = "train_tiger.py"
    print("cuda:", torch.cuda.is_available(),
          torch.cuda.get_device_name(0) if torch.cuda.is_available() else "-",
          "| trainer:", script, "| cfg:", params_path, flush=True)
    import sys as _sys
    env = {**os.environ, "HF_HOME": HF_CACHE}
    if smoke:
        env["MODAL_SMOKE"] = "1"
    r = subprocess.run([_sys.executable, script, "--params", params_path], cwd=APP_DIR,
                       env=env)
    if r.returncode != 0:
        print(f"[ERROR] {script} exited with code {r.returncode}", flush=True)
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


def _maybe_smoke_cfg(exp, enabled):
    """If enabled, write a reduced-step copy of the exp config so train/grid smoke-gates cheaply."""
    if not enabled:
        return None, EXPERIMENTS[exp]
    import json as _json
    src = EXPERIMENTS[exp]
    cfg = _json.load(open(src))
    cfg["train_steps_num"] = 60
    cfg["valid_step"] = 60   # only ~1 mid eval; with mid-epoch cap-break it effectively runs just the final eval
    cfg["eval_step"] = 9999       # DISABLE eval during smoke to avoid slow beam-search stalls
    cfg["log_steps"] = 10
    cfg["eval_mid_subset"] = 20  # keep mid-evals tiny/fast in smoke (unused since eval_step=9999)
    cfg["eval_subset"] = 20   # smoke eval must be tiny (.venv: 500-user beam eval got stuck); 20 users keeps smoke seconds-long
    cfg["early_stop_check"] = 500  # match eval_subset
    if cfg.get("slm_id"):
        cfg["dataloader"]["train_batch_size"] = 8     # 1.5B: keep small batch even in smoke
        cfg["dataloader"]["validation_batch_size"] = 8
    else:
        cfg["dataloader"]["train_batch_size"] = 128
        cfg["dataloader"]["validation_batch_size"] = 64
    sp = f"configs/_smoke_{exp}.json"
    _json.dump(cfg, open(sp, "w"), indent=2)
    return sp, EXPERIMENTS[exp]


@APP.local_entrypoint()
def main(step: str = os.environ.get("MODAL_STEP", "embed"),
         exp: str = os.environ.get("MODAL_EXP", ""),
         smoke: bool = bool(os.environ.get("MODAL_SMOKE", ""))):
    # step = embed | tok | rqvaesweep | metrics | train | grid ; exp = expA/expB/expC/expC2
    import asyncio
    smoke_cfg = {e: _maybe_smoke_cfg(e, smoke)[0] for e in EXPERIMENTS} if smoke else {}
    asyncio.run(_prep())
    if step == "embed":
        embed_items.remote()          # T4 -> data/content_embeddings_s512.pkl (my-volume)
    elif step == "tok":
        rq_kmeans.remote()            # cpu -> index_rqkmeans_s512.json
        rq_vae.remote()               # T4   -> index_rqvae_s512.json + weights
    elif step == "rqvaesweep":
        rq_vae_sweep.remote()         # T4 -> v2a/v2b/v2c versioned, one app
    elif step == "metrics":
        tokenizer_metrics.remote()    # cpu -> results_tokenizer.csv
    elif step == "fetchslm":
        slm = os.environ.get("MODAL_SLM_ID", "Qwen/Qwen2.5-1.5B")
        fetch_slm.remote(slm)         # T4 -> Qwen weights into my-volume/hf_cache
    elif step == "train":
        assert exp in EXPERIMENTS, f"unknown exp {exp!r}; choose from {list(EXPERIMENTS)}"
        train.remote(smoke_cfg.get(exp) or EXPERIMENTS[exp], smoke=smoke)

    elif step == "grid":
        exps = [exp] if exp else list(EXPERIMENTS)
        for e in exps:
            train.remote(smoke_cfg.get(e) or EXPERIMENTS[e], smoke=smoke)   # sequential; each completes then app done
    else:
        raise ValueError(f"unknown step {step!r}")
    stop_completed(step)
    print(f"DONE step={step} exp={exp} smoke={smoke}")