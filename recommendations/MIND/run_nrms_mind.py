"""
Modal app for NRMS training on the MIND dataset (GPU).

Design (see plan in /memories/session/plan.md):
  - CODE is mirrored into the container via Image.copy_local_dir (rebuild on
    `modal run`/`deploy`), so local edits to src/ and main.py are reflected.
  - EMBEDDING MODELS (HuggingFace) are DOWNLOADED INSIDE the Modal app at runtime
    into a Volume-backed cache (/data/model_cache). They are NEVER uploaded from
    local. The cache persists across runs (downloaded once, reused after).
  - DATA + CHECKPOINTS + MODEL CACHE live on a persistent modal.Volume mounted at
    /data, so outputs survive restarts and are pulled back with `modal volume get`.
  - SECRETS are provided via the SECRET SLOT below — fill in later, no logic edits.

Usage:
  # Run with `modal run --detach` (NOT plain `modal run`). `--detach` keeps the
  # app alive on Modal's side after the client disconnects, and the entrypoint
  # uses train.spawn() (non-blocking) so the client is NOT holding the function's
  # input stream — meaning pressing Ctrl+C, closing the terminal, or shutting your
  # laptop does NOT cancel the run. Training keeps running on Modal's GPU until it
  # finishes. Nothing is written to / stored on your laptop.
  modal run --detach run_nrms_mind.py --run-name exp01 --epochs 5 --train-mode listwise
  modal run --detach run_nrms_mind.py --run-name exp02 --epochs 5 --use-hf-embeddings

  # 2) pull checkpoints back later (from any machine) — they live on the Volume:
  modal volume get nrms-mind-vol /data/checkpoints/exp01 ./checkpoints/exp01
"""

import io
import os
import subprocess

import modal

# =====================================================================
# SECRETS — loaded from a SEPARATE file (secrets_local.py, git-ignored).
# Keeps tokens out of this script and out of version control. If the file or a
# value is missing, the script warns and skips that part (pointwise / random-init
# still works; the HF embedding download will just be rate-limited).
# =====================================================================
try:
    from secrets_local import MODAL_TOKEN_ID, MODAL_TOKEN_SECRET, HF_TOKEN
except Exception as _e:  # noqa: BLE001 - missing/empty secrets are non-fatal
    print(f"[secrets] Could not load secrets_local.py ({_e}); "
          f"proceeding without secrets (HF download may be rate-limited).")
    MODAL_TOKEN_ID = MODAL_TOKEN_SECRET = HF_TOKEN = ""
# =================================================================


# ---------------------------------------------------------------------------
# Image: code is baked in via add_local_dir; deps pinned; HF cache on Volume.
# ---------------------------------------------------------------------------
# The MIND project code lives on the LOCAL machine next to this script (the
# folder containing main.py). add_local_dir needs a LOCAL source path; the
# second arg "/app" is the destination inside the container image. The code is
# baked into /app at build time, so no runtime path check is needed here (this
# script itself is copied to /root in the container, so __file__ differs there).
_HERE = os.path.dirname(os.path.abspath(__file__))
LOCAL_MIND_DIR = _HERE

# CUDA-enabled torch wheels come from the pytorch cu121 index, not PyPI. We use
# pip_install (with the index URL) for the torch stack and uv_pip_install for the
# rest. The +cu121 suffix is invalid for uv/PyPI, so we pin plain versions here and
# let the index URL supply the CUDA build.
_CU121_INDEX = "https://download.pytorch.org/whl/cu121"

image = (
    modal.Image.debian_slim(python_version="3.11",force_build=False)
    .apt_install("curl", "git", "wget", "build-essential")
    # CUDA torch/torchvision from the pytorch cu121 index.
    .pip_install(
        "torch==2.3.0",
        "torchvision==0.18.0",
        index_url=_CU121_INDEX,
    )
    .uv_pip_install(
        "transformers==4.54.0",
        "huggingface_hub==0.34.2",
        "hf-transfer==0.1.9",
        "sentence-transformers",
        "pandas==2.2.2",
        "numpy",
        "matplotlib",

        "scikit-learn",
    )
    # Bake the LOCAL CODE into the image (no model weights — those download in-app).
    # copy=True so the code is committed to the image and later build steps (.env) work.
    .add_local_dir(LOCAL_MIND_DIR, "/app", copy=True)
    # Point HF/transformers caches at the Volume so downloads persist across runs.
    .env({
        "HF_HOME": "/data/model_cache",
        "TRANSFORMERS_CACHE": "/data/model_cache",
        "HF_HUB_DISABLE_SYMLINKS_WARNING": "1",
        "HF_HUB_ENABLE_HF_TRANSFER": "1",
        # Disable tokenizers' internal thread pool so forking DataLoader workers
        # (num_workers > 0) on Linux/Modal doesn't trigger the fork-after-
        # parallelism deadlock warning. Safe: HF tokenizer is only used once,
        # upfront, to build the embedding matrix.
        "TOKENIZERS_PARALLELISM": "false",
    })
)

app = modal.App("nrms-mind-training-1")
volume = modal.Volume.from_name("nrms-mind-vol", create_if_missing=True)

# Volume mount layout inside the container:
#   /data/MINDsmall_train   (uploaded once, or present from a prior run)
#   /data/MINDsmall_dev
#   /data/checkpoints/<run_name>   (written by the run, pulled back locally)
#   /data/model_cache               (HF models downloaded IN-APP, persisted)


def setup_secrets():
    """Idempotently configure Modal auth + HF secret from the SECRET SLOT.

    Safe to call every run: if the slot is empty it warns and returns. If the
    values are present it sets the Modal token (local CLI) and creates/refreshes
    the Modal secret `hf-token-secret` used by the function.
    """
    if MODAL_TOKEN_ID and MODAL_TOKEN_SECRET:
        print("[secrets] Setting Modal token (local auth)...")
        subprocess.run(
            ["modal", "token", "set",
             "--token-id", MODAL_TOKEN_ID,
             "--token-secret", MODAL_TOKEN_SECRET],
            check=False,
        )
    else:
        print("[secrets] MODAL_TOKEN_* not set — skipping Modal auth "
              "(ensure `modal token set` was run manually if deploy fails).")

    if HF_TOKEN:
        print("[secrets] Creating/refreshing Modal secret 'hf-token-secret'...")
        modal.Secret.from_dict({"HF_TOKEN": HF_TOKEN})
    else:
        print("[secrets] HF_TOKEN not set — HF embedding download will be "
              "rate-limited (pointwise/random-init still works).")


@app.function(
    image=image,
    volumes={"/data": volume},
    gpu="T4",                 # switch to "A10G" if 384-dim MiniLM + batch 128 needs more VRAM
    timeout=3600 * 6,
    cpu=2.0,
    memory=8192,
    secrets=[modal.Secret.from_name("hf-token-secret")],
)
def train(
    run_name: str,
    use_hf_embeddings: bool = False,
    embed_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    epochs: int = 5,
    train_mode: str = "listwise",
    max_train_impressions: int = None,
    max_dev_impressions: int = None,
    in_time_val_frac: float = 0.0,
    neg_samples: int = None,
    use_amp: bool = False,
    freeze_embeddings: bool = False,
    bottleneck_dim: int = None,
    extra_args: list = None,
):
    """Run NRMS training inside the Modal container.

    Code lives in /app (image-baked). Data + checkpoints + HF cache live in /data
    (Volume). Embedding models are downloaded IN-APP into /data/model_cache.
    """
    os.chdir("/app")

    # Ensure data is present on the Volume. If not uploaded yet, this is a clear
    # signal to the user (they should `modal volume put` the MINDsmall folders once).
    for d in ("MINDsmall_train", "MINDsmall_dev"):
        if not os.path.isdir(os.path.join("/data", d)):
            print(f"[data] WARNING: /data/{d} not found on the Volume. "
                  f"Upload it once with: modal volume put nrms-mind-vol "
                  f"<local {d}> /data/{d}")

    args = [
        "python", "-u", "main.py",
        "--run_name", run_name,
        "--checkpoint_dir", "/data/checkpoints",
        "--hf_cache", "/data/model_cache",
        "--epochs", str(epochs),
        "--train_mode", train_mode,
        "--in_time_val_frac", str(in_time_val_frac),
    ]
    if max_train_impressions is not None:
        args += ["--max_train_impressions", str(max_train_impressions)]
    if max_dev_impressions is not None:
        args += ["--max_dev_impressions", str(max_dev_impressions)]
    if neg_samples is not None:
        args += ["--neg_samples", str(neg_samples)]
    if use_amp:
        args += ["--use_amp"]
    if bottleneck_dim is not None:
        args += ["--bottleneck_dim", str(bottleneck_dim)]
    if use_hf_embeddings:
        args += ["--use_hf_embeddings", "--embed_model", embed_model]
        if freeze_embeddings:
            args += ["--freeze_embeddings"]
    if extra_args:
        args += list(extra_args)

    print("[train] Running:", " ".join(args))
    try:
        subprocess.run(args, check=True)

        # Persist outputs to the Volume via the Volume API (batch_upload), which is
        # reliable regardless of mount/commit behavior. We upload BOTH the run's
        # checkpoints AND the HF model cache, so subsequent --use-hf-embeddings runs
        # skip the download (the cache lives on the Volume at /data/model_cache).
        upload_dirs = [f"/data/checkpoints/{run_name}", "/data/model_cache"]
        total = 0
        with volume.batch_upload(force=True) as upload:
            for d in upload_dirs:
                if not os.path.isdir(d):
                    continue
                for root, _, files in os.walk(d):
                    for fn in files:
                        local_path = os.path.join(root, fn)
                        rel = os.path.relpath(local_path, "/data")  # e.g. checkpoints/<run>/x.png
                        upload.put_file(local_path, rel)
                        total += 1
        print(f"[train] Uploaded {total} file(s) to Volume "
              f"(checkpoints/{run_name} + model_cache).")
        print(f"[train] Done. Checkpoints at /data/checkpoints/{run_name}")
    except Exception as _e:  # noqa: BLE001 - capture failures so they are inspectable
        import traceback
        err_text = f"TRAIN FAILED for run={run_name}\n\n{traceback.format_exc()}"
        print(err_text, flush=True)
        # Persist the error so it is visible even from a detached/spawned run
        # (whose stdout is otherwise discarded). Read it back with:
        #   modal volume get nrms-mind-vol /train_error_{run_name}.txt .
        try:
            with volume.batch_upload(force=True) as upload:
                upload.put_file(io.BytesIO(err_text.encode("utf-8")),
                                f"train_error_{run_name}.txt")
        except Exception as _e2:  # noqa: BLE001
            print(f"[train] Could not persist error file: {_e2}", flush=True)
        raise  # re-raise so the Modal function still reports failure


@app.local_entrypoint()
def main(
    run_name: str = "modal_run",
    use_hf_embeddings: bool = False,
    embed_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    epochs: int = 5,
    train_mode: str = "listwise",
    max_train_impressions: int = None,
    max_dev_impressions: int = None,
    in_time_val_frac: float = 0.0,
    neg_samples: int = None,
    use_amp: bool = False,
    freeze_embeddings: bool = False,
    bottleneck_dim: int = None,
):
    """Local entrypoint: configures secrets (from the slot) then launches the
    remote GPU training function.

    IMPORTANT — survive Ctrl+C / terminal close / laptop shutdown:
      Launch with `modal run --detach ...`. Two things make the run immune to
      closing the terminal or turning off your laptop:
        1. `--detach` keeps the Modal app alive on Modal's side after the client
           disconnects (nothing is stored on your laptop).
        2. We use `train.spawn()` (NON-blocking) instead of `train.remote()`.
           `train.remote()` holds the function's input stream; pressing Ctrl+C
           forwards a cancellation signal on that stream and kills the run (this
           is exactly the earlier failure). `spawn()` returns immediately and
           holds no stream, so Ctrl+C only kills the local client — the spawned
           function keeps running on Modal, and the detached app stays up until
           training completes.

    Examples:
      modal run --detach run_nrms_mind.py --run-name exp01 --epochs 5 --train-mode listwise
      modal run --detach run_nrms_mind.py --run-name exp02 --epochs 5 --use-hf-embeddings

    Monitor / retrieve later (from any machine):
      modal app list
      modal app logs <app-id>     # app-id is printed by `modal run --detach`
      modal volume get nrms-mind-vol /data/checkpoints/<run_name> ./checkpoints/<run_name>

    If training fails, the error is written to the Volume as
    /train_error_<run_name>.txt (retrieve with `modal volume get ...`).
    """
    setup_secrets()
    # spawn() (non-blocking) launches training on Modal and returns immediately.
    # There is no held input stream, so a Ctrl+C on the client does NOT cancel the
    # run. Combined with `modal run --detach`, the app (and the spawned function)
    # keeps running on Modal after the client/laptop exits.
    handle = train.spawn(
        run_name=run_name,
        use_hf_embeddings=use_hf_embeddings,
        embed_model=embed_model,
        epochs=epochs,
        train_mode=train_mode,
        max_train_impressions=max_train_impressions,
        max_dev_impressions=max_dev_impressions,
        in_time_val_frac=in_time_val_frac,
        neg_samples=neg_samples,
        use_amp=use_amp,
        freeze_embeddings=freeze_embeddings,
        bottleneck_dim=bottleneck_dim,
    )
    print(f"[launch] Training spawned (handle={handle.object_id}). Run continues "
          f"on Modal even if you press Ctrl+C / close the terminal / shut your "
          f"laptop. Monitor with: modal app logs <app-id>")

