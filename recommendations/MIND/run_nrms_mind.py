"""
Modal app for NRMS training on the MIND dataset (GPU).

Design (see plan in /memories/session/plan.md):
  - CODE is baked into the container image via Image.add_local_dir (rebuild on
    `modal run`/`deploy`), so local edits to src/ and main.py are reflected.
  - EMBEDDING MODELS (HuggingFace) are DOWNLOADED INSIDE the Modal app at runtime
    into a Volume-backed cache (/data/model_cache). They are NEVER uploaded from
    local. The cache persists across runs (downloaded once, reused after).
  - DATA + CHECKPOINTS + MODEL CACHE live on a persistent modal.Volume mounted at
    /data inside the container. Outputs are persisted back to the Volume via
    volume.batch_upload (force=True), which writes paths RELATIVE to /data — so on
    the Volume the checkpoints live at /checkpoints/<run_name>/..., NOT
    /data/checkpoints/... (that path does not exist on the Volume).
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

  # Category-aware variants (news category/subcategory conditioning; default
  # category_mode is "none" = ignore). concat = append cat/subcat embeddings to
  # each title-word vector; cross = category embedding cross-attends over words.
  modal run --detach run_nrms_mind.py --run-name exp03 --epochs 5 --category-mode concat
  modal run --detach run_nrms_mind.py --run-name exp04 --epochs 5 --category-mode cross

  # Pull checkpoints back later (from any machine) — they live on the Volume at
  # /checkpoints/<run_name> (NOTE: NOT /data/checkpoints/...):
  modal volume get nrms-mind-vol /checkpoints/exp01 ./checkpoints/exp01
"""

import io
import os
import subprocess
from typing import Optional

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
#   /data/checkpoints/<run_name>   (written by the run INSIDE the container)
#   /data/model_cache               (HF models downloaded IN-APP, persisted)
# NOTE: batch_upload writes paths relative to /data, so on the Volume these
# appear at /checkpoints/<run_name>/... and /model_cache/... (NOT /data/...).


def setup_secrets():
    """Idempotently configure Modal auth + HF secret from the SECRET SLOT.

    Safe to call every run: if the slot is empty it warns and returns. If the
    values are present it sets the Modal token (local CLI) and creates/refreshes
    the Modal secret `hf-token-secret` used by the function.
    """
    if MODAL_TOKEN_ID and MODAL_TOKEN_SECRET:
        print("[secrets] Setting Modal token (local auth)...")
        # Capture output instead of streaming it. Modal's CLI prints a '✓'
        # (U+2713) on success, which the Windows console codec (cp1252) cannot
        # encode -> would raise "charmap codec can't encode" and abort the
        # local entrypoint before the app launches. Capturing avoids that.
        try:
            subprocess.run(
                ["modal", "token", "set",
                 "--token-id", MODAL_TOKEN_ID,
                 "--token-secret", MODAL_TOKEN_SECRET],
                check=False, capture_output=True, text=True,
            )
        except Exception as _se:  # noqa: BLE001
            print(f"[secrets] modal token set failed: {_se}")
    else:
        print("[secrets] MODAL_TOKEN_* not set — skipping Modal auth "
              "(ensure `modal token set` was run manually if deploy fails).")

    if HF_TOKEN:
        print("[secrets] Creating/refreshing Modal secret 'hf-token-secret'...")
        modal.Secret.from_dict({"HF_TOKEN": HF_TOKEN})
    else:
        print("[secrets] HF_TOKEN not set — HF embedding download will be "
              "rate-limited (pointwise/random-init still works).")


# ---------------------------------------------------------------------------
# Per-phase functions. GPU is attached via the `train_phase` decorator
# (gpu="L4") — the ONLY phase that needs it. `evaluate_phase` and `report_phase`
# have NO gpu argument, so they run on CPU (matplotlib / inference only).
# (We set gpu in the decorator rather than `with_options(gpu=...)` at call time,
# which is version-sensitive across Modal client releases.)
# ---------------------------------------------------------------------------
_PHASE_TIMEOUT = 3600 * 6
_PHASE_CPU = 2.0
_PHASE_MEM = 8192 * 2


def _warn_missing_data():
    """Warn (non-fatal) if the MINDsmall data folders are absent on the Volume."""
    for d in ("MINDsmall_train", "MINDsmall_dev"):
        if not os.path.isdir(os.path.join("/data", d)):
            print(f"[data] WARNING: /data/{d} not found on the Volume. "
                  f"Upload it once with: modal volume put nrms-mind-vol "
                  f"<local {d}> /data/{d}")


def _build_main_args(run_name: str, phase: str, params: dict) -> list:
    """Build the `python main.py --phase <phase> ...` argv list from params.

    RETRIEVAL / HARD-NEGATIVE MINING DELEGATION
    -------------------------------------------
    This launcher does NOT implement retrieval itself. When `train_mode ==
    "listwise_hn"`, we forward the mining flags (`--mine_num_hn`, `--mine_model`,
    `--mine_cache_dir`, `--mine_max_news`) to `main.py`. `main.py`'s train phase
    then performs the retrieval internally inside `prepare_data()`:
        DenseRetriever.build_index()  -> builds the dense (MiniLM) ANN index
        mine_hard_negatives()         -> mines hard negatives per impression
        build_impression_samples_hn() -> assembles [positives + mined] samples
    So `run_nrms_mind.py` "does what main.py does" for retrieval by delegating to
    the same code path (src/retrieval.py via src/data.py), not by re-implementing
    it. This keeps the launcher and main.py in lockstep (no divergence).
    """
    args = [
        "python", "-u", "main.py",
        "--phase", phase,
        "--run_name", run_name,
        "--checkpoint_dir", "/data/checkpoints",
        "--hf_cache", "/data/model_cache",
        "--epochs", str(params.get("epochs", 5)),
        "--train_mode", params.get("train_mode", "listwise"),
        "--in_time_val_frac", str(params.get("in_time_val_frac", 0.0)),
    ]
    if params.get("max_train_impressions") is not None:
        args += ["--max_train_impressions", str(params["max_train_impressions"])]
    if params.get("max_dev_impressions") is not None:
        args += ["--max_dev_impressions", str(params["max_dev_impressions"])]
    if params.get("neg_samples") is not None:
        args += ["--neg_samples", str(params["neg_samples"])]
    if params.get("use_amp"):
        args += ["--use_amp"]
    if params.get("bottleneck_dim") is not None:
        args += ["--bottleneck_dim", str(params["bottleneck_dim"])]
    if params.get("use_hf_embeddings"):
        args += ["--use_hf_embeddings", "--embed_model",
                 params.get("embed_model", "sentence-transformers/all-MiniLM-L6-v2")]
        if params.get("freeze_embeddings"):
            args += ["--freeze_embeddings"]
    if params.get("category_mode", "none") != "none":
        args += ["--category_mode", params["category_mode"],
                 "--cat_embed_dim", str(params.get("cat_embed_dim", 8)),
                 "--subcat_embed_dim", str(params.get("subcat_embed_dim", 8))]
    if params.get("train_mode") == "listwise_hn":
        args += ["--mine_num_hn", str(params.get("mine_num_hn", 4)),
                 "--mine_model", params.get("mine_model",
                                            "sentence-transformers/all-MiniLM-L6-v2"),
                 "--mine_cache_dir", params.get("mine_cache_dir", "/data/model_cache")]
        if params.get("mine_max_news") is not None:
            args += ["--mine_max_news", str(params["mine_max_news"])]
    if params.get("batch_size") is not None:
        args += ["--batch_size", str(params["batch_size"])]
    if params.get("eval_batch_size") is not None:
        args += ["--eval_batch_size", str(params["eval_batch_size"])]
    return args


def _upload_run_artifacts(run_name: str, include_model_cache: bool = False):
    """Persist run outputs to the Volume via batch_upload (paths relative to /data)."""
    upload_dirs = [f"/data/checkpoints/{run_name}"]
    if include_model_cache:
        upload_dirs.append("/data/model_cache")
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
    print(f"[upload] Uploaded {total} file(s) to Volume "
          f"(checkpoints/{run_name}" + (" + model_cache" if include_model_cache else "") + ").")


def _persist_error(run_name: str, phase: str, exc: Exception):
    """Write a phase failure traceback to the Volume so it is inspectable later."""
    import traceback
    err_text = f"{phase.upper()} FAILED for run={run_name}\n\n{traceback.format_exc()}"
    print(err_text, flush=True)
    try:
        with volume.batch_upload(force=True) as upload:
            upload.put_file(io.BytesIO(err_text.encode("utf-8")),
                            f"{phase}_error_{run_name}.txt")
    except Exception as _e2:  # noqa: BLE001
        print(f"[{phase}] Could not persist error file: {_e2}", flush=True)


@app.function(
    image=image,
    volumes={"/data": volume},
    gpu="L4",                 # the ONLY phase that requests a GPU
    timeout=_PHASE_TIMEOUT,
    cpu=_PHASE_CPU,
    memory=_PHASE_MEM,
    secrets=[modal.Secret.from_name("hf-token-secret")],
)
def train_phase(run_name: str, params: dict):
    """TRAINING phase — the ONLY phase that needs a GPU (gpu="L4" in decorator).

    When `params["train_mode"] == "listwise_hn"`, the forwarded `main.py` call
    performs hard-negative mining (retrieval) as part of its data preparation
    before training — i.e. this phase is also where retrieval happens.
    """
    os.chdir("/app")
    _warn_missing_data()
    args = _build_main_args(run_name, "train", params)
    print("[train_phase] Running:", " ".join(args))
    try:
        subprocess.run(args, check=True)
        # Upload checkpoints + (if used) the HF model cache so later runs skip the
        # download. The cache lives on the Volume at /data/model_cache.
        _upload_run_artifacts(run_name, include_model_cache=params.get("use_hf_embeddings", False))
        print(f"[train_phase] Done. Checkpoints at /data/checkpoints/{run_name}")
    except Exception as _e:  # noqa: BLE001
        _persist_error(run_name, "train", _e)
        raise


@app.function(
    image=image,
    volumes={"/data": volume},
    timeout=_PHASE_TIMEOUT,
    cpu=_PHASE_CPU,
    memory=_PHASE_MEM,
    secrets=[modal.Secret.from_name("hf-token-secret")],
)
def evaluate_phase(run_name: str, params: dict):
    """EVALUATION phase — CPU only (loads best_model.pt, runs final eval + attribution)."""
    os.chdir("/app")
    args = _build_main_args(run_name, "eval", params)
    print("[evaluate_phase] Running:", " ".join(args))
    try:
        subprocess.run(args, check=True)
        # Upload eval artifacts (eval_results.npz, attribution_results.json) so the
        # separate-process report_phase can reload them from the Volume.
        _upload_run_artifacts(run_name, include_model_cache=False)
        print(f"[evaluate_phase] Done for {run_name}.")
    except Exception as _e:  # noqa: BLE001
        _persist_error(run_name, "evaluate", _e)
        raise


@app.function(
    image=image,
    volumes={"/data": volume},
    timeout=_PHASE_TIMEOUT,
    cpu=_PHASE_CPU,
    memory=_PHASE_MEM,
    secrets=[modal.Secret.from_name("hf-token-secret")],
)
def report_phase(run_name: str, params: dict):
    """REPORTING phase — CPU only (matplotlib figures + run_config.json update)."""
    os.chdir("/app")
    args = _build_main_args(run_name, "report", params)
    print("[report_phase] Running:", " ".join(args))
    try:
        subprocess.run(args, check=True)
        # Upload the run folder again so report figures + updated run_config.json
        # are persisted to the Volume.
        _upload_run_artifacts(run_name, include_model_cache=False)
        print(f"[report_phase] Done for {run_name}.")
    except Exception as _e:  # noqa: BLE001
        _persist_error(run_name, "report", _e)
        raise


@app.function(
    image=image,
    volumes={"/data": volume},
    timeout=_PHASE_TIMEOUT,
    cpu=_PHASE_CPU,
    memory=_PHASE_MEM,
    secrets=[modal.Secret.from_name("hf-token-secret")],
)
def run_pipeline(run_name: str, params: dict):
    """Orchestrate the full pipeline: GPU training, then CPU eval + report.

    GPU is attached ONLY to the training phase (gpu="L4" in its decorator).
    Evaluation and reporting run on CPU (no GPU requested).
    """
    # GPU is used ONLY inside train_phase (decorator has gpu="L4").
    train_phase.remote(run_name, params)
    evaluate_phase.remote(run_name, params)
    report_phase.remote(run_name, params)
    print(f"[run_pipeline] Pipeline complete for {run_name}.")


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
    category_mode: str = "none",
    cat_embed_dim: int = 8,
    subcat_embed_dim: int = 8,
    batch_size: int = 128,
    eval_batch_size: int = 256,
    mine_num_hn: int = 4,
    mine_model: str = "sentence-transformers/all-MiniLM-L6-v2",
    mine_cache_dir: str = "/data/model_cache",
    mine_max_news: Optional[int] = None,
):
    """Local entrypoint: configures secrets (from the slot) then launches the
    remote pipeline (GPU training + CPU eval/report).

    IMPORTANT — survive Ctrl+C / terminal close / laptop shutdown:
      Launch with `modal run --detach ...`. Two things make the run immune to
      closing the terminal or turning off your laptop:
        1. `--detach` keeps the Modal app alive on Modal's side after the client
           disconnects (nothing is stored on your laptop).
        2. We use `run_pipeline.spawn()` (NON-blocking) instead of `.remote()`.
           `.remote()` holds the function's input stream; pressing Ctrl+C forwards
           a cancellation signal on that stream and kills the run. `spawn()`
           returns immediately and holds no stream, so Ctrl+C only kills the local
           client — the spawned pipeline keeps running on Modal, and the detached
           app stays up until training completes.

    GPU policy: only the training phase requests an L4 GPU (gpu="L4" in its
    decorator). Evaluation and reporting run on CPU.

    Examples:
      modal run --detach run_nrms_mind.py --run-name exp01 --epochs 5 --train-mode listwise
      modal run --detach run_nrms_mind.py --run-name exp02 --epochs 5 --use-hf-embeddings
      modal run --detach run_nrms_mind.py --run-name exp04 --epochs 5 --category-mode cross

    Monitor / retrieve later (from any machine):
      modal app list
      modal app logs <app-id>     # app-id is printed by `modal run --detach`
      modal volume get nrms-mind-vol /checkpoints/<run_name> ./checkpoints/<run_name>

    If a phase fails, its error is written to the Volume as
    /<phase>_error_<run_name>.txt (retrieve with `modal volume get ...`).
    """
    setup_secrets()
    # Bundle all CLI args into a params dict forwarded to each phase function.
    params = dict(
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
        category_mode=category_mode,
        cat_embed_dim=cat_embed_dim,
        subcat_embed_dim=subcat_embed_dim,
        batch_size=batch_size,
        eval_batch_size=eval_batch_size,
        mine_num_hn=mine_num_hn,
        mine_model=mine_model,
        mine_cache_dir=mine_cache_dir,
        mine_max_news=mine_max_news,
    )
    # spawn() (non-blocking) launches the pipeline on Modal and returns immediately.
    # There is no held input stream, so a Ctrl+C on the client does NOT cancel the
    # run. Combined with `modal run --detach`, the app (and the spawned pipeline)
    # keeps running on Modal after the client/laptop exits.
    handle = run_pipeline.spawn(run_name, params)
    print(f"[launch] Pipeline spawned (handle={handle.object_id}). Training uses "
          f"GPU (L4); eval/report run on CPU. The run continues on Modal even if "
          f"you press Ctrl+C / close the terminal / shut your laptop. Monitor "
          f"with: modal app logs <app-id>")

