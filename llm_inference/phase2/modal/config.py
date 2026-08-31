"""Central configuration for the phase2 Modal deployment.

Everything is env-overridable so the same deploy.py can be deployed to GPU or CPU
and the served model can be swapped without code changes.

Key env vars
------------
MODAL_HARDWARE   "gpu" (default) or "cpu"
MODEL_NAME       HuggingFace model id (default Qwen/Qwen2.5-0.5B-Instruct)
SERVED_MODEL_NAME  name clients use in requests (default chat-model)
MODAL_GPU        Modal GPU spec for the GPU profile (default "T4")
MODAL_CPU        CPU cores for the CPU profile (default 2)
MODAL_CPU_MEMORY  container memory for the CPU vLLM function, MiB (default 12288)
MODAL_CPU_KVCACHE_GB  vLLM CPU KV-cache cap in GiB (default 4; without this
                 vLLM auto-sizes KV cache to host RAM and blows the limit)
MODAL_WORKSPACE  workspace suffix, only used to keep app names unique
METRICS_TOKEN    optional bearer token protecting /metrics (Grafana Cloud
                 scraping requires auth on the target URL); unset -> open
"""

import os

MINUTES = 60

# Model ---------------------------------------------------------------------
MODEL_NAME = os.environ.get("MODEL_NAME", "Qwen/Qwen2.5-0.5B-Instruct")
SERVED_MODEL_NAME = os.environ.get("SERVED_MODEL_NAME", "chat-model")

# vLLM serve settings (kept in sync with phase1's vllm-config.yaml)
MAX_MODEL_LEN = int(os.environ.get("MAX_MODEL_LEN", "2048"))
MAX_NUM_SEQS = int(os.environ.get("MAX_NUM_SEQS", "16"))
MAX_NUM_BATCHED_TOKENS = int(os.environ.get("MAX_NUM_BATCHED_TOKENS", "2048"))
ENABLE_PREFIX_CACHING = os.environ.get("ENABLE_PREFIX_CACHING", "true").lower() == "true"

# Ports ---------------------------------------------------------------------
# The exposed port (8000) is served by the metrics gateway, which reverse
# proxies the OpenAI API to vLLM running on the internal port (8001) and
# appends GPU / container metrics to /metrics.
VLLM_PORT = int(os.environ.get("VLLM_PORT", "8000"))
VLLM_INTERNAL_PORT = int(os.environ.get("VLLM_INTERNAL_PORT", "8001"))

# API wrapper ---------------------------------------------------------------
API_PORT = int(os.environ.get("API_PORT", "80"))

# Hardware profiles ---------------------------------------------------------
HARDWARE = os.environ.get("MODAL_HARDWARE", "gpu").lower()
if HARDWARE not in ("gpu", "cpu"):
    raise ValueError(f"MODAL_HARDWARE must be 'gpu' or 'cpu', got {HARDWARE!r}")

DEFAULT_GPU = os.environ.get("MODAL_GPU", "T4")
DEFAULT_CPU = int(os.environ.get("MODAL_CPU", "2"))

# Container memory for the vLLM function (MiB). Only applied on the CPU profile;
# on GPU the memory request is derived from the GPU spec.
CPU_MEMORY_MIB = int(os.environ.get("MODAL_CPU_MEMORY", "12288"))

# Cap for the vLLM CPU KV cache (GiB). Without it vLLM sizes the KV cache to
# ~all host RAM and the container is OOM-killed during warmup.
CPU_KVCACHE_GB = int(os.environ.get("MODAL_CPU_KVCACHE_GB", "4"))

# vLLM container startup timeout (seconds). CPU cold starts -- even in eager
# mode -- are much slower than GPU, so give the CPU profile more headroom.
# Default 45 min for CPU, 10 min for GPU.
STARTUP_TIMEOUT_S = int(
    os.environ.get("STARTUP_TIMEOUT_S", "2700" if HARDWARE == "cpu" else "600")
)

# Metrics-gateway upstream request timeout (seconds). Long generations on the
# slow CPU profile exceed the default, so give CPU more headroom.
UPSTREAM_TIMEOUT_S = int(
    os.environ.get("UPSTREAM_TIMEOUT_S", "600" if HARDWARE == "cpu" else "120")
)

# App names -----------------------------------------------------------------
# vLLM apps are suffixed by hardware so a GPU and a CPU deployment can coexist.
_VLLM_APP_BASE = "phase2-vllm"
VLLM_APP_NAME = f"{_VLLM_APP_BASE}-{HARDWARE}"
API_APP_NAME = "phase2-api"

# Image / build -------------------------------------------------------------
VLLM_GPU_VERSION = os.environ.get("VLLM_GPU_VERSION", "0.21.0")
CUDA_IMAGE = os.environ.get("CUDA_IMAGE", "nvidia/cuda:12.9.0-devel-ubuntu22.04")
CPU_IMAGE = os.environ.get("CPU_IMAGE", "vllm/vllm-openai-cpu:latest-x86_64")
PYTHON_VERSION = os.environ.get("PYTHON_VERSION", "3.12")


def vllm_serve_command() -> list[str]:
    """Build the `vllm serve` argv for the selected hardware profile.

    Note: `--device` and `--cpu-kvcache-space` were removed from the vLLM CLI
    in 0.21.0 (the GPU / CPU images default to the right device, and CPU KV
    cache sizing is auto-configured), so neither is emitted here.
    """
    cmd = [
        "vllm",
        "serve",
        MODEL_NAME,
        "--served-model-name",
        SERVED_MODEL_NAME,
        "--host",
        "0.0.0.0",
        "--port",
        str(VLLM_INTERNAL_PORT),
        "--max-model-len",
        str(MAX_MODEL_LEN),
        "--max-num-seqs",
        str(MAX_NUM_SEQS),
        "--max-num-batched-tokens",
        str(MAX_NUM_BATCHED_TOKENS),
        "--uvicorn-log-level=info",
    ]
    if HARDWARE == "cpu":
        # CPU image (vllm-openai-cpu:latest = vLLM 0.27.1) inductor-compiles the
        # model at startup; on 2 cores that takes a long time. Eager mode skips
        # the compile for much faster cold starts.
        cmd.append("--enforce-eager")
    if ENABLE_PREFIX_CACHING:
        cmd.append("--enable-prefix-caching")
    return cmd


def gpu_spec():
    """Modal GPU spec for the GPU profile (may be several GPUs: 'T4:2')."""
    return DEFAULT_GPU


def cpu_spec():
    """Modal CPU cores for the CPU profile."""
    return DEFAULT_CPU
