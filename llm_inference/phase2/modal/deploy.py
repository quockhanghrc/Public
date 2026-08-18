"""Phase2 Modal deployment: two separate serverless services.

Service 1 - `vllm_app` (phase2-vllm-{gpu|cpu})
    Runs `vllm serve` as a subprocess, exposed via a `@modal.web_server`.
    The exposed port is served by metrics_gateway.py, which reverse-proxies
    the OpenAI-compatible API to vLLM on an internal port and appends GPU /
    container metrics to /metrics.

Service 2 - `api_app` (phase2-api)
    Reuses phase2/app/main.py (the phase1 FastAPI wrapper) unchanged. At
    container start it resolves the vLLM web URL and injects it as VLLM_URL.

Deploy (GPU, default):
    modal deploy phase2/modal/deploy.py::vllm_app
    modal deploy phase2/modal/deploy.py::api_app

Deploy (CPU):
    MODAL_HARDWARE=cpu modal deploy phase2/modal/deploy.py::vllm_app
    MODAL_HARDWARE=cpu modal deploy phase2/modal/deploy.py::api_app

Smoke test (ephemeral):
    modal run phase2/modal/deploy.py
"""

import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import modal

MODAL_DIR = Path(__file__).resolve().parent
BASE_DIR = MODAL_DIR.parent
APP_DIR = BASE_DIR / "app"

sys.path.insert(0, str(MODAL_DIR))
import config  # noqa: E402

# --------------------------------------------------------------------------
# Apps, volumes
# --------------------------------------------------------------------------
vllm_app = modal.App(name=config.VLLM_APP_NAME)
api_app = modal.App(name=config.API_APP_NAME)

HF_CACHE_VOL = modal.Volume.from_name("phase2-hf-cache", create_if_missing=True)
# The torch.compile cache is NOT safe to share across vLLM versions: the GPU
# image (vLLM 0.21.0) and CPU image (vLLM 0.27.1) write incompatible binaries,
# causing a compile/load-failure loop. Keep one volume per hardware profile.
VLLM_CACHE_VOL = modal.Volume.from_name(
    f"phase2-vllm-cache-{config.HARDWARE}", create_if_missing=True
)

_api_key = os.environ.get("API_KEY")


# --------------------------------------------------------------------------
# Images
# --------------------------------------------------------------------------
def build_vllm_image() -> modal.Image:
    base = (
        modal.Image.from_registry(config.CUDA_IMAGE, add_python=config.PYTHON_VERSION)
        .entrypoint([])
        .uv_pip_install(
            f"vllm=={config.VLLM_GPU_VERSION}",
            # vLLM 0.21.0's config convertor reads head_dim globally; transformers
            # 5.15.0 made Gemma 4's heterogeneous attention config explicit and
            # raises AmbiguousGlobalPerLayerAttributeError. Pin to 5.14.1.
            "transformers==5.14.1",
            "huggingface-hub",
            "nvidia-ml-py",
            "psutil",
        )
        .env({"HF_XET_HIGH_PERFORMANCE": "1"})
    ) if config.HARDWARE == "gpu" else (
        modal.Image.from_registry(config.CPU_IMAGE)
        .entrypoint([])
        .pip_install("nvidia-ml-py", "psutil")
        # vLLM 0.27 auto-sizes the CPU KV cache from host RAM (cgroup-ignorant),
        # which on Modal is ~360 GB and OOM-kills the container. Force a small
        # fixed budget instead.
        .env({"VLLM_CPU_KVCACHE_SPACE": str(config.CPU_KVCACHE_GB)})
    )
    # Local files (config.py, metrics_gateway.py, deploy.py) are added last so
    # they mount on container start instead of forcing a slow image rebuild on
    # every edit. MODAL_HARDWARE is baked in so the container's config.py picks
    # the right hardware profile at runtime (Modal does not pass client env).
    return (
        base.env({"PYTHONPATH": "/opt/modal", "MODEL_NAME": config.MODEL_NAME})
        .add_local_dir(MODAL_DIR, remote_path="/opt/modal")
    )


APP_REMOTE = "/workspace/app"


def build_api_image() -> modal.Image:
    return (
        modal.Image.debian_slim(python_version=config.PYTHON_VERSION)
        .pip_install(
            "fastapi==0.115.0",
            "uvicorn[standard]==0.30.6",
            "openai==1.51.0",
            "python-dotenv==1.0.1",
            "httpx==0.27.2",
            "requests==2.32.3",
            "prometheus-fastapi-instrumentator==6.1.0",
            "modal",  # so the wrapper can resolve the vLLM web URL at runtime
        )
        # Local files are added last so they mount on container start
        # instead of forcing an image rebuild on every edit.
        # APP_DIR is mounted at /workspace/app and /workspace is on PYTHONPATH,
        # so `from app.main import app` resolves like a regular package.
        # MODAL_HARDWARE is baked in at deploy time so the container resolves
        # the right vLLM deployment (phase2-vllm-gpu vs -cpu) via config.
        .env({"PYTHONPATH": "/workspace:/opt/modal", "MODAL_HARDWARE": config.HARDWARE})
        .add_local_dir(APP_DIR, remote_path=APP_REMOTE)
        .add_local_dir(MODAL_DIR, remote_path="/opt/modal")
    )


vllm_image = build_vllm_image()
api_image = build_api_image()


# --------------------------------------------------------------------------
# Service 1 - vLLM
# --------------------------------------------------------------------------
def _wait_ready(url: str, timeout: float = 1800.0) -> None:
    """Wait for readiness. Default 30 min: CPU cold starts (eager) can take a
    while on the small CPU profile."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5.0) as resp:
                if resp.status == 200:
                    return
        except OSError:
            time.sleep(2.0)
    raise RuntimeError(f"server at {url} did not become ready within {timeout}s")


def _start_vllm() -> subprocess.Popen:
    cmd = config.vllm_serve_command()
    env = dict(os.environ)
    if config.HARDWARE == "cpu":
        # vLLM CPU sizes its KV cache from host RAM (cgroup-ignorant); without a
        # hard cap it requests ~all of it and the container is OOM-killed. Set
        # the cap here too so it is guaranteed to reach the worker subprocess.
        env["VLLM_CPU_KVCACHE_SPACE"] = str(config.CPU_KVCACHE_GB)
    print("starting vLLM:", " ".join(cmd))
    print("image VLLM_CPU_KVCACHE_SPACE=", os.environ.get("VLLM_CPU_KVCACHE_SPACE"))
    try:
        return subprocess.Popen(cmd, env=env)
    except FileNotFoundError:
        # Some vLLM images expose the entrypoint as the `vllm` module only.
        return subprocess.Popen(["python", "-m", "vllm.entrypoints.openai.api_server", *cmd[1:]], env=env)


def _start_gateway() -> subprocess.Popen:
    env = dict(os.environ)
    env["GATEWAY_PORT"] = str(config.VLLM_PORT)
    env["UPSTREAM_HOST"] = "127.0.0.1"
    env["UPSTREAM_PORT"] = str(config.VLLM_INTERNAL_PORT)
    env["UPSTREAM_TIMEOUT"] = str(config.UPSTREAM_TIMEOUT_S)
    return subprocess.Popen(["python", "/opt/modal/metrics_gateway.py"], env=env)


_vllm_kwargs = {
    "image": vllm_image,
    "volumes": {
        "/root/.cache/huggingface": HF_CACHE_VOL,
        "/root/.cache/vllm": VLLM_CACHE_VOL,
    },
    # HF token for gated models (e.g. meta-llama/Llama-3.2-3B-Instruct).
    "secrets": [modal.Secret.from_name("hf-token-secret")],
    "scaledown_window": 15 * config.MINUTES,
    "timeout": config.STARTUP_TIMEOUT_S,
    # Never autoscale past a single container: concurrent test/probe requests
    # were spinning up multiple (costly, GPU) replicas. Requests are queued
    # instead, so at most one container per app exists at a time.
    "max_containers": 1,
}
if config.HARDWARE == "gpu":
    _vllm_kwargs["gpu"] = config.gpu_spec()
else:
    _vllm_kwargs["cpu"] = config.cpu_spec()
    # vLLM CPU needs headroom for the engine + KV cache.
    _vllm_kwargs["memory"] = config.CPU_MEMORY_MIB


@vllm_app.cls(**_vllm_kwargs)
class VllmServer:
    @modal.enter()
    def start(self):
        self.vllm_proc = _start_vllm()
        self.gateway_proc = _start_gateway()
        _wait_ready(f"http://127.0.0.1:{config.VLLM_INTERNAL_PORT}/health", timeout=config.STARTUP_TIMEOUT_S)
        _wait_ready(f"http://127.0.0.1:{config.VLLM_PORT}/health", timeout=config.STARTUP_TIMEOUT_S)

    @modal.web_server(port=config.VLLM_PORT, startup_timeout=config.STARTUP_TIMEOUT_S)
    def serve(self):
        pass

    @modal.exit()
    def stop(self):
        for proc in (self.vllm_proc, self.gateway_proc):
            try:
                proc.terminate()
            except OSError:
                pass


# --------------------------------------------------------------------------
# Service 2 - API wrapper (reuses phase1 main.py unchanged)
# --------------------------------------------------------------------------
def _resolve_vllm_url() -> str:
    """Return the public vLLM web URL (without the /v1 suffix)."""
    # Deployed: look the class up by app name.
    try:
        cls = modal.Cls.from_name(config.VLLM_APP_NAME, "VllmServer")
        url = cls().serve.get_web_url()
        if url:
            return url.rstrip("/")
    except Exception as exc:  # noqa: BLE001 - tolerate SDK version differences
        print(f"from_name lookup failed ({exc!r}), falling back to same-file class")
    # Ephemeral (modal run): the class defined here is the live one.
    try:
        url = VllmServer().serve.get_web_url()
        if url:
            return url.rstrip("/")
    except Exception as exc:  # noqa: BLE001 - tolerate SDK version differences
        print(f"local web URL lookup failed ({exc!r}), defaulting to localhost")
    return "http://localhost:8000"


_web_kwargs = {
    "image": api_image,
    "scaledown_window": 15 * config.MINUTES,
    "timeout": 10 * config.MINUTES,
    "max_containers": 1,
}
# Pin the hardware in the container (not just the deploy-time env): this makes
# the GPU and CPU API deployments differ (so a redeploy after a hardware switch
# is never a no-op) and makes _resolve_vllm_url pick the right vLLM app.
_web_kwargs["secrets"] = [modal.Secret.from_dict({"MODAL_HARDWARE": config.HARDWARE})]
if _api_key:
    _web_kwargs["secrets"].append(modal.Secret.from_dict({"API_KEY": _api_key}))


@api_app.function(**_web_kwargs)
@modal.asgi_app()
def web():
    # Resolve the vLLM web URL and wait until the backend is actually reachable,
    # re-resolving each attempt. This prevents a cold API container from caching
    # a dead/stale URL (e.g. deployed before the vLLM app was visible) and
    # serving 503s forever.
    vllm_base = _resolve_vllm_url()
    deadline = time.time() + 300.0
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(vllm_base.rstrip("/") + "/health", timeout=5.0) as resp:
                if resp.status == 200:
                    break
        except OSError:
            pass
        print("vLLM not reachable yet, re-resolving URL and retrying")
        vllm_base = _resolve_vllm_url()
        time.sleep(10.0)

    os.environ["VLLM_URL"] = f"{vllm_base}/v1"
    os.environ.setdefault("MODEL_NAME", config.SERVED_MODEL_NAME)
    from app.main import app

    return app


# --------------------------------------------------------------------------
# Smoke test
# --------------------------------------------------------------------------
@vllm_app.local_entrypoint()
def smoke():
    vllm_url = _resolve_vllm_url()
    print("vLLM web URL:", vllm_url)
    health = urllib.request.urlopen(vllm_url.rstrip("/") + "/health", timeout=60.0)
    print("vLLM /health:", health.status)
    try:
        api_url = web.get_web_url()
        print("API web URL:", api_url)
    except Exception as exc:  # noqa: BLE001 - API may not be deployed yet
        print("API URL unavailable (deploy first):", exc)