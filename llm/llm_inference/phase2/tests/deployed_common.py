"""Shared helpers for the deployed-stack integration tests.

These hit a *live* Modal deployment (GPU or CPU), so they are marked
`integration` and excluded from the default offline test run.

URLs are env-overridable:
  MODAL_WORKSPACE    Modal workspace name (default "quockhang-hrc")
  BASE_URL           API wrapper base URL
  VLLM_METRICS_URL   vLLM /metrics URL (else derived from MODAL_WORKSPACE + hardware)
  API_KEY            Bearer token (default "secret-key", same as phase1)
"""

import os
import time
import urllib.request
import urllib.error

import requests

WORKSPACE = os.environ.get("MODAL_WORKSPACE", "quockhang-hrc")
API_KEY = os.environ.get("API_KEY", "secret-key")
API_BASE = os.environ.get("BASE_URL", f"https://{WORKSPACE}--phase2-api-web.modal.run")

# Bearer token protecting /metrics on both deployed endpoints (set by deploy.py
# when METRICS_TOKEN was present at deploy time). Optional: when unset, the
# fetchers make unauthenticated requests (local/unauth deployments).
METRICS_TOKEN = os.environ.get("METRICS_TOKEN")

COLD_START_BUDGET_S = int(os.environ.get("COLD_START_BUDGET_S", "300"))


def vllm_metrics_url(hardware: str) -> str:
    return os.environ.get(
        "VLLM_METRICS_URL",
        f"https://{WORKSPACE}--phase2-vllm-{hardware}-vllmserver-serve.modal.run/metrics",
    )


def vllm_health_url(hardware: str) -> str:
    return vllm_metrics_url(hardware).rstrip("/").removesuffix("/metrics") + "/health"


def _wake_vllm(hardware: str) -> None:
    """The serverless vLLM may be scaled to zero; wake it before fetching."""
    deadline = time.time() + COLD_START_BUDGET_S
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(vllm_health_url(hardware), timeout=30) as resp:
                if resp.status == 200:
                    return
        except urllib.error.HTTPError:
            return  # reached the proxy but upstream not ready; give metrics a try
        except OSError:
            time.sleep(10)
    raise RuntimeError(f"vLLM at {vllm_health_url(hardware)} did not wake within {COLD_START_BUDGET_S}s")


def api_headers() -> dict:
    return {"Authorization": f"Bearer {API_KEY}"}


def metrics_headers() -> dict:
    return {"Authorization": f"Bearer {METRICS_TOKEN}"} if METRICS_TOKEN else {}


def api_json(method: str, path: str, **kwargs) -> dict:
    r = requests.request(method, f"{API_BASE}{path}", headers=api_headers(), timeout=180, **kwargs)
    r.raise_for_status()
    return r.json()


def api_stream_lines(path: str, params: dict) -> list[str]:
    r = requests.post(
        f"{API_BASE}{path}", headers=api_headers(), params=params, timeout=180, stream=True
    )
    r.raise_for_status()
    return [line for line in r.iter_lines(decode_unicode=True) if line]


def api_stream_text(path: str, params: dict) -> str:
    """Return the full streamed body, reading it incrementally.

    Unlike `iter_lines`, this also captures responses that arrive as a single
    line without a trailing newline (iter_lines yields nothing for those).
    """
    r = requests.post(
        f"{API_BASE}{path}", headers=api_headers(), params=params, timeout=180, stream=True
    )
    r.raise_for_status()
    return "".join(chunk.decode("utf-8", errors="replace") for chunk in r.iter_content())


def api_metrics_text() -> str:
    r = requests.get(f"{API_BASE}/metrics", headers=metrics_headers(), timeout=60)
    r.raise_for_status()
    return r.text


def fetch_metrics(hardware: str) -> list[str]:
    """Return non-comment Prometheus text lines from the vLLM /metrics endpoint.

    Wakes the (possibly scaled-to-zero) vLLM container first, then retries a few
    times so the test tolerates cold starts.
    """
    _wake_vllm(hardware)
    last: Exception | None = None
    for _ in range(3):
        try:
            req = urllib.request.Request(vllm_metrics_url(hardware), headers=metrics_headers())
            with urllib.request.urlopen(req, timeout=120) as resp:
                text = resp.read().decode()
            return [line for line in text.splitlines() if line and not line.startswith("#")]
        except OSError as exc:
            last = exc
            time.sleep(15)
    raise last or RuntimeError("metrics fetch failed")


def has_metric(lines: list[str], name: str) -> bool:
    """True if the metric `name` is present, matching either a plain gauge/counter
    line (`name{...}` / `name value`) or a histogram family (`name_bucket/...`)."""
    suffixes = ("_bucket", "_count", "_sum", "_created")
    for line in lines:
        if line.startswith(name):
            rest = line[len(name):]
            if rest[:1] in ("{", " "):
                return True
            if rest.startswith(suffixes):
                return True
    return False
