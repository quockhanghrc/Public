# Phase 2 — LLM Inference on Modal (CPU + GPU)

Serverless version of the phase1 stack. Two separate Modal services, deployed
from a single file, switchable between **GPU** (default, `T4`) and **CPU** via
an env var, plus a GPU-focused Grafana dashboard hosted separately.

```
phase2/
  modal/
    deploy.py            # both Modal apps: vllm_app + api_app (env-switched hardware)
    config.py            # model, ports, hardware profiles, app names (env-overridable)
    metrics_gateway.py   # reverse proxy + GPU / container metrics merge on /metrics
  app/                   # copy of phase1 FastAPI wrapper (unchanged)
  client/                # test_client.py + metrics.py (URLs via env, GPU section added)
  monitoring/            # separately-hosted Prometheus + Grafana (docker compose)
  tests/                 # app + config unit tests, gateway smoke test, deployed integration tests
```

## Services

| Service | Modal app | What it does |
|---------|-----------|--------------|
| vLLM | `phase2-vllm-gpu` or `phase2-vllm-cpu` | `vllm serve` subprocess behind the metrics gateway |
| API wrapper | `phase2-api` | phase1 `main.py` (auth, `/ask`, `/ask-stream`, `/health`, `/usage`, `/metrics`) |

The **metrics gateway** runs inside the vLLM container: it proxies the
OpenAI-compatible API to vLLM on an internal port (streaming-safe) and, on
`/metrics`, appends GPU gauges (util, memory, temp, power, fan, clocks via
`nvidia-ml-py`) plus container CPU/memory (cgroup v2). On CPU runs the GPU
gauges are simply absent.

---

## How to run

### 1. Prerequisites

- Python 3.12 (only needed to run the client/tests locally)
- Modal CLI + auth:
  ```bash
  pip install modal
  modal token new            # logs you into your workspace
  ```
- Docker (for the optional local monitoring stack)
- `pytest` (for the tests): `pip install pytest`
- A Modal secret named **`hf-token-secret`** with an env var `HF_TOKEN`
  (needed for **gated** models such as `meta-llama/Llama-3.2-3B-Instruct`).
  Create it in the Modal dashboard: **Secrets → Create → name `hf-token-secret`**
  → add `HF_TOKEN=<your-token>`. The vLLM container attaches this secret, so no
  code change is needed when you switch models.
  - Token: https://huggingface.co/settings/tokens (a fresh `hf_...` token)
  - For gated repos you must also **accept the license** on the HF model page
    (e.g. https://huggingface.co/meta-llama/Llama-3.2-3B-Instruct) with the
    **same HF account** the token belongs to.

### 2. Settings (all env vars)

Everything is read at deploy/import time — see the [full table](#config-knobs-env-vars-read-at-deployimport-time) below.

```bash
# Hardware profile (read at deploy time)
export MODAL_HARDWARE=gpu        # "gpu" (default) or "cpu"
export MODAL_GPU=T4              # Modal GPU spec: T4 (default), L4, A100:1, ...
export MODAL_CPU=2               # CPU cores for the CPU profile

# Model + client-facing name
export MODEL_NAME=Qwen/Qwen2.5-0.5B-Instruct   # default
export SERVED_MODEL_NAME=chat-model            # default

# Optional API key for the wrapper (default "secret-key")
export API_KEY=your-secret
```

### 3. Deploy

Each app deploys with its own ref. Deploy **vLLM first**, then the API wrapper
(the API resolves the vLLM web URL at container start).

**GPU (default):**

```bash
modal deploy phase2/modal/deploy.py::vllm_app
modal deploy phase2/modal/deploy.py::api_app
```

**CPU** (same code, different image, no GPU):

```bash
MODAL_HARDWARE=cpu modal deploy phase2/modal/deploy.py::vllm_app
MODAL_HARDWARE=cpu modal deploy phase2/modal/deploy.py::api_app
```

**Swap the model without code changes:**

```bash
# Llama 3.2 3B (gated — requires hf-token-secret + accepted license)
MODEL_NAME=meta-llama/Llama-3.2-3B-Instruct modal deploy phase2/modal/deploy.py::vllm_app
MODEL_NAME=meta-llama/Llama-3.2-3B-Instruct                     modal deploy phase2/modal/deploy.py::api_app

# Any other HF model, e.g. a bigger Qwen on a beefier GPU
MODEL_NAME=Qwen/Qwen2.5-7B-Instruct MODAL_GPU=L4 modal deploy phase2/modal/deploy.py::vllm_app
MODEL_NAME=Qwen/Qwen2.5-7B-Instruct                     modal deploy phase2/modal/deploy.py::api_app
```

> **Gemma 4 E2B caveat:** `google/gemma-4-E2B-it` (Apache-2.0, no consent
> needed) will **not** start on the default `T4`. vLLM forces a Triton
> attention backend for its heterogeneous heads, which needs 96 KB of shared
> memory per block — the T4 (compute 7.5) only provides 64 KB, so the engine
> dies with `triton.runtime.errors.OutOfResources`. It needs a newer GPU (the
> vLLM recipe lists 24 GB+, e.g. `MODAL_GPU=L4`).

The URLs printed at the end of each deploy are what you use below:

| Service | GPU URL | CPU URL |
|---------|---------|---------|
| vLLM | `https://<workspace>--phase2-vllm-gpu-vllmserver-serve.modal.run` | `...--phase2-vllm-cpu-vllmserver-serve.modal.run` |
| API | `https://<workspace>--phase2-api-web.modal.run` | same |

Notes:
- Model weights download once into the `phase2-hf-cache` Modal Volume; later
  cold starts are fast (~2 min on GPU).
- The `-gpu` / `-cpu` suffix lets both vLLM deployments exist at once.
- **Cost:** both apps run with `max_containers: 1` (`modal/deploy.py`), so at
  most one container per app exists at a time — concurrent requests are queued
  rather than scaling out replicas. The container auto-scales to zero after
  `scaledown_window` (default 15 min) of idle. Lower it (e.g. `5 * MINUTES`) in
  `modal/config.py` if you want shorter idle billing. Do **not** `modal app
  stop` a deployment to "save money" — a stopped deployment 404s and must be
  redeployed to wake.

### 4. Run the tests

**Offline (no deployment needed)** — app unit tests (ported from phase1),
config unit tests + gateway smoke test:

```bash
python -m pip install -r phase2/app/requirements-dev.txt   # fastapi deps + pytest + ruff
python -m pytest phase2/tests -q          # 16 unit tests (integration auto-excluded)
python phase2/tests/gateway_smoke.py      # proxy + metrics-merge check
```

**Integration — GPU case** (requires the GPU stack deployed):

```bash
python -m pytest phase2/tests/test_deployed_gpu.py -m integration
```

**Integration — CPU case** (requires the CPU stack deployed):

```bash
python -m pytest phase2/tests/test_deployed_cpu.py -m integration
```

The GPU suite asserts the `vllm_container_gpu_*` gauges are present; the CPU
suite asserts the same gauges are **absent** (graceful degradation). Both hit
`/health`, `/ask`, `/ask-stream` and `/metrics` end-to-end.

> If the stack was deployed with `METRICS_TOKEN` set (required for Grafana
> Cloud), run the deployed tests with the **same** token or the `/metrics`
> fetches will 401:
>
> ```bash
> METRICS_TOKEN=<your-token> python -m pytest phase2/tests/test_deployed_gpu.py -m integration
> ```

**Verified test matrix** (Modal workspace `quockhang-hrc`, all apps at
`max_containers: 1`):

| Model | GPU (T4) | CPU |
|-------|----------|-----|
| `Qwen/Qwen2.5-0.5B-Instruct` | 6/6 pass | 5/5 pass |
| `meta-llama/Llama-3.2-3B-Instruct` (gated) | 6/6 pass | 5/5 pass |
| `google/gemma-4-E2B-it` | ❌ fails to start on T4 (Triton shared-mem limit, see above) | untested |

URL overrides (defaults are your real workspace URL):

```bash
MODAL_WORKSPACE=your-workspace \
BASE_URL=https://your-workspace--phase2-api-web.modal.run \
VLLM_METRICS_URL=https://your-workspace--phase2-vllm-gpu-vllmserver-serve.modal.run/metrics \
API_KEY=your-secret \
  python -m pytest phase2/tests/test_deployed_gpu.py -m integration
```

**Manual load test** (from the client dir):

```bash
BASE_URL=https://your-workspace--phase2-api-web.modal.run \
    python phase2/client/test_client.py

# Metrics summary (GPU section appears on GPU runs)
METRICS_URL=https://your-workspace--phase2-vllm-gpu-vllmserver-serve.modal.run/metrics \
    python phase2/client/metrics.py
```

### 5. Monitoring + Grafana dashboard

**Recommended — Grafana Cloud (no local storage).** The Modal `/metrics`
endpoints are scraped directly by the Grafana Cloud **Metrics Endpoint**
integration; no local Prometheus/Grafana needed. Follow
[`monitoring/grafana-cloud/README.md`](monitoring/grafana-cloud/README.md).
Grafana Cloud requires **auth on the scrape target**, so deploy with a
`METRICS_TOKEN` (opt-in bearer token that guards `/metrics` on both services):

```bash
export METRICS_TOKEN=<your-token>
MODEL_NAME=meta-llama/Llama-3.2-3B-Instruct             modal deploy phase2/modal/deploy.py::vllm_app
MODEL_NAME=meta-llama/Llama-3.2-3B-Instruct             modal deploy phase2/modal/deploy.py::api_app
```

Without `METRICS_TOKEN` the endpoints stay open (local dev / unit tests) and
Grafana Cloud will reject the scrape jobs — use the local stack below instead.

**Local alternative (docker compose)** — documented, keeps working as before:

```bash
cd phase2/monitoring

# Point the scraper at your deployed services
export VLLM_METRICS_HOST=your-workspace--phase2-vllm-gpu-vllmserver-serve.modal.run
export API_METRICS_HOST=your-workspace--phase2-api-web.modal.run
export SCRAPE_SCHEME=https          # default https (Modal); use http for local phase1

# Optionally set a Grafana admin password
export GF_ADMIN_PASSWORD=admin

docker compose up -d
```

**Access the dashboard:**

1. Grafana: **http://localhost:3000** — log in `admin` / `$GF_ADMIN_PASSWORD`
   (default `admin`).
2. The `Prometheus` datasource is auto-provisioned (`http://prometheus:9090`).
3. The phase2 dashboard is auto-provisioned from
   `monitoring/grafana/dashboards/vllm.json` (appears under **Dashboards →
   Manage** within ~10 s; no manual import needed).
4. Prometheus itself: **http://localhost:9090** (e.g. query
   `vllm_container_gpu_utilization_percent`).

Verify the scrape is healthy before reading panels:
Prometheus **Status → Targets** should show both `vllm` and `myapp` as **UP**.

Dashboard rows:

- **API**: request latency, tokens, error rates (from the API wrapper)
- **vLLM**: model throughput, KV cache, prefix-cache hit vs prefilled
- **Container (Modal)**: CPU %, memory % (was "Host CPU/Memory %")
- **GPU** (populated only when `MODAL_HARDWARE=gpu`): GPU utilization, GPU
  memory used/total + %, temperature, power, fan, KV cache (GPU), preemptions,
  GPU prefix-cache hit vs prefilled tokens

> When a service scales to zero (idle past `scaledown_window`), Prometheus
> scrapes will fail and dashboard graphs will show gaps. Keep the window long
> (default 15 min) during experiments.

### 6. Teardown

```bash
# Stop the local monitoring stack
cd phase2/monitoring && docker compose down

# Delete the Modal deployments (removes their web endpoints; model stays cached
# in the phase2-hf-cache volume)
modal app list                      # copy the app IDs
modal app stop <app-id>             # or use the Modal dashboard
```

---

## Instructions — recommended workflow

The order below avoids surprises and cost bleed. Each app is pinned to
`max_containers: 1`, so you never pay for more than one container per app.

### Step 1 — Check the repo is error-free (no deployment, no cost)

```bash
cd phase2
python -m pytest tests -q                 # 16 offline unit tests
python tests/gateway_smoke.py             # proxy + metrics-merge check
```

### Step 2 — Pick the model and deploy (vLLM first, then the API wrapper)

```bash
# GPU + CPU, same model. The -gpu/-cpu suffix keeps both vLLM deployments alive.
MODEL_NAME=meta-llama/Llama-3.2-3B-Instruct             modal deploy phase2/modal/deploy.py::vllm_app
MODEL_NAME=meta-llama/Llama-3.2-3B-Instruct             modal deploy phase2/modal/deploy.py::api_app
MODEL_NAME=meta-llama/Llama-3.2-3B-Instruct MODAL_HARDWARE=cpu modal deploy phase2/modal/deploy.py::vllm_app
MODEL_NAME=meta-llama/Llama-3.2-3B-Instruct MODAL_HARDWARE=cpu modal deploy phase2/modal/deploy.py::api_app
```

> The API app is a single shared deployment (`phase2-api`); the last API deploy
> wins and resolves the matching vLLM app (GPU or CPU) at container start. Run
> the GPU test suite right after deploying the GPU API, then deploy the CPU API
> and run the CPU suite — don't deploy both APIs first.

### Step 3 — Wake the server and watch the app log (not the tests)

Cold starts take a while (weight download + vLLM engine init). Watch the log
until you see `Starting vLLM server` before running anything against it:

```bash
# Wake the GPU vLLM container by hitting its health endpoint
curl -s https://<workspace>--phase2-vllm-gpu-vllmserver-serve.modal.run/health

modal app logs phase2-vllm-gpu      # repeat until "Starting vLLM server ..." appears
```

### Step 4 — Run the integration tests

```bash
python -m pytest phase2/tests/test_deployed_gpu.py -m integration   # GPU stack
python -m pytest phase2/tests/test_deployed_cpu.py -m integration   # CPU stack
```

### Step 5 — Stop the apps when done (avoids cost bleed)

Stopped deployments 404 and must be redeployed to wake, but they are the only
guaranteed way to stop billing while you're away:

```bash
modal app list --json                # find the deployed phase2-* app IDs
modal app stop <phase2-vllm-gpu-id>  # stop each deployed app
modal app stop <phase2-vllm-cpu-id>
modal app stop <phase2-api-id>
```

---

## Metrics reference

GPU / container gauges exported by the metrics gateway:

```
vllm_container_gpu_utilization_percent       vllm_container_gpu_memory_used_bytes
vllm_container_gpu_memory_total_bytes        vllm_container_gpu_memory_utilization_percent
vllm_container_gpu_temperature_celsius       vllm_container_gpu_power_watts
vllm_container_gpu_fan_percent               vllm_container_gpu_sm_clock_mhz
vllm_container_gpu_memory_clock_mhz
vllm_container_cpu_percent                   vllm_container_memory_used_bytes
vllm_container_memory_total_bytes            vllm_container_memory_utilization_percent
```

vLLM-native GPU metrics already present on `/metrics` when running on GPU:
`vllm:gpu_cache_usage_perc`, `vllm:num_preemptions_total`,
`vllm:gpu_prefix_cache_acc_num_hit_tokens_total`,
`vllm:gpu_prefix_cache_acc_num_prefilled_tokens_total`.

## Local dev & tests

```bash
python -m pip install -r phase2/app/requirements-dev.txt
python -m pytest phase2/tests -q           # offline unit tests (app + config)
ruff check phase2                          # lint
python phase2/tests/gateway_smoke.py       # proxy + metrics-merge check (psutil optional)
```

## Config knobs (env vars, read at deploy/import time)

| Var | Default | Meaning |
|-----|---------|---------|
| `MODAL_HARDWARE` | `gpu` | `gpu` or `cpu` |
| `MODAL_GPU` | `T4` | Modal GPU spec (e.g. `L4`, `A100:1`) — note Gemma 4 needs `L4`+, see above |
| `MODAL_CPU` | `2` | CPU cores for the CPU profile |
| `MODEL_NAME` | `Qwen/Qwen2.5-0.5B-Instruct` | HF model id (gated models use `hf-token-secret`) |
| `SERVED_MODEL_NAME` | `chat-model` | name clients send in requests |
| `API_KEY` | unset → `secret-key` | Bearer token for `/ask*` |
| `METRICS_TOKEN` | unset → metrics open | Bearer token for `/metrics` (opt-in; required for Grafana Cloud scraping) |
| `VLLM_GPU_VERSION` | `0.21.0` | vLLM version in the GPU image |
| `MAX_MODEL_LEN` | `2048` | max model context length |
| `ENABLE_PREFIX_CACHING` | `true` | pass `--enable-prefix-caching` to vLLM |
| `MODAL_WORKSPACE` | your Modal workspace | used to derive URLs in tests/clients |

The GPU image also pins `transformers==5.14.1` (`modal/deploy.py`). This is
required for Gemma 4 class models: transformers 5.15.0 made their heterogeneous
attention config explicit and vLLM 0.21.0's config convertor fails on it with
`AmbiguousGlobalPerLayerAttributeError`.

## Security

- Modal web endpoints are publicly reachable; the API wrapper keeps the phase1
  Bearer-token auth, but the vLLM endpoint itself is unauthenticated (same as
  phase1). Use a real `API_KEY` and restrict access as needed.
- This is a reference/experiment setup, not a hardened production gateway.