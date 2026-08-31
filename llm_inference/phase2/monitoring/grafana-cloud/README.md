# Grafana Cloud monitoring for phase2 (no local storage)

Managed alternative to the local `docker-compose` Prometheus + Grafana stack.
Grafana Cloud's **Metrics Endpoint** integration scrapes the public Modal
`/metrics` URLs directly — no local storage, no VM, no extra infrastructure.

The `/metrics` endpoints on both deployed services are protected by an opt-in
bearer token (`METRICS_TOKEN`). Grafana Cloud **rejects unauthenticated scrape
targets**, so auth must be enabled at deploy time (see below) before the scrape
jobs will stay `UP`.

## Prerequisites

- A deployed phase2 stack **with `METRICS_TOKEN` set** at deploy time:
  ```bash
  export METRICS_TOKEN=<your-token>
  MODEL_NAME=meta-llama/Llama-3.2-3B-Instruct modal deploy phase2/modal/deploy.py::vllm_app
  MODEL_NAME=meta-llama/Llama-3.2-3B-Instruct                     modal deploy phase2/modal/deploy.py::api_app
  ```
  Both the vLLM gateway and the API wrapper enforce `Authorization: Bearer
  <token>` on `/metrics` when the token is set; all other endpoints are
  untouched. Use the same token in the scrape job below.
- A free Grafana Cloud stack (https://grafana.com/products/cloud/ — no credit
  card). Free tier (verified 2026): **10k active series, 14-day retention,
  3 users, forever free**. These two services are far below 10k series.
  Pro = $6.50/1k series + $19/mo platform fee (13-month retention), if ever
  needed.

## 1. Add the scrape jobs

**Connections → Metrics Endpoint** (or *Connections → Data sources → Prometheus*
→ *Metrics Endpoint*) → **Add integration** → select **Prometheus** → **Metrics
Endpoint**.

For each job, use **Bearer token** auth and enter the token **without** the
`Bearer ` prefix. Scrape interval default `60s`. Use **Test connection** to
verify.

| Job name | URL |
|----------|-----|
| `vllm-gpu` | `https://<workspace>--phase2-vllm-gpu-vllmserver-serve.modal.run/metrics` |
| `vllm-cpu` (optional) | `https://<workspace>--phase2-vllm-cpu-vllmserver-serve.modal.run/metrics` |
| `myapp` | `https://<workspace>--phase2-api-web.modal.run/metrics` |

> For the default workspace this is `quockhang-hrc`, so the URLs are
> `https://quockhang-hrc--phase2-vllm-gpu-vllmserver-serve.modal.run/metrics`
> and `https://quockhang-hrc--phase2-api-web.modal.run/metrics`.

## 2. Import the dashboard

**Dashboards → New → Import** → upload
`monitoring/grafana/dashboards/vllm.json` → map the datasource to the Cloud
Prometheus datasource (Grafana Cloud remaps the `prometheus` datasource uid at
import time).

## 3. Validate

- **Explore**: query `vllm:kv_cache_usage_perc`,
  `vllm_container_gpu_utilization_percent` or
  `http_requests_total` — panels should populate within ~60 s.
- **Metrics Endpoint Overview**: both `vllm-gpu` and `myapp` jobs should show
  **UP**.

## 4. Optional alert

Alert on a dead target (e.g. container scaled to zero / deploy broken):

```
up{job="vllm-gpu"} == 0
```

Route to an email/webhook contact point in **Alerting**.

## Cost / retention notes

- Free tier: 10k active series, 14-day retention, 3 users. Our two services use
  a few thousand series at most.
- Grafana Cloud bills by active series *during the month* — with
  `max_containers: 1` and scale-to-zero, series disappear when apps are stopped,
  which also pauses most billing.
- Pro: $6.50/1k series + $19/mo platform fee, 13-month retention.

## Scale-to-zero caveat

When a service scales to zero (idle past `scaledown_window`, or `modal app
stop`), scrapes fail and dashboard graphs show gaps. Keep `scaledown_window`
long (default 15 min) during experiments, and expect gaps after teardown.

## Local alternative

The old local stack is still supported — see the phase2 `README.md` §5
(`phase2/monitoring/docker-compose.yaml`). It does **not** require a
`METRICS_TOKEN`; just omit it at deploy time and scrape over `https` as before.
