"""Integration tests for the deployed **GPU** stack (phase2-vllm-gpu + phase2-api).

Requires a live deployment:
    MODAL_HARDWARE=gpu modal deploy phase2/modal/deploy.py::vllm_app
    MODAL_HARDWARE=gpu modal deploy phase2/modal/deploy.py::api_app

Run (from the repo root):
    python -m pytest phase2/tests/test_deployed_gpu.py -m integration

URLs/env overrides live in tests/deployed_common.py.
"""

import time

import pytest

import deployed_common as dc

pytestmark = pytest.mark.integration


def test_api_health():
    j = dc.api_json("GET", "/health")
    assert j["status"] == "ok"
    assert j["vllm"] is True


def test_ask_completion():
    j = dc.api_json(
        "POST",
        "/ask",
        params={"prompt": "What is 2+2? Answer in one word.", "max_tokens": 50, "temperature": 0.2},
    )
    assert j["answer"]
    assert j["usage"]["total_tokens"] > 0


def test_ask_stream():
    text = dc.api_stream_text(
        "/ask-stream", {"prompt": "Count from 1 to 5.", "max_tokens": 16}
    )
    assert text


def test_api_metrics_endpoint():
    text = dc.api_metrics_text()
    assert "http_requests_total" in text


def test_gpu_metrics_present():
    lines = dc.fetch_metrics("gpu")
    for name in (
        "vllm_container_gpu_utilization_percent",
        "vllm_container_gpu_memory_used_bytes",
        "vllm_container_gpu_memory_total_bytes",
        "vllm_container_gpu_temperature_celsius",
        "vllm_container_gpu_power_watts",
        "vllm_container_cpu_percent",
        "vllm_container_memory_used_bytes",
    ):
        assert dc.has_metric(lines, name), f"missing {name}"
    assert any("gpu_name=" in line for line in lines)


def test_vllm_native_metrics():
    # vLLM's per-request counters only appear after a request completes, so
    # drive one first to avoid a spurious failure on a freshly-started engine.
    # (vLLM 0.21 dropped `vllm:e2e_request_latency_seconds`; request_success_total
    # is the reliable completion signal.)
    dc.api_json(
        "POST",
        "/ask",
        params={"prompt": "ping", "max_tokens": 2},
    )
    names = (
        "vllm:request_success_total",
        "vllm:kv_cache_usage_perc",
        "vllm:num_requests_running",
    )
    lines: list[str] = []
    for _ in range(5):
        lines = dc.fetch_metrics("gpu")
        if all(dc.has_metric(lines, name) for name in names):
            return
        time.sleep(2)
    missing = [name for name in names if not dc.has_metric(lines, name)]
    raise AssertionError(f"missing metrics: {missing}")
