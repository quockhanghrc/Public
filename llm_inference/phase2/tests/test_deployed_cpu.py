"""Integration tests for the deployed **CPU** stack (phase2-vllm-cpu + phase2-api).

Requires a live CPU deployment:
    MODAL_HARDWARE=cpu modal deploy phase2/modal/deploy.py::vllm_app
    MODAL_HARDWARE=cpu modal deploy phase2/modal/deploy.py::api_app

Run (from the repo root):
    python -m pytest phase2/tests/test_deployed_cpu.py -m integration

The key difference vs the GPU test: on a CPU run the metrics gateway serves the
vLLM-native + container gauges but **no** GPU gauges (graceful degradation).

URLs/env overrides live in tests/deployed_common.py.
"""

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
        params={"prompt": "What is the capital of France?", "max_tokens": 50, "temperature": 0.2},
    )
    assert j["answer"]
    assert j["usage"]["total_tokens"] > 0


def test_ask_stream():
    text = dc.api_stream_text(
        "/ask-stream", {"prompt": "Count from 1 to 5.", "max_tokens": 16}
    )
    assert text


def test_cpu_container_metrics_present():
    lines = dc.fetch_metrics("cpu")
    for name in (
        "vllm_container_cpu_percent",
        "vllm_container_memory_used_bytes",
        "vllm_container_memory_total_bytes",
    ):
        assert dc.has_metric(lines, name), f"missing {name}"


def test_no_gpu_metrics_on_cpu():
    lines = dc.fetch_metrics("cpu")
    for name in (
        "vllm_container_gpu_utilization_percent",
        "vllm_container_gpu_memory_used_bytes",
        "vllm_container_gpu_temperature_celsius",
        "vllm_container_gpu_power_watts",
    ):
        assert not dc.has_metric(lines, name), f"unexpected {name} on CPU"
