"""Unit tests for the API wrapper (phase2/app/main.py), ported from phase1/tests/test_main.py.

Uses FastAPI's TestClient against the app directly — no vLLM or Modal needed.
Run from the phase2 dir:
    python -m pytest tests/test_main.py -q
"""

import sys
from pathlib import Path

from fastapi import HTTPException
from fastapi.testclient import TestClient

# Make `phase2/app/main.py` importable as the `main` module (same trick as test_config.py).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "app"))

from main import API_KEY, MAX_TOKENS_LIMIT, app, build_messages, check_auth  # noqa: E402

client = TestClient(app)


def test_build_messages():
    msgs = build_messages("hello")
    assert msgs[0]["role"] == "system"
    assert msgs[0]["content"]
    assert msgs[1] == {"role": "user", "content": "hello"}


def test_check_auth_valid():
    assert check_auth(f"Bearer {API_KEY}") is None


def test_check_auth_invalid():
    try:
        check_auth("Bearer wrong")
        assert False, "should have raised"
    except HTTPException as e:
        assert e.status_code == 401


def test_health_shape():
    r = client.get("/health")
    body = r.json()
    payload = body.get("detail") if isinstance(body.get("detail"), dict) else body
    assert payload.get("status") in ("ok", "degraded")
    assert "vllm" in payload


def test_unauthorized():
    r = client.post("/ask", params={"prompt": "hi"},
                    headers={"Authorization": "Bearer wrong"})
    assert r.status_code == 401


def test_max_tokens_clamped():
    r = client.post("/ask", params={"prompt": "hi", "max_tokens": MAX_TOKENS_LIMIT + 1},
                    headers={"Authorization": "Bearer secret-key"})
    assert r.status_code == 422


def test_temperature_clamped():
    r = client.post("/ask", params={"prompt": "hi", "temperature": 3.0},
                    headers={"Authorization": "Bearer secret-key"})
    assert r.status_code == 422


def test_metrics_endpoint():
    r = client.get("/metrics")
    assert r.status_code == 200
    assert "http_request" in r.text