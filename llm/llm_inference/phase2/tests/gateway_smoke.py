"""Local functional test for metrics_gateway.py proxy + metrics merge.

Spins up a tiny upstream (mimics vLLM) on :8001 and the gateway on :8000,
then checks: /health proxying, /metrics merging, and streaming (chunked).
Run with:  python -m pip install psutil  (optional) then  python gateway_smoke.py
"""
import http.server
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

MODAL_DIR = Path(__file__).resolve().parent.parent / "modal"
sys.path.insert(0, str(MODAL_DIR))
os.environ["GATEWAY_PORT"] = "8000"
os.environ["UPSTREAM_HOST"] = "127.0.0.1"
os.environ["UPSTREAM_PORT"] = "8001"
# Opt-in: exercise the 401/200 bearer-token guard on /metrics (the gateway only
# enforces auth when METRICS_TOKEN is set).
os.environ["METRICS_TOKEN"] = "smoke-test-token"

import metrics_gateway

UPSTREAM_PORT = 8001
GATEWAY_PORT = 8000

METRICS_BODY = (
    "# HELP vllm:gpu_cache_usage_perc GPU KV cache usage.\n"
    "# TYPE vllm:gpu_cache_usage_perc gauge\n"
    'vllm:gpu_cache_usage_perc{model_name="chat-model"} 0.42\n'
)

STREAM_BODY = b'data: {"choices": [{"delta": {"content": "hi"}}]}\n\n'


class Upstream(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _send(self, body: bytes, ctype: str, chunked: bool = False):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        if chunked:
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            self.wfile.write(f"{len(body):X}\r\n".encode() + body + b"\r\n")
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        else:
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()

    def do_GET(self):
        if self.path.startswith("/metrics"):
            self._send(METRICS_BODY.encode(), "text/plain")
        elif self.path.startswith("/health"):
            self._send(b'{"status":"ok"}', "application/json")
        else:
            self._send(STREAM_BODY, "text/event-stream", chunked=True)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        if length:
            self.rfile.read(length)
        self._send(STREAM_BODY, "text/event-stream", chunked=True)


def main():
    upstream = http.server.ThreadingHTTPServer(("127.0.0.1", UPSTREAM_PORT), Upstream)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    threading.Thread(target=metrics_gateway.main, daemon=True).start()
    time.sleep(1.0)

    # 1. /health proxied (no token needed — auth only guards /metrics)
    with urllib.request.urlopen(f"http://127.0.0.1:{GATEWAY_PORT}/health") as r:
        assert r.status == 200, r.status
        assert b"ok" in r.read(), "health body"

    # 2. /metrics without token -> 401
    try:
        urllib.request.urlopen(f"http://127.0.0.1:{GATEWAY_PORT}/metrics")
        raise AssertionError("metrics should require a token")
    except urllib.error.HTTPError as e:
        assert e.code == 401, e.code

    # 3. /metrics with token -> 200 + merged
    with urllib.request.urlopen(
        urllib.request.Request(
            f"http://127.0.0.1:{GATEWAY_PORT}/metrics",
            headers={"Authorization": "Bearer smoke-test-token"},
        )
    ) as r:
        text = r.read().decode()
        assert "vllm:gpu_cache_usage_perc" in text, "vllm metric present"
        assert "vllm_container_cpu_percent" in text, "container metric merged"

    # 4. streaming endpoint stays chunked / SSE (no token needed)
    with urllib.request.urlopen(f"http://127.0.0.1:{GATEWAY_PORT}/v1/chat/completions") as r:
        assert b"hi" in r.read(), "stream content"

    # 5. POST body forwarding (no token needed)
    req = urllib.request.Request(
        f"http://127.0.0.1:{GATEWAY_PORT}/v1/completions",
        data=b'{"prompt":"x"}',
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as r:
        assert b"hi" in r.read(), "POST stream content"

    upstream.shutdown()
    print("gateway smoke test: ALL PASSED")


if __name__ == "__main__":
    main()