"""Metrics gateway for the phase2 vLLM Modal container.

Runs inside the same container as `vllm serve`. It binds the *exposed* port and
reverse proxies the OpenAI-compatible API + health checks to vLLM on the
internal port. For `/metrics` it additionally appends container-level gauges:

  GPU (via nvidia-ml-py / pynvml, only when a GPU is present):
    vllm_container_gpu_utilization_percent
    vllm_container_gpu_memory_used_bytes        vllm_container_gpu_memory_total_bytes
    vllm_container_gpu_memory_utilization_percent
    vllm_container_gpu_temperature_celsius
    vllm_container_gpu_power_watts
    vllm_container_gpu_fan_percent
    vllm_container_gpu_sm_clock_mhz             vllm_container_gpu_memory_clock_mhz

  Container CPU / memory (cgroup v2, psutil fallback):
    vllm_container_cpu_percent
    vllm_container_memory_used_bytes            vllm_container_memory_total_bytes
    vllm_container_memory_utilization_percent

Everything degrades gracefully: if pynvml/psutil is missing or no GPU is
present, only the available metrics are appended.
"""

import http.client
import http.server
import os
import threading
import time
import urllib.parse

try:
    import psutil
except ImportError:  # graceful degradation
    psutil = None

GATEWAY_PORT = int(os.environ.get("GATEWAY_PORT", "8000"))
UPSTREAM_HOST = os.environ.get("UPSTREAM_HOST", "127.0.0.1")
UPSTREAM_PORT = int(os.environ.get("UPSTREAM_PORT", "8001"))
# Upper bound for a single upstream request. CPU inference is slow enough that
# the old fixed 120s cap caused 502s on long generations; the deployer sets a
# larger value for the CPU profile.
UPSTREAM_TIMEOUT = int(os.environ.get("UPSTREAM_TIMEOUT", "120"))
CHUNK = 65536

_metrics_lock = threading.Lock()
_cpu_prev = {"usec": None, "ts": None}


def _gpu_metrics() -> list[str]:
    """Sample nvidia-ml metrics; empty list when unavailable."""
    try:
        import pynvml  # nvidia-ml-py
    except ImportError:
        return []

    lines: list[str] = []
    try:
        pynvml.nvmlInit()
    except pynvml.NVMLError:
        return []
    try:
        count = pynvml.nvmlDeviceGetCount()
    except pynvml.NVMLError:
        count = 0
    try:
        for i in range(count):
            handle = pynvml.nvmlDeviceGetHandleByIndex(i)
            try:
                name = pynvml.nvmlDeviceGetName(handle)
                if isinstance(name, bytes):  # older nvidia-ml-py returns bytes
                    name = name.decode()
            except pynvml.NVMLError:
                name = "unknown"
            label = f'gpu_id="{i}",gpu_name="{name}"'
            try:
                util = pynvml.nvmlDeviceGetUtilizationRates(handle).gpu
            except pynvml.NVMLError:
                util = -1
            try:
                mem = pynvml.nvmlDeviceGetMemoryInfo(handle)
                used, total = mem.used, mem.total
            except pynvml.NVMLError:
                used, total = -1, -1
            try:
                temp = pynvml.nvmlDeviceGetTemperature(handle, pynvml.NVML_TEMPERATURE_GPU)
            except pynvml.NVMLError:
                temp = -1
            try:
                power_mw = pynvml.nvmlDeviceGetPowerUsage(handle)
            except pynvml.NVMLError:
                power_mw = -1
            try:
                fan = pynvml.nvmlDeviceGetFanSpeed(handle)
            except pynvml.NVMLError:
                fan = -1
            try:
                sm_clock = pynvml.nvmlDeviceGetClockInfo(handle, pynvml.NVML_CLOCK_SM)
            except pynvml.NVMLError:
                sm_clock = -1
            try:
                mem_clock = pynvml.nvmlDeviceGetClockInfo(handle, pynvml.NVML_CLOCK_MEM)
            except pynvml.NVMLError:
                mem_clock = -1

            values = {
                "vllm_container_gpu_utilization_percent": util,
                "vllm_container_gpu_memory_used_bytes": used,
                "vllm_container_gpu_memory_total_bytes": total,
                "vllm_container_gpu_memory_utilization_percent": (
                    (used / total * 100.0) if used >= 0 and total > 0 else -1
                ),
                "vllm_container_gpu_temperature_celsius": temp,
                "vllm_container_gpu_power_watts": power_mw / 1000.0 if power_mw >= 0 else -1,
                "vllm_container_gpu_fan_percent": fan,
                "vllm_container_gpu_sm_clock_mhz": sm_clock,
                "vllm_container_gpu_memory_clock_mhz": mem_clock,
            }
            for metric, value in values.items():
                if value >= 0:
                    lines.append(f'{metric}{{{label}}} {value}')
    finally:
        try:
            pynvml.nvmlShutdown()
        except Exception:  # noqa: BLE001, S110 - best-effort teardown
            pass
    return lines


def _cgroup_cpu_percent() -> float:
    """Container CPU % vs allocated quota (cgroup v2), psutil fallback."""
    global _cpu_prev
    try:
        with open("/sys/fs/cgroup/cpu.stat", encoding="utf-8") as f:
            data = dict(line.split() for line in f if line.strip())
        usec = int(data.get("usage_usec", 0))
        now = time.monotonic()
        prev_use, prev_ts = _cpu_prev["usec"], _cpu_prev["ts"]
        _cpu_prev = {"usec": usec, "ts": now}
        if prev_use is None or prev_ts is None:
            return 0.0
        wall = now - prev_ts
        if wall <= 0:
            return 0.0
        pct = (usec - prev_use) / 1e6 / wall * 100.0
        quota = None
        try:
            with open("/sys/fs/cgroup/cpu.max", encoding="utf-8") as f:
                q, p = f.read().split()
            if q != "max" and float(p) > 0:
                quota = float(q) / float(p)
        except (OSError, ValueError):
            pass
        if quota:
            pct = pct / quota
        return max(0.0, min(100.0, pct))
    except (OSError, KeyError):
        try:
            return psutil.cpu_percent(interval=0.1)
        except Exception:  # noqa: BLE001 - psutil can raise a variety of errors
            return -1


def _container_metrics() -> list[str]:
    """Container CPU + memory gauges."""
    lines: list[str] = []
    if psutil is None:
        return lines

    try:
        with open("/sys/fs/cgroup/memory.current", encoding="utf-8") as f:
            used = int(f.read().strip())
        with open("/sys/fs/cgroup/memory.max", encoding="utf-8") as f:
            limit = int(f.read().strip())
        if limit <= 0:
            raise OSError("unlimited memory")
    except (OSError, ValueError):
        vm = psutil.virtual_memory()
        used, limit = vm.used, vm.total

    cpu_pct = _cgroup_cpu_percent()
    lines.append(f"vllm_container_cpu_percent {cpu_pct}")
    lines.append(f"vllm_container_memory_used_bytes {used}")
    lines.append(f"vllm_container_memory_total_bytes {limit}")
    lines.append(
        f"vllm_container_memory_utilization_percent "
        f"{(used / limit * 100.0) if limit > 0 else -1}"
    )
    return lines


def _extra_metrics() -> str:
    """Prometheus text-format block appended to the upstream /metrics output."""
    blocks: list[str] = []

    gpu = _gpu_metrics()
    if gpu:
        blocks.append("# HELP vllm_container_gpu_utilization_percent GPU utilization percent.")
        blocks.append("# TYPE vllm_container_gpu_utilization_percent gauge")
        blocks.append("# HELP vllm_container_gpu_memory_used_bytes GPU memory used in bytes.")
        blocks.append("# TYPE vllm_container_gpu_memory_used_bytes gauge")
        blocks.append("# HELP vllm_container_gpu_memory_total_bytes GPU memory total in bytes.")
        blocks.append("# TYPE vllm_container_gpu_memory_total_bytes gauge")
        blocks.append("# HELP vllm_container_gpu_memory_utilization_percent GPU memory utilization percent.")
        blocks.append("# TYPE vllm_container_gpu_memory_utilization_percent gauge")
        blocks.append("# HELP vllm_container_gpu_temperature_celsius GPU temperature in Celsius.")
        blocks.append("# TYPE vllm_container_gpu_temperature_celsius gauge")
        blocks.append("# HELP vllm_container_gpu_power_watts GPU power draw in watts.")
        blocks.append("# TYPE vllm_container_gpu_power_watts gauge")
        blocks.append("# HELP vllm_container_gpu_fan_percent GPU fan speed percent.")
        blocks.append("# TYPE vllm_container_gpu_fan_percent gauge")
        blocks.append("# HELP vllm_container_gpu_sm_clock_mhz GPU SM clock in MHz.")
        blocks.append("# TYPE vllm_container_gpu_sm_clock_mhz gauge")
        blocks.append("# HELP vllm_container_gpu_memory_clock_mhz GPU memory clock in MHz.")
        blocks.append("# TYPE vllm_container_gpu_memory_clock_mhz gauge")
        blocks.extend(gpu)

    container = _container_metrics()
    if container:
        blocks.append("# HELP vllm_container_cpu_percent Container CPU percent of allocated quota.")
        blocks.append("# TYPE vllm_container_cpu_percent gauge")
        blocks.append("# HELP vllm_container_memory_used_bytes Container memory used in bytes.")
        blocks.append("# TYPE vllm_container_memory_used_bytes gauge")
        blocks.append("# HELP vllm_container_memory_total_bytes Container memory limit in bytes.")
        blocks.append("# TYPE vllm_container_memory_total_bytes gauge")
        blocks.append("# HELP vllm_container_memory_utilization_percent Container memory utilization percent.")
        blocks.append("# TYPE vllm_container_memory_utilization_percent gauge")
        blocks.extend(container)

    return "\n".join(blocks)


def _proxy_request(handler: "http.server.BaseHTTPRequestHandler") -> None:
    """Stream a request to the upstream vLLM and relay the response back."""
    url = urllib.parse.urlsplit(handler.path)
    path = url.path or "/"
    if url.query:
        path = f"{path}?{url.query}"

    conn = http.client.HTTPConnection(UPSTREAM_HOST, UPSTREAM_PORT, timeout=UPSTREAM_TIMEOUT)
    length = int(handler.headers.get("Content-Length", 0) or 0)
    body = handler.rfile.read(length) if length else None

    headers = {
        k: v
        for k, v in handler.headers.items()
        if k.lower() not in ("host", "connection", "content-length", "transfer-encoding")
    }
    headers["Host"] = f"{UPSTREAM_HOST}:{UPSTREAM_PORT}"

    try:
        conn.request(handler.command, path, body=body, headers=headers)
        resp = conn.getresponse()
    except (http.client.HTTPException, OSError):
        handler.send_response(502)
        handler.send_header("Content-Length", "0")
        handler.end_headers()
        return

    # /metrics is fully buffered so we can append the extra gauges.
    if path == "/metrics" or path.startswith("/metrics?"):
        upstream_body = resp.read()
        handler.send_response(resp.status)
        for k, v in resp.getheaders():
            if k.lower() in ("transfer-encoding", "content-length", "connection"):
                continue
            handler.send_header(k, v)
        extra = _extra_metrics()
        if extra:
            combined = (upstream_body.rstrip() + b"\n" + extra.encode() + b"\n")
        else:
            combined = upstream_body
        handler.send_header("Content-Length", str(len(combined)))
        handler.send_header("Content-Type", resp.getheader("Content-Type", "text/plain"))
        handler.end_headers()
        handler.wfile.write(combined)
        handler.wfile.flush()
    else:
        chunked = any(
            h.lower() == "transfer-encoding" and "chunked" in v.lower()
            for h, v in resp.getheaders()
        )
        handler.send_response(resp.status)
        for k, v in resp.getheaders():
            if k.lower() in ("transfer-encoding", "connection"):
                continue
            handler.send_header(k, v)
        if chunked:
            handler.send_header("Transfer-Encoding", "chunked")
        handler.end_headers()
        if chunked:
            while True:
                chunk = resp.read(CHUNK)
                if not chunk:
                    break
                handler.wfile.write(f"{len(chunk):X}\r\n".encode() + chunk + b"\r\n")
                handler.wfile.flush()
            handler.wfile.write(b"0\r\n\r\n")
            handler.wfile.flush()
        else:
            while True:
                chunk = resp.read(CHUNK)
                if not chunk:
                    break
                handler.wfile.write(chunk)
            handler.wfile.flush()
    conn.close()


class GatewayHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        _proxy_request(self)

    def do_POST(self):
        _proxy_request(self)

    def do_PUT(self):
        _proxy_request(self)

    def do_DELETE(self):
        _proxy_request(self)


def main() -> None:
    server = http.server.ThreadingHTTPServer(("0.0.0.0", GATEWAY_PORT), GatewayHandler)
    print(f"metrics gateway listening on :{GATEWAY_PORT} -> {UPSTREAM_HOST}:{UPSTREAM_PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
