import os
import time
import urllib.request

# Point at the phase2 vLLM Modal web URL, e.g.
#   METRICS_URL=https://<workspace>--phase2-vllm-gpu-serve.modal.run/metrics \
#       python client/metrics.py
METRICS_URL = os.environ.get("METRICS_URL", "http://localhost:8000/metrics")
NUM_PARAMS = float(os.environ.get("NUM_PARAMS", "0.494e9"))  # Qwen2.5-0.5B-Instruct
# Optional bearer token for /metrics (set when deployed with METRICS_TOKEN).
METRICS_TOKEN = os.environ.get("METRICS_TOKEN")

def fetch_metrics():
    req = urllib.request.Request(METRICS_URL)
    if METRICS_TOKEN:
        req.add_header("Authorization", f"Bearer {METRICS_TOKEN}")
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode()

def get_value(lines, name):
    """Parse a Prometheus gauge line: `name{labels} value` or `name value`."""
    for line in lines:
        if line.startswith((name + "{", name + " ")):
            return float(line.rsplit(" ", 1)[1])
    return 0.0

def has_metric(lines, name):
    return any(l.startswith((name + "{", name + " ")) for l in lines)

def main():
    text = fetch_metrics()
    lines = [l for l in text.splitlines() if l and not l.startswith("#")]

    e2e_sum = get_value(lines, "vllm:e2e_request_latency_seconds_sum")
    e2e_cnt = get_value(lines, "vllm:e2e_request_latency_seconds_count")
    ttft_sum = get_value(lines, "vllm:time_to_first_token_seconds_sum")
    ttft_cnt = get_value(lines, "vllm:time_to_first_token_seconds_count")
    inter_sum = get_value(lines, "vllm:inter_token_latency_seconds_sum")
    inter_cnt = get_value(lines, "vllm:inter_token_latency_seconds_count")
    out_sum = get_value(lines, "vllm:request_time_per_output_token_seconds_sum")
    out_cnt = get_value(lines, "vllm:request_time_per_output_token_seconds_count")
    gen_tokens = get_value(lines, "vllm:generation_tokens_total")
    prompt_tokens = get_value(lines, "vllm:prompt_tokens_total")
    created = get_value(lines, "vllm:e2e_request_latency_seconds_created")
    running = get_value(lines, "vllm:num_requests_running")
    kv_usage = get_value(lines, "vllm:kv_cache_usage_perc")

    print("=== Latency ===")
    print(f"  e2e avg            : {e2e_sum / e2e_cnt if e2e_cnt else 0:.2f} s/request")
    print(f"  TTFT avg           : {ttft_sum / ttft_cnt if ttft_cnt else 0:.2f} s")
    print(f"  inter-token avg    : {inter_sum / inter_cnt if inter_cnt else 0:.3f} s/token")

    print("=== Throughput ===")
    print(f"  decode rate (eng)  : {inter_cnt / inter_sum if inter_sum else 0:.1f} tok/s")
    print(f"  per-request output : {out_cnt / out_sum if out_sum else 0:.1f} tok/s")
    print(f"  prompt tokens      : {prompt_tokens:.0f}")
    print(f"  generation tokens  : {gen_tokens:.0f}")

    total_tokens = prompt_tokens + gen_tokens
    print("=== FLOPS (analytic) ===")
    flops = 2 * NUM_PARAMS * total_tokens
    print(f"  total FLOPs        : {flops / 1e9:.1f} GFLOP")
    if created:
        elapsed = time.time() - created
        print(f"  effective FLOPS    : {flops / elapsed / 1e9:.1f} GFLOP/s")
    print(f"  running requests   : {running:.0f}")
    print("=== KV Cache ===")
    print(f"  usage              : {kv_usage * 100:.1f}%")

    # --- GPU + container metrics (phase2, exposed by the metrics gateway) ---
    gpu_util = get_value(lines, "vllm_container_gpu_utilization_percent")
    gpu_mem_used = get_value(lines, "vllm_container_gpu_memory_used_bytes")
    gpu_mem_total = get_value(lines, "vllm_container_gpu_memory_total_bytes")
    gpu_temp = get_value(lines, "vllm_container_gpu_temperature_celsius")
    gpu_power = get_value(lines, "vllm_container_gpu_power_watts")
    gpu_fan = get_value(lines, "vllm_container_gpu_fan_percent")
    gpu_kv = get_value(lines, "vllm:gpu_cache_usage_perc")
    preemptions = get_value(lines, "vllm:num_preemptions_total")
    cpu_pct = get_value(lines, "vllm_container_cpu_percent")
    mem_used = get_value(lines, "vllm_container_memory_used_bytes")
    mem_total = get_value(lines, "vllm_container_memory_total_bytes")

    if has_metric(lines, "vllm_container_gpu_utilization_percent"):
        print("=== GPU ===")
        print(f"  utilization        : {gpu_util:.0f}%")
        if gpu_mem_total:
            print(f"  memory used        : {gpu_mem_used / 1e9:.2f} / {gpu_mem_total / 1e9:.2f} GiB")
        print(f"  temperature        : {gpu_temp:.0f} C")
        print(f"  power              : {gpu_power:.0f} W")
        if gpu_fan >= 0:
            print(f"  fan                : {gpu_fan:.0f}%")
        print(f"  KV cache (GPU)     : {gpu_kv * 100:.1f}%")
        print(f"  preemptions        : {preemptions:.0f}")
    if has_metric(lines, "vllm_container_cpu_percent"):
        print("=== Container ===")
        print(f"  cpu                : {cpu_pct:.1f}%")
        if mem_total:
            print(f"  memory             : {mem_used / 1e9:.2f} / {mem_total / 1e9:.2f} GiB")

if __name__ == "__main__":
    main()