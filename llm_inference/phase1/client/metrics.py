import re
import time
import urllib.request

METRICS_URL = "http://localhost:8000/metrics"
NUM_PARAMS = 0.494e9  # Qwen2.5-0.5B-Instruct

def fetch_metrics():
    with urllib.request.urlopen(METRICS_URL) as resp:
        return resp.read().decode()

def get_value(lines, name):
    for line in lines:
        if line.startswith(name + "{"):
            return float(line.split("} ")[1])
    return 0.0

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

if __name__ == "__main__":
    main()