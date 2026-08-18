import concurrent.futures
import os
import sys
import time

import requests

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE = os.environ.get("BASE_URL", "http://localhost:8080")
API_KEY = os.environ.get("API_KEY", "secret-key")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

PROMPTS = [
    "What is Grouped Query Attention?",
    "Explain quantum computing in simple terms.",
    "Write a haiku about machine learning.",
    "What are the differences between REST and gRPC?",
    "Give me 5 tips to improve Python performance.",
    "Count from 1 to 10.",
    "Translate 'hello world' into Japanese and French.",
    "What is the capital of France?",
    "Explain how a transformer model works.",
    "Summarize the plot of a famous novel in 3 sentences.",
    "What causes the seasons to change on Earth?",
    "Describe the water cycle step by step.",
    "How does a neural network learn from data?",
    "List the planets in order from the Sun.",
    "What is the difference between HTTP and HTTPS?",
]

MAX_TOKENS_PATTERN = [50, 100, 150, 300]
TEMP_PATTERN = [0.2, 0.7, 1.2]

PREFIX = (
    "We are going to have a long conversation about the fundamentals of deep "
    "learning and how modern language models are built and trained. First, let "
    "us recall that a transformer processes text as sequences of tokens, and it "
    "uses an attention mechanism to decide which parts of the input matter most. "
    "Every token attends to every other token in the sequence, which is why "
    "transformers can capture long range dependencies that older recurrent "
    "networks struggled with. The model is trained on huge amounts of text by "
    "predicting the next token, a task called language modeling, and during "
    "training the weights are adjusted with backpropagation so that the "
    "predictions keep getting better. After training, the model can be used for "
    "many different tasks such as summarization, translation, and answering "
    "questions. In production, these models are served through an inference "
    "engine that keeps the model weights in memory and processes requests as "
    "fast as possible. A powerful trick used by such engines is prefix caching: "
    "if several requests start with the same text, the intermediate attention "
    "results for that shared part are stored and reused instead of recomputed. "
    "This saves a great deal of compute time, especially when prompts are long. "
    "Now, with all that context in mind, please answer the following question: "
)


def ask(prompt, max_tokens, temperature):
    t0 = time.time()
    r = requests.post(f"{BASE}/ask", headers=HEADERS, params={
        "prompt": prompt,
        "max_tokens": max_tokens,
        "temperature": temperature,
    })
    r.raise_for_status()
    j = r.json()
    return time.time() - t0, j


def print_stats(label, samples):
    latencies = [s[0] for s in samples]
    tokens = [s[1]["usage"]["total_tokens"] for s in samples]
    print(f"--- {label} ---")
    print(f"  requests       : {len(samples)}")
    print(f"  latency avg    : {sum(latencies) / len(latencies):.2f} s")
    print(f"  tokens total   : {sum(tokens)}")
    print(f"  throughput     : {sum(tokens) / sum(latencies):.1f} tok/s")


def prefix_cache_test():
    print("--- Prefix caching test (long shared prefix) ---")
    samples = []

    # Identical prompts -> the FULL sequence KV from req #1 is reused by reqs #2, #3
    for i in range(3):
        dt, j = ask(PREFIX + "Summarize the key points above.", 150, 0.3)
        note = "cold (computes prefix)" if i == 0 else "cached (reuses prefix)"
        samples.append((dt, j))
        print(f"  [same {i + 1}/3] {note:<28} {dt:6.2f}s | {j['usage']['total_tokens']:>3} tok")

    # Shared prefix, distinct questions -> only the shared part is reused
    tails = [
        "What is backpropagation and why is it important?",
        "Explain attention in one sentence.",
        "What does prefix caching save during inference?",
        "Name three tasks a language model can perform.",
        "Why do transformers handle long contexts well?",
    ]
    for i, tail in enumerate(tails):
        dt, j = ask(PREFIX + tail, 150, 0.7)
        note = "prefix cached" if i else "cold"
        samples.append((dt, j))
        print(f"  [tail {i + 1}] {note:<28} {dt:6.2f}s | {j['usage']['total_tokens']:>3} tok")

    print_stats("Prefix caching", samples)


def main():
    # 1. Health
    print(requests.get(f"{BASE}/health").json())

    # 2. Sequential varied traffic (mixed prompt length / max_tokens / temperature)
    samples = []
    for i, prompt in enumerate(PROMPTS[:10]):
        mt = MAX_TOKENS_PATTERN[i % len(MAX_TOKENS_PATTERN)]
        temp = TEMP_PATTERN[i % len(TEMP_PATTERN)]
        dt, j = ask(prompt, mt, temp)
        samples.append((dt, j))
        print(f"[seq {i + 1:>2}] {mt:>3} tok, T={temp:.1f}, {dt:6.2f}s | {j['answer'][:60]}...")
    print_stats("Sequential", samples)

    # 3. Prefix caching (long shared prefix -> KV cache reuse)
    prefix_cache_test()

    # 4. Concurrency burst (mixed prompts in parallel -> spikes running/KV)
    burst = []
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(ask, p, 100, 0.7) for p in PROMPTS[5:13]]
        for f in concurrent.futures.as_completed(futures):
            burst.append(f.result())
    print_stats(f"Concurrent burst ({len(burst)} requests, {time.time() - t0:.1f}s wall)", burst)

    # 5. Streaming
    print("--- Streaming ---")
    r = requests.post(f"{BASE}/ask-stream", headers=HEADERS, params={
        "prompt": "Count from 1 to 10.",
    }, stream=True)
    for line in r.iter_lines(decode_unicode=True):
        if line:
            print(line, end="", flush=True)
    print()


if __name__ == "__main__":
    main()