import logging
import os
import time
import uuid

import httpx
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from openai import APIConnectionError, APIError, APITimeoutError, OpenAI
from prometheus_fastapi_instrumentator import Instrumentator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("myapp")

VLLM_URL = os.environ.get("VLLM_URL", "http://localhost:8000/v1")
MODEL_NAME = os.environ.get("MODEL_NAME", "chat-model")
API_KEY = os.environ.get("API_KEY", "secret-key")
# Opt-in bearer token for /metrics (Grafana Cloud Metrics Endpoint scraping
# requires auth on the target URL). When set, /metrics returns 401 without a
# valid `Authorization: Bearer <token>`; when unset, /metrics stays open
# (local dev / unit tests unchanged).
METRICS_TOKEN = os.environ.get("METRICS_TOKEN")
MAX_TOKENS_LIMIT = int(os.environ.get("MAX_TOKENS_LIMIT", "2048"))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "300.0"))

client = OpenAI(
    base_url=VLLM_URL,
    api_key="EMPTY",
    timeout=REQUEST_TIMEOUT,
    max_retries=0,
)

app = FastAPI(title="My LLM App")

USAGE_LOG = []

SYSTEM_PROMPT = (
    "You are a concise technical assistant. "
    "Answer in at most 3 sentences."
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
    request.state.request_id = request_id
    start = time.time()
    response = await call_next(request)
    response.headers["X-Request-ID"] = request_id
    logger.info(
        "request_id=%s method=%s path=%s status=%s latency_ms=%.1f",
        request_id, request.method, request.url.path, response.status_code,
        (time.time() - start) * 1000,
    )
    return response


@app.middleware("http")
async def metrics_auth(request: Request, call_next):
    if METRICS_TOKEN and request.url.path == "/metrics":
        if request.headers.get("Authorization") != f"Bearer {METRICS_TOKEN}":
            return JSONResponse(status_code=401, content={"detail": "Invalid metrics token"})
    return await call_next(request)


def build_messages(user_text: str) -> list[dict]:
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_text},
    ]


def check_auth(authorization: str | None = Header(None)):
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Invalid API key")


@app.get("/health")
def health():
    try:
        resp = httpx.get(VLLM_URL.removesuffix("/v1") + "/health", timeout=5.0)
        vllm_ok = resp.status_code == 200
    except httpx.HTTPError:
        vllm_ok = False
    body = {"status": "ok" if vllm_ok else "degraded", "vllm": vllm_ok}
    if not vllm_ok:
        raise HTTPException(status_code=503, detail=body)
    return body


@app.get("/usage")
def usage():
    prompt = sum(r["prompt_tokens"] for r in USAGE_LOG)
    completion = sum(r["completion_tokens"] for r in USAGE_LOG)
    return {
        "requests": len(USAGE_LOG),
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": prompt + completion,
        "recent": USAGE_LOG[-10:],
    }


@app.post("/ask")
def ask(prompt: str, request: Request,
        max_tokens: int = Query(200, ge=1, le=MAX_TOKENS_LIMIT),
        temperature: float = Query(0.7, ge=0.0, le=2.0),
               authorization: str | None = Header(None)):
    check_auth(authorization)
    start = time.time()
    try:
        resp = client.chat.completions.create(
            model=MODEL_NAME,
            messages=build_messages(prompt),
            max_tokens=max_tokens,
            temperature=temperature,
        )
    except APITimeoutError:
        logger.error("request_id=%s vllm timeout", request.state.request_id)
        raise HTTPException(status_code=503, detail="vLLM backend timeout")
    except APIConnectionError:
        logger.error("request_id=%s vllm unreachable", request.state.request_id)
        raise HTTPException(status_code=503, detail="vLLM backend unreachable")
    except APIError as e:
        logger.error("request_id=%s vllm api error: %s", request.state.request_id, e)
        raise HTTPException(status_code=502, detail=f"vLLM API error: {e}")

    elapsed = time.time() - start
    usage = resp.usage
    record = {
        "ts": time.time(),
        "model": MODEL_NAME,
        "prompt_tokens": usage.prompt_tokens,
        "completion_tokens": usage.completion_tokens,
        "total_tokens": usage.total_tokens,
        "latency_s": round(elapsed, 2),
    }
    USAGE_LOG.append(record)
    logger.info(
        "request_id=%s model=%s prompt_tokens=%s completion_tokens=%s latency_s=%.2f",
        request.state.request_id, MODEL_NAME, usage.prompt_tokens,
        usage.completion_tokens, elapsed,
    )
    return {
        "answer": resp.choices[0].message.content,
        "model": MODEL_NAME,
        "latency_s": record["latency_s"],
        "usage": {
            "prompt_tokens": usage.prompt_tokens,
            "completion_tokens": usage.completion_tokens,
            "total_tokens": usage.total_tokens,
        },
    }


@app.post("/ask-stream")
def ask_stream(prompt: str, request: Request,
        max_tokens: int = Query(200, ge=1, le=MAX_TOKENS_LIMIT),
        authorization: str | None = Header(None)):
    check_auth(authorization)
    def generate():
        try:
            stream = client.chat.completions.create(
                model=MODEL_NAME,
                messages=build_messages(prompt),
                max_tokens=max_tokens,
                temperature=0.7,
                stream=True,
            )
            for chunk in stream:
                if chunk.choices[0].delta.content:
                    yield chunk.choices[0].delta.content
        except (APITimeoutError, APIConnectionError) as e:
            logger.error("request_id=%s stream error: %s", request.state.request_id, e)
            yield "\n[vLLM backend unavailable]"
    return StreamingResponse(generate(), media_type="text/plain")


Instrumentator().instrument(app).expose(app, endpoint="/metrics")