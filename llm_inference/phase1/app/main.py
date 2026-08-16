import os
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import StreamingResponse
from openai import OpenAI

VLLM_URL = os.environ.get("VLLM_URL", "http://localhost:8000/v1")
MODEL_NAME = "chat-model"
API_KEY = os.environ.get("API_KEY", "secret-key")

client = OpenAI(base_url=VLLM_URL, api_key="EMPTY")

app = FastAPI(title="My LLM App")

SYSTEM_PROMPT = (
    "You are a concise technical assistant. "
    "Answer in at most 3 sentences."
)

def build_messages(user_text: str) -> list[dict]:
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_text},
    ]

def check_auth(authorization: Optional[str] = Header(None)):
    if authorization != f"Bearer {API_KEY}":
        raise HTTPException(status_code=401, detail="Invalid API key")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/ask")
def ask(prompt: str, max_tokens: int = 200, temperature: float = 0.7,
        authorization: Optional[str] = Header(None)):
    check_auth(authorization)
    start = time.time()
    resp = client.chat.completions.create(
        model=MODEL_NAME,
        messages=build_messages(prompt),
        max_tokens=max_tokens,
        temperature=temperature,
    )
    return {
        "answer": resp.choices[0].message.content,
        "model": MODEL_NAME,
        "latency_s": round(time.time() - start, 2),
        "usage": {
            "prompt_tokens": resp.usage.prompt_tokens,
            "completion_tokens": resp.usage.completion_tokens,
            "total_tokens": resp.usage.total_tokens,
        },
    }

@app.post("/ask-stream")
def ask_stream(prompt: str, authorization: Optional[str] = Header(None)):
    check_auth(authorization)
    def generate():
        stream = client.chat.completions.create(
            model=MODEL_NAME,
            messages=build_messages(prompt),
            max_tokens=200,
            temperature=0.7,
            stream=True,
        )
        for chunk in stream:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
    return StreamingResponse(generate(), media_type="text/plain")