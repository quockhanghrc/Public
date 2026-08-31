"""
FastAPI server for the Banking RAG Chatbot with Guardrails.
Runs locally for small-scale testing.
"""

import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# Import the pipeline from rag_guardrail
from rag_guardrail import (
    handle_banking_query,
    bot,
    sparse_encoder,
    cross_encoder,
    nli_model,
    audit_logger,
    GuardrailLayer,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("rag_api")


# ---------------------------------------------------------------------------
# Lifespan — warm up models at startup
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("--- Warming up models ---")
    try:
        bot.predict("warmup query")
        bot.encoder.encode("warmup query")
        list(sparse_encoder.embed("warmup query"))
        cross_encoder.predict([("warmup query", "warmup response")])
        nli_model.predict([("warmup context", "warmup answer")])
        logger.info("Models warmed up successfully.")
    except Exception as e:
        logger.warning("Model warmup failed (will lazy-load on first request): %s", e)
    yield
    logger.info("Shutting down.")


app = FastAPI(
    title="Banking RAG Chatbot",
    description="Multi-layered guardrail pipeline for banking FAQ",
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------
class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=2000, description="User's banking question")
    user_id: str = Field(default="anonymous", max_length=128)


class ChatResponse(BaseModel):
    answer: str
    blocked: bool = False
    reason: str = ""


class HealthResponse(BaseModel):
    status: str = "ok"


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health", response_model=HealthResponse)
async def health():
    """Kubernetes readiness/liveness probe."""
    return HealthResponse()


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    Main chat endpoint.
    Runs the full pipeline: Input Guardrail → RAG → Agent → LLM Judge → Audit.
    """
    try:
        answer = await handle_banking_query(
            user_query=request.query,
            user_id=request.user_id,
        )
    except Exception as e:
        logger.exception("Unhandled error in /chat")
        raise HTTPException(status_code=500, detail="Internal server error")

    # Detect if the response was blocked
    blocked = answer.startswith("Request blocked:") or "compliance" in answer.lower()

    return ChatResponse(
        answer=answer,
        blocked=blocked,
        reason="Blocked by guardrail" if blocked else "",
    )


# ---------------------------------------------------------------------------
# Entry point (for local dev)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    print(f"🚀 Starting Banking RAG Chatbot on http://127.0.0.1:{port}")
    print(f"   Health check: http://127.0.0.1:{port}/health")
    print(f"   Chat API:     POST http://127.0.0.1:{port}/chat")
    uvicorn.run("main:app", host="127.0.0.1", port=port, reload=True)
