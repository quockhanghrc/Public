import asyncio
import re
import json
import logging
import os
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Any

from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator

from qdrant_client import models, QdrantClient

from agents import Agent, Runner
from artifacts.intent_classifier import BankingClassifier
from fastembed import SparseTextEmbedding
from sentence_transformers import CrossEncoder

# --- Load environment variables (app/.env first, then parent) ---
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

# --- Configuration ---
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "banking_faq")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
MODEL_DIR = os.getenv("MODEL_DIR", "./")

# Agent model selection (override via .env or env vars)
SUPPORT_AGENT_MODEL = os.getenv("SUPPORT_AGENT_MODEL", "gpt-4o-mini")
JUDGE_AGENT_MODEL = os.getenv("JUDGE_AGENT_MODEL", "gpt-4o")

# Feature flags — toggle each pipeline step on/off (default: all on)
ENABLE_INPUT_GUARDRAIL = os.getenv("ENABLE_INPUT_GUARDRAIL", "true").lower() == "true"
ENABLE_RAG = os.getenv("ENABLE_RAG", "true").lower() == "true"
ENABLE_DOCUMENT_GRADING = os.getenv("ENABLE_DOCUMENT_GRADING", "true").lower() == "true"
ENABLE_AGENT = os.getenv("ENABLE_AGENT", "true").lower() == "true"
ENABLE_LLM_JUDGE = os.getenv("ENABLE_LLM_JUDGE", "true").lower() == "true"

# Cross-encoder / document grading config
RAG_TOP_K = int(os.getenv("RAG_TOP_K", "5"))
CROSS_ENCODER_MODEL = os.getenv("CROSS_ENCODER_MODEL", "cross-encoder/ms-marco-MiniLM-L-6-v2")

# Generation grading (NLI) config
ENABLE_GENERATION_GRADING = os.getenv("ENABLE_GENERATION_GRADING", "true").lower() == "true"
NLI_CONTRADICTION_THRESHOLD = float(os.getenv("NLI_CONTRADICTION_THRESHOLD", "0.5"))
NLI_ENTAILMENT_THRESHOLD = float(os.getenv("NLI_ENTAILMENT_THRESHOLD", "0.3"))
NLI_MODEL = os.getenv("NLI_MODEL", "cross-encoder/nli-deberta-v3-base")

if not QDRANT_URL or not QDRANT_API_KEY:
    raise ValueError("QDRANT_URL and QDRANT_API_KEY must be set in environment or .env file")
if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY must be set in environment or .env file")

os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
if GOOGLE_API_KEY:
    os.environ["GOOGLE_API_KEY"] = GOOGLE_API_KEY

client = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
bot = BankingClassifier(model_dir=MODEL_DIR)

sparse_encoder = SparseTextEmbedding(model_name="prithivida/Splade_PP_en_v1")

cross_encoder = CrossEncoder(
    CROSS_ENCODER_MODEL,
    cache_folder=os.path.join(MODEL_DIR, "cross_encoder"),
)

nli_model = CrossEncoder(
    NLI_MODEL,
    cache_folder=os.path.join(MODEL_DIR, "nli_model"),
)


# NOTE: Import Agent from your specific library
# from openai_agents import Agent

# =============================================================================
# 1. CONFIGURATION
# =============================================================================

COMPETITOR_NAMES = [
    "chase", "wells fargo", "bank of america", "citi", "citibank",
    "capital one", "amex", "american express", "goldman sachs",
    "jpmorgan", "morgan stanley", "td bank", "pnc", "truist",
    "ally", "discover", "us bank",
]

COMPETITOR_PATTERNS = [
    re.compile(rf"\b{re.escape(name)}\b", re.IGNORECASE)
    for name in COMPETITOR_NAMES
]

RISK_KEYWORDS = [r"\bbitcoin\b", r"\bcrypto\b", r"\binvest(?:ment|ing|or)?\b", r"\bhack\b", r"\blaunder\b"]

INPUT_PROHIBITED_PHRASES = [
    "get rich", "get rich quick", "guaranteed returns",
    "guaranteed profit", "buy this stock", "investment advice",
]


def find_competitor(text: str) -> Optional[str]:
    """Returns the matched competitor name if found, else None."""
    for pattern, name in zip(COMPETITOR_PATTERNS, COMPETITOR_NAMES):
        if pattern.search(text):
            return name
    return None


# =============================================================================
# 2. AUDIT LOGGING
# =============================================================================

class GuardrailLayer(str, Enum):
    INPUT = "input_guardrail"
    RAG_RETRIEVAL = "rag_retrieval"
    GENERATION_GRADING = "generation_grading"
    OUTPUT_VALIDATOR = "output_validator"
    LLM_JUDGE = "llm_judge"
    SUCCESS = "success"
    ERROR = "error"


class AuditLogger:
    def __init__(self, log_path: str = "compliance_audit.log"):
        self.log_path = log_path
        self._logger = logging.getLogger("compliance_audit")
        self._logger.setLevel(logging.INFO)
        if not self._logger.handlers:
            handler = logging.FileHandler(log_path)
            handler.setFormatter(logging.Formatter("%(message)s"))
            self._logger.addHandler(handler)

    def log_event(self, *, query: str, layer: GuardrailLayer, blocked: bool,
                  reason: str = "", response: Optional[str] = None,
                  user_id: str = "anonymous", metadata: Optional[dict] = None,
                  latency_ms: Optional[float] = None) -> None:
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user_id,
            "layer": layer.value,
            "blocked": blocked,
            "query": query,
            "response": response,
            "reason": reason,
            "latency_ms": latency_ms,
            "metadata": metadata or {},
        }
        self._logger.info(json.dumps(event, ensure_ascii=False))


audit_logger = AuditLogger(
    log_path=os.getenv("COMPLIANCE_LOG_PATH", "compliance_audit.log")
)


# =============================================================================
# 3. PYDANTIC OUTPUT MODELS
# =============================================================================

class BankingResponse(BaseModel):
    answer: str = Field(description="The response to the customer")
    source: str = Field(default="rag", description="Source of the answer: 'rag' or 'fallback'")
    category: str = Field(default="GENERAL", description="Detected category")
    confidence: float = Field(default=0.0, description="Category classification confidence")

    @model_validator(mode='after')
    def validate_compliance(self) -> 'BankingResponse':
        answer_lower = self.answer.lower()

        prohibited_phrases = [
            "investment advice", "get rich", "guaranteed returns",
            "buy this stock", "guaranteed profit",
        ]
        for phrase in prohibited_phrases:
            if phrase in answer_lower:
                raise ValueError(
                    f"Compliance violation: Output contains prohibited phrase '{phrase}'."
                )

        competitor = find_competitor(self.answer)
        if competitor:
            raise ValueError(
                f"Compliance violation: Output mentions competitor '{competitor}'."
            )

        return self


class ComplianceResult(BaseModel):
    is_compliant: bool = Field(description="True if the response passes compliance, False otherwise")
    reason: str = Field(description="Brief explanation of the compliance decision")


# =============================================================================
# 4. AGENTS
# =============================================================================

support_agent = Agent(
    name="BankingSupportAgent",
    model=SUPPORT_AGENT_MODEL,
    instructions=(
        "You are a helpful banking assistant. Provide factual, policy-compliant answers only.\n\n"
        "CRITICAL RULES:\n"
        "1. You will be given a KNOWLEDGE BASE retrieved from a RAG system.\n"
        "2. Your answer MUST be grounded strictly in the KNOWLEDGE BASE.\n"
        "3. Do NOT fabricate, add, or remove steps. Use the exact terminology provided.\n"
        "4. Format the response in a clear, numbered list format.\n"
        "5. If the KNOWLEDGE BASE is insufficient, say: 'I don't have enough information to answer that.'\n"
        "6. NEVER mention any competitor bank (Chase, Wells Fargo, Citi, etc.) in any language.\n"
        "7. NEVER provide investment advice, stock recommendations, or guaranteed return promises.\n"
        "8. Respond in the same language the user used.\n"
    ),
    output_type=BankingResponse,
)

compliance_judge = Agent(
    name="ComplianceJudge",
    model=JUDGE_AGENT_MODEL,
    instructions=(
        "You are a strict compliance auditor. Evaluate the provided banking response "
        "for compliance in ANY language.\n\n"
        "You must return a JSON object with 'is_compliant' (boolean) and 'reason' (string).\n\n"
        "Set is_compliant to False if the text:\n"
        "1. Contains financial advice, unapproved promises, or speculative investment guidance "
        "   — IN ANY LANGUAGE.\n"
        "2. Mentions any external competitor name — IN ANY LANGUAGE "
        "(e.g., Chase, Wells Fargo, Citi, Goldman Sachs, JPMorgan, Morgan Stanley).\n"
        "3. Contains information NOT grounded in the provided knowledge base (fabrication).\n"
        "4. Contains translated equivalents of prohibited phrases.\n\n"
        "Otherwise, set it to True.\n"
    ),
    output_type=ComplianceResult,
)


# =============================================================================
# 5. RAG RETRIEVAL MODULE
# =============================================================================

def classify_query(user_query: str) -> tuple[str, float]:
    """Classify the query category using your classifier."""
    category, conf, _latency = bot.predict(user_query)
    return category, conf


def retrieve_from_qdrant(
    user_query: str,
    query_vector: list[float],
    category: str,
    top_k: int = 5,
) -> list[dict]:
    """
    Retrieve top-K matching documents from Qdrant.
    Returns a list of payload dicts (empty list if no match).
    """
    filter_condition = models.Filter(
        must=[models.FieldCondition(key="category",
                                     match=models.MatchValue(value=category))]
    )

    # --- Pure vector search (unnamed vector) ---
    search_results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vector,
        query_filter=filter_condition,
        limit=top_k,
    )

    if search_results.points:
        return [p.payload for p in search_results.points]

    # --- Fallback: search entire DB without category filter ---
    fallback_results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vector,
        limit=top_k,
    )

    if fallback_results.points:
        return [p.payload for p in fallback_results.points]

    return []


def rerank_documents(
    query: str,
    documents: list[dict],
    top_k: int = 1,
) -> tuple[Optional[dict], list[dict]]:
    """
    Re-rank documents using cross-encoder.
    Returns (best_doc, all_scores) where all_scores is a list of
    {answer_preview, score} dicts sorted by score descending.
    """
    if not documents:
        return None, []

    pairs = [(query, doc.get("answer", "")) for doc in documents]
    scores = cross_encoder.predict(pairs)

    # Build scored list with previews
    scored = []
    for doc, score in zip(documents, scores):
        answer = doc.get("answer", "")
        scored.append({
            "doc": doc,
            "score": float(score),
            "answer_preview": answer[:80] + "…" if len(answer) > 80 else answer,
        })

    # Sort by score descending
    scored.sort(key=lambda x: x["score"], reverse=True)

    best = scored[0]["doc"] if scored else None
    all_scores = [
        {"answer_preview": s["answer_preview"], "score": round(s["score"], 4)}
        for s in scored
    ]

    return best, all_scores


def check_faithfulness(
    answer: str,
    context: str,
) -> tuple[bool, dict]:
    """
    Check if the generated answer is faithful to the RAG context using NLI.
    Returns (passed, scores_dict) where scores_dict contains:
      - entailment_prob, neutral_prob, contradiction_prob
      - verdict: "faithful", "uncertain", or "hallucination"
    """
    if not context or not answer:
        return True, {
            "entailment_prob": 0.0,
            "neutral_prob": 0.0,
            "contradiction_prob": 0.0,
            "verdict": "faithful",
        }

    # NLI model returns probabilities for [entailment, neutral, contradiction]
    scores = nli_model.predict([(context, answer)])
    entail_prob, neutral_prob, contra_prob = scores[0]

    # Convert numpy floats to Python floats
    entail_prob = float(entail_prob)
    neutral_prob = float(neutral_prob)
    contra_prob = float(contra_prob)

    if contra_prob > NLI_CONTRADICTION_THRESHOLD:
        return False, {
            "entailment_prob": round(entail_prob, 4),
            "neutral_prob": round(neutral_prob, 4),
            "contradiction_prob": round(contra_prob, 4),
            "verdict": "hallucination",
        }

    if entail_prob < NLI_ENTAILMENT_THRESHOLD:
        return True, {
            "entailment_prob": round(entail_prob, 4),
            "neutral_prob": round(neutral_prob, 4),
            "contradiction_prob": round(contra_prob, 4),
            "verdict": "uncertain",
        }

    return True, {
        "entailment_prob": round(entail_prob, 4),
        "neutral_prob": round(neutral_prob, 4),
        "contradiction_prob": round(contra_prob, 4),
        "verdict": "faithful",
    }


def rag_retrieve(user_query: str) -> dict:
    """
    Full RAG retrieval pipeline: classify -> embed -> search -> (optional) re-rank -> fallback.
    Returns a dict with keys: 'answer', 'category', 'confidence', 'source',
    and optionally 'rerank_score', 'candidate_scores', 'candidate_count',
    and 'latency_breakdown_ms'.
    """
    # 1. Classify category
    category, conf = classify_query(user_query)

    # 2. Embed query (dense vector only)
    t_search = time.perf_counter()
    query_vector = bot.encoder.encode(user_query).tolist()

    # 3. Retrieve top-K candidates
    candidates = retrieve_from_qdrant(
        user_query=user_query,
        query_vector=query_vector,
        category=category,
        top_k=RAG_TOP_K,
    )
    t_search_end = time.perf_counter()
    search_latency = round((t_search_end - t_search) * 1000, 2)

    # 4. Optional: re-rank with cross-encoder
    result = None
    rerank_score = None
    candidate_scores = []
    rerank_latency = 0.0

    if candidates:
        if ENABLE_DOCUMENT_GRADING and len(candidates) > 1:
            t_rerank = time.perf_counter()
            result, candidate_scores = rerank_documents(user_query, candidates)
            rerank_score = candidate_scores[0]["score"] if candidate_scores else None
            rerank_latency = round((time.perf_counter() - t_rerank) * 1000, 2)
        else:
            # No grading: use top-1 from Qdrant
            result = candidates[0]

    if result:
        return {
            "answer": result.get("answer", ""),
            "category": category,
            "confidence": conf,
            "source": "rag",
            "rerank_score": rerank_score,
            "candidate_scores": candidate_scores,
            "candidate_count": len(candidates),
            "latency_breakdown_ms": {
                "rag_search": search_latency,
                "rag_rerank": rerank_latency,
            },
        }

    # 5. No match at all
    return {
        "answer": "I'm sorry, I don't have information about that.",
        "category": "GENERAL",
        "confidence": 0.0,
        "source": "fallback",
        "rerank_score": None,
        "candidate_scores": [],
        "candidate_count": 0,
        "latency_breakdown_ms": {
            "rag_search": search_latency,
            "rag_rerank": 0.0,
        },
    }


# =============================================================================
# 6. INPUT GUARDRAIL
# =============================================================================

async def check_input_guardrail(query: str) -> bool:
    query_lower = query.lower()

    # --- Prohibited phrases ---
    for phrase in INPUT_PROHIBITED_PHRASES:
        if phrase in query_lower:
            raise ValueError(f"Input blocked: Prohibited phrase '{phrase}'.")

    # --- Risk keyword check ---
    for pattern in RISK_KEYWORDS:
        if re.search(pattern, query_lower):
            raise ValueError(f"Input blocked: Prohibited banking topic (pattern: {pattern}).")

    # --- Competitor check ---
    competitor = find_competitor(query)
    if competitor:
        raise ValueError(f"Input blocked: Competitor name '{competitor}' is prohibited.")

    return True


# =============================================================================
# 7. INTEGRATED PIPELINE: RAG + Agent + Compliance
# =============================================================================

async def run_agent(agent: Agent, prompt: str):
    """Run an agent, trying multiple API patterns for compatibility."""
    try:
        # Pattern 1: Runner.run() (OpenAI Agents SDK >= 0.0.5)
        result = await Runner.run(agent, prompt)
        return result.final_output
    except (AttributeError, TypeError) as e:
        print(f"[WARN] Runner.run() failed ({e}), trying agent.run()...")
        # Pattern 2: agent.run() (older SDK versions)
        result = await agent.run(prompt)
        return result.final_output


async def handle_banking_query(
    user_query: str,
    user_id: str = "anonymous",
) -> str:
    """
    Full pipeline with per-stage latency tracking and feature flags:
      [Input Guardrail] -> [RAG Retrieval + Doc Grading] -> [Agent] -> [LLM Judge] -> Audit
    """
    t_total = time.perf_counter()
    try:
        # -- Step 1: Input Guardrail (optional) --
        if ENABLE_INPUT_GUARDRAIL:
            t0 = time.perf_counter()
            await check_input_guardrail(user_query)
            t1 = time.perf_counter()
            audit_logger.log_event(
                query=user_query,
                layer=GuardrailLayer.INPUT,
                blocked=False,
                reason="Passed input guardrail",
                user_id=user_id,
                latency_ms=round((t1 - t0) * 1000, 2),
            )
        else:
            t1 = t0 = time.perf_counter()
            logger.info("Input guardrail disabled — skipping")

        # -- Step 2: RAG Retrieval (optional) --
        rag_data = None
        if ENABLE_RAG:
            t2 = time.perf_counter()
            rag_data = rag_retrieve(user_query)
            t3 = time.perf_counter()
            audit_logger.log_event(
                query=user_query,
                layer=GuardrailLayer.RAG_RETRIEVAL,
                blocked=False,
                reason=f"Retrieved from {rag_data['source']}, category={rag_data['category']}",
                user_id=user_id,
                latency_ms=round((t3 - t2) * 1000, 2),
                metadata={
                    "category": rag_data["category"],
                    "confidence": rag_data["confidence"],
                    "source": rag_data["source"],
                    "rerank_score": rag_data.get("rerank_score"),
                    "candidate_count": rag_data.get("candidate_count", 0),
                    "candidate_scores": rag_data.get("candidate_scores", []),
                },
            )
            rag_context = rag_data["answer"]
        else:
            t3 = t2 = time.perf_counter()
            rag_context = ""
            logger.info("RAG disabled — skipping retrieval")

        # -- Step 3: Build agent prompt with RAG context --
        agent_prompt = f"""
        You are a banking support assistant.
        Your ONLY source of information is the "KNOWLEDGE BASE" provided below.

        RULES:
        1. STRICT ADHERENCE: You must use the steps provided in the KNOWLEDGE BASE exactly as they appear.
        2. NO FABRICATION: Do not add steps, do not remove steps, and do not change the terminology.
        3. FORMATTING: Output the response in a clear, numbered list format.
        4. ALIGNMENT: If the KNOWLEDGE BASE is missing a step, do not invent one.
        5. NEVER mention competitor banks.
        6. NEVER provide investment advice or guaranteed returns.

        KNOWLEDGE BASE:
        {rag_context}

        USER QUESTION:
        {user_query}

        RESPONSE:
        """

        # -- Step 4: Agent generates answer (optional) --
        if ENABLE_AGENT:
            t4 = time.perf_counter()
            agent_response = await run_agent(support_agent, agent_prompt)  # BankingResponse instance
            t5 = time.perf_counter()
            audit_logger.log_event(
                query=user_query,
                layer=GuardrailLayer.OUTPUT_VALIDATOR,
                blocked=False,
                reason="Pydantic validator passed",
                response=agent_response.answer,
                user_id=user_id,
                latency_ms=round((t5 - t4) * 1000, 2),
                metadata={
                    "source": agent_response.source,
                    "category": agent_response.category,
                    "confidence": agent_response.confidence,
                },
            )
        else:
            t5 = t4 = time.perf_counter()
            # When agent is off, use RAG answer directly or a fallback
            agent_response = type('obj', (object,), {
                'answer': rag_context or "I'm sorry, I don't have information about that.",
                'source': 'direct',
                'category': 'GENERAL',
                'confidence': 0.0,
            })()
            logger.info("Agent disabled — using RAG answer directly")

        # -- Step 4.5: Generation Grading (NLI faithfulness check, optional) --
        nli_scores = None
        if ENABLE_GENERATION_GRADING and ENABLE_RAG and rag_context:
            t5a = time.perf_counter()
            faithful, nli_scores = check_faithfulness(
                answer=agent_response.answer,
                context=rag_context,
            )
            t5b = time.perf_counter()

            if not faithful:
                audit_logger.log_event(
                    query=user_query,
                    layer=GuardrailLayer.GENERATION_GRADING,
                    blocked=True,
                    reason=f"Hallucination detected: NLI contradiction={nli_scores['contradiction_prob']}",
                    response=agent_response.answer,
                    user_id=user_id,
                    latency_ms=round((t5b - t5a) * 1000, 2),
                    metadata={
                        "nli_verdict": nli_scores["verdict"],
                        "nli_entailment_prob": nli_scores["entailment_prob"],
                        "nli_neutral_prob": nli_scores["neutral_prob"],
                        "nli_contradiction_prob": nli_scores["contradiction_prob"],
                        "context_preview": rag_context[:100],
                    },
                )
                return ("I'm unable to provide a response for this request due to "
                        "a potential factual inconsistency. Please contact our support team for assistance.")
            else:
                audit_logger.log_event(
                    query=user_query,
                    layer=GuardrailLayer.GENERATION_GRADING,
                    blocked=False,
                    reason=f"NLI verdict: {nli_scores['verdict']}",
                    response=agent_response.answer,
                    user_id=user_id,
                    latency_ms=round((t5b - t5a) * 1000, 2),
                    metadata={
                        "nli_verdict": nli_scores["verdict"],
                        "nli_entailment_prob": nli_scores["entailment_prob"],
                        "nli_neutral_prob": nli_scores["neutral_prob"],
                        "nli_contradiction_prob": nli_scores["contradiction_prob"],
                        "context_preview": rag_context[:100],
                    },
                )
        else:
            t5b = t5a = time.perf_counter()
            if not ENABLE_GENERATION_GRADING:
                logger.info("Generation grading disabled — skipping NLI check")

        # -- Step 5: LLM Compliance Judge (optional) --
        if ENABLE_LLM_JUDGE:
            judge_prompt = (
                f"Review this banking response for compliance.\n\n"
                f"User question: {user_query}\n"
                f"Knowledge base used: {rag_context}\n"
                f"Agent response: {agent_response.answer}\n\n"
                f"Check for: competitor mentions, investment advice, fabrication "
                f"(content not grounded in the knowledge base), or prohibited promises.\n"
            )
            t6 = time.perf_counter()
            judge_assessment = await run_agent(compliance_judge, judge_prompt)  # ComplianceResult
            t7 = time.perf_counter()

            if not judge_assessment.is_compliant:
                audit_logger.log_event(
                    query=user_query,
                    layer=GuardrailLayer.LLM_JUDGE,
                    blocked=True,
                    reason=f"Judge blocked: {judge_assessment.reason}",
                    response=agent_response.answer,
                    user_id=user_id,
                    latency_ms=round((t7 - t6) * 1000, 2),
                )
                return ("I'm unable to provide a response for this request due to "
                        "compliance considerations. Please contact our support team for assistance.")
        else:
            t7 = t6 = time.perf_counter()
            logger.info("LLM Judge disabled — skipping compliance check")

        # -- Step 6: Success --
        t_total_end = time.perf_counter()
        # Build latency breakdown, including sub-stages from rag_retrieve
        latency_breakdown = rag_data.get("latency_breakdown_ms", {}) if ENABLE_RAG else {}
        stage_latency_ms = {
            "input_guardrail": round((t1 - t0) * 1000, 2),
            "rag_search": latency_breakdown.get("rag_search", round((t3 - t2) * 1000, 2)),
            "rag_rerank": latency_breakdown.get("rag_rerank", 0.0),
            "rag_total": round((t3 - t2) * 1000, 2),
            "agent_llm": round((t5 - t4) * 1000, 2),
            "generation_grading": round((t5b - t5a) * 1000, 2),
            "llm_judge": round((t7 - t6) * 1000, 2),
            "total": round((t_total_end - t_total) * 1000, 2),
        }
        audit_logger.log_event(
            query=user_query,
            layer=GuardrailLayer.SUCCESS,
            blocked=False,
            reason="All guardrails passed",
            response=agent_response.answer,
            user_id=user_id,
            latency_ms=round((t_total_end - t_total) * 1000, 2),
            metadata={
                "source": agent_response.source,
                "category": agent_response.category,
                "confidence": agent_response.confidence,
                "rag_context": rag_context,
                "stage_latency_ms": stage_latency_ms,
                "nli_verdict": nli_scores["verdict"] if nli_scores else None,
                "nli_entailment_prob": nli_scores["entailment_prob"] if nli_scores else None,
                "nli_contradiction_prob": nli_scores["contradiction_prob"] if nli_scores else None,
            },
        )

        return agent_response.answer

    except ValueError as e:
        error_str = str(e)

        if "Input blocked" in error_str:
            layer = GuardrailLayer.INPUT
        elif "Compliance violation" in error_str:
            layer = GuardrailLayer.OUTPUT_VALIDATOR
        elif "Compliance AI Judge" in error_str:
            layer = GuardrailLayer.LLM_JUDGE
        else:
            layer = GuardrailLayer.ERROR

        t_end = time.perf_counter()
        audit_logger.log_event(
            query=user_query,
            layer=layer,
            blocked=True,
            reason=error_str,
            user_id=user_id,
            latency_ms=round((t_end - t_total) * 1000, 2),
        )
        return f"Request blocked: {error_str}"

    except Exception as e:
        print(f"\n[DEBUG] Unexpected error caught: {type(e).__name__}: {e}")

        t_end = time.perf_counter()
        audit_logger.log_event(
            query=user_query,
            layer=GuardrailLayer.ERROR,
            blocked=True,
            reason=f"Unexpected error: {type(e).__name__}: {e}",
            user_id=user_id,
            latency_ms=round((t_end - t_total) * 1000, 2),
        )
        return "An unexpected error occurred. Please try again or contact support."


# =============================================================================
# 8. MAIN EXECUTION LOOP (TEST CASES)
# =============================================================================

async def main():
    # --- 1. WARM UP MODELS FIRST ---
    print("--- Initializing and warming up models ---")
    try:
        # Trigger the classifier to load weights
        bot.predict("warmup query")
        # Trigger the dense encoder
        bot.encoder.encode("warmup query")
        # Trigger the sparse encoder (only for warmup, not used in retrieval)
        list(sparse_encoder.embed("warmup query"))
        print("Models warmed up successfully.\n")
    except Exception as e:
        print(f"Error warming up models: {e}\n")
        return

    # --- 2. RUN TEST CASES ---
    test_cases = [
        ("Should I invest in bitcoin?", "Input Guardrail Block (Keyword)"),
        ("How do your fees compare to Chase?", "Input Guardrail Block (Competitor)"),
        ("I bank with Wells Fargo, why should I switch?", "Input Guardrail Block (Competitor Context)"),
        ("What are your current interest rates?", "RAG Success Case"),
        ("How do I reset my password?", "RAG Success Case (Procedural)"),
        ("Help me get rich quick.", "Output Guardrail Block (Rule-based)"),
        ("What should I do with my extra cash to gain high returns?", "LLM-Judge Block"),
    ]

    for query, description in test_cases:
        print(f"\n{'='*60}")
        print(f"  {description}")
        print(f"  Query: '{query}'")
        print(f"{'='*60}")

        # ✅ FIX: use_hybrid parameter removed — always uses pure dense vector search
        response = await handle_banking_query(query)
        print(f"\n  -> {response}")


if __name__ == "__main__":
    asyncio.run(main())