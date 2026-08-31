# Banking RAG Chatbot — Multi-Layered Guardrail Pipeline

A production-grade banking FAQ chatbot that goes far beyond simple "retrieve-then-generate" RAG. It layers **five guardrail stages** between the user query and the final response, ensuring safety, compliance, and factual accuracy.

---

## Why not just RAG + LLM?

A naive RAG pipeline (retrieve documents → feed to LLM → return answer) is **not safe enough** for regulated domains like banking. Common failure modes:

| Problem | Example | How naive RAG fails |
|---|---|---|
| **Prohibited topics** | User asks about crypto, investment advice | LLM may answer despite policy |
| **Competitor mentions** | "How do you compare to Chase?" | LLM may discuss competitors |
| **Irrelevant context** | Retrieved doc is about ATMs, question is about mortgages | Wrong answer |
| **Hallucination** | LLM adds steps not in the source document | Misleading instructions |
| **Non-compliant output** | Answer contains "guaranteed returns" | Regulatory violation |

This pipeline addresses **every one** of these with dedicated guardrail layers.

---

## Pipeline Overview

```
User Query
    │
    ▼
┌─────────────────────────────┐
│ 1. Input Guardrail          │  ← Rule-based: prohibited phrases, risk keywords, competitor names
│    (rule-based, ~0.1ms)     │
└─────────────────────────────┘
    │ (blocked → return immediately)
    ▼
┌─────────────────────────────┐
│ 2. Intent Classification    │  ← SentenceTransformer + Logistic Regression
│    (ML classifier)          │     Categorizes query (PASSWORD, TRANSFER, LOAN, etc.)
└─────────────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ 3. RAG Retrieval            │  ← Qdrant vector DB (dense vector search)
│    (vector search)          │     Fetches top-K candidate documents
└─────────────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ 4. Document Grading         │  ← Cross-encoder re-ranker
│    (cross-encoder)          │     Scores each (query, doc) pair, picks the best
└─────────────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ 5. Agent (LLM)              │  ← GPT-4o-mini with Pydantic output validation
│    (OpenAI Agents SDK)      │     Generates answer grounded in the graded document
└─────────────────────────────┘
    │
    ▼
┌─────────────────────────────┐
│ 6. Generation Grading       │  ← NLI cross-encoder (hallucination detection)
│    (NLI model)              │     Checks if answer is entailed by the source context
└─────────────────────────────┘
    │ (hallucination detected → blocked)
    ▼
┌─────────────────────────────┐
│ 7. LLM Compliance Judge     │  ← GPT-4o
│    (LLM-as-judge)           │     Reviews output for policy compliance
└─────────────────────────────┘
    │ (non-compliant → blocked)
    ▼
    Response to User
```

Every stage is **individually toggleable** via feature flags in `.env`.

---

## Guardrail Stages — Why Each Matters

### 1. Input Guardrail (rule-based, zero cost)
Blocks queries containing:
- **Prohibited phrases**: "get rich quick", "guaranteed returns", "investment advice"
- **Risk keywords**: bitcoin, crypto, hack, launder
- **Competitor names**: Chase, Wells Fargo, Citi, etc.

Without this, the LLM would see these inputs and might generate a non-compliant response.

### 2. Intent Classification (ML classifier)
A `SentenceTransformer` + `LogisticRegression` model trained on banking intents (PASSWORD, TRANSFER, LOAN, CARD, ACCOUNT, etc.). This narrows the vector search to the relevant category, improving retrieval precision.
Fine-tuning BERT is more advanced techniques but need huge efforts on system (CPU, GPU,...). Only consider if traditional ML not work well

### 3. RAG Retrieval (Qdrant vector search)
Dense vector search against a Qdrant collection of banking FAQ documents. Uses `paraphrase-multilingual-MiniLM-L12-v2` embeddings. Returns top-K candidates (configurable via `RAG_TOP_K`).

### 4. Document Grading — Cross-Encoder Re-ranker
**Why this is needed**: Bi-encoder vector search (cosine similarity) is fast but can return irrelevant top results. A cross-encoder scores each `(query, document)` pair jointly, producing much more accurate relevance scores.

- Model: `cross-encoder/ms-marco-MiniLM-L-6-v2`
- Cached in `artifacts/cross_encoder/`
- Adds ~150–350ms latency but significantly improves retrieval quality

Without this, the LLM might receive a poorly matched document and generate a wrong answer.

### 5. Agent (LLM with Pydantic validation)
Uses OpenAI Agents SDK (`gpt-4o-mini`) with structured output (`BankingResponse` Pydantic model). The Pydantic validator enforces:
- No prohibited phrases in the output
- No competitor mentions in the output

This is a **rule-based safety net** on the LLM's output.

### 6. Generation Grading — NLI Hallucination Detection
**Why this is needed**: LLMs can hallucinate — adding steps, changing terminology, or fabricating information not present in the source document. A compliance judge can't catch factual inaccuracies; it only checks policy.

Uses an **NLI (Natural Language Inference)** model to check if the answer is factually grounded in the source context:

- Model: `cross-encoder/nli-deberta-v3-base`
- Returns probabilities for `[entailment, neutral, contradiction]`
- If contradiction probability > threshold → hallucination → blocked
- Cached in `artifacts/nli_model/`
- No LLM cost — pure cross-encoder inference

Without this, the user could receive instructions that include steps never mentioned in the official documentation.

### 7. LLM Compliance Judge (LLM-as-judge)
A separate `gpt-4o` agent reviews the final answer for:
- Financial advice or speculative investment guidance
- Competitor mentions (in any language)
- Fabrication (content not grounded in the knowledge base)
- Translated equivalents of prohibited phrases

This catches edge cases that rule-based filters miss (e.g., paraphrased prohibited content).

---

## Dataset

The Qdrant collection (`banking_faq`) contains banking FAQ documents covering:

| Category | Example Questions |
|---|---|
| PASSWORD | How do I reset my password? |
| ACCOUNT | What is my account balance? |
| TRANSFER | How do I transfer money? |
| LOAN | How do I apply for a mortgage? |
| CARD | What are the benefits of your credit card? |
| FIND | Where is the nearest ATM? |
| GENERAL | What are your interest rates? |

Each document includes:
- `answer`: The official response text
- `category`: Intent category label

The intent classifier was trained on a labeled dataset of ~banking queries mapped to these categories.

---

## Models Used

| Model | Purpose | Type | Size |
|---|---|---|---|
| `paraphrase-multilingual-MiniLM-L12-v2` | Query embedding for vector search | SentenceTransformer bi-encoder | ~500MB |
| `intent_classification_lr_L12_model.pkl` | Intent classification | Logistic Regression | ~100KB |
| `cross-encoder/ms-marco-MiniLM-L-6-v2` | Document re-ranking | Cross-encoder | ~400MB |
| `cross-encoder/nli-deberta-v3-base` | Hallucination detection (NLI) | Cross-encoder | ~1.4GB |
| `gpt-4o-mini` | Answer generation | LLM (OpenAI) | API |
| `gpt-4o` | Compliance judging | LLM (OpenAI) | API |
| `prithivida/Splade_PP_en_v1` | Sparse embedding (warmup only) | Sparse encoder | ~2GB |

---

## Feature Flags

Every pipeline stage can be toggled via `.env`:

```env
ENABLE_INPUT_GUARDRAIL=true    # Toggle input guardrail
ENABLE_RAG=true                # Toggle RAG retrieval
ENABLE_DOCUMENT_GRADING=true   # Toggle cross-encoder re-ranker
ENABLE_AGENT=true              # Toggle LLM agent
ENABLE_GENERATION_GRADING=true # Toggle NLI hallucination check
ENABLE_LLM_JUDGE=true          # Toggle compliance judge
```

Set any to `false` to skip that stage (useful for testing or cost reduction).

---

## Audit Logging

Every pipeline stage logs a structured JSON event to `compliance_audit.log`:

```json
{
  "timestamp": "2026-07-03T08:18:33.259Z",
  "layer": "rag_retrieval",
  "blocked": false,
  "latency_ms": 661.06,
  "metadata": {
    "rerank_score": 8.1854,
    "candidate_count": 5,
    "candidate_scores": [...]
  }
}
```

The final success event includes a full latency breakdown:

```json
{
  "layer": "success",
  "stage_latency_ms": {
    "input_guardrail": 0.16,
    "rag_search": 471.69,
    "rag_rerank": 173.55,
    "agent_llm": 7261.51,
    "generation_grading": 312.45,
    "llm_judge": 2212.75,
    "total": 10137.24
  }
}
```

---

## Quick Start

```bash
cd rag/app
python -m pip install -r requirements.txt
python main.py
```

Server starts at `http://127.0.0.1:8000`.

### Test

```bash
python test_client.py
```

### API

```bash
# Health check
curl http://127.0.0.1:8000/health

# Chat
curl -X POST http://127.0.0.1:8000/chat \
  -H "Content-Type: application/json" \
  -d '{"query": "How do I reset my password?"}'
```

---

## Project Structure

```
rag/app/
├── main.py                    # FastAPI server
├── rag_guardrail.py           # Core pipeline logic
├── test_client.py             # Test suite
├── requirements.txt           # Python dependencies
├── .env                       # Configuration (credentials — DO NOT COMMIT)
├── .dockerignore              # Ignores .env in Docker builds
├── Dockerfile                 # Container image
├── deployment.yaml            # Kubernetes manifest (remove secrets before commit)
├── cloudbuild.yaml            # GCP Cloud Build config
├── artifacts/
│   ├── intent_classifier.py   # Intent classification model code
│   ├── intent_classification_lr_L12_model.pkl
│   ├── label_encoder.pkl
│   ├── cross_encoder/         # Cached cross-encoder model
│   └── nli_model/             # Cached NLI model
└── README.md
```
