"""
Test client for the Banking RAG Chatbot.
Sends sample queries to the local server and prints responses.
"""

import httpx
import json
import sys

BASE_URL = "http://127.0.0.1:8000"

# ── Test cases ──────────────────────────────────────────────────────────────
TEST_CASES = [
    # (description, payload, expected_behavior)
    ("✅ Normal — password reset", {"query": "How do I reset my password?"}, "success"),
    ("✅ Normal — check balance", {"query": "What is my account balance?"}, "success"),
    ("✅ Normal — transfer money", {"query": "How do I transfer money to another account?"}, "success"),
    ("✅ Normal — interest rates", {"query": "What are your current interest rates?"}, "success"),
    ("✅ Normal — mortgage info", {"query": "How do I apply for a mortgage?"}, "success"),
    ("✅ Normal — credit card", {"query": "What are the benefits of your credit card?"}, "success"),
    ("✅ Normal — foreign language", {"query": "¿Cómo restablezco mi contraseña?"}, "success"),
    ("🚫 Guardrail — competitor mention", {"query": "How do your fees compare to Chase?"}, "blocked"),
    ("🚫 Guardrail — crypto keyword", {"query": "Should I invest in bitcoin?"}, "blocked"),
    ("🚫 Guardrail — prohibited phrase", {"query": "Help me get rich quick"}, "blocked"),
    ("✅ Normal — investment advice (safe refusal)", {"query": "What should I do with my extra cash to gain high returns?"}, "success"),
    ("🚫 Guardrail — competitor (Wells Fargo)", {"query": "I bank with Wells Fargo, why should I switch?"}, "blocked"),
]


def print_separator(char="─", width=72):
    print(char * width)


def run_tests():
    # ── 1. Health check ──
    print_separator("━")
    print("  🔍  HEALTH CHECK")
    print_separator("━")
    try:
        resp = httpx.get(f"{BASE_URL}/health", timeout=10)
        print(f"  Status: {resp.status_code}  →  {resp.json()}")
    except Exception as e:
        print(f"  ❌  Health check failed: {e}")
        print("\n  Is the server running? Try:  python main.py")
        sys.exit(1)

    print()

    # ── 2. Chat tests ──
    print_separator("━")
    print("  💬  CHAT TEST CASES")
    print_separator("━")

    passed = 0
    failed = 0

    for description, payload, expected in TEST_CASES:
        print(f"\n  {description}")
        print(f"     Query: {payload['query']}")
        print_separator("·")

        try:
            resp = httpx.post(
                f"{BASE_URL}/chat",
                json=payload,
                timeout=60,
            )
            data = resp.json()
            status_icon = "✅" if resp.status_code == 200 else "❌"
            print(f"     {status_icon} HTTP {resp.status_code}")

            if resp.status_code == 200:
                blocked = data.get("blocked", False)
                answer = data.get("answer", "")
                reason = data.get("reason", "")

                # Truncate long answers for display
                display_answer = answer[:200] + "…" if len(answer) > 200 else answer

                if blocked:
                    print(f"     🚫 BLOCKED  —  {reason}")
                    print(f"     Response: {display_answer}")
                else:
                    print(f"     ✅ PASSED")
                    print(f"     Response: {display_answer}")

                # Check if behavior matches expectation
                if expected == "blocked" and blocked:
                    print(f"     ✓ Expected block — correct")
                    passed += 1
                elif expected == "success" and not blocked:
                    print(f"     ✓ Expected success — correct")
                    passed += 1
                else:
                    print(f"     ⚠ Unexpected result (expected {expected})")
                    failed += 1
            else:
                print(f"     Detail: {data}")
                failed += 1

        except httpx.TimeoutException:
            print(f"     ⏱  Request timed out")
            failed += 1
        except Exception as e:
            print(f"     ❌  Error: {e}")
            failed += 1

    # ── Summary ──
    print()
    print_separator("━")
    total = passed + failed
    print(f"  📊  RESULTS:  {passed}/{total} passed  ({failed} failed)")
    print_separator("━")

    return failed


if __name__ == "__main__":
    exit_code = run_tests()
    sys.exit(exit_code)