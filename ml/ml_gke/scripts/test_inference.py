import json
import urllib.error
import urllib.request

BASE_URL = "http://localhost:8080"

payload = {
    "annual_income": 65000,
    "current_debt": 12000,
    "fico_score": 750,
    "active_credit_lines": 6,
    "past_due_incidents": 30,
}

def execute_request(label, request):
    try:
        with urllib.request.urlopen(request) as response:
            body = response.read().decode("utf-8")
            print(f"{label}:", response.status, body)
    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        print(f"{label} ERROR:", error.code, error.reason)
        print(f"{label} BODY:", error_body)
    except urllib.error.URLError as error:
        print(f"{label} ERROR:", f"Connection failed: {error.reason}")
    except Exception as error:
        print(f"{label} ERROR:", str(error))

# 1) Health check
execute_request("HEALTH", f"{BASE_URL}/health")

# 2) Prediction request
req = urllib.request.Request(
    f"{BASE_URL}/predict",
    data=json.dumps(payload).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST",
)

execute_request("PREDICT", req)
