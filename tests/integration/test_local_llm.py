import json
import time
import requests


OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "deepseek-r1"

prompt = """
You are the workload planner for AIDAR.

AIDAR has these available workers:

worker-1
worker-2

The user wants to render four independent frames:

frame-1
frame-2
frame-3
frame-4

Each frame can be rendered independently.

Create a workload distribution plan.

Return ONLY valid JSON in exactly this structure:

{
  "workloads": [
    {
      "id": "frame-1",
      "worker": "worker-1"
    }
  ]
}

Every frame must appear exactly once.
Only worker-1 and worker-2 may be used.
Do not include any explanation outside the JSON.
"""


print("=" * 60)
print("AIDAR LOCAL LLM WORKLOAD PLANNING TEST")
print("=" * 60)

start = time.perf_counter()

response = requests.post(
    OLLAMA_URL,
    json={
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
    },
    timeout=120,
)

elapsed = time.perf_counter() - start

response.raise_for_status()

data = response.json()

print("\nMODEL:", data.get("model"))
print("TIME:", round(elapsed, 2), "seconds")
print("EVAL COUNT:", data.get("eval_count"))
print("EVAL DURATION:", data.get("eval_duration"))
print("\nRAW RESPONSE:")
print(data.get("response"))

# ---------------------------------------------------------
# Validate JSON
# ---------------------------------------------------------

raw = data.get("response", "").strip()

try:
    plan = json.loads(raw)
except json.JSONDecodeError as e:
    print("\n❌ JSON VALIDATION FAILED")
    print("Error:", e)
    raise SystemExit(1)

print("\n✅ JSON parsed successfully")

# ---------------------------------------------------------
# Validate workload structure
# ---------------------------------------------------------

workloads = plan.get("workloads")

if not isinstance(workloads, list):
    print("❌ 'workloads' is not a list")
    raise SystemExit(1)

expected_frames = {
    "frame-1",
    "frame-2",
    "frame-3",
    "frame-4",
}

actual_frames = {
    item.get("id")
    for item in workloads
}

allowed_workers = {
    "worker-1",
    "worker-2",
}

actual_workers = {
    item.get("worker")
    for item in workloads
}

if actual_frames != expected_frames:
    print("❌ Frame assignment validation failed")
    print("Expected:", expected_frames)
    print("Received:", actual_frames)
    raise SystemExit(1)

if not actual_workers.issubset(allowed_workers):
    print("❌ Invalid worker detected")
    print("Received:", actual_workers)
    raise SystemExit(1)

if len(workloads) != 4:
    print("❌ Expected exactly 4 workloads")
    raise SystemExit(1)

print("✅ Exactly 4 workloads")
print("✅ Every frame assigned exactly once")
print("✅ Only valid workers used")

print("\n" + "=" * 60)
print("AIDAR LOCAL LLM PLANNING TEST: PASS")
print("=" * 60)