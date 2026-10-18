"""AIDAR Local LLM Workload Planning Integration Test.

Tests local LLM inference against Ollama (deepseek-r1) to generate
a structured frame-to-worker workload distribution plan.
"""
from __future__ import annotations

import json
import time
from typing import Any, Dict

import pytest
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


def parse_workload_plan_json(raw: str) -> Dict[str, Any]:
    """Parse workload plan JSON from raw LLM response.

    Accepts:
    - Plain JSON: '{ ... }'
    - Markdown code block with language: '```json ... ```'
    - Markdown code block without language: '``` ... ```'

    Does not silently extract arbitrary unrelated text; the block
    or trimmed content must be a valid JSON object.
    """
    if not raw or not isinstance(raw, str):
        raise ValueError("Empty or invalid response string")

    text = raw.strip()

    # Strip deepseek-r1 <think>...</think> reasoning blocks if present
    if "<think>" in text and "</think>" in text:
        text = text.split("</think>", 1)[1].strip()

    # Handle markdown code fences (```json ... ``` or ``` ... ```)
    if text.startswith("```"):
        first_newline = text.find("\n")
        if first_newline != -1:
            remaining = text[first_newline + 1:]
            last_fence = remaining.rfind("```")
            if last_fence != -1:
                text = remaining[:last_fence].strip()
            else:
                text = remaining.strip()
        else:
            text = text.strip("`").strip()
            if text.lower().startswith("json"):
                text = text[4:].strip()
    elif "```" in text:
        # Markdown fence embedded within text
        start_idx = text.find("```")
        end_idx = text.rfind("```")
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            fence_block = text[start_idx:end_idx + 3]
            first_nl = fence_block.find("\n")
            if first_nl != -1:
                text = fence_block[first_nl + 1:-3].strip()

    # Locate outermost JSON object braces if minor whitespace or artifacts exist
    if not (text.startswith("{") and text.endswith("}")):
        start_brace = text.find("{")
        end_brace = text.rfind("}")
        if start_brace != -1 and end_brace != -1 and end_brace > start_brace:
            text = text[start_brace:end_brace + 1].strip()

    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object, got {type(data).__name__}")
    return data


def validate_workload_plan(plan: Dict[str, Any]) -> None:
    """Validate the schema and invariants of the workload distribution plan."""
    workloads = plan.get("workloads")
    assert isinstance(workloads, list), f"'workloads' is not a list (got {type(workloads).__name__})"

    expected_frames = {
        "frame-1",
        "frame-2",
        "frame-3",
        "frame-4",
    }
    actual_frames = {item.get("id") for item in workloads if isinstance(item, dict)}

    allowed_workers = {
        "worker-1",
        "worker-2",
    }
    actual_workers = {item.get("worker") for item in workloads if isinstance(item, dict)}

    assert actual_frames == expected_frames, (
        f"Frame assignment validation failed. Expected: {expected_frames}, Received: {actual_frames}"
    )
    assert actual_workers.issubset(allowed_workers), (
        f"Invalid worker detected. Allowed: {allowed_workers}, Received: {actual_workers}"
    )
    assert len(workloads) == 4, (
        f"Expected exactly 4 workloads, received {len(workloads)}"
    )


# ---------------------------------------------------------------------------
# Unit tests for the JSON parsing helper (focused regression coverage)
# ---------------------------------------------------------------------------


def test_parse_workload_plan_json_plain() -> None:
    raw = '{"workloads": [{"id": "frame-1", "worker": "worker-1"}]}'
    result = parse_workload_plan_json(raw)
    assert isinstance(result, dict)
    assert "workloads" in result


def test_parse_workload_plan_json_markdown_code_fence() -> None:
    raw = """```json
{
  "workloads": [
    {"id": "frame-1", "worker": "worker-1"},
    {"id": "frame-2", "worker": "worker-2"},
    {"id": "frame-3", "worker": "worker-1"},
    {"id": "frame-4", "worker": "worker-2"}
  ]
}
```"""
    result = parse_workload_plan_json(raw)
    assert isinstance(result, dict)
    assert len(result["workloads"]) == 4


def test_parse_workload_plan_json_markdown_no_lang() -> None:
    raw = """```
{
  "workloads": [
    {"id": "frame-1", "worker": "worker-1"}
  ]
}
```"""
    result = parse_workload_plan_json(raw)
    assert isinstance(result, dict)
    assert result["workloads"][0]["id"] == "frame-1"


def test_parse_workload_plan_json_with_think_tags() -> None:
    raw = """<think>
Let's distribute 4 frames evenly across worker-1 and worker-2.
</think>
```json
{
  "workloads": [
    {"id": "frame-1", "worker": "worker-1"},
    {"id": "frame-2", "worker": "worker-2"}
  ]
}
```"""
    result = parse_workload_plan_json(raw)
    assert isinstance(result, dict)
    assert len(result["workloads"]) == 2


def test_parse_workload_plan_json_invalid() -> None:
    with pytest.raises((json.JSONDecodeError, ValueError)):
        parse_workload_plan_json("This is not valid json")


# ---------------------------------------------------------------------------
# Main integration test against live Ollama deepseek-r1
# ---------------------------------------------------------------------------


def test_local_llm_workload_planning() -> None:
    print("\n" + "=" * 60)
    print("AIDAR LOCAL LLM WORKLOAD PLANNING TEST")
    print("=" * 60)

    start = time.perf_counter()

    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": MODEL,
                "prompt": prompt,
                "stream": False,
            },
            timeout=300,
        )
        response.raise_for_status()
    except Exception as exc:
        pytest.fail(f"Failed to query Ollama at {OLLAMA_URL} with model {MODEL}: {exc}")

    elapsed = time.perf_counter() - start
    data = response.json()

    print("\nMODEL:", data.get("model"))
    print("TIME:", round(elapsed, 2), "seconds")
    print("EVAL COUNT:", data.get("eval_count"))
    print("EVAL DURATION:", data.get("eval_duration"))
    print("\nRAW RESPONSE:")
    raw = data.get("response", "").strip()
    print(raw)

    # 1. Parse JSON with Markdown code-fence support
    try:
        plan = parse_workload_plan_json(raw)
    except Exception as e:
        print("\nJSON VALIDATION FAILED")
        print("Error:", e)
        pytest.fail(f"JSON validation failed: {e}")

    print("\nJSON parsed successfully")

    # 2. Validate workload plan structure & invariants
    try:
        validate_workload_plan(plan)
    except AssertionError as e:
        pytest.fail(str(e))

    print("Exactly 4 workloads")
    print("Every frame assigned exactly once")
    print("Only valid workers used")

    print("\n" + "=" * 60)
    print("AIDAR LOCAL LLM PLANNING TEST: PASS")
    print("=" * 60)


if __name__ == "__main__":
    test_local_llm_workload_planning()