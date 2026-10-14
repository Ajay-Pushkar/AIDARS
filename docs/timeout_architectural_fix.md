# AIDAR Timeout Investigation & Architectural Fix Proposal

## A. Current Timeout Flow
1. A payload arrives at the Coordinator containing `estimated_duration_seconds` at the root and `timeout_seconds` inside `execution_spec`.
2. The payload is successfully validated and deserialized into the `WorkloadSpec` object.
3. The Coordinator dispatches the workload over HTTP to the selected worker.
4. On the Worker, the `ExecutionManager` receives the `WorkloadSpec`.
5. In `ExecutionManager._process_workload` (line 123), it calculates a hard execution timeout exclusively using the multiplier heuristic:
   `timeout_seconds = spec.estimated_duration_seconds * 3.0`
6. `ExecutionManager` invokes `GenericSubprocessRuntime.execute_with_context(spec)` wrapped in an `asyncio.wait_for(..., timeout=timeout_seconds)`.
7. `GenericSubprocessRuntime` processes `spec.execution_spec` to configure `asyncio.create_subprocess_exec`, but deliberately **ignores** `spec.execution_spec.timeout_seconds`, leaving termination control entirely to the outer `ExecutionManager`.
8. Once 30 seconds elapse (for an estimated duration of 10), `asyncio.wait_for` raises `asyncio.TimeoutError`, causing `ExecutionManager` to kill the subprocess and emit a `FailureCategory.EXECUTION_TIMEOUT` result.

## B. Tests Depending on Current Timeout Behavior
- `tests/unit/test_m10_observability.py::test_timeout_failure_still_reports_execution_duration`: Heavily relies on the `estimated_duration_seconds * 3.0` multiplier. It uses an estimated duration of `0.01` to force a `0.03s` timeout against a `sleep 5` command to verify telemetry.
- `tests/unit/test_m10_execution_attempts.py::test_placement_recovery_retries_a_different_worker_after_execution_timeout`: Tests the coordinator retry logic, though it mocks the worker HTTP response rather than running the true subprocess logic.

## C. Proposed Minimal Architectural Fix
The safest architectural fix is to keep the timeout enforcement mechanism inside `ExecutionManager` via `asyncio.wait_for`, but change how the timeout value is resolved. 
Pushing timeout enforcement down into `GenericSubprocessRuntime` is unsafe because it would require rewriting the core error boundaries, risking regressions in cancellation and `EXECUTION_TIMEOUT` classification.

**Proposed Logic Update in `ExecutionManager._process_workload`:**
```python
# Check if an authoritative timeout was explicitly provided
if spec.execution_spec and spec.execution_spec.timeout_seconds is not None:
    timeout_seconds = float(spec.execution_spec.timeout_seconds)
else:
    # Backward compatible fallback
    timeout_seconds = spec.estimated_duration_seconds * 3.0
```

This guarantees:
1. `execution_spec.timeout_seconds` acts as the explicit, authoritative upper bound when present.
2. `estimated_duration_seconds` remains purely an optimization/scheduling hint unless a timeout is omitted.
3. The `asyncio.TimeoutError` exception boundary remains exactly as it is today, preserving all checkpointing, cancellation, and error classifications.

## D. Backward-Compatible Default Behavior
If `execution_spec.timeout_seconds` is absent (as is the case in all legacy workloads lacking an `execution_spec`, or early M10 tasks), the system seamlessly falls back to `spec.estimated_duration_seconds * 3.0`. This ensures existing M10, M12, and E2E tests pass without modification.

## E. Exact Files/Functions Requiring Modification
- **File:** `src/aidars/distributed/execution.py`
- **Class:** `ExecutionManager`
- **Method:** `_process_workload` (around line 123)

No changes are required in `runtime.py` or the data `models.py`.

## F. Exact Tests to Add
A new test file, e.g. `tests/unit/test_m20_timeout_semantics.py`, should be added to specifically validate the execution boundaries:
1. **Explicit 90-second timeout test**: Verify that `execution_spec.timeout_seconds = 90` allows a 60-second `sleep` command to succeed, even if `estimated_duration_seconds = 10`.
2. **Estimated duration different from timeout fallback**: Verify that omitting `timeout_seconds` correctly falls back to the `* 3.0` multiplier.
3. **Timeout expiry test**: Verify that if `execution_spec.timeout_seconds = 0.5`, a `sleep 5` command is terminated at 0.5 seconds and returns `EXECUTION_TIMEOUT`.
4. **Cancellation test**: Verify that an explicit cancellation request properly kills a workload governed by a custom `execution_spec.timeout_seconds`.
5. **Omitted timeout_seconds**: Verify that legacy workloads using `command` instead of `execution_spec` still correctly timeout using the `estimated_duration_seconds * 3.0` fallback.
