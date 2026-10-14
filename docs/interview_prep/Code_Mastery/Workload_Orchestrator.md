# AIDAR CODE MASTERY: M8-M10 Execution & Recovery
## File: `src/aidars/distributed/workload.py`

### 1. Purpose of the File
Contains the `WorkloadOrchestrator`, which bridges the placement engine (scheduling decisions) with the actual physical execution of workloads over the network, and manages the entire lifecycle of a workload including failure and recovery.

### 2. Role in Architecture
It acts as the primary Control Plane loop. Once a workload is validated, this orchestrator manages its state transitions from `SUBMITTED` -> `PLACED` -> `RUNNING` -> `COMPLETED`/`FAILED`.

### 3. Important Classes/Functions
- `WorkloadOrchestrator` (Class)
- `_process_workload` (Method - main entrypoint)
- `_dispatch_workloads_locked` (Method - critical placement/dispatch loop)

### 4. Line-by-Line / Block-by-Block Explanation: `_dispatch_workloads_locked`

```python
        attempts_used = len(attempt_records)
        exhausted_worker_ids: Set[str] = set()
        if self.attempt_registry is not None:
            exhausted_worker_ids = {
                attempt.worker_id
                for attempt in attempt_records
                if(
                    attempt.worker_id is not None
                    and attempt.failure_category in (
                        FailureCategory.WORKER_UNAVAILABLE,
                        FailureCategory.ASSET_STAGING_FAILURE,
                        FailureCategory.EXECUTION_TIMEOUT,
                    )
                    and attempt.status != AttemptStatus.LOST
                )
            }
```
**Explanation:**
- **Recovery State**: Before attempting placement, we calculate `exhausted_worker_ids`. If a previous attempt crashed the worker (`WORKER_UNAVAILABLE`) or locked the workspace (`ASSET_STAGING_FAILURE`), we extract that `worker_id` into a Python Set.
- **Why?**: This prevents the placement engine from naively recommending the exact same broken worker again, which would cause an infinite failure loop.

```python
        while True:
            decision = self.placement_engine.evaluate_workload(
                workload,
                eligible_worker_ids=set(self.registry.get_active_worker_ids()) - exhausted_worker_ids
            )
```
**Explanation:**
- **Inline Retry Loop**: The `while True` loop allows the orchestrator to try scheduling, dispatching, and recovering sequentially without yielding control of the workload's state. 
- **Set Subtraction**: We subtract `exhausted_worker_ids` from `active_worker_ids`. 

```python
            if decision is None:
                if attempts_used > 0:
                    self.workload_registry.update_state(
                        workload_id, WorkloadState.FAILED,
                        error_message="Retry budget exhausted or no eligible workers remaining"
                    )
                else:
                    self.workload_registry.update_state(
                        workload_id, WorkloadState.SUBMITTED,
                        error_message="Waiting for an eligible worker; placement will be reconsidered"
                    )
                return
```
**Explanation:**
- **Hard Affinity Blocking**: If no workers are returned (e.g. `decision is None`), and it's the very first attempt (`attempts_used == 0`), the workload is marked `SUBMITTED`. This is critical for **hard affinity**. If a workload is strictly bound to Worker A, but Worker A is offline, it drops into `SUBMITTED` to wait, rather than failing immediately.

```python
            try:
                # Dispatch over HTTP
                response = await self.http_client.post(
                    f"{worker_info.endpoint_url}/api/v1/workloads/execute",
                    json=payload,
                    timeout=60.0
                )
```
**Explanation:**
- **Synchronous Await**: We use `httpx.AsyncClient` to dispatch the JSON payload. `timeout=60.0` prevents the orchestrator from hanging forever if the worker's network drops packets. This suspends the current async task but allows the Coordinator's event loop to process other workloads concurrently.

### 5-17. Core Dimensions
- **Data Flow**: `WorkloadSpec` -> `PlacementDecision` -> HTTP POST payload.
- **State Changes**: Progresses workload state. Creates new `Attempt` records in `AttemptRegistry`.
- **Error Paths**: `httpx.RequestError` triggers inline recovery. Application-level failures (`result.failure_category == FailureCategory.APPLICATION_ERROR`) break the loop and fail the workload permanently because no amount of retrying will fix a bad user script.
- **Concurrency**: Because `_dispatch_workloads_locked` awaits network calls, multiple workloads process concurrently on the event loop.

### 18. Code Interview Questions
- **Q1**: What happens if the `while True:` loop never breaks? How is it bounded? (Answer: Bounded by `self.max_attempts`. When `attempts_used >= self.max_attempts`, it breaks and fails).
- **Q2**: Why is `exhausted_worker_ids` updated *inside* the `while True:` loop's error handler? (Answer: Because if a dispatch fails immediately due to a timeout, we don't want to re-evaluate the entire event-loop state; we just add the worker to the set and try the next worker in the same tick).

---
## TRACE: Workload Recovery
Worker B dies mid-execution
    ↓
**`WorkloadOrchestrator._dispatch_workloads_locked`** (waiting on `httpx.post`)
*Action*: Raises `httpx.ConnectError` or Timeout
    ↓
**Exception Handler**
*Action*: Classifies as `FailureCategory.WORKER_UNAVAILABLE`.
    ↓
**Attempt Recording**
*Action*: Logs failed attempt. Appends Worker B to `exhausted_worker_ids`.
    ↓
**Next `while True` Iteration**
*Action*: Calls `placement_engine.evaluate_workload` with Worker B excluded.
*Action*: Worker C is selected. Dispatches to Worker C.
