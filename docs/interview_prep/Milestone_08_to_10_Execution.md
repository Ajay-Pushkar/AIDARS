# AIDAR INTERVIEW PREPARATION
## Milestones 08 to 10: Job Identity, Security, and Production Distributed Execution

### 1. What was actually introduced or changed
- **M8 (Job Identity & Artifacts)**: Introduced the concepts of a `Job` (a logical collection of workloads), output verification (checking `expected_output_count`), and the `Artifact` lifecycle model for managing generated outputs.
- **M9 (Security & Adversarial Controls)**: Implemented security boundaries, path traversal prevention, isolated generic subprocess runtime execution (ensuring workloads cannot escape their `workdir`), and strict validation of inputs.
- **M10 (Production Distributed Execution)**: Introduced the `WorkloadOrchestrator`, explicit attempt tracking (`AttemptRegistry`), retry budgets (`max_attempts=3`), and deterministic `FailureCategory` classification (retryable vs. non-retryable failures). Established the core HTTP dispatch loop between the Coordinator and Worker nodes.

### 2. Architecture and Implementation Decisions
- **Coordinator-Worker HTTP Communication**: Chose standard asynchronous HTTP (using `httpx` and `fastapi`) for coordinator-worker coordination over heavy message brokers (like Kafka or RabbitMQ) to maintain architectural simplicity and ease of deployment.
- **Explicit Failure Classification**: Failures are strictly categorized (e.g., `WORKER_UNAVAILABLE`, `EXECUTION_TIMEOUT`, `APPLICATION_ERROR`). Only environment/infrastructure failures are retried; application failures (like a bad script) fail immediately to preserve cluster bandwidth.
- **Subprocess Isolation**: Workloads are executed via standard OS subprocesses within uniquely minted temporary `workdir` sandboxes, explicitly protecting the worker daemon from workload crashes.

### 3. Algorithms and Data Structures
- **Retry Dispatch Loop**: An infinite loop (`while True:`) in `_dispatch_workloads_locked` that evaluates placement, dispatches the workload, and handles retryable HTTP/timeout exceptions inline without losing the placement context.
- **Exhausted Worker Sets**: `exhausted_worker_ids` is an O(1) hash set used during retry evaluation to guarantee a workload doesn't infinitely loop on a consistently broken worker.

### 4. Important APIs, Classes, Modules, Interfaces, and Execution Flows
- **Classes**: `WorkloadOrchestrator`, `WorkloadRegistry`, `AttemptRegistry`, `GenericSubprocessRuntime`.
- **Enums**: `FailureCategory`, `WorkloadState` (SUBMITTED, VALIDATING, PLACING, PLACED, RUNNING, COMPLETED, FAILED), `AttemptStatus`.
- **Flow (M10 Dispatch)**: `_process_workload` -> PlacementEngine evaluates -> Worker selected -> HTTP POST to `/api/v1/workloads/execute` -> Subprocess runs -> Result HTTP 200 -> State updated to COMPLETED.

### 5. Tests and Validation Performed
- **M10 Execution Attempts**: `test_m10_execution_attempts.py` extensively covers the retry budget, ensuring application errors aren't retried, timeouts are handled, and failed workers are excluded from subsequent retry placements.
- **M9 Security**: Validated that malicious workloads cannot traverse paths or access sibling `workdir` directories.

### 6. Known Failures, Limitations, and Unfinished Work
- **Orphaned Subprocesses (WinError 32)**: As discovered in later physical validation, if a worker is abruptly killed, its subprocesses might survive as orphans, keeping the `workdir` locked and causing `ASSET_STAGING_FAILURE` on restart/retry.
- **Subprocess Isolation Limit**: Standard subprocesses do not provide strict cgroup/Docker containerization, meaning CPU/RAM bounds are soft and an adversarial workload could theoretically execute an OS fork-bomb.

### 7. Design Alternatives Considered
- **Why not Kafka/RabbitMQ for dispatch?** Centralized message queues add massive operational complexity, state-sync issues, and require a secondary service. Direct HTTP maintains a lightweight, embedded coordinator.
- **Why not Docker for M9 isolation?** Docker was bypassed for the baseline generic runtime to ensure AIDAR could run natively on any host OS (especially Windows desktops) without Docker Desktop prerequisites.

---

### INTERVIEW QUESTIONS

#### A. BASIC UNDERSTANDING
- **Level 2**: What is the difference between a Job and a Workload in AIDAR?
- **Level 2**: What does the `WorkloadOrchestrator` do?

#### B. WHY QUESTIONS
- **Level 3**: Why does AIDAR distinguish between `FailureCategory.WORKER_UNAVAILABLE` and `FailureCategory.APPLICATION_ERROR`?
- **Level 4**: Why was asynchronous HTTP chosen for worker dispatch instead of a message queue like RabbitMQ?

#### C. WHY-NOT QUESTIONS
- **Level 4**: Why not use Docker containers for strict execution isolation?

#### D. HOW QUESTIONS
- **Level 3**: How does the coordinator track how many times a workload has failed?
- **Level 4**: How does the `GenericSubprocessRuntime` prevent a workload from writing over the worker daemon's internal files?

#### E. ALGORITHM / DATA STRUCTURE
- **Level 3**: What mechanism prevents a workload from retrying infinitely on the same broken worker?

#### F. CODE-LEVEL QUESTIONS
- **Level 3**: Where is the retry budget defined? (Answer: `DEFAULT_MAX_ATTEMPTS = 3` in `retry.py`).
- **Level 4**: What happens in `workload.py` when an `httpx.ConnectError` is raised during dispatch?

#### G. FAILURE / RECOVERY
- **Level 3**: What happens if the coordinator requests execution but the worker's HTTP endpoint times out after 60 seconds?
- **Level 4**: What happens if a workload script has a syntax error? Does it retry?
- **Level 5**: What happens if the worker process is killed via `SIGKILL` while a workload is running? How does the coordinator detect it?

#### H. PERFORMANCE
- **Level 4**: How does synchronously waiting for workload completion via HTTP (`await client.post(...)` with 60s timeout) affect the coordinator's event loop?

#### I. DISTRIBUTED SYSTEMS
- **Level 4**: How does the coordinator ensure idempotency if a network partition occurs *after* the worker finishes the job but *before* the coordinator receives the 200 OK response?

#### J. SECURITY
- **Level 4**: What isolation does NOT exist in the M9 `GenericSubprocessRuntime`? (Answer: Network isolation, cgroup resource limits, root privilege dropping).

#### K. TESTING
- **Level 3**: How did you test the retry loop without requiring actual physical worker failures? (Answer: Using `httpx.MockTransport` in `test_m10_execution_attempts.py`).

#### L. TRADE-OFFS
- **Level 4**: What did the direct HTTP dispatch design sacrifice? (Sacrificed native asynchronous pub/sub capabilities and robust offline queuing).

#### M. SYSTEM DESIGN FOLLOW-UPS
- **Question**: How does AIDAR handle worker failure?
- **Follow-up 1**: What if the worker fails immediately before starting the job?
- **Follow-up 2**: What if the worker fails mid-execution?
- **Follow-up 3**: How does the coordinator prevent assigning the retry to the same failed worker?
- **Follow-up 4**: What if the workload had a hard affinity requirement to that specific worker?

#### N. REAL-WORLD QUESTIONS
- **Level 4**: During physical validation, what bug was discovered regarding `ASSET_STAGING_FAILURE` and workspace file locks on Windows? How was it fixed? (Answer: Added `ASSET_STAGING_FAILURE` and `EXECUTION_TIMEOUT` to the `exhausted_worker_ids` exclusion set).

#### O. RESUME DEFENSE
- **Claim you can make**: "Implemented a distributed workload orchestrator with deterministic failure classification and bounded retry logic."
- **Evidence**: `WorkloadOrchestrator`, `AttemptRegistry`, HTTP dispatch, retry exclusion sets.
- **Do NOT claim**: "Built a fully containerized, secure multitenant execution environment." (It uses simple subprocesses).
