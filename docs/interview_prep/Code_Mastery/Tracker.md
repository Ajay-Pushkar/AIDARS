# AIDAR CODE MASTERY: TRACKER
## Final Code Mastery & Ownership Log

### Files I Understand
- `src/aidars/cache/storage.py` (CAS split-hash ingestion, atomic replace).
- `src/aidars/distributed/workload.py` (Orchestrator, HTTP dispatch, inline retries, `exhausted_worker_ids`).
- `tests/unit/test_m10_execution_attempts.py` (Mocking httpx endpoints, asserting retry behavior).
- `src/aidars/distributed/placement.py` (Hard/soft constraints, EMA intelligence adjustment).

### Files I Partially Understand
- `src/aidars/cache/verifier.py` (Understand the SHA-256 chunking, but weak on the automatic SQLite auto-evict interaction during read races).
- `src/aidars/adapters/*` (Understand the generic adapter pattern, but less clear on the exact FFmpeg CLI flags generated).

### Files I Don't Understand
- `src/aidars/distributed/server.py` (Haven't fully traced the FastAPI router mounting for the coordinator).

### Weak Concepts
- **Windows File Locking**: Why specifically `WinError 32` happens when a subprocess hangs, and how Python's garbage collector interacts with lingering file handles.
- **SQLite WAL Checkpointing**: How SQLite handles massive telemetry inserts and when the WAL file actually syncs to the main DB.

### Common Mistakes
- Assuming `same_worker` hard affinity will migrate to a new worker if the anchor worker fails. (Correction: It drops to `SUBMITTED` and blocks, preserving data locality at the expense of availability).
- Assuming `IntelligenceAPI` can override the CPU requirements. (Correction: Intelligence is purely a score adjustment *after* hard constraints).

### Code Paths Successfully Explained
- CAS Atomic Ingestion Flow.
- M10 Workload Retry & Worker Exclusion Loop.
- M11 Affinity Placement Evaluation.

### Code Paths Requiring Revision
- Active Garbage Collection Sweep logic (need to trace how `GarbageCollector` calculates active references).
