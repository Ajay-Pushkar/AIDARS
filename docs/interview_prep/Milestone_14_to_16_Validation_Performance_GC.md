# AIDAR INTERVIEW PREPARATION
## Milestones 14 to 16: Validation, Performance, and GC / Checkpoints

### 1. What was actually introduced or changed
- **M14 (Distributed Execution Validation)**: Formalized the E2E multi-worker execution pipeline in a physical environment. Validated cross-worker affinity, output aggregation, artifact staging, and recovery under physical network conditions.
- **M15 (Performance Optimization)**: Scaled the coordinator. Major change: offloaded CAS ingestion and physical disk I/O operations away from the primary asyncio event loop into thread pools (`run_in_executor`) to prevent blocking the Coordinator's HTTP dispatcher. Validated throughput and scale boundaries.
- **M16 (Garbage Collection & Checkpoints)**: Introduced Active Artifact Garbage Collection to clean up temporary staging files, abandoned workdirs, and unreferenced output artifacts. Introduced the Checkpoint/Resume protocol allowing long-running workloads to persist intermediate state and resume on a different worker if preempted or failed.

### 2. Architecture and Implementation Decisions
- **Async I/O Offloading (M15)**: The coordinator's core is an asyncio event loop. Since Python's `asyncio` is single-threaded, synchronous disk I/O (hashing large files, writing to the SQLite WAL) blocks the entire event loop. M15 specifically decoupled disk/compute tasks using `ThreadPoolExecutor`, restoring concurrency.
- **Checkpoint Protocol (M16)**: Workloads are responsible for writing state to a designated checkpoint directory. If a workload fails but `was_checkpointed` is true, the orchestrator preserves those assets, adds them to the retry payload, and stages them on the new worker, allowing the workload to pick up where it left off.
- **Deterministic GC (M16)**: Garbage collection uses reference counting and state evaluation (e.g., deleting `workdir` only when `AttemptStatus` is strictly terminal).

### 3. Algorithms and Data Structures
- **Reference Counting**: Artifacts and cache entries track active consumers. GC sweeps identify and reap zero-reference entries.
- **Thread Pools**: `concurrent.futures.ThreadPoolExecutor` manages background blocking tasks (CAS ingestion).

### 4. Important APIs, Classes, Modules, Interfaces, and Execution Flows
- **Classes**: `GarbageCollector`, `CheckpointManager`.
- **Flow (M15 Offload)**: Request -> `asyncio.get_running_loop().run_in_executor(None, synchronous_hash_function, file_bytes)` -> await result -> HTTP 200.
- **Flow (M16 Checkpoint)**: Workload runs -> writes to `./checkpoint/` -> Workload fails -> Worker uploads checkpoint as an artifact -> Coordinator flags attempt as `was_checkpointed` -> Next attempt payload includes checkpoint artifact hash -> New worker stages checkpoint -> Workload resumes.

### 5. Tests and Validation Performed
- **M14 Physical Validation**: Real-world LAN execution across multiple physical machines to prove network bridging, actual latency, and real filesystem limits.
- **M15 Scalability Benchmarks**: Synthetic load tests hammering the coordinator with thousands of concurrent placement requests to prove event loop non-blocking behavior.
- **M16 E2E Checkpoint Tests**: `test_m16_2_checkpoint_validation.py` verifying that workloads can crash, state is uploaded, and the subsequent attempt successfully mounts the state.

### 6. Known Failures, Limitations, and Unfinished Work
- **Windows `WinError 32` (M16 GC)**: GC routines attempting to delete old `workdirs` frequently crash on Windows if a background subprocess (like a lingering Python script) holds an open file handle. GC requires robust try/except suppression and deferred retry logic.
- **Large Checkpoint Latency**: Checkpointing 10GB of state involves massive network I/O, which blocks recovery.

### 7. Design Alternatives Considered
- **Why not `ProcessPoolExecutor`?** Thread pools were chosen over process pools for I/O bound tasks (like disk reading/writing) because of lower overhead. Process pools are strictly for CPU-bound tasks (which are handled by the actual workload subprocesses anyway).
- **Why active GC instead of TTL (Time To Live)?** Relying purely on TTL could prematurely delete artifacts that a slow workload still needs, or leave large artifacts on disk too long. Active state-based GC is deterministic.

---

### INTERVIEW QUESTIONS

#### A. BASIC UNDERSTANDING
- **Level 2**: What does it mean to "offload" a task from the asyncio event loop?
- **Level 2**: How does a workload inform the worker that it has created a checkpoint?

#### B. WHY QUESTIONS
- **Level 3**: Why did disk I/O in the CAS adapter block the Coordinator's HTTP endpoints before M15?
- **Level 4**: Why does AIDAR use a `ThreadPoolExecutor` instead of a `ProcessPoolExecutor` for offloading CAS hashing?

#### C. WHY-NOT QUESTIONS
- **Level 4**: Why not rely on the OS's temporary file clearer (like `/tmp` cleanup on reboot) instead of building a dedicated Garbage Collector?

#### D. HOW QUESTIONS
- **Level 3**: How does the coordinator restore a checkpoint on a new worker?
- **Level 4**: How does the active GC algorithm determine if an artifact is safe to delete?

#### E. ALGORITHM / DATA STRUCTURE
- **Level 4**: Explain how reference counting is used in the GC layer and what edge cases exist (e.g., circular references, though unlikely here, or orphaned references from crashes).

#### F. CODE-LEVEL QUESTIONS
- **Level 3**: What Python standard library module is used to achieve the M15 offloading? (Answer: `asyncio` and `concurrent.futures`).

#### G. FAILURE / RECOVERY
- **Level 3**: What happens if the worker dies *while* uploading a checkpoint?
- **Level 4**: What happens if the garbage collector encounters `WinError 32: The process cannot access the file because it is being used by another process`?

#### H. PERFORMANCE
- **Level 4**: What was the performance impact of M15's event-loop offloading on maximum concurrent HTTP throughput?

#### I. DISTRIBUTED SYSTEMS
- **Level 5**: How do you prevent a "split-brain" scenario where the GC deletes a checkpoint artifact because it assumes the workload is dead, but a partitioned worker is still actively trying to resume from it?

#### J. SECURITY
- **Level 3**: Does a workload have access to other workloads' checkpoints? (No, isolated `workdir`).

#### K. TESTING
- **Level 4**: How did you simulate thousands of concurrent workloads in M15 without requiring thousands of physical machines? (Answer: Synthetic load generation, mocking the physical worker execution layer).

#### L. TRADE-OFFS
- **Level 4**: What did the active GC design sacrifice? (It sacrificed extreme simplicity—adding complex state-tracking overhead—to gain deterministic disk space management).

#### M. SYSTEM DESIGN FOLLOW-UPS
- **Question**: How would you optimize the checkpoint protocol for a 50GB neural network state?
- **Follow-up 1**: Would you upload the entire 50GB every 10 minutes?
- **Follow-up 2**: How could you implement delta-checkpoints using the existing CAS architecture?
- **Follow-up 3**: What happens to network bandwidth if 100 workers delta-checkpoint simultaneously?

#### N. REAL-WORLD QUESTIONS
- **Level 4**: Describe the specific physical validation evidence from M14 regarding cross-worker asset transfer. What actually happened when Worker B needed an asset Worker A had?

#### O. RESUME DEFENSE
- **Claim you can make**: "Diagnosed and eliminated event-loop blocking in an asynchronous Python orchestrator, dramatically improving concurrent throughput."
- **Evidence**: M15 thread pool offloading of synchronous CAS ingestions.
- **Do NOT claim**: "Built a distributed fault-tolerant database." (It's an orchestrator and CAS).
