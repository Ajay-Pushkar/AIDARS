# AIDAR MASTER INTERVIEW QUESTION BANK

## 11. AIDAR 5-Minute Explanation
"AIDAR is a distributed computing platform I built to intelligently orchestrate application workloads across multiple network nodes. Initially designed for Blender rendering, I generalized it into a universal compute engine. It consists of three main layers: 
1) A high-performance, atomic, zero-dependency Content-Addressed Storage (CAS) layer that manages all input and output assets using SHA-256 hashes to guarantee deduplication and integrity. 
2) A deterministic Control Plane (Coordinator and Workers) using asynchronous HTTP that evaluates workloads, strictly manages state/retries, isolates execution via subprocesses, and bridges physical environments. 
3) An advisory Distributed Intelligence layer that uses heuristic telemetry to predict resource limits and optimize scheduling, alongside strict affinity rules. 
The system operates securely on untrusted nodes without requiring heavy external infrastructure like Kubernetes, Redis, or Kafka."

## 12. AIDAR 10-Minute Deep Explanation
(Expands on the 5-minute pitch):
"The architecture separates application semantics from core orchestration through the 'Adapter Pattern'. Clients use an adapter (like FFmpeg or LLM) to package their inputs, hash them into the CAS, and generate a generic JSON `WorkloadSpec`. 
When the Coordinator receives this, the Placement Engine evaluates available workers against strict hard constraints (e.g., minimum RAM, CPU architecture). It then applies soft constraints via an Intelligence API—using exponential moving averages of past telemetry to rank workers based on likely completion time and asset locality (calculating the O(A) set difference of missing CAS hashes).
Once scheduled, the Coordinator dispatches the job over HTTP. The selected worker synchronizes missing assets via the CAS, stages them into an isolated, ephemeral `workdir`, and executes the binary. 
If the worker dies mid-flight, the Coordinator's inline retry loop excludes that worker and attempts a reschedule—unless the workload was bound by strict `same_worker` affinity, in which case it safely queues. We handle garbage collection via a reference-counting GC that actively reaps orphaned artifacts. The entire system is strictly validated across Windows and Linux nodes with physical network fault injections."

## 13. AIDAR Architecture Whiteboard Interview
**Draw 4 boxes:**
1. **Client / Adapter**: Converts App logic -> CAS Hashes + `WorkloadSpec`.
2. **Coordinator (Control Plane)**: `WorkloadOrchestrator`, `PlacementEngine`, `IntelligenceAPI`, `WorkerRegistry`.
3. **Worker (Execution)**: `GenericSubprocessRuntime`, local CAS, `tmp` workdirs.
4. **Storage (CAS)**: Split-hash directories, SQLite WAL metadata index.

*Whiteboard Flow*: 
Client -> (Uploads to CAS) -> Submits `WorkloadSpec` to Coordinator -> Coordinator calculates Placement Score (RAM + CPU + Locality + Intelligence) -> Assigns Worker -> Worker downloads missing CAS chunks -> Worker executes -> Worker hashes output -> Uploads to CAS -> Coordinator marks COMPLETED.

---

## 2-10. Curated Top Question Bank

### Top System Design Questions
1. **[Design]** How does AIDAR ensure consistent state if the Coordinator crashes *after* a worker finishes a job, but *before* the Coordinator's SQLite DB updates?
2. **[Design]** How would you modify AIDAR to support 10,000 workers? (Answer: Shard the coordinator, move off SQLite to a distributed DB like CockroachDB, implement P2P CAS syncing like BitTorrent instead of central pulling).
3. **[Design]** Design a distributed garbage collection algorithm for the CAS that prevents split-brain deletion.
4. **[Design]** Why does AIDAR use pull-based HTTP for execution dispatch instead of a push-based message queue?

### Top "Why Not?" Questions
1. **[Why Not]** Why not use Redis for the metadata index instead of embedded SQLite?
2. **[Why Not]** Why not use Docker for strict execution isolation?
3. **[Why Not]** Why not use Machine Learning for the Intelligence API?
4. **[Why Not]** Why not store binary blobs directly in SQLite?

### Top Failure / Recovery Questions
1. **[Failure]** What happens when `os.replace` fails on Windows with `WinError 32` during an active garbage collection sweep?
2. **[Failure]** A workload fails with `EXECUTION_TIMEOUT`. Does it retry? If so, does it retry on the same worker?
3. **[Failure]** What happens if a workload requires `same_worker` affinity, but that specific worker has a permanent hardware failure?
4. **[Failure]** A worker claims it has a CAS asset, but the filesystem chunk is corrupted. How does the system detect and recover? (Answer: `IntegrityVerifier` does a deep SHA-256 stream check, auto-evicts the corrupt record, and re-requests the asset).

### Top Code-Level Questions
1. **[Code]** In `workload.py`, where is the `exhausted_worker_ids` set updated, and why must it happen inline inside the `while True` loop instead of globally?
2. **[Code]** What is the exact data structure used to perform the hit/miss asset resolution in O(A) time? (Answer: Python `set()`).
3. **[Code]** How is the execution isolation enforced in `GenericSubprocessRuntime`?

### Top Performance Questions
1. **[Perf]** Why was CAS ingestion moved to a `ThreadPoolExecutor` in M15? What was the exact bottleneck?
2. **[Perf]** What is the memory footprint of reading a 10GB asset into the CAS? (Answer: O(1) bounded by the 64 KiB chunk size).
3. **[Perf]** How does SQLite's WAL mode improve concurrent throughput compared to the default journal mode?

### Top Security Questions
1. **[Security]** How does the system prevent path traversal attacks when staging CAS assets? (Answer: Strict SHA-256 regex validation `^[0-9a-f]{64}$`).
2. **[Security]** What prevents a malicious worker from spoofing an execution result?
3. **[Security]** Can a workload escape its `workdir` and delete the worker daemon's internal files?

### Top Resume-Defense Questions
1. **[Defense]** You claim "distributed intelligence" on your resume. How exactly is it intelligent? What algorithms are used? (Be ready to defend EMA heuristics vs. Neural Networks).
2. **[Defense]** You claim "fault tolerance." Prove how your system handles network partitions during workload dispatch.
3. **[Defense]** What physical constraints were hit when scaling this? (Be ready to discuss Windows file locking and event-loop blocking).

---

## 14. Weak Areas I Need to Study
1. **Windows OS-level File Locking Mechanisms**: Understand exactly why and when Windows throws `WinError 32` compared to POSIX systems, as this was the root cause of multiple physical test failures in AIDAR.
2. **Asyncio Event Loop Internals**: Be able to deeply explain the GIL, `run_in_executor`, thread pools vs process pools, and exactly why blocking I/O crashes an async web server.
3. **SQLite WAL Mode Mechanics**: Be prepared to explain how the WAL file is checkpointed into the main DB and how concurrency works at the filesystem level.

## 15. Claims I Must NOT Make (Because They Are Not Proven)
1. **Do NOT claim**: "AIDAR is a fully containerized, secure multitenant platform." (It relies on generic OS subprocesses, not strict cgroups or hypervisor isolation. A malicious workload can still fork-bomb the host).
2. **Do NOT claim**: "AIDAR uses Machine Learning to schedule workloads." (It uses statistical heuristics and exponential moving averages).
3. **Do NOT claim**: "AIDAR is highly available and masterless." (The architecture has a single central Coordinator. If the Coordinator goes down, the cluster halts).
4. **Do NOT claim**: "AIDAR supports millions of nodes in production." (It is physically validated on LAN-scale physical clusters, and the SQLite backing store would bottleneck long before 1,000,000 nodes).
