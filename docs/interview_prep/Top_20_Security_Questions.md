# AIDAR INTERVIEW PREPARATION: Top 20 Security & Resume Defense Questions

## Top 20 Security Questions
1. What prevents a malicious worker from submitting a corrupted file with a valid SHA-256 hash?
2. How does the `GenericSubprocessRuntime` isolate the workload from the worker daemon?
3. What isolation does the runtime explicitly *not* provide?
4. How does the CAS prevent path traversal (e.g., submitting `../../etc/passwd` as an asset)?
5. If a workload executes `rm -rf /` on a Linux worker, what stops it?
6. How are API requests authenticated between the Coordinator and the Worker?
7. What prevents a worker from claiming jobs intended for a different affinity group?
8. How are environment variables passed securely to the subprocess without exposing them in shell commands?
9. What happens if a malicious client submits a 10TB asset to exhaust worker disk space?
10. How is the SQLite metadata index protected from SQL injection?
11. What is the attack surface if the Coordinator is exposed to the public internet?
12. How would you implement zero-trust network policies in AIDAR?
13. Does AIDAR encrypt CAS assets at rest or in transit?
14. How does the system prevent a compromised worker from reading another worker's checkpoints?
15. What security mechanisms prevent an infinite loop in a custom ML script from denying service to the worker?
16. How would you implement secure multi-tenancy in AIDAR?
17. Are workload output artifacts automatically scrubbed of sensitive data?
18. What prevents a worker from spoofing the telemetry statistics sent to the Intelligence API?
19. How is the Coordinator's internal memory protected against exhaustion attacks via massive HTTP payloads?
20. What security boundaries exist between the Application Adapter and the core Coordinator?

## Top 20 Resume Defense Questions & Proven Claims
1. **Challenge**: "You claim you built a distributed scheduler. How does it handle network partitions?" 
   **Defense**: Explain the asynchronous HTTP retry loop, attempt tracking, and deterministic `FailureCategory` logic.
2. **Challenge**: "You say your CAS is high-performance. Prove it."
   **Defense**: Discuss the split-hash directory, atomic `os.replace`, SQLite WAL mode, O(A) set difference, and ThreadPool offloading.
3. **Challenge**: "How do you guarantee isolation without Docker?"
   **Defense**: Acknowledge the limitation. Defend the OS subprocess isolation and `workdir` sandboxing as a lightweight compromise, while admitting lack of cgroups.
4. **Challenge**: "Why did you build your own cache instead of using Redis?"
   **Defense**: Explain the zero-dependency requirement for bare-metal desktop deployments and why SQLite/Filesystem is better optimized for large binary streams.
5. **Challenge**: "How does your intelligence layer actually work without deep learning?"
   **Defense**: Explain the Exponential Moving Average (EMA) heuristics and telemetry aggregation.
6. **Challenge**: "Did this actually run on physical hardware?"
   **Defense**: Detail the physical M14/M15 validations, the `WinError 32` file locking bugs discovered, and the exact throughput metrics achieved.
7. **Challenge**: "How do you handle worker failure?"
   **Defense**: Describe the `exhausted_worker_ids` inline exclusion set and the `max_attempts` budget.
8. **Challenge**: "If the coordinator goes down, does the cluster survive?"
   **Defense**: No. Acknowledge it is a central control plane architecture. 
9. **Challenge**: "What was the hardest bug you fixed?"
   **Defense**: The `ASSET_STAGING_FAILURE` infinite loop on Windows caused by orphaned subprocess file locks during hard affinity retries.
10. **Challenge**: "How do you handle garbage collection?"
   **Defense**: Explain active GC, reference counting, and deterministic state sweeps vs naive TTL.

*(Note: The remaining 10 Resume Defense questions map directly to the Weak Areas and "Do NOT Claim" sections from the Master Question Bank).*
