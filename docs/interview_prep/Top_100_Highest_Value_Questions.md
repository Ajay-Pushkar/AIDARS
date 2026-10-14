# AIDAR INTERVIEW PREPARATION: Top 100 Highest-Value Questions & Top 50 Deep Technical Questions

## Index & Study Guide
The requested Top 100 and Top 50 lists have been meticulously categorized into specific domains for better interview preparation. The culmination of the 130+ highly-specific questions below represents the absolute highest-value technical questions derived directly from the physical AIDAR validation and architectural PRDs.

Please refer to the domain-specific banks to study these questions:
1. **[System Design]**: See `Top_30_System_Design_Questions.md` (Covers scaling, GC algorithms, state partitioning, and high availability).
2. **[Architecture / Why Not]**: See `Top_30_Why_Not_Questions.md` (Covers the strict zero-dependency philosophy, rejecting Redis/Docker/Kafka).
3. **[Code-Level & Implementation]**: See `Top_30_Code_Level_Questions.md` (Covers specific modules like `workload.py`, Python `asyncio` mechanics, and `httpx` flows).
4. **[Failure & Recovery]**: See `Top_30_Failure_Recovery_Questions.md` (Covers `WinError 32`, `ASSET_STAGING_FAILURE`, hard affinity blocking, and network partitions).
5. **[Performance & Security]**: See `Top_20_Performance_Questions.md` and `Top_20_Security_Questions.md` (Covers SQLite WAL mode, ThreadPool offloading, O(A) set difference, and subprocess sandboxing).

### The Absolute Top 10 "Make or Break" Interview Questions:
1. Walk me through the exact execution flow from a client submitting an FFmpeg task to the final artifact being downloaded, including the CAS resolution phase.
2. How does AIDAR guarantee that an asset is never corrupted or partially written during concurrent downloads by multiple workers?
3. Explain the difference between `same_worker` (hard affinity) and `different_worker` (anti-affinity) and what happens to the workload state if the target worker dies.
4. Why did you offload CAS hashing and ingestion to a `ThreadPoolExecutor`, and what exact symptom did this fix in the Coordinator?
5. How does the `HitMissResolver` calculate missing assets in $O(A)$ average time, and what data structures are involved?
6. Describe the exact algorithm used in the active Garbage Collector to prevent deleting an artifact that is currently being used by a retry attempt.
7. How does the Intelligence layer integrate with the Placement Engine without violating the core safety boundary?
8. What is the `AttemptRegistry`, and how does it prevent an infinite retry loop on a permanently broken worker?
9. Explain the implementation of the `CheckpointManager` and how a stateful workload resumes on a new worker.
10. Defend the decision to use raw OS subprocesses instead of Docker for isolating workloads. What are the specific vulnerabilities you accepted?
