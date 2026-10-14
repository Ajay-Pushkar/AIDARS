# AIDAR INTERVIEW PREPARATION: Top 30 Code-Level Questions
1. Where in the codebase is the CAS hit/miss logic implemented, and what data structure does it use?
2. Which module handles the translation of application-specific requests into generic `WorkloadSpec` objects?
3. In `workload.py`, why is `exhausted_worker_ids` updated inline inside the `while True` loop?
4. What is the exact purpose of the `CacheStore` ABC in `base.py`?
5. How does `SplitHashStorage` structure its directories? (Provide an exact string format example).
6. Which file contains the `GenericSubprocessRuntime` implementation?
7. In `test_m10_execution_attempts.py`, how is the worker HTTP endpoint mocked out?
8. What specific `FailureCategory` triggers an immediate workload failure without retry?
9. Where is the `ThreadPoolExecutor` initialized for CAS offloading, and how does it interact with the event loop?
10. How does the `PlacementEngine` handle the `same_worker` affinity flag in code?
11. Which class handles the verification of an artifact's `expected_output_count`?
12. In the CAS ingestion flow, what is the exact OS call used for atomic movement? (`os.replace`).
13. How is the SQLite connection configured in `index.py` to enable concurrent reads/writes?
14. Which dataclass holds the result of a cache resolution request?
15. How are environmental variables passed to the subprocess in `execution_spec`?
16. In the Garbage Collector, how are active references tracked programmatically?
17. What exception is caught when attempting to delete a locked file on Windows?
18. Where is the retry budget defined in the configuration files or constants?
19. How does the `FFmpegAdapter` extract output video frames and package them as artifacts?
20. In the Intelligence API, what specific statistical formula is used to calculate the predicted duration?
21. Which Pydantic model enforces the SHA-256 regex validation?
22. How does the coordinator build the payload for the `/api/v1/workloads/execute` endpoint?
23. In `verifier.py`, how is deep chunked SHA-256 streaming implemented without loading the whole file into RAM?
24. Which module handles the parsing of the Blender `.blend` file (Trick question: None, the adapter handles it).
25. How is the `m7_ranking_adjustments` dictionary structured and used in placement?
26. In the `AttemptRegistry`, what fields differentiate an attempt from a workload?
27. How does the `CheckpointManager` intercept a failed workload's state directory?
28. Which function calculates the `byte_hit_ratio` in the CAS metrics?
29. How is the coordinator's HTTP client configured to prevent hanging requests?
30. Where is the `WorkerRegistry` failure penalty logic implemented?
