# AIDAR INTERVIEW PREPARATION: Top 30 Failure & Recovery Questions
1. How does the system handle an `httpx.ConnectError` during workload dispatch?
2. What happens when an active worker daemon process is killed via `SIGKILL` mid-execution?
3. How is `WinError 32` handled during an active Garbage Collection sweep on Windows?
4. What happens when a workload fails with `FailureCategory.ASSET_STAGING_FAILURE`?
5. Does the coordinator retry a workload if it encounters `FailureCategory.APPLICATION_ERROR`? Why or why not?
6. If an anchor workload establishes a `same_worker` affinity, but that worker fails permanently, what state does the next workload enter?
7. How does the CAS `IntegrityVerifier` handle a silently corrupted disk file?
8. What happens if a worker has an asset listed in SQLite but the file is missing from the `objects/` directory?
9. How is a split-brain scenario prevented if the coordinator times out an HTTP request but the worker actually finished the job?
10. What happens if the coordinator dies immediately after dispatching a workload?
11. If a checkpoint upload fails due to network outage, is the `was_checkpointed` flag set to true?
12. How does the `exhausted_worker_ids` set prevent infinite loops during recovery?
13. What happens if a workload exceeds the global `max_attempts` budget?
14. If a custom ML script has a syntax error, how is that failure classified and handled?
15. What happens if `os.replace` fails during the atomic CAS ingestion step?
16. How is recovery handled if the SQLite `index.db` is completely corrupted?
17. What happens if an adversarial worker attempts to upload a file that does not match its claimed SHA-256 hash?
18. How does the system recover if a workload enters an infinite `while True` loop and ignores termination signals?
19. What happens if the physical host disk runs out of space during a CAS staging operation?
20. How does the Intelligence layer handle an API timeout when querying for placement predictions?
21. What happens if the worker's expected executable (e.g., `ffmpeg`) is missing from the system `PATH`?
22. How does the coordinator detect that a worker has gracefully shut down vs. crashed?
23. What happens if a network partition separates a worker from the coordinator for 10 minutes?
24. How is a failed cross-worker asset transfer detected and recovered from?
25. What happens if the `workdir` cleanup fails after a workload finishes?
26. If the Coordinator restarts, how does it reconstruct the state of running workloads?
27. How does the system handle clock skew between the coordinator and workers?
28. What happens if a worker tries to heartbeat but its IP address has changed?
29. How does the Garbage Collector handle cyclic references in the artifact tree?
30. If an Intelligence layer prediction is absurdly wrong (e.g., predicting 1 ms for a 10-hour job), how does the system recover its scheduling balance?
