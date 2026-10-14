# AIDAR INTERVIEW PREPARATION: Top 20 Performance Questions
1. Why does AIDAR use 64 KiB chunking when streaming files into the CAS instead of loading the whole file?
2. What is the Big O time complexity of computing the `missing_assets_on_worker` set?
3. How did moving CAS ingestion into a `ThreadPoolExecutor` improve Coordinator throughput?
4. What happens to the Coordinator's event loop if a worker takes 10 minutes to download a 50GB asset?
5. How does SQLite's WAL mode prevent reader starvation during heavy concurrent ingestion?
6. How would you benchmark the maximum placement throughput of the `WorkloadOrchestrator`?
7. What is the performance penalty of enforcing strict hard affinity vs. soft affinity?
8. How does the active Garbage Collector avoid thrashing the disk IOPS during a sweep?
9. What is the memory footprint of the Intelligence layer's telemetry aggregation?
10. How does the split-hash directory structure optimize NTFS/ext4 inode lookups?
11. If the network bandwidth is saturated during cross-worker transfer, how does the system degrade gracefully?
12. Why doesn't AIDAR compress CAS assets by default, and what is the trade-off?
13. How does the system handle a massive spike in small-file requests (e.g., 100,000 tiny assets)?
14. What is the latency overhead of validating SHA-256 integrity on every file fetch?
15. How would you optimize the checkpoint protocol for extremely large state files?
16. How does the `PlacementEngine` scale when evaluating 10,000 available workers?
17. What caching strategies are used for the `IntelligenceAPI` heuristic models?
18. How does the use of Python's `asyncio` compare to a multi-threaded web server for this orchestrator?
19. What physical bottlenecks were encountered during the M15 baseline performance tests?
20. How would you redesign the CAS to support zero-copy asset transfers?
