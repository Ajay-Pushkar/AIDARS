# AIDAR INTERVIEW PREPARATION: Top 30 System Design Questions
1. Design a distributed execution orchestrator that ensures zero data loss if a worker dies mid-execution.
2. How would you scale the AIDAR CAS to 10,000 workers?
3. How do you implement global rate-limiting across a distributed cluster without a central Redis server?
4. Design a distributed Garbage Collection algorithm that avoids split-brain deletions.
5. If the Coordinator becomes a bottleneck, how would you partition the workload state?
6. Design an auto-scaler for AIDAR workers on AWS EC2.
7. How would you modify AIDAR to support peer-to-peer CAS chunk sharing?
8. Design a system to aggregate gigabytes of real-time telemetry from thousands of workers.
9. How would you implement distributed tracing (like Jaeger) across the HTTP execution dispatches?
10. Design a multi-tenant version of AIDAR with strict resource quotas.
11. How would you implement active-active high availability for the Coordinator?
12. If the SQLite WAL mode becomes a disk I/O bottleneck, what is the migration path?
13. Design a fault-tolerant job queue using only HTTP and local SQLite.
14. How would you integrate Kubernetes natively into AIDAR's execution plane?
15. Design a delta-checkpointing system for 50GB neural network weights.
16. How would you handle network partitions between a cluster of workers and the central coordinator?
17. Design an anomaly detection system for worker misbehavior without heavy ML models.
18. How do you guarantee exactly-once execution semantics in a distributed system?
19. Design a system that securely routes physical GPU workloads in a bare-metal environment.
20. How would you implement cross-region CAS replication?
21. Design a predictive scheduler for workloads with highly variable durations.
22. How would you handle strict data residency requirements (e.g., GDPR) in the CAS?
23. Design a system to prevent 'thundering herd' asset requests when a popular job is scheduled.
24. How do you synchronize distributed clocks for accurate telemetry aggregation?
25. Design a transparent proxy layer for worker-to-worker asset transfers.
26. How would you implement a distributed locking mechanism using only the Coordinator API?
27. Design a workflow engine (DAG support) on top of AIDAR's flat workload spec.
28. How would you detect and mitigate a compromised worker sending fake telemetry?
29. Design a cold-storage tier for the CAS.
30. How would you handle rolling zero-downtime upgrades of the Coordinator?
