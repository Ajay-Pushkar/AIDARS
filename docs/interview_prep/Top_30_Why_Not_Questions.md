# AIDAR INTERVIEW PREPARATION: Top 30 "Why Not?" Questions
1. Why not use Redis for cache metadata instead of SQLite?
2. Why not use Docker/cgroups for worker execution isolation?
3. Why not use RabbitMQ or Kafka for the dispatch queue instead of HTTP?
4. Why not use PyTorch/ML models for the Intelligence predictions instead of EMA heuristics?
5. Why not store the binary assets directly inside the SQLite database?
6. Why not use gRPC instead of standard HTTP for Coordinator-Worker communication?
7. Why not parse `.blend` files directly in the Coordinator to map dependencies?
8. Why not use a flat directory structure for the CAS instead of split-hash?
9. Why not build the worker daemon in Golang or Rust for performance?
10. Why not let workers push telemetry synchronously instead of async batching?
11. Why not use standard Time-To-Live (TTL) for garbage collection?
12. Why not deploy the Coordinator as a serverless AWS Lambda?
13. Why not use PostgreSQL instead of SQLite?
14. Why not allow workers to schedule themselves based on a shared queue?
15. Why not use a ProcessPoolExecutor instead of ThreadPoolExecutor for CAS ingestion?
16. Why not upload checkpoints continuously instead of on explicit failure/interrupt?
17. Why not use UUIDs instead of SHA-256 for asset identities?
18. Why not compress all assets by default in the CAS?
19. Why not allow the Intelligence layer to override hard constraints?
20. Why not dynamically install dependencies like `ffmpeg` on the worker at runtime?
21. Why not retry `APPLICATION_ERROR` failures?
22. Why not run the worker daemon as root to enforce strict isolation?
23. Why not stream the subprocess stdout directly back to the Coordinator in real-time?
24. Why not use Windows Named Pipes instead of loopback HTTP for local testing?
25. Why not clear the exhausted workers list if a worker restarts?
26. Why not use standard standard Python dictionaries instead of `pydantic` models for the API?
27. Why not put the entire `HitMissResolver` logic on the Coordinator side?
28. Why not write the CAS entirely in C++ via a Python extension?
29. Why not use MongoDB for telemetry storage?
30. Why not use long-polling or WebSockets for execution dispatch?
