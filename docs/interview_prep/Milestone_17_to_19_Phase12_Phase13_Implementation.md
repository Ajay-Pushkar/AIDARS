# AIDAR INTERVIEW PREPARATION
## Milestones 17 to 19: Phase 12 (Intelligence) and Phase 13 (Platform Expansion) Implementation

### 1. What was actually introduced or changed
- **M17 (Final Phase 11 Audit)**: Hardened and audited the advanced scheduling features (affinity, predictive scoring scaffolding). Resolved critical bugs related to worker failure recovery where hard affinity constraints were being improperly dropped or violated during a retry.
- **M18 (Phase 12: Intelligence Implementation)**: Fully implemented the `IntelligenceAPI` and telemetry aggregation. Built heuristic prediction models for memory/duration forecasting. Hooked the intelligence scores directly into the `PlacementEngine` via `m7_ranking_adjustments`.
- **M19 (Phase 13: Platform Expansion Implementation)**: Fully implemented the Application Adapter pattern. Built and E2E tested specific adapters: Blender, LLM, ML, FFmpeg, and Custom Executables. Finalized the system's transition to a completely generic compute platform.

### 2. Architecture and Implementation Decisions
- **Loose Coupling via API (M18)**: The intelligence engine was implemented to be loosely coupled. If the Intelligence API fails or times out, the `PlacementEngine` gracefully degrades to deterministic hard-rules (CPU/RAM/Locality) without crashing the workload admission process.
- **Adapter Standard Interface (M19)**: Adapters are purely client-side or gateway-side constructs. They take domain-specific inputs (e.g., `ffmpeg -i in.mp4 out.mp4`), package the files, compute hashes, and submit a strictly compliant `WorkloadSpec` JSON to the Coordinator. The Coordinator never imports or runs FFmpeg logic itself.

### 3. Algorithms and Data Structures
- **Heuristic Exponential Moving Average (EMA)**: Used in M18 intelligence to predict execution times based on past identical job signatures, smoothing out anomalous spikes.
- **Polymorphic Workload Execution**: The generic `subprocess` runner relies on the `executable` and `args` defined in the `WorkloadSpec.execution_spec`, blindly invoking the requested binary (e.g., `python`, `ffmpeg`, `blender`) as long as the inputs are staged correctly.

### 4. Important APIs, Classes, Modules, Interfaces, and Execution Flows
- **Classes**: `BlenderAdapter`, `LLMAdapter`, `MLExecutionAdapter`, `FFmpegAdapter` (representing the M19 integrations). `TelemetryAggregator` (M18).
- **Flow (M19 End-to-End)**: Client CLI -> `FFmpegAdapter.prepare(video_file)` -> Computes SHA-256 -> Uploads via CAS -> Generates `WorkloadSpec` (`executable="ffmpeg"`) -> Submits to Coordinator -> Worker stages CAS asset -> Executes `ffmpeg` -> Collects outputs -> Coordinator updates state.

### 5. Tests and Validation Performed
- **M18 Intelligence Testing**: `test_m18_intelligence_api_performance.py` validating that the API can handle concurrent telemetry bursts without degrading.
- **M19 Adapter E2E Testing**: Exhaustive E2E physical validation of all adapters (Blender, LLM, ML, FFmpeg) proving that the core orchestrator successfully routes, executes, and verifies completely different application binaries using the exact same underlying logic.

### 6. Known Failures, Limitations, and Unfinished Work
- **Binary Dependencies**: M19 assumes that the required executable (e.g., `ffmpeg`, `blender`) is already installed and on the `PATH` of the selected worker. The current implementation does not dynamically provision or install missing host-level binaries (unless packaged inside the CAS assets as a portable binary, which is heavy).
- **Cross-Platform Execution**: If a workload specifies `executable="python3"` but lands on a Windows worker where the alias is `python`, it will fail with `FileNotFoundError`.

### 7. Design Alternatives Considered
- **Why not Docker/WASM for M19 Adapters?** Wrapping every workload in a Docker container or WASM module would guarantee dependency isolation (solving the binary dependency limitation), but it dramatically increases the operational complexity for end-users running AIDAR on standard desktop workstations. It remains a future extension.
- **Why not synchronous intelligence updates?** If the intelligence layer updated predictions synchronously on every telemetry ping, it would throttle the event loop. Updates are batched and processed asynchronously.

---

### INTERVIEW QUESTIONS

#### A. BASIC UNDERSTANDING
- **Level 2**: What does the FFmpeg adapter actually do before sending the job to the Coordinator?
- **Level 2**: How does the Intelligence layer react if it has never seen a specific workload before?

#### B. WHY QUESTIONS
- **Level 3**: Why does the `PlacementEngine` gracefully degrade if the `IntelligenceAPI` crashes, rather than halting execution?
- **Level 4**: Why was the adapter pattern strictly enforced on the client side rather than building parsing logic (like `.blend` parsing) into the Coordinator?

#### C. WHY-NOT QUESTIONS
- **Level 4**: Why not use Docker to ensure `ffmpeg` or `blender` is actually present on the worker?

#### D. HOW QUESTIONS
- **Level 3**: How does the coordinator know *how* to execute a custom ML script?
- **Level 4**: How does the Exponential Moving Average (EMA) algorithm handle a single massive latency spike caused by a network outage?

#### E. ALGORITHM / DATA STRUCTURE
- **Level 4**: What is the space complexity of storing telemetry for millions of executed workloads in order to generate intelligence predictions? How would you prune it?

#### F. CODE-LEVEL QUESTIONS
- **Level 3**: In M19, where is the specific logic that decides whether to run a workload on CPU vs GPU? (Answer: The adapter sets the minimum VRAM or specific hardware tags in the `WorkloadSpec`, which the core placement engine enforces).

#### G. FAILURE / RECOVERY
- **Level 3**: What happens if the FFmpeg executable is missing on the worker? How is the failure classified? (Answer: Application Error / Execution Failure. It will not retry on that worker, and might fail the job if it's a persistent environment issue not caught by tags).
- **Level 4**: How does the intelligence layer prevent a "thundering herd" problem if all workers report telemetry at the exact same millisecond?

#### H. PERFORMANCE
- **Level 5**: If 10,000 workers are executing 1-second workloads, the telemetry aggregation will overwhelm a single SQLite database. How would you redesign the M18 telemetry ingestion?

#### I. DISTRIBUTED SYSTEMS
- **Level 4**: How does the system ensure that a telemetry update from an old, delayed workload doesn't overwrite a newer prediction state?

#### J. SECURITY
- **Level 4**: Since M19 allows arbitrary executables to run via the Custom workload adapter, how does AIDAR prevent a user from submitting `executable="rm", args=["-rf", "/"]`?

#### K. TESTING
- **Level 3**: What specific physical tests were run to prove the M19 adapters worked?

#### L. TRADE-OFFS
- **Level 4**: What trade-off was made by assuming host-level binaries (Blender, FFmpeg) are pre-installed? (Optimized for fast, lightweight worker deployment; sacrificed strict environment reproducibility).

#### M. SYSTEM DESIGN FOLLOW-UPS
- **Question**: How would you modify AIDAR to execute Docker containers?
- **Follow-up 1**: Where would the container image come from? The CAS or an external registry?
- **Follow-up 2**: How would the worker mount the CAS assets into the container?
- **Follow-up 3**: How would you handle permissions if the container runs as root but the worker daemon runs as a standard user?

#### N. REAL-WORLD QUESTIONS
- **Level 4**: During the M19 implementation, how did you handle cross-platform paths when executing workloads on Windows workers vs Linux workers?

#### O. RESUME DEFENSE
- **Claim you can make**: "Expanded a specialized rendering orchestrator into a generalized, multi-workload distributed compute platform using an extensible Adapter pattern."
- **Evidence**: M19 PRD, FFmpeg/ML/LLM adapters, generic `WorkloadSpec`.
- **Do NOT claim**: "Built a secure, sandboxed, containerized grid-computing engine."
