# AIDAR INTERVIEW PREPARATION
## Milestones 11 to 13: Advanced Scheduling, Distributed Intelligence, and Platform Expansion

### 1. What was actually introduced or changed
- **M11 (Advanced Scheduling)**: Evolved the basic placement engine into a rich scheduler supporting workload affinity (`same_worker`), anti-affinity (`different_worker`), deadline-aware scheduling, GPU heterogeneity awareness, and cost-aware placement.
- **M12 (Distributed Intelligence)**: Introduced the Intelligence Layer, an advisory component that predicts resource utilization, execution duration, and potential failures based on historical telemetry. It provides ranking recommendations to the scheduler.
- **M13 (Platform Expansion)**: Generalized AIDAR from a Blender-specific orchestrator into a generic compute platform by formalizing the "Application Adapter" pattern. Introduced adapters for Blender, LLM Inference, ML Training, FFmpeg (video transcoding), and generic custom CPU/GPU workloads.

### 2. Architecture and Implementation Decisions
- **Advisory Intelligence Boundary**: The intelligence layer (M12) was strictly architected as an *advisory* component. It can adjust placement scores and recommend workers, but it cannot override deterministic hard constraints (e.g., if a worker lacks a required GPU, it remains ineligible regardless of intelligence predictions).
- **Core Generality Rule**: "Adapters know applications; Core knows workloads." M13 ensured the core engine never parses application-specific files (like `.blend` or `.py` files) directly. Instead, adapters translate application needs into generic `WorkloadSpec` objects.
- **Affinity Semantics**: M11 introduced strict affinity groups. `same_worker` pins subsequent workloads to the worker that executed an "anchor" workload (essential for iterative rendering or stateful ML steps). `different_worker` enforces worker diversity to mitigate single-node localized failures.

### 3. Algorithms and Data Structures
- **Scoring Algorithm**: The Placement Engine calculates a normalized score (0.0 to 1.0) using a weighted sum of memory headroom, CPU capacity, network locality (CAS asset presence), and intelligence recommendations.
- **Prediction Models**: Heuristic-based statistical models analyzing historical execution times and telemetry to extrapolate future performance bounds and anomaly probabilities.

### 4. Important APIs, Classes, Modules, Interfaces, and Execution Flows
- **Classes**: `IntelligenceAPI` / `ResourcePredictor` / `PlacementEngine` / `WorkloadSpec` (enhanced with affinity constraints) / Application Adapters (e.g., `BlenderAdapter`, `LLMAdapter`).
- **Execution Flow (M12 Placement)**: `_process_workload` -> Hard filtering -> Intelligence ranking (adjusts scores) -> Highest score selected -> Dispatch.
- **Execution Flow (M13 Expansion)**: User submits `Job` -> Adapter translates to `WorkloadSpec` -> Core executes generically -> Output verified -> Adapter returns result to user.

### 5. Tests and Validation Performed
- **M11 Affinity Testing**: Verified that an anchor workload establishes an affinity group, and subsequent workloads correctly land on the same worker or are explicitly rejected/delayed if that worker is unavailable.
- **M12 Intelligence E2E**: Validated that prediction APIs return expected heuristic bounds and correctly influence the placement scoring function.
- **M13 Platform Tests**: Validated end-to-end execution of arbitrary Python workloads (simulating LLM/ML tasks) through the generic platform adapter interface without Blender dependencies.

### 6. Known Failures, Limitations, and Unfinished Work
- **Hard Affinity Deadlocks**: If an anchor worker permanently fails (e.g., hardware failure), subsequent workloads bound via hard `same_worker` affinity will indefinitely hang in the `SUBMITTED` state waiting for that worker, requiring manual job cancellation or migration.
- **Intelligence Cold Starts**: Without historical telemetry data, the intelligence layer defaults to naive baseline predictions, providing little value until sufficient data is accumulated.

### 7. Design Alternatives Considered
- **Why not Machine Learning for the Intelligence Layer?** Deep learning models were considered but rejected in favor of robust statistical heuristics to avoid heavy dependencies (like PyTorch) in the core coordinator, keeping the orchestrator lightweight.
- **Why not build application logic into the Coordinator?** Rejected. Hardcoding Blender or FFmpeg flags into the coordinator would destroy its ability to serve as a generic distributed computing platform.

---

### INTERVIEW QUESTIONS

#### A. BASIC UNDERSTANDING
- **Level 2**: What does it mean for a workload to have a `same_worker` affinity constraint?
- **Level 2**: What is the role of an Application Adapter in M13?

#### B. WHY QUESTIONS
- **Level 3**: Why is the Intelligence layer strictly advisory and forbidden from making absolute placement decisions?
- **Level 3**: Why does AIDAR use statistical heuristics instead of a neural network for resource prediction?

#### C. WHY-NOT QUESTIONS
- **Level 4**: Why not let the worker decide if it can execute a workload, instead of the coordinator making the placement decision?

#### D. HOW QUESTIONS
- **Level 3**: How does the coordinator aggregate the final placement score for a worker?
- **Level 4**: How does an application adapter translate an FFmpeg video transcoding job into a generic `WorkloadSpec`?

#### E. ALGORITHM / DATA STRUCTURE
- **Level 4**: Explain the weighting formula used in the placement engine. What happens if a worker has a high CPU capacity but is missing 90% of the CAS assets?

#### F. CODE-LEVEL QUESTIONS
- **Level 3**: How are affinity groups identified within the codebase? (Answer: via `affinity_group_id` on the `WorkloadSpec`).
- **Level 4**: What state does a workload enter if its hard affinity constraint cannot currently be met? (Answer: `SUBMITTED`).

#### G. FAILURE / RECOVERY
- **Level 4**: What happens if a workload with hard `same_worker` affinity fails due to an `ASSET_STAGING_FAILURE`? Will it retry on a different worker? (Answer: No. Hard affinity overrides failure rotation, keeping it bound to the original worker or blocking).
- **Level 5**: How does the system handle an anomaly prediction from the Intelligence layer? (It negatively adjusts the placement score, pushing the workload to a healthier node).

#### H. PERFORMANCE
- **Level 4**: What is the overhead of querying the Intelligence API during the placement critical path?

#### I. DISTRIBUTED SYSTEMS
- **Level 4**: How does the platform decouple application logic from distributed state management?

#### J. SECURITY
- **Level 4**: How do generic custom workloads (M13) impact the security boundaries established in M9?

#### K. TESTING
- **Level 3**: How did you validate that `different_worker` anti-affinity actually works?

#### L. TRADE-OFFS
- **Level 4**: What trade-off was made when implementing strict hard affinity? (Optimized for correctness and data locality; sacrificed high availability if that specific node fails).

#### M. SYSTEM DESIGN FOLLOW-UPS
- **Question**: How would you schedule a complex machine learning pipeline that requires sequential steps (Data Prep -> Training -> Evaluation)?
- **Follow-up 1**: How do you ensure the training step lands on a node with a GPU?
- **Follow-up 2**: How do you ensure the evaluation step has access to the massive model weights generated during training without transferring them over the network? (Answer: `same_worker` affinity).
- **Follow-up 3**: What if the node crashes during the evaluation step?

#### N. REAL-WORLD QUESTIONS
- **Level 4**: In physical testing, why did workloads occasionally fail with `WinError 32` file locks during retry sequences, and how did M10/M11 recovery logic interact with this?

#### O. RESUME DEFENSE
- **Claim you can make**: "Architected an advanced distributed scheduler with hard/soft affinity constraints and extensible application adapters."
- **Evidence**: `PlacementEngine`, `WorkloadSpec` affinity fields, M13 adapter patterns.
- **Do NOT claim**: "Built a deep-learning powered predictive scheduler." (It uses heuristics).
