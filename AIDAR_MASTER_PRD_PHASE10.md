# AIDAR MASTER PRODUCT REQUIREMENTS DOCUMENT
## AI-Driven Adaptive Render & Asset Distribution System

**Document type:** Master PRD + System Architecture Specification  
**Baseline:** AIDAR repository uploaded by the user, through the current M8 / Phase-9 security-adversarial state  
**Purpose:** Define what AIDAR is, what it is intended to become, its complete pipeline, architecture, contracts, CAS/storage design, distributed execution model, intelligence layer, security, failure handling, testing philosophy, and future phases.

---

# 1. Executive Summary

AIDAR is a **generic distributed computing and intelligence platform** designed to take application-specific workloads, understand their requirements, intelligently place them on available compute nodes, move only the required data, execute the workload safely, collect verified outputs, and continuously observe the health of the distributed system.

The original implementation began around Blender scene intelligence and render optimization. The architecture has since been deliberately generalized.

AIDAR must ultimately be able to support workloads such as:

- Blender rendering
- LLM inference
- ML training
- Video rendering/transcoding
- Future GPU/CPU workloads
- Other applications added through adapters without modifying the distributed core

The central architectural rule is:

> **Adapters may know applications. Core may know workloads. The core must never need to understand application internals.**

This allows AIDAR to evolve from a Blender-oriented render optimizer into a generic distributed workload platform.

---

# 2. Product Vision

AIDAR's long-term goal is to behave like an intelligent compute fabric.

A user should not need to manually answer:

- Which worker should execute this?
- Does that worker have enough CPU/RAM/VRAM?
- Does it already have the required data?
- Where should missing assets come from?
- How should the workload be isolated?
- What happens if the worker dies?
- How should the output be verified?
- Where should the output be stored?
- How should repeated identical requests be deduplicated?
- Is a worker becoming unhealthy?
- Should a risky worker receive new work?

AIDAR should make these decisions through explicit contracts, deterministic algorithms, resource-aware placement, content-addressed storage, and advisory intelligence.

The target experience is:

```text
User / Application
        |
        v
   Workload Request
        |
        v
 Application Adapter
        |
        v
   Generic WorkloadSpec
        |
        v
 Validation / Admission
        |
        v
 Intelligent Placement
        |
        v
 Dependency Resolution
        |
        v
 CAS / Asset Synchronization
        |
        v
 Isolated Execution
        |
        v
 Output Discovery
        |
        v
 SHA-256 Verification
        |
        v
 Output CAS Commit
        |
        v
 Result / Artifacts
        |
        v
 Observability + Durable State
```

---

# 3. Product Principles

## 3.1 Generic Core

The distributed core must not contain application-specific execution logic.

Forbidden in generic core:

- Blender APIs
- `bpy`
- Blender scene assumptions
- Torch-specific logic
- Transformers-specific logic
- HuggingFace-specific logic
- FFmpeg-specific execution semantics
- LLM prompt semantics
- ML model semantics

Applications belong behind adapters.

---

## 3.2 Explicit Contracts

Every major subsystem communicates through explicit data contracts.

Important contracts include:

- `WorkloadSpec`
- `RuntimeResult`
- `PlacementDecision`
- `WorkloadExecutionResult`
- Worker resource profiles
- Asset hashes
- CAS manifests
- Artifact records
- Runtime adapter contracts

Contracts must describe facts and requirements, not application-specific implementation details.

---

## 3.3 No False Success

AIDAR must never report successful execution simply because a function returned.

Success means the system has evidence that:

```text
process succeeded
AND
required outputs exist
AND
outputs are valid
AND
outputs were integrity-checked
AND
outputs were committed successfully
```

Synthetic output is permitted only when an explicit simulation mode has been requested.

Simulation must never masquerade as production execution.

---

## 3.4 Content-Addressed Identity

Assets are identified by SHA-256 content hashes rather than filenames or paths.

For an asset:

```text
H = SHA256(content)
```

The hash is its identity.

Therefore:

```text
same content -> same identity
different content -> different identity
different filename + same content -> same identity
same filename + different content -> different identity
```

This is the foundation of AIDAR's storage and distributed asset system.

---

## 3.5 Fail Closed

When a required dependency, executable, asset, credential, output, or integrity condition is missing, AIDAR should fail explicitly.

It must not silently:

- substitute another asset
- select an arbitrary hash
- fabricate output
- ignore a CAS failure
- execute using an undeclared local path
- report success after an incomplete transfer

---

## 3.6 M6/M7/M8 Ownership Boundaries

The architecture separates:

```text
M5 = data / asset layer
M6 = admission + placement + execution
M7 = predictive/advisory intelligence
M8 = durable operations + observability + security
```

M7 may advise the control plane.

M7 must not arbitrarily mutate M6 internals.

The control plane remains authoritative.

---

# 4. What AIDAR Is

AIDAR is a combination of:

1. **Application intelligence**
2. **Workload normalization**
3. **Content-addressed storage**
4. **Distributed asset transfer**
5. **Resource-aware scheduling**
6. **Sandboxed execution**
7. **Predictive system intelligence**
8. **Durable cluster state**
9. **Security and transport protection**
10. **Observability and recovery**

It is not simply:

- a Blender plugin
- a file uploader
- a render queue
- a cache
- a worker pool
- an LLM orchestrator

It combines these capabilities behind a generic architecture.

---

# 5. High-Level Architecture

```text
                         AIDAR
                           |
             +-------------+-------------+
             |                           |
       APPLICATIONS                  CONTROL PLANE
             |                           |
      +------+------+              +-----+------+
      |      |      |              |            |
   Blender  LLM    ML           M6 Placement  M7 Intelligence
      |      |      |              |            |
   Video / Future adapters         +-----+------+
                                         |
                                    M8 Operations
                                         |
                              +----------+----------+
                              |                     |
                           Workers              Persistent State
                              |
                    +---------+---------+
                    |                   |
                  M5 CAS             Runtime
                    |                   |
              Asset transfer       Isolated execution
                    |                   |
                    +---------+---------+
                              |
                         Verified outputs
```

---

# 6. Complete End-to-End Pipeline

## 6.1 Input

AIDAR can receive:

```text
.blend
Scene JSON
LLM model + prompt
ML dataset + training script
Video + render specification
Generic application workload
```

The input is application-specific at the edge.

---

## 6.2 Application Adapter

The adapter understands the application.

Examples:

```text
BlenderAdapter
LLMAdapter
MLTrainingAdapter
VideoRenderingAdapter
```

Its responsibility is to translate application-specific input into generic AIDAR contracts.

Conceptually:

```text
Application input
      |
      v
Application adapter
      |
      v
WorkloadSpec
```

The distributed core never needs to understand the original application structure.

---

# 7. Generic Workload Model

A `WorkloadSpec` describes what must be executed.

Conceptual fields include:

```text
workload_id
task_type
minimum CPU
minimum RAM
minimum VRAM
GPU requirement
estimated duration
input asset hashes
role-specific asset hashes
parameters
runtime requirements
timeout constraints
sensitivity metadata
simulation mode
```

Application-specific information can exist in parameters or adapter-owned metadata, but generic scheduling must rely only on generic resource and dependency requirements.

---

# 8. Blender Pipeline

Blender remains a first-class adapter, but Blender knowledge stays outside the generic distributed core.

## Blender-specific pipeline

```text
.blend
  |
  v
Blender Adapter
  |
  v
Canonical Scene Snapshot
  |
  v
Dependency Graph
  |
  v
Render Requirement Analysis
  |
  +--> Eligibility
  +--> Camera
  +--> Frustum
  +--> Occlusion
  +--> Lighting / Influence
  +--> Simulation
  +--> Dependency Closure
  |
  v
Smart Packaging
  |
  v
Content-addressed assets
  |
  v
Generic WorkloadSpec
  |
  v
M6 Placement
  |
  v
Worker
  |
  v
Blender Runtime
  |
  v
Rendered output
  |
  v
CAS
```

---

# 9. Blender Scene Intelligence

The original AIDAR pipeline contains a detailed scene intelligence layer.

## M1: Canonical Scene Intelligence

Responsibilities:

- ingest Blender-like scene information
- normalize scene data
- represent objects
- represent collections
- represent lights
- represent materials
- represent textures
- represent images
- represent animation metadata
- emit stable machine-readable scene snapshots

Purpose:

Prevent downstream systems from repeatedly interpreting Blender-specific structures.

---

# 10. M2: Dependency Graph

The dependency graph represents relationships such as:

```text
Scene
 |
 +-- Object
      |
      +-- Modifier
      |
      +-- Material
            |
            +-- Texture
                  |
                  +-- Image
```

The graph provides:

- dependency traversal
- transitive closure
- integrity analysis
- orphan detection
- unresolved reference detection

Algorithms include graph traversal such as BFS/DFS.

---

# 11. M3: Render Requirement Analysis

M3 determines which scene information is required to satisfy a render request.

Inputs include:

```text
camera
frame range
resolution
scene graph
visibility
lighting
simulation
dependencies
```

Major components:

### Eligibility Analyzer

Handles:

- `hide_render`
- animated visibility
- frame ranges

### Camera Analyzer

Handles:

- view basis
- projection
- field of view
- clipping

### Frustum Culler

Handles:

- transformed AABBs
- six-plane frustum tests
- conservative spatial rejection

### Occlusion

Uses conservative ray/AABB reasoning.

### Influence Analyzer

Preserves relevant:

- lights
- parent hierarchies
- simulations
- HDRI/world influence

### Dependency Resolver

Computes the transitive dependency closure.

Important safety principle:

> When uncertainty exists, preserve the dependency rather than incorrectly deleting it.

---

# 12. M4: Smart Packaging

M4 transforms render requirements into a portable package.

Pipeline:

```text
RenderRequirementReport
        |
        v
RequirementResolver
        |
        v
DependencyClosureResolver
        |
        v
PhysicalAssetResolver
        |
        v
PackagePlanner
        |
        v
PackageBuilder
        |
        v
PackageValidator
```

Responsibilities:

- resolve required assets
- resolve relative Blender paths
- detect missing assets
- classify embedded assets
- calculate SHA-256
- deduplicate identical content
- create portable package structure
- rewrite Blender internal paths
- verify package integrity

Canonical structure:

```text
AIDAR_PACKAGE/
├── manifest.json
├── scene/
│   └── scene.blend
├── assets/
│   ├── images/
│   ├── textures/
│   ├── libraries/
│   ├── caches/
│   └── generated/
└── metadata/
    └── requirements.json
```

---

# 13. M5: Content-Addressed Storage / CAS

M5 is AIDAR's data plane.

The key concept is:

> Store data by what it contains, not where it came from.

## Identity

```text
SHA256(content)
```

## Physical layout

```text
objects/
├── aa/
│   └── bbbbbbbbbbbbbbbbbbbbb...
├── 4f/
│   └── ccccccccccccccccccccc...
└── ...
```

A two-level fanout prevents a single directory from becoming enormous.

---

# 14. CAS Ingestion

A safe CAS write should be:

```text
input stream
    |
    v
chunked read
    |
    +--> SHA-256
    |
    +--> staging file
    |
    v
verify hash
    |
    v
atomic rename
    |
    v
metadata index
```

Large files must not require loading the entire object into RAM.

The architecture uses bounded chunking.

---

# 15. CAS Metadata

The metadata index tracks information such as:

```text
hash
size_bytes
asset_type
original_name
source_path
created_at
last_accessed_at
access_count
verification_status
```

SQLite WAL is used for durable metadata indexing.

---

# 16. Cache Resolution

Given:

```text
required = {H1, H2, H3, H4}
cached   = {H1, H3}
```

AIDAR calculates:

```text
missing = required - cached
        = {H2, H4}
```

This is set-based resolution with average O(A) behavior relative to the number of asset hashes.

This determines what must actually be transferred.

---

# 17. CAS Integrity

Integrity checking occurs at multiple levels.

### Fast verification

Checks:

- object exists
- metadata exists
- size matches

### Deep verification

Recomputes SHA-256 from file contents.

If:

```text
SHA256(file) != expected_hash
```

the object is considered corrupted.

AIDAR must not silently continue using corrupted content.

Self-healing can remove the invalid object and trigger re-fetching from a trusted source.

---

# 18. CAS Eviction

The local cache has finite capacity.

AIDAR tracks access information and can evict older entries using an LRU-style policy.

Conceptually:

```text
quota exceeded
      |
      v
sort by last_accessed
      |
      v
evict oldest entries
      |
      v
continue until quota satisfied
```

Eviction must coordinate metadata and physical storage safely.

---

# 19. Distributed CAS Mesh

AIDAR extends local CAS into a distributed asset layer.

Conceptually:

```text
                 Coordinator
                     |
        +------------+------------+
        |            |            |
     Worker A     Worker B     Worker C
        |            |            |
       CAS          CAS          CAS
```

The coordinator maintains knowledge of which workers have which hashes.

Conceptually:

```text
SHA256_HASH -> {WorkerA, WorkerC}
```

This is an inverted inventory index.

---

# 20. Asset Transfer

When a worker requires asset `H`:

```text
Worker
  |
  | request H
  v
Coordinator
  |
  | locate candidates
  v
Candidate workers
  |
  | stream chunks
  v
Destination staging
  |
  | progressive SHA-256
  v
Integrity verification
  |
  v
Atomic CAS commit
```

Transfer must be:

- chunked
- resumable where supported
- integrity verified
- failure aware
- memory bounded

---

# 21. Network Locality

The asset source-selection system can use locality tiers such as:

```text
LOOPBACK
   >
SUBNET
   >
LAN
   >
WAN
```

The exact ordering and scoring must remain configurable.

The goal is to reduce:

- latency
- network traffic
- transfer time
- unnecessary WAN movement

---

# 22. M6: Computational Resource System

M6 is responsible for deciding:

> Where should this workload run?

It observes workers and evaluates whether they can satisfy the workload.

Worker information includes:

```text
CPU cores
CPU utilization
RAM total
RAM available
GPU availability
GPU identity
VRAM total
VRAM available
active workload count
cached asset hashes
worker status
telemetry timestamp
```

---

# 23. Placement

The conceptual placement objective is:

```text
select an eligible worker
that satisfies hard requirements
and optimizes the configured placement score
```

Candidate filtering happens before scoring.

A worker is not eligible if it cannot satisfy mandatory requirements.

Examples:

```text
insufficient RAM -> reject
required GPU absent -> reject
insufficient VRAM -> reject
worker unhealthy -> reject
stale resource profile -> reject
```

---

# 24. Placement Factors

Placement can incorporate:

- compute headroom
- memory headroom
- GPU suitability
- data locality
- network locality
- latency
- active workload count

A conceptual score is:

```text
S(w, workload)
 =
 compute
 + memory
 + GPU
 + data locality
 + network locality
 - load penalty
```

The score must remain explainable.

A `PlacementDecision` should contain a breakdown such as:

```text
compute: ...
locality: ...
latency: ...
load: ...
```

This prevents the scheduler from becoming an opaque black box.

---

# 25. Admission Control

Before dispatch, AIDAR must ensure that accepting the workload does not exceed cluster or worker limits.

Admission reservations can account for:

```text
workload count
CPU
RAM
VRAM
```

Temporary capacity conflicts may result in bounded retry/queue behavior rather than immediate failure.

---

# 26. SingleFlight Deduplication

If 100 workloads simultaneously request the same missing asset:

```text
100 requests for H
        |
        v
   SingleFlight
        |
        v
    1 transfer
        |
        +----> 100 waiters
```

Invariant:

```text
N concurrent requests for H
        =>
network transfers for H = 1
```

This prevents transfer storms.

---

# 27. M6 Execution Pipeline

```text
WorkloadSpec
     |
     v
Validate
     |
     v
Admission
     |
     v
Placement
     |
     v
Dependency resolution
     |
     v
CAS synchronization
     |
     v
Workspace creation
     |
     v
Runtime validation
     |
     v
Execution
     |
     v
Output discovery
     |
     v
SHA-256
     |
     v
CAS commit
     |
     v
WorkloadExecutionResult
```

---

# 28. Workspace Isolation

Workloads must never execute directly inside CAS object directories.

A conceptual workspace is:

```text
workloads/<workload_id>/
├── inputs/
├── outputs/
├── logs/
│   ├── stdout.log
│   └── stderr.log
└── metadata.json
```

Inputs are staged or linked from CAS.

Outputs are newly produced files.

This prevents a runtime from accidentally modifying canonical CAS objects.

---

# 29. Generic Runtime Contract

All application runtimes use one generic execution contract.

Conceptually:

```python
async def validate(
    workload: WorkloadSpec,
    workspace: Path,
) -> None:
    ...

async def execute(
    workload: WorkloadSpec,
    workspace: Path,
) -> RuntimeResult:
    ...
```

The contract must not contain:

```text
Blender-specific fields
LLM-specific result fields
ML-specific result fields
FFmpeg-specific objects
```

---

# 30. RuntimeResult

Generic runtime results describe execution facts.

Core fields include:

```text
success
exit_code
duration_seconds
output_paths
stdout
stderr
error_message
```

Optional generic fields:

```text
timed_out
metadata
```

Application-specific interpretation remains in the adapter.

---

# 31. Runtime Implementations

## Blender

Runs a real Blender executable when real mode is requested.

Real mode must use staged CAS input rather than assuming the source path exists on the worker.

## LLM

Uses the configured real inference executable/runtime.

If the executable is unavailable, real mode fails closed.

## ML Training

Runs a real training script/runtime with explicitly identified dataset/model/script assets.

## Video

Runs the real FFmpeg pipeline against the staged project input.

It must not replace the user's input with synthetic `testsrc` data in real mode.

---

# 32. Simulation Mode

Simulation exists for controlled tests and development.

Example:

```text
simulation_mode = true
```

Simulation must be explicit.

Real mode must never silently downgrade into simulation.

This rule exists because a green test that executed no real workload is not evidence of production correctness.

---

# 33. Workload State Machine

A generic workload lifecycle is:

```text
SUBMITTED
    |
    v
VALIDATING
    |
    +---- invalid ----> FAILED
    |
    v
PLACING
    |
    +---- no eligible worker ----> PENDING / UNSCHEDULABLE
    |
    v
PLACED
    |
    v
SYNCING_ASSETS
    |
    +---- transfer failure ----> RETRY / FAILED
    |
    v
READY
    |
    v
EXECUTING
    |
    +---- timeout ----> TIMEOUT
    |
    +---- crash ----> FAILED / RECOVERY
    |
    v
INGESTING
    |
    +---- integrity failure ----> FAILED
    |
    v
COMPLETED
```

Migration/recovery states can exist where required.

---

# 34. M7: Predictive / Adaptive Intelligence

M7 is the intelligence layer that observes system behavior.

It can analyze:

- CPU trends
- memory trends
- latency
- throughput
- worker behavior
- anomaly signals
- degradation
- risk
- predicted instability

The important boundary is:

```text
M7 observes
     |
     v
M7 predicts/advises
     |
     v
M6/control plane decides
```

M7 must not become a second hidden scheduler.

---

# 35. Worker Health

Workers expose health and telemetry.

Possible states include:

```text
ACTIVE
DRAINING
UNHEALTHY
OFFLINE
```

A worker that is predicted to become unsafe can be drained so new work is not placed there.

Existing work can be allowed to finish or be recovered according to policy.

---

# 36. Predictive Recovery

A conceptual recovery path:

```text
Telemetry
   |
   v
Feature extraction
   |
   v
Anomaly / behavior analysis
   |
   v
Risk estimate
   |
   v
M7 advisory policy
   |
   v
M6 control-plane action
   |
   +--> drain worker
   +--> checkpoint
   +--> migrate/re-place workload
```

Recovery must preserve the authority of M6/control-plane state.

---

# 37. Worker Failure

If a worker disappears:

```text
heartbeat failure
       |
       v
worker marked unavailable
       |
       v
executing workloads identified
       |
       v
recovery policy
       |
       v
re-placement
       |
       v
dependency synchronization
       |
       v
execution restart/resume
```

Recovery behavior depends on whether the runtime is restartable and whether checkpoint state exists.

---

# 38. M8: Production Cluster Operations

M8 hardens the system for real operational use.

Major capabilities include:

- durable state
- worker lifecycle
- workload persistence
- readiness checks
- health checks
- metrics
- secure transport
- authentication
- TLS support
- graceful shutdown
- restart recovery

---

# 39. Durable State

The coordinator persists control-plane state using SQLite with WAL.

State can include:

```text
workers
workloads
placement information
execution information
```

On restart, persisted workers must not automatically become trusted/live.

They are restored as:

```text
OFFLINE
```

until fresh registration/heartbeat proves liveness.

This prevents stale worker records from becoming active accidentally.

---

# 40. Observability

AIDAR exposes operational signals such as:

```text
worker registrations
heartbeats
workload submissions
submit latency
execution status
cluster health
```

Endpoints include concepts such as:

```text
/healthz
/readyz
/metrics
```

Metrics are Prometheus-compatible.

---

# 41. Security

AIDAR's control plane and data plane must be protected.

Security mechanisms include:

- bearer authentication
- constant-time token comparison
- TLS configuration
- CA verification
- secure client transport
- explicit insecure-mode opt-in
- fail-closed authentication

Authenticated clients should reject plain HTTP by default unless insecure transport has been explicitly enabled for a trusted environment.

---

# 42. Security Boundary

The following should be treated as security-sensitive:

```text
worker registration
worker inventory
workload submission
workload execution
asset streaming
artifact retrieval
control-plane state
LLM prompts
credentials
TLS private keys
```

Sensitive workload parameter names can be declared in the generic workload contract.

Durable metadata must redact declared sensitive values.

---

# 43. API / Control Plane

Conceptual control-plane operations:

```text
register worker
heartbeat
worker inventory
submit workload
get workload status
get worker status
stream asset
retrieve artifact
health
readiness
metrics
```

The exact HTTP routes may evolve, but the contracts must remain stable.

---

# 44. Coordinator

The coordinator is the control-plane authority.

Responsibilities:

- worker registry
- workload registry
- admission
- placement
- dispatch
- asset coordination
- M7 integration
- persistence
- health
- metrics
- security

It should not contain application-specific execution logic.

---

# 45. Worker

A worker is a compute/data node.

Responsibilities:

- advertise resources
- send heartbeat
- expose CAS content
- receive workload assignments
- synchronize dependencies
- execute runtime
- produce outputs
- commit verified artifacts
- report results

A worker should not decide global placement.

---

# 46. Failure Model

AIDAR must assume failures happen.

Failure classes include:

### Input failure

```text
missing input
corrupt input
invalid hash
```

### Network failure

```text
connection timeout
partial stream
worker disappears
```

### Worker failure

```text
crash
heartbeat loss
resource exhaustion
```

### Runtime failure

```text
non-zero exit
missing executable
invalid arguments
timeout
cancellation
```

### Storage failure

```text
corrupted CAS object
metadata inconsistency
disk full
SQLite corruption
```

### Security failure

```text
invalid token
unauthorized request
TLS verification failure
```

Every failure should produce an explicit state and diagnostic.

---

# 47. Adversarial Testing

AIDAR is intentionally tested against hostile or malformed conditions.

Examples:

- corrupted CAS bytes
- interrupted transfers
- stale worker inventory
- worker disappearance
- malformed workload payloads
- oversized parameters
- zombie workers
- authentication forgery
- database corruption
- concurrent access races
- duplicate requests
- invalid output paths
- runtime timeouts
- missing executables

The goal is not merely to prove the happy path.

---

# 48. Test Philosophy

Tests must distinguish:

```text
unit correctness
integration correctness
end-to-end correctness
real-runtime execution
environment-gated tests
physical deployment validation
```

A mocked runtime must not be reported as proof of real runtime execution.

For example:

```text
Blender executable unavailable
```

means:

```text
Blender real-runtime test = environment-gated
```

It does not mean:

```text
Blender test = passed
```

---

# 49. Current Validation Truth

The uploaded repository contains a documented validation state in which:

- core namespaces contain no adapter imports
- application-specific leakage checks are clean
- stale legacy dependency references are absent from executable source
- real FFmpeg execution was validated
- real ML execution was validated
- LLM real mode fails closed when its executable is unavailable
- Blender real mode fails closed when Blender is unavailable
- loopback authenticated worker validation was exercised

The repository documentation also records a Blender-gated environment skip because the environment did not contain a Blender executable.

A future machine with Blender installed must execute the genuine Blender test instead of replacing it with a fake executable.

---

# 50. Physical Deployment

AIDAR does not fundamentally require Wi-Fi.

It requires IP reachability between nodes.

Possible deployment:

```text
                Router / LAN
                    |
        +-----------+-----------+
        |           |           |
    Coordinator   Worker A   Worker B
        |           |           |
       CAS         CAS         CAS
```

The transport may be:

- Ethernet
- Wi-Fi
- routed private network
- another IP-capable network

The important property is network reachability and security.

---

# 51. Mobile / Lightweight Workers

Android devices can potentially be used as lightweight physical worker nodes for selected validation or compute workloads if a compatible worker runtime is implemented.

They should not be treated as equivalent to a full PC/GPU worker.

Possible uses:

- connectivity validation
- heartbeat validation
- lightweight generic workloads
- control-plane testing
- asset-transfer testing

Application-specific heavyweight runtimes such as Blender remain dependent on supported host environments.

---

# 52. Data Flow

The complete data flow is:

```text
                APPLICATION
                     |
                     v
              APPLICATION ADAPTER
                     |
                     v
               WorkloadSpec
                     |
                     v
          +----------------------+
          | VALIDATION / ADMISSION|
          +----------------------+
                     |
                     v
               M6 PLACEMENT
                     |
          +----------+----------+
          |                     |
          v                     v
      Worker profile        M7 advice
          |
          v
       selected worker
          |
          v
       M5 CAS lookup
          |
      +---+---+
      |       |
   cached   missing
      |       |
      |       v
      |   asset transfer
      |       |
      +---+---+
          |
          v
      isolated workspace
          |
          v
      runtime validate
          |
          v
       execute
          |
          v
     output discovery
          |
          v
       SHA-256
          |
          v
       CAS commit
          |
          v
   WorkloadExecutionResult
          |
          v
     client / artifact
```

---

# 53. Control Plane vs Data Plane

This separation is critical.

## Control plane

Handles:

```text
workers
workloads
placement
admission
health
state
security
metrics
recovery policy
```

## Data plane

Handles:

```text
CAS objects
asset streaming
input synchronization
output artifacts
large binary movement
```

The data plane must scale independently from control metadata.

---

# 54. Storage Model

AIDAR effectively contains multiple storage classes.

## Canonical CAS

Immutable content-addressed objects.

## Metadata

SQLite control/index data.

## Workspaces

Temporary execution data.

## Durable state

Coordinator lifecycle/control-plane persistence.

## Artifacts

Verified output objects committed back into CAS.

---

# 55. Artifact Lifecycle

```text
runtime output
     |
     v
output discovery
     |
     v
size/path validation
     |
     v
SHA-256
     |
     v
staging
     |
     v
CAS commit
     |
     v
artifact hash
```

The result should reference immutable artifact hashes rather than fragile filesystem paths.

---

# 56. Idempotency

Repeated workload submission should be handled safely.

Possible identity strategies include:

```text
workload_id
request identity
content identity
application-defined deterministic key
```

The system must not accidentally execute the same logical request multiple times merely because a client retries after a network failure.

---

# 57. Concurrency

AIDAR must support concurrent workloads without cross-talk.

Isolation boundaries include:

```text
workload IDs
workspaces
resource reservations
CAS staging files
locks
singleflight keys
database transactions
```

Concurrency correctness is more important than merely maximizing throughput.

---

# 58. Resource Accounting

The scheduler should track reserved versus available resources.

For example:

```text
RAM available
-
RAM reserved
=
schedulable RAM
```

Likewise:

```text
CPU
VRAM
workload slots
storage
```

Admission must use consistent accounting to prevent overcommit caused by race conditions.

---

# 59. Performance Goals

AIDAR should optimize:

- asset transfer volume
- network distance
- worker utilization
- scheduling latency
- memory consumption
- duplicate work
- repeated computation
- storage footprint

Algorithms should be chosen for scalability.

Examples already present in the design:

- hash-set difference for asset resolution
- BFS/DFS for dependency closure
- inverted hash index
- locality-aware placement
- SingleFlight request coalescing
- bounded streaming
- LRU eviction
- WAL-backed metadata
- asynchronous subprocess execution

---

# 60. Scalability Direction

The architecture should eventually support:

```text
1 coordinator
       |
       +---- 1 worker
       |
       +---- 10 workers
       |
       +---- 100 workers
       |
       +---- 1000+ workers
```

The architecture must avoid assumptions that:

- all workers are on localhost
- all workers have the same hardware
- all workers have identical applications installed
- all workers have identical storage
- all assets fit in memory
- all requests are short

---

# 61. Heterogeneous Compute

AIDAR should understand that workers differ.

Example:

```text
Worker A
CPU: 16
RAM: 64 GB
GPU: RTX-class
VRAM: 16 GB

Worker B
CPU: 8
RAM: 32 GB
GPU: none

Worker C
CPU: 32
RAM: 128 GB
GPU: high-memory
```

A workload should declare requirements.

The scheduler decides eligibility and placement.

---

# 62. Application Adapter Expansion

Future adapters should follow the same pattern:

```text
application
    |
    v
Adapter
    |
    v
generic WorkloadSpec
    |
    v
M6/M5/M7/M8
```

Potential future applications:

- CAD rendering
- scientific simulations
- video encoding
- batch image processing
- data preprocessing
- game-engine builds
- compiler workloads
- simulation engines
- GPU inference
- distributed training

Adding an application should primarily require an adapter/runtime implementation, not a rewrite of the control plane.

---

# 63. Architecture Rule for New Applications

When adding a new application:

### Allowed

```text
src/aidars/adapters/<application>/
```

may know:

- application API
- executable
- model format
- project format
- application-specific parameters
- output interpretation

### Not allowed

```text
src/aidars/distributed/
src/aidars/cache/
src/aidars/core/
src/aidars/m7/
```

must not start importing application implementations.

---

# 64. API Contract Stability

The internal architecture can evolve, but stable boundaries should be preserved.

Priority contracts:

```text
WorkloadSpec
Runtime contract
CAS identity
Worker registration
Placement decision
Execution result
Artifact identity
```

Changing implementation behind these contracts should not require rewriting every application adapter.

---

# 65. Security Model

A production deployment should eventually support stronger identity than a shared bearer token.

Long-term direction:

```text
Node identity
      |
      v
Mutual authentication
      |
      v
Authorization
      |
      v
Capability-based permissions
```

Potential future mechanisms:

- mTLS
- per-node credentials
- certificate rotation
- short-lived tokens
- role-based permissions
- workload authorization
- audit logging

The exact mechanism is a future implementation decision.

---

# 66. Observability Architecture

Observability should exist at three levels.

## Node

```text
CPU
RAM
GPU
VRAM
disk
network
process state
```

## Workload

```text
queue time
placement time
transfer time
execution time
output ingestion time
failure reason
```

## Cluster

```text
worker count
healthy workers
draining workers
failed workers
queue depth
throughput
transfer volume
cache hit ratio
network savings
```

---

# 67. Explainability

AIDAR should explain decisions.

For placement:

```text
selected worker
why eligible
compute score
memory score
GPU score
data locality
network locality
load penalty
```

For recovery:

```text
observed anomaly
risk signal
advisory action
control-plane action
```

For asset transfer:

```text
requested hash
source worker
locality tier
transfer size
verification result
```

This is essential for debugging a distributed system.

---

# 68. Recovery Philosophy

AIDAR should prefer:

```text
detect
contain
verify
recover
resume
```

rather than:

```text
ignore
continue
hope
```

Examples:

```text
corrupt CAS object
    -> remove/quarantine
    -> fetch trusted copy
    -> verify
    -> continue

worker failure
    -> detect
    -> mark unavailable
    -> re-place workload
    -> synchronize dependencies
    -> execute

runtime failure
    -> capture diagnostics
    -> release resources
    -> report explicit failure
```

---

# 69. Configuration

Configuration should eventually control:

```text
cluster identity
worker identity
ports
TLS
authentication
CAS root
cache quota
heartbeat interval
worker timeout
placement weights
admission limits
runtime timeouts
logging
metrics
```

Configuration should not require code modification.

Secrets must not be committed to source control.

---

# 70. CLI / User Experience

A future CLI can expose operations such as:

```text
aidar worker start
aidar coordinator start
aidar worker status
aidar cluster status
aidar submit
aidar workload status
aidar artifact get
aidar cache status
aidar inspect
```

A user should be able to submit a workload without understanding internal placement algorithms.

---

# 71. Example User Journey

User wants to render a Blender project.

```text
aidar submit project.blend --frames 1:500
```

AIDAR:

1. loads the project
2. analyzes scene requirements
3. resolves dependencies
4. packages required assets
5. hashes assets
6. creates workload
7. checks cluster capacity
8. finds workers
9. determines data locality
10. transfers only missing assets
11. executes isolated render work
12. collects output
13. verifies output
14. stores output in CAS
15. reports artifact hashes
16. exposes result to user

The user does not manually copy scene assets between machines.

---

# 72. Example LLM Journey

```text
LLM request
    |
    v
LLM Adapter
    |
    v
WorkloadSpec
    |
    +--> model hash
    +--> prompt
    +--> GPU/RAM requirements
    |
    v
M6 placement
    |
    v
model CAS synchronization
    |
    v
isolated inference runtime
    |
    v
result artifact
```

Prompt values can be treated as sensitive metadata.

---

# 73. Example ML Training Journey

```text
training script
dataset
model/checkpoint
configuration
        |
        v
ML Adapter
        |
        v
WorkloadSpec
        |
        v
resource placement
        |
        v
CAS synchronization
        |
        v
isolated training
        |
        v
checkpoint
        |
        v
CAS
```

---

# 74. Example Video Journey

```text
video input
    |
    v
Video Adapter
    |
    v
WorkloadSpec
    |
    v
FFmpeg runtime
    |
    v
output video
    |
    v
SHA-256
    |
    v
CAS artifact
```

Real mode must process the actual staged input.

---

# 75. Phase / Milestone Structure

The documented architecture has evolved through multiple milestones.

## M1

Canonical scene intelligence.

## M2

Dependency graph.

## M3

Spatial render requirement analysis.

## M4

Smart packaging.

## M5

Content-addressed distributed asset layer.

## M6

Computational resource system.

## M7

Predictive/adaptive intelligence.

## M8

Production cluster operations.

## Phase 9

Adversarial/security-oriented validation and hardening around the distributed system.

## Phase 10+

The next stages should focus on production-grade scale, broader runtime support, stronger recovery, physical deployment validation, and advanced distributed intelligence rather than rewriting the established foundations.

Exact future Phase-10 feature names should be finalized before implementation rather than invented retroactively.

---

# 76. Proposed Future Phase Direction

The future roadmap can be organized into:

### Phase 10: Production Distributed Execution

Focus:

- stronger workload recovery
- production-grade checkpointing
- artifact lifecycle management
- richer scheduler behavior
- real cluster deployment
- multi-worker scaling
- execution observability

### Phase 11: Advanced Scheduling

Focus:

- predictive placement
- queue optimization
- heterogeneous GPU scheduling
- cost-aware placement
- workload affinity
- deadline-aware scheduling

### Phase 12: Distributed Intelligence

Focus:

- better anomaly prediction
- adaptive policies
- learned resource models
- workload behavior models
- automated capacity planning

### Phase 13: Platform Expansion

Focus:

- additional application adapters
- external API
- user management
- multi-tenant isolation
- stronger security
- cluster federation

These are roadmap proposals, not claims about already implemented repository functionality.

---

# 77. Non-Goals

AIDAR should not become:

- a replacement for every application
- a generic container orchestration clone without reason
- an opaque AI scheduler that cannot explain itself
- a system that hides failures
- a filesystem that ignores content integrity
- an application-specific monolith
- a fake benchmark environment

The architecture should remain focused on intelligent distributed workload and asset orchestration.

---

# 78. Critical Invariants

## I1: No Invalid Placement

```text
requirements > available resources
=> worker cannot be selected
```

## I2: No Execution Without Dependencies

```text
missing required assets != empty
=> execution cannot begin
```

## I3: No Unverified Output

```text
output hash invalid
=> CAS commit rejected
```

## I4: SingleFlight

```text
N concurrent requests for same hash
=> one in-flight transfer
```

## I5: No False Success

```text
SUCCESS
iff
execution succeeded
AND outputs exist
AND outputs are verified
AND outputs are committed
```

## I6: Application Isolation

```text
generic core
!=
application implementation
```

## I7: Durable Worker Trust

```text
restored worker
=> OFFLINE
until fresh liveness proof
```

## I8: M7 Authority Boundary

```text
M7 advisory intelligence
!=
direct uncontrolled mutation of M6 state
```

---

# 79. Definition of Done for the Overall Platform

AIDAR should ultimately be considered production-ready only when it can:

1. Accept multiple application types through adapters.
2. Normalize them into generic workloads.
3. Validate resource and dependency requirements.
4. Maintain a distributed worker registry.
5. Persist critical control-plane state.
6. Select eligible workers using explainable placement.
7. Resolve content-addressed dependencies.
8. Avoid duplicate transfers.
9. Execute in isolated workspaces.
10. Run real application runtimes.
11. Detect runtime failure.
12. Detect worker failure.
13. Recover eligible workloads.
14. Verify outputs.
15. Commit outputs into CAS.
16. Provide durable artifact identity.
17. Expose health and metrics.
18. Secure control/data-plane communication.
19. Preserve application/core isolation.
20. Survive adversarial and concurrent workloads.
21. Scale beyond a localhost-only demonstration.
22. Produce truthful validation results.

---

# 80. Current Repository Boundary

The uploaded repository demonstrates substantial implementation across:

```text
scene intelligence
dependency analysis
visibility
smart packaging
CAS
distributed workers
placement
execution
runtime adapters
M7 intelligence
durable state
observability
authentication
TLS support
adversarial tests
```

The repository also contains historical/import-stage material and prior milestone artifacts.

When continuing development, the active source tree must be treated as authoritative rather than historical copies stored under import-stage directories.

---

# 81. Engineering Rules for Future Development

Before changing code:

1. Read the existing contract.
2. Search for existing abstractions.
3. Do not create duplicate implementations.
4. Preserve application/core boundaries.
5. Add tests before weakening behavior.
6. Prefer real execution over simulated success.
7. Use explicit simulation mode.
8. Preserve backward compatibility where practical.
9. Keep failure states observable.
10. Validate resource cleanup.
11. Validate concurrency.
12. Validate security boundaries.
13. Run architecture leakage scans.
14. Run regression suites.
15. Record environment-gated tests honestly.

---

# 82. Final System Picture

The final AIDAR concept can be visualized as:

```text
                         ┌─────────────────────┐
                         │       USERS         │
                         │ Apps / API / CLI    │
                         └──────────┬──────────┘
                                    |
                                    v
                         ┌─────────────────────┐
                         │ APPLICATION ADAPTER │
                         │ Blender / LLM / ML  │
                         │ Video / Future Apps │
                         └──────────┬──────────┘
                                    |
                                    v
                         ┌─────────────────────┐
                         │   WORKLOAD SPEC     │
                         │ generic contract    │
                         └──────────┬──────────┘
                                    |
                     ┌──────────────┼──────────────┐
                     |              |              |
                     v              v              v
                 Admission       M7 Intel      Validation
                     |              |              |
                     +──────────────┼──────────────+
                                    v
                         ┌─────────────────────┐
                         │ M6 PLACEMENT ENGINE │
                         │ resources + locality│
                         └──────────┬──────────┘
                                    |
                                    v
                         ┌─────────────────────┐
                         │ M5 CAS / DATA PLANE │
                         │ hashes / transfer   │
                         └──────────┬──────────┘
                                    |
                                    v
                  ┌────────────────────────────────────┐
                  │              WORKERS               │
                  │                                    │
                  │  Worker A   Worker B   Worker C   │
                  │    CAS        CAS        CAS       │
                  │    CPU/GPU    CPU/GPU    CPU/GPU   │
                  │      |          |          |       │
                  │   Runtime    Runtime    Runtime   │
                  └────────────────┬───────────────────┘
                                   |
                                   v
                         ┌─────────────────────┐
                         │ OUTPUT VERIFICATION │
                         │ SHA-256 + CAS       │
                         └──────────┬──────────┘
                                    |
                                    v
                         ┌─────────────────────┐
                         │ ARTIFACTS / RESULTS │
                         └─────────────────────┘

                    M8 OPERATIONS SURROUND EVERYTHING
              persistence + security + health + metrics
```

---

# 83. One-Sentence Definition

> **AIDAR is a generic, content-addressed, resource-aware distributed execution platform that converts application-specific work into verified workloads, intelligently places them across heterogeneous workers, synchronizes only the data required, executes them in isolation, verifies their outputs, and continuously observes and recovers the system.**

---

# 84. Important Source-Derivation Note

This PRD is grounded primarily in the uploaded AIDAR repository and its architecture/validation documents.

Where the repository documents an implemented capability, it is described as part of the current system.

Where this document describes future Phase-10+ capabilities, those sections are explicitly roadmap proposals and must not be interpreted as claims that those features already exist.

The repository's own validation documents should remain the authority for exact test counts and environment-specific validation status.
