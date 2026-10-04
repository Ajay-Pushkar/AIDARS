# AIDAR Phase 13 PRD

## Platform Expansion

**Architectural source:**
`AIDAR_IMPLEMENTATION_CONTINUATION_PRD_M8_TO_M13.md`\
**Predecessor:** Phase 12 Distributed Intelligence\
**Verified baseline:** `a0a7dddb4a0f403b00da9f9d3b8df18a81723de8`\
**Proposed implementation milestones:** M19.1--M19.10

## 1. Purpose

Phase 13 generalizes AIDAR from a distributed execution engine into a
general workload platform.

The architectural source defines M13 platform expansion around
application adapters for Blender, LLM inference, ML training,
FFmpeg/video, custom CPU workloads, custom GPU workloads, and future
workloads.

The core rule is:

> Applications understand application semantics. AIDAR Core understands
> workload orchestration.

The generic flow is:

``` text
Application
    ↓
Application Adapter
    ↓
Generic Workload Contract
    ↓
Job / Workload / Attempt
    ↓
Hard Validation
    ↓
M11 Scheduling
    ↓
M12 Advisory Intelligence
    ↓
Execution / CAS / Verification / Recovery
    ↓
Artifacts / Job Result
```

## 2. Phase 12 Baseline

Phase 12 is closed and verified at commit
`a0a7dddb4a0f403b00da9f9d3b8df18a81723de8`.

Verified capabilities include historical execution data,
feature/data-quality contracts, duration prediction, failure prediction,
behavior classification, anomaly detection, capacity forecasting, M7→M11
advisory integration, deterministic fallback, security, and measured
intelligence API performance.

M18.4 Resource Prediction remains explicitly deferred because
authoritative per-attempt resource-consumption history does not yet
exist. Phase 13 must not fabricate that history.

## 3. Architectural Boundary

### Adapter owns

-   application input interpretation
-   application validation
-   application-specific packaging
-   application-specific resource requirements
-   application-specific execution command
-   expected-output semantics
-   output validation
-   application-specific checkpoint semantics
-   sensitive application metadata handling

### Core owns

-   Job identity
-   Workload identity
-   Attempt identity
-   worker registration
-   eligibility
-   placement
-   queueing
-   CAS
-   asset transfer
-   execution isolation
-   retry and recovery
-   artifact identity/lifecycle
-   persistence
-   observability
-   security

No adapter may create a second scheduler, CAS, telemetry system,
persistence system, or execution engine.

## 4. Phase 13 Non-Goals

Phase 13 must not automatically become:

-   a Blender-only system
-   a Kubernetes clone
-   a second scheduler
-   a second CAS
-   a second telemetry system
-   a deep-learning platform merely for appearance
-   HA without an explicit coordination design
-   federation without trust/routing semantics
-   multi-tenancy without ownership/isolation semantics
-   distributed ML training without collective-communication semantics
-   model-parallel inference without explicit distributed inference
    semantics
-   a reason to replace SQLite without measured requirements

## 5. Target Architecture

``` text
USER / CLI / API / SDK
          ↓
     CONTROL PLANE
          ↓
         JOB
          ↓
 APPLICATION ADAPTER
          ↓
      WORKLOADS
          ↓
    HARD VALIDATION
          ↓
    M11 PLACEMENT
          ↓
 M12 ADVISORY INTELLIGENCE
          ↓
       WORKER
          ↓
   RUNTIME EXECUTION
          ↓
   OUTPUT VERIFICATION
          ↓
        CAS
          ↓
      ARTIFACT
          ↓
   JOB AGGREGATION
          ↓
       RESULT
```

## 6. M19.1 Platform Architecture Investigation

Before implementation, inspect:

``` text
WorkloadSpec
WorkloadRecord
Job / JobRegistry
AttemptRecord
WorkerResourceProfile
PlacementEngine
WorkloadRegistry
WorkloadOrchestrator
ExecutionManager
RuntimeAdapter
CAS
ArtifactRegistry
CoordinatorStateStore
retry
checkpoint
API
CLI
BlenderAdapter
M7/M12 bridge
```

Also inspect current application boundaries, resource models, security,
observability, deployment, and real-runtime test infrastructure.

Produce an investigation report classifying every capability as:

``` text
IMPLEMENTED
PARTIAL
ARCHITECTED
MISSING
DEFERRED
```

Do not modify architecture during the investigation.

## 7. M19.2 Generic Application Adapter Contract

Formalize a reusable adapter boundary. The exact API must be derived
from the existing repository.

Conceptual capabilities:

``` text
validate(input)
describe_resources(input)
package(input)
create_workloads(input)
build_execution_spec(workload)
describe_expected_outputs(workload)
verify_outputs(workload, outputs)
describe_metadata(input)
```

The adapter converts application semantics into generic AIDAR semantics.

The core must not need to understand what a `.blend`, model checkpoint,
prompt, or video timeline means.

## 8. M19.3 Real Blender Workload

This is the first major real-application proof.

Required flow:

``` text
.blend + dependencies
        ↓
Blender Adapter
        ↓
Job / Workloads
        ↓
AIDAR placement
        ↓
Real Worker
        ↓
CAS staging
        ↓
Real Blender binary
        ↓
Real rendered frames
        ↓
Output ingestion
        ↓
Hash verification
        ↓
Artifact
        ↓
Job completion
```

Evidence must include real input, real executable, real worker
execution, real outputs, real hashes, real artifact registration, and
real Job completion.

Constructing a Blender command is not equivalent to executing Blender.

If Blender is unavailable, classify the validation as
`SKIPPED / ENVIRONMENT-GATED`, not PASS.

## 9. M19.4 Resource Observation and GPU Contract

Keep these distinct:

``` text
REQUESTED RESOURCE
OBSERVED RESOURCE
PREDICTED RESOURCE
```

Potential observed metrics:

``` text
CPU utilization
RAM peak
GPU utilization
VRAM peak
GPU identity
execution duration
transfer bytes
```

Only collect metrics that can be measured reliably.

Generic GPU requirements may include:

``` text
GPU required
vendor
model
minimum VRAM
compute capability
driver/runtime compatibility
device count
```

Do not implement M18.4 prediction merely because resource observation
becomes available. First establish authoritative observation.

## 10. M19.5 LLM Inference Adapter

Represent local LLM inference using generic workloads.

Possible inputs:

``` text
model hash
prompt
generation parameters
context limits
precision
GPU/RAM requirements
```

The adapter owns LLM semantics. The core owns assets, resources,
execution, recovery, and artifacts.

Prompts may be sensitive. Do not log prompt contents by default.

No claim of distributed/model-parallel inference without explicit
implementation and validation.

## 11. M19.6 ML Adapter

Initial scope should be single-worker training/inference:

``` text
code
dataset
model
configuration
GPU requirements
checkpoint
metrics
```

The core handles placement, asset staging, execution, checkpointing,
artifact collection, retry, and recovery.

Do not claim distributed ML training. That requires additional semantics
for ranks, process groups, synchronization, topology, checkpoint
coordination, and failure handling.

## 12. M19.7 FFmpeg / Video Adapter

Represent:

``` text
video
audio
images
fonts
codec settings
resolution
frame range
output format
```

The adapter owns FFmpeg semantics.

The core owns execution orchestration.

Do not automatically partition arbitrary video jobs. Frame-independent
operations may partition safely, while temporal/stateful operations may
not.

Partitionability must be declared by the adapter.

## 13. M19.8 Custom CPU/GPU Workloads

Support a generic custom execution contract:

``` text
command
input assets
resource requirements
environment
expected outputs
verification policy
```

Possible uses include Python, C++, simulation, image processing, and
custom GPU programs.

Arbitrary command execution must not imply arbitrary privilege.
Sandboxing and resource controls remain mandatory.

## 14. M19.9 External Interfaces

Potential interfaces:

``` text
REST API
CLI
Python SDK
Web UI
```

All clients must converge on the same control-plane contracts.

No client may implement a separate scheduler.

Public abstractions should center on:

``` text
Job
Workload
Attempt
Artifact
Worker
```

Possible route families:

``` text
POST /jobs
GET /jobs/{id}
GET /jobs/{id}/workloads
GET /jobs/{id}/artifacts
POST /jobs/{id}/cancel
GET /workloads/{id}
GET /attempts/{id}
GET /workers
GET /artifacts/{id}
```

Exact routes must be reconciled with existing API conventions.

## 15. M19.10 Final Audit

Independently verify:

-   adapter boundaries
-   real workload execution
-   GPU/resource contracts
-   artifact correctness
-   multi-worker execution
-   security
-   API behavior
-   observability
-   performance
-   restart/recovery
-   test classification

Every capability must be classified:

``` text
VERIFIED
IMPLEMENTED
PARTIAL
DEFERRED
UNVERIFIED
NOT IMPLEMENTED
ENVIRONMENT-GATED
```

Do not collapse these into one green status.

## 16. Real-Workload Validation Matrix

  Workload        Evidence
  --------------- ----------------------------------------------
  Blender         Real render + real artifact
  LLM inference   Real model inference on worker
  ML              Real training/inference within defined scope
  FFmpeg          Real encode/export
  Custom CPU      Real executable
  Custom GPU      Real GPU execution where hardware exists

Environment-dependent tests must be explicitly classified.

## 17. Multi-Worker Real Application Test

A strong Phase 13 proof is:

``` text
Coordinator
   ├── Worker A
   ├── Worker B
   └── Worker C
```

Example:

``` text
Job: render_scene
frames: 1..120

Workload A: 1..40
Workload B: 41..80
Workload C: 81..120
```

Evidence:

``` text
different workers execute chunks
assets transfer correctly
outputs are retained
all frames are accounted for
artifacts are registered
Job becomes COMPLETED
```

A localhost-only synthetic test is insufficient for this claim.

## 18. Resource/Dataflow Model

The canonical distinction is:

``` text
requested → eligibility
observed → historical evidence
predicted → advisory intelligence
```

Prediction can never override hard resource eligibility.

Long-term loop:

``` text
Requested
   ↓
Eligibility
   ↓
Execution
   ↓
Observed Usage
   ↓
Durable History
   ↓
M12 Prediction
   ↓
M11 Advisory Ranking
```

## 19. Failure Flow

``` text
Adapter failure → JOB REJECTED

Placement failure → QUEUED / UNSCHEDULABLE

Transfer failure → retry policy

Worker failure
    ↓
Attempt classification
    ├── checkpoint → resume
    ├── restartable → retry
    └── exhausted → fail

Process success
    ↓
Output verification
    ├── invalid → verification failure
    └── valid → artifact

All required workloads complete
    ↓
JOB COMPLETED
```

## 20. Security

Phase 13 expands the attack surface.

Threats include:

``` text
malicious workload
malicious model/media
path traversal
command injection
oversized metadata
resource exhaustion
artifact access abuse
prompt leakage
credential leakage
worker impersonation
```

Preserve:

``` text
fail-closed authentication
TLS verification
constant-time token comparison
explicit insecure-mode opt-in
secret redaction
authorization
```

Do not weaken authentication for local testing.

## 21. Resource Isolation

Where supported, enforce:

``` text
CPU limits
RAM limits
GPU selection
process timeout
disk/workspace limits
network policy
environment isolation
```

Separate workspaces alone do not constitute complete isolation.

## 22. Observability

Correlate:

``` text
trace_id
job_id
workload_id
attempt_id
worker_id
artifact_id
adapter
application type
```

through:

``` text
submission
placement
transfer
execution
verification
completion
```

Sensitive application data must not automatically enter logs.

## 23. Performance

Use the established:

``` text
MEASURE
BASELINE
PROFILE
OPTIMIZE
RE-MEASURE
```

methodology.

Measure separately:

``` text
adapter packaging
job creation
workload creation
placement
asset transfer
execution
artifact ingestion
verification
job aggregation
API latency
```

Do not claim scalability without measurements.

## 24. Testing Contract

Every future milestone should contain the relevant combination of:

``` text
unit
integration
failure
concurrency
security
real-runtime
environment-gated
performance
```

Test statuses must be:

``` text
PASS
FAIL
SKIPPED / ENVIRONMENT-GATED
FLAKY
NOT EXECUTED
```

Never report environment-gated validation as PASS.

## 25. No-Fake-Validation Rule

These are different claims:

``` text
construct Blender command != execute Blender
copy ML dataset != train model
start LLM subprocess != perform inference
mock FFmpeg != real encode
```

Phase 13 must distinguish:

``` text
ARCHITECTED
IMPLEMENTED
MOCKED
ENVIRONMENT-GATED
PHYSICALLY VALIDATED
```

## 26. Multi-Tenancy Boundary

Multi-tenancy is not automatically required for initial Phase 13
closure.

Before adding:

``` text
Tenant
Project
Role
Quota
```

define:

``` text
identity
ownership
authorization
quota
resource isolation
artifact isolation
secret isolation
```

A tenant ID alone is not isolation.

## 27. Federation Boundary

Federation is future work.

Before implementing it, define:

``` text
cluster identity
trust
authentication
job routing
artifact routing
asset locality
failure semantics
policy ownership
```

Each cluster should remain independently functional.

## 28. Kubernetes / Infrastructure Boundary

Kubernetes, PostgreSQL, Redis, Kafka, RabbitMQ, Terraform, object
storage, and similar infrastructure are optional.

Before adding any infrastructure dependency, document:

``` text
problem
existing solution
gap
benefit
operational cost
failure behavior
migration impact
testing impact
```

Do not add infrastructure because it is fashionable or familiar.

## 29. Critical Invariants

Phase 13 inherits all prior AIDAR invariants.

Additional invariants:

**P13-I1:** Application semantics remain behind adapters.

**P13-I2:** Generic core does not interpret application-specific data
unnecessarily.

**P13-I3:** Adapters cannot bypass hard worker eligibility.

**P13-I4:** Adapters cannot create a second CAS.

**P13-I5:** Adapters cannot create a second scheduler.

**P13-I6:** Process success is not artifact success.

**P13-I7:** Artifact identity remains content-derived.

**P13-I8:** Requested and observed resources remain distinct.

**P13-I9:** Observed and predicted resources remain distinct.

**P13-I10:** Predictions cannot override hard eligibility.

**P13-I11:** Sensitive application metadata is not logged by default.

**P13-I12:** Environment-gated validation is never reported as
successful execution.

**P13-I13:** Application partitioning must be declared safe by the
adapter.

**P13-I14:** Distributed execution does not imply distributed
application semantics.

**P13-I15:** Multi-worker execution does not imply distributed ML
training.

**P13-I16:** Multiple coordinators do not imply HA.

**P13-I17:** Multiple tenants do not imply isolation.

**P13-I18:** Object storage does not replace content-derived artifact
identity.

## 30. Proposed Milestone Structure

Because the current repository uses M18.x for Phase 12, this PRD
proposes:

``` text
M19.1  Platform Architecture Investigation
M19.2  Generic Application Adapter Contract
M19.3  Real Blender Adapter / E2E
M19.4  Resource Observation / GPU Contract
M19.5  LLM Inference Adapter
M19.6  ML Adapter
M19.7  FFmpeg / Video Adapter
M19.8  Custom CPU/GPU Workloads
M19.9  External API / SDK / CLI convergence
M19.10 Phase 13 Final Audit
```

This is a proposed implementation decomposition, not a replacement for
the conceptual M13.1--M13.7 terminology in the architectural source.

## 31. Definition of Done

Phase 13 is closed only when the repository can demonstrate, within
explicitly defined scope:

-   generic application adapter contract
-   existing Blender adapter compatibility
-   at least one real Blender workload where environment permits
-   verified real artifacts
-   generic GPU/resource requirements
-   distinct requested/observed/predicted resource concepts
-   no fabricated resource history
-   LLM adapter implemented or explicitly deferred/environment-gated
-   ML adapter implemented within defined scope
-   FFmpeg adapter implemented within defined scope
-   custom CPU path validated
-   custom GPU path validated where hardware exists
-   multi-worker real application execution
-   coherent Job/Workload/Attempt/Artifact relationships
-   preserved retry/checkpoint/recovery semantics
-   absolute M11 hard constraints
-   advisory M12 intelligence
-   preserved security boundaries
-   converged API/CLI/SDK contracts
-   no duplicate scheduler/CAS/telemetry/persistence
-   measured performance
-   correctly classified tests
-   clean diff
-   final audit with limitations and deferred capabilities

Phase 13 must not claim distributed ML training, model-parallel
inference, HA, federation, or multi-tenancy unless actually implemented
and validated.

## 32. Future-Sight

Phase 13 should create the instrumentation and adapter boundary for
later capabilities:

``` text
real resource observation
    ↓
durable resource history
    ↓
future M12 resource prediction

asset usage history
    ↓
locality intelligence

workload fingerprints
    ↓
better duration/resource prediction

adapter ecosystem
    ↓
third-party workload support

control-plane API
    ↓
web/CLI/SDK ecosystem

real multi-worker execution
    ↓
future HA/federation/autoscaling work
```

The key is to build the foundations that make these capabilities
possible without prematurely implementing them.

## 33. Final Architecture

``` text
                         USER
                          |
             +------------+------------+
             |            |            |
            CLI          API          SDK
             |            |            |
             +------------+------------+
                          |
                    CONTROL PLANE
                          |
                         JOB
                          |
                    APPLICATION
                       ADAPTER
                          |
                    WORKLOADS
                          |
              +-----------+-----------+
              |                       |
        HARD VALIDATION          M12 ADVISORY
              |                       |
              +-----------+-----------+
                          |
                     M11 PLACEMENT
                          |
                     DISTRIBUTION
                          |
             +------------+------------+
             |            |            |
          Worker A     Worker B     Worker C
             |            |            |
          Runtime       Runtime      Runtime
             |            |            |
             +------------+------------+
                          |
                      CAS / DATA
                          |
                    VERIFIED OUTPUT
                          |
                       ARTIFACT
                          |
                    JOB AGGREGATION
                          |
                        RESULT
```

## 34. Immediate Antigravity Action

Do not begin with `/goal complete Phase 13`.

Begin with:

``` text
/plan

Read AIDAR_PHASE13_PRD.md completely before making changes.

Phase 12 is closed at commit:
a0a7dddb4a0f403b00da9f9d3b8df18a81723de8

Investigate the current repository against Phase 13.

Do NOT modify code yet.

Map:
1. Job / Workload / Attempt / Artifact architecture
2. Application adapter architecture
3. BlenderAdapter capabilities
4. RuntimeAdapter boundaries
5. Resource and GPU models
6. CAS and artifact flow
7. M11 hard constraints
8. M12 integration
9. API / CLI / SDK
10. Persistence and recovery
11. Security
12. Observability
13. Docker and physical-worker validation
14. Real-runtime test infrastructure
15. Blender / GPU / FFmpeg / ML / LLM environment readiness

Classify every Phase 13 capability:
IMPLEMENTED
PARTIAL
ARCHITECTED
MISSING
DEFERRED

Explicitly identify which real workloads can already execute today versus which are only architected.

Do not create:
- another scheduler
- another CAS
- another telemetry system
- another persistence system
- another execution engine

Preserve:
HARD CORRECTNESS > SCHEDULING PREFERENCE > PREDICTION

and:
APPLICATION SEMANTICS → ADAPTER
GENERIC ORCHESTRATION → CORE

Produce:
A. Architecture map
B. Capability matrix
C. Gap analysis
D. Reusable components
E. Real workload readiness
F. GPU/resource readiness
G. Security gaps
H. API gaps
I. Performance risks
J. Minimal implementation sequence
K. Decisions requiring approval
L. Recommended M19.1 boundary

STOP after the investigation report.
Do not begin M19.2.
```

The first Phase 13 milestone is therefore **investigation, not
implementation**. The phase is deliberately wider than Phase 12, so the
architecture needs a map before the machinery starts moving.
