# AIDAR IMPLEMENTATION CONTINUATION PRD
## Post-5.2C Execution Roadmap: M8 Completion → M10 → M13

**Document type:** Implementation PRD / Engineering Execution Contract  
**Companion to:** `AIDAR_MASTER_PRD_PHASE10.md`  
**Current verified baseline:** commit `de5964c`  
**Branch:** `claude/aidar-leiqvc`  
**Purpose:** Continue AIDAR from the exact point where implementation stopped, without re-implementing completed foundations, silently changing contracts, or allowing implementation agents to invent architecture.

---

# 1. READ THIS FIRST

This document is **not a replacement for the Master PRD**.

The Master PRD defines:

- what AIDAR is
- the overall architecture
- the application/core boundary
- M1–M8 architecture
- security principles
- failure model
- critical invariants
- long-term M10–M13 direction

This document defines:

- where implementation currently stops
- what is already complete and therefore must NOT be rebuilt
- what remains incomplete
- the exact continuation sequence
- architectural constraints for future work
- acceptance criteria
- testing requirements
- Claude/implementation-agent operating rules

## The most important rule

> **Do not restart AIDAR from the beginning. Continue from the verified repository state.**

The current repository is the implementation source of truth.

The Master PRD is the architectural source of truth.

This document is the continuation contract between them.

---

# 2. CURRENT IMPLEMENTATION CHECKPOINT

## 2.1 Baseline

The continuation starts at:

```text
Commit: de5964c
Branch: claude/aidar-leiqvc
Status: clean after Phase 5.2C
```

Phase 5.2C completed coordinator startup recovery.

The current recovery path is:

```text
Coordinator starts
      |
      v
Restore persisted workers
      |
      v
Workers restored as OFFLINE
      |
      v
Restore persisted workloads
      |
      v
Identify non-terminal workloads
      |
      v
Coordinator begins normal health/eviction loops
      |
      v
Workers re-register / heartbeat
      |
      v
Recovered workloads are re-driven through
the existing workload dispatcher
```

Recovery semantics are currently **at-least-once**.

A workload may execute again after a coordinator crash if the coordinator did not durably record its completion before the crash.

That is an accepted current limitation.

---

# 3. COMPLETED FOUNDATIONS: DO NOT REIMPLEMENT

The following are existing foundations.

Future work must integrate with them.

It must not recreate parallel versions.

## 3.1 M1–M4

Already established:

```text
M1 canonical scene intelligence
M2 dependency analysis
M3 spatial / visibility analysis
M4 smart packaging
```

The Blender adapter already uses the M1–M4 pipeline.

Do not create a second scene analysis pipeline.

Do not create a second packaging implementation.

---

## 3.2 M5 CAS

The content-addressed storage layer already exists.

The identity rule is:

```text
SHA256(content) = asset identity
```

Do not introduce filename-based asset identity.

Do not create a second CAS.

Do not bypass CAS for distributed asset synchronization.

---

## 3.3 M6 Worker and Execution Foundation

Already established:

```text
WorkerRegistry
WorkloadRegistry
WorkerResourceProfile
PlacementEngine
DistributedWorker
WorkerServer
DistributedClient
ExecutionManager
WorkloadOrchestrator
```

Existing placement constraints and worker lifecycle behavior must be preserved.

In particular:

```text
can_execute_workloads = false
        =>
worker cannot be selected for compute
```

Do not invent another worker eligibility mechanism.

---

## 3.4 M7

M7 remains advisory.

The boundary is:

```text
M7 observes
    |
    v
M7 predicts / recommends
    |
    v
M6 / control plane decides
```

M7 must not become a hidden second scheduler.

---

## 3.5 M8 Durable State Foundation

Already implemented:

```text
CoordinatorStateStore
worker persistence
workload persistence
placement persistence
execution-result persistence
SQLite WAL
registry write-through persistence
```

The registry persistence pattern is established.

Do not replace it with a new persistence mechanism during continuation work unless a later milestone explicitly requires migration.

---

## 3.6 M8 Worker Recovery

Already implemented in Phase 5.2C:

```text
persisted worker
      |
      v
OFFLINE
      |
      v
fresh heartbeat / registration
      |
      v
ACTIVE
```

The stale-heartbeat restoration issue has already been addressed.

Do not remove the OFFLINE trust boundary.

---

## 3.7 M8 Workload Recovery

Already implemented:

```text
persisted non-terminal workload
        |
        v
restored
        |
        v
re-driven through existing dispatcher
```

Do not create a second workload dispatch path.

---

# 4. WHAT IS ACTUALLY MISSING NOW

The remaining work is not "build distributed execution."

Distributed execution already exists.

The next problem is:

> **Turn the existing workload-level execution system into a coherent production job/result/recovery system.**

The most important gaps are:

```text
Job identity
      |
      v
Workload/chunk relationship
      |
      v
Attempt/retry semantics
      |
      v
Job completion semantics
      |
      v
Output verification
      |
      v
Artifact lifecycle
      |
      v
Stronger recovery
```

The system currently has workload-level results, but a production system needs a higher-level concept that answers:

```text
What user operation does this workload belong to?
How many workloads belong to it?
Which ones completed?
Which outputs are expected?
Is the entire operation complete?
What happens when one chunk fails?
What happens after coordinator restart?
What artifacts belong to the completed operation?
```

---

# 5. IMMEDIATE CONTINUATION: M8 COMPLETION SEMANTICS

Before beginning broad M10 work, close the remaining semantic gap between workload execution and user-level completion.

This is intentionally a continuation of the existing M8 foundation.

Do not call it a new distributed architecture.

---

# 6. M8.1 JOB IDENTITY INVESTIGATION

## Objective

Determine the smallest clean way to introduce a first-class Job concept without breaking `WorkloadSpec`, `WorkloadRecord`, or existing dispatch behavior.

## Required investigation

Inspect:

```text
WorkloadSpec
WorkloadRecord
WorkloadRegistry
WorkloadOrchestrator
ScheduledChunk
SceneEngine
BlenderAdapter
WorkloadExecutionResult
CoordinatorStateStore
```

Also search every current use of:

```text
chunk_index
input_path
workload_id
output_asset_hashes
```

## Do not assume

Do not assume that:

```text
input_path = job identity
```

Do not assume that:

```text
chunk_index = permanent workload identity
```

Do not infer Job identity from filenames.

## Expected design

The eventual model should conceptually become:

```text
Job
 |
 +---- Workload
 |
 +---- Workload
 |
 +---- Workload
 |
 +---- Workload
```

A Job represents the user-level operation.

A Workload represents one executable unit.

For a Blender render:

```text
Job: render_scene_001

Workload A: frames 1-100
Workload B: frames 101-200
Workload C: frames 201-300
```

The exact schema must be derived from the current code before implementation.

---

# 7. M8.2 JOB REGISTRY

## Objective

Introduce a durable registry for Job-level state only after the Job contract has been investigated.

Possible responsibilities:

```text
create job
get job
list jobs
update job state
associate workload IDs
record expected outputs
record completed outputs
record failure summary
```

The Job registry must not duplicate WorkloadRegistry responsibilities.

### Boundary

```text
JobRegistry
    |
    +--> user-level operation
    |
    +--> workload membership
    |
    +--> aggregate state
    |
    +--> completion policy

WorkloadRegistry
    |
    +--> executable unit
    |
    +--> placement
    |
    +--> execution result
    |
    +--> individual failure
```

---

# 8. M8.3 DURABLE JOB STATE

Job state must survive coordinator restart.

Conceptual states:

```text
SUBMITTED
RUNNING
PARTIALLY_COMPLETED
COMPLETED
FAILED
CANCELLED
```

The exact state machine must be derived before implementation.

Do not introduce states merely because they look useful.

The key rule is:

```text
Job state must be derived from durable workload facts.
```

It must not be a second independent truth source.

---

# 9. M8.4 WORKLOAD AGGREGATION

A Job must know:

```text
total workloads
completed workloads
failed workloads
pending workloads
running workloads
```

Example:

```text
Job
total = 10

completed = 7
running = 2
failed = 1
```

The Job must not report `COMPLETED`.

The aggregation policy must be explicit.

Possible policies include:

```text
ALL_REQUIRED
ANY_SUCCESS
THRESHOLD
BEST_EFFORT
```

For rendering, the default should generally be equivalent to:

```text
ALL_REQUIRED
```

unless the application adapter explicitly defines another completion policy.

---

# 10. M8.5 RESULT PERSISTENCE

The current implementation already persists `WorkloadExecutionResult`.

The continuation must not replace that.

Instead:

```text
WorkloadExecutionResult
        |
        v
WorkloadRegistry
        |
        v
Job aggregation
```

A workload result remains the authoritative result for that workload.

The Job aggregates those results.

---

# 11. M8.6 OUTPUT VERIFICATION

A successful process exit is not enough.

The future completion pipeline must distinguish:

```text
PROCESS SUCCESS
```

from:

```text
VALID ARTIFACT
```

The required evidence should eventually be:

```text
process succeeded
AND
expected outputs exist
AND
outputs are readable/valid
AND
output hashes are computed
AND
output hashes are committed
AND
expected output coverage is satisfied
```

This is an extension of the existing No False Success invariant.

Do not weaken the invariant to simplify implementation.

---

# 12. M8.7 FRAME COVERAGE

For workloads that represent frame ranges, expected coverage must eventually be explicit.

Example:

```text
Expected:
1..500

Actual:
1..500

Result:
COMPLETE
```

Example:

```text
Expected:
1..500

Actual:
1..497, 499, 500

Result:
INCOMPLETE
```

A successful renderer process must not hide missing frames.

The frame model must remain generic enough that non-Blender workloads do not inherit Blender-specific assumptions.

Therefore frame coverage belongs in an application-specific completion contract or generic artifact/output contract, not hardcoded into the generic scheduler.

---

# 13. M8.8 ARTIFACT MODEL

An Artifact represents a verified output.

Conceptual fields:

```text
artifact_id
content_hash
size
media/type information
producer_workload_id
producer_job_id
created_at
verification_state
storage_location
```

The exact model must be derived from existing CAS and result structures.

Do not create a second hash model.

The artifact must ultimately reference the existing CAS identity.

---

# 14. M8.9 ARTIFACT LIFECYCLE

Future artifact states may include:

```text
CREATED
VERIFIED
AVAILABLE
REFERENCED
EXPIRED
GC_ELIGIBLE
DELETED
```

These states must only be added if the implementation requires them.

The essential rule is:

```text
unreferenced artifact
        |
        v
eligible for garbage collection
```

not:

```text
old artifact
        |
        v
delete immediately
```

Artifacts referenced by active Jobs or durable user records must remain available.

---

# 15. M8.10 RECOVERY AFTER JOB INTRODUCTION

Coordinator restart recovery must be extended carefully.

Current behavior:

```text
restore workload
redrive non-terminal workload
```

Future behavior:

```text
restore Job
      |
      v
restore Workloads
      |
      v
reconstruct Job aggregate
      |
      v
identify incomplete workloads
      |
      v
redrive eligible workloads
      |
      v
recalculate Job state
```

The Job must not blindly persist an independently contradictory state.

---

# 16. M8.11 AT-LEAST-ONCE SEMANTICS

The current system must remain honest.

After coordinator failure:

```text
worker may have completed
coordinator may not know
recovery may redispatch
```

Therefore:

```text
AIDAR does not yet guarantee exactly-once execution.
```

Do not claim exactly-once semantics until there is an explicit execution-attempt model and corresponding validation.

---

# 17. M9 SECURITY / ADVERSARIAL CONTINUATION

M9 is not a reason to rebuild security.

Existing authentication/TLS/adversarial foundations must be audited and extended.

Focus:

```text
job authorization
artifact authorization
worker identity
recovery abuse
duplicate submission
replay
malformed job payloads
oversized metadata
CAS access control
secret redaction
```

Tests must include:

```text
invalid token
expired/invalid credential
unauthorized artifact retrieval
unauthorized workload inspection
malformed recovery state
replayed request
duplicate submission
worker impersonation attempt
```

Security failures must remain explicit.

---

# 18. M10: PRODUCTION DISTRIBUTED EXECUTION

M10 begins only after the M8 job/result semantic foundation is stable.

## M10 goals

```text
stronger recovery
retry attempts
checkpointing
artifact lifecycle
multi-worker deployment
execution observability
failure injection
production deployment
```

M10 is not:

```text
rewrite M1-M8
```

---

# 19. M10.1 EXECUTION ATTEMPTS

Introduce an explicit attempt model.

Conceptually:

```text
Logical Workload
      |
      +--> Attempt 1
      |
      +--> Attempt 2
      |
      +--> Attempt 3
```

Each attempt should record:

```text
attempt_id
workload_id
attempt_number
worker_id
started_at
finished_at
status
failure_reason
execution_result
```

The logical Workload ID remains stable.

Do not create a new logical workload for every retry.

---

# 20. M10.2 RETRY POLICY

Retry decisions must be explicit.

Possible retryable failures:

```text
worker crash
network interruption
temporary CAS failure
temporary resource exhaustion
```

Possible non-retryable failures:

```text
invalid input
invalid dependency
missing executable
invalid workload
deterministic application error
```

The exact classifications must be implemented through explicit policy.

No generic:

```text
except Exception:
    retry
```

behavior is permitted.

---

# 21. M10.3 RETRY BUDGET

Each workload may eventually have:

```text
max_attempts
```

or an equivalent policy.

When retry budget is exhausted:

```text
WORKLOAD FAILED
```

must be recorded explicitly.

The system must not retry indefinitely.

---

# 22. M10.4 CHECKPOINTING

Checkpointing must be capability-based.

A runtime may advertise:

```text
supports_checkpointing = true
```

or equivalent.

A runtime that cannot safely checkpoint must not pretend it can.

Conceptual flow:

```text
Execution
   |
   v
Checkpoint
   |
   v
CAS
   |
   v
Worker failure
   |
   v
Restore checkpoint
   |
   v
Resume
```

Checkpoint integrity must use content-addressed identity.

---

# 23. M10.5 CHECKPOINT SAFETY

A checkpoint must not be accepted merely because a file exists.

Required evidence should eventually include:

```text
checkpoint exists
checkpoint hash valid
checkpoint metadata valid
checkpoint belongs to workload
checkpoint version compatible
checkpoint is restorable
```

Where runtime-specific validation is required, the adapter/runtime owns that knowledge.

---

# 24. M10.6 WORKER FAILURE RECOVERY

Future worker failure flow:

```text
heartbeat loss
      |
      v
worker unavailable
      |
      v
identify affected attempts
      |
      v
classify attempt state
      |
      +---- checkpoint available --> resume
      |
      +---- restartable ----------> retry
      |
      +---- non-restartable ------> fail
```

The recovery policy must remain explicit.

---

# 25. M10.7 DRAINING

A worker can be marked:

```text
DRAINING
```

meaning:

```text
accept no new work
allow existing work to finish
or recover it according to policy
```

Draining must not be equivalent to OFFLINE.

Conceptually:

```text
ACTIVE
  |
  v
DRAINING
  |
  +--> existing workloads finish
  |
  +--> new workloads rejected
  |
  v
OFFLINE
```

---

# 26. M10.8 MULTI-WORKER VALIDATION

AIDAR must eventually be tested with multiple real workers.

Minimum topology:

```text
Coordinator
    |
    +---- Worker A
    |
    +---- Worker B
    |
    +---- Worker C
```

Validation must include:

```text
worker registration
heartbeats
placement
asset synchronization
execution
result reporting
worker failure
recovery
artifact retrieval
```

A localhost-only test is insufficient evidence for multi-worker production readiness.

---

# 27. M10.9 CONTROL PLANE / DATA PLANE SCALING

The coordinator should remain responsible for control decisions.

Large data should move through CAS/data-plane paths where possible.

Avoid:

```text
worker
   |
   v
coordinator
   |
   v
worker
```

for every large artifact.

Prefer:

```text
worker A
   |
   +------> worker B
            CAS transfer
```

when the existing architecture permits it.

---

# 28. M10.10 EXECUTION OBSERVABILITY

Each execution attempt should eventually expose:

```text
queued duration
placement duration
asset synchronization duration
startup duration
execution duration
output ingestion duration
verification duration
total duration
```

This enables identification of:

```text
scheduler bottleneck
network bottleneck
CAS bottleneck
runtime bottleneck
storage bottleneck
```

---

# 29. M10.11 PRODUCTION DEPLOYMENT

The system must eventually support reproducible deployment.

Possible deployment layers:

```text
Docker
Docker Compose
systemd
Kubernetes
cloud VMs
bare-metal nodes
```

The deployment mechanism must be chosen from actual requirements.

No Kubernetes migration is implied by this PRD.

---

# 30. M10.12 INFRASTRUCTURE AS CODE

Production infrastructure should eventually be reproducible.

Infrastructure definitions may cover:

```text
network
machines
firewall
TLS
secrets
storage
monitoring
coordinator
workers
```

Infrastructure code must not contain application-specific secrets.

---

# 31. M10.13 HIGH AVAILABILITY

A single coordinator remains a potential SPOF.

HA is a later production capability.

Before implementation, explicitly design:

```text
leader election
state ownership
concurrent writes
worker registration ownership
in-flight workload ownership
failover
split-brain prevention
```

Do not run two coordinators against the same SQLite database and call that HA.

---

# 32. M10.14 DATABASE EVOLUTION

SQLite remains valid for the current architecture.

A future PostgreSQL migration is permitted only when requirements justify it.

Possible triggers:

```text
multiple coordinator instances
high concurrent writes
HA requirement
large state volume
remote durable database requirement
```

Migration must preserve:

```text
worker state
workload state
placement
execution results
job state
recovery semantics
```

---

# 33. M11: ADVANCED SCHEDULING

M11 improves scheduling while preserving hard correctness constraints.

The scheduler may consider:

```text
CPU
RAM
GPU
VRAM
worker health
asset locality
queue depth
historical duration
deadline
priority
cost
affinity
anti-affinity
```

But hard constraints remain absolute.

Example:

```text
Predicted fastest worker
        |
        v
missing required GPU
        |
        v
INELIGIBLE
```

Prediction cannot override resource validity.

---

# 34. M11.1 EXPLAINABLE PLACEMENT

Every placement decision should eventually be explainable.

Example:

```text
Selected Worker-07 because:

CPU requirement: satisfied
RAM requirement: satisfied
GPU requirement: satisfied
VRAM requirement: satisfied
asset locality: available
worker health: acceptable
queue depth: low
predicted duration: low
```

The exact scoring model may evolve.

The explanation must remain factual.

---

# 35. M11.2 DATA LOCALITY

Placement should eventually consider the cost of moving assets.

Conceptually:

```text
Worker A:
required assets = 100% local

Worker B:
required assets = 20% local

Worker C:
required assets = 0% local
```

If all satisfy hard resource constraints, locality may influence ranking.

Locality must never override resource requirements.

---

# 36. M11.3 HETEROGENEOUS GPU SCHEDULING

Support future worker differences:

```text
GPU vendor
GPU model
VRAM
compute capability
driver/runtime compatibility
```

The workload contract should express requirements generically.

The core must not contain application-specific GPU assumptions.

---

# 37. M11.4 QUEUE OPTIMIZATION

AIDAR should eventually support explicit queues.

Possible concepts:

```text
priority
fairness
aging
deadline
resource class
tenant quota
```

The scheduler must avoid starvation.

---

# 38. M11.5 AFFINITY / ANTI-AFFINITY

Workloads may eventually request:

```text
same worker
same rack
same GPU class
different workers
same data locality domain
```

These are scheduling preferences or constraints and must be explicitly represented.

Do not encode affinity through arbitrary worker names.

---

# 39. M11.6 DEADLINE-AWARE SCHEDULING

Workloads may declare:

```text
deadline
```

The scheduler may optimize for deadline satisfaction.

Deadline-aware scheduling must not violate:

```text
resource constraints
security constraints
worker eligibility
dependency requirements
```

---

# 40. M12: DISTRIBUTED INTELLIGENCE

M12 extends predictive intelligence across the cluster.

Potential capabilities:

```text
resource prediction
duration prediction
failure prediction
capacity forecasting
anomaly detection
workload behavior models
adaptive policy recommendations
```

M12 does not replace deterministic safety rules.

---

# 41. M12.1 HISTORICAL EXECUTION DATA

Potential learning features:

```text
workload type
requested resources
actual resources
execution duration
queue time
asset transfer time
worker
success/failure
retry count
```

Historical data must be validated before becoming training data.

---

# 42. M12.2 PREDICTION SAFETY

A prediction may say:

```text
Worker A likely faster
```

but cannot override:

```text
Worker A lacks required resources
```

Hard rules remain deterministic.

---

# 43. M12.3 ADAPTIVE CAPACITY PLANNING

AIDAR may eventually estimate:

```text
expected workload volume
expected resource demand
expected queue growth
expected failure rate
```

and recommend capacity changes.

Recommendations must remain observable and explainable.

---

# 44. M12.4 M7/M12 AUTHORITY BOUNDARY

The intelligence layer may:

```text
observe
predict
rank
recommend
```

The control plane remains responsible for:

```text
admit
place
execute
recover
reject
```

Any automated mutation must have an explicit policy contract.

---

# 45. M13: PLATFORM EXPANSION

M13 generalizes AIDAR into a platform.

Potential adapters:

```text
Blender
LLM inference
ML training
FFmpeg/video
custom CPU
custom GPU
future workloads
```

The generic pipeline remains:

```text
Application
    |
    v
Adapter
    |
    v
WorkloadSpec
    |
    v
Generic orchestration
```

---

# 46. M13.1 LLM ADAPTER

An LLM adapter may translate:

```text
model
prompt
generation parameters
GPU requirement
RAM requirement
model hash
```

into generic workload requirements.

Prompts must be treated as potentially sensitive metadata.

The core must not interpret prompt semantics.

---

# 47. M13.2 ML ADAPTER

An ML adapter may represent:

```text
training code
dataset
model
checkpoint
configuration
GPU requirements
```

The core sees:

```text
assets
resources
execution
outputs
```

not ML-specific semantics.

---

# 48. M13.3 VIDEO ADAPTER

A video adapter may represent:

```text
input media
codec
resolution
frame range
encoding parameters
GPU/CPU requirements
```

The adapter owns FFmpeg-specific interpretation.

The generic core owns execution orchestration.

---

# 49. M13.4 EXTERNAL API

Eventually AIDAR may expose:

```text
REST API
CLI
Python SDK
Web UI
```

All should converge on the same control-plane contracts.

No client should implement its own scheduler.

---

# 50. M13.5 USER MANAGEMENT

Potential concepts:

```text
User
Tenant
Project
Role
Permission
Quota
```

Authentication and authorization must remain separate concepts.

Do not introduce multi-tenancy until resource ownership semantics are defined.

---

# 51. M13.6 MULTI-TENANT ISOLATION

Future multi-tenancy may require:

```text
tenant-scoped jobs
tenant-scoped assets
tenant-scoped artifacts
quotas
authorization
resource isolation
secret isolation
```

Adding a tenant ID alone does not establish isolation.

---

# 52. M13.7 CLUSTER FEDERATION

Future federation may connect:

```text
Cluster A
     |
Federation
     |
Cluster B
```

Federation must define:

```text
trust
authentication
job routing
asset movement
policy
failure handling
```

Each cluster should remain independently functional.

---

# 53. API / DATABASE / INFRASTRUCTURE TECHNOLOGY POLICY

The following technologies are optional:

```text
PostgreSQL
Redis
Kafka
RabbitMQ
Docker
Kubernetes
Terraform
Prometheus
OpenTelemetry
Nginx
object storage
```

None are mandatory simply because they are industry-standard technologies.

Before adding one, implementation must document:

```text
problem
existing solution
gap
benefit
operational cost
failure behavior
migration impact
testing impact
```

---

# 54. SECURITY CONTINUATION

Security-sensitive operations include:

```text
worker registration
worker inventory
workload submission
job inspection
asset streaming
artifact retrieval
control-plane state
credentials
TLS private keys
sensitive workload metadata
```

Future security work must preserve:

```text
fail-closed authentication
TLS verification
constant-time token comparison
explicit insecure-mode opt-in
secret redaction
```

Do not weaken authentication to simplify local testing.

---

# 55. RATE LIMITING

Future API protection may include rate limits for:

```text
workload submission
authentication
artifact retrieval
asset retrieval
worker registration
```

Worker heartbeat traffic must have separate treatment so rate limiting cannot accidentally cause healthy workers to be declared dead.

---

# 56. CACHING

AIDAR must distinguish:

```text
cache
```

from:

```text
authoritative CAS
```

Cache eviction must never destroy the authoritative identity of an artifact.

Possible future caches:

```text
worker-local asset cache
metadata cache
placement cache
API response cache
```

Any cache must have an explicit invalidation policy.

---

# 57. OBSERVABILITY

Future production observability should expose:

```text
logs
metrics
traces
```

Core identifiers should eventually be correlated:

```text
trace_id
job_id
workload_id
attempt_id
worker_id
artifact_id
```

Sensitive values must be redacted.

---

# 58. CORE METRICS

Future metrics may include:

```text
jobs_submitted_total
jobs_completed_total
jobs_failed_total

workloads_submitted_total
workloads_completed_total
workloads_failed_total

workload_queue_seconds
placement_seconds
asset_sync_seconds
execution_seconds
verification_seconds

worker_active_count
worker_offline_count
worker_draining_count

cas_hits_total
cas_misses_total
asset_transfer_bytes

recovery_events_total
retry_attempts_total
checkpoint_restore_total
```

Metrics must represent real observations.

---

# 59. DISTRIBUTED TRACING

A future trace should be able to represent:

```text
API request
    |
    v
Job creation
    |
    v
Workload creation
    |
    v
Placement
    |
    v
Asset synchronization
    |
    v
Execution
    |
    v
Output ingestion
    |
    v
Verification
    |
    v
Artifact commit
```

Tracing must be observational.

It must not alter workload semantics.

---

# 60. TESTING CONTRACT

Every future phase must contain:

```text
unit tests
integration tests
failure tests
concurrency tests where relevant
security tests where relevant
real-runtime tests where available
environment-gated tests where required
```

A test must not be weakened merely to make a new implementation pass.

---

# 61. TEST CLASSIFICATION

Every test result must be classified as one of:

```text
PASS
FAIL
SKIPPED / ENVIRONMENT-GATED
FLAKY
NOT EXECUTED
```

Do not report environment-gated validation as success.

---

# 62. FAILURE INJECTION

Future production validation must intentionally test:

```text
coordinator crash
worker crash
network interruption
partial transfer
CAS corruption
disk exhaustion
database failure
TLS failure
authentication failure
invalid workload
runtime timeout
duplicate submission
concurrent submission
```

Expected behavior must be defined before execution.

---

# 63. LOAD TESTING

Scale testing should progressively increase:

```text
1 worker
2 workers
5 workers
10 workers
25 workers
50 workers
100+
```

Actual limits must be measured.

Record:

```text
throughput
latency
CPU
RAM
database load
network load
CAS load
recovery time
```

Never claim scalability based solely on architecture diagrams.

---

# 64. PRODUCTION READINESS

AIDAR is not production-ready merely because the unit-test suite is green.

Production readiness eventually requires evidence for:

```text
correctness
recovery
security
multi-worker operation
real-runtime execution
observability
deployment
data integrity
failure handling
scalability
```

---

# 65. IMPLEMENTATION AGENT RULES

This section is mandatory for Claude or any coding agent.

## Rule 1: Investigate first

Before coding:

```text
search
inspect
trace callers
inspect tests
inspect state transitions
inspect persistence
```

## Rule 2: Do not guess

If an architecture decision is unclear:

```text
STOP
REPORT
PROPOSE OPTIONS
WAIT
```

## Rule 3: Do not rewrite working foundations

No broad refactoring unless explicitly approved.

## Rule 4: Do not create duplicate abstractions

Search first.

## Rule 5: Do not change semantics silently

Report every state-machine or contract change.

## Rule 6: Do not add infrastructure casually

Every dependency requires justification.

## Rule 7: Do not weaken tests

Fix the implementation or prove the test is incorrect.

## Rule 8: Do not claim untested behavior

Separate:

```text
implemented
tested
environment-gated
planned
```

## Rule 9: Keep changes narrow

One phase should have one coherent responsibility.

## Rule 10: Preserve clean architecture

Application-specific behavior remains behind adapters.

---

# 66. REQUIRED CLAUDE WORKFLOW

For every phase:

```text
STEP 1
Repository investigation

STEP 2
Architecture findings

STEP 3
Gap analysis

STEP 4
Minimal implementation proposal

STOP FOR APPROVAL

STEP 5
Implementation

STEP 6
Targeted tests

STEP 7
Affected regression tests

STEP 8
Review diff

STEP 9
Document remaining gaps

STEP 10
Commit
```

Do not begin the next phase automatically.

---

# 67. CLOUD SESSION EFFICIENCY RULES

Because implementation is being performed through cloud coding sessions:

Do not repeatedly run the entire test suite after every small modification.

Preferred sequence:

```text
small change
   ↓
targeted tests
   ↓
related regression tests
   ↓
full suite once at phase boundary
```

Avoid prompts such as:

```text
fix everything
clean the whole repository
make all tests pass
finish M10
```

Prefer narrow tasks:

```text
Investigate Job identity only.
Do not modify code.
```

then:

```text
Implement the approved Job identity design.
```

then:

```text
Run targeted Job tests and directly affected registry tests.
```

---

# 68. PHASE ORDER

The intended continuation is:

```text
CURRENT
de5964c
   |
   v
M8.1 Job investigation
   |
   v
M8.2 Job contract
   |
   v
M8.3 Job persistence
   |
   v
M8.4 Workload aggregation
   |
   v
M8.5 Output verification
   |
   v
M8.6 Artifact model/lifecycle
   |
   v
M9 Security/adversarial extension
   |
   v
M10.1 Execution attempts
   |
   v
M10.2 Retry policy
   |
   v
M10.3 Checkpointing
   |
   v
M10.4 Worker failure recovery
   |
   v
M10.5 Multi-worker deployment
   |
   v
M10.6 Production observability
   |
   v
M11 Advanced scheduling
   |
   v
M12 Distributed intelligence
   |
   v
M13 Platform expansion
```

The exact sub-phase breakdown may be adjusted after repository investigation, but no phase should skip the required architectural review.

---

# 69. DO NOT BUILD YET

The following are intentionally deferred until their prerequisite architecture is established:

```text
Kafka
RabbitMQ
Redis
PostgreSQL migration
Kubernetes
Terraform
multi-region federation
full microservice split
exactly-once execution
automatic ML scheduler mutation
multi-tenancy
cluster federation
```

They are possible future tools/capabilities, not immediate requirements.

---

# 70. GLOBAL NON-GOALS

AIDAR must not become:

```text
a generic Kubernetes replacement
a generic message broker
a generic database
a generic filesystem
an opaque AI scheduler
a Blender-only monolith
a fake benchmark system
a system that hides failures
```

The core mission remains:

```text
intelligent distributed workload
+
asset orchestration
+
resource-aware execution
+
verified artifacts
+
durable recovery
```

---

# 71. FINAL SUCCESS MODEL

The eventual complete pipeline is:

```text
USER / APPLICATION
        |
        v
APPLICATION ADAPTER
        |
        v
JOB
        |
        +----------------------+
        |                      |
        v                      v
WORKLOAD 1                WORKLOAD N
        |                      |
        v                      v
VALIDATION               VALIDATION
        |                      |
        +----------+-----------+
                   |
                   v
              PLACEMENT
                   |
                   v
           DEPENDENCY SYNC
                   |
                   v
              EXECUTION
                   |
                   v
              ATTEMPT
                   |
                   v
             OUTPUT DISCOVERY
                   |
                   v
              VERIFICATION
                   |
                   v
              OUTPUT CAS
                   |
                   v
               RESULT
                   |
                   v
          JOB AGGREGATION
                   |
                   v
          ARTIFACT AVAILABLE
                   |
                   v
       DURABLE STATE + OBSERVABILITY
```

Recovery can enter at appropriate points without destroying the logical Job identity.

---

# 72. FINAL AIDAR INVARIANTS

These must remain true through M13.

```text
I1  Invalid resource requirements cannot be placed.

I2  Missing dependencies cannot be silently ignored.

I3  CAS identity is content-derived.

I4  Duplicate transfers must be controlled by existing CAS mechanisms.

I5  Successful process exit does not automatically mean successful Job.

I6  Outputs must be verified before authoritative artifact completion.

I7  Restored workers remain OFFLINE until fresh liveness proof.

I8  M7/M12 intelligence cannot silently override M6 hard constraints.

I9  Application-specific logic remains behind adapters.

I10 Durable state must not be silently discarded.

I11 Recovery must remain observable.

I12 Retry must have explicit limits.

I13 Infrastructure dependencies must be justified.

I14 Security failures must fail closed.

I15 Environment-gated tests must not be reported as passed.

I16 Future milestones must extend rather than casually replace validated foundations.
```

---

# 73. FINAL IMPLEMENTATION CONTRACT

The project is considered correctly continued only when the implementation agent follows this rule:

> **AIDAR must evolve by extending verified contracts, not by replacing them with the agent's preferred architecture.**

The implementation agent must always distinguish:

```text
CURRENT
IMPLEMENTED
VALIDATED
PLANNED
PROPOSED
```

These are different categories.

A planned capability must not be reported as implemented.

An architectural proposal must not be treated as an approved design.

A passing unit test must not be treated as proof of production deployment.

A localhost integration test must not be treated as proof of physical multi-worker scalability.

A successful process must not be treated as proof of verified artifact correctness.

---

# 74. FIRST TASK AFTER THIS DOCUMENT IS GIVEN TO CLAUDE

Claude must NOT start implementing M10.

Claude must first perform:

## "M8 Completion Boundary Audit"

Read the repository at the current commit and report:

1. Current `WorkloadSpec` contract.
2. Current `WorkloadRecord` contract.
3. Current `WorkloadExecutionResult`.
4. Current chunk representation.
5. Every location where chunk identity is represented.
6. Every location where `output_asset_hashes` are produced and stored.
7. Current result persistence path.
8. Current coordinator recovery path.
9. Current CAS artifact representation.
10. Existing output verification behavior.
11. Existing workload states actually reachable in code.
12. Whether a Job abstraction already partially exists.
13. Smallest safe Job abstraction.
14. Required persistence changes.
15. Required tests.
16. Any contradictions between the Master PRD and active source tree.

### Critical restriction

During this audit:

```text
DO NOT MODIFY SOURCE CODE.
DO NOT REFACTOR.
DO NOT ADD DEPENDENCIES.
DO NOT CREATE DATABASE TABLES.
DO NOT CHANGE TESTS.
DO NOT COMMIT.
```

Only produce the architecture report.

The report must end with:

```text
CURRENT IMPLEMENTATION
REMAINING GAP
PROPOSED MINIMAL DESIGN
FILES AFFECTED
TEST PLAN
RISKS
QUESTIONS / DECISIONS REQUIRED
```

Only after review and approval should implementation begin.

---

# 75. END STATE

The ultimate AIDAR architecture is:

```text
                    USERS / APPS
                         |
                         v
                APPLICATION ADAPTERS
                         |
                         v
                       JOB
                         |
               +---------+---------+
               |         |         |
               v         v         v
           WORKLOAD   WORKLOAD   WORKLOAD
               |         |         |
               +---------+---------+
                         |
                         v
                VALIDATION / ADMISSION
                         |
                         v
                RESOURCE-AWARE M6
                    PLACEMENT
                         |
                         v
                 CAS / DATA PLANE
                         |
                         v
                  EXECUTION
                         |
                         v
               ATTEMPTS / RETRIES
                         |
                         v
                 CHECKPOINTING
                         |
                         v
               OUTPUT VERIFICATION
                         |
                         v
                    OUTPUT CAS
                         |
                         v
                    ARTIFACTS
                         |
                         v
                 JOB AGGREGATION
                         |
                         v
              DURABLE CONTROL STATE
                         |
                         v
             OBSERVABILITY / SECURITY
                         |
                         v
              M11 SCHEDULING INTELLIGENCE
                         |
                         v
              M12 DISTRIBUTED INTELLIGENCE
                         |
                         v
                M13 PLATFORM LAYER
```

The foundation is already built.

The continuation is about making the foundation **coherent at job level, recoverable at attempt level, verifiable at artifact level, scalable at cluster level, and extensible at platform level.**

**Do not rebuild the foundation. Continue from `de5964c`.**
