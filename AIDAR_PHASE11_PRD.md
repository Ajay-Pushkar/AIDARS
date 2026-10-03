# AIDAR Phase 11 PRD

## Advanced Scheduling

**Status:** Planning / Architecture Definition\
**Previous phase:** Phase 10, Production Distributed Execution\
**Phase 10 closure commit:** `558faf1`

------------------------------------------------------------------------

## 1. Executive Purpose

Phase 11 evolves AIDAR from a validated distributed execution engine
into a system capable of making richer, explainable scheduling
decisions.

The existing Phase 10 foundations must remain intact. Phase 11 does not
rebuild:

-   Coordinator/Worker architecture
-   WorkerRegistry
-   WorkloadRegistry
-   PlacementEngine foundations
-   CAS
-   dependency staging
-   execution runtime
-   retry/recovery
-   checkpoint/resume
-   artifact verification
-   worker lifecycle
-   observability
-   physical distributed execution
-   real Blender E2E execution

The source roadmap defines Phase 11 as **Advanced Scheduling**, focused
on:

-   predictive placement
-   queue optimization
-   heterogeneous GPU scheduling
-   cost-aware placement
-   workload affinity
-   anti-affinity
-   deadline-aware scheduling

The scheduler may consider:

``` text
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

The fundamental rule is:

``` text
HARD CONSTRAINTS
      >
SCHEDULING PREFERENCES
      >
PREDICTIONS
```

A worker that violates a hard requirement is never selectable merely
because its ranking score is attractive.

------------------------------------------------------------------------

# 2. Source-of-Truth and Change Discipline

Authoritative material:

1.  Current repository implementation
2.  Existing Phase 10 implementation and tests
3.  `AIDAR_IMPLEMENTATION_CONTINUATION_PRD_M8_TO_M13.md`
4.  `AIDAR_MASTER_PRD_PHASE10.md`
5.  Existing architecture invariants
6.  Phase 10 closure evidence

If this PRD and the repository disagree:

``` text
STOP
  ↓
investigate
  ↓
document discrepancy
  ↓
propose reconciliation
  ↓
obtain approval
```

Never silently replace an existing contract.

Every milestone follows:

``` text
Repository investigation
        ↓
Architecture findings
        ↓
Gap analysis
        ↓
Minimal implementation proposal
        ↓
STOP FOR APPROVAL
        ↓
Implementation
        ↓
Targeted tests
        ↓
Affected regressions
        ↓
Diff review
        ↓
Documentation
        ↓
Commit
```

------------------------------------------------------------------------

# 3. Phase 10 Baseline

Phase 10 is closed.

The validated execution pipeline is:

``` text
Application workload
      ↓
WorkloadSpec
      ↓
hard validation
      ↓
worker eligibility
      ↓
placement
      ↓
CAS dependency synchronization
      ↓
worker staging
      ↓
real subprocess execution
      ↓
output harvesting
      ↓
output CAS
      ↓
verification
      ↓
COMPLETED
```

A real Blender E2E workload has already passed through this pipeline.

Phase 11 therefore changes the **decision quality before dispatch**, not
the execution foundation after dispatch.

------------------------------------------------------------------------

# 4. Phase 11 Problem Statement

Phase 10 can answer:

> "Which workers are currently eligible?"

Phase 11 must increasingly answer:

> "Among the eligible workers, which one should receive this workload,
> and when should it run?"

Conceptual flow:

``` text
Workload
   ↓
Validate hard requirements
   ↓
Filter ineligible workers
   ↓
Eligible candidate set
   ↓
Collect scheduling features
   ↓
Apply scheduling policy
   ↓
Rank candidates
   ↓
Explain decision
   ↓
Select worker
   ↓
Existing dispatcher
```

If no worker is currently usable:

``` text
Workload
   ↓
No currently usable capacity
   ↓
Queue
   ↓
Resource/worker event
   ↓
Re-evaluate
   ↓
Rank
   ↓
Dispatch
```

------------------------------------------------------------------------

# 5. Core Architectural Boundary

The generic core must remain application-independent.

``` text
Application
   ↓
Application Adapter
   ↓
Generic WorkloadSpec
   ↓
Control Plane
   ├── validation
   ├── eligibility
   ├── advanced scheduling
   └── placement
   ↓
Worker
   ↓
Runtime
```

Do not implement:

``` python
if blender:
    ...
if pytorch:
    ...
if llama:
    ...
```

The scheduler receives generic requirements such as:

``` text
CPU
RAM
GPU
VRAM
assets
priority
deadline
affinity
anti-affinity
cost policy
locality policy
```

Application-specific interpretation remains inside adapters.

------------------------------------------------------------------------

# 6. Hard Constraints vs Soft Preferences

## 6.1 Hard constraints

Examples:

``` text
required CPU
required RAM
required GPU
required VRAM
GPU compatibility
worker execution capability
worker health
security requirements
dependency availability
```

If a worker fails any hard constraint:

``` text
worker = INELIGIBLE
```

## 6.2 Soft scheduling factors

Examples:

``` text
asset locality
queue depth
historical duration
resource headroom
deadline pressure
priority
cost
affinity preference
anti-affinity preference
GPU suitability
```

Soft factors can change ranking only among eligible candidates.

------------------------------------------------------------------------

# 7. Existing Placement Must Be Extended, Not Rebuilt

Existing eligibility semantics must survive.

In particular:

``` text
can_execute_workloads = false
        ↓
worker cannot receive compute workloads
```

Do not create an independent worker registry or second dispatcher.

Preferred structure:

``` text
Existing hard eligibility
        ↓
Existing eligible candidates
        ↓
Phase 11 feature collection
        ↓
Phase 11 ranking
        ↓
Existing dispatch path
```

Repository investigation must determine whether the current
`PlacementEngine` should be extended directly or decomposed minimally.

------------------------------------------------------------------------

# 8. Logical Phase 11 Components

The logical architecture is:

``` text
                  WorkloadSpec
                       |
                       v
              +------------------+
              | Hard Validator   |
              +--------+---------+
                       |
                eligible workers
                       |
                       v
              +------------------+
              | Feature Collector|
              +--------+---------+
                       |
                       v
              +------------------+
              | Scheduling Policy|
              +--------+---------+
                       |
                       v
              +------------------+
              | Candidate Ranker |
              +--------+---------+
                       |
                       v
              +------------------+
              | Explanation      |
              +--------+---------+
                       |
                       v
                Placement Decision
                       |
                       v
                Existing Dispatcher
```

These are responsibilities, not mandatory classes.

Do not create abstractions just to match the diagram.

------------------------------------------------------------------------

# 9. Scheduling Data Model

Potential workload scheduling attributes:

``` text
priority
deadline
estimated_duration
affinity
anti_affinity
cost_policy
resource_class
GPU requirements
locality policy
queue policy
```

Potential worker scheduling attributes:

``` text
health
CPU availability
RAM availability
GPU inventory
VRAM availability
GPU vendor
GPU model
compute capability
driver/runtime compatibility
queue depth
active workload count
asset locality
cost metadata
scheduling domain
```

For every new field:

1.  Search for an existing representation.
2.  Determine whether it is already persisted.
3.  Determine whether it is ephemeral or durable.
4.  Define validation.
5.  Define serialization.
6.  Add tests.
7.  Avoid duplicate models.

Do not add every possible field in one milestone.

------------------------------------------------------------------------

# 10. M11.1 Explainable Placement

Every placement decision should eventually be explainable.

Example:

``` text
Selected worker-07.

Hard constraints:
  CPU: satisfied
  RAM: satisfied
  GPU: satisfied
  VRAM: satisfied
  health: acceptable

Ranking factors:
  asset locality: high
  queue depth: low
  predicted duration: low
  cost class: standard
  affinity: satisfied
```

The explanation must be factual.

Do not claim:

``` text
Worker-07 is objectively fastest
```

unless actual measured evidence supports that statement.

A better statement is:

``` text
Worker-07 ranked higher under the active scheduling policy.
```

Possible decision metadata:

``` text
decision_id
workload_id
candidate_workers
eligible_workers
rejected_workers
rejection_reasons
ranking_features
selected_worker
policy_version
timestamp
```

Only persist fields that have an actual durability requirement.

------------------------------------------------------------------------

# 11. M11.2 Data Locality

Placement should eventually consider the cost of moving required assets.

Concept:

``` text
Worker A: 100% local
Worker B:  20% local
Worker C:   0% local
```

If all satisfy hard constraints, locality can influence ranking.

It cannot override:

``` text
GPU requirement
VRAM requirement
CPU requirement
RAM requirement
security
worker health
dependency requirements
```

Required locality information may include:

``` text
required asset hashes
local asset hashes
local asset count
local asset bytes
missing asset count
missing asset bytes
```

A possible measured feature is:

``` text
locality_ratio =
local_required_bytes / total_required_bytes
```

This is a design option, not an existing repository contract.

Do not pull large artifacts through the coordinator simply to calculate
locality.

------------------------------------------------------------------------

# 12. Locality Cost

A future cost model may estimate:

``` text
transfer cost =
missing bytes / estimated transfer rate
```

A richer model may later include:

``` text
network path
worker transfer capacity
historical transfer rate
CAS availability
```

Do not introduce predictive transfer models before measuring available
signals.

Start with facts that can be observed.

------------------------------------------------------------------------

# 13. M11.3 Heterogeneous GPU Scheduling

Workers may differ in:

``` text
GPU vendor
GPU model
VRAM
compute capability
driver version
runtime compatibility
device count
utilization
availability
```

Workload requirements may include:

``` text
GPU required
minimum VRAM
vendor constraint
model constraint
minimum compute capability
runtime compatibility
GPU count
```

The core must treat these as generic resource constraints.

It must not contain:

``` text
Blender GPU rules
PyTorch GPU rules
LLM GPU rules
```

------------------------------------------------------------------------

# 14. GPU Eligibility Example

``` text
Workload:
GPU required = true
VRAM required = 12 GB

Worker A:
no GPU
→ INELIGIBLE

Worker B:
8 GB available VRAM
→ INELIGIBLE

Worker C:
16 GB available VRAM
→ ELIGIBLE

Worker D:
24 GB available VRAM
→ ELIGIBLE
```

If C and D are eligible, scheduling factors may rank them.

A higher score can never rescue A or B.

------------------------------------------------------------------------

# 15. GPU Compatibility

Compatibility checks must be explicit and testable.

Possible requirements:

``` text
vendor
model
compute capability
driver/runtime compatibility
```

Do not infer compatibility from arbitrary model-name string matching.

Use normalized capability metadata.

If physical GPUs are unavailable during CI, distinguish:

``` text
simulated resource-profile validation
```

from:

``` text
physical GPU validation
```

A simulated GPU test is not evidence of physical GPU execution.

------------------------------------------------------------------------

# 16. M11.4 Queue Optimization

When no eligible worker is currently available:

``` text
Submit
  ↓
Validate
  ↓
No currently usable capacity
  ↓
Queue
  ↓
Capacity changes
  ↓
Re-evaluate
  ↓
Rank
  ↓
Dispatch
```

Do not immediately convert temporary resource exhaustion into permanent
workload failure.

First inspect existing workload states.

Do not introduce `QUEUED` if existing `PENDING` already provides the
required semantics without ambiguity.

Any state-machine change requires explicit review.

------------------------------------------------------------------------

# 17. Queue Concepts

Possible queue features:

``` text
priority
fairness
aging
deadline
resource class
quota
```

Quota is recognized as a future scheduling concept, but full
multi-tenancy is outside Phase 11 unless explicitly approved.

The queue must avoid starvation.

------------------------------------------------------------------------

# 18. Priority

Priority must have documented semantics.

Conceptually:

``` text
higher priority
    ↓
earlier scheduling opportunity
```

Priority cannot bypass:

``` text
resource constraints
security
dependencies
worker eligibility
```

Priority also must not automatically starve lower-priority workloads.

------------------------------------------------------------------------

# 19. Fairness and Aging

A conceptual model could be:

``` text
effective priority =
base priority + aging contribution
```

Do not implement this exact formula without investigation.

Define:

-   aging trigger
-   aging rate
-   maximum aging effect
-   interaction with priority
-   interaction with deadlines
-   interaction with resource classes

Fairness behavior must be tested.

------------------------------------------------------------------------

# 20. M11.5 Affinity / Anti-Affinity

Affinity may express:

``` text
same worker
same scheduling domain
same GPU class
same data locality domain
```

Anti-affinity may express:

``` text
avoid same worker
avoid same GPU
avoid same scheduling domain
avoid co-location with workload X
```

Relationships must be represented explicitly.

Do not encode affinity through arbitrary worker names.

Each rule must be classified as:

``` text
hard constraint
or
soft preference
```

------------------------------------------------------------------------

# 21. Scheduling Domains

Future topology metadata may include:

``` text
worker
host
rack
zone
network domain
GPU group
```

Keep the model provider-neutral.

Do not add cloud-specific topology merely because it is common
elsewhere.

------------------------------------------------------------------------

# 22. M11.6 Deadline-Aware Scheduling

A workload may declare:

``` text
deadline
```

The scheduler may use deadline pressure in ranking.

A conceptual feature is:

``` text
slack =
deadline - current_time - predicted_remaining_duration
```

This is a design option, not a mandatory formula.

Deadline handling must never violate:

``` text
resource constraints
security
worker eligibility
dependencies
```

The system must distinguish:

``` text
deadline declared
deadline feasible
deadline at risk
deadline missed
```

Do not claim deadline guarantees unless the system actually provides
them.

------------------------------------------------------------------------

# 23. Historical Duration

Historical execution data may include:

``` text
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

Phase 11 should initially prefer deterministic statistics:

``` text
median runtime
p95 runtime
worker-specific runtime
workload-class runtime
```

Do not introduce ML simply because the roadmap contains the phrase
"predictive placement".

Learned models belong primarily to Phase 12.

------------------------------------------------------------------------

# 24. Cost-Aware Placement

Cost is a scheduling factor, not a hard requirement by default.

Possible cost metadata:

``` text
worker cost class
cloud price
energy estimate
internal accounting unit
resource price
```

Before implementation define:

``` text
what cost means
where it comes from
how fresh it is
how it affects ranking
```

Do not invent fake cost values just to make a benchmark pass.

------------------------------------------------------------------------

# 25. Scheduling Score

A conceptual score may combine:

``` text
locality
queue
duration
resource headroom
priority
deadline pressure
cost
affinity
anti-affinity
GPU suitability
```

The critical ordering is:

``` text
hard eligibility
      ↓
candidate ranking
```

Never:

``` text
global score
      ↓
try to compensate for invalid resources
```

If a weighted score is introduced, its factors must be inspectable.

Avoid hidden constants.

Prefer named policy parameters.

------------------------------------------------------------------------

# 26. Deterministic Tie-Breaking

If two candidates have equivalent ranking:

``` text
score(A) == score(B)
```

use a deterministic tie-breaker.

Possible choices:

``` text
stable worker ID
least recent assignment
lower current queue
```

Choose one deliberately.

Never rely on incidental dictionary ordering as scheduling policy.

------------------------------------------------------------------------

# 27. Scheduling Decision Lifecycle

The complete decision flow:

``` text
1. workload submitted
2. scheduling requirements normalized
3. worker state read
4. hard eligibility evaluated
5. candidate set built
6. scheduling features collected
7. policy applied
8. candidates ranked
9. explanation generated
10. worker selected
11. decision recorded if required
12. existing dispatcher executes
```

------------------------------------------------------------------------

# 28. Queue Re-Evaluation

Queue re-evaluation may be triggered by:

``` text
worker heartbeat/resource update
workload completion
worker registration
worker becomes ACTIVE
worker leaves DRAINING
resource availability change
```

Avoid scheduling storms.

A heartbeat should not necessarily force every queued workload through a
full ranking pass if nothing relevant changed.

------------------------------------------------------------------------

# 29. Resource Freshness

Scheduling decisions are based on observed state.

Potential race:

``` text
scheduler sees 16 GB free VRAM
worker actually has 4 GB free VRAM
```

The system must document telemetry freshness.

If telemetry is approximate:

``` text
latest known state
```

must not be represented as perfect real-time truth.

Where necessary, execution-time resource validation or reservation may
be considered.

------------------------------------------------------------------------

# 30. Resource Reservation

Concurrent scheduling may create:

``` text
Workload A sees 16 GB free
Workload B sees 16 GB free
both reserve it
actual demand = 32 GB
```

Before implementing reservations, investigate:

-   current scheduling lock
-   telemetry frequency
-   concurrent placement
-   dispatch timing
-   persistence transaction boundaries

Only introduce reservations if the current model creates a demonstrated
correctness problem.

------------------------------------------------------------------------

# 31. Control Plane / Data Plane

The coordinator remains responsible for control decisions.

Large artifacts remain in CAS/data-plane paths.

Scheduling should use metadata such as:

``` text
asset hash
asset size
locality
transfer estimate
```

rather than transferring large payloads merely to make a scheduling
decision.

Preferred:

``` text
metadata
  ↓
scheduler
  ↓
worker selection
  ↓
data-plane transfer
```

------------------------------------------------------------------------

# 32. Persistence

Potential durable scheduling information:

``` text
priority
deadline
affinity
anti-affinity
policy
queue state
placement decision
```

Potential ephemeral information:

``` text
current candidate ranking
temporary feature snapshot
current telemetry
```

Do not persist every transient value without a reason.

SQLite remains the current persistence foundation unless a real Phase 11
requirement proves otherwise.

------------------------------------------------------------------------

# 33. Restart Recovery

After coordinator restart:

``` text
restore workers
workers remain OFFLINE
restore workloads
restore required scheduling state
workers re-register
fresh liveness
workers become ACTIVE
non-terminal workloads re-evaluated
```

Preserve:

``` text
restored worker
    ↓
OFFLINE
    ↓
fresh heartbeat
    ↓
ACTIVE
```

Never dispatch to a restored worker merely because it existed in
persistent state.

------------------------------------------------------------------------

# 34. DRAINING Interaction

A DRAINING worker:

``` text
accepts no new work
```

but may finish existing work.

Therefore:

``` text
DRAINING
→ excluded from new-work candidates
```

Do not equate:

``` text
DRAINING == OFFLINE
```

------------------------------------------------------------------------

# 35. Retry Interaction

Scheduling and retry remain separate responsibilities.

``` text
failure
  ↓
retry policy
  ↓
new scheduling decision
  ↓
eligible worker
  ↓
existing dispatcher
```

Do not hardcode retry to the same worker unless explicitly required.

------------------------------------------------------------------------

# 36. Checkpoint Interaction

Checkpointing remains a runtime capability.

A resumed workload is scheduled using:

``` text
checkpoint asset
resource requirements
locality
GPU requirements
deadline
priority
affinity
```

The scheduler must not interpret checkpoint contents.

------------------------------------------------------------------------

# 37. Failure Behavior

Scheduling problems must remain explicit.

Examples:

``` text
invalid resource request
no eligible worker
temporary capacity shortage
stale telemetry
policy conflict
deadline impossible
worker unavailable
```

Do not map every scheduling problem to a generic application failure.

Reuse existing failure taxonomy where possible.

------------------------------------------------------------------------

# 38. API and CLI

Potential future information surfaces:

``` text
submit workload
workload status
queue position
placement explanation
scheduling decision
```

First inspect existing APIs.

Extend existing endpoints when practical.

Do not create duplicate control-plane contracts.

Potential CLI concepts:

``` text
aidar submit
aidar status
aidar explain
aidar queue
```

Only implement those actually required by Phase 11.

No client should implement its own scheduler.

------------------------------------------------------------------------

# 39. Observability

Useful scheduling metrics:

``` text
scheduling_attempts_total
scheduling_success_total
scheduling_blocked_total
scheduling_rejected_total
queue_depth
queue_wait_seconds
placement_decision_seconds
candidate_count
eligible_candidate_count
worker_rejection_count
locality_ratio
predicted_duration
deadline_risk
```

Follow existing repository telemetry conventions.

Do not introduce a second telemetry system.

Useful structured events:

``` text
WORKLOAD_QUEUED
SCHEDULING_STARTED
CANDIDATE_REJECTED
SCHEDULING_DECISION
WORKLOAD_DISPATCHED
WORKLOAD_RESCHEDULED
DEADLINE_RISK_CHANGED
```

Never log sensitive workload data unnecessarily.

------------------------------------------------------------------------

# 40. Testing Strategy

Testing sequence:

``` text
unit
  ↓
integration
  ↓
E2E
  ↓
performance
  ↓
phase audit
```

## Unit tests

Cover:

-   hard eligibility
-   GPU/VRAM eligibility
-   locality
-   ranking
-   priority
-   fairness/aging
-   deadlines
-   affinity
-   anti-affinity
-   cost
-   deterministic tie-breaking
-   explanation generation

## Integration tests

Cover:

-   scheduler + WorkerRegistry
-   scheduler + telemetry
-   scheduler + persistence
-   scheduler + dispatcher
-   scheduler + retry
-   scheduler + checkpoint
-   scheduler + CAS locality

## E2E tests

Cover:

-   multiple workers
-   real CAS
-   queued workload
-   worker resource changes
-   actual dispatch
-   real application workload where appropriate

## Performance tests

Measure:

``` text
candidate count
scheduling latency
queue throughput
concurrent submissions
ranking latency
explanation generation
```

Report:

``` text
min
p50
p95
p99
max
```

------------------------------------------------------------------------

# 41. Required Invariants

### I1: No Invalid Placement

``` text
requirements > available resources
→ worker cannot be selected
```

### I2: No Missing Dependencies

``` text
required dependency missing
→ execution cannot begin
```

### I3: CAS Identity

``` text
SHA256(content) = asset identity
```

### I4: No False Success

``` text
SUCCESS
iff
execution succeeded
AND outputs exist
AND outputs are verified
AND outputs are committed
```

### I5: Worker Trust

``` text
restored worker
→ OFFLINE
until fresh liveness
```

### I6: Intelligence Boundary

``` text
prediction
cannot override
hard constraint
```

### I7: Application Isolation

``` text
generic scheduler
!=
application implementation
```

### I8: Durable State

``` text
durable state
cannot be silently discarded
```

### I9: Environment-Gated Tests

``` text
unavailable hardware/software
→ SKIPPED or NOT VALIDATED
```

Never report an environment-gated test as passed.

------------------------------------------------------------------------

# 42. Performance Methodology

Use:

``` text
MEASURE
  ↓
BASELINE
  ↓
PROFILE
  ↓
OPTIMIZE
  ↓
RE-MEASURE
```

Measure separately:

``` text
eligibility filtering
feature collection
ranking
explanation
queue operations
persistence
dispatch
```

Do not attribute total workload latency to the scheduler without
component measurements.

Suggested simulated candidate populations:

``` text
1
10
50
100
500
1000
```

Suggested worker populations where practical:

``` text
1
10
50
100
500
```

Do not claim production scale from simulation.

------------------------------------------------------------------------

# 43. Complexity and Caching

An initial:

``` text
O(W)
```

worker scan may be perfectly acceptable.

If locality introduces:

``` text
O(W × A)
```

behavior, measure it.

Do not build indexes/caches before proving they are needed.

Potential cacheable information:

``` text
worker capabilities
GPU inventory
asset locality
historical duration
policy configuration
```

Cache invalidation must be explicit.

------------------------------------------------------------------------

# 44. Backward Compatibility

Existing workloads with no Phase 11 scheduling fields must continue to
work.

Conceptually:

``` text
legacy WorkloadSpec
      ↓
default scheduling policy
      ↓
existing eligible-worker behavior
```

New fields should preferably be optional initially.

Migration process:

1.  preserve existing fields
2.  add optional scheduling fields
3.  define defaults
4.  validate
5.  serialize
6.  persist where necessary
7.  add compatibility tests
8.  update adapters only where required

Avoid broad schema rewrites.

------------------------------------------------------------------------

# 45. Security

Scheduling must respect:

-   worker trust state
-   authorization
-   protected asset policies
-   sensitive metadata
-   access controls

Security is a hard constraint.

A cheaper, faster, or more local worker is not selectable if it is
unauthorized.

------------------------------------------------------------------------

# 46. Skills Required

Implementation should deliberately use:

### Python

Scheduling algorithms, models, tests, persistence integration.

### asyncio

Queue processing, concurrency, worker events, non-blocking coordinator
behavior.

### FastAPI / HTTP

Scheduling/status/explanation APIs where needed.

### Pydantic

Scheduling contracts, validation, configuration.

### SQLite

Durable scheduling state where justified.

### Distributed Systems

Consistency, stale telemetry, worker failure, retry, recovery,
idempotency, scheduling races.

### Scheduling Algorithms

Constraint filtering, priority queues, fairness, aging, ranking,
affinity, anti-affinity, deadline scheduling.

### GPU Systems

VRAM, GPU inventory, compute capability, compatibility, utilization,
reservation.

### Networking

Data locality, transfer cost, network capacity, latency.

### Performance Engineering

p50/p95/p99, profiling, throughput, contention, algorithmic complexity.

### Testing

pytest, async testing, integration/E2E tests, failure injection,
deterministic fixtures, benchmarks.

------------------------------------------------------------------------

# 47. Dependency Policy

Use existing dependencies first.

Likely existing stack:

``` text
Python
pytest
pytest-asyncio
httpx
FastAPI
Pydantic
SQLite
Docker
```

Do not introduce:

``` text
Kafka
RabbitMQ
Redis
PostgreSQL
Kubernetes
Terraform
```

unless a specific Phase 11 requirement demonstrates the need.

Every new dependency needs justification.

------------------------------------------------------------------------

# 48. Proposed Milestone Structure

The following is a proposed decomposition, not an existing repository
commitment:

``` text
M17.1 Scheduling Investigation & Contract
        ↓
M17.2 Explainable Placement
        ↓
M17.3 Data Locality
        ↓
M17.4 Heterogeneous GPU Scheduling
        ↓
M17.5 Queue Optimization
        ↓
M17.6 Affinity / Anti-Affinity
        ↓
M17.7 Deadline-Aware Scheduling
        ↓
M17.8 Cost-Aware Placement
        ↓
M17.9 Integrated Scheduler Validation
        ↓
M17.10 Phase 11 Final Audit
```

The first milestone is mandatory before implementation of the later
scheduling features.

------------------------------------------------------------------------

# 49. M17.1 Investigation

The agent must inspect:

``` text
PlacementEngine
WorkloadSpec
WorkerResourceProfile
WorkerRegistry
worker telemetry
dispatcher
persistence
retry
checkpoint
state machine
CAS locality
existing placement tests
```

It must answer:

1.  What hard constraints already exist?
2.  What ranking already exists?
3.  What resource information exists?
4.  How fresh is telemetry?
5.  Is locality already measurable?
6.  Is queueing already present?
7.  Are priority/deadline fields present?
8.  Are affinity concepts present?
9.  Is GPU metadata present?
10. How does retry interact with placement?
11. How does checkpoint resume interact with placement?
12. What scheduling state is durable?
13. What concurrency races exist?
14. What is the smallest architecture extension?

Output:

``` text
Current Architecture
Existing Features
Missing Features
Risks
Proposed Design
Files
Tests
STOP FOR APPROVAL
```

No code changes during this investigation.

------------------------------------------------------------------------

# 50. M17.2 Explainable Placement

Required evidence:

``` text
candidate workers
hard rejection reasons
eligible candidates
ranking factors
selected worker
policy version
```

Test:

``` text
same inputs
+
same worker state
+
same policy
=
same decision
```

unless a documented dynamic input changed.

------------------------------------------------------------------------

# 51. M17.3 Locality

Required evidence:

-   required assets known
-   worker-local assets measurable
-   locality affects eligible-worker ranking
-   locality appears in explanation
-   no large data transfer merely to calculate locality

------------------------------------------------------------------------

# 52. M17.4 GPU

Required evidence:

-   generic GPU capability model
-   VRAM hard constraint
-   compatibility checks
-   heterogeneous worker profiles
-   explainable rejection
-   simulated tests
-   physical GPU validation only where hardware exists

------------------------------------------------------------------------

# 53. M17.5 Queue

Required evidence:

-   queue representation
-   priority semantics
-   fairness semantics
-   starvation tests
-   resource-change re-evaluation
-   concurrent submission behavior
-   restart behavior where queue state is durable

------------------------------------------------------------------------

# 54. M17.6 Affinity

Required evidence:

-   generic affinity representation
-   hard/soft semantics
-   candidate filtering/ranking
-   same-domain or same-worker behavior
-   anti-affinity separation
-   deterministic explanations

Never encode affinity using arbitrary worker names.

------------------------------------------------------------------------

# 55. M17.7 Deadline

Required evidence:

-   deadline representation
-   ranking influence
-   deadline risk if supported
-   queue interaction
-   feasible/infeasible tests
-   no false deadline guarantee

------------------------------------------------------------------------

# 56. M17.8 Cost

Before implementation define:

``` text
cost source
cost unit
freshness
ranking effect
default behavior
```

No fabricated cost measurements.

------------------------------------------------------------------------

# 57. M17.9 Integrated Validation

Test combinations:

``` text
resource constraints
+
GPU
+
locality
+
queue
+
priority
+
deadline
+
affinity
+
anti-affinity
+
cost
```

Critical precedence tests:

``` text
missing GPU
beats
high locality

insufficient RAM
beats
low cost

OFFLINE
beats
high priority

security violation
beats
deadline pressure
```

Expected result: hard invalidity always wins.

------------------------------------------------------------------------

# 58. M17.10 Final Audit

Classify each Phase 11 capability:

``` text
IMPLEMENTED
VALIDATED
PARTIALLY VALIDATED
ARCHITECTED ONLY
DEFERRED
```

Explicitly distinguish:

``` text
simulated worker profiles
real workers
simulated GPU resources
real GPU hardware
generic workloads
real application workloads
```

Do not claim physical GPU scheduling based on mocked telemetry.

------------------------------------------------------------------------

# 59. Definition of Done

Phase 11 can be closed only when the implemented scope has evidence
that:

1.  Hard eligibility constraints remain intact.
2.  Advanced scheduling factors are represented explicitly.
3.  Placement decisions are explainable.
4.  Locality can influence ranking where implemented.
5.  GPU/VRAM scheduling is generic where implemented.
6.  Queue behavior is explicit and tested where implemented.
7.  Priority/fairness behavior is tested.
8.  Affinity/anti-affinity semantics are explicit where implemented.
9.  Deadline semantics are explicit where implemented.
10. Cost-aware scheduling has a documented measurable model where
    implemented.
11. Scheduling remains observable.
12. Required scheduling state survives restart.
13. Retry uses the existing dispatcher.
14. Checkpoint resume uses the existing scheduler.
15. No soft preference overrides a hard constraint.
16. Scheduling performance has been measured.
17. Real and simulated validation are clearly separated.
18. No unnecessary infrastructure dependency was introduced.
19. Phase 10 regressions remain valid.
20. Remaining limitations are documented.

------------------------------------------------------------------------

# 60. Phase 11 Final Architecture

``` text
                 USER / APPLICATION
                         |
                         v
                 APPLICATION ADAPTER
                         |
                         v
                    WorkloadSpec
                         |
                         v
                 HARD VALIDATION
                         |
              +----------+----------+
              |                     |
          INVALID                 VALID
              |                     |
            REJECT                  v
                            WORKER ELIGIBILITY
                                   |
                        +----------+----------+
                        |                     |
                    NONE ELIGIBLE          ELIGIBLE
                        |                     |
                      QUEUE                  v
                                      FEATURE COLLECTION
                                             |
                                             v
                                      POLICY EVALUATION
                                             |
                                             v
                                       CANDIDATE RANKING
                                             |
                                             v
                                        EXPLANATION
                                             |
                                             v
                                     PLACEMENT DECISION
                                             |
                                             v
                                      EXISTING DISPATCH
                                             |
                                             v
                                           WORKER
                                             |
                                             v
                                         RUNTIME
                                             |
                                             v
                                      VERIFIED OUTPUT
```

------------------------------------------------------------------------

# 61. Boundary With Phase 12

Phase 12 is where broader distributed intelligence belongs:

``` text
resource prediction
duration prediction
failure prediction
capacity forecasting
anomaly detection
workload behavior models
adaptive policy recommendations
```

The authority boundary remains:

``` text
Intelligence:
observe
predict
rank
recommend

Control plane:
admit
place
execute
recover
reject
```

A prediction cannot override a hard constraint.

Phase 11 should primarily use deterministic, measurable scheduling
features. Phase 12 can build learning systems on validated historical
data.

------------------------------------------------------------------------

# 62. Boundary With Phase 13

Phase 13 may eventually expand AIDAR with:

``` text
additional application adapters
LLM inference
ML training
FFmpeg/video
external API
user management
multi-tenant isolation
stronger security
cluster federation
```

Phase 11 provides the scheduling substrate for these future workloads.

It must not become an application-specific scheduler.

------------------------------------------------------------------------

# 63. Global Non-Goals

AIDAR must not become:

``` text
a Kubernetes replacement
a generic message broker
a generic database
a generic filesystem
an opaque AI scheduler
a Blender-only monolith
a fake benchmark environment
a system that hides failures
```

Core mission:

``` text
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

------------------------------------------------------------------------

# 64. Final Principle

Phase 10 made distributed execution real.

Phase 11 makes scheduling more capable.

Phase 12 makes intelligence predictive.

Phase 13 expands the platform.

The governing rule is:

``` text
HARD CORRECTNESS
       >
SCHEDULING PREFERENCE
       >
PREDICTION
```

AIDAR can become smarter without becoming less trustworthy.

------------------------------------------------------------------------

# 65. Immediate Next Action

Do not implement the entire phase.

Begin with:

``` text
M17.1 Scheduling Investigation & Contract
```

Use this prompt:

``` text
/plan

M17.1 — Phase 11 Advanced Scheduling Investigation

Do not modify code.

Inspect the current AIDAR repository and determine how the
existing PlacementEngine, WorkerRegistry, WorkloadSpec,
resource models, worker telemetry, dispatcher, persistence,
retry system, checkpoint system, and workload state machine
can be extended for Phase 11 Advanced Scheduling.

Investigate:

- current hard eligibility constraints
- current placement scoring/selection behavior
- current resource model
- CPU/RAM/GPU/VRAM representation
- worker telemetry freshness
- asset locality information
- queue behavior
- workload priority/deadline support
- affinity/anti-affinity support
- cost metadata
- placement explanation support
- scheduling persistence
- concurrent scheduling behavior
- retry interaction
- checkpoint interaction
- worker DRAINING/OFFLINE behavior

Do not assume a feature exists because the PRD mentions it.

Classify each Phase 11 capability:

IMPLEMENTED
PARTIALLY IMPLEMENTED
ARCHITECTED
MISSING
DEFERRED

Identify the smallest architecture extension required.

Return:

1. Current scheduling architecture
2. Existing placement flow
3. Existing hard constraints
4. Existing scheduling features
5. Missing Phase 11 features
6. Risks and race conditions
7. Files that would need changes
8. Proposed M17 milestone decomposition
9. Targeted test strategy
10. STOP FOR APPROVAL

Do not implement anything.
```

------------------------------------------------------------------------

## Final instruction

**Phase 11 must extend the validated Phase 10 foundation. It must not
restart the platform.**

The first implementation question is therefore not:

> "How do we build an advanced scheduler?"

It is:

> **"What scheduling capability already exists, what is actually
> missing, and what is the smallest safe extension that adds advanced
> scheduling without violating the established AIDAR contracts?"**
