# AIDAR PHASE 12 PRD

## Distributed Intelligence --- Complete Implementation Manual

**Phase:** 12 --- Distributed Intelligence\
**Current baseline:** Phase 11 Advanced Scheduling\
**Proposed current milestone numbering:** M18.1 → M18.10\
**Status:** Architecture and implementation blueprint

------------------------------------------------------------------------

# 1. Executive Purpose

Phase 12 extends AIDAR from deterministic distributed execution and
advanced scheduling into a **validated distributed intelligence layer**.

The continuation PRD defines Phase 12 around:

-   resource prediction
-   duration prediction
-   failure prediction
-   capacity forecasting
-   anomaly detection
-   workload behavior models
-   adaptive policy recommendations

The intelligence layer must not replace deterministic safety rules.

The central authority boundary is:

``` text
INTELLIGENCE
    observe
    predict
    rank
    recommend

CONTROL PLANE
    admit
    place
    execute
    recover
    reject
```

A prediction cannot make an ineligible worker eligible.

------------------------------------------------------------------------

# 2. Source of Truth

Before implementation:

1.  Read this entire PRD.
2.  Read the current repository.
3.  Read the Phase 11 final audit.
4.  Read the M8--M13 continuation PRD.
5.  Treat the repository as implementation truth.
6.  Treat this PRD as architectural intent.
7.  Never assume a feature exists because a document says it exists.
8.  Reuse existing M7, M10, and M11 foundations.
9.  Do not create parallel telemetry, scheduler, registry, CAS, retry,
    persistence, or execution systems.
10. Do not add infrastructure or ML dependencies without evidence.
11. Application-specific intelligence stays behind adapters.

Required workflow:

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

# 3. Phase 11 Baseline

Phase 11 established:

``` text
WorkloadSpec
    ↓
Hard eligibility
    ↓
Eligible workers
    ↓
Priority / aging / queue
    ↓
Locality / affinity / deadline / cost
    ↓
M7 predictive ranking
    ↓
Explainable placement
    ↓
Dispatch
```

The governing rule remains:

``` text
HARD CORRECTNESS
       >
SCHEDULING PREFERENCE
       >
PREDICTION
```

Phase 12 adds richer intelligence without changing this rule.

------------------------------------------------------------------------

# 4. Problem Statement

AIDAR already has historical signals across:

-   WorkloadSpec
-   WorkloadRegistry
-   AttemptRegistry
-   WorkerRegistry
-   WorkerResourceProfile
-   WorkerMetrics
-   WorkloadExecutionResult
-   failure categories
-   checkpoint records
-   artifact metadata
-   transfer timing
-   M7 TelemetryMemory
-   M7 prediction outputs
-   M11 scheduling decisions

However, historical intelligence has important limitations:

-   M7 temporal memory is primarily in-memory.
-   Historical execution data is not yet a formal training-data
    pipeline.
-   Some timestamps do not represent the exact interval their names
    imply.
-   Retries can contaminate naive training data.
-   Checkpoint/resume executions can create multiple observations for
    one logical workload.
-   LOST attempts represent unknown outcomes.
-   Requested resources are not the same as measured resource
    consumption.
-   Worker snapshots are not automatically per-attempt measurements.
-   Transfer history can be volatile.
-   Feature/schema provenance is incomplete.
-   Training-example eligibility is not a general contract.
-   Anomaly detection exists conceptually but must be audited for live
    integration.
-   Capacity signals can be derived but lack a formal
    forecasting/recommendation layer.

Phase 12 must solve the **data → feature → prediction → policy →
recommendation** lifecycle.

------------------------------------------------------------------------

# 5. Target Architecture

``` text
                    DISTRIBUTED EXECUTION
                           |
                           v
                 Durable Execution History
                           |
                           v
                 Historical Data Pipeline
                           |
                           v
                 Validation / Eligibility
                           |
                           v
                    Feature Layer
                           |
          +----------------+----------------+
          |                |                |
          v                v                v
    Resource Model   Duration Model   Failure Model
          |                |                |
          +----------------+----------------+
                           |
                           v
                 Workload Behavior Model
                           |
                           v
                    Anomaly Detection
                           |
                           v
                  Capacity Forecasting
                           |
                           v
               Intelligence Aggregator
                           |
                           v
                  Explicit Policy Layer
                           |
                           v
                    Recommendation
                           |
                           v
                    CONTROL PLANE
                           |
                           v
                  EXISTING SCHEDULER
                           |
                           v
                         WORKER
```

The intelligence plane informs the control plane. It does not replace
it.

------------------------------------------------------------------------

# 6. Existing Components to Reuse

The investigation must inspect and reuse where appropriate:

-   `TelemetryMemory`
-   `TelemetryIngestor`
-   `FeatureExtractor`
-   `BehaviorInferencer`
-   `PerformancePredictor`
-   `PlacementRiskEvaluator`
-   `AdaptivePolicyEngine`
-   `M7OrchestratorBridge`
-   `PlacementEngine`
-   `WorkloadOrchestrator`
-   `WorkloadRegistry`
-   `AttemptRegistry`
-   `WorkerRegistry`
-   `CoordinatorStateStore`
-   `WorkerResourceProfile`
-   `WorkerMetrics`
-   `WorkloadExecutionResult`
-   failure categories
-   checkpoint state
-   artifact metadata
-   CAS metadata
-   candidate explanations

Do not create a second version of any of these without proving the
existing abstraction is insufficient.

------------------------------------------------------------------------

# 7. M18.1 --- Phase 12 Investigation

## Objective

Audit the existing intelligence and historical-data architecture before
changing code.

## Investigate

-   M7 telemetry ingestion
-   temporal memory
-   feature extraction
-   behavior inference
-   performance prediction
-   risk evaluation
-   adaptive policy
-   anomaly detection
-   M7 bridge
-   M11 placement
-   historical execution records
-   attempt timestamps
-   retry semantics
-   checkpoint semantics
-   LOST attempts
-   resource telemetry
-   transfer metrics
-   persistence
-   restart recovery
-   existing intelligence tests
-   current authority mutations

## Classification

Every Phase 12 capability:

``` text
IMPLEMENTED
PARTIALLY IMPLEMENTED
ARCHITECTED
MISSING
DEFERRED
```

## Deliverable

Return:

-   architecture map
-   data-flow map
-   historical-data inventory
-   data-quality gaps
-   authority-boundary findings
-   persistence findings
-   reusable components
-   risks
-   minimal architecture extension
-   proposed milestone decomposition
-   test strategy

**Do not modify code. STOP FOR APPROVAL.**

------------------------------------------------------------------------

# 8. Historical Observation Contract

A future historical execution observation should conceptually contain:

``` text
observation_id

workload_id
job_id
attempt_id
attempt_number

task_type
workload_schema_version

requested_cpu
requested_ram
requires_gpu
requested_vram
gpu_vendor
gpu_model
compute_capability
driver_version
runtime_compatibility

worker_id
worker_profile_snapshot
worker_status_at_assignment

submitted_at
assigned_at
started_at
finished_at

queue_duration
transfer_duration
execution_duration
total_duration

asset_count
asset_bytes
local_asset_count
local_asset_bytes

result_status
failure_category
failure_reason

retry_count
checkpointed
resumed_from_checkpoint

output_count
output_bytes

observed_cpu
observed_ram
observed_gpu
observed_vram

prediction_snapshot
placement_snapshot
policy_snapshot

feature_schema_version
provenance
eligibility_status
```

Not every field needs to exist immediately.

Missing data must remain explicitly missing. Never silently replace
missing measurements with fake zeroes.

------------------------------------------------------------------------

# 9. M18.2 --- Historical Execution Data Pipeline

## Objective

Create trustworthy historical observations from authoritative execution
records.

## Requirements

-   preserve workload identity
-   preserve attempt identity
-   preserve worker identity
-   preserve timestamps
-   preserve failure categories
-   preserve checkpoint information
-   preserve measured durations
-   preserve provenance
-   distinguish measured, derived, estimated, and missing
-   provide validation status

Possible lifecycle:

``` text
RAW
 ↓
VALIDATED
 ↓
TRAINING_ELIGIBLE
```

or:

``` text
RAW → EXCLUDED
RAW → QUARANTINED
RAW → CENSORED
```

Every exclusion needs a reason.

## Tests

-   valid observation
-   malformed record
-   duplicate observation
-   missing field
-   invalid timestamp
-   retry
-   checkpoint
-   LOST
-   provenance
-   deterministic reconstruction

------------------------------------------------------------------------

# 10. Training Data Safety

Historical data is not automatically training data.

Required pipeline:

``` text
RAW OBSERVATION
      ↓
Schema validation
      ↓
Identity validation
      ↓
Timestamp validation
      ↓
Duplicate detection
      ↓
Retry/checkpoint handling
      ↓
LOST/censoring handling
      ↓
Required-feature validation
      ↓
Outlier policy
      ↓
PROVENANCE
      ↓
TRAINING ELIGIBLE
```

Training data must have:

-   sample eligibility
-   provenance
-   schema version
-   feature version
-   missing-value policy
-   censoring policy
-   retry policy
-   checkpoint policy

------------------------------------------------------------------------

# 11. Retry Semantics

Example:

``` text
workload W
 attempt 1 → worker A → timeout
 attempt 2 → worker B → success
```

Do not interpret the timeout as an ordinary successful duration.

The history must distinguish:

``` text
success
failure
unknown
censored
retry
```

Retry count should remain available as a feature.

------------------------------------------------------------------------

# 12. Checkpoint Semantics

Example:

``` text
attempt 1
   ↓
checkpoint
   ↓
resume
   ↓
attempt 2
   ↓
final completion
```

Do not treat every checkpoint segment as an independent normal
completion.

Historical data should distinguish:

-   checkpoint creation
-   resumed execution
-   final completion
-   partial execution
-   logical workload duration

------------------------------------------------------------------------

# 13. LOST Attempt Semantics

``` text
LOST != SUCCESS
LOST != ordinary FAILURE
```

A LOST attempt means the outcome is unknown.

It may be useful for recovery analysis and censored-data handling, but
must not silently enter ordinary success-duration training.

------------------------------------------------------------------------

# 14. Timestamp Semantics

Every duration must define its interval.

Examples:

``` text
queue_duration
submission → assignment

transfer_duration
asset transfer start → asset transfer end

execution_duration
runtime start → runtime finish

total_duration
logical workload start → terminal completion
```

If a value cannot be measured accurately:

``` text
value = missing
```

not:

``` text
value = guessed
```

unless explicitly labeled as estimated.

------------------------------------------------------------------------

# 15. M18.3 --- Feature Engineering and Feature Contract

## Objective

Create deterministic, versioned, explainable feature extraction.

Potential feature families:

### Workload

-   task type
-   requested CPU
-   requested RAM
-   GPU requirement
-   VRAM requirement
-   asset count
-   asset bytes

### Worker

-   CPU capacity
-   RAM capacity
-   GPU capability
-   VRAM
-   health
-   recent reliability

### History

-   duration EMA
-   failure rate
-   retry rate
-   execution count

### Transfer

-   asset bytes
-   transfer duration
-   locality
-   cache hit information

### Scheduling

-   priority
-   deadline
-   cost
-   affinity

## Requirements

Features must be:

-   deterministic
-   versioned
-   provenance-aware
-   explicit about missing values
-   free of target leakage

Never use future execution results to predict that same execution.

------------------------------------------------------------------------

# 16. M18.4 --- Resource Demand Prediction

Predict where useful:

``` text
CPU demand
RAM peak
GPU utilization
VRAM peak
transfer demand
```

The first implementation may use statistical baselines.

Do not require deep learning merely because the phase is called
intelligence.

Every prediction should contain:

``` text
prediction
confidence
model_version
feature_schema_version
timestamp
evidence_count
explanation
```

Safety example:

``` text
predicted RAM = 2 GB
declared minimum RAM = 8 GB
worker RAM = 4 GB

=> worker remains INELIGIBLE
```

Predicted lower demand can never weaken a hard requirement.

------------------------------------------------------------------------

# 17. M18.5 --- Duration and Failure Prediction

## Duration

Predict:

``` text
expected execution duration
uncertainty/confidence
```

Use validated historical data.

## Failure

Estimate:

``` text
failure probability
failure-category tendency
worker/workload interaction risk
```

Failure prediction is initially advisory.

It must not silently become a hard rejection rule.

------------------------------------------------------------------------

# 18. M18.6 --- Workload Behavior Models

Potential descriptive behavior classes:

``` text
CPU-bound
RAM-heavy
GPU-bound
GPU-memory-heavy
I/O-heavy
transfer-heavy
short-running
long-running
bursty
stable
failure-prone
```

These classifications must be evidence-based and explainable.

Example:

``` text
Behavior: GPU-heavy

Evidence:
validated executions: 31
median GPU utilization: ...
confidence: ...
model version: ...
```

Do not make behavior labels silently authoritative.

------------------------------------------------------------------------

# 19. M18.7 --- Anomaly Detection

Connect anomaly detection to actual validated runtime observations.

Potential anomalies:

-   duration spike
-   RAM spike
-   GPU deviation
-   transfer spike
-   worker instability
-   telemetry instability
-   unexpected failure pattern

Initial authority:

``` text
observe
score
explain
surface
```

Not:

``` text
kill
ban
rewrite scheduler
```

unless a separate explicit policy contract is approved.

------------------------------------------------------------------------

# 20. M18.8 --- Adaptive Capacity Planning

Estimate:

``` text
expected workload volume
expected CPU demand
expected RAM demand
expected GPU demand
expected VRAM demand
expected queue growth
expected failure rate
expected transfer demand
```

Forecast output:

``` text
forecast_window
metric
expected_value
uncertainty
baseline
evidence_count
model_version
```

Recommendations remain advisory.

Example:

``` text
GPU capacity pressure expected in forecast window.
```

Do not automatically provision hardware.

------------------------------------------------------------------------

# 21. M18.9 --- Intelligence-to-Scheduler Integration

This is the critical authority milestone.

Conceptually:

``` text
Hard eligibility
      ↓
Phase 11 eligible candidates
      ↓
Phase 11 deterministic ranking
      ↓
Validated intelligence signal
      ↓
Policy evaluation
      ↓
Final ranking
      ↓
Existing control-plane placement
```

Intelligence must not directly mutate:

-   worker eligibility
-   hard constraints
-   workload state
-   retry limits
-   artifact state
-   security state

unless an explicit policy contract authorizes the mutation.

------------------------------------------------------------------------

# 22. Prediction Contract

Every production prediction should conceptually expose:

``` text
prediction_id
prediction_type
subject_id
prediction_value
confidence
uncertainty
model_version
feature_schema_version
created_at
valid_until
evidence_count
explanation
```

Predictions are time-bound observations, not permanent truth.

------------------------------------------------------------------------

# 23. Confidence and Cold Start

Sparse history must reduce confidence.

New workers/workloads must use safe fallback:

``` text
no history
    ↓
deterministic Phase 11 behavior
```

Never fabricate history.

Possible fallbacks:

-   static workload estimate
-   global baseline
-   deterministic worker capabilities
-   existing Phase 11 ranking

Fallback behavior must be explainable.

------------------------------------------------------------------------

# 24. Model Lifecycle

Conceptual lifecycle:

``` text
CREATED
  ↓
TRAINING
  ↓
VALIDATED
  ↓
READY
  ↓
ACTIVE
  ↓
RETIRED
```

Training completion does not automatically make a model active.

Model activation requires validation evidence.

------------------------------------------------------------------------

# 25. Model Validation

Validation should include:

-   baseline comparison
-   held-out data
-   temporal separation where appropriate
-   error metrics
-   calibration for probabilities
-   sample count
-   retry/censoring handling
-   workload coverage
-   worker coverage
-   cold-start behavior

Never report training-set performance as production model quality.

------------------------------------------------------------------------

# 26. M7 → M12 Boundary

Existing M7 abstractions should be reused.

M7 currently provides patterns for:

-   telemetry ingestion
-   temporal memory
-   feature extraction
-   behavior inference
-   duration/resource prediction
-   risk scoring
-   adaptive policy
-   orchestration bridge

Phase 12 must determine which pieces are mature enough to extend and
which need contracts/data-quality improvements.

Special attention is required for any existing M7 path that can affect
control state.

If an M7-derived health classification can move a worker into/out of
`DRAINING` or `ACTIVE`, Phase 12 must document the exact authority and
policy contract before expanding automatic mutation.

------------------------------------------------------------------------

# 27. Persistence

Prefer the existing `CoordinatorStateStore`.

Potential durable intelligence metadata:

``` text
validated observation
model metadata
feature schema metadata
prediction audit
capacity forecast
recommendation
```

Do not create a second state store.

Do not migrate to PostgreSQL without evidence.

M7 in-memory summaries may remain ephemeral if the architecture
explicitly treats them as cache/state rather than authoritative history.

------------------------------------------------------------------------

# 28. Control Plane / Data Plane

Intelligence belongs to the control-plane side.

Workers:

-   execute workloads
-   report observations
-   expose resource state

Workers do not become independent schedulers or AI authorities.

------------------------------------------------------------------------

# 29. Application Isolation

The core may understand:

``` text
task_type
resources
assets
execution
history
behavior
```

The core must not interpret:

``` text
Blender scene semantics
LLM prompt semantics
ML architecture
video-editor semantics
```

Application-specific interpretation stays behind adapters.

------------------------------------------------------------------------

# 30. Security

Intelligence data may contain sensitive information:

-   workload parameters
-   model identifiers
-   prompts
-   asset metadata
-   worker identity
-   execution history

Required:

-   least privilege
-   authenticated access
-   no secrets in features
-   explicit sensitive-field classification
-   authorized prediction access
-   no model/data export without authorization

Prediction must never become an information-leak path.

------------------------------------------------------------------------

# 31. Observability

Every actionable prediction should make it possible to answer:

``` text
What was predicted?
Why?
Using what evidence?
With what confidence?
Which model?
Which feature schema?
Did the control plane use it?
What policy consumed it?
What happened afterward?
```

Useful metrics:

``` text
prediction_count
prediction_latency
model_error_count
fallback_count
low_confidence_count
training_eligible_count
excluded_observation_count
anomaly_count
forecast_count
recommendation_count
```

------------------------------------------------------------------------

# 32. Failure Behavior

If intelligence fails:

``` text
prediction unavailable
        ↓
deterministic scheduler continues
```

Examples:

``` text
model unavailable → fallback
feature unavailable → missing-value policy/fallback
prediction timeout → skip prediction
invalid prediction → reject prediction
corrupt model → model rejected
```

Intelligence failure must not cause unsafe placement.

------------------------------------------------------------------------

# 33. Performance

Intelligence must not make normal scheduling unbounded.

Prefer:

-   cached aggregates
-   bounded history queries
-   precomputed features
-   asynchronous training
-   background forecasting
-   bounded inference

Avoid:

-   full historical scans on every placement
-   model training during submission
-   blocking scheduling on expensive inference

Measure:

-   feature latency
-   prediction latency
-   history query latency
-   model update time
-   memory
-   storage growth
-   scheduling overhead

------------------------------------------------------------------------

# 34. Testing Strategy

## Unit

-   validation
-   feature extraction
-   prediction
-   confidence
-   anomaly scoring
-   model metadata
-   fallback

## Integration

``` text
history → validation
validation → features
features → prediction
prediction → policy
policy → scheduler
```

## E2E

``` text
real execution
→ historical record
→ validated observation
→ intelligence
→ future workload
→ prediction
→ scheduling
```

## Adversarial

Test:

-   poisoned history
-   duplicate observations
-   stale observations
-   invalid prediction
-   model failure
-   sparse history
-   retry contamination
-   checkpoint contamination
-   LOST attempts
-   hard-constraint conflict
-   worker replacement

------------------------------------------------------------------------

# 35. Required Safety Scenarios

## Scenario A --- Prediction vs GPU

``` text
Worker A predicted fastest
Worker A lacks required GPU
```

Expected:

``` text
Worker A remains INELIGIBLE
```

## Scenario B --- Prediction vs stale telemetry

``` text
Prediction says worker is healthy
Telemetry is stale
```

Expected:

``` text
worker remains rejected
```

## Scenario C --- Prediction vs cost

``` text
Prediction favors expensive worker
cheap worker is eligible
```

Expected:

``` text
existing Phase 11 ranking/policy remains authoritative
```

## Scenario D --- Prediction vs security

``` text
prediction favors worker
worker violates hard security requirement
```

Expected:

``` text
worker remains ineligible
```

## Scenario E --- Cold start

``` text
no historical evidence
```

Expected:

``` text
safe deterministic fallback
```

------------------------------------------------------------------------

# 36. Capacity Forecast Tests

Test:

-   stable workload
-   rising workload
-   falling workload
-   burst
-   sparse history
-   worker addition
-   worker removal
-   GPU-heavy workload
-   RAM-heavy workload
-   mixed workload

Forecasts must remain observable and explainable.

------------------------------------------------------------------------

# 37. Backward Compatibility

Existing Phase 10 and Phase 11 workloads must continue working without
intelligence metadata.

The system must support deterministic behavior when intelligence is
unavailable.

Existing:

-   hard constraints
-   placement
-   retry
-   checkpointing
-   artifact verification
-   worker lifecycle

must remain intact.

------------------------------------------------------------------------

# 38. Non-Goals

Phase 12 must not become:

``` text
an opaque AI scheduler
a self-modifying control plane
a generic ML platform
a data warehouse
a model-hosting platform
a Kubernetes replacement
a second scheduler
a second telemetry system
a Blender intelligence monolith
```

The core mission remains:

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

# 39. Critical Invariants

## I1 --- Hard constraints remain absolute

``` text
prediction cannot make an ineligible worker eligible
```

## I2 --- No false training data

``` text
invalid observation != training example
```

## I3 --- LOST is unknown

``` text
LOST != FAILURE
```

## I4 --- No target leakage

``` text
future outcome != prediction input for that same outcome
```

## I5 --- No silent authority

``` text
prediction != control mutation
```

unless an explicit policy contract exists.

## I6 --- Explainability

``` text
actionable prediction
=> evidence + model/version + confidence + explanation
```

## I7 --- Safe fallback

``` text
intelligence unavailable
=> deterministic safe behavior
```

## I8 --- Application isolation

``` text
generic intelligence != application implementation
```

## I9 --- Durable truth remains authoritative

``` text
prediction != replacement for workload/attempt/artifact truth
```

## I10 --- No fake validation

``` text
synthetic test != production validation
```

------------------------------------------------------------------------

# 40. Proposed Milestone Structure

The continuation PRD names the conceptual M12 capabilities as
M12.1--M12.4. For the current repository, the implementation sequence
below expands them into proposed M18 milestones so they follow the
existing post-Phase-11 numbering.

``` text
M18.1  Phase 12 Investigation
   ↓
M18.2  Historical Execution Data
   ↓
M18.3  Feature Engineering / Feature Contract
   ↓
M18.4  Resource Demand Prediction
   ↓
M18.5  Duration + Failure Prediction
   ↓
M18.6  Workload Behavior Models
   ↓
M18.7  Anomaly Detection
   ↓
M18.8  Adaptive Capacity Planning
   ↓
M18.9  Intelligence-to-Scheduler Integration
   ↓
M18.10 Phase 12 Final Audit
```

This is a proposed decomposition, not a claim that the repository
already uses these milestone names.

------------------------------------------------------------------------

# 41. M18.10 Final Audit Requirements

The final audit must classify every capability:

``` text
IMPLEMENTED + VALIDATED
IMPLEMENTED + PARTIALLY VALIDATED
IMPLEMENTED + NOT VALIDATED
ARCHITECTED ONLY
DEFERRED
MISSING
```

Audit:

-   historical data
-   data quality
-   feature provenance
-   retry semantics
-   checkpoint semantics
-   LOST semantics
-   target leakage
-   resource prediction
-   duration prediction
-   failure prediction
-   behavior models
-   anomaly detection
-   capacity forecasting
-   model lifecycle
-   confidence
-   explainability
-   authority boundary
-   persistence
-   restart
-   security
-   performance
-   backward compatibility
-   Phase 11 invariant preservation

Do not declare Phase 12 complete merely because code exists.

------------------------------------------------------------------------

# 42. Definition of Done

Phase 12 is complete only when the repository can demonstrate:

1.  Historical execution data can be extracted from authoritative
    records.
2.  Historical observations have explicit validation semantics.
3.  Retry/checkpoint/LOST behavior is represented correctly.
4.  Features have deterministic definitions and provenance.
5.  Resource prediction exists or is explicitly deferred with evidence.
6.  Duration prediction exists or is explicitly deferred with evidence.
7.  Failure prediction exists or is explicitly deferred with evidence.
8.  Workload behavior modeling exists or is explicitly deferred with
    evidence.
9.  Anomaly detection is connected to validated observations.
10. Capacity forecasting/recommendation exists or is explicitly
    deferred.
11. Intelligence outputs are explainable.
12. Intelligence failures have deterministic fallback.
13. Hard scheduling constraints remain absolute.
14. M7/M12 cannot bypass control-plane safety.
15. Historical data is not silently treated as trustworthy training
    data.
16. Prediction inputs avoid target leakage.
17. Model and feature versions are explicit.
18. Persistence behavior is documented.
19. Restart behavior is tested where required.
20. Security boundaries are preserved.
21. Existing Phase 10/11 workloads remain compatible.
22. Intelligence overhead is measured.
23. Relevant unit/integration/E2E tests pass.
24. Limitations are explicitly classified.
25. No hidden or untested claims are made.

------------------------------------------------------------------------

# 43. Final Architecture

``` text
                         AIDAR
                           |
             +-------------+-------------+
             |                           |
       EXECUTION PLANE              INTELLIGENCE PLANE
             |                           |
       Worker Runtime              Historical Data
             |                           |
       Execution Result            Validation
             |                           |
       Verified Artifact            Features
             |                           |
             +-------------+-------------+
                           |
                    Prediction Layer
                           |
            +--------------+--------------+
            |              |              |
         Resource       Duration       Failure
         Prediction     Prediction     Prediction
            |              |              |
            +--------------+--------------+
                           |
                  Behavior / Anomaly
                           |
                           v
                  Capacity Forecast
                           |
                           v
                 Policy / Recommendation
                           |
                           v
                    CONTROL PLANE
                           |
                     SCHEDULER
                           |
                         WORKER
```

The intelligence layer informs decisions.

The control plane owns authoritative decisions.

------------------------------------------------------------------------

# 44. Boundary With Phase 13

Phase 13 is where platform expansion belongs:

``` text
additional application adapters
LLM inference
ML training
FFmpeg/video
external API
user management
multi-tenancy
stronger security
cluster federation
```

Phase 12 should provide reusable intelligence primitives for those
workloads without implementing their application semantics.

------------------------------------------------------------------------

# 45. Final Principles

Phase 10 made distributed execution real.

Phase 11 made scheduling more capable.

Phase 12 makes intelligence predictive.

Phase 13 expands the platform.

The governing rule remains:

``` text
HARD CORRECTNESS
       >
SCHEDULING PREFERENCE
       >
PREDICTION
```

Phase 12 adds:

``` text
PREDICTION
    ↓
VALIDATED EVIDENCE
    ↓
EXPLICIT POLICY
    ↓
CONTROL-PLANE AUTHORITY
```

AIDAR should become smarter without becoming less trustworthy.

------------------------------------------------------------------------

# 46. Immediate Next Action

Start only with M18.1.

Use this Antigravity prompt:

``` text
/plan

M18.1 — Phase 12 Distributed Intelligence Investigation

Do not modify code.

Read the complete AIDAR Phase 12 PRD first.

Then inspect the CURRENT repository as the implementation source of truth.

Investigate:

- existing M7 telemetry
- TelemetryMemory
- TelemetryIngestor
- FeatureExtractor
- BehaviorInferencer
- PerformancePredictor
- PlacementRiskEvaluator
- AdaptivePolicyEngine
- M7OrchestratorBridge
- PlacementEngine
- WorkloadOrchestrator
- WorkloadRegistry
- AttemptRegistry
- WorkerRegistry
- WorkerResourceProfile
- WorkerMetrics
- WorkloadExecutionResult
- failure categories
- checkpoint state
- artifact metadata
- transfer timing
- CoordinatorStateStore
- persistence/restart
- anomaly detection
- existing prediction tests
- capacity-related signals

Determine:

1. What Phase 12 already has.
2. What is partially implemented.
3. What is merely architected.
4. What is missing.
5. What is unsafe or incomplete to reuse.
6. Which historical fields are authoritative.
7. Which fields are measured, derived, estimated, or missing.
8. Where retry/checkpoint/LOST contamination can occur.
9. Where target leakage can occur.
10. What provenance exists.
11. What prediction interfaces exist.
12. What authority boundaries currently exist.
13. Whether any M7-derived control mutation lacks an explicit policy contract.
14. What persistence extensions are actually required.
15. What minimum architecture is required for M18.2 onward.

Classify every Phase 12 capability:

IMPLEMENTED
PARTIALLY IMPLEMENTED
ARCHITECTED
MISSING
DEFERRED

Do not create a second telemetry system, scheduler, registry, state store,
CAS, or intelligence engine if an existing component can be safely extended.

Do not add ML dependencies yet.

Do not modify production code.

Return:

- architecture map
- data-flow map
- historical-data inventory
- data-quality gaps
- intelligence authority map
- persistence map
- M7 → M12 boundary findings
- Phase 12 gap matrix
- risks
- proposed M18.2 → M18.10 decomposition
- exact files likely to change
- targeted test strategy

STOP FOR APPROVAL.
```

**This PRD deliberately treats intelligence as a disciplined engineering
layer, not "throw an AI model at the scheduler."** That matches the
source architecture's M12 direction: resource/duration/failure
prediction, capacity forecasting, anomaly detection, workload behavior
models, and adaptive recommendations, while deterministic safety remains
authoritative. fileciteturn73file3
