# M10: Production Distributed Execution

This document describes what M10 actually built on top of the M8 (Job/
Artifact/recovery) and M9 (security/authentication) milestones. It
documents the implementation as it exists, not the aspirational PRD
restatement -- see `docs/AIDAR_CONTINUATION_PRD.md` §18-32 for the
original requirements this satisfies.

**Architectural discipline preserved throughout**: no second scheduler,
dispatcher, persistence mechanism, CAS, artifact identity system, worker
state machine, recovery engine, or duplicate job/result abstraction was
introduced. Every M10 entity extends or is read by the EXISTING
`WorkloadOrchestrator` dispatch loop, `CoordinatorStateStore` SQLite/WAL
persistence, `WorkerRegistry` heartbeat/eviction machinery, and M9
`CredentialStore`/`ReplayGuard` authentication -- none of it was
replaced.

---

## 1. Execution Attempt Model (M10.1)

**New module: `src/aidars/distributed/attempt.py`.**

```
Job --> Workload (stable workload_id) --> Attempt(1), Attempt(2), ... --> ExecutionResult --> Artifact
```

- `WorkloadRecord` (workload_registry.py) is **unchanged** and remains
  the single source of *current* truth: its `execution_result` field is
  still the latest/final result, read unchanged by `JobRegistry.
  get_aggregate()` and `GET /workloads/{id}`.
- `AttemptRecord` is a new, additive entity that is the *historical*
  ledger: one row per actual dispatch try, with its own
  `execution_result` copy, `worker_id`, timing, and failure
  classification.
- `attempt_id = f"{workload_id}#{attempt_number}"`. `attempt_number`
  starts at 1 and is stable/monotonic per `workload_id` -- **the logical
  workload_id never changes across retries** (verified by
  `test_m10_execution_attempts.py::test_workload_id_remains_stable_across_every_attempt`
  and the retry tests).
- States: `QUEUED -> ASSIGNED -> RUNNING -> {SUCCEEDED | FAILED | LOST}`.
  No `CANCELLED` member -- there is no cancel operation anywhere in this
  codebase (same reachability discipline already applied to
  `WorkloadState`/`JobState`'s unreachable members).
  - `LOST` is distinct from `FAILED`: `FAILED` means an explicit,
    observed failure outcome. `LOST` means the coordinator's owning
    worker disappeared while the attempt was `RUNNING`/`ASSIGNED` and
    the coordinator has **no observed outcome at all** -- the work may
    have actually completed on the worker before it vanished. See §3.
- Persisted through the *existing* `CoordinatorStateStore` (new
  `attempts` SQLite table, same WAL/upsert/`_safe_reconstruct` pattern
  as every other table) -- not a second persistence system.

## 2. Retry Policy (M10.5)

**New module: `src/aidars/distributed/retry.py`.**

An additive `FailureCategory` enum (`models.py`) is populated at the
*specific point of failure* in `execution.py` (for worker-observed
failures) or `workload.py` (for dispatch-level failures the worker never
saw), never inferred after the fact from string matching:

| Category | Retryable | Where it's raised |
|---|---|---|
| `WORKER_UNAVAILABLE` | yes | dispatch exception (can't reach worker); checkpoint-unsupported abort; restart-time LOST reclassification |
| `TEMPORARY_CAS_FAILURE` | yes | dependency staging copy/link failure; missing dependencies locally; output-ingestion CAS failure |
| `RESOURCE_EXHAUSTION` | yes | workspace-creation failure; execution timeout |
| `INVALID_INPUT`, `INVALID_DEPENDENCY`, `MISSING_EXECUTABLE`, `INVALID_WORKLOAD`, `AUTHORIZATION_FAILURE`, `MALFORMED_REQUEST` | no | reserved for adapter/runtime-specific validation (no current call site emits these -- see §11 limitations) |
| `APPLICATION_ERROR` | no | generic non-zero exit; M8.6 output-verification failure; unclassified runtime exception (conservative default) |

`retry.is_retryable(None)` is `False`: an unrecognized/absent
classification is **never** silently retried -- this is the concrete
enforcement of "explicitly forbid `except Exception: retry`".

`WorkloadOrchestrator._process_workload()` is now an explicit,
bounded loop (`retry.DEFAULT_MAX_ATTEMPTS = 3`): each iteration creates
a new `AttemptRecord` for the same `workload_id`, and a retryable
failure `continue`s the loop rather than recursing or falling through a
one-shot inline fallback (the pre-M10 code's single ad hoc retry). On
exhaustion, the workload is recorded `WorkloadState.FAILED` explicitly.
A worker that fails to dispatch to is excluded from *that workload's*
remaining attempts via a local `exhausted_worker_ids` set (scoped to
this one retry loop) **and** reported to `WorkerRegistry.record_failure()`
(cluster-wide health tracking, affecting every other workload's
placement too) -- two different, correctly-scoped mechanisms, not one
overloaded to do both jobs.

Job aggregation (`JobRegistry.get_aggregate()`) is untouched and
continues to read `WorkloadRegistry` live -- confirmed by
`test_m10_artifact_lifecycle.py::test_job_aggregate_reflects_final_attempt_not_every_attempt`
that a workload retried 3 times still counts as exactly one workload in
its Job's aggregate, never one per attempt.

## 3. Worker Failure Recovery (M10.6)

Integrated into the **existing** heartbeat/eviction/restore machinery --
no second recovery engine.

- **Live eviction** (`CoordinatorService._run_eviction_loop`): when
  `WorkerRegistry.evict_expired_workers()` evicts a worker, `
  WorkloadOrchestrator.handle_worker_lost(worker_id)` marks every
  `ASSIGNED`/`RUNNING` attempt on that worker `LOST`. This does **not**
  spawn a competing redispatch: the in-flight `_process_workload()`
  coroutine already dispatching to that worker independently observes
  the dead connection (its HTTP call eventually errors/times out) and
  retries through the normal retry-budget loop. Two racing dispatches
  for the same `workload_id` would be pure waste, not a benefit.
- **Coordinator restart** (`CoordinatorService._restore_persisted_state`):
  recovery order is `workers -> jobs -> workloads -> attempts ->
  artifacts`. Any restored attempt still `ASSIGNED`/`RUNNING` has an
  **unknown** outcome -- it is marked `LOST` (category
  `WORKER_UNAVAILABLE`) *before* `_redrive_recovered_workloads()` fires,
  so the redrive creates a fresh, correctly-numbered attempt rather than
  leaving a stale one looking perpetually in-progress.
- **At-least-once, not exactly-once** (unchanged M8 invariant): a
  worker that completed the work right before the coordinator lost
  contact with it will have its result **duplicated** by the redrive.
  CAS content-addressing makes the duplicate *write* a no-op (identical
  hash -> identical bytes); it does not prevent the duplicate *compute*.
  This is explicitly tested
  (`test_m10_recovery.py::test_duplicate_execution_of_identical_output_remains_semantically_correct`)
  and explicitly NOT claimed as exactly-once anywhere in this codebase.

## 4. Checkpointing (M10.7)

**New module: `src/aidars/distributed/checkpoint.py`.** New capability
flag: `RuntimeAdapter.supports_checkpointing: bool = False`
(`runtime.py`).

Pre-M10, `ExecutionManager.execute_workload()` treated *any* checkpoint
request as a successful migration, regardless of whether the runtime
could actually preserve any state -- `GenericSubprocessRuntime.
checkpoint()` only terminates the subprocess, saving nothing. This was
dishonest and has been corrected:

- `GenericSubprocessRuntime.supports_checkpointing = False`, explicitly
  and honestly declared.
- When a checkpoint is requested against a runtime that doesn't support
  it, `execution.py` now produces a genuine `FAILED` result
  (`failure_category = WORKER_UNAVAILABLE`, retryable) instead of a
  fabricated success. The workload flows back through the normal
  retry-budget loop and gets a fresh attempt on (possibly) a different
  worker.
- When a runtime genuinely declares `supports_checkpointing = True`,
  the checkpoint result carries `checkpoint_hash` (ordinary CAS content
  -- no second checkpoint storage system), `checkpoint_runtime_type`,
  and `checkpoint_format_version` (`checkpoint.
  CURRENT_CHECKPOINT_FORMAT_VERSION`). `checkpoint.validate_checkpoint()`
  checks presence, format-version compatibility, and cluster-wide hash
  resolvability -- the last check reuses the *existing*
  `WorkerRegistry.get_workers_for_hash()`/`locate_hashes()` inverted
  index rather than a new CAS query mechanism.
- **No runtime in this codebase currently supports checkpointing.** The
  capability model exists so a future runtime can opt in honestly; today
  it makes the previous fake-success path impossible rather than
  claiming resumability that doesn't exist.

## 5. Worker Draining (M10.8)

**Audited and extended**, not replaced.

- `PlacementEngine.evaluate()` **already** hard-excludes
  `DRAINING`/`UNHEALTHY` workers (`placement.py`) -- confirmed
  unmodified/correct (VALIDATE ONLY).
- **Bug fixed**: `WorkloadOrchestrator.drain_worker()` referenced
  `record.placement` (an attribute that does not exist on
  `WorkloadRecord` -- the field is `placement_decision`), so it raised
  `AttributeError` on every record it examined and its `active_workloads`
  filter always silently produced an empty list. `drain_worker()` never
  actually found or checkpointed anything before this fix. Fixed to
  read `record.placement_decision.selected_worker_id`. Existing
  placement correctness was unaffected by this bug (draining workers
  were still excluded from *new* placement); only in-flight work on a
  worker transitioning to `DRAINING` was affected.
- **New**: `DRAINING -> ACTIVE` revival. No prior code path could revive
  a `DRAINING` worker (`WorkerRegistry.record_heartbeat()` only promotes
  `OFFLINE -> ACTIVE`) -- `DRAINING` was a one-way trap. `CoordinatorService.
  _evaluate_cluster_health_loop()` (the same M7 behavioral-risk loop that
  already puts a worker into `DRAINING`) now also promotes it back to
  `ACTIVE` once the same signal recovers to `M7WorkerState.STABLE`.
  Reuses the existing M7 evaluation already running there -- no second
  monitoring mechanism.

## 6. Artifact Lifecycle / GC (M10.9)

M8's `ArtifactRegistry` (creation, `AVAILABLE`/`GC_ELIGIBLE`/`DELETED`
lifecycle, `compute_gc_eligible()`) is **unmodified**. M10's wiring
contribution is at the retry-loop seam: `WorkloadOrchestrator.
_record_artifacts()` is still called exactly once, exactly when a
workload transitions to `WorkloadState.COMPLETED` -- a failed or
retried attempt's (possibly empty, possibly stale) `output_asset_hashes`
never reaches `ArtifactRegistry` (verified by
`test_m10_artifact_lifecycle.py`). Active-job/GC-eligibility protection
logic itself is untouched M8 code and is not re-described here -- see
`artifact.py`'s own docstrings and `test_artifact_registry.py`.

No automatic GC sweep exists (unchanged from M8): `compute_gc_eligible()`/
`mark_gc_eligible()`/`mark_deleted()` remain explicit, caller-invoked
operations. Building an automatic sweep was not required by any M10
requirement actually present in this repository and was not added.

## 7. Control Plane / Data Plane (M10.10)

Unmodified by M10. The coordinator issues placement decisions and
control-plane HTTP calls only (`/workloads/execute`, `/heartbeat`,
`/checkpoint`); binary asset transfer stays on the existing
worker-to-worker/CAS streaming path (`server.py`'s `stream_asset`
endpoint, `DistributedClient.sync_assets`/`download_missing_assets`) --
large artifacts never round-trip through the coordinator. No M10
requirement in this repository's actual state required a change here;
audited and confirmed unchanged.

## 8. Execution Observability (M10.11)

Two additive layers, both structured data (no scattered print
statements):

- **Worker-side phase timing** on `WorkloadExecutionResult` (`models.py`):
  `staging_duration_seconds` (dependency staging into the sandbox --
  the asset-synchronization-equivalent phase for this in-process
  dispatch path), `execution_duration_seconds` (unchanged pre-M10
  field: pure runtime-execution phase), `output_ingestion_duration_seconds`,
  `verification_duration_seconds` (the M8.6 expected-output-count
  check). `total_duration_seconds` is a pure derived `@property` (sum
  of the four), not a duplicated stored field. **A failed attempt still
  reports these** -- timing is captured before the failure branch
  returns, not only on success.
- **Coordinator-side attempt timeline** on `AttemptRecord` (`attempt.py`):
  `queued_at -> assigned_at -> started_at -> finished_at`, with derived
  `queued_duration_seconds` (placement-wait latency) and
  `total_duration_seconds`. Exposed via `AttemptRecord.to_summary_dict()`
  and the `GET /workloads/{id}` API's new `attempts`/`attempt_count`
  fields.

Together these let a caller distinguish scheduler latency (queued
duration), staging/CAS-transfer latency, runtime latency, and output-path
latency for any given attempt -- including failed ones.

**Honest limitation**: real cross-worker asset-synchronization timing
(a worker fetching a missing dependency from a *peer* worker before
executing) is not separately instrumented from `staging_duration_seconds`
in this dispatch path, because `ExecutionManager.execute_workload()`'s
staging step assumes dependencies are already present in the local CAS
(a pre-existing, documented assumption -- "This assumes the
coordinator/client has already fetched missing hashes to CAS", not
something M10 introduced or was asked to change).

## 9. Multi-Worker Validation (M10.12)

`tests/unit/test_m10_multi_worker.py` validates registration,
heartbeats, placement (across three independently-constructed
`DistributedWorker` objects, each with its own CAS), dispatch, one
worker's failure not affecting the others, and attempt-level worker-loss
reclassification -- all over real FastAPI routing via
`httpx.ASGITransport` (the same in-process-ASGI pattern already
established in `test_streaming_client.py`), not mocked coordinator/worker
logic.

**ENVIRONMENT-GATED** (explicitly, via a `@pytest.mark.skip` with a
reason string, not silently omitted): genuine separate-host network
transport, real process isolation/crash behavior, and real network
partition/latency are NOT validated by the in-process tests above. That
requires either `deploy/docker-compose.yml` run against real Docker
(coordinator + worker-a/b/c as separate containers, real TCP/IP) or
actual multiple physical/virtual hosts -- neither is available inside
this repository's automated test execution environment. **Do not read
the in-process test pass as a claim of real distributed deployment.**

## 10. Production Deployment (M10.13)

**Smallest reproducible mechanism, chosen deliberately over Kubernetes**:
Docker + Docker Compose. Before M10, this repository had *zero*
containerization/deployment infrastructure and *zero* process
entrypoints for running a coordinator or worker at all -- the `Makefile`
declared `start-coordinator`/`start-worker` `.PHONY` targets with no
recipe bodies, and no `__main__.py`/`main.py` existed anywhere under
`src/`.

- **New**: `src/aidars/distributed/cli.py` -- `aidars-coordinator` /
  `aidars-worker` console scripts (registered in `pyproject.toml`),
  entirely environment-variable/CLI-flag driven (`AIDAR_COORDINATOR_*`,
  `AIDAR_WORKER_*`, `AIDAR_HEARTBEAT_*`, `AIDAR_LOG_LEVEL`; see
  `build_coordinator_arg_parser()`/`build_worker_arg_parser()` for the
  full list). No hardcoded localhost-only assumption, Windows path, or
  developer-specific secret. Wires together only classes every existing
  test already exercises (`CoordinatorService`, `DistributedWorker`,
  `CoordinatorStateStore`, `LocalCASAdapter`) -- no new architecture.
  `Makefile`'s `start-coordinator`/`start-worker` targets now actually
  run something.
- **New**: `deploy/Dockerfile` (one image serves both roles; role
  selected by the command at `docker run`/compose time), `deploy/
  docker-compose.yml` (Coordinator + Worker A/B/C -- the M10.12 minimum
  topology -- persistent volumes, health-checked startup ordering),
  `deploy/.env.example` (template; the real `.env` is git-ignored and
  never committed).
- A static-IP Docker network is used in `docker-compose.yml` because
  `WorkerRegistrationPayload.ip_address` (`models.py`) is validated as a
  real IP address, not a DNS hostname -- Compose's default per-service
  DNS name can't satisfy that validation, so each service gets a fixed
  address instead. `cli.py`'s `run_worker()` mirrors this: `--ip-address`
  must be a real, routable IP (best-effort auto-detected via a UDP
  socket trick when not supplied, but **must** be set explicitly in any
  container/NAT deployment).
- Pre-existing, unrelated ad hoc root-level scripts (`start_worker_a.py`,
  `start_worker_b.py`, `start_worker_c.py`, `run_lan_test_pc_*.py`, etc.)
  predate M10, hardcode a Windows path (`C:\AIDAR-M5\...`) and fixed
  LAN IPs, and were **left untouched** -- they are manual developer test
  scripts, not part of the installed package or CI, and touching them
  was out of scope (no unrelated cleanup). `cli.py` is the proper,
  portable replacement going forward.

## 11. Infrastructure as Code (M10.14)

Scoped to exactly what the chosen deployment architecture (§10) needs --
no Kubernetes manifests, no Terraform, no cloud-provider-specific IaC,
none of which is justified by this repository's actual requirements.
`deploy/docker-compose.yml` IS this milestone's IaC: it describes the
network (a dedicated static-IP bridge network), the four
services/containers, persistent volumes, secrets-by-reference (`${VAR}`
substitution from a git-ignored `.env`, never an embedded credential),
and health-check-gated startup ordering. TLS termination is explicitly
left to deployment-layer infrastructure (a reverse proxy / load balancer
in front of the coordinator in a real deployment) rather than added to
application code -- nothing here required otherwise. Outbound TLS
verification (already `httpx`'s default, unmodified) is preserved.

## 12. High Availability (M10.15)

**No real HA was implemented.** A single coordinator remains the only
valid configuration for this architecture. What M10 does instead:

- **This readiness audit** (this section), documenting why:
  - **State ownership problem**: every piece of coordinator state
    (`WorkerRegistry`, `WorkloadRegistry`, `JobRegistry`,
    `ArtifactRegistry`, `AttemptRegistry`) is an in-memory
    `threading.RLock`-guarded structure with one specific SQLite file as
    its write-through backing store. Two coordinator processes would
    each hold an independent, divergent in-memory copy; there is no
    shared-state or leader-follower replication mechanism between them.
  - **Database migration prerequisite**: SQLite's WAL mode supports one
    writer process at a time (multiple readers, one writer, and even
    that is per-machine, not networked). Real multi-coordinator HA needs
    a database that supports concurrent writers from multiple hosts --
    i.e. PostgreSQL or equivalent (see §13) -- as a hard prerequisite,
    not an optional enhancement.
  - **Leader-election/failover requirements**: a real HA design needs
    (a) a consensus mechanism (e.g. Raft, or an external coordination
    service) to elect exactly one active coordinator, (b) a way for
    workers/clients to discover the current leader, (c) safe handoff of
    in-flight dispatch/recovery state, and (d) split-brain prevention.
    None of this exists here, and implementing it "to check a PRD box"
    without the database prerequisite in place would produce an unsafe
    system that looks like HA but isn't.
- **One concrete, honest safety mechanism**: `CoordinatorStateStore`
  now acquires an exclusive, non-blocking `fcntl.flock` on a sidecar
  lock file (`<db_path>.coordinator.lock`) at construction time. A
  second coordinator process pointed at the same SQLite file fails
  **immediately and loudly** (`CoordinatorAlreadyRunningError`) instead
  of silently corrupting or racing on shared state. This is NOT
  distributed consensus and does not claim to be -- it prevents exactly
  one specific unsafe mistake (`docker-compose scale coordinator=2`
  against the same volume, or two manually-launched processes), tested
  in `test_m10_persistence.py`.

## 13. Database Evolution (M10.16)

**SQLite/WAL is kept, unchanged.** No M10 requirement actually present
in this repository justifies a PostgreSQL migration: HA was not
implemented (§12), and every other M10 requirement (attempts, retry,
checkpoint metadata, draining, observability) is satisfiable by adding
one more table to the existing SQLite schema, which is what was done.
If real HA is ever implemented per §12's prerequisites, the migration
would need to preserve worker state, workload state, attempt state,
placement, execution results, jobs, artifacts, and recovery semantics --
i.e. every table this document describes -- but performing that
migration speculatively, with no consumer of the resulting HA
capability, was explicitly out of scope.

## 14. API Contracts (M10.17)

`GET /api/v1/workloads/{workload_id}` (existing route, `require_admin`
auth unchanged) gained two additive response fields:
`attempts: List[AttemptSummary]` and `attempt_count: int`. Each
attempt summary (`AttemptRecord.to_summary_dict()`) carries
`attempt_id`, `attempt_number`, `status`, `worker_id`, the timeline
timestamps/durations, `failure_category`/`failure_reason`, and
`was_checkpointed`/`checkpoint_hash` -- and nothing else. It never
carries a worker credential or any other secret (enforced by
`test_m10_attempt_registry.py::test_to_summary_dict_never_leaks_worker_credential_shaped_fields`
and `test_m10_security_regression.py`'s explicit response-body scan for
the issued credential string). No new route was added; M9 authentication
is unchanged and still gates this response the same way it gated the
route before M10 (still 401 without a valid admin token, still 401 for a
worker's own credential or the bootstrap secret -- verified in
`test_m10_security_regression.py`). Fail-closed behavior (a credential
verification error rejects rather than defaults-open) is unchanged.

## 15. Persistence / Recovery (M10.18)

New `attempts` SQLite table (`state_store.py`), same conventions as
every other table: WAL mode, parameterized SQL, `ON CONFLICT DO UPDATE`
upsert, one table per entity, JSON blob for the nested
`WorkloadExecutionResult`, and `_safe_reconstruct()`'s per-row
try/except deserialization (a malformed `attempts` row is logged and
skipped, never fatal to loading every other row -- tested in
`test_m10_persistence.py`). Recovery ordering:
`workers -> jobs -> workloads -> attempts -> artifacts -> (redrive)`,
matching §3's description. `AttemptRegistry` participates in normal
writes (every `mark_*` transition persists), normal reads
(`load_attempts()`), restart recovery (`restore_attempt()`, called from
`_restore_persisted_state()`), and malformed-state handling
(`_safe_reconstruct`) -- there is no in-memory-only attempt state that
would silently vanish on restart, which would have made the whole
"explain retries after a coordinator restart" requirement meaningless.

## 16. Test Strategy / Results (M10.19/M10.20)

Targeted M10 suite: **115 passed, 1 skipped** (the environment-gated
multi-host test, §9) in `tests/unit/test_m10_*.py` (10 new files:
`test_m10_attempt_registry.py`, `test_m10_retry_policy.py`,
`test_m10_execution_attempts.py`, `test_m10_checkpoint_capability.py`,
`test_m10_worker_draining.py`, `test_m10_recovery.py`,
`test_m10_observability.py`, `test_m10_multi_worker.py`,
`test_m10_security_regression.py`, `test_m10_persistence.py`,
`test_m10_artifact_lifecycle.py`, `test_m10_cli.py`).

Full repository suite run once, after the targeted M10 run: **1237
passed, 3 skipped, 0 failed** (`tests/`). The 3 skips are the 2
pre-existing skips already present before M10 plus the 1 new
environment-gated multi-host test described above; none are hidden or
unexplained. No existing test was modified to make M10 pass -- the one
regression the retry-loop rewrite caused
(`test_workload_result_persistence.py::test_checkpointed_result_does_not_persist_execution_result`,
which asserted the pre-M10 fire-and-forget-task migration behavior) was
fixed by preserving that exact behavior (spawn a new task and return for
the checkpoint/migrate branch) rather than by weakening the test's
assertion. One additional real bug was found and fixed during test
writing: the restart-time LOST-reclassification path in
`_restore_persisted_state()` initially forgot to set `failure_category`
on the reclassified attempt (caught by
`test_m10_recovery.py::test_restart_marks_in_flight_attempts_as_lost`).

## 17. Remaining Limitations / Explicitly Deferred

- **No checkpointing-capable runtime exists** in this codebase (§4) --
  the capability model is built and honest, but currently always
  reports `supports_checkpointing = False` everywhere. This is
  DESIGNED, not IMPLEMENTED-as-a-working-feature: it corrects a dishonest
  prior behavior and provides the extension point, but does not itself
  add checkpoint-capable execution.
- **Real multi-host validation is environment-gated** (§9), not run in
  this environment.
- **No automatic Artifact GC sweep** (§6) -- unchanged from M8, and not
  required by any M10 requirement genuinely present in this repository.
- **Real HA is not implemented** (§12) -- by design, per the PRD's own
  instruction not to fake it. A readiness audit and one concrete safety
  mechanism (the single-writer lock) are what M10 delivers here.
- **PostgreSQL migration was not performed** (§13) -- not justified
  without real HA to consume it.
- **`INVALID_INPUT`/`INVALID_DEPENDENCY`/`MISSING_EXECUTABLE`/
  `AUTHORIZATION_FAILURE`/`MALFORMED_REQUEST` failure categories exist
  in the `FailureCategory` enum but have no current call site that
  emits them** -- the existing `execution.py`/`workload.py` failure
  surface doesn't yet distinguish these specific conditions from the
  generic `APPLICATION_ERROR` default. They are reserved, documented,
  non-retryable-by-default classifications for a future adapter
  (e.g. Blender-specific validation) to use, not currently reachable
  dead code paths that were faked as tested.
- **Cross-worker asset-synchronization timing is not separately
  instrumented** from `staging_duration_seconds` (§8) -- a pre-existing
  dispatch-path assumption (dependencies already locally present),
  unchanged by M10.
