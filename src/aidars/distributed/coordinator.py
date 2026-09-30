"""AIDAR Distributed Control Plane Coordinator Service.

Manages worker registration, heartbeat tracking, inverted hash index,
and candidate prioritization for missing asset location over FastAPI/Starlette REST routes.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Set, Tuple

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator

from aidars.distributed.attempt import AttemptRegistry, AttemptStatus
from aidars.distributed.auth import (
    CredentialStore,
    ReplayGuard,
    make_check_replay,
    make_require_admin,
    make_require_admin_or_any_worker,
    make_require_bootstrap,
    make_require_worker,
    make_require_worker_or_admin,
)
from aidars.distributed.models import (
    CandidateSource,
    ClusterTelemetry,
    FailureCategory,
    HeartbeatPayload,
    HeartbeatResponse,
    LocateAssetsRequest,
    LocateAssetsResponse,
    PingRequest,
    PongResponse,
    WorkerInfo,
    WorkerRegistrationPayload,
    WorkerRegistrationResponse,
    WorkerStatus,
    WorkloadSpec,
    validate_sha256_hex,
)
from aidars.m7.telemetry import TelemetryMemory, TelemetryIngestor
from aidars.m7.controller import M7OrchestratorBridge
from aidars.distributed.artifact import ArtifactRegistry
from aidars.distributed.job_registry import CompletionPolicy, JobRegistry
from aidars.distributed.prioritizer import CandidatePrioritizer, LatencyTracker
from aidars.distributed.registry import ClusterStats, WorkerRegistry
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload_registry import (
    TERMINAL_WORKLOAD_STATES,
    WorkloadIdConflictError,
    WorkloadRecord,
    WorkloadRegistry,
    WorkloadState,
)
from aidars.distributed.workload import WorkloadOrchestrator

logger = logging.getLogger(__name__)

# M9: explicit, documented, deterministic bound on how many WorkloadSpecs
# one Job submission can carry -- an oversized-metadata guard at the
# request-structure level, distinct from WorkloadSpec.parameters' own
# byte-size limit (models.py::MAX_WORKLOAD_PARAMETERS_BYTES).
MAX_SPECS_PER_JOB = 1000

# M9: coarse body-size cap enforced by middleware (see _create_fastapi_app).
# Deliberately generous relative to a single WorkloadSpec/JobSubmitRequest
# so legitimate multi-chunk job submissions aren't squeezed by two
# overlapping limits -- this catches genuinely oversized bodies, the
# per-field limits above catch oversized individual fields.
MAX_REQUEST_BODY_BYTES = 4 * 1024 * 1024  # 4 MiB


class JobSubmitRequest(BaseModel):
    """M8: request body for POST /api/v1/jobs/submit. Lives here rather
    than models.py to avoid a circular import (models.py <- job_registry.py
    <- workload_registry.py <- models.py would cycle if CompletionPolicy
    were imported into models.py directly)."""

    specs: List[WorkloadSpec]
    completion_policy: CompletionPolicy = CompletionPolicy.ALL_REQUIRED
    threshold: Optional[int] = None

    @field_validator("specs")
    @classmethod
    def validate_specs_count(cls, v: List[WorkloadSpec]) -> List[WorkloadSpec]:
        """M9: oversized-metadata guard at the request-structure level."""
        if len(v) > MAX_SPECS_PER_JOB:
            raise ValueError(
                f"job submission exceeds the maximum of {MAX_SPECS_PER_JOB} "
                f"workload specs (got {len(v)})"
            )
        return v


class CoordinatorService:
    """Centralized control plane service for AIDAR distributed asset caching."""

    def __init__(
        self,
        coordinator_id: Optional[str] = None,
        heartbeat_interval_seconds: float = 5.0,
        heartbeat_timeout_seconds: float = 15.0,
        eviction_interval_seconds: float = 5.0,
        penalty_decay_interval_seconds: float = 30.0,
        registry: Optional[WorkerRegistry] = None,
        prioritizer: Optional[CandidatePrioritizer] = None,
        latency_tracker: Optional[LatencyTracker] = None,
        state_store: Optional[CoordinatorStateStore] = None,
        credential_store: Optional[CredentialStore] = None,
    ) -> None:
        self.coordinator_id = coordinator_id or f"coord-{uuid.uuid4().hex[:8]}"
        self.heartbeat_interval_seconds = float(heartbeat_interval_seconds)
        self.heartbeat_timeout_seconds = float(heartbeat_timeout_seconds)
        self.eviction_interval_seconds = float(eviction_interval_seconds)
        self.penalty_decay_interval_seconds = float(penalty_decay_interval_seconds)

        # Optional coordinator-state persistence/recovery (Phase 5.2C).
        # None (the default) preserves pure in-memory behavior exactly as
        # before -- no SQLite dependency, no recovery, unless a store is
        # explicitly injected by the caller.
        self.state_store = state_store

        # M9: authentication foundation. Unlike state_store, this is never
        # None -- there is no legitimate "auth disabled" state other than
        # the explicit AIDAR_INSECURE_MODE opt-in that CredentialStore
        # itself resolves from the environment when not overridden here.
        # Secure by default: a CoordinatorService() with no arguments is
        # fully authenticated unless AIDAR_INSECURE_MODE is explicitly set.
        self.credential_store = credential_store or CredentialStore()
        self.replay_guard = ReplayGuard()

        self.latency_tracker = latency_tracker or LatencyTracker()
        self.prioritizer = prioritizer or CandidatePrioritizer(latency_tracker=self.latency_tracker)
        self.registry = registry or WorkerRegistry(
            heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
            state_store=state_store,
        )
        self.workload_registry = WorkloadRegistry(state_store=state_store)

        # M8: Job/Artifact registries. Same optional-state_store pattern as
        # the registries above -- None (the default) means pure in-memory,
        # no SQLite dependency.
        self.job_registry = JobRegistry(self.workload_registry, state_store=state_store)
        self.artifact_registry = ArtifactRegistry(state_store=state_store)

        # M10.1: same optional-state_store pattern as the registries
        # above -- always constructed (matching job_registry/
        # artifact_registry's own always-on construction here), None
        # state_store means pure in-memory, no SQLite dependency.
        self.attempt_registry = AttemptRegistry(state_store=state_store)

        # M7 Intelligence Initialization
        self.m7_memory = TelemetryMemory()
        self.m7_ingestor = TelemetryIngestor(self.m7_memory)
        self.m7_bridge = M7OrchestratorBridge(self.m7_memory)

        self.orchestrator = WorkloadOrchestrator(
            self.registry,
            self.workload_registry,
            m7_bridge=self.m7_bridge,
            m7_ingestor=self.m7_ingestor,
            job_registry=self.job_registry,
            artifact_registry=self.artifact_registry,
            attempt_registry=self.attempt_registry,
        )

        self._start_time_utc = time.time()
        self._running: bool = False
        self._eviction_task: Optional[asyncio.Task] = None
        self._health_task: Optional[asyncio.Task] = None
        self.app: FastAPI = self._create_fastapi_app()

    # ========================================================================
    # Lifecycle & Background Loops
    # ========================================================================

    async def start(self) -> None:
        """Start the background eviction and decay task.

        If a state_store was injected, this also performs Phase 5.2C
        startup recovery in the order: restore workers -> restore
        workloads -> start normal background loops -> re-drive recovered
        non-terminal workloads. Recovery is a no-op when state_store is
        None, so default construction is unaffected.
        """
        if self._running:
            return
        self._running = True
        pending_redrive = self._restore_persisted_state()
        self._eviction_task = asyncio.create_task(self._run_eviction_loop())
        self._health_task = asyncio.create_task(self._evaluate_cluster_health_loop())
        self._redrive_recovered_workloads(pending_redrive)
        logger.info("CoordinatorService %s started.", self.coordinator_id)

    async def stop(self) -> None:
        """Stop the background eviction task gracefully."""
        if not self._running:
            await self.orchestrator.stop_queue()
            return
        self._running = False
        if self._eviction_task:
            self._eviction_task.cancel()
            try:
                await self._eviction_task
            except asyncio.CancelledError:
                pass
            self._eviction_task = None
            
        if self._health_task:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
            self._health_task = None
        await self.orchestrator.stop_queue()

        logger.info("CoordinatorService %s stopped.", self.coordinator_id)

    def _restore_persisted_state(self) -> List[str]:
        """Phase 5.2C: load persisted workers/workloads from the optional
        state store into the live registries. No-op if state_store is None.

        Worker recovery: restored workers are NOT trusted as live compute
        candidates merely because a row exists in SQLite. Each restored
        WorkerInfo has its status forced to OFFLINE before being inserted
        via the normal WorkerRegistry.register_worker() path -- the same
        status value the registry already excludes from
        list_workers(active_only=True) and from locate_assets_sync's
        candidate scan. A worker only becomes usable again once it proves
        liveness with a fresh heartbeat (WorkerRegistry.record_heartbeat
        promotes OFFLINE -> ACTIVE) or a fresh /workers/register call
        (which always sets status=ACTIVE). No new worker state is
        introduced.

        Workload recovery: every persisted WorkloadRecord is restored
        as-is via WorkloadRegistry.restore_workload() so historical state,
        placement, and execution_result data survive a restart untouched.
        Records whose persisted state is COMPLETED, FAILED, or
        UNSCHEDULABLE (TERMINAL_WORKLOAD_STATES) are left alone. Any other
        persisted state (SUBMITTED, VALIDATING, PLACING, PLACED,
        MIGRATING -- the only states live code actually assigns) had an
        UNKNOWN execution outcome when the coordinator went down, so its
        workload_id is returned for re-drive by _redrive_recovered_workloads.

        M8.10 Job/Artifact recovery: Jobs are restored BEFORE workloads
        (JobRegistry.restore_job() just inserts durable identity/
        membership/policy -- it doesn't need the workloads to exist yet).
        Artifacts are restored after, order-independent either way since
        ArtifactRegistry doesn't read WorkloadRegistry/JobRegistry.
        "Reconstruct Job aggregate" from the PRD's recovery flow requires
        no explicit code here: JobAggregate is never stored, only derived
        (JobRegistry.get_aggregate()), so the instant both a Job's
        membership and its member WorkloadRecords are restored, its
        aggregate is already correct on the next read -- there is no
        separate reconstruction step that could disagree with reality.

        Returns the list of workload_ids that need re-driving. Re-driving
        itself is deferred to a separate call so the caller can start the
        normal background loops first, per the phase's specified startup
        order.
        """
        if self.state_store is None:
            return []

        restored_workers = self.state_store.load_workers()
        for info in restored_workers:
            info.status = WorkerStatus.OFFLINE
            # The persisted last_heartbeat_utc is from before the crash and
            # is almost certainly already older than heartbeat_timeout_seconds
            # by the time the coordinator restarts. Without resetting it, the
            # very next eviction loop pass would immediately purge (and, per
            # Phase 5.2C's delete wiring, un-persist) every restored worker
            # before it gets any chance to send a real heartbeat -- defeating
            # the point of restoring it as OFFLINE/unverified rather than
            # just dropping it. Resetting it to now grants the same
            # heartbeat_timeout_seconds grace window a brand-new worker gets;
            # it does not mark the worker ACTIVE or trusted, only un-expired.
            info.last_heartbeat_utc = time.time()
            self.registry.register_worker(info)
        if restored_workers:
            logger.info(
                "Coordinator %s restored %d worker(s) from persisted state (unverified until next heartbeat/registration).",
                self.coordinator_id, len(restored_workers),
            )

        restored_jobs = self.state_store.load_jobs()
        for job in restored_jobs:
            self.job_registry.restore_job(job)
        if restored_jobs:
            logger.info(
                "Coordinator %s restored %d job(s) from persisted state.",
                self.coordinator_id, len(restored_jobs),
            )

        restored_workloads = self.state_store.load_workloads()
        pending_redrive: List[str] = []
        for record in restored_workloads:
            self.workload_registry.restore_workload(record)
            if record.state not in TERMINAL_WORKLOAD_STATES:
                pending_redrive.append(record.spec.workload_id)
        if restored_workloads:
            logger.info(
                "Coordinator %s restored %d workload(s) from persisted state (%d non-terminal, pending re-drive).",
                self.coordinator_id, len(restored_workloads), len(pending_redrive),
            )

        # M10.1/M10.18: Attempts are restored after Workloads (an Attempt
        # references a workload_id, mirroring Job->Workload ordering
        # above) and before Artifacts (order-independent either way,
        # kept consistent with the Job->Workload->Attempt->Artifact
        # relationship documented in attempt.py).
        #
        # M10.6: any attempt that was ASSIGNED or RUNNING when the
        # coordinator crashed has an UNKNOWN outcome -- the worker may
        # have completed it, may still be working on it, or may itself
        # be gone. It is marked LOST here (see AttemptStatus.LOST's
        # docstring) BEFORE _redrive_recovered_workloads() fires, so the
        # redrive creates a fresh, numbered attempt rather than leaving
        # the stale one looking perpetually "in progress".
        restored_attempts = self.state_store.load_attempts()
        lost_count = 0
        for attempt in restored_attempts:
            if attempt.status in (AttemptStatus.ASSIGNED, AttemptStatus.RUNNING):
                attempt.status = AttemptStatus.LOST
                attempt.finished_at = attempt.finished_at or time.time()
                attempt.failure_category = FailureCategory.WORKER_UNAVAILABLE
                attempt.failure_reason = (
                    attempt.failure_reason or "coordinator restarted while this attempt was in flight"
                )
                lost_count += 1
            self.attempt_registry.restore_attempt(attempt)
        if restored_attempts:
            logger.info(
                "Coordinator %s restored %d attempt(s) from persisted state (%d marked LOST as in-flight-at-crash).",
                self.coordinator_id, len(restored_attempts), lost_count,
            )

        restored_artifacts = self.state_store.load_artifacts()
        for artifact in restored_artifacts:
            self.artifact_registry.restore_artifact(artifact)
        if restored_artifacts:
            logger.info(
                "Coordinator %s restored %d artifact(s) from persisted state.",
                self.coordinator_id, len(restored_artifacts),
            )

        return pending_redrive

    def _redrive_recovered_workloads(self, workload_ids: List[str]) -> None:
        """Re-dispatch recovered non-terminal workloads through the
        existing WorkloadOrchestrator._process_workload() path -- the same
        dispatcher used for freshly-submitted workloads, not a second,
        recovery-specific dispatch mechanism.

        AT-LEAST-ONCE, NOT EXACTLY-ONCE: a workload that was actually
        EXECUTING on a worker when the coordinator crashed may have
        already completed there; the coordinator has no way to know that
        and will dispatch it again here, which can duplicate compute.
        CAS content-addressing makes duplicate *output storage* safe
        (identical hash -> identical bytes, second write is a no-op), but
        it does not prevent the duplicate *compute* itself. Exactly-once
        execution, worker-side execution IDs, and cross-worker
        reconciliation are explicitly out of scope for this phase.
        """
        for workload_id in workload_ids:
            record = self.workload_registry.get_workload(workload_id)
            print("API RECORD EXPLANATION:", record.placement_explanation if record else None)
            print("API RECORD DECISION:", record.placement_decision if record else None)
            if record is not None and record.state not in TERMINAL_WORKLOAD_STATES:
                # Re-enter the single admission queue while preserving the durable
                # WorkloadRecord and its original submitted_at age.
                self.workload_registry.set_placement(workload_id, None)
                self.workload_registry.set_placement_explanation(workload_id, {})
                self.workload_registry.update_state(workload_id, WorkloadState.SUBMITTED)
                self.orchestrator.enqueue_workload(workload_id)

    async def _run_eviction_loop(self) -> None:
        """Periodic loop that purges expired dead workers and decays penalties."""
        last_decay_time = time.time()
        while self._running:
            try:
                await asyncio.sleep(self.eviction_interval_seconds)
                if not self._running:
                    break

                # 1. Evict expired workers
                evicted = self.registry.evict_expired_workers(
                    timeout_seconds=self.heartbeat_timeout_seconds
                )
                if evicted:
                    logger.warning(
                        "Coordinator evicted %d expired workers: %s", len(evicted), evicted
                    )
                    # M10.6: reclassify any attempt this worker was
                    # actively running -- see WorkloadOrchestrator.
                    # handle_worker_lost()'s docstring for why this does
                    # not itself trigger a competing dispatch.
                    for wid in evicted:
                        self.orchestrator.handle_worker_lost(wid)

                # 2. Periodic penalty decay
                now = time.time()
                if now - last_decay_time >= self.penalty_decay_interval_seconds:
                    self.registry.decay_penalties(current_time=now)
                    last_decay_time = now

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Error in coordinator background eviction loop: %s", exc, exc_info=True)

    async def _evaluate_cluster_health_loop(self) -> None:
        """Periodic loop that evaluates M7 predictive anomalies to drain unhealthy workers."""
        from aidars.m7.policy import AdaptivePolicyEngine
        from aidars.m7.controller import M7OrchestratorBridge
        from aidars.distributed.models import WorkerStatus
        from aidars.m7.behavior import BehaviorInferencer
        from aidars.m7.contracts import WorkerState as M7WorkerState
        
        while self._running:
            try:
                await asyncio.sleep(self.eviction_interval_seconds)
                if not self._running:
                    break
                    
                # Evaluate cluster policy using M7 memory
                active_workers = list(self.m7_memory.workers.values())
                print(f"DEBUG LOOP: Evaluated {len(active_workers)} active workers")
                
                # Check for Early Warnings (M7.16)
                for worker_state in active_workers:
                    # Construct features from temporal state directly for health check
                    from aidars.m7.contracts import WorkerFeatureVector
                    
                    features = WorkerFeatureVector(
                        cpu_available_ratio=1.0 - worker_state.cpu_utilization_ema.value,
                        ram_available_ratio=1.0 - worker_state.ram_utilization_ema.value,
                        vram_available_ratio=1.0,
                        has_gpu=0.0,
                        active_workload_ratio=0.0,
                        cache_locality_ratio=0.0,
                        heartbeat_stability=1.0,
                        recent_failure_rate=worker_state.failure_rate_ema.value,
                        recent_latency_normalized=min(1.0, worker_state.latency_ema.value / 100.0),
                        throughput_normalized=0.5
                    )
                    
                    behavior = BehaviorInferencer.infer_worker_behavior(worker_state.worker_id, features)
                    
                    if behavior.state in (M7WorkerState.DEGRADED, M7WorkerState.ERRATIC, M7WorkerState.OVERLOADED):
                        # Mark worker as DRAINING in M6 registry (M7.17)
                        wid = worker_state.worker_id
                        worker_info = self.registry.get_worker(wid)
                        if worker_info and worker_info.status == WorkerStatus.ACTIVE:
                            logger.warning(f"Worker {wid} marked as DRAINING due to M7 behavioral risk: {behavior.state.value}")
                            self.registry._workers[wid].status = WorkerStatus.DRAINING
                            # Kick off workload migration for all active workloads on this worker (M7.18)
                            asyncio.create_task(self.orchestrator.drain_worker(wid))
                    elif behavior.state == M7WorkerState.STABLE:
                        # M10.8: a worker previously put into DRAINING by
                        # this same behavioral signal can transition back
                        # to ACTIVE once that signal genuinely recovers
                        # to STABLE. This reuses the existing M7 health
                        # evaluation already running here rather than
                        # inventing a second monitoring mechanism -- no
                        # other code path in this codebase revives a
                        # DRAINING worker (heartbeat revival only handles
                        # OFFLINE -> ACTIVE; see WorkerRegistry.
                        # record_heartbeat), so without this the DRAINING
                        # state was previously a one-way trap.
                        wid = worker_state.worker_id
                        worker_info = self.registry.get_worker(wid)
                        if worker_info and worker_info.status == WorkerStatus.DRAINING:
                            logger.info(f"Worker {wid} behavioral risk recovered to STABLE; reviving DRAINING -> ACTIVE")
                            self.registry._workers[wid].status = WorkerStatus.ACTIVE

            except Exception as exc:
                logger.error("Coordinator health loop error: %s", exc)

    # ========================================================================
    # Programmatic / Core API Methods
    # ========================================================================

    def register_worker_sync(self, payload: WorkerRegistrationPayload) -> WorkerRegistrationResponse:
        """Register worker programmatically without HTTP overhead."""
        received_at = time.time()
        resource_profile = None
        if payload.resource_profile is not None and payload.resource_profile.timestamp_utc is not None:
            resource_profile = payload.resource_profile.model_copy(update={
                "worker_id": payload.worker_id,
                "endpoint_url": payload.endpoint_url,
                "ip_address": payload.ip_address,
                "status": WorkerStatus.ACTIVE,
                "local_cached_hashes": set(payload.inventory_hashes),
                "timestamp_utc": received_at,
                "can_execute_workloads": payload.can_execute_workloads,
            })
        worker_info = WorkerInfo(
            worker_id=payload.worker_id,
            endpoint_url=payload.endpoint_url,
            ip_address=payload.ip_address,
            port=payload.port,
            hostname=payload.hostname,
            status=WorkerStatus.ACTIVE,
            capacity_bytes=payload.capacity_bytes,
            used_bytes=payload.used_bytes,
            capabilities=payload.capabilities,
            inventory_hashes=payload.inventory_hashes,
            last_heartbeat_utc=received_at,
            registered_at_utc=received_at,
            tags=payload.tags,
            resource_profile=resource_profile,
            can_execute_workloads=payload.can_execute_workloads,
        )
        registered = self.registry.register_worker(worker_info)
        # M9: mint a fresh per-worker credential on every successful
        # registration (including re-registration after a restart/crash),
        # replacing any previous one. Returned exactly once, in this
        # response, never logged or persisted.
        issued_credential = self.credential_store.issue_worker_credential(registered.worker_id)
        return WorkerRegistrationResponse(
            status="registered",
            worker_id=registered.worker_id,
            coordinator_id=self.coordinator_id,
            heartbeat_interval_seconds=self.heartbeat_interval_seconds,
            heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
            registered_at_utc=registered.registered_at_utc,
            acknowledged_inventory_count=len(registered.inventory_hashes),
            worker_credential=issued_credential,
        )

    def locate_assets_sync(
        self,
        req: LocateAssetsRequest,
        client_host: Optional[str] = None,
    ) -> LocateAssetsResponse:
        """Locate candidate workers for missing assets programmatically with batch resolution."""
        requester_ip = req.requester_ip
        if not requester_ip:
            req_worker = self.registry.get_worker(req.requester_worker_id, copy=False)
            if req_worker:
                requester_ip = req_worker.ip_address
            elif client_host:
                requester_ip = client_host
            else:
                requester_ip = "127.0.0.1"

        locations: Dict[str, List[CandidateSource]] = {}
        unresolved_hashes: List[str] = []

        # 1. Single lock reads: worker map and inverted hash locations
        worker_map = self.registry.get_workers_map()
        located_map = self.registry.locate_hashes(req.missing_hashes)

        # 2. Pre-evaluate candidate score & CandidateSource once per eligible worker
        with self.prioritizer._lock:
            error_snapshot = dict(self.prioritizer._error_counts)

        evaluated_candidates: Dict[str, Tuple[float, CandidateSource]] = {}
        for wid, w in worker_map.items():
            if req.requester_worker_id and wid == req.requester_worker_id:
                continue
            if w.status == WorkerStatus.OFFLINE:
                continue
            if not req.include_degraded and w.status in (WorkerStatus.DEGRADED, WorkerStatus.UNHEALTHY):
                continue

            evaluated_candidates[wid] = self.prioritizer.evaluate_candidate(
                requester_ip=requester_ip,
                worker=w,
                error_snapshot=error_snapshot,
            )

        # 3. Assemble and sort candidates for each missing hash
        max_cands = req.max_candidates_per_asset
        for raw_h in req.missing_hashes:
            try:
                norm_h = validate_sha256_hex(raw_h)
            except ValueError:
                unresolved_hashes.append(raw_h)
                locations[raw_h] = []
                continue

            worker_ids = located_map.get(norm_h)
            if not worker_ids:
                locations[raw_h] = []
                unresolved_hashes.append(norm_h)
                continue

            candidates = [
                evaluated_candidates[wid]
                for wid in worker_ids
                if wid in evaluated_candidates
            ]

            if not candidates:
                locations[raw_h] = []
                unresolved_hashes.append(norm_h)
                continue

            candidates.sort(key=lambda item: item[0], reverse=True)
            if max_cands and max_cands > 0:
                candidates = candidates[:max_cands]

            locations[raw_h] = [item[1] for item in candidates]

        return LocateAssetsResponse(
            locations=locations,
            unresolved_hashes=unresolved_hashes,
        )

    def get_cluster_stats_sync(self) -> ClusterTelemetry:
        """Retrieve aggregated cluster statistics."""
        workers = self.registry.list_workers(active_only=False)
        stats = self.registry.get_cluster_stats()

        total_workers = len(workers)
        active = 0
        degraded = 0
        unhealthy = 0
        offline = 0
        total_inventory = 0
        active_transfers = 0

        for w in workers:
            total_inventory += len(w.inventory_hashes)
            active_transfers += getattr(w, "active_transfers", 0)
            if w.status == WorkerStatus.ACTIVE:
                active += 1
            elif w.status == WorkerStatus.DEGRADED:
                degraded += 1
            elif w.status == WorkerStatus.UNHEALTHY:
                unhealthy += 1
            elif w.status == WorkerStatus.OFFLINE:
                offline += 1

        uptime = max(0.0, time.time() - self._start_time_utc)

        return ClusterTelemetry(
            coordinator_id=self.coordinator_id,
            uptime_seconds=round(uptime, 2),
            total_registered_workers=total_workers,
            active_workers=active,
            degraded_workers=degraded,
            unhealthy_workers=unhealthy,
            offline_workers=offline,
            unique_cached_assets_count=stats.total_unique_hashes,
            total_inventory_records=total_inventory,
            total_cluster_capacity_bytes=stats.total_capacity_bytes,
            total_cluster_used_bytes=stats.total_used_bytes,
            aggregate_active_transfers=active_transfers,
        )

    # ========================================================================
    # FastAPI Application Setup
    # ========================================================================

    def _create_fastapi_app(self) -> FastAPI:
        """Construct the FastAPI application with registered control plane routes."""

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            await self.start()
            yield
            await self.stop()

        app = FastAPI(
            title="AIDAR Distributed Asset Coordinator",
            description="Control plane REST API for worker registration, heartbeats, and asset location.",
            version="1.0.0",
            lifespan=lifespan,
        )

        # M9: coarse request-body-size guard. Checks Content-Length only
        # (cheap, no body read) -- a chunked-encoding request with no
        # declared length is not caught here; that's a documented boundary
        # rather than an attempt at full streaming-size enforcement, which
        # would be more infrastructure than this milestone's scope calls
        # for. Per-field limits (WorkloadSpec.parameters, JobSubmitRequest
        # .specs) catch oversized content within an otherwise-valid body.
        @app.middleware("http")
        async def limit_request_body_size(request: Request, call_next):
            content_length = request.headers.get("content-length")
            if content_length is not None:
                try:
                    declared_size = int(content_length)
                except ValueError:
                    declared_size = None
                if declared_size is not None and declared_size > MAX_REQUEST_BODY_BYTES:
                    return JSONResponse(
                        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                        content={"detail": f"Request body exceeds maximum size of {MAX_REQUEST_BODY_BYTES} bytes"},
                    )
            return await call_next(request)

        # M9: auth dependencies, built once here from this service's own
        # CredentialStore/ReplayGuard, per the locked authorization matrix.
        require_admin = make_require_admin(self.credential_store)
        require_bootstrap = make_require_bootstrap(self.credential_store)
        require_worker = make_require_worker(self.credential_store)
        require_worker_or_admin = make_require_worker_or_admin(self.credential_store)
        require_admin_or_any_worker = make_require_admin_or_any_worker(self.credential_store)
        check_replay = make_check_replay(self.credential_store, self.replay_guard)

        router = APIRouter(prefix="/api/v1")

        # --- Worker Registration ---
        @router.post(
            "/workers/register",
            response_model=WorkerRegistrationResponse,
            status_code=status.HTTP_200_OK,
            summary="Register a new or returning worker node",
            dependencies=[Depends(require_bootstrap), Depends(check_replay)],
        )
        async def register_worker(payload: WorkerRegistrationPayload) -> WorkerRegistrationResponse:
            try:
                return self.register_worker_sync(payload)
            except ValueError as exc:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

        # --- Worker Heartbeat ---
        @router.post(
            "/workers/{worker_id}/heartbeat",
            response_model=HeartbeatResponse,
            status_code=status.HTTP_200_OK,
            summary="Record periodic heartbeat and telemetry metrics",
            dependencies=[Depends(require_worker)],
        )
        async def worker_heartbeat(
            worker_id: str,
            payload: Optional[HeartbeatPayload] = None,
        ) -> HeartbeatResponse:
            now = time.time()
            if not self.registry.has_worker(worker_id):
                return HeartbeatResponse(
                    status="re_register_required",
                    acknowledged_at_utc=now,
                    coordinator_time_utc=now,
                    re_register_required=True,
                )

            recorded = self.registry.record_heartbeat(
                worker_id=worker_id,
                payload=payload,
                current_time=now,
            )

            if not recorded:
                return HeartbeatResponse(
                    status="re_register_required",
                    acknowledged_at_utc=now,
                    coordinator_time_utc=now,
                    re_register_required=True,
                )

            # Route to M7 Ingestor
            worker = self.registry.get_worker(worker_id)
            if worker and worker.resource_profile:
                self.m7_ingestor.on_worker_heartbeat(
                    worker_id=worker_id,
                    cpu_utilization_percent=worker.resource_profile.cpu_utilization_percent,
                    ram_total=worker.resource_profile.ram_total_bytes,
                    ram_available=worker.resource_profile.ram_available_bytes,
                    failed=False,
                    latency_ms=0.0  # TODO: compute from RTT
                )

            return HeartbeatResponse(
                status="healthy",
                acknowledged_at_utc=now,
                coordinator_time_utc=now,
                re_register_required=False,
            )

        # --- Worker Unregister ---
        @router.post(
            "/workers/{worker_id}/unregister",
            status_code=status.HTTP_200_OK,
            summary="Gracefully unregister a worker and prune its assets",
            dependencies=[Depends(require_worker_or_admin)],
        )
        async def unregister_worker(worker_id: str) -> Dict[str, Any]:
            unregistered = self.registry.unregister_worker(worker_id)
            if not unregistered:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Worker '{worker_id}' is not registered.",
                )
            return {"status": "unregistered", "worker_id": worker_id}

        # --- Missing Asset Location ---
        @router.post(
            "/assets/locate",
            response_model=LocateAssetsResponse,
            status_code=status.HTTP_200_OK,
            summary="Locate candidate worker nodes for missing assets",
            dependencies=[Depends(require_admin_or_any_worker)],
        )
        async def locate_assets(
            req: LocateAssetsRequest,
            request: Request,
        ) -> LocateAssetsResponse:
            client_host = request.client.host if request.client else None
            return self.locate_assets_sync(req, client_host=client_host)

        # --- Cluster Stats & Telemetry ---
        @router.get(
            "/cluster/stats",
            response_model=ClusterTelemetry,
            status_code=status.HTTP_200_OK,
            summary="Retrieve global cluster statistics and health status",
            dependencies=[Depends(require_admin)],
        )
        async def cluster_stats() -> ClusterTelemetry:
            return self.get_cluster_stats_sync()

        # --- Worker Queries ---
        # M9: admin/client only. WorkerInfo exposes every worker's IP,
        # port, capacity/utilization, health metrics, and full CAS
        # inventory -- a worker has no functional need for this (its real
        # peer-discovery need is served by /assets/locate, scoped to the
        # specific hashes it actually asked about), so full-registry
        # visibility is not extended to workers.
        @router.get(
            "/workers",
            response_model=List[WorkerInfo],
            status_code=status.HTTP_200_OK,
            summary="List all currently registered workers",
            dependencies=[Depends(require_admin)],
        )
        async def list_workers(active_only: bool = True) -> List[WorkerInfo]:
            return self.registry.list_workers(active_only=active_only)

        @router.get(
            "/workers/{worker_id}",
            response_model=WorkerInfo,
            status_code=status.HTTP_200_OK,
            summary="Get specific worker details",
            dependencies=[Depends(require_admin)],
        )
        async def get_worker(worker_id: str) -> WorkerInfo:
            worker = self.registry.get_worker(worker_id)
            if not worker:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Worker '{worker_id}' not found.",
                )
            return worker

        # --- PING / PONG ---
        @router.post(
            "/ping",
            response_model=PongResponse,
            status_code=status.HTTP_200_OK,
            summary="PING / PONG RTT latency probe",
        )
        async def ping_post(payload: PingRequest) -> PongResponse:
            return PongResponse(
                worker_id=self.coordinator_id,
                client_timestamp_utc=payload.client_timestamp_utc,
                server_timestamp_utc=time.time(),
                status="pong",
                sequence_number=payload.sequence_number,
            )

        @router.get(
            "/ping",
            response_model=PongResponse,
            status_code=status.HTTP_200_OK,
            summary="GET PING / PONG health probe",
        )
        async def ping_get() -> PongResponse:
            now = time.time()
            return PongResponse(
                worker_id=self.coordinator_id,
                client_timestamp_utc=now,
                server_timestamp_utc=now,
                status="pong",
            )

        # --- Workloads ---
        @router.post(
            "/workloads/submit",
            response_model=Dict[str, str],
            status_code=status.HTTP_202_ACCEPTED,
            summary="Submit a computational workload",
            dependencies=[Depends(require_admin), Depends(check_replay)],
        )
        async def submit_workload(spec: WorkloadSpec) -> Dict[str, str]:
            try:
                workload_id = await self.orchestrator.submit_workload(spec)
            except WorkloadIdConflictError as exc:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
            return {"workload_id": workload_id, "status": "submitted"}

        @router.get(
            "/workloads/{workload_id}",
            status_code=status.HTTP_200_OK,
            summary="Get workload status",
            dependencies=[Depends(require_admin)],
        )
        async def get_workload_status(workload_id: str) -> Dict[str, Any]:
            record = self.workload_registry.get_workload(workload_id)

            if not record:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Workload '{workload_id}' not found.",
                )

            # M11: Prefer the persisted detailed placement explanation.
            # Fall back to the legacy PlacementDecision-derived explanation
            # only when no explicit explanation has been recorded.
            if record.placement_explanation:
                placement_explanation = record.placement_explanation
            elif record.placement_decision:
                placement_explanation = {
                    "candidates": record.placement_decision.candidate_explanations,
                    "eligible_worker_ids": [
                        c["worker_id"]
                        for c in record.placement_decision.candidate_explanations
                        if c.get("eligible")
                    ],
                    "selected_worker_id": record.placement_decision.selected_worker_id,
                }
            else:
                placement_explanation = {}

            resp = {
                "workload_id": workload_id,
                "state": record.state.value,
                "submitted_at": record.submitted_at,
                "completed_at": record.completed_at,
                "error_message": record.error_message,
                "placement_explanation": placement_explanation,
                "queue": self.orchestrator.queue_snapshot(workload_id),
                "scheduling": {
                    "priority": record.spec.priority,
                    "normalized_priority": min(100, record.spec.priority),
                    "effective_priority_range": [0, 100],
                    "priority_direction": "larger_is_higher",
                    "admission_queue": {
                        "max_dispatch_tasks": self.orchestrator.max_dispatch_tasks,
                        "responsibility": "workload_admission_order_only",
                    },
                    "aging": {
                        "points_per_minute": 10,
                        "maximum_bonus": 100,
                        "age_source": "original_submitted_at",
                        "starvation_override_after_seconds": (
                            self.orchestrator.STARVATION_PREVENTION_SECONDS
                        ),
                        "override_order": (
                            "oldest_submitted_at_then_workload_id"
                        ),
                    },
                    "deadline_state": self.orchestrator._deadline_state(
                        record.spec,
                        time.time(),
                    ),
                    "deadline_at_utc": record.spec.deadline_at_utc,
                    "affinity_group_id": record.spec.affinity_group_id,
                    "affinity_mode": record.spec.affinity_mode,
                    "affinity_tag_key": record.spec.affinity_tag_key,
                },
            }

            if record.placement_decision:
                resp["placement"] = record.placement_decision.model_dump()

            if record.execution_result:
                resp["result"] = record.execution_result.model_dump()

            # M10.17: expose attempt/retry/timing/checkpoint state through
            # the existing workload API surface rather than a new endpoint.
            attempts = self.attempt_registry.list_attempts_for_workload(
                workload_id
            )
            resp["attempts"] = [a.to_summary_dict() for a in attempts]
            resp["attempt_count"] = len(attempts)

            return resp

        # --- Jobs (M8) ---
        @router.post(
            "/jobs/submit",
            response_model=Dict[str, str],
            status_code=status.HTTP_202_ACCEPTED,
            summary="Submit a Job (a durable grouping of one or more workloads)",
            dependencies=[Depends(require_admin), Depends(check_replay)],
        )
        async def submit_job(req: JobSubmitRequest) -> Dict[str, str]:
            try:
                job_id = await self.orchestrator.submit_job(
                    req.specs, completion_policy=req.completion_policy, threshold=req.threshold,
                )
            except WorkloadIdConflictError as exc:
                # More specific than plain ValueError -- an existing
                # workload_id under a different spec is a conflict with
                # existing state (409), not a malformed request (400).
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc))
            except ValueError as exc:
                # Expected input-validation errors from submit_job() itself
                # (empty specs, specs with conflicting job_ids) or from
                # JobRecord construction (THRESHOLD policy without a
                # threshold) -- same convention as /workers/register.
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
            return {"job_id": job_id, "status": "submitted"}

        @router.get(
            "/jobs/{job_id}",
            status_code=status.HTTP_200_OK,
            summary="Get Job status (derived aggregate over its member workloads)",
            dependencies=[Depends(require_admin)],
        )
        async def get_job_status(job_id: str) -> Dict[str, Any]:
            job = self.job_registry.get_job(job_id)
            if not job:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Job '{job_id}' not found.",
                )
            aggregate = self.job_registry.get_aggregate(job_id)
            return {
                "job_id": job_id,
                "workload_ids": sorted(job.workload_ids),
                "completion_policy": job.completion_policy.value,
                "threshold": job.threshold,
                "created_at": job.created_at,
                "state": aggregate.state.value,
                "total": aggregate.total,
                "completed": aggregate.completed,
                "failed": aggregate.failed,
                "pending": aggregate.pending,
                "running": aggregate.running,
                "output_asset_hashes": sorted(aggregate.output_asset_hashes),
            }

        # --- Artifacts (M9 minimal retrieval surface) ---
        # M9 requires "unauthorized artifact retrieval" to fail -- there was
        # no artifact HTTP surface at all before this, so nothing existed to
        # secure. This is the minimum needed to make that requirement
        # testable: metadata only (no CAS byte-serving), admin/client only,
        # matching the exact pattern of /jobs/{job_id} and
        # /workloads/{workload_id}. Not a general artifact service -- no
        # list endpoint, no byte download, no per-artifact ACLs.
        @router.get(
            "/artifacts/{artifact_id}",
            status_code=status.HTTP_200_OK,
            summary="Get Artifact metadata (minimal M9 retrieval surface)",
            dependencies=[Depends(require_admin)],
        )
        async def get_artifact(artifact_id: str) -> Dict[str, Any]:
            artifact = self.artifact_registry.get_artifact(artifact_id)
            if not artifact:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Artifact '{artifact_id}' not found.",
                )
            return {
                "artifact_id": artifact.artifact_id,
                "content_hash": artifact.content_hash,
                "producer_workload_id": artifact.producer_workload_id,
                "producer_job_id": artifact.producer_job_id,
                "created_at": artifact.created_at,
                "verification_state": artifact.verification_state.value,
                "lifecycle_state": artifact.lifecycle_state.value,
                "storage_location": artifact.storage_location,
                "size_bytes": artifact.size_bytes,
            }

        app.include_router(router)
        return app
