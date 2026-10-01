"""Workload lifecycle orchestration.

Ties together models, placement, and registry to drive a workload from SUBMITTED to COMPLETED.
"""

import asyncio
import logging
import time
import uuid
import httpx
from typing import List, Optional, Set

from aidars.distributed.artifact import ArtifactRegistry
from aidars.distributed.attempt import AttemptRegistry, AttemptStatus
from aidars.distributed.job_registry import CompletionPolicy, JobRegistry
from aidars.distributed.models import FailureCategory, WorkerStatus, WorkloadSpec, WorkloadExecutionResult
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.retry import DEFAULT_MAX_ATTEMPTS, should_retry
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState

logger = logging.getLogger(__name__)


class WorkloadOrchestrator:
    """Central orchestrator for workload lifecycle on the Coordinator."""

    STARVATION_PREVENTION_SECONDS = 600.0

    def __init__(
        self,
        registry: WorkerRegistry,
        workload_registry: WorkloadRegistry,
        m7_bridge=None,
        m7_ingestor=None,
        job_registry: Optional[JobRegistry] = None,
        artifact_registry: Optional[ArtifactRegistry] = None,
        attempt_registry: Optional[AttemptRegistry] = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        max_dispatch_tasks: int = 16,
    ) -> None:
        self.registry = registry
        self.workload_registry = workload_registry
        self.m7_ingestor = m7_ingestor
        self.placement_engine = PlacementEngine(m7_bridge=m7_bridge)
        # Serializes placement through coordinator-side attempt assignment so
        # simultaneous submissions cannot all consume the same concurrency
        # slot based on an identical snapshot.
        self._placement_lock = asyncio.Lock()

        # M8: optional -- lazily constructed on first use by submit_job()
        # if not injected, so every existing caller/test that only ever
        # calls submit_workload() sees zero behavior change and doesn't
        # need to know these exist.
        self.job_registry = job_registry
        self.artifact_registry = artifact_registry

        # M10.1/M10.5: optional -- None preserves pre-M10 behavior exactly
        # (no attempt records, no retry-budget loop beyond a single try),
        # matching the same "None means the feature isn't wired in" idiom
        # already established for job_registry/artifact_registry above.
        self.attempt_registry = attempt_registry
        self.max_attempts = max_attempts
        self.max_dispatch_tasks = max(1, int(max_dispatch_tasks))
        self._queue_event = asyncio.Event()
        self._queue_task: Optional[asyncio.Task] = None
        self._dispatch_tasks: Set[asyncio.Task] = set()
        self._queued_ids: Set[str] = set()
        self._processing_ids: Set[str] = set()

    async def submit_workload(self, spec: WorkloadSpec) -> str:
        """Submit a new workload for execution. Returns the workload_id."""
        record = self.workload_registry.add_workload(spec)

        self.enqueue_workload(spec.workload_id)

        return spec.workload_id

    def enqueue_workload(self, workload_id: str) -> None:
        """Queue an existing durable workload record for admission."""
        if workload_id in self._processing_ids:
            return
        record = self.workload_registry.get_workload(workload_id)
        if record is None or record.state in (WorkloadState.COMPLETED, WorkloadState.FAILED,
                                               WorkloadState.TIMEOUT, WorkloadState.UNSCHEDULABLE):
            return
        self._queued_ids.add(workload_id)
        self._ensure_queue_task()
        self._queue_event.set()

    def enqueue_recovered_workloads(self, workload_ids: List[str]) -> None:
        for workload_id in workload_ids:
            self.enqueue_workload(workload_id)

    def _ensure_queue_task(self) -> None:
        if self._queue_task is None or self._queue_task.done():
            self._queue_task = asyncio.create_task(self._admission_queue_loop())

    def _effective_priority(self, record, now: float) -> float:
        # Effective priority is 0..100 (larger is more urgent); values above
        # 100 from legacy callers are accepted but saturated for ordering.
        # Aging adds at most 100 points at 10 points/minute. A separate 10-minute oldest-first
        # promotion guarantees service even under a continuous stream of
        # newly arriving higher-priority/deadline-urgent work.
        age_minutes = max(0.0, now - record.submitted_at) / 60.0
        normalized_priority = min(100.0, float(record.spec.priority))
        return (normalized_priority + min(100.0, age_minutes * 10.0)
                + self._deadline_priority_bonus(record.spec, now))

    def _predicted_duration(self, spec: WorkloadSpec) -> tuple[Optional[float], float]:
        """Use M7's observed duration EMA when available; otherwise use the
        declared deterministic estimate as a soft fallback, never as a guarantee.
        """
        memory = getattr(self.placement_engine.m7_bridge, "memory", None)
        state = memory.get_workload_state(spec.task_type) if memory is not None else None
        if state is not None and state.duration_ema.initialized and state.duration_ema.value > 0:
            return float(state.duration_ema.value), float(state.duration_ema.value)
        return None, float(spec.estimated_duration_seconds)

    def _deadline_priority_bonus(self, spec: WorkloadSpec, now: float) -> float:
        if spec.deadline_at_utc is None:
            return 0.0
        _, duration = self._predicted_duration(spec)
        slack = spec.deadline_at_utc - now - duration
        if spec.deadline_at_utc < now or slack <= 0:
            return 100.0
        # Within ten predicted durations, urgency grows deterministically
        # toward a bounded 100 point bonus. Farther deadlines add no urgency.
        return max(0.0, 100.0 - (slack / max(duration, 1.0)) * 10.0) if slack <= 10 * duration else 0.0

    def _trusted_asset_sizes(self, spec: WorkloadSpec) -> dict:
        """Reuse verified Artifact sizes. Missing/unverified sizes stay unknown;
        configured bandwidth caps and caller estimates are not used as byte facts.
        """
        if self.artifact_registry is None:
            return {}
        sizes = {}
        for artifact in self.artifact_registry.list_artifacts():
            verification = getattr(artifact.verification_state, "value", artifact.verification_state)
            if (artifact.content_hash in spec.input_asset_hashes and verification == "verified"
                    and artifact.size_bytes is not None and artifact.size_bytes >= 0):
                sizes[artifact.content_hash] = artifact.size_bytes
        return sizes

    def queue_snapshot(self, workload_id: Optional[str] = None) -> List[dict]:
        now = time.time()
        rows = []
        for record in self.workload_registry.list_workloads():
            wid = record.spec.workload_id
            is_dispatching = wid in self._processing_ids
            if record.state != WorkloadState.SUBMITTED and not is_dispatching:
                continue
            predicted_duration, _ = self._predicted_duration(record.spec)
            rows.append({"workload_id": wid, "state": "dispatching" if is_dispatching else "pending",
                         "priority": record.spec.priority,
                         "normalized_priority": min(100, record.spec.priority),
                         "effective_priority": self._effective_priority(record, now),
                         "deadline_priority_bonus": self._deadline_priority_bonus(record.spec, now),
                         "submitted_at": record.submitted_at,
                         "deadline_at_utc": record.spec.deadline_at_utc,
                         "deadline_state": self._deadline_state(record.spec, now),
                         "predicted_duration_seconds": predicted_duration,
                         "duration_estimate_source": "m7_history" if predicted_duration is not None else "workload_estimate_fallback",
                         "starvation_override": now - record.submitted_at >= self.STARVATION_PREVENTION_SECONDS,
                         "queued": record.spec.workload_id in self._queued_ids,
                         "dispatching": record.spec.workload_id in self._processing_ids})
        rows.sort(key=lambda r: (0, r["submitted_at"], r["workload_id"])
                  if r["starvation_override"] else
                  (1, -r["effective_priority"], r["submitted_at"], r["workload_id"]))
        pending_index = 0
        for row in rows:
            if row["state"] == "pending":
                pending_index += 1
                row["queue_position"] = pending_index
            else:
                row["queue_position"] = None
        return [row for row in rows if workload_id is None or row["workload_id"] == workload_id]

    def _deadline_state(self, spec: WorkloadSpec, now: float) -> str:
        if spec.deadline_at_utc is None:
            return "none"
        if spec.deadline_at_utc < now:
            return "expired"
        _, duration = self._predicted_duration(spec)
        return "approaching" if spec.deadline_at_utc - now <= duration else "future"

    async def _admission_queue_loop(self) -> None:
        while True:
            if not self._queued_ids and not self._processing_ids:
                return
            now = time.time()
            pending = [self.workload_registry.get_workload(wid) for wid in self._queued_ids]
            pending = [r for r in pending if r is not None and r.state == WorkloadState.SUBMITTED
                       and r.spec.workload_id not in self._processing_ids]
            pending.sort(key=lambda r: (
                0 if now - r.submitted_at >= self.STARVATION_PREVENTION_SECONDS else 1,
                r.submitted_at if now - r.submitted_at >= self.STARVATION_PREVENTION_SECONDS else -self._effective_priority(r, now),
                r.spec.workload_id,
            ))
            while pending and len(self._processing_ids) < self.max_dispatch_tasks:
                record = pending.pop(0)
                wid = record.spec.workload_id
                self._queued_ids.discard(wid)
                self._processing_ids.add(wid)
                task = asyncio.create_task(self._run_queued_workload(wid))
                self._dispatch_tasks.add(task)
            self._queue_event.clear()
            try:
                await asyncio.wait_for(self._queue_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                # Reconsider pending workloads after resource/liveness changes,
                # even if an event was missed during a coordinator restart.
                for record in self.workload_registry.list_workloads():
                    if record.state == WorkloadState.SUBMITTED and record.spec.workload_id not in self._processing_ids:
                        self._queued_ids.add(record.spec.workload_id)

    async def _run_queued_workload(self, workload_id: str) -> None:
        try:
            await self._process_workload(workload_id)
        finally:
            record = self.workload_registry.get_workload(workload_id)
            current = asyncio.current_task()
            try:
                if (record is not None and record.state == WorkloadState.SUBMITTED
                        and current is not None and current.cancelling() == 0):
                    self._queued_ids.add(workload_id)
                    await asyncio.sleep(1.0)
            finally:
                self._processing_ids.discard(workload_id)
                if current is not None:
                    self._dispatch_tasks.discard(current)
                self._queue_event.set()

    async def stop_queue(self) -> None:
        if self._queue_task is not None:
            self._queue_task.cancel()
            try:
                await self._queue_task
            except asyncio.CancelledError:
                pass
            self._queue_task = None
        tasks = list(self._dispatch_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def submit_job(
        self,
        specs: List[WorkloadSpec],
        completion_policy: CompletionPolicy = CompletionPolicy.ALL_REQUIRED,
        threshold: Optional[int] = None,
    ) -> str:
        """Submit a Job: a durable grouping of one or more WorkloadSpecs.

        CRITICAL (M8.1/M8.2): this delegates every spec through the
        existing, unmodified submit_workload() path -- it is not a second
        scheduler/dispatcher, just a membership-recording wrapper around
        the same per-workload submission every caller already uses.

        If every spec already carries the same job_id (e.g. set by
        BlenderAdapter.evaluate_request(), which mints one job_id shared
        across all chunks of one request), that id is reused rather than
        minting a conflicting new one. If no spec carries a job_id, a
        fresh one is minted and stamped onto copies of the specs (the
        caller's original spec objects are never mutated in place).
        """
        if not specs:
            raise ValueError("submit_job() requires at least one WorkloadSpec")

        job_ids_present: Set[str] = {s.job_id for s in specs if s.job_id}
        if len(job_ids_present) > 1:
            raise ValueError(
                f"submit_job() specs must share a single job_id or have none set; found {job_ids_present}"
            )
        job_id = job_ids_present.pop() if job_ids_present else f"job-{uuid.uuid4().hex[:16]}"

        stamped_specs = [
            spec if spec.job_id == job_id else spec.model_copy(update={"job_id": job_id})
            for spec in specs
        ]
        workload_ids = {s.workload_id for s in stamped_specs}

        if self.job_registry is None:
            self.job_registry = JobRegistry(self.workload_registry)
        self.job_registry.create_job(job_id, workload_ids, completion_policy=completion_policy, threshold=threshold)

        for spec in stamped_specs:
            await self.submit_workload(spec)

        return job_id

    def _build_worker_profiles(self, workers):
        """Merge worker hardware snapshots with coordinator-owned state."""
        profiles = []
        for info in workers:
            telemetry = info.resource_profile
            if telemetry is None:
                continue

            if self.attempt_registry is not None:
                active_count = len(self.attempt_registry.list_running_attempts_for_worker(info.worker_id))
            else:
                # Compatibility for standalone orchestrators created without
                # the M10 attempt ledger: derive active work from the existing
                # workload registry rather than inventing a zero count.
                active_count = sum(
                    1 for record in self.workload_registry.list_workloads()
                    if record.state == WorkloadState.PLACED
                    and record.placement_decision is not None
                    and record.placement_decision.selected_worker_id == info.worker_id
                )

            profiles.append(telemetry.model_copy(update={
                "worker_id": info.worker_id,
                "endpoint_url": info.endpoint_url,
                "ip_address": info.ip_address,
                "active_workload_count": active_count,
                "status": info.status,
                "local_cached_hashes": set(info.inventory_hashes),
                "can_execute_workloads": info.can_execute_workloads,
            }))
        return profiles

    async def _process_workload(self, workload_id: str) -> None:
        """Drive the workload through its lifecycle phases.

        M10.1/M10.5: this is now an explicit, bounded retry loop. Each
        iteration is one execution Attempt (persisted via
        self.attempt_registry when configured) against the SAME, stable
        workload_id -- a retry never mints a new logical workload. A
        retryable failure (per retry.should_retry(), driven by the
        result's FailureCategory) creates a fresh attempt and loops;
        anything else (success, a non-retryable failure, or budget
        exhaustion) is terminal for this workload and returns.
        """
        record = self.workload_registry.get_workload(workload_id)
        if not record:
            return

        spec = record.spec

        # Validating
        self.workload_registry.update_state(workload_id, WorkloadState.VALIDATING)
        # Assuming Pydantic models validate on parse, we can skip explicit re-validation here

        # Placing
        self.workload_registry.update_state(workload_id, WorkloadState.PLACING)

        attempt_records = self.attempt_registry.list_attempts_for_workload(workload_id) if self.attempt_registry else []
        # A checkpointed success resumes the same logical workload and is
        # explicitly outside the retry budget; other durable attempts count.
        attempts_used = sum(
            1 for attempt in attempt_records
            if not (attempt.status == AttemptStatus.SUCCEEDED and attempt.execution_result is not None
                    and attempt.execution_result.was_checkpointed)
        )
        # Workers this WORKLOAD's own retry loop has already tried and
        # failed to dispatch to. Scoped to this one workload_id's
        # attempts only -- deliberately NOT the same thing as
        # WorkerRegistry.record_failure()'s cluster-wide health penalty
        # below, which affects every OTHER workload's placement too and
        # (correctly) doesn't exclude a worker after a single failure.
        # Without this, a worker that fails once would simply be
        # re-selected again next attempt (identical placement inputs ->
        # identical decision), defeating the point of retrying on a
        # different candidate.
        exhausted_worker_ids: Set[str] = set()
        if self.attempt_registry is not None:
            exhausted_worker_ids = {
                attempt.worker_id
                for attempt in attempt_records
                if(
                    attempt.worker_id is not None
                    and attempt.failure_category == FailureCategory.WORKER_UNAVAILABLE
                    and attempt.status != AttemptStatus.LOST
                )
            }
        if attempts_used >= self.max_attempts and attempts_used > 0:
            self.workload_registry.update_state(
                workload_id, WorkloadState.FAILED,
                error_message="Retry budget exhausted after prior execution attempts",
            )
            return

        while True:
            attempts_used += 1
            attempt_id: Optional[str] = None

            async with self._placement_lock:
                all_workers = self.registry.list_workers(active_only=False)
                workers = [
                    w for w in all_workers
                    if w.worker_id not in exhausted_worker_ids
                ]
                profiles = self._build_worker_profiles(workers)

                group_workers: dict = {}
                if spec.affinity_group_id:
                    group_workers[spec.affinity_group_id] = {
                        r.placement_decision.selected_worker_id
                        for r in self.workload_registry.list_workloads()
                        if r.spec.workload_id != workload_id
                        and r.spec.affinity_group_id == spec.affinity_group_id
                        and r.placement_decision is not None
                    }
                decision = self.placement_engine.evaluate(
                    spec, profiles,
                    worker_tags={w.worker_id: dict(w.tags) for w in workers},
                    group_workers=group_workers,
                    predicted_duration_seconds=self._predicted_duration(spec)[0],
                    asset_sizes_bytes=self._trusted_asset_sizes(spec),
                )
                evaluation = dict(self.placement_engine.last_evaluation)
                evaluated_ids = {c.get("worker_id") for c in evaluation.get("candidates", [])}
                for worker in all_workers:
                    if worker.worker_id in evaluated_ids:
                        continue
                    if worker.worker_id in exhausted_worker_ids:
                        evaluation.setdefault("candidates", []).append({
                            "worker_id": worker.worker_id, "worker_status": worker.status.value,
                            "health_eligible": worker.is_healthy,
                            "can_execute_workloads": worker.can_execute_workloads,
                            "eligible": False,
                            "rejection_reasons": ["WORKER_PREVIOUSLY_FAILED_THIS_WORKLOAD"],
                            "checks": {"retry_exclusion": {"result": False,
                                "reason": "prior WORKER_UNAVAILABLE attempt"}},
                        })
                        continue
                    if worker.resource_profile is None:
                        missing_reasons = ["MISSING_TELEMETRY"]
                        if worker.status != WorkerStatus.ACTIVE:
                            missing_reasons.append("WORKER_NOT_ACTIVE")
                        if not worker.is_healthy:
                            missing_reasons.append("WORKER_UNHEALTHY")
                        if not worker.can_execute_workloads:
                            missing_reasons.append("WORKER_CANNOT_EXECUTE")
                        evaluation.setdefault("candidates", []).append({
                            "worker_id": worker.worker_id, "worker_status": worker.status.value,
                            "health_eligible": worker.is_healthy,
                            "can_execute_workloads": worker.can_execute_workloads,
                            "eligible": False, "rejection_reasons": missing_reasons,
                            "checks": {"status": {"actual": worker.status.value,
                                "result": worker.status == WorkerStatus.ACTIVE},
                                "health": {"actual": worker.status.value,
                                "result": worker.is_healthy},
                                "can_execute_workloads": {"actual": worker.can_execute_workloads,
                                "result": worker.can_execute_workloads},
                                "telemetry": {"age_seconds": None,
                                "maximum_age_seconds": self.placement_engine.STALE_PROFILE_SECONDS,
                                "result": False}},
                        })
                if not decision:
                    self.workload_registry.set_placement_explanation(workload_id, evaluation)
                    self.workload_registry.set_placement(workload_id, None)
                    if spec.required_runtime_compatibility:
                        self.workload_registry.update_state(
                            workload_id, WorkloadState.UNSCHEDULABLE,
                            error_message="Required GPU runtime compatibility cannot be reported by the current worker telemetry",
                        )
                        return
                    self.workload_registry.update_state(
                        workload_id, WorkloadState.SUBMITTED,
                        error_message="Waiting for an eligible worker; placement will be reconsidered",
                    )
                    return

                self.workload_registry.set_placement(workload_id, decision)
                self.workload_registry.set_placement_explanation(workload_id, {})
                self.workload_registry.update_state(workload_id, WorkloadState.PLACED)
                if self.attempt_registry is not None:
                    attempt = self.attempt_registry.create_attempt(
                        workload_id,
                        placement_decision=decision,
                    )
                    attempt_id = attempt.attempt_id
                if attempt_id:
                    self.attempt_registry.mark_assigned(attempt_id, decision.selected_worker_id)

            logger.info(f"Workload {workload_id} placed on {decision.selected_worker_id} (attempt {attempts_used})")

            worker_info = next((w for w in workers if w.worker_id == decision.selected_worker_id), None)
            if not worker_info:
                # Placement selected a worker no longer present in this
                # snapshot -- treat exactly like a dispatch failure.
                failure_category = FailureCategory.WORKER_UNAVAILABLE
                error_msg = f"Selected worker {decision.selected_worker_id} vanished before dispatch"
                if attempt_id:
                    self.attempt_registry.mark_failed(attempt_id, failure_category, error_msg)
                exhausted_worker_ids.add(decision.selected_worker_id)
                if should_retry(failure_category, attempts_used, self.max_attempts):
                    continue
                self.workload_registry.update_state(workload_id, WorkloadState.FAILED, error_message=error_msg)
                return

            try:
                if attempt_id:
                    self.attempt_registry.mark_running(attempt_id)

                # Use http client (which defaults to httpx.AsyncClient or mock)
                client = getattr(self, "http_client", None) or httpx.AsyncClient()
                url = f"{worker_info.endpoint_url}/api/v1/workloads/execute"

                resp = await client.post(url, json=spec.model_dump(mode='json'), timeout=60.0)
                resp.raise_for_status()
                result_data = resp.json()
                result = WorkloadExecutionResult(**result_data)

                if result.was_checkpointed:
                    # M10.7: only reachable when the executing runtime
                    # genuinely declared supports_checkpointing=True (see
                    # execution.py) -- this attempt itself completed its
                    # assigned sub-task (a safe checkpoint) successfully;
                    # the WORKLOAD continues via a fresh attempt that
                    # resumes from the checkpoint, so this is not counted
                    # against the retry budget. Unchanged from pre-M10
                    # behavior: resubmit via a new task and return rather
                    # than looping in place, so the MIGRATING state is
                    # immediately observable to a caller awaiting this
                    # dispatch, and each resumed attempt gets its own
                    # call stack (matching the original design here).
                    logger.warning(f"Workload {workload_id} was checkpointed on {decision.selected_worker_id}. Migrating...")
                    self.workload_registry.update_state(workload_id, WorkloadState.MIGRATING)
                    if attempt_id:
                        self.attempt_registry.mark_succeeded(attempt_id, result)

                    if result.checkpoint_hash:
                        spec.input_asset_hashes.add(result.checkpoint_hash)
                        spec.parameters["resume_from_checkpoint"] = result.checkpoint_hash

                    self.workload_registry.set_placement(workload_id, None)
                    self.workload_registry.set_placement_explanation(workload_id, {})
                    self.workload_registry.update_state(workload_id, WorkloadState.SUBMITTED)
                    self.enqueue_workload(workload_id)
                    return

                if result.success:
                    self.workload_registry.set_result(workload_id, result)
                    self.workload_registry.update_state(workload_id, WorkloadState.COMPLETED)
                    self._record_artifacts(spec, result)
                    if attempt_id:
                        self.attempt_registry.mark_succeeded(attempt_id, result)
                    return

                # Failed result with an explicit, worker-reported outcome.
                if attempt_id:
                    self.attempt_registry.mark_failed(
                        attempt_id, result.failure_category, result.error_message, result
                    )
                if should_retry(result.failure_category, attempts_used, self.max_attempts):
                    logger.warning(
                        f"Workload {workload_id} attempt {attempts_used} failed retryably "
                        f"({result.failure_category}); retrying."
                    )
                    continue

                self.workload_registry.set_result(workload_id, result)
                self.workload_registry.update_state(
                    workload_id, WorkloadState.FAILED, error_message=result.error_message
                )
                return

            except Exception as e:
                logger.error(f"Failed to dispatch workload {workload_id} to worker {decision.selected_worker_id}: {e}")
                # Reuses the EXISTING WorkerRegistry health-penalty
                # machinery for cluster-wide health tracking (affects
                # every workload's future placement, not just this one),
                # AND excludes this worker from THIS workload's own
                # remaining retry attempts via exhausted_worker_ids above
                # -- a single failure isn't enough to cross the registry's
                # own degraded/suspect thresholds, so without the local
                # exclusion this loop would simply re-select the same
                # worker again (identical placement inputs).
                self.registry.record_failure(decision.selected_worker_id, reason=f"dispatch failed: {e}")
                exhausted_worker_ids.add(decision.selected_worker_id)

                failure_category = FailureCategory.WORKER_UNAVAILABLE
                if attempt_id:
                    self.attempt_registry.mark_failed(attempt_id, failure_category, str(e))

                if should_retry(failure_category, attempts_used, self.max_attempts):
                    continue

                self.workload_registry.update_state(workload_id, WorkloadState.FAILED, error_message=str(e))
                return

    def handle_worker_lost(self, worker_id: str) -> None:
        """M10.6: called when WorkerRegistry evicts worker_id (heartbeat
        timeout). Marks any attempt this worker was actively running as
        LOST -- the coordinator has no observed outcome for it, and the
        work may have actually completed on the worker before it
        vanished (see AttemptStatus.LOST's docstring).

        Deliberately does NOT itself spawn a new dispatch task: the
        in-flight _process_workload() coroutine already dispatching to
        this worker will independently observe the failure (its HTTP
        call to a dead worker eventually errors/times out) and retry
        through the normal retry-budget loop above. Spawning a second,
        racing dispatch here would risk two concurrent attempts
        executing the same workload_id simultaneously for no benefit --
        at-least-once duplication is an accepted, already-documented
        invariant for the crash-during-execution case, not something to
        introduce gratuitously for the merely-slow-to-time-out case.
        """
        if self.attempt_registry is None:
            return
        for attempt in self.attempt_registry.list_running_attempts_for_worker(worker_id):
            self.attempt_registry.mark_lost(
                attempt.attempt_id, reason=f"worker {worker_id} evicted (heartbeat timeout)"
            )

    def _record_artifacts(self, spec: WorkloadSpec, result: WorkloadExecutionResult) -> None:
        """M8.5 tail / M8.8: after a COMPLETED result is persisted via
        set_result() (the existing, unmodified result-persistence path),
        record one Artifact per output hash. This extends that path's
        consumers -- it does not replace or duplicate it. No-op if
        artifact_registry isn't configured or there's nothing to record,
        so callers/tests that never touch M8 see no behavior change."""
        if self.artifact_registry is None or not result.output_asset_hashes:
            return
        self.artifact_registry.record_artifacts(
            producer_workload_id=spec.workload_id,
            producer_job_id=spec.job_id,
            content_hashes=result.output_asset_hashes,
            sizes=result.output_asset_sizes,
        )

    async def drain_worker(self, worker_id: str) -> None:
        """Trigger migration for all executing workloads on a specific worker.

        M10.8: fixed a pre-existing bug -- WorkloadRecord has no
        `placement` attribute (it's `placement_decision`; see
        WorkloadRecord.__init__ in workload_registry.py), so this filter
        previously raised AttributeError on every non-None record it
        examined and therefore never matched anything, silently
        no-op'ing drain_worker() entirely. PlacementEngine.evaluate()
        already hard-excludes DRAINING/UNHEALTHY workers from new
        placement (placement.py), so this fix only affects EXISTING
        in-flight work on a worker transitioning to DRAINING -- new
        placement correctness was unaffected by the bug.
        """
        # Find all workloads currently running on this worker
        with self.workload_registry._lock:
            active_workloads = [
                wid for wid, record in self.workload_registry._workloads.items()
                if record.state == WorkloadState.PLACED
                and record.placement_decision is not None
                and record.placement_decision.selected_worker_id == worker_id
            ]
        
        if not active_workloads:
            return
            
        logger.info(f"Draining worker {worker_id}. Triggering checkpoint for {len(active_workloads)} workloads.")
        
        worker_info = self.registry.get_worker(worker_id)
        if not worker_info:
            return
            
        client = getattr(self, "http_client", None) or httpx.AsyncClient()
        url = f"{worker_info.endpoint_url}/api/v1/workloads"
        
        for wid in active_workloads:
            try:
                # Fire and forget the checkpoint request; the worker will abort and the 
                # blocked _process_workload loop will receive the checkpoint result and migrate it.
                await client.post(f"{url}/{wid}/checkpoint", timeout=10.0)
            except Exception as e:
                logger.error(f"Failed to trigger checkpoint for {wid} on {worker_id}: {e}")
