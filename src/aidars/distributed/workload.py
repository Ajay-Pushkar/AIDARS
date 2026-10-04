"""Workload lifecycle orchestration.

Ties together models, placement, and registry to drive a workload from SUBMITTED to COMPLETED.
"""

import asyncio
import logging
import time
import uuid
import httpx
from urllib.parse import urlsplit, urlunsplit
from typing import List, Optional, Set

from aidars.distributed.artifact import ArtifactRegistry
from aidars.distributed.attempt import AttemptRegistry, AttemptStatus
from aidars.distributed.job_registry import CompletionPolicy, JobRegistry
from aidars.distributed.models import (
    ExecutionDescription, ExecutionGroup, ExecutionShape, FailureCategory,
    RuntimeExecutionContext, WorkerStatus, WorkloadExecutionRequest,
    WorkloadSpec, WorkloadExecutionResult,
)
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.retry import DEFAULT_MAX_ATTEMPTS, should_retry
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState, TERMINAL_WORKLOAD_STATES

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
        # A coordinated group drain must fan out abort/checkpoint requests
        # before its in-flight dispatch is allowed to retry on a new group.
        self._group_drain_events: dict[str, asyncio.Event] = {}

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

    def _dependency_state(self, record) -> str:
        """Return ready, waiting, or failed for the explicit success-only edges."""
        for dependency_id in record.spec.depends_on:
            dependency = self.workload_registry.get_workload(dependency_id)
            if dependency is None:
                return "waiting"
            if dependency.state in (WorkloadState.FAILED, WorkloadState.UNSCHEDULABLE,
                                    WorkloadState.TIMEOUT):
                self.workload_registry.update_state(
                    record.spec.workload_id, WorkloadState.FAILED,
                    error_message=f"Dependency {dependency_id} did not complete successfully",
                )
                return "failed"
            if dependency.state != WorkloadState.COMPLETED:
                return "waiting"
        return "ready"

    def _spec_with_dependency_outputs(self, record) -> WorkloadSpec:
        """Add successfully produced dependency assets to a stage's generic inputs."""
        if not record.spec.depends_on:
            return record.spec
        spec = record.spec.model_copy(deep=True)
        for dependency_id in record.spec.depends_on:
            dependency = self.workload_registry.get_workload(dependency_id)
            if (dependency is None or dependency.state != WorkloadState.COMPLETED
                    or dependency.execution_result is None
                    or not dependency.execution_result.success):
                raise ValueError(f"Dependency {dependency_id} has no successful result")
            spec.input_asset_hashes.update(dependency.execution_result.output_asset_hashes)
        return spec

    async def _admission_queue_loop(self) -> None:
        while True:
            if not self._queued_ids and not self._processing_ids:
                return
            now = time.time()
            pending = [self.workload_registry.get_workload(wid) for wid in self._queued_ids]
            pending = [r for r in pending if r is not None and r.state == WorkloadState.SUBMITTED
                       and r.spec.workload_id not in self._processing_ids]
            pending = [r for r in pending if self._dependency_state(r) == "ready"]
            self._queued_ids = {wid for wid in self._queued_ids
                                if (self.workload_registry.get_workload(wid) is not None
                                    and self.workload_registry.get_workload(wid).state == WorkloadState.SUBMITTED)}
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
        execution_shape: Optional[ExecutionShape] = None,
        required_worker_count: int = 1,
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

        if execution_shape is None:
            execution_shape = (ExecutionShape.PIPELINE if any(s.depends_on for s in specs)
                               else ExecutionShape.TASK_SPLIT if len(specs) > 1
                               else ExecutionShape.SINGLE_MACHINE)
        ids = {s.workload_id for s in specs}
        if execution_shape == ExecutionShape.PIPELINE:
            if len(specs) < 2 or not any(spec.depends_on for spec in specs):
                raise ValueError("PIPELINE requires multiple workloads and at least one dependency")
            if any(dep not in ids for spec in specs for dep in spec.depends_on):
                raise ValueError("pipeline dependencies must refer to workloads in the same job")
            from aidars.distributed.models import _validate_dependency_graph
            _validate_dependency_graph({s.workload_id: s.depends_on for s in specs})
        elif any(s.depends_on for s in specs):
            raise ValueError("workload dependencies require PIPELINE execution shape")
        elif execution_shape == ExecutionShape.SINGLE_MACHINE and len(specs) != 1:
            raise ValueError("SINGLE_MACHINE requires exactly one workload")
        if execution_shape == ExecutionShape.DISTRIBUTED_NATIVE:
            if len(specs) != 1 or required_worker_count < 2:
                raise ValueError("DISTRIBUTED_NATIVE requires one workload and at least two workers")
        elif required_worker_count != 1:
            raise ValueError("required_worker_count is only valid for DISTRIBUTED_NATIVE")

        stamped_specs = [
            spec if spec.job_id == job_id else spec.model_copy(update={"job_id": job_id})
            for spec in specs
        ]
        workload_ids = {s.workload_id for s in stamped_specs}

        if self.job_registry is None:
            self.job_registry = JobRegistry(self.workload_registry)
        self.job_registry.create_job(job_id, workload_ids, completion_policy=completion_policy,
                                     threshold=threshold, execution_shape=execution_shape,
                                     required_worker_count=required_worker_count)

        for spec in stamped_specs:
            await self.submit_workload(spec)

        return job_id

    async def submit_execution(self, description: ExecutionDescription,
                               completion_policy: CompletionPolicy = CompletionPolicy.ALL_REQUIRED,
                               threshold: Optional[int] = None) -> str:
        """Submit an adapter-produced generic execution description."""
        return await self.submit_job(
            description.workloads, completion_policy, threshold,
            execution_shape=description.execution_shape,
            required_worker_count=description.required_worker_count,
        )

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

        if self._dependency_state(record) != "ready":
            return
        spec = self._spec_with_dependency_outputs(record)
        job = self.job_registry.get_job(spec.job_id) if (self.job_registry and spec.job_id) else None
        execution_shape = job.execution_shape if job is not None else ExecutionShape.SINGLE_MACHINE
        required_worker_count = job.required_worker_count if job is not None else 1

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
                    and attempt.failure_category in (
                        FailureCategory.WORKER_UNAVAILABLE,
                        FailureCategory.ASSET_STAGING_FAILURE,
                        FailureCategory.EXECUTION_TIMEOUT,
                    )
                    and attempt.status != AttemptStatus.LOST
                )
            }
            exhausted_worker_ids.update(
                worker_id
                for attempt in attempt_records
                if attempt.failure_category in (
                    FailureCategory.WORKER_UNAVAILABLE,
                    FailureCategory.ASSET_STAGING_FAILURE,
                    FailureCategory.EXECUTION_TIMEOUT,
                )
                and attempt.execution_group is not None
                for worker_id in attempt.execution_group.unavailable_worker_ids
            )
        if attempts_used >= self.max_attempts and attempts_used > 0:
            self.workload_registry.update_state(
                workload_id, WorkloadState.FAILED,
                error_message="Retry budget exhausted after prior execution attempts",
            )
            return

        while True:
            attempts_used += 1
            attempt_id: Optional[str] = None
            group_decisions = None
            execution_group = None

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
                placement_kwargs = dict(
                    worker_tags={w.worker_id: dict(w.tags) for w in workers},
                    group_workers=group_workers,
                    predicted_duration_seconds=self._predicted_duration(spec)[0],
                    asset_sizes_bytes=self._trusted_asset_sizes(spec),
                )
                if execution_shape == ExecutionShape.DISTRIBUTED_NATIVE:
                    group_decisions = self.placement_engine.evaluate_group(
                        spec, profiles, required_worker_count, **placement_kwargs,
                    )
                    decision = group_decisions[0] if group_decisions else None
                    evaluation = dict(getattr(self.placement_engine, "last_group_evaluation", {}))
                else:
                    decision = self.placement_engine.evaluate(spec, profiles, **placement_kwargs)
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
                if group_decisions:
                    execution_group = ExecutionGroup(
                        execution_id=f"exec-{uuid.uuid4().hex}",
                        worker_ids=[item.selected_worker_id for item in group_decisions],
                        required_worker_count=required_worker_count,
                        placement_decisions=group_decisions,
                    )
                if self.attempt_registry is not None:
                    attempt = self.attempt_registry.create_attempt(
                        workload_id,
                        placement_decision=decision,
                        execution_group=execution_group,
                    )
                    attempt_id = attempt.attempt_id
                if attempt_id:
                    self.attempt_registry.mark_assigned(attempt_id, decision.selected_worker_id)

            logger.info(f"Workload {workload_id} placed on {decision.selected_worker_id} (attempt {attempts_used})")

            worker_by_id = {worker.worker_id: worker for worker in workers}
            selected_ids = execution_group.worker_ids if execution_group is not None else [decision.selected_worker_id]
            missing_worker_ids = [worker_id for worker_id in selected_ids if worker_id not in worker_by_id]
            worker_info = worker_by_id.get(decision.selected_worker_id)
            if missing_worker_ids or not worker_info:
                # Placement selected a worker no longer present in this
                # snapshot -- treat exactly like a dispatch failure.
                failure_category = FailureCategory.WORKER_UNAVAILABLE
                error_msg = f"Selected worker group member(s) vanished before dispatch: {missing_worker_ids or [decision.selected_worker_id]}"
                if attempt_id:
                    self.attempt_registry.mark_lost(attempt_id, error_msg,
                                                    unavailable_worker_ids=missing_worker_ids)
                exhausted_worker_ids.update(missing_worker_ids or [decision.selected_worker_id])
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

                unavailable_group_workers: Set[str] = set()
                transport_unavailable_group_workers: Set[str] = set()
                if execution_group is not None:
                    (result, unavailable_group_workers,
                     transport_unavailable_group_workers) = await self._dispatch_execution_group(
                        client, spec, execution_group, worker_by_id,
                    )
                    url = "distributed execution group"
                else:
                    resp = await client.post(url, json=spec.model_dump(mode='json'), timeout=60.0)
                    resp.raise_for_status()
                    result_data = resp.json()
                    result = WorkloadExecutionResult(**result_data)

                if result.was_checkpointed and execution_group is not None:
                    result.success = False
                    result.was_checkpointed = False
                    result.failure_category = FailureCategory.APPLICATION_ERROR
                    result.error_message = "Distributed execution groups require a coordinated checkpoint, which this runtime did not provide"

                group_attempt_was_lost = False
                if execution_group is not None and attempt_id and self.attempt_registry is not None:
                    latest_attempt = self.attempt_registry.get_attempt(attempt_id)
                    if latest_attempt is not None and latest_attempt.status == AttemptStatus.LOST:
                        group_attempt_was_lost = True
                        drain_event = self._group_drain_events.get(attempt_id)
                        if drain_event is not None:
                            await drain_event.wait()
                            self._group_drain_events.pop(attempt_id, None)
                        if latest_attempt.execution_group is not None:
                            # The durable group record is authoritative here.
                            # A coordinated drain aborts healthy peer processes
                            # too; those checkpoint-abort results must not
                            # make every healthy peer unavailable for retry.
                            unavailable_group_workers = set(
                                latest_attempt.execution_group.unavailable_worker_ids
                            )
                            unavailable_group_workers.update(transport_unavailable_group_workers)
                        result.success = False
                        result.failure_category = FailureCategory.WORKER_UNAVAILABLE
                        result.error_message = "A distributed execution group member was lost before completion"

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
                        self.workload_registry.update_spec(spec)

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
                    if unavailable_group_workers or group_attempt_was_lost:
                        self.attempt_registry.mark_lost(
                            attempt_id, "distributed execution group lost a worker before completion",
                            unavailable_worker_ids=sorted(unavailable_group_workers),
                        )
                    else:
                        self.attempt_registry.mark_failed(
                            attempt_id, result.failure_category, result.error_message, result
                        )
                if unavailable_group_workers:
                    for unavailable_id in unavailable_group_workers:
                        self.registry.record_failure(unavailable_id, reason="distributed group member unavailable")
                        exhausted_worker_ids.add(unavailable_id)
                elif result.failure_category in (
                    FailureCategory.WORKER_UNAVAILABLE,
                    FailureCategory.ASSET_STAGING_FAILURE,
                    FailureCategory.EXECUTION_TIMEOUT,
                ) and decision:
                    exhausted_worker_ids.add(decision.selected_worker_id)

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
                failed_ids = execution_group.worker_ids if execution_group is not None else [decision.selected_worker_id]
                for failed_id in failed_ids:
                    reason = "execution group dispatch failed" if execution_group is not None else f"dispatch failed: {e}"
                    self.registry.record_failure(failed_id, reason=reason)
                exhausted_worker_ids.update(failed_ids)

                failure_category = FailureCategory.WORKER_UNAVAILABLE
                if attempt_id:
                    if execution_group is not None:
                        self.attempt_registry.mark_lost(
                            attempt_id, str(e), unavailable_worker_ids=failed_ids,
                        )
                    else:
                        self.attempt_registry.mark_failed(attempt_id, failure_category, str(e))

                if should_retry(failure_category, attempts_used, self.max_attempts):
                    continue

                self.workload_registry.update_state(workload_id, WorkloadState.FAILED, error_message=str(e))
        return

    async def _dispatch_execution_group(self, client, spec: WorkloadSpec,
                                        group: ExecutionGroup, worker_by_id: dict):
        """Launch the same logical workload once per group member and aggregate
        success only when every member reports success.
        """
        def public_endpoint(value: str) -> str:
            parts = urlsplit(value)
            hostname = parts.hostname or ""
            if ":" in hostname and not hostname.startswith("["):
                hostname = f"[{hostname}]"
            netloc = hostname + (f":{parts.port}" if parts.port is not None else "")
            return urlunsplit((parts.scheme, netloc, parts.path.rstrip("/"), "", ""))

        worker_views = [
            {"worker_id": worker_id, "endpoint_url": public_endpoint(worker_by_id[worker_id].endpoint_url),
             "rank": rank}
            for rank, worker_id in enumerate(group.worker_ids)
        ]

        async def dispatch(rank: int, worker_id: str):
            worker = worker_by_id[worker_id]
            context = RuntimeExecutionContext(
                execution_id=group.execution_id, worker_id=worker_id,
                rank=rank, world_size=group.required_worker_count,
                workers=worker_views,
            )
            request = WorkloadExecutionRequest(spec=spec, execution_context=context)
            response = await client.post(
                f"{worker.endpoint_url}/api/v1/workloads/execute-group",
                json=request.model_dump(mode="json"), timeout=60.0,
            )
            response.raise_for_status()
            return worker_id, WorkloadExecutionResult(**response.json())

        outcomes = await asyncio.gather(
            *(dispatch(rank, worker_id) for rank, worker_id in enumerate(group.worker_ids)),
            return_exceptions=True,
        )
        worker_results = [item for item in outcomes
                          if (isinstance(item, tuple) and len(item) == 2
                              and isinstance(item[1], WorkloadExecutionResult))]
        results = [result for _, result in worker_results]
        unavailable = {group.worker_ids[index] for index, item in enumerate(outcomes)
                       if isinstance(item, BaseException)}
        reported_unavailable = {
            worker_id for worker_id, result in worker_results
            if (not result.success and result.failure_category == FailureCategory.WORKER_UNAVAILABLE)
        }
        unavailable.update(reported_unavailable)
        member_failure = next(
            ((worker_id, result) for worker_id, result in worker_results
             if not result.success or result.was_checkpointed),
            None,
        )
        success = len(results) == group.required_worker_count and not unavailable and member_failure is None
        failure_category = None
        error_message = None
        if unavailable:
            failure_category = FailureCategory.WORKER_UNAVAILABLE
            error_message = "One or more distributed execution group members became unavailable"
        elif member_failure is not None:
            _, failed_result = member_failure
            failure_category = failed_result.failure_category or FailureCategory.APPLICATION_ERROR
            error_message = ("Distributed execution groups require a coordinated checkpoint, which this runtime did not provide"
                             if failed_result.was_checkpointed else
                             "A distributed execution group member reported failure")
        aggregate = WorkloadExecutionResult(
            workload_id=spec.workload_id,
            worker_id=group.worker_ids[0],
            success=success,
            output_asset_hashes=set().union(*(item.output_asset_hashes for item in results)) if results else set(),
            output_asset_sizes={key: value for item in results for key, value in item.output_asset_sizes.items()},
            execution_duration_seconds=max((item.execution_duration_seconds for item in results), default=0.0),
            staging_duration_seconds=max((item.staging_duration_seconds for item in results), default=0.0),
            output_ingestion_duration_seconds=max((item.output_ingestion_duration_seconds for item in results), default=0.0),
            verification_duration_seconds=max((item.verification_duration_seconds for item in results), default=0.0),
            error_message=error_message,
            failure_category=failure_category,
        )
        expected_output_count = spec.parameters.get("expected_output_count")
        if (aggregate.success and expected_output_count is not None
                and len(aggregate.output_asset_hashes) < expected_output_count):
            aggregate.success = False
            aggregate.failure_category = FailureCategory.APPLICATION_ERROR
            aggregate.error_message = (
                f"Distributed output verification failed: expected at least {expected_output_count} "
                f"output asset(s), found {len(aggregate.output_asset_hashes)}."
            )
        return aggregate, unavailable, {
            group.worker_ids[index] for index, item in enumerate(outcomes)
            if isinstance(item, BaseException)
        }

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
                attempt.attempt_id, reason=f"worker {worker_id} evicted (heartbeat timeout)",
                unavailable_worker_ids=[worker_id],
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
        """Recover ordinary work and whole execution groups using existing checkpoints.

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
        group_attempts = []
        group_workload_ids: Set[str] = set()
        if self.attempt_registry is not None:
            group_attempts = [
                attempt for attempt in self.attempt_registry.list_running_attempts_for_worker(worker_id)
                if attempt.execution_group is not None
            ]
            for attempt in group_attempts:
                group_workload_ids.add(attempt.workload_id)
                self._group_drain_events[attempt.attempt_id] = asyncio.Event()
                # Mark the logical execution lost as one unit and record only
                # the worker actually being drained as unavailable. Healthy
                # peers will be stopped as part of group recovery but can join
                # the replacement group.
                self.attempt_registry.mark_lost(
                    attempt.attempt_id,
                    reason=f"execution group drained because worker {worker_id} is draining",
                    unavailable_worker_ids=[worker_id],
                )

        # Find ordinary workloads selected on this worker. Group workloads
        # are handled from the durable attempt membership above, not the
        # leader-only placement decision stored on WorkloadRecord.
        with self.workload_registry._lock:
            active_workloads = [
                wid for wid, record in self.workload_registry._workloads.items()
                if record.state == WorkloadState.PLACED
                and record.placement_decision is not None
                and record.placement_decision.selected_worker_id == worker_id
                and wid not in group_workload_ids
            ]

        if not active_workloads and not group_attempts:
            return

        logger.info(
            "Draining worker %s. Triggering checkpoint for %d workloads and %d execution groups.",
            worker_id, len(active_workloads), len(group_attempts),
        )
        client = getattr(self, "http_client", None) or httpx.AsyncClient()

        async def checkpoint_member(member_id: str, workload_id: str) -> None:
            member = self.registry.get_worker(member_id)
            if member is None:
                return
            try:
                await client.post(
                    f"{member.endpoint_url}/api/v1/workloads/{workload_id}/checkpoint",
                    timeout=10.0,
                )
            except Exception as exc:
                logger.error("Failed to trigger checkpoint for %s on %s: %s", workload_id, member_id, exc)

        group_checkpoint_tasks = []
        for attempt in group_attempts:
            group = attempt.execution_group
            if group is not None:
                group_checkpoint_tasks.extend(
                    checkpoint_member(member_id, attempt.workload_id)
                    for member_id in group.worker_ids
                )

        try:
            # Send the abort/checkpoint request to every member before the
            # in-flight dispatch is allowed to retry on a replacement group.
            if group_checkpoint_tasks:
                await asyncio.gather(*group_checkpoint_tasks)
        finally:
            for attempt in group_attempts:
                event = self._group_drain_events.get(attempt.attempt_id)
                if event is not None:
                    event.set()

        for wid in active_workloads:
            await checkpoint_member(worker_id, wid)

    async def cancel_workload(self, workload_id: str) -> bool:
        """Cancel a workload and terminate its execution if running."""
        record = self.workload_registry.get_workload(workload_id)
        if not record:
            return False

        if record.state in TERMINAL_WORKLOAD_STATES:
            return True

        if record.state in (WorkloadState.EXECUTING, WorkloadState.SYNCING_ASSETS):
            # Attempt to cancel on the worker
            attempt = self.attempt_registry.get_active_attempt(workload_id)
            if attempt and attempt.worker_id:
                worker = self.registry.get_worker(attempt.worker_id)
                if worker:
                    client = getattr(self, "http_client", None) or httpx.AsyncClient()
                    try:
                        await client.post(
                            f"{worker.endpoint_url}/api/v1/workloads/{workload_id}/cancel",
                            timeout=5.0,
                        )
                    except Exception as exc:
                        logger.warning("Failed to reach worker %s to cancel workload %s: %s", worker.worker_id, workload_id, exc)

        self.workload_registry.update_state(
            workload_id,
            WorkloadState.CANCELLED,
            error_message="Execution cancelled by user request",
        )
        self._queued_ids.discard(workload_id)
        return True

    async def cancel_job(self, job_id: str) -> bool:
        """Cancel all active workloads for a job."""
        job = self.job_registry.get_job(job_id)
        if not job:
            return False

        success = True
        for wid in job.workload_ids:
            if not await self.cancel_workload(wid):
                success = False

        # Job aggregate state handles evaluating WorkloadState.CANCELLED as failed/cancelled
        return success

