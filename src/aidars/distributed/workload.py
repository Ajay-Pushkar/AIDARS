"""Workload lifecycle orchestration.

Ties together models, placement, and registry to drive a workload from SUBMITTED to COMPLETED.
"""

import asyncio
import logging
import uuid
import httpx
from typing import List, Optional, Set

from aidars.distributed.artifact import ArtifactRegistry
from aidars.distributed.attempt import AttemptRegistry
from aidars.distributed.job_registry import CompletionPolicy, JobRegistry
from aidars.distributed.models import FailureCategory, WorkloadSpec, WorkloadExecutionResult
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.retry import DEFAULT_MAX_ATTEMPTS, should_retry
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState

logger = logging.getLogger(__name__)


class WorkloadOrchestrator:
    """Central orchestrator for workload lifecycle on the Coordinator."""

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
    ) -> None:
        self.registry = registry
        self.workload_registry = workload_registry
        self.m7_ingestor = m7_ingestor
        self.placement_engine = PlacementEngine(m7_bridge=m7_bridge)

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

    async def submit_workload(self, spec: WorkloadSpec) -> str:
        """Submit a new workload for execution. Returns the workload_id."""
        record = self.workload_registry.add_workload(spec)

        # Asynchronously process the placement and execution
        asyncio.create_task(self._process_workload(spec.workload_id))

        return spec.workload_id

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
        """Snapshot the given WorkerInfo list into WorkerResourceProfile
        candidates for PlacementEngine.evaluate(). Extracted, unmodified
        logic (was previously inlined once per dispatch attempt)."""
        from aidars.distributed.models import WorkerResourceProfile
        import time as _time

        profiles = []
        for info in workers:
            if not info.is_healthy:
                continue
            profiles.append(WorkerResourceProfile(
                worker_id=info.worker_id,
                endpoint_url=info.endpoint_url,
                ip_address=info.ip_address,
                cpu_cores_total=info.cpu_cores_total if hasattr(info, 'cpu_cores_total') and info.cpu_cores_total else 8,
                cpu_utilization_percent=info.last_metrics.cpu_percent if info.last_metrics else 0.0,
                ram_total_bytes=info.capacity_bytes or 8_000_000_000,
                ram_available_bytes=info.available_bytes,
                active_workload_count=0,  # Placeholder
                status=info.status,
                local_cached_hashes=info.inventory_hashes,
                timestamp_utc=_time.time(),
                can_execute_workloads=info.can_execute_workloads,
            ))
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

        attempts_used = self.attempt_registry.count_attempts(workload_id) if self.attempt_registry else 0
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

        while True:
            attempts_used += 1
            attempt_id: Optional[str] = None
            if self.attempt_registry is not None:
                attempt = self.attempt_registry.create_attempt(workload_id)
                attempt_id = attempt.attempt_id

            workers = [
                w for w in self.registry.list_workers(active_only=True)
                if w.worker_id not in exhausted_worker_ids
            ]
            profiles = self._build_worker_profiles(workers)

            decision = self.placement_engine.evaluate(spec, profiles)
            if not decision:
                self.workload_registry.update_state(
                    workload_id, WorkloadState.UNSCHEDULABLE, error_message="No suitable worker found"
                )
                if attempt_id:
                    self.attempt_registry.mark_failed(
                        attempt_id, FailureCategory.WORKER_UNAVAILABLE, "No suitable worker found"
                    )
                return

            self.workload_registry.set_placement(workload_id, decision)
            self.workload_registry.update_state(workload_id, WorkloadState.PLACED)
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
                client = getattr(self, 'http_client', httpx.AsyncClient())
                url = f"{worker_info.endpoint_url}/api/v1/workloads/execute"

                resp = await client.post(url, json=spec.model_dump(mode='json'), timeout=60.0)
                resp.raise_for_status()
                result_data = resp.json()
                result = WorkloadExecutionResult(**result_data)

                if self.m7_ingestor:
                    self.m7_ingestor.on_workload_completed(
                        workload_type=spec.task_type,
                        duration=result.execution_duration_seconds,
                        ram_peak=1024,
                        failed=not result.success,
                    )

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

                    asyncio.create_task(self._process_workload(workload_id))
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
            
        client = getattr(self, 'http_client', httpx.AsyncClient())
        url = f"{worker_info.endpoint_url}/api/v1/workloads"
        
        for wid in active_workloads:
            try:
                # Fire and forget the checkpoint request; the worker will abort and the 
                # blocked _process_workload loop will receive the checkpoint result and migrate it.
                await client.post(f"{url}/{wid}/checkpoint", timeout=10.0)
            except Exception as e:
                logger.error(f"Failed to trigger checkpoint for {wid} on {worker_id}: {e}")

