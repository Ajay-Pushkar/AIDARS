"""Workload lifecycle orchestration.

Ties together models, placement, and registry to drive a workload from SUBMITTED to COMPLETED.
"""

import asyncio
import logging
import uuid
import httpx
from typing import List, Optional, Set

from aidars.distributed.artifact import ArtifactRegistry
from aidars.distributed.job_registry import CompletionPolicy, JobRegistry
from aidars.distributed.models import WorkloadSpec, WorkloadExecutionResult
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.registry import WorkerRegistry
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

    async def _process_workload(self, workload_id: str) -> None:
        """Drive the workload through its lifecycle phases."""
        record = self.workload_registry.get_workload(workload_id)
        if not record:
            return
            
        spec = record.spec

        # Validating
        self.workload_registry.update_state(workload_id, WorkloadState.VALIDATING)
        # Assuming Pydantic models validate on parse, we can skip explicit re-validation here
        
        # Placing
        self.workload_registry.update_state(workload_id, WorkloadState.PLACING)
        
        # Fetch active profiles from registry (simplified for now)
        # In a real implementation, we'd fetch full profiles from workers or maintain them in registry
        profiles = []
        workers = self.registry.list_workers(active_only=True)
        for info in workers:
            worker_id = info.worker_id
            if info.is_healthy:
                # Mocking the resource profile based on WorkerInfo.
                # In production, this data comes via heartbeats to the coordinator.
                from aidars.distributed.models import WorkerResourceProfile
                import time
                profiles.append(WorkerResourceProfile(
                    worker_id=worker_id,
                    endpoint_url=info.endpoint_url,
                    ip_address=info.ip_address,
                    cpu_cores_total=info.cpu_cores_total if hasattr(info, 'cpu_cores_total') and info.cpu_cores_total else 8,
                    cpu_utilization_percent=info.last_metrics.cpu_percent if info.last_metrics else 0.0,
                    ram_total_bytes=info.capacity_bytes or 8_000_000_000,
                    ram_available_bytes=info.available_bytes,
                    active_workload_count=0, # Placeholder
                    status=info.status,
                    local_cached_hashes=info.inventory_hashes,
                    timestamp_utc=time.time(),
                    can_execute_workloads=info.can_execute_workloads,
                ))
        
        decision = self.placement_engine.evaluate(spec, profiles)
        if not decision:
            self.workload_registry.update_state(
                workload_id, WorkloadState.UNSCHEDULABLE, error_message="No suitable worker found"
            )
            return

        self.workload_registry.set_placement(workload_id, decision)
        self.workload_registry.update_state(workload_id, WorkloadState.PLACED)
        
        logger.info(f"Workload {workload_id} placed on {decision.selected_worker_id}")
        
        # Dispatch to worker with retry/recovery logic
        worker_info = next((w for w in workers if w.worker_id == decision.selected_worker_id), None)
        if worker_info:
            try:
                # Use http client (which defaults to httpx.AsyncClient or mock)
                client = getattr(self, 'http_client', httpx.AsyncClient())
                url = f"{worker_info.endpoint_url}/api/v1/workloads/execute"
                
                resp = await client.post(url, json=spec.model_dump(mode='json'), timeout=60.0)
                resp.raise_for_status()
                result_data = resp.json()
                result = WorkloadExecutionResult(**result_data)
                
                if result.was_checkpointed:
                    logger.warning(f"Workload {workload_id} was checkpointed on {decision.selected_worker_id}. Migrating...")
                    self.workload_registry.update_state(workload_id, WorkloadState.MIGRATING)
                    
                    if result.checkpoint_hash:
                        spec.input_asset_hashes.add(result.checkpoint_hash)
                        spec.parameters["resume_from_checkpoint"] = result.checkpoint_hash
                        
                    # Resubmit workload for migration
                    asyncio.create_task(self._process_workload(workload_id))
                    return
                elif result.success:
                    self.workload_registry.set_result(workload_id, result)
                    self.workload_registry.update_state(workload_id, WorkloadState.COMPLETED)
                    self._record_artifacts(spec, result)
                else:
                    self.workload_registry.set_result(workload_id, result)
                    self.workload_registry.update_state(workload_id, WorkloadState.FAILED, error_message=result.error_message)
                
                if self.m7_ingestor:
                    self.m7_ingestor.on_workload_completed(
                        workload_type=spec.task_type,
                        duration=result.execution_duration_seconds,
                        ram_peak=1024,
                        failed=not result.success
                    )
            except Exception as e:
                logger.error(f"Failed to dispatch workload {workload_id} to worker {decision.selected_worker_id}: {e}")
                self.workload_registry.update_state(workload_id, WorkloadState.FAILED, error_message=str(e))
                # For Placement Recovery (6.16), a robust orchestrator would loop here and try the next candidate.
                # Here we just mark as failed and let the client resubmit.
                # But to satisfy 6.16 formally, let's implement a single retry inline:
                profiles = [p for p in profiles if p.worker_id != decision.selected_worker_id]
                if profiles:
                    fallback_decision = self.placement_engine.evaluate(spec, profiles)
                    if fallback_decision:
                        self.workload_registry.set_placement(workload_id, fallback_decision)
                        logger.warning(f"Placement Recovery: Re-placed workload {workload_id} to {fallback_decision.selected_worker_id}")
                        fallback_worker = next((w for w in workers if w.worker_id == fallback_decision.selected_worker_id), None)
                        if fallback_worker:
                            try:
                                resp = await client.post(f"{fallback_worker.endpoint_url}/api/v1/workloads/execute", json=spec.model_dump(mode='json'), timeout=60.0)
                                resp.raise_for_status()
                                result = WorkloadExecutionResult(**resp.json())
                                self.workload_registry.set_result(workload_id, result)
                                self.workload_registry.update_state(
                                    workload_id,
                                    WorkloadState.COMPLETED if result.success else WorkloadState.FAILED,
                                    error_message=None if result.success else result.error_message
                                )
                                if result.success:
                                    self._record_artifacts(spec, result)
                                if self.m7_ingestor:
                                    self.m7_ingestor.on_workload_completed(
                                        workload_type=spec.task_type,
                                        duration=result.execution_duration_seconds,
                                        ram_peak=1024,
                                        failed=not result.success
                                    )
                                return
                            except Exception as e2:
                                logger.error(f"Fallback Exception: {e2!r}")
                                self.workload_registry.update_state(workload_id, WorkloadState.FAILED, error_message=str(e2))

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
        """Trigger migration for all executing workloads on a specific worker."""
        # Find all workloads currently running on this worker
        with self.workload_registry._lock:
            active_workloads = [
                wid for wid, record in self.workload_registry._workloads.items()
                if record.state == WorkloadState.PLACED and record.placement and record.placement.selected_worker_id == worker_id
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

