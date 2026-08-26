"""Workload lifecycle orchestration.

Ties together models, placement, and registry to drive a workload from SUBMITTED to COMPLETED.
"""

import asyncio
import logging
from typing import List

from aidars.distributed.models import WorkloadSpec
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadState

logger = logging.getLogger(__name__)


class WorkloadOrchestrator:
    """Central orchestrator for workload lifecycle on the Coordinator."""

    def __init__(self, registry: WorkerRegistry, workload_registry: WorkloadRegistry) -> None:
        self.registry = registry
        self.workload_registry = workload_registry
        self.placement_engine = PlacementEngine()

    async def submit_workload(self, spec: WorkloadSpec) -> str:
        """Submit a new workload for execution. Returns the workload_id."""
        record = self.workload_registry.add_workload(spec)
        
        # Asynchronously process the placement and execution
        asyncio.create_task(self._process_workload(spec.workload_id))
        
        return spec.workload_id

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
                    cpu_cores_total=4, # Placeholder
                    cpu_utilization_percent=info.last_metrics.cpu_percent if info.last_metrics else 0.0,
                    ram_total_bytes=info.capacity_bytes or 8_000_000_000,
                    ram_available_bytes=info.available_bytes,
                    active_workload_count=0, # Placeholder
                    local_cached_hashes=info.inventory_hashes,
                    timestamp_utc=time.time(),
                ))
        
        decision = self.placement_engine.evaluate(spec, profiles)
        if not decision:
            self.workload_registry.update_state(
                workload_id, WorkloadState.UNSCHEDULABLE, error_message="No suitable worker found"
            )
            return

        self.workload_registry.set_placement(workload_id, decision)
        self.workload_registry.update_state(workload_id, WorkloadState.PLACED)
        
        # Further steps (Syncing, Executing) require dispatching to the selected worker.
        # This orchestrator would make HTTP calls to the selected worker's API.
        logger.info(f"Workload {workload_id} placed on {decision.selected_worker_id}")
