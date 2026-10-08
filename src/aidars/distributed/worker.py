"""AIDAR Distributed Worker Node Runtime.

Orchestrates local CAS storage, data plane HTTP streaming server,
control plane coordinator registration/heartbeats, missing-set resolution,
and resilient peer asset transfer.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Union

import httpx

from aidars.distributed.cas_adapter import CASAdapter, LocalCASAdapter
from aidars.distributed.client import DistributedClient
from aidars.distributed.metrics import TransferMetricsTracker
from aidars.distributed.models import (
    CandidateSource,
    HeartbeatPayload,
    HeartbeatResponse,
    FailureCategory,
    LocateAssetsResponse,
    TransferResult,
    WorkerCapabilities,
    WorkerInfo,
    WorkerMetrics,
    WorkerRegistrationPayload,
    WorkerRegistrationResponse,
    WorkerResourceProfile,
    WorkerStatus,
    RuntimeExecutionContext,
    WorkloadSpec,
    WorkloadExecutionResult,
    ExecutionState, ExecutionStatus,
    validate_sha256_hex,
)
from aidars.distributed.server import WorkerServer
from aidars.distributed.singleflight import SingleFlight
from aidars.distributed.resources import WorkerResourceMonitor
from aidars.distributed.execution import ExecutionManager
from aidars.distributed.runtime import GenericSubprocessRuntime

logger = logging.getLogger(__name__)


@dataclass
class _ExecutionRecord:
    """Worker-local execution ledger keyed by coordinator attempt_id."""

    attempt_id: str
    workload_id: str
    state: ExecutionState = ExecutionState.ACCEPTED
    result: Optional[WorkloadExecutionResult] = None
    task: Optional[asyncio.Task] = None
    cancel_requested: bool = False
    accepted_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


class DistributedWorker:
    """Worker node participant in the AIDAR distributed asset distribution mesh."""

    def __init__(
        self,
        worker_id: Optional[str] = None,
        cas_adapter: Optional[CASAdapter] = None,
        cas_dir: Optional[Union[str, Path]] = None,
        ip_address: str = "127.0.0.1",
        port: int = 8000,
        coordinator_url: Optional[str] = None,
        capabilities: Optional[WorkerCapabilities] = None,
        capacity_bytes: int = 100 * 1024 * 1024 * 1024,
        heartbeat_interval_seconds: float = 5.0,
        http_client: Optional[httpx.AsyncClient] = None,
        can_execute_workloads: bool = True,
        bootstrap_secret: Optional[str] = None,
    ) -> None:
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.ip_address = ip_address
        self.port = port
        self.endpoint_url = f"http://{ip_address}:{port}"
        self.coordinator_url = coordinator_url.rstrip("/") if coordinator_url else None
        self.capacity_bytes = capacity_bytes
        self.heartbeat_interval_seconds = max(0.5, float(heartbeat_interval_seconds))
        # False for a pure CAS/asset-source node (e.g. a Master ingestion
        # point): it registers, serves, and receives assets like any other
        # node, but PlacementEngine must never select it for compute.
        self.can_execute_workloads = can_execute_workloads
        self._last_reported_inventory: Set[str] = set()

        # 1. CAS Adapter Initialization
        if cas_adapter is not None:
            self.cas = cas_adapter
        elif cas_dir is not None:
            self.cas = LocalCASAdapter(cas_dir=cas_dir)
        else:
            self.cas = LocalCASAdapter(cas_dir=Path(".aidars_cas"))

        # 2. Worker Capabilities & Metrics
        self.capabilities = capabilities or WorkerCapabilities()
        self.metrics_tracker = TransferMetricsTracker()
        self.node_metrics = WorkerMetrics(
            used_bytes=getattr(self.cas, "get_cas_stats", lambda: {})().get("total_bytes", 0),
            available_bytes=max(0, self.capacity_bytes - getattr(self.cas, "get_cas_stats", lambda: {})().get("total_bytes", 0)),
        )

        # 3. Server & Client
        self.server = WorkerServer(
            cas_adapter=self.cas,
            worker_id=self.worker_id,
            host=self.ip_address,
            port=self.port,
            endpoint_url=self.endpoint_url,
            capabilities=self.capabilities,
            metrics=self.node_metrics,
            distributed_worker=self
        )

        self.client = DistributedClient(
            cas_adapter=self.cas,
            coordinator_url=self.coordinator_url,
            worker_id=self.worker_id,
            http_client=http_client,
            chunk_size=self.capabilities.chunk_size_bytes,
            bootstrap_secret=bootstrap_secret,
        )

        self.resource_monitor = WorkerResourceMonitor(
            worker_id=self.worker_id,
            endpoint_url=self.endpoint_url,
            ip_address=self.ip_address,
        )
        self.execution_manager = ExecutionManager(
            cas_adapter=self.cas,
            workloads_dir=str(Path(self.cas.cas_dir).parent / "workloads")
        )
        self.single_flight = SingleFlight()

        # 4. State & Background Tasks
        self.status = WorkerStatus.ACTIVE
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._last_heartbeat_ack_utc: float = 0.0
        # M20: execution control-plane state is keyed by attempt_id so a
        # submit can be safely retried without starting a second process.
        self._executions: Dict[str, _ExecutionRecord] = {}

    @property
    def inventory_hashes(self) -> Set[str]:
        """Return currently cached inventory hashes."""
        if hasattr(self.cas, "get_inventory_hashes"):
            return self.cas.get_inventory_hashes()
        return set()

    def get_resource_profile(self) -> WorkerResourceProfile:
        """Collect a fresh hardware snapshot using the existing monitor.

        The coordinator owns scheduling state (status, inventory, and active
        workload count), so those values are replaced from coordinator state
        when the snapshot is received. Resource measurements themselves come
        from this worker's monitor.
        """
        profile = self.resource_monitor.get_profile(
            active_workload_count=0,
            local_cached_hashes=self.inventory_hashes,
        )
        profile.status = self.status
        profile.can_execute_workloads = self.can_execute_workloads

        # Keep the existing general metrics current as well. available_bytes
        # remains CAS capacity; physical RAM bytes live only in resource_profile.
        self.node_metrics.cpu_percent = profile.cpu_utilization_percent
        self.node_metrics.ram_percent = (
            100.0 * (profile.ram_total_bytes - profile.ram_available_bytes)
            / profile.ram_total_bytes
            if profile.ram_total_bytes > 0 else 0.0
        )
        return profile

    def get_worker_info(self) -> WorkerInfo:
        """Construct WorkerInfo model snapshot."""
        stats = getattr(self.cas, "get_cas_stats", lambda: {})()
        used = stats.get("total_bytes", 0)
        return WorkerInfo(
            worker_id=self.worker_id,
            endpoint_url=self.endpoint_url,
            ip_address=self.ip_address,
            port=self.port,
            status=self.status,
            capacity_bytes=self.capacity_bytes,
            used_bytes=used,
            capabilities=self.capabilities,
            inventory_hashes=self.inventory_hashes,
            last_heartbeat_utc=time.time(),
            last_metrics=self.node_metrics,
            resource_profile=self.get_resource_profile(),
            can_execute_workloads=self.can_execute_workloads,
        )

    # ========================================================================
    # Lifecycle Management
    # ========================================================================

    async def start(self) -> None:
        """Start worker runtime, register with coordinator, and start heartbeats."""
        if self._running:
            return
        self._running = True

        if self.coordinator_url:
            await self.register()
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info("DistributedWorker %s started at %s", self.worker_id, self.endpoint_url)

    async def stop(self) -> None:
        """Gracefully stop worker runtime and unregister from coordinator."""
        if not self._running:
            return
        self._running = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        if self.coordinator_url:
            try:
                await self.client.unregister_worker(self.worker_id)
            except Exception as exc:
                logger.warning("Failed to unregister worker %s: %s", self.worker_id, exc)

        await self.client.aclose()
        self.resource_monitor.shutdown()
        logger.info("DistributedWorker %s stopped.", self.worker_id)

    async def __aenter__(self) -> DistributedWorker:
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()

    # ========================================================================
    # Control Plane RPCs
    # ========================================================================

    async def register(self, coordinator_url: Optional[str] = None) -> WorkerRegistrationResponse:
        """Register worker with the coordinator."""
        coord_url = coordinator_url or self.coordinator_url
        if not coord_url:
            raise ValueError("Coordinator URL required for registration")

        current_inventory = self.inventory_hashes
        resource_profile = self.get_resource_profile()
        payload = WorkerRegistrationPayload(
            worker_id=self.worker_id,
            endpoint_url=self.endpoint_url,
            ip_address=self.ip_address,
            port=self.port,
            capacity_bytes=self.capacity_bytes,
            used_bytes=self.node_metrics.used_bytes,
            resource_profile=resource_profile,
            capabilities=self.capabilities,
            inventory_hashes=current_inventory,
            can_execute_workloads=self.can_execute_workloads,
        )
        resp = await self.client.register_worker(payload, coordinator_url=coord_url)
        self._last_heartbeat_ack_utc = time.time()
        # Registration already reported the full inventory, so the next
        # heartbeat should only report hashes added/removed since now.
        self._last_reported_inventory = set(current_inventory)
        logger.info("Worker %s registered with coordinator %s", self.worker_id, resp.coordinator_id)
        return resp

    async def send_heartbeat(self, coordinator_url: Optional[str] = None) -> HeartbeatResponse:
        """Send a single heartbeat ping to coordinator."""
        coord_url = coordinator_url or self.coordinator_url
        if not coord_url:
            raise ValueError("Coordinator URL required for heartbeat")

        stats = getattr(self.cas, "get_cas_stats", lambda: {})()
        used = stats.get("total_bytes", 0)
        self.node_metrics.used_bytes = used
        self.node_metrics.available_bytes = max(0, self.capacity_bytes - used)
        resource_profile = self.get_resource_profile()

        # Report inventory changes since the last successful heartbeat/
        # registration, so newly-ingested assets (e.g. a Master ingesting
        # M4-resolved files between heartbeats) become visible to
        # WorkerRegistry/locate_assets_sync without requiring re-registration.
        current_inventory = self.inventory_hashes
        added = current_inventory - self._last_reported_inventory
        removed = self._last_reported_inventory - current_inventory

        payload = HeartbeatPayload(
            worker_id=self.worker_id,
            timestamp_utc=time.time(),
            metrics=self.node_metrics,
            resource_profile=resource_profile,
            active_transfers=self.node_metrics.active_transfers,
            used_bytes=used,
            available_bytes=self.node_metrics.available_bytes,
            inventory_delta_added=added,
            inventory_delta_removed=removed,
        )
        resp = await self.client.send_heartbeat(self.worker_id, payload, coordinator_url=coord_url)
        self._last_heartbeat_ack_utc = time.time()
        if resp.re_register_required:
            logger.warning("Coordinator requested re-registration for worker %s", self.worker_id)
            await self.register(coord_url)
        else:
            self._last_reported_inventory = current_inventory
        return resp

    async def _heartbeat_loop(self) -> None:
        """Background heartbeat loop."""
        while self._running:
            try:
                await asyncio.sleep(self.heartbeat_interval_seconds)
                if not self._running:
                    break
                await self.send_heartbeat()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Heartbeat failed for worker %s: %s", self.worker_id, exc)

    # ========================================================================
    # Asset Sync & Missing-Set Resolution
    # ========================================================================

    def calculate_missing_set(self, required_hashes: Iterable[str]) -> Set[str]:
        """Compute set difference `required \\ cached` locally via CASAdapter."""
        return self.cas.get_missing_hashes(required_hashes)

    async def sync_assets(
        self,
        required_hashes: Iterable[str],
        coordinator_url: Optional[str] = None,
    ) -> Dict[str, TransferResult]:
        """Synchronize required assets with missing-set calculation, locate, and streaming download."""
        t0 = time.time()
        missing = self.calculate_missing_set(required_hashes)
        results: Dict[str, TransferResult] = {}

        # 1. Record local cache hits
        for raw_h in required_hashes:
            try:
                norm_h = validate_sha256_hex(raw_h)
            except ValueError:
                continue
            if norm_h not in missing:
                size = getattr(self.cas, "get_asset_size", lambda x: 0)(norm_h) or 0
                path = getattr(self.cas, "get_asset_path", lambda x: None)(norm_h)
                self.metrics_tracker.record_cache_hit(norm_h, size)
                results[norm_h] = TransferResult(
                    sha256=norm_h,
                    success=True,
                    bytes_transferred=0,
                    total_bytes=size,
                    verified_sha256=norm_h,
                    committed_path=str(path) if path else None,
                    source_worker_id="local_cas",
                    source_endpoint_url="local://cas",
                    duration_seconds=0.0,
                )

        if not missing:
            return results

        # 2. Query Coordinator for missing asset candidates
        coord_url = coordinator_url or self.coordinator_url
        if not coord_url:
            for m_h in missing:
                results[m_h] = TransferResult(
                    sha256=m_h,
                    success=False,
                    error_message="No coordinator URL configured to locate missing assets",
                )
                self.metrics_tracker.record_transfer_failure(m_h)
            return results

        locate_resp = await self.client.locate_assets(
            missing_hashes=list(missing),
            requester_worker_id=self.worker_id,
            requester_ip=self.ip_address,
            coordinator_url=coord_url,
        )

        # 3. Stream missing assets concurrently
        download_targets: Dict[str, List[CandidateSource]] = {
            h: cands for h, cands in locate_resp.locations.items() if cands
        }

        if download_targets:
            transferred_map = await self.client.download_missing_assets(
                download_targets,
                cas_adapter=self.cas,
            )
            for h, res in transferred_map.items():
                results[h] = res
                if res.success:
                    self.metrics_tracker.record_network_transfer(
                        sha256=h,
                        size_bytes=res.total_bytes,
                        bytes_transferred=res.bytes_transferred,
                        duration_seconds=res.duration_seconds,
                        source_worker_id=res.source_worker_id,
                        resumed=res.resumed_bytes > 0,
                        failed_over=res.retry_count > 0,
                    )
                else:
                    self.metrics_tracker.record_transfer_failure(
                        sha256=h,
                        size_bytes=res.total_bytes,
                        source_worker_id=res.source_worker_id,
                    )

        # 4. Handle unresolvable hashes
        for unres_h in locate_resp.unresolved_hashes:
            if unres_h not in results:
                results[unres_h] = TransferResult(
                    sha256=unres_h,
                    success=False,
                    error_message=f"No candidate sources holding asset {unres_h}",
                )
                self.metrics_tracker.record_transfer_failure(unres_h)
        
        return results

    def _execution_status(self, record: _ExecutionRecord) -> ExecutionStatus:
        return ExecutionStatus(
            attempt_id=record.attempt_id,
            workload_id=record.workload_id,
            state=record.state,
            result=record.result,
        )

    async def submit_execution(
        self, attempt_id: str, spec: WorkloadSpec,
        execution_context: Optional[RuntimeExecutionContext] = None,
    ) -> ExecutionStatus:
        """Idempotently accept an execution and run it in the background."""
        existing = self._executions.get(attempt_id)
        if existing is not None:
            return self._execution_status(existing)

        record = _ExecutionRecord(attempt_id=attempt_id, workload_id=spec.workload_id)
        self._executions[attempt_id] = record
        record.task = asyncio.create_task(
            self._run_execution_record(record, spec, execution_context),
            name=f"aidar-execution-{attempt_id}",
        )
        return self._execution_status(record)

    async def _run_execution_record(
        self, record: _ExecutionRecord, spec: WorkloadSpec,
        execution_context: Optional[RuntimeExecutionContext],
    ) -> None:
        record.state = ExecutionState.RUNNING
        record.updated_at = time.time()
        try:
            if record.cancel_requested:
                record.state = ExecutionState.CANCELLED
                record.updated_at = time.time()
                return

            result = await self.execute_workload(spec, execution_context=execution_context)
            record.result = result
            record.state = ExecutionState.CANCELLED if record.cancel_requested else (
                ExecutionState.SUCCEEDED if result.success else ExecutionState.FAILED
            )
        except asyncio.CancelledError:
            record.state = ExecutionState.CANCELLED
            record.result = WorkloadExecutionResult(
                workload_id=spec.workload_id, worker_id=self.worker_id, success=False,
                output_asset_hashes=set(), execution_duration_seconds=0.0,
                error_message="Execution cancelled by request",
                failure_category=FailureCategory.EXECUTION_FAILURE,
            )
        except Exception as exc:
            logger.exception("Execution %s failed outside ExecutionManager", record.attempt_id)
            record.state = ExecutionState.FAILED
            record.result = WorkloadExecutionResult(
                workload_id=spec.workload_id, worker_id=self.worker_id, success=False,
                output_asset_hashes=set(), execution_duration_seconds=0.0,
                error_message=str(exc), failure_category=FailureCategory.EXECUTION_FAILURE,
            )
        finally:
            record.updated_at = time.time()

    async def get_execution(self, attempt_id: str) -> Optional[ExecutionStatus]:
        record = self._executions.get(attempt_id)
        return self._execution_status(record) if record is not None else None

    async def cancel_execution(self, attempt_id: str) -> Optional[ExecutionStatus]:
        record = self._executions.get(attempt_id)
        if record is None:
            return None
        if record.state in (ExecutionState.SUCCEEDED, ExecutionState.FAILED, ExecutionState.CANCELLED):
            return self._execution_status(record)

        record.cancel_requested = True
        record.updated_at = time.time()
        cancelled_runtime = await self.execution_manager.cancel_workload(record.workload_id)
        if not cancelled_runtime and record.task is not None and not record.task.done():
            # The task may still be in asset synchronization, before a
            # RuntimeAdapter exists. Cancelling the task is then safe and the
            # wrapper records the terminal cancelled state.
            record.task.cancel()
        return self._execution_status(record)

    async def ack_execution(self, attempt_id: str) -> bool:
        record = self._executions.get(attempt_id)
        if record is None:
            return False
        if record.state not in (ExecutionState.SUCCEEDED, ExecutionState.FAILED, ExecutionState.CANCELLED):
            raise RuntimeError("execution is not terminal")
        self._executions.pop(attempt_id, None)
        return True

    async def checkpoint_workload(self, workload_id: str) -> bool:
        """Request the ExecutionManager to gracefully checkpoint an active workload."""
        logger.info("Worker %s received checkpoint request for workload %s", self.worker_id, workload_id)
        return await self.execution_manager.checkpoint_workload(workload_id)

    async def cancel_workload(self, workload_id: str) -> bool:
        """Request the ExecutionManager to gracefully cancel an active workload."""
        logger.info("Worker %s received cancel request for workload %s", self.worker_id, workload_id)
        return await self.execution_manager.cancel_workload(workload_id)

    # ========================================================================
    # Workload Execution
    # ========================================================================

    async def execute_workload(self, spec: WorkloadSpec,
                               execution_context: Optional[RuntimeExecutionContext] = None) -> WorkloadExecutionResult:
        """Synchronize required assets and execute the workload in a sandbox."""
        logger.info("Worker %s executing workload %s", self.worker_id, spec.workload_id)
        
        # 1. Sync dependencies with SingleFlight deduplication per asset
        transfer_start = time.time()
        if spec.input_asset_hashes:
            # For simplicity, we just sync the whole set at once, but we could wrap 
            # each hash in single_flight.run. The DistributedClient manages concurrency.
            # Using singleflight at a higher level to prevent concurrent syncs of the same workload spec.
            def _sync():
                return self.sync_assets(spec.input_asset_hashes)
            
            sync_results = await self.single_flight.run(
                key=f"sync-{spec.workload_id}",
                operation=_sync
            )
            
            transfer_duration = time.time() - transfer_start
            for h, res in sync_results.items():
                if not res.success:
                    return WorkloadExecutionResult(
                        workload_id=spec.workload_id,
                        worker_id=self.worker_id,
                        success=False,
                        output_asset_hashes=set(),
                        execution_duration_seconds=0.0,
                        transfer_duration_seconds=transfer_duration,
                        error_message=f"Failed to sync dependency {h}: {res.error_message}",
                        failure_category=FailureCategory.ASSET_TRANSFER_FAILURE,
                    )
        else:
            transfer_duration = time.time() - transfer_start

        # 2. Setup runtime and execute
        runtime = GenericSubprocessRuntime()
        result = await self.execution_manager.execute_workload(
            spec=spec,
            worker_id=self.worker_id,
            runtime=runtime,
            execution_context=execution_context,
        )
        result.transfer_duration_seconds = transfer_duration
        
        return result
