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
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Union

import httpx

from aidars.distributed.cas_adapter import CASAdapter, LocalCASAdapter
from aidars.distributed.client import DistributedClient
from aidars.distributed.metrics import TransferMetricsTracker
from aidars.distributed.models import (
    CandidateSource,
    HeartbeatPayload,
    HeartbeatResponse,
    LocateAssetsResponse,
    TransferResult,
    WorkerCapabilities,
    WorkerInfo,
    WorkerMetrics,
    WorkerRegistrationPayload,
    WorkerRegistrationResponse,
    WorkerStatus,
    validate_sha256_hex,
)
from aidars.distributed.server import WorkerServer

logger = logging.getLogger(__name__)


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
    ) -> None:
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.ip_address = ip_address
        self.port = port
        self.endpoint_url = f"http://{ip_address}:{port}"
        self.coordinator_url = coordinator_url.rstrip("/") if coordinator_url else None
        self.capacity_bytes = capacity_bytes
        self.heartbeat_interval_seconds = max(0.5, float(heartbeat_interval_seconds))

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
        )

        self.client = DistributedClient(
            cas_adapter=self.cas,
            coordinator_url=self.coordinator_url,
            worker_id=self.worker_id,
            http_client=http_client,
            chunk_size=self.capabilities.chunk_size_bytes,
        )

        # 4. State & Background Tasks
        self.status = WorkerStatus.ACTIVE
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._last_heartbeat_ack_utc: float = 0.0

    @property
    def inventory_hashes(self) -> Set[str]:
        """Return currently cached inventory hashes."""
        if hasattr(self.cas, "get_inventory_hashes"):
            return self.cas.get_inventory_hashes()
        return set()

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

        payload = WorkerRegistrationPayload(
            worker_id=self.worker_id,
            endpoint_url=self.endpoint_url,
            ip_address=self.ip_address,
            port=self.port,
            capacity_bytes=self.capacity_bytes,
            used_bytes=self.node_metrics.used_bytes,
            capabilities=self.capabilities,
            inventory_hashes=self.inventory_hashes,
        )
        resp = await self.client.register_worker(payload, coordinator_url=coord_url)
        self._last_heartbeat_ack_utc = time.time()
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

        payload = HeartbeatPayload(
            worker_id=self.worker_id,
            timestamp_utc=time.time(),
            metrics=self.node_metrics,
            active_transfers=self.node_metrics.active_transfers,
            used_bytes=used,
            available_bytes=self.node_metrics.available_bytes,
        )
        resp = await self.client.send_heartbeat(self.worker_id, payload, coordinator_url=coord_url)
        self._last_heartbeat_ack_utc = time.time()
        if resp.re_register_required:
            logger.warning("Coordinator requested re-registration for worker %s", self.worker_id)
            await self.register(coord_url)
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
