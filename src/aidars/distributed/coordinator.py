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

from fastapi import APIRouter, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse

from aidars.distributed.models import (
    CandidateSource,
    ClusterTelemetry,
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
from aidars.distributed.prioritizer import CandidatePrioritizer, LatencyTracker
from aidars.distributed.registry import ClusterStats, WorkerRegistry
from aidars.distributed.workload_registry import WorkloadRegistry, WorkloadRecord
from aidars.distributed.workload import WorkloadOrchestrator

logger = logging.getLogger(__name__)


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
    ) -> None:
        self.coordinator_id = coordinator_id or f"coord-{uuid.uuid4().hex[:8]}"
        self.heartbeat_interval_seconds = float(heartbeat_interval_seconds)
        self.heartbeat_timeout_seconds = float(heartbeat_timeout_seconds)
        self.eviction_interval_seconds = float(eviction_interval_seconds)
        self.penalty_decay_interval_seconds = float(penalty_decay_interval_seconds)

        self.latency_tracker = latency_tracker or LatencyTracker()
        self.prioritizer = prioritizer or CandidatePrioritizer(latency_tracker=self.latency_tracker)
        self.registry = registry or WorkerRegistry(
            heartbeat_timeout_seconds=self.heartbeat_timeout_seconds
        )
        self.workload_registry = WorkloadRegistry()
        
        # M7 Intelligence Initialization
        self.m7_memory = TelemetryMemory()
        self.m7_ingestor = TelemetryIngestor(self.m7_memory)
        self.m7_bridge = M7OrchestratorBridge(self.m7_memory)
        
        self.orchestrator = WorkloadOrchestrator(
            self.registry, 
            self.workload_registry, 
            m7_bridge=self.m7_bridge,
            m7_ingestor=self.m7_ingestor
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
        """Start the background eviction and decay task."""
        if self._running:
            return
        self._running = True
        self._eviction_task = asyncio.create_task(self._run_eviction_loop())
        self._health_task = asyncio.create_task(self._evaluate_cluster_health_loop())
        logger.info("CoordinatorService %s started.", self.coordinator_id)

    async def stop(self) -> None:
        """Stop the background eviction task gracefully."""
        if not self._running:
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

        logger.info("CoordinatorService %s stopped.", self.coordinator_id)

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
                                
            except Exception as exc:
                logger.error("Coordinator health loop error: %s", exc)

    # ========================================================================
    # Programmatic / Core API Methods
    # ========================================================================

    def register_worker_sync(self, payload: WorkerRegistrationPayload) -> WorkerRegistrationResponse:
        """Register worker programmatically without HTTP overhead."""
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
            last_heartbeat_utc=time.time(),
            registered_at_utc=time.time(),
            tags=payload.tags,
        )
        registered = self.registry.register_worker(worker_info)
        return WorkerRegistrationResponse(
            status="registered",
            worker_id=registered.worker_id,
            coordinator_id=self.coordinator_id,
            heartbeat_interval_seconds=self.heartbeat_interval_seconds,
            heartbeat_timeout_seconds=self.heartbeat_timeout_seconds,
            registered_at_utc=registered.registered_at_utc,
            acknowledged_inventory_count=len(registered.inventory_hashes),
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

        router = APIRouter(prefix="/api/v1")

        # --- Worker Registration ---
        @router.post(
            "/workers/register",
            response_model=WorkerRegistrationResponse,
            status_code=status.HTTP_200_OK,
            summary="Register a new or returning worker node",
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
            if worker and worker.last_metrics:
                self.m7_ingestor.on_worker_heartbeat(
                    worker_id=worker_id,
                    cpu_utilization_percent=worker.last_metrics.cpu_percent,
                    ram_total=worker.capacity_bytes or 0,
                    ram_available=worker.available_bytes,
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
        )
        async def cluster_stats() -> ClusterTelemetry:
            return self.get_cluster_stats_sync()

        # --- Worker Queries ---
        @router.get(
            "/workers",
            response_model=List[WorkerInfo],
            status_code=status.HTTP_200_OK,
            summary="List all currently registered workers",
        )
        async def list_workers(active_only: bool = True) -> List[WorkerInfo]:
            return self.registry.list_workers(active_only=active_only)

        @router.get(
            "/workers/{worker_id}",
            response_model=WorkerInfo,
            status_code=status.HTTP_200_OK,
            summary="Get specific worker details",
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
        )
        async def submit_workload(spec: WorkloadSpec) -> Dict[str, str]:
            workload_id = await self.orchestrator.submit_workload(spec)
            return {"workload_id": workload_id, "status": "submitted"}

        @router.get(
            "/workloads/{workload_id}",
            status_code=status.HTTP_200_OK,
            summary="Get workload status",
        )
        async def get_workload_status(workload_id: str) -> Dict[str, Any]:
            record = self.workload_registry.get_workload(workload_id)
            if not record:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Workload '{workload_id}' not found.",
                )
            
            resp = {
                "workload_id": workload_id,
                "state": record.state.value,
                "submitted_at": record.submitted_at,
                "completed_at": record.completed_at,
                "error_message": record.error_message,
            }
            if record.placement_decision:
                resp["placement"] = record.placement_decision.model_dump()
            if record.execution_result:
                resp["result"] = record.execution_result.model_dump()
                
            return resp

        app.include_router(router)
        return app
