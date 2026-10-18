"""AIDAR Distributed Client for Cluster Operations & Binary Asset Transfers.

Provides an asynchronous client for control plane RPCs (worker registration, heartbeat,
asset location) and data plane streaming asset transfers with concurrency control,
incremental SHA-256 verification, and automatic candidate fail-over.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

import httpx

from aidars.distributed.cas_adapter import CASAdapter
from aidars.distributed.models import (
    CandidateSource,
    ClusterTelemetry,
    HeartbeatPayload,
    HeartbeatResponse,
    LocateAssetsRequest,
    LocateAssetsResponse,
    PingRequest,
    PongResponse,
    TransferResult,
    WorkerRegistrationPayload,
    WorkerRegistrationResponse,
    validate_sha256_hex,
)
from aidars.distributed.transfer import (
    DEFAULT_CHUNK_SIZE,
    CandidateExhaustedError,
    transfer_asset_with_failover,
)

logger = logging.getLogger(__name__)


class DistributedClient:
    """Asynchronous client for AIDAR distributed cluster operations and binary asset transfers."""

    def __init__(
        self,
        cas_adapter: Optional[CASAdapter] = None,
        coordinator_url: Optional[str] = None,
        worker_id: Optional[str] = None,
        http_client: Optional[httpx.AsyncClient] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        default_timeout_seconds: float = 30.0,
        max_concurrent_transfers: int = 16,
        bootstrap_secret: Optional[str] = None,
    ) -> None:
        self.cas_adapter = cas_adapter
        self.coordinator_url = coordinator_url.rstrip("/") if coordinator_url else None
        self.worker_id = worker_id or f"client-{uuid.uuid4().hex[:8]}"
        self.chunk_size = chunk_size
        self.default_timeout_seconds = default_timeout_seconds
        self.max_concurrent_transfers = max(1, int(max_concurrent_transfers))
        self.semaphore = asyncio.Semaphore(self.max_concurrent_transfers)

        # M9: optional credentials. bootstrap_secret authorizes ONLY
        # register_worker(); nothing else. Never logged. _worker_credential
        # is populated automatically from a successful register_worker()
        # response and used for send_heartbeat()/unregister_worker() from
        # then on -- callers don't need to pass it explicitly. Both are
        # None by default, preserving prior unauthenticated behavior for
        # any caller/test that doesn't opt in (e.g. a coordinator running
        # in insecure mode).
        self._bootstrap_secret = bootstrap_secret
        self._worker_credential: Optional[str] = None

        self._internal_client = http_client is None
        self.http_client = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(default_timeout_seconds, connect=10.0),
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )
        self._http_client = self.http_client

    # ========================================================================
    # Lifecycle Context Management
    # ========================================================================

    async def aclose(self) -> None:
        """Close the underlying HTTP client if managed internally."""
        if self._internal_client:
            await self.http_client.aclose()

    async def __aenter__(self) -> DistributedClient:
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.aclose()

    # ========================================================================
    # Control Plane RPCs
    # ========================================================================

    def _resolve_coord_url(self, coordinator_url: Optional[str] = None) -> str:
        url = coordinator_url.rstrip("/") if coordinator_url else self.coordinator_url
        if not url:
            raise ValueError("Coordinator URL must be provided either in constructor or method call")
        return url

    @staticmethod
    def _auth_headers(token: Optional[str]) -> Dict[str, str]:
        """M9: build an Authorization header if a credential is set, or an
        empty dict otherwise -- never send a header with an empty/None
        token, and never log the token itself."""
        return {"Authorization": f"Bearer {token}"} if token else {}

    async def register_worker(
        self,
        payload: WorkerRegistrationPayload,
        coordinator_url: Optional[str] = None,
    ) -> WorkerRegistrationResponse:
        """Register a worker node with the coordinator.

        Authenticated with this client's bootstrap_secret, if configured.
        On success, the coordinator-issued worker_credential (if present
        in the response) is stored and used automatically by
        send_heartbeat()/unregister_worker() from then on.
        """
        base_url = self._resolve_coord_url(coordinator_url)
        url = f"{base_url}/api/v1/workers/register"
        resp = await self.http_client.post(
            url,
            json=payload.model_dump(mode="json"),
            headers=self._auth_headers(self._bootstrap_secret),
            timeout=self.default_timeout_seconds,
        )
        resp.raise_for_status()
        result = WorkerRegistrationResponse.model_validate(resp.json())
        if result.worker_credential:
            self._worker_credential = result.worker_credential
        return result

    async def send_heartbeat(
        self,
        worker_id: str,
        payload: Optional[HeartbeatPayload] = None,
        coordinator_url: Optional[str] = None,
    ) -> HeartbeatResponse:
        """Send a periodic heartbeat from a worker to the coordinator."""
        base_url = self._resolve_coord_url(coordinator_url)
        url = f"{base_url}/api/v1/workers/{worker_id}/heartbeat"
        body = payload.model_dump(mode="json") if payload else {}
        resp = await self.http_client.post(
            url,
            json=body,
            headers=self._auth_headers(self._worker_credential),
            timeout=self.default_timeout_seconds,
        )
        resp.raise_for_status()
        return HeartbeatResponse.model_validate(resp.json())

    async def unregister_worker(
        self,
        worker_id: str,
        coordinator_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Unregister a worker node gracefully from the coordinator."""
        base_url = self._resolve_coord_url(coordinator_url)
        url = f"{base_url}/api/v1/workers/{worker_id}/unregister"
        resp = await self.http_client.post(
            url,
            headers=self._auth_headers(self._worker_credential),
            timeout=self.default_timeout_seconds,
        )
        resp.raise_for_status()
        return resp.json()

    async def locate_assets(
        self,
        missing_hashes: List[str],
        requester_worker_id: Optional[str] = None,
        requester_ip: Optional[str] = None,
        coordinator_url: Optional[str] = None,
        max_candidates: int = 5,
        include_degraded: bool = True,
    ) -> LocateAssetsResponse:
        """Query coordinator to locate candidate peer sources holding missing asset hashes."""
        base_url = self._resolve_coord_url(coordinator_url)
        url = f"{base_url}/api/v1/assets/locate"
        req_payload = LocateAssetsRequest(
            requester_worker_id=requester_worker_id or self.worker_id,
            missing_hashes=missing_hashes,
            requester_ip=requester_ip,
            max_candidates_per_asset=max_candidates,
            include_degraded=include_degraded,
        )
        resp = await self.http_client.post(
            url,
            json=req_payload.model_dump(mode="json"),
            headers=self._auth_headers(self._worker_credential),
            timeout=self.default_timeout_seconds,
        )
        resp.raise_for_status()
        return LocateAssetsResponse.model_validate(resp.json())

    async def ping(self, target_url: str) -> float:
        """Measure network round-trip time (RTT) in milliseconds to a worker or coordinator."""
        clean_url = target_url.strip().rstrip("/")
        url = f"{clean_url}/api/v1/ping"
        t0 = time.time()
        req = PingRequest(
            client_worker_id=self.worker_id,
            client_timestamp_utc=t0,
        )
        resp = await self.http_client.post(url, json=req.model_dump(mode="json"), timeout=10.0)
        resp.raise_for_status()
        rtt_ms = max(0.01, (time.time() - t0) * 1000.0)
        return rtt_ms

    async def get_cluster_stats(
        self,
        coordinator_url: Optional[str] = None,
    ) -> ClusterTelemetry:
        """Retrieve aggregated cluster telemetry and stats from the coordinator."""
        base_url = self._resolve_coord_url(coordinator_url)
        url = f"{base_url}/api/v1/cluster/stats"
        resp = await self.http_client.get(url, timeout=self.default_timeout_seconds)
        resp.raise_for_status()
        return ClusterTelemetry.model_validate(resp.json())

    # ========================================================================
    # Data Plane Binary Transfers
    # ========================================================================

    async def download_asset(
        self,
        sha256: str,
        candidates: List[CandidateSource],
        cas_adapter: Optional[CASAdapter] = None,
        on_candidate_error: Optional[Callable[[CandidateSource, Exception], None]] = None,
    ) -> TransferResult:
        """Download asset from candidates with semaphore concurrency limiting and failover."""
        adapter = cas_adapter or self.cas_adapter
        if not adapter:
            raise ValueError("CASAdapter must be provided either at client init or transfer call")

        async with self.semaphore:
            return await transfer_asset_with_failover(
                client=self.http_client,
                candidates=candidates,
                sha256=sha256,
                cas_adapter=adapter,
                chunk_size=self.chunk_size,
                timeout_seconds=self.default_timeout_seconds,
                on_candidate_error=on_candidate_error,
            )

    async def download_missing_assets(
        self,
        locations: Dict[str, List[CandidateSource]],
        cas_adapter: Optional[CASAdapter] = None,
    ) -> Dict[str, TransferResult]:
        """Download multiple missing assets concurrently bounded by concurrency semaphore."""
        adapter = cas_adapter or self.cas_adapter
        if not adapter:
            raise ValueError("CASAdapter must be provided")

        tasks = {
            sha256: self.download_asset(sha256, cands, cas_adapter=adapter)
            for sha256, cands in locations.items()
        }

        results: Dict[str, TransferResult] = {}
        if not tasks:
            return results

        keys = list(tasks.keys())
        coros = [tasks[k] for k in keys]
        outcomes = await asyncio.gather(*coros, return_exceptions=True)

        for sha256, outcome in zip(keys, outcomes):
            if isinstance(outcome, TransferResult):
                results[sha256] = outcome
            elif isinstance(outcome, Exception):
                results[sha256] = TransferResult(
                    sha256=sha256,
                    success=False,
                    error_message=str(outcome),
                )
        return results

    async def sync_assets(
        self,
        required_hashes: Iterable[str],
        coordinator_url: Optional[str] = None,
        cas_adapter: Optional[CASAdapter] = None,
    ) -> Dict[str, TransferResult]:
        """End-to-end sync: identify local missing set -> locate on cluster -> stream download -> verify & commit."""
        adapter = cas_adapter or self.cas_adapter
        if not adapter:
            raise ValueError("CASAdapter must be provided")

        # 1. Missing-set calculation (zero network calls on full hit)
        missing = adapter.get_missing_hashes(required_hashes)
        results: Dict[str, TransferResult] = {}

        # Record local hits
        for h in required_hashes:
            try:
                norm_h = validate_sha256_hex(h)
            except ValueError:
                continue
            if norm_h not in missing:
                size = getattr(adapter, "get_asset_size", lambda x: 0)(norm_h) or 0
                path = getattr(adapter, "get_asset_path", lambda x: None)(norm_h)
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

        # 2. Query coordinator for locations
        locate_resp = await self.locate_assets(
            missing_hashes=list(missing),
            coordinator_url=coordinator_url,
        )

        # 3. Stream missing assets from candidates
        resolved_locations = {
            h: cands for h, cands in locate_resp.locations.items() if cands
        }
        if resolved_locations:
            transferred = await self.download_missing_assets(resolved_locations, cas_adapter=adapter)
            results.update(transferred)

        # 4. Mark unresolvable hashes as failed
        for unres_h in locate_resp.unresolved_hashes:
            if unres_h not in results:
                results[unres_h] = TransferResult(
                    sha256=unres_h,
                    success=False,
                    error_message=f"No candidate sources holding asset {unres_h}",
                )

        return results

    async def upload_asset(
        self,
        worker_endpoint_url: str,
        data_or_path: Union[bytes, Path, str],
        token: Optional[str] = None,
        timeout_seconds: float = 60.0,
    ) -> str:
        """Upload raw bytes or a local file into a worker's CAS via PUT /api/v1/assets/{sha256}."""
        if isinstance(data_or_path, (Path, str)):
            file_path = Path(data_or_path)
            payload = file_path.read_bytes()
        else:
            payload = bytes(data_or_path)

        sha256 = hashlib.sha256(payload).hexdigest().lower()
        clean_url = worker_endpoint_url.strip().rstrip("/")
        put_url = f"{clean_url}/api/v1/assets/{sha256}"

        headers: Dict[str, str] = {"Content-Type": "application/octet-stream"}
        auth_token = token if token is not None else os.environ.get("AIDAR_WORKER_BOOTSTRAP_SECRET", "").strip()
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        client = self._http_client or httpx.AsyncClient()
        close_after = self._http_client is None
        try:
            resp = await client.put(put_url, content=payload, headers=headers, timeout=timeout_seconds)
            resp.raise_for_status()
            return sha256
        finally:
            if close_after:
                await client.aclose()

    async def pull_job_artifacts(
        self,
        coordinator_url: str,
        job_id: str,
        output_dir: Union[Path, str],
        admin_token: Optional[str] = None,
        timeout_seconds: float = 60.0,
    ) -> Dict[str, Path]:
        """Locate and download all output_asset_hashes for a completed job into output_dir."""
        out_root = Path(output_dir)
        out_root.mkdir(parents=True, exist_ok=True)
        clean_coord = coordinator_url.strip().rstrip("/")

        headers: Dict[str, str] = {}
        tok = admin_token if admin_token is not None else os.environ.get("AIDAR_ADMIN_TOKENS", "").split(",")[0].strip()
        if tok:
            headers["Authorization"] = f"Bearer {tok}"

        client = self._http_client or httpx.AsyncClient()
        close_after = self._http_client is None
        saved_files: Dict[str, Path] = {}
        try:
            job_resp = await client.get(f"{clean_coord}/api/v1/jobs/{job_id}", headers=headers, timeout=timeout_seconds)
            job_resp.raise_for_status()
            job_data = job_resp.json()
            output_hashes: List[str] = list(job_data.get("output_asset_hashes", []))
            if not output_hashes:
                return saved_files

            loc_resp = await client.post(
                f"{clean_coord}/api/v1/assets/locate",
                json={"requester_worker_id": "client-pull", "missing_hashes": output_hashes},
                headers=headers,
                timeout=timeout_seconds,
            )
            loc_resp.raise_for_status()
            locations = loc_resp.json().get("locations", {})

            for h in output_hashes:
                candidates = locations.get(h, [])
                if not candidates:
                    continue
                for cand in candidates:
                    ep = cand["endpoint_url"].rstrip("/")
                    stream_url = f"{ep}/api/v1/assets/{h}/stream"
                    try:
                        dl_resp = await client.get(stream_url, timeout=timeout_seconds)
                        dl_resp.raise_for_status()
                        content = dl_resp.content
                        if hashlib.sha256(content).hexdigest().lower() == h.lower():
                            dest = out_root / h
                            dest.write_bytes(content)
                            saved_files[h] = dest
                            break
                    except Exception:
                        continue
            return saved_files
        finally:
            if close_after:
                await client.aclose()
