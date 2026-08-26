"""AIDAR Distributed Worker Server & Binary Streaming Data Plane.

Provides FastAPI/Starlette routes for binary streaming (GET /api/v1/assets/{sha256}/stream),
supporting 1 MiB chunked transfers, HTTP Range resumption (206 Partial Content),
and leak-free resource cleanup on client aborts.
"""
from __future__ import annotations

import io
import logging
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, FastAPI, Header, HTTPException, Response, status
from fastapi.responses import StreamingResponse

from aidars.distributed.cas_adapter import CASAdapter
from aidars.distributed.models import (
    PingRequest,
    PongResponse,
    WorkerCapabilities,
    WorkerMetrics,
    validate_sha256_hex,
)
from aidars.distributed.transfer import (
    DEFAULT_CHUNK_SIZE,
    generate_bounded_chunks,
    parse_byte_range_header,
)

logger = logging.getLogger(__name__)


class WorkerServer:
    """Worker node server hosting data plane streaming and local control plane endpoints."""

    def __init__(
        self,
        cas_adapter: CASAdapter,
        worker_id: Optional[str] = None,
        host: str = "0.0.0.0",
        port: int = 8000,
        endpoint_url: Optional[str] = None,
        capabilities: Optional[WorkerCapabilities] = None,
        metrics: Optional[WorkerMetrics] = None,
    ) -> None:
        self.cas = cas_adapter
        self.worker_id = worker_id or f"worker-{int(time.time())}"
        self.host = host
        self.port = port
        self.endpoint_url = endpoint_url or f"http://{host}:{port}"
        self.capabilities = capabilities or WorkerCapabilities()
        self.metrics = metrics or WorkerMetrics()
        self._start_time_utc = time.time()
        self.app: FastAPI = self._create_fastapi_app()

    def _create_fastapi_app(self) -> FastAPI:
        app = FastAPI(
            title=f"AIDAR Worker Data Plane ({self.worker_id})",
            description="High-throughput binary streaming server and worker REST APIs.",
            version="1.0.0",
        )

        router = APIRouter(prefix="/api/v1")

        # ====================================================================
        # Data Plane: Binary Streaming Endpoint
        # ====================================================================
        @router.get(
            "/assets/{sha256}/stream",
            summary="Stream binary asset in 1 MiB chunks with HTTP Range support",
            response_class=StreamingResponse,
        )
        async def stream_asset(
            sha256: str,
            range: Optional[str] = Header(default=None, alias="Range"),
        ) -> Response:
            # 1. Validate & sanitize SHA-256 hash parameter
            try:
                norm_sha256 = validate_sha256_hex(sha256)
            except (ValueError, TypeError) as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid SHA-256 hash format: {exc}",
                )

            # 2. Check existence in local CAS
            if not self.cas.has_asset(norm_sha256):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Asset {norm_sha256} not found in local CAS.",
                )

            # 3. Determine total asset size
            total_size: Optional[int] = None
            if hasattr(self.cas, "get_asset_size"):
                total_size = self.cas.get_asset_size(norm_sha256)

            if total_size is None:
                try:
                    probe_stream = self.cas.open_asset_stream(norm_sha256, offset=0)
                    try:
                        probe_stream.seek(0, io.SEEK_END)
                        total_size = probe_stream.tell()
                    finally:
                        probe_stream.close()
                except Exception as exc:
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"Failed to determine size of asset {norm_sha256}: {exc}",
                    )

            # 4. Parse Range header
            try:
                start, end, content_length, is_partial = parse_byte_range_header(
                    range_header=range,
                    total_size=total_size,
                )
            except IndexError:
                # HTTP 416 Range Not Satisfiable
                return Response(
                    status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
                    headers={
                        "Content-Range": f"bytes */{total_size}",
                        "Accept-Ranges": "bytes",
                    },
                )
            except ValueError:
                # Gracefully fallback to full content stream on malformed range syntax
                start = 0
                end = max(0, total_size - 1) if total_size > 0 else 0
                content_length = total_size
                is_partial = False

            # 5. Open stream at start offset
            try:
                stream = self.cas.open_asset_stream(norm_sha256, offset=start)
            except (FileNotFoundError, IndexError, ValueError) as exc:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Unable to open stream for asset {norm_sha256}: {exc}",
                )

            # 6. Prepare response headers
            resp_headers: Dict[str, str] = {
                "Content-Type": "application/octet-stream",
                "Content-Length": str(content_length),
                "Accept-Ranges": "bytes",
                "X-Asset-SHA256": norm_sha256,
            }

            status_code = status.HTTP_200_OK
            if is_partial:
                status_code = status.HTTP_206_PARTIAL_CONTENT
                resp_headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"

            # 7. Track metrics and return StreamingResponse
            self.metrics.active_transfers += 1
            self.metrics.active_uploads += 1

            chunk_size = self.capabilities.chunk_size_bytes or DEFAULT_CHUNK_SIZE

            def tracking_generator():
                try:
                    yield from generate_bounded_chunks(
                        stream=stream,
                        bytes_to_send=content_length,
                        chunk_size=chunk_size,
                    )
                finally:
                    self.metrics.active_transfers = max(0, self.metrics.active_transfers - 1)
                    self.metrics.active_uploads = max(0, self.metrics.active_uploads - 1)
                    self.metrics.total_bytes_sent += content_length

            return StreamingResponse(
                content=tracking_generator(),
                status_code=status_code,
                headers=resp_headers,
                media_type="application/octet-stream",
            )

        # ====================================================================
        # Diagnostic & Control Plane Endpoints
        # ====================================================================
        @router.get("/assets/{sha256}/exists", summary="Check if asset is cached")
        async def asset_exists(sha256: str) -> Dict[str, Any]:
            try:
                norm_h = validate_sha256_hex(sha256)
            except ValueError:
                return {"sha256": sha256, "exists": False, "size_bytes": None}
            exists = self.cas.has_asset(norm_h)
            size = getattr(self.cas, "get_asset_size", lambda h: None)(norm_h) if exists else None
            return {"sha256": norm_h, "exists": exists, "size_bytes": size}

        @router.get("/inventory", summary="List cached asset hashes")
        async def get_inventory() -> Dict[str, Any]:
            hashes = (
                list(self.cas.get_inventory_hashes())
                if hasattr(self.cas, "get_inventory_hashes")
                else []
            )
            return {"worker_id": self.worker_id, "count": len(hashes), "inventory": sorted(hashes)}

        @router.get("/worker/info", summary="Retrieve worker capabilities and telemetry")
        async def get_worker_info() -> Dict[str, Any]:
            return {
                "worker_id": self.worker_id,
                "endpoint_url": self.endpoint_url,
                "capabilities": self.capabilities.model_dump(),
                "metrics": self.metrics.model_dump(),
                "uptime_seconds": round(time.time() - self._start_time_utc, 2),
            }

        # Latency probe / ping endpoints
        @router.post("/ping", response_model=PongResponse, summary="Latency probe POST")
        async def ping_post(payload: PingRequest) -> PongResponse:
            return PongResponse(
                worker_id=self.worker_id,
                client_timestamp_utc=payload.client_timestamp_utc,
                server_timestamp_utc=time.time(),
                status="pong",
                sequence_number=payload.sequence_number,
            )

        @router.get("/ping", response_model=PongResponse, summary="Latency probe GET")
        async def ping_get() -> PongResponse:
            now = time.time()
            return PongResponse(
                worker_id=self.worker_id,
                client_timestamp_utc=now,
                server_timestamp_utc=now,
                status="pong",
            )

        app.include_router(router)
        return app


def create_worker_app(
    cas_adapter: CASAdapter,
    worker_id: Optional[str] = None,
    host: str = "0.0.0.0",
    port: int = 8000,
    endpoint_url: Optional[str] = None,
    capabilities: Optional[WorkerCapabilities] = None,
    metrics: Optional[WorkerMetrics] = None,
) -> FastAPI:
    """Factory creating a FastAPI app instance for a WorkerServer."""
    server = WorkerServer(
        cas_adapter=cas_adapter,
        worker_id=worker_id,
        host=host,
        port=port,
        endpoint_url=endpoint_url,
        capabilities=capabilities,
        metrics=metrics,
    )
    return server.app
