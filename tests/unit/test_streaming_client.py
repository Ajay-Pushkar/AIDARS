"""Unit tests for DistributedClient asynchronous transfer and synchronization client.

File: tests/unit/test_distributed/test_streaming_client.py
"""
from __future__ import annotations

import asyncio
import hashlib
import os
from pathlib import Path
from typing import Dict, List

import httpx
import pytest
from httpx import ASGITransport

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.client import DistributedClient
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    CandidateSource,
    ClusterTelemetry,
    HeartbeatPayload,
    LocalityTier,
    LocateAssetsResponse,
    TransferResult,
    WorkerRegistrationPayload,
)
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.server import create_worker_app


@pytest.fixture
def local_cas(tmp_path: Path) -> LocalCASAdapter:
    return LocalCASAdapter(cas_dir=tmp_path / "client_cas", staging_dir=tmp_path / "client_staging")


@pytest.fixture
def coordinator_service() -> CoordinatorService:
    registry = WorkerRegistry(heartbeat_timeout_seconds=30.0)
    return CoordinatorService(registry=registry)


class TestDistributedClientSync:
    """Test sync_assets, missing-set shortcutting, and coordinator integration."""

    @pytest.mark.asyncio
    async def test_sync_all_cached_requires_zero_network_calls(self, local_cas: LocalCASAdapter):
        data = b"Pre-cached asset"
        h = local_cas.store_bytes(data)

        async with DistributedClient(cas_adapter=local_cas) as client:
            results = await client.sync_assets([h])
            assert len(results) == 1
            assert results[h].success is True
            assert results[h].bytes_transferred == 0
            assert results[h].source_worker_id == "local_cas"

    @pytest.mark.asyncio
    async def test_download_missing_assets_with_semaphore_limit(self, local_cas: LocalCASAdapter):
        assets = {
            hashlib.sha256(f"asset-{i}".encode()).hexdigest(): f"asset-{i}".encode()
            for i in range(10)
        }

        current_active_streams = 0
        max_seen_concurrent = 0
        lock = asyncio.Lock()

        async def stream_app(scope, receive, send):
            nonlocal current_active_streams, max_seen_concurrent
            async with lock:
                current_active_streams += 1
                if current_active_streams > max_seen_concurrent:
                    max_seen_concurrent = current_active_streams

            path = scope.get("path", "")
            parts = path.strip("/").split("/")
            # path is /api/v1/assets/{sha256}/stream -> parts: ["api", "v1", "assets", sha256, "stream"]
            sha256 = parts[3]
            payload = assets.get(sha256, b"")

            await asyncio.sleep(0.01)

            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/octet-stream"),
                    (b"content-length", str(len(payload)).encode()),
                    (b"x-asset-sha256", sha256.encode()),
                ],
            })
            await send({"type": "http.response.body", "body": payload, "more_body": False})

            async with lock:
                current_active_streams -= 1

        transport = ASGITransport(app=stream_app)
        mock_http = httpx.AsyncClient(transport=transport, base_url="http://mockworker")

        async with DistributedClient(
            cas_adapter=local_cas,
            http_client=mock_http,
            max_concurrent_transfers=3,
        ) as client:
            locations = {
                sha256: [
                    CandidateSource(
                        worker_id="w-01",
                        endpoint_url="http://mockworker",
                        ip_address="127.0.0.1",
                        port=8000,
                    )
                ]
                for sha256 in assets
            }

            results = await client.download_missing_assets(locations)

            assert len(results) == 10
            for h in assets:
                assert results[h].success is True
                assert local_cas.has_asset(h) is True

            assert max_seen_concurrent <= 3

    @pytest.mark.asyncio
    async def test_sync_assets_with_coordinator_and_workers(
        self, tmp_path: Path, local_cas: LocalCASAdapter, coordinator_service: CoordinatorService
    ):
        # 1. Peer worker with CAS containing asset A
        peer_cas = LocalCASAdapter(cas_dir=tmp_path / "peer_cas", staging_dir=tmp_path / "peer_staging")
        data_a = b"Cluster Asset A Content"
        hash_a = peer_cas.store_bytes(data_a)

        # Create worker app
        worker_app = create_worker_app(cas_adapter=peer_cas, worker_id="w-peer", endpoint_url="http://peer-worker")

        # Register peer in coordinator
        reg_payload = WorkerRegistrationPayload(
            worker_id="w-peer",
            endpoint_url="http://peer-worker",
            ip_address="127.0.0.1",
            port=8001,
            inventory_hashes=[hash_a],
        )
        coordinator_service.register_worker_sync(reg_payload)

        # Composite ASGI app routing coordinator and peer worker
        async def cluster_app(scope, receive, send):
            headers = dict(scope.get("headers", []))
            host = headers.get(b"host", b"").decode()
            if "coordinator" in host:
                await coordinator_service.app(scope, receive, send)
            elif "peer-worker" in host:
                await worker_app(scope, receive, send)
            else:
                await coordinator_service.app(scope, receive, send)

        transport = ASGITransport(app=cluster_app)
        mock_http = httpx.AsyncClient(transport=transport)

        # 2. Client CAS contains asset B locally
        data_b = b"Pre-cached Asset B"
        hash_b = local_cas.store_bytes(data_b)

        # Unresolvable hash C
        hash_c = hashlib.sha256(b"Missing Unresolvable Asset C").hexdigest()

        async with DistributedClient(
            cas_adapter=local_cas,
            coordinator_url="http://coordinator",
            http_client=mock_http,
        ) as client:
            required = [hash_a, hash_b, hash_c]
            results = await client.sync_assets(required)

            assert len(results) == 3

            # Hash B was local hit
            assert results[hash_b].success is True
            assert results[hash_b].source_worker_id == "local_cas"

            # Hash A was transferred from peer worker
            assert results[hash_a].success is True
            assert results[hash_a].source_worker_id == "w-peer"
            assert local_cas.has_asset(hash_a) is True

            # Hash C was unresolvable
            assert results[hash_c].success is False
            assert "No candidate sources" in (results[hash_c].error_message or "")


class TestDistributedClientControlPlaneRPCs:
    """Test control plane RPC interactions (register, heartbeat, ping, stats)."""

    @pytest.mark.asyncio
    async def test_control_plane_rpcs(self, coordinator_service: CoordinatorService):
        transport = ASGITransport(app=coordinator_service.app)
        mock_http = httpx.AsyncClient(transport=transport, base_url="http://coordinator")

        async with DistributedClient(
            coordinator_url="http://coordinator",
            http_client=mock_http,
        ) as client:
            # 1. Register worker
            reg_resp = await client.register_worker(
                WorkerRegistrationPayload(
                    worker_id="w-rpc-01",
                    endpoint_url="http://127.0.0.1:8080",
                    ip_address="127.0.0.1",
                    port=8080,
                    inventory_hashes=["a" * 64],
                )
            )
            assert reg_resp.status == "registered"
            assert reg_resp.worker_id == "w-rpc-01"

            # 2. Send heartbeat
            hb_resp = await client.send_heartbeat(
                worker_id="w-rpc-01",
                payload=HeartbeatPayload(worker_id="w-rpc-01", active_transfers=2),
            )
            assert hb_resp.status == "healthy"
            assert hb_resp.re_register_required is False

            # 3. Locate assets
            loc_resp = await client.locate_assets(missing_hashes=["a" * 64])
            assert "a" * 64 in loc_resp.locations
            assert len(loc_resp.locations["a" * 64]) == 1

            # 4. Get cluster stats
            stats = await client.get_cluster_stats()
            assert stats.active_workers >= 1

            # 5. Unregister
            unreg = await client.unregister_worker("w-rpc-01")
            assert unreg["status"] == "unregistered"
