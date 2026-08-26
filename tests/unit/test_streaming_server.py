"""Unit tests for server-side binary streaming data plane and HTTP Range compliance.

File: tests/unit/test_distributed/test_streaming_server.py
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Dict, Tuple

import pytest
from fastapi.testclient import TestClient

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.models import PingRequest, WorkerCapabilities
from aidars.distributed.server import WorkerServer, create_worker_app


@pytest.fixture
def populated_cas(tmp_path: Path) -> Tuple[LocalCASAdapter, Dict[str, bytes]]:
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas", staging_dir=tmp_path / "staging")
    catalog = {}

    catalog["zero"] = b""
    catalog["100b"] = os.urandom(100)
    catalog["1mib"] = os.urandom(1024 * 1024)
    catalog["3mib"] = os.urandom(3 * 1024 * 1024)

    for key, data in catalog.items():
        cas.store_bytes(data)

    return cas, catalog


class TestStreamingServerEndpoint:
    """Verify GET /api/v1/assets/{sha256}/stream endpoint contract and headers."""

    def test_stream_full_asset_200(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, catalog = populated_cas
        data = catalog["1mib"]
        h = hashlib.sha256(data).hexdigest()

        app = create_worker_app(cas_adapter=cas)
        client = TestClient(app)

        resp = client.get(f"/api/v1/assets/{h}/stream")
        assert resp.status_code == 200
        assert resp.headers["content-type"] == "application/octet-stream"
        assert resp.headers["content-length"] == str(len(data))
        assert resp.headers["x-asset-sha256"] == h
        assert resp.headers.get("accept-ranges") == "bytes"
        assert resp.content == data

    def test_stream_zero_byte_asset(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, catalog = populated_cas
        data = catalog["zero"]
        h = hashlib.sha256(data).hexdigest()

        app = create_worker_app(cas_adapter=cas)
        client = TestClient(app)

        resp = client.get(f"/api/v1/assets/{h}/stream")
        assert resp.status_code == 200
        assert resp.headers["content-length"] == "0"
        assert resp.content == b""

    def test_stream_missing_asset_returns_404(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, _ = populated_cas
        fake_hash = hashlib.sha256(b"does-not-exist").hexdigest()

        app = create_worker_app(cas_adapter=cas)
        client = TestClient(app)

        resp = client.get(f"/api/v1/assets/{fake_hash}/stream")
        assert resp.status_code == 404

    def test_stream_invalid_hash_format_returns_400(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, _ = populated_cas
        app = create_worker_app(cas_adapter=cas)
        client = TestClient(app)

        # Path traversal / invalid character
        resp = client.get("/api/v1/assets/../../etc/passwd/stream")
        assert resp.status_code in (400, 404)

        # Non-64 hex string
        resp2 = client.get("/api/v1/assets/invalid-hex-hash/stream")
        assert resp2.status_code == 400


class TestHTTPRangeHeaderCompliance:
    """Verify HTTP Range header compliance: bytes=0-, bytes=offset-, out of bounds, invalid."""

    def test_range_bytes_zero_prefix(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, catalog = populated_cas
        data = catalog["3mib"]
        h = hashlib.sha256(data).hexdigest()

        app = create_worker_app(cas_adapter=cas)
        client = TestClient(app)

        resp = client.get(f"/api/v1/assets/{h}/stream", headers={"Range": "bytes=0-"})
        assert resp.status_code in (200, 206)
        assert resp.content == data
        if resp.status_code == 206:
            assert resp.headers["content-range"] == f"bytes 0-{len(data)-1}/{len(data)}"

    def test_range_bytes_partial_offset(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, catalog = populated_cas
        data = catalog["3mib"]
        h = hashlib.sha256(data).hexdigest()
        offset = 1048576  # start at 1 MiB offset

        app = create_worker_app(cas_adapter=cas)
        client = TestClient(app)

        resp = client.get(f"/api/v1/assets/{h}/stream", headers={"Range": f"bytes={offset}-"})
        assert resp.status_code == 206
        assert resp.headers["content-range"] == f"bytes {offset}-{len(data)-1}/{len(data)}"
        assert resp.headers["content-length"] == str(len(data) - offset)
        assert resp.content == data[offset:]

    def test_range_bytes_start_and_end(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, catalog = populated_cas
        data = catalog["3mib"]
        h = hashlib.sha256(data).hexdigest()
        start = 100
        end = 199

        app = create_worker_app(cas_adapter=cas)
        client = TestClient(app)

        resp = client.get(f"/api/v1/assets/{h}/stream", headers={"Range": f"bytes={start}-{end}"})
        assert resp.status_code == 206
        assert resp.headers["content-range"] == f"bytes {start}-{end}/{len(data)}"
        assert resp.headers["content-length"] == str(end - start + 1)
        assert resp.content == data[start : end + 1]

    def test_range_out_of_bounds_offset_returns_416(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, catalog = populated_cas
        data = catalog["1mib"]
        h = hashlib.sha256(data).hexdigest()
        excess_offset = len(data) + 1000

        app = create_worker_app(cas_adapter=cas)
        client = TestClient(app)

        resp = client.get(f"/api/v1/assets/{h}/stream", headers={"Range": f"bytes={excess_offset}-"})
        assert resp.status_code == 416  # Range Not Satisfiable
        assert "content-range" in resp.headers
        assert resp.headers["content-range"] == f"bytes */{len(data)}"

    def test_malformed_range_header_falls_back_to_full_stream(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, catalog = populated_cas
        data = catalog["100b"]
        h = hashlib.sha256(data).hexdigest()

        app = create_worker_app(cas_adapter=cas)
        client = TestClient(app)

        resp = client.get(f"/api/v1/assets/{h}/stream", headers={"Range": "invalid_range_format"})
        assert resp.status_code == 200
        assert resp.content == data


class TestWorkerServerControlPlane:
    """Test worker discovery, inventory, info, and ping routes."""

    def test_asset_exists_endpoint(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, catalog = populated_cas
        data = catalog["100b"]
        h = hashlib.sha256(data).hexdigest()
        missing_h = hashlib.sha256(b"missing").hexdigest()

        app = create_worker_app(cas_adapter=cas)
        client = TestClient(app)

        resp1 = client.get(f"/api/v1/assets/{h}/exists")
        assert resp1.status_code == 200
        assert resp1.json()["exists"] is True
        assert resp1.json()["size_bytes"] == 100

        resp2 = client.get(f"/api/v1/assets/{missing_h}/exists")
        assert resp2.status_code == 200
        assert resp2.json()["exists"] is False

    def test_inventory_endpoint(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, catalog = populated_cas
        app = create_worker_app(cas_adapter=cas, worker_id="w-test-inv")
        client = TestClient(app)

        resp = client.get("/api/v1/inventory")
        assert resp.status_code == 200
        data = resp.json()
        assert data["worker_id"] == "w-test-inv"
        assert data["count"] == len(catalog)
        assert len(data["inventory"]) == len(catalog)

    def test_worker_info_endpoint(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, _ = populated_cas
        app = create_worker_app(
            cas_adapter=cas,
            worker_id="w-test-info",
            endpoint_url="http://192.168.1.50:8000",
            capabilities=WorkerCapabilities(max_concurrent_streams=8),
        )
        client = TestClient(app)

        resp = client.get("/api/v1/worker/info")
        assert resp.status_code == 200
        data = resp.json()
        assert data["worker_id"] == "w-test-info"
        assert data["endpoint_url"] == "http://192.168.1.50:8000"
        assert data["capabilities"]["max_concurrent_streams"] == 8

    def test_ping_endpoints(self, populated_cas: Tuple[LocalCASAdapter, Dict[str, bytes]]):
        cas, _ = populated_cas
        app = create_worker_app(cas_adapter=cas, worker_id="w-ping")
        client = TestClient(app)

        # GET ping
        resp_get = client.get("/api/v1/ping")
        assert resp_get.status_code == 200
        assert resp_get.json()["status"] == "pong"
        assert resp_get.json()["worker_id"] == "w-ping"

        # POST ping
        req = PingRequest(client_worker_id="client-01", sequence_number=42)
        resp_post = client.post("/api/v1/ping", json=req.model_dump())
        assert resp_post.status_code == 200
        assert resp_post.json()["sequence_number"] == 42
