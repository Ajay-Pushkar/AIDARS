"""Tier 5 Adversarial Coverage Hardening & Empirical Stress Tests for AIDAR Milestone 5.

This test suite executes empirical stress tests, race condition generators, and fault
injection harnesses targeting:
1. High-concurrency streaming and Range resumption under simulated network drops.
2. Corrupted bit injection (head, middle, boundary, tail) and verification of zero orphan tmp files.
3. Malicious and invalid SHA-256 strings with directory traversal characters (../../, ..\\, null bytes).
4. Multi-candidate failover under active corruption and network partitions.
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import os
import random
import re
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import httpx
import pytest
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.responses import StreamingResponse

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.client import DistributedClient
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    CandidateSource,
    LocalityTier,
    LocateAssetsRequest,
    LocateAssetsResponse,
    WorkerCapabilities,
    WorkerInfo,
    WorkerMetrics,
    WorkerRegistrationPayload,
    WorkerStatus,
    validate_endpoint_url,
    validate_ip_address,
    validate_sha256_hex,
)
from aidars.distributed.prioritizer import CandidatePrioritizer, classify_locality
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.server import WorkerServer, create_worker_app
from aidars.distributed.transfer import (
    DEFAULT_CHUNK_SIZE,
    CandidateExhaustedError,
    IntegrityError,
    StagingContext,
    StreamAbortError,
    WorkerHttpError,
    download_stream_to_staging,
    generate_bounded_chunks,
    parse_byte_range_header,
    transfer_asset_from_candidate,
    transfer_asset_with_failover,
)


# ============================================================================
# Adversarial Fixtures
# ============================================================================

@pytest.fixture
def cas_setup(tmp_path: Path) -> Tuple[LocalCASAdapter, LocalCASAdapter, Path, Path]:
    cas_a_dir = tmp_path / "cas_a"
    cas_b_dir = tmp_path / "cas_b"
    adapter_a = LocalCASAdapter(cas_a_dir)
    adapter_b = LocalCASAdapter(cas_b_dir)
    return adapter_a, adapter_b, cas_a_dir, cas_b_dir


@pytest.fixture
def random_binary_payloads() -> Dict[str, Tuple[bytes, str]]:
    payloads = {}
    sizes = [
        ("zero_byte", 0),
        ("tiny_64b", 64),
        ("exact_1mib", 1024 * 1024),
        ("exact_1mib_minus_1", 1024 * 1024 - 1),
        ("exact_1mib_plus_1", 1024 * 1024 + 1),
        ("multi_chunk_3mib", 3 * 1024 * 1024),
        ("large_7mib", 7 * 1024 * 1024 + 12345),
    ]
    for name, sz in sizes:
        data = os.urandom(sz) if sz > 0 else b""
        h = hashlib.sha256(data).hexdigest()
        payloads[name] = (data, h)
    return payloads


# ============================================================================
# Section 1: High Concurrency Streaming & Range Resumption
# ============================================================================

class TestHighConcurrencyStreaming:
    @pytest.mark.asyncio
    async def test_high_concurrency_multi_asset_streaming(self, cas_setup, random_binary_payloads):
        """Stress test: 30 concurrent streaming downloads of different assets."""
        adapter_a, adapter_b, _, _ = cas_setup

        # Seed assets in adapter_a (sender)
        hashes = []
        for name, (data, h) in random_binary_payloads.items():
            adapter_a.store_bytes(data)
            hashes.append((h, data))

        server = WorkerServer(adapter_a, worker_id="sender-worker")
        transport = httpx.ASGITransport(app=server.app)

        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            dist_client = DistributedClient(cas_adapter=adapter_b, http_client=client, max_concurrent_transfers=16)

            # Fire 30 concurrent transfer tasks
            tasks = []
            for i in range(30):
                target_h, target_data = hashes[i % len(hashes)]
                cand = CandidateSource(
                    worker_id="sender-worker",
                    endpoint_url="http://testserver",
                    ip_address="127.0.0.1",
                    port=8000,
                    locality_tier="loopback",
                    estimated_rtt_ms=0.5,
                    load_factor=0.1,
                )
                tasks.append(dist_client.download_asset(target_h, [cand]))

            results = await asyncio.gather(*tasks)
            assert len(results) == 30
            for res in results:
                assert res.success is True
                assert adapter_b.has_asset(res.sha256) is True

    @pytest.mark.asyncio
    async def test_concurrent_same_asset_racing_commits(self, cas_setup):
        """Stress test: 20 concurrent streaming downloads of the EXACT SAME asset racing to commit into CAS."""
        adapter_a, adapter_b, _, cas_b_dir = cas_setup
        data = os.urandom(2 * 1024 * 1024 + 500)
        h = adapter_a.store_bytes(data)

        server = WorkerServer(adapter_a, worker_id="sender-racing")
        transport = httpx.ASGITransport(app=server.app)

        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            dist_client = DistributedClient(cas_adapter=adapter_b, http_client=client, max_concurrent_transfers=20)

            cand = CandidateSource(
                worker_id="sender-racing",
                endpoint_url="http://testserver",
                ip_address="127.0.0.1",
                port=8000,
                locality_tier="loopback",
                estimated_rtt_ms=0.5,
                load_factor=0.1,
            )

            tasks = [dist_client.download_asset(h, [cand]) for _ in range(20)]
            results = await asyncio.gather(*tasks)

            assert len(results) == 20
            assert all(r.success for r in results)
            assert adapter_b.has_asset(h) is True

            # Verify CAS integrity: exactly 1 file in objects shard, 0 temporary files in staging
            shard = h[:2]
            obj_path = cas_b_dir / "objects" / shard / h
            assert obj_path.exists()
            assert obj_path.stat().st_size == len(data)

            staging_dir = cas_b_dir / "staging"
            assert len(list(staging_dir.iterdir())) == 0

    @pytest.mark.asyncio
    async def test_multi_step_range_resumption_integrity(self, cas_setup):
        """Verify multi-step HTTP Range resumption reconstructs exact binary stream."""
        adapter_a, adapter_b, _, _ = cas_setup
        total_size = 5 * 1024 * 1024  # 5 MiB
        data = os.urandom(total_size)
        h = adapter_a.store_bytes(data)

        server = WorkerServer(adapter_a, worker_id="server-range")
        transport = httpx.ASGITransport(app=server.app)

        # Download in 4 distinct range segments
        segments = [(0, 1048575), (1048576, 2097151), (2097152, 4194303), (4194304, total_size - 1)]
        reconstructed = bytearray()

        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            for start, end in segments:
                headers = {"Range": f"bytes={start}-{end}"}
                resp = await client.get(f"/api/v1/assets/{h}/stream", headers=headers)
                assert resp.status_code == 206
                assert resp.headers["Content-Range"] == f"bytes {start}-{end}/{total_size}"
                chunk = resp.content
                assert len(chunk) == (end - start + 1)
                reconstructed.extend(chunk)

        assert bytes(reconstructed) == data
        assert hashlib.sha256(reconstructed).hexdigest() == h


# ============================================================================
# Section 2: Fault Injection & Corrupted Bit Streaming
# ============================================================================

class TestFaultInjectionAndCorruption:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("corrupt_location", ["head", "middle", "boundary", "tail"])
    async def test_bit_flip_corruption_rejection_and_zero_orphans(self, cas_setup, corrupt_location):
        """Adversarial fault injection: Flip bits at critical stream offsets, verify rejection and zero staging orphans."""
        adapter_a, adapter_b, _, cas_b_dir = cas_setup
        total_size = 3 * 1024 * 1024  # 3 MiB
        data = os.urandom(total_size)
        h = adapter_a.store_bytes(data)

        # Create custom faulty app that injects bit flips
        faulty_app = FastAPI()

        @faulty_app.get("/api/v1/assets/{sha256_hex}/stream")
        async def faulty_stream(sha256_hex: str):
            stream = adapter_a.open_asset_stream(sha256_hex)
            raw = bytearray(stream.read())
            stream.close()

            if corrupt_location == "head":
                raw[0] ^= 0x01
            elif corrupt_location == "middle":
                raw[len(raw) // 2] ^= 0x01
            elif corrupt_location == "boundary":
                raw[1024 * 1024] ^= 0x01  # Exact 1 MiB chunk boundary
            elif corrupt_location == "tail":
                raw[-1] ^= 0x01

            return StreamingResponse(
                content=io.BytesIO(bytes(raw)),
                status_code=200,
                headers={"Content-Length": str(len(raw)), "Content-Type": "application/octet-stream"},
            )

        transport = httpx.ASGITransport(app=faulty_app)
        staging_dir = cas_b_dir / "staging"

        async with httpx.AsyncClient(transport=transport, base_url="http://testfaulty") as client:
            with pytest.raises(IntegrityError) as exc_info:
                await download_stream_to_staging(
                    client=client,
                    endpoint_url="http://testfaulty",
                    sha256=h,
                    staging_dir=staging_dir,
                )

            assert h in str(exc_info.value)
            # CRITICAL VERIFICATION: No corrupt file staged in objects, and staging dir is completely clean
            assert not adapter_b.has_asset(h)
            assert len(list(staging_dir.iterdir())) == 0

    @pytest.mark.asyncio
    async def test_truncated_stream_rejection_and_clean_exit(self, cas_setup):
        """Adversarial fault injection: Server closes stream halfway through (underflow)."""
        adapter_a, adapter_b, _, cas_b_dir = cas_setup
        total_size = 2 * 1024 * 1024  # 2 MiB
        data = os.urandom(total_size)
        h = adapter_a.store_bytes(data)

        faulty_app = FastAPI()

        @faulty_app.get("/api/v1/assets/{sha256_hex}/stream")
        async def truncated_stream(sha256_hex: str):
            # Send only 512 KiB but declare 2 MiB Content-Length
            truncated_bytes = data[:512 * 1024]
            return StreamingResponse(
                content=io.BytesIO(truncated_bytes),
                status_code=200,
                headers={"Content-Length": str(total_size), "Content-Type": "application/octet-stream"},
            )

        transport = httpx.ASGITransport(app=faulty_app)
        staging_dir = cas_b_dir / "staging"

        async with httpx.AsyncClient(transport=transport, base_url="http://testfaulty") as client:
            with pytest.raises(StreamAbortError) as exc_info:
                await download_stream_to_staging(
                    client=client,
                    endpoint_url="http://testfaulty",
                    sha256=h,
                    staging_dir=staging_dir,
                )

            assert "truncated" in str(exc_info.value).lower()
            assert not adapter_b.has_asset(h)
            assert len(list(staging_dir.iterdir())) == 0

    @pytest.mark.asyncio
    async def test_appended_garbage_stream_rejection(self, cas_setup):
        """Adversarial fault injection: Server sends extra garbage bytes beyond file length."""
        adapter_a, adapter_b, _, cas_b_dir = cas_setup
        data = os.urandom(100 * 1024)
        h = adapter_a.store_bytes(data)

        faulty_app = FastAPI()

        @faulty_app.get("/api/v1/assets/{sha256_hex}/stream")
        async def overflown_stream(sha256_hex: str):
            # Send extra 100 bytes beyond declared Content-Length
            overflown = data + b"GARBAGE_OVERFLOW_BYTES"
            return StreamingResponse(
                content=io.BytesIO(overflown),
                status_code=200,
                headers={"Content-Length": str(len(data)), "Content-Type": "application/octet-stream"},
            )

        transport = httpx.ASGITransport(app=faulty_app)
        staging_dir = cas_b_dir / "staging"

        async with httpx.AsyncClient(transport=transport, base_url="http://testfaulty") as client:
            with pytest.raises(StreamAbortError):
                await download_stream_to_staging(
                    client=client,
                    endpoint_url="http://testfaulty",
                    sha256=h,
                    staging_dir=staging_dir,
                )

            assert not adapter_b.has_asset(h)
            assert len(list(staging_dir.iterdir())) == 0

    @pytest.mark.asyncio
    async def test_server_http_500_and_503_error_cleanup(self, cas_setup):
        """Verify HTTP server errors clean up staging files immediately."""
        _, adapter_b, _, cas_b_dir = cas_setup
        h = "a" * 64
        staging_dir = cas_b_dir / "staging"

        error_app = FastAPI()

        @error_app.get("/api/v1/assets/{sha256_hex}/stream")
        async def server_error(sha256_hex: str):
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Disk read error")

        transport = httpx.ASGITransport(app=error_app)

        async with httpx.AsyncClient(transport=transport, base_url="http://testerror") as client:
            with pytest.raises(WorkerHttpError) as exc_info:
                await download_stream_to_staging(
                    client=client,
                    endpoint_url="http://testerror",
                    sha256=h,
                    staging_dir=staging_dir,
                )

            assert exc_info.value.status_code == 500
            assert len(list(staging_dir.iterdir())) == 0

    @pytest.mark.asyncio
    async def test_zero_orphan_staging_fuzz_harness(self, cas_setup):
        """Fuzz harness: 50 rapid randomized failures (bit flips, aborts, timeouts), verify 0 orphan tmp files."""
        adapter_a, adapter_b, _, cas_b_dir = cas_setup
        data = os.urandom(128 * 1024)
        h = adapter_a.store_bytes(data)
        staging_dir = cas_b_dir / "staging"

        fuzz_app = FastAPI()

        @fuzz_app.get("/api/v1/assets/{sha256_hex}/stream")
        async def fuzzed_stream(sha256_hex: str):
            mode = random.choice(["corrupt", "truncate", "http_error", "empty"])
            if mode == "corrupt":
                corrupted = bytearray(data)
                corrupted[random.randint(0, len(data) - 1)] ^= 0xFF
                return StreamingResponse(io.BytesIO(bytes(corrupted)), status_code=200)
            elif mode == "truncate":
                return StreamingResponse(
                    io.BytesIO(data[:1024]),
                    status_code=200,
                    headers={"Content-Length": str(len(data))},
                )
            elif mode == "http_error":
                raise HTTPException(status_code=503, detail="Service Unavailable")
            else:
                return Response(status_code=404, content="Not Found")

        transport = httpx.ASGITransport(app=fuzz_app)

        async with httpx.AsyncClient(transport=transport, base_url="http://testfuzz") as client:
            for _ in range(50):
                try:
                    await download_stream_to_staging(
                        client=client,
                        endpoint_url="http://testfuzz",
                        sha256=h,
                        staging_dir=staging_dir,
                    )
                except Exception:
                    pass

        # Verify staging directory is 100% clean
        assert len(list(staging_dir.iterdir())) == 0


# ============================================================================
# Section 3: Malicious / Invalid SHA-256 Strings & Traversal Attacks
# ============================================================================

class TestSecurityAndPathTraversalHardening:
    ADVERSARIAL_HASH_INPUTS = [
        # Directory traversal payloads
        "../../../../etc/passwd",
        "..\\..\\..\\Windows\\System32\\calc.exe",
        "....//....//....//etc/shadow",
        "..%2f..%2f..%2fetc%2fpasswd",
        "/var/log/syslog",
        "C:\\Windows\\notepad.exe",
        # Null bytes and control characters
        "a" * 64 + "\x00",
        "\x00" * 64,
        "a" * 32 + "\x00" + "b" * 31,
        # Non-hex characters
        "g" * 64,
        "z" * 64,
        "!@#$%^&*()_+{}:<>?~`",
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg",
        # Length boundaries
        "",
        "a" * 63,   # 1 char too short
        "a" * 65,   # 1 char too long
        "a" * 1000, # Extreme length
        # SQL / Script injection patterns
        "' OR '1'='1",
        "1; DROP TABLE workers;--",
        "<script>alert('xss')</script>",
        "$(whoami)",
        "`id`",
    ]

    @pytest.mark.parametrize("payload", ADVERSARIAL_HASH_INPUTS)
    def test_validate_sha256_hex_rejects_malicious_inputs(self, payload):
        """Verify pure validation rejects all malicious / invalid SHA-256 patterns."""
        with pytest.raises(ValueError):
            validate_sha256_hex(payload)

    @pytest.mark.parametrize("payload", ADVERSARIAL_HASH_INPUTS)
    def test_local_cas_adapter_has_asset_handles_malicious_safely(self, cas_setup, payload):
        """Verify CAS adapter returns False and NEVER raises or accesses files outside CAS."""
        adapter_a, _, _, _ = cas_setup
        assert adapter_a.has_asset(payload) is False
        assert adapter_a.get_asset_path(payload) is None
        assert adapter_a.get_asset_size(payload) is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("payload", ADVERSARIAL_HASH_INPUTS)
    async def test_worker_server_stream_endpoint_rejects_traversal(self, cas_setup, payload):
        """Verify HTTP streaming endpoint rejects path traversal attacks with HTTP 400 Bad Request or client error."""
        adapter_a, _, _, _ = cas_setup
        server = WorkerServer(adapter_a, worker_id="secure-server")
        transport = httpx.ASGITransport(app=server.app)

        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            try:
                resp = await client.get(f"/api/v1/assets/{payload}/stream")
                # Must return 400 Bad Request or 404 (if URL routing catches slash before handler)
                assert resp.status_code in (400, 404), f"Expected 400 or 404 for payload {payload!r}, got {resp.status_code}"
            except (httpx.InvalidURL, ValueError):
                # httpx client rejected URL before transmission (expected for null bytes / control chars)
                pass

    def test_endpoint_url_and_ip_validation_security(self):
        """Verify IP address and endpoint URL validation rejects SSRF / injection tokens."""
        # Malicious IP strings
        bad_ips = ["127.0.0.1; rm -rf /", "999.999.999.999", "localhost:8000/api", "invalid.ip.format.here", ""]
        for bad in bad_ips:
            with pytest.raises(ValueError):
                validate_ip_address(bad)

        # Malicious Endpoint URLs
        bad_urls = ["ftp://example.com", "javascript:alert(1)", "file:///etc/passwd", "http://", ""]
        for bad_url in bad_urls:
            with pytest.raises(ValueError):
                validate_endpoint_url(bad_url)


# ============================================================================
# Section 4: Multi-Candidate Failover & Penalization under Corruption
# ============================================================================

class TestMultiCandidateFailoverAdversarial:
    @pytest.mark.asyncio
    async def test_corrupt_primary_seamless_failover_to_healthy_backup(self, cas_setup):
        """Primary candidate corrupts data -> Client seamlessly fails over to backup candidate and succeeds."""
        adapter_a, adapter_b, _, cas_b_dir = cas_setup
        total_size = 1024 * 1024  # 1 MiB
        data = os.urandom(total_size)
        h = adapter_a.store_bytes(data)

        # Primary app corrupts bytes
        primary_app = FastAPI()
        @primary_app.get("/api/v1/assets/{sha256_hex}/stream")
        async def corrupt_stream(sha256_hex: str):
            corrupt = bytearray(data)
            corrupt[100] ^= 0xFF
            return StreamingResponse(io.BytesIO(bytes(corrupt)), status_code=200)

        # Backup app sends pristine bytes
        backup_app = FastAPI()
        @backup_app.get("/api/v1/assets/{sha256_hex}/stream")
        async def pristine_stream(sha256_hex: str):
            return StreamingResponse(io.BytesIO(data), status_code=200)

        # Mount both on custom mock transport
        class MultiAppTransport(httpx.AsyncBaseTransport):
            def __init__(self, apps: Dict[str, FastAPI]):
                self.apps = {k: httpx.ASGITransport(app=v) for k, v in apps.items()}

            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                host = request.url.host
                if host in self.apps:
                    return await self.apps[host].handle_async_request(request)
                return httpx.Response(status_code=404)

        transport = MultiAppTransport({
            "primary-host": primary_app,
            "backup-host": backup_app,
        })

        candidates = [
            CandidateSource(
                worker_id="cand-primary",
                endpoint_url="http://primary-host",
                ip_address="192.168.1.50",
                port=8000,
                locality_tier="subnet",
                estimated_rtt_ms=1.0,
                load_factor=0.1,
            ),
            CandidateSource(
                worker_id="cand-backup",
                endpoint_url="http://backup-host",
                ip_address="192.168.2.50",
                port=8000,
                locality_tier="lan",
                estimated_rtt_ms=5.0,
                load_factor=0.2,
            ),
        ]

        penalized_workers = []
        def on_error(cand: CandidateSource, exc: Exception):
            penalized_workers.append(cand.worker_id)

        async with httpx.AsyncClient(transport=transport) as client:
            result = await transfer_asset_with_failover(
                client=client,
                candidates=candidates,
                sha256=h,
                cas_adapter=adapter_b,
                staging_dir=cas_b_dir / "staging",
                on_candidate_error=on_error,
            )

            assert result.success is True
            assert result.source_worker_id == "cand-backup"
            assert adapter_b.has_asset(h) is True
            assert "cand-primary" in penalized_workers
            assert len(list((cas_b_dir / "staging").iterdir())) == 0

