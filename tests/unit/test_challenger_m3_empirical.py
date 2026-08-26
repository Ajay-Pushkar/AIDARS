from __future__ import annotations

import asyncio
import hashlib
import os
import tempfile
import time
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
    HeartbeatPayload,
    TransferResult,
    WorkerRegistrationPayload,
    validate_sha256_hex,
)
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.server import create_worker_app
from aidars.distributed.transfer import (
    CandidateExhaustedError,
    IntegrityError,
    StreamAbortError,
    WorkerHttpError,
    download_stream_to_staging,
    generate_bounded_chunks,
    parse_byte_range_header,
    transfer_asset_from_candidate,
    transfer_asset_with_failover,
)


class TestAdversarialFailoverScenarios:
    """Comprehensive multi-candidate failover and resilience stress tests."""

    @pytest.mark.asyncio
    async def test_3_tier_candidate_failover_500_corrupt_success(self, tmp_path: Path):
        """Worker 1 (500) -> Worker 2 (Corrupt Stream) -> Worker 3 (Success 200)."""
        cas_client = LocalCASAdapter(cas_dir=tmp_path / "cas_c", staging_dir=tmp_path / "stg_c")
        
        valid_data = os.urandom(128 * 1024)
        valid_sha256 = hashlib.sha256(valid_data).hexdigest()
        corrupt_data = bytearray(valid_data)
        corrupt_data[100] ^= 0xFF  # flip bit

        async def cluster_app(scope, receive, send):
            headers = dict(scope.get("headers", []))
            host = headers.get(b"host", b"").decode()
            if "w1" in host:
                await send({"type": "http.response.start", "status": 500, "headers": []})
                await send({"type": "http.response.body", "body": b"Server Failure", "more_body": False})
            elif "w2" in host:
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/octet-stream"),
                        (b"content-length", str(len(corrupt_data)).encode()),
                        (b"x-asset-sha256", valid_sha256.encode()),
                    ],
                })
                await send({"type": "http.response.body", "body": bytes(corrupt_data), "more_body": False})
            elif "w3" in host:
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/octet-stream"),
                        (b"content-length", str(len(valid_data)).encode()),
                        (b"x-asset-sha256", valid_sha256.encode()),
                    ],
                })
                await send({"type": "http.response.body", "body": valid_data, "more_body": False})
            else:
                await send({"type": "http.response.start", "status": 404, "headers": []})
                await send({"type": "http.response.body", "body": b"", "more_body": False})

        transport = ASGITransport(app=cluster_app)
        client = httpx.AsyncClient(transport=transport)

        candidates = [
            CandidateSource(worker_id="w-01", endpoint_url="http://w1", ip_address="10.0.0.1", port=8001),
            CandidateSource(worker_id="w-02", endpoint_url="http://w2", ip_address="10.0.0.2", port=8002),
            CandidateSource(worker_id="w-03", endpoint_url="http://w3", ip_address="10.0.0.3", port=8003),
        ]

        failed: List[str] = []
        res = await transfer_asset_with_failover(
            client=client,
            candidates=candidates,
            sha256=valid_sha256,
            cas_adapter=cas_client,
            on_candidate_error=lambda cand, exc: failed.append(cand.worker_id),
        )
        await client.aclose()

        assert res.success is True
        assert res.source_worker_id == "w-03"
        assert res.bytes_transferred == len(valid_data)
        assert cas_client.has_asset(valid_sha256) is True
        assert failed == ["w-01", "w-02"]
        assert len(list(cas_client.staging_dir.glob("*.tmp"))) == 0

    @pytest.mark.asyncio
    async def test_all_candidates_exhausted_error(self, tmp_path: Path):
        """When all candidates fail, raise CandidateExhaustedError and clean staging files."""
        cas_client = LocalCASAdapter(cas_dir=tmp_path / "cas_c_ex", staging_dir=tmp_path / "stg_c_ex")
        sha256 = "c" * 64

        async def dead_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 503, "headers": []})
            await send({"type": "http.response.body", "body": b"Service Unavailable", "more_body": False})

        transport = ASGITransport(app=dead_app)
        client = httpx.AsyncClient(transport=transport)
        candidates = [
            CandidateSource(worker_id="w-01", endpoint_url="http://w1", ip_address="10.0.0.1", port=8001),
            CandidateSource(worker_id="w-02", endpoint_url="http://w2", ip_address="10.0.0.2", port=8002),
        ]

        with pytest.raises(CandidateExhaustedError) as exc_info:
            await transfer_asset_with_failover(
                client=client,
                candidates=candidates,
                sha256=sha256,
                cas_adapter=cas_client,
            )
        await client.aclose()

        assert exc_info.value.candidate_count == 2
        assert len(list(cas_client.staging_dir.glob("*.tmp"))) == 0


class TestAdversarialConcurrencyRaces:
    """Stress tests on 32 concurrent identical and distinct transfers."""

    @pytest.mark.asyncio
    async def test_32_concurrent_identical_hash_race(self, tmp_path: Path):
        """32 simultaneous transfer attempts of the exact same 2 MiB file."""
        cas_client = LocalCASAdapter(cas_dir=tmp_path / "cas_r32", staging_dir=tmp_path / "stg_r32")
        payload = os.urandom(2 * 1024 * 1024)
        sha256 = hashlib.sha256(payload).hexdigest()

        async def srv_app(scope, receive, send):
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

        transport = ASGITransport(app=srv_app)
        client = httpx.AsyncClient(transport=transport, base_url="http://peer")
        cand = CandidateSource(worker_id="w-peer", endpoint_url="http://peer", ip_address="127.0.0.1", port=8000)

        tasks = [
            transfer_asset_from_candidate(
                client=client,
                candidate=cand,
                sha256=sha256,
                cas_adapter=cas_client,
            )
            for _ in range(32)
        ]
        results = await asyncio.gather(*tasks)
        await client.aclose()

        assert len(results) == 32
        for r in results:
            assert r.success is True

        assert cas_client.has_asset(sha256) is True
        assert cas_client.get_cas_stats()["total_assets"] == 1
        assert len(list(cas_client.staging_dir.glob("*.tmp"))) == 0

    @pytest.mark.asyncio
    async def test_20_concurrent_distinct_hash_race(self, tmp_path: Path):
        """20 distinct assets generated and transferred simultaneously with matching hashes."""
        cas_client = LocalCASAdapter(cas_dir=tmp_path / "cas_dist20", staging_dir=tmp_path / "stg_dist20")
        
        # Build valid dictionary of {sha256: payload}
        payload_map: Dict[str, bytes] = {}
        for i in range(20):
            data = f"distinct-payload-{i}-".encode() * 5000  # ~100 KB
            h = hashlib.sha256(data).hexdigest()
            payload_map[h] = data

        async def dist_app(scope, receive, send):
            path = scope.get("path", "")
            sha256 = path.strip("/").split("/")[3]
            data = payload_map.get(sha256, b"")
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/octet-stream"),
                    (b"content-length", str(len(data)).encode()),
                    (b"x-asset-sha256", sha256.encode()),
                ],
            })
            await send({"type": "http.response.body", "body": data, "more_body": False})

        transport = ASGITransport(app=dist_app)
        client = httpx.AsyncClient(transport=transport, base_url="http://peer")
        cand = CandidateSource(worker_id="w-peer", endpoint_url="http://peer", ip_address="127.0.0.1", port=8000)

        tasks = [
            transfer_asset_from_candidate(
                client=client,
                candidate=cand,
                sha256=h,
                cas_adapter=cas_client,
            )
            for h in payload_map
        ]
        results = await asyncio.gather(*tasks)
        await client.aclose()

        assert len(results) == 20
        for r in results:
            assert r.success is True
            assert cas_client.has_asset(r.sha256) is True

        assert cas_client.get_cas_stats()["total_assets"] == 20
        assert len(list(cas_client.staging_dir.glob("*.tmp"))) == 0


class TestAdversarialCorruptionAndCleanup:
    """Stress tests on corrupt streams, truncation, and zero-leak invariants."""

    @pytest.mark.asyncio
    async def test_corrupt_bit_flip_head_middle_tail(self, tmp_path: Path):
        """Bit flips at head, middle, and tail must all be rejected and deleted."""
        staging_dir = tmp_path / "stg_flips"
        staging_dir.mkdir(parents=True, exist_ok=True)
        raw_data = os.urandom(64 * 1024)
        expected_hash = hashlib.sha256(raw_data).hexdigest()

        for flip_pos in [0, 32768, len(raw_data) - 1]:
            bad_data = bytearray(raw_data)
            bad_data[flip_pos] ^= 0xFF

            async def corrupt_app(scope, receive, send):
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/octet-stream"),
                        (b"content-length", str(len(bad_data)).encode()),
                        (b"x-asset-sha256", expected_hash.encode()),
                    ],
                })
                await send({"type": "http.response.body", "body": bytes(bad_data), "more_body": False})

            transport = ASGITransport(app=corrupt_app)
            async with httpx.AsyncClient(transport=transport, base_url="http://corrupt") as client:
                with pytest.raises(IntegrityError):
                    await download_stream_to_staging(
                        client=client,
                        endpoint_url="http://corrupt",
                        sha256=expected_hash,
                        staging_dir=staging_dir,
                    )

            # Staging directory must have zero temporary files remaining
            assert len(list(staging_dir.glob("*.tmp"))) == 0

    @pytest.mark.asyncio
    async def test_stream_truncation_cleanup(self, tmp_path: Path):
        """Stream terminating early before sending Content-Length bytes must be cleaned up."""
        staging_dir = tmp_path / "stg_trunc"
        staging_dir.mkdir(parents=True, exist_ok=True)
        sha256 = "d" * 64

        async def trunc_app(scope, receive, send):
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-length", b"1048576"),  # Declares 1 MiB
                ],
            })
            await send({"type": "http.response.body", "body": b"partial" * 100, "more_body": False})

        transport = ASGITransport(app=trunc_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://trunc") as client:
            with pytest.raises(StreamAbortError):
                await download_stream_to_staging(
                    client=client,
                    endpoint_url="http://trunc",
                    sha256=sha256,
                    staging_dir=staging_dir,
                )

        assert len(list(staging_dir.glob("*.tmp"))) == 0


class TestHTTPRangeCompliance:
    """Validate HTTP Range parsing and generator behaviors."""

    def test_range_parsing_matrix(self):
        # Full content
        assert parse_byte_range_header(None, 1000) == (0, 999, 1000, False)
        assert parse_byte_range_header("", 1000) == (0, 999, 1000, False)

        # Valid ranges
        assert parse_byte_range_header("bytes=0-499", 1000) == (0, 499, 500, True)
        assert parse_byte_range_header("bytes=500-", 1000) == (500, 999, 500, True)
        assert parse_byte_range_header("bytes=0-0", 1000) == (0, 0, 1, True)
        assert parse_byte_range_header("bytes=999-999", 1000) == (999, 999, 1, True)

        # Out-of-bounds (IndexError for HTTP 416)
        with pytest.raises(IndexError):
            parse_byte_range_header("bytes=1000-", 1000)
        with pytest.raises(IndexError):
            parse_byte_range_header("bytes=500-400", 1000)

        # Malformed syntax (ValueError for fallback)
        with pytest.raises(ValueError):
            parse_byte_range_header("invalid-range", 1000)
        with pytest.raises(ValueError):
            parse_byte_range_header("bytes=-500", 1000)

    def test_generate_bounded_chunks_closes_handle(self, tmp_path: Path):
        file_path = tmp_path / "test_file.bin"
        file_path.write_bytes(b"HELLO_WORLD_STREAMING_DATA")
        
        handle = file_path.open("rb")
        chunks = list(generate_bounded_chunks(handle, bytes_to_send=11, chunk_size=4))
        
        assert b"".join(chunks) == b"HELLO_WORLD"
        assert handle.closed is True
