"""Adversarial stress, chaos, and empirical memory boundedness tests for Milestone 3 streaming data plane.

File: tests/unit/test_distributed/test_m3_adversarial_stress.py
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import threading
import time
import tracemalloc
from pathlib import Path
from typing import AsyncIterator, List

import httpx
import pytest
from httpx import ASGITransport

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.models import CandidateSource, TransferResult
from aidars.distributed.transfer import (
    IntegrityError,
    StreamAbortError,
    download_stream_to_staging,
    transfer_asset_from_candidate,
)


class StreamingByteStream(httpx.AsyncByteStream):
    """Pulls chunks lazily on demand to avoid in-memory queue accumulation."""

    def __init__(self, total_bytes: int, chunk_size: int, pattern: bytes) -> None:
        self.total_bytes = total_bytes
        self.chunk_size = chunk_size
        self.pattern = pattern

    async def __aiter__(self) -> AsyncIterator[bytes]:
        remaining = self.total_bytes
        pat_len = len(self.pattern)
        while remaining > 0:
            take = min(remaining, self.chunk_size)
            if pat_len == take:
                chunk = self.pattern
            else:
                chunk = self.pattern * (take // pat_len) + self.pattern[: (take % pat_len)]
            remaining -= len(chunk)
            yield chunk


class ChunkedStreamingTransport(httpx.AsyncBaseTransport):
    """Streaming transport serving lazy byte streams with pull-based backpressure."""

    def __init__(
        self, total_bytes: int, chunk_size: int, sha256: str, pattern: bytes
    ) -> None:
        self.total_bytes = total_bytes
        self.chunk_size = chunk_size
        self.sha256 = sha256
        self.pattern = pattern

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        stream = StreamingByteStream(self.total_bytes, self.chunk_size, self.pattern)
        headers = {
            "content-type": "application/octet-stream",
            "content-length": str(self.total_bytes),
            "x-asset-sha256": self.sha256,
        }
        return httpx.Response(200, headers=headers, stream=stream)


class TestMemoryBoundedness:
    """Verify that process RAM (RSS / heap allocation) does NOT scale with file size (O(1) memory bound)."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("file_size_mb", [10, 50, 100])
    async def test_rss_memory_bounded_large_stream(self, tmp_path: Path, file_size_mb: int):
        """Streaming 10 MB, 50 MB, 100 MB files must maintain a peak heap memory delta < 5 MiB."""
        staging_dir = tmp_path / f"stg_mem_{file_size_mb}"
        staging_dir.mkdir(parents=True, exist_ok=True)

        total_bytes = file_size_mb * 1024 * 1024
        chunk_size = 1024 * 1024  # 1 MiB

        # Precompute deterministic hash from chunk pattern without storing 100 MB in RAM
        hasher = hashlib.sha256()
        pattern = b"M3_MEMORY_BOUND_PATTERN_" + b"0" * (chunk_size - 24)
        for _ in range(file_size_mb):
            hasher.update(pattern)
        expected_hash = hasher.hexdigest()

        transport = ChunkedStreamingTransport(
            total_bytes=total_bytes,
            chunk_size=chunk_size,
            sha256=expected_hash,
            pattern=pattern,
        )

        # Track memory usage during transfer
        tracemalloc.start()
        tracemalloc.reset_peak()

        async with httpx.AsyncClient(transport=transport, base_url="http://largeworker") as client:
            stg_path, actual_hash, bytes_received = await download_stream_to_staging(
                client=client,
                endpoint_url="http://largeworker",
                sha256=expected_hash,
                staging_dir=staging_dir,
                chunk_size=chunk_size,
            )

        _, peak_mem = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert actual_hash == expected_hash
        assert bytes_received == total_bytes
        assert stg_path.exists()
        assert stg_path.stat().st_size == total_bytes
        stg_path.unlink()

        # Peak memory delta during 100 MB stream must be under 5 MiB
        peak_mb = peak_mem / (1024 * 1024)
        assert peak_mb < 5.0, f"Memory spike detected during {file_size_mb} MB transfer: peak {peak_mb:.2f} MiB"


class TestConcurrentTransferRaces:
    """Stress tests on parallel identical-hash and distinct-hash downloads."""

    @pytest.mark.asyncio
    async def test_concurrent_identical_hash_download_race(self, tmp_path: Path):
        """16 concurrent tasks downloading and committing the exact same asset simultaneously."""
        cas = LocalCASAdapter(cas_dir=tmp_path / "cas_race", staging_dir=tmp_path / "stg_race")
        payload = os.urandom(2 * 1024 * 1024)  # 2 MiB
        sha256 = hashlib.sha256(payload).hexdigest()

        async def stream_app(scope, receive, send):
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-length", str(len(payload)).encode()),
                    (b"x-asset-sha256", sha256.encode()),
                ],
            })
            await send({"type": "http.response.body", "body": payload, "more_body": False})

        transport = ASGITransport(app=stream_app)
        mock_http = httpx.AsyncClient(transport=transport, base_url="http://peer")
        cand = CandidateSource(worker_id="w-peer", endpoint_url="http://peer", ip_address="127.0.0.1", port=8000)

        async def worker_transfer():
            return await transfer_asset_from_candidate(
                client=mock_http,
                candidate=cand,
                sha256=sha256,
                cas_adapter=cas,
            )

        tasks = [worker_transfer() for _ in range(16)]
        results: List[TransferResult] = await asyncio.gather(*tasks)
        await mock_http.aclose()

        # All 16 operations succeed
        for r in results:
            assert r.success is True

        # Exactly 1 asset exists in CAS
        assert cas.has_asset(sha256) is True
        assert cas.get_cas_stats()["total_assets"] == 1

        # Zero temporary files leaked
        assert len(list(cas.staging_dir.glob("*.tmp"))) == 0

    @pytest.mark.asyncio
    async def test_concurrent_distinct_hash_download_race(self, tmp_path: Path):
        """16 concurrent tasks downloading 16 distinct assets simultaneously."""
        cas = LocalCASAdapter(cas_dir=tmp_path / "cas_distinct", staging_dir=tmp_path / "stg_distinct")
        raw_payloads = [f"content-distinct-asset-{i}".encode() * 1000 for i in range(16)]
        payloads = {
            hashlib.sha256(data).hexdigest(): data
            for data in raw_payloads
        }

        async def stream_app(scope, receive, send):
            path = scope.get("path", "")
            sha256 = path.strip("/").split("/")[3]
            payload = payloads.get(sha256, b"")

            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-length", str(len(payload)).encode()),
                    (b"x-asset-sha256", sha256.encode()),
                ],
            })
            await send({"type": "http.response.body", "body": payload, "more_body": False})

        transport = ASGITransport(app=stream_app)
        mock_http = httpx.AsyncClient(transport=transport, base_url="http://peer")
        cand = CandidateSource(worker_id="w-peer", endpoint_url="http://peer", ip_address="127.0.0.1", port=8000)

        tasks = [
            transfer_asset_from_candidate(
                client=mock_http,
                candidate=cand,
                sha256=sha256,
                cas_adapter=cas,
            )
            for sha256 in payloads
        ]
        results: List[TransferResult] = await asyncio.gather(*tasks)
        await mock_http.aclose()

        assert len(results) == 16
        for r in results:
            assert r.success is True
            assert cas.has_asset(r.sha256) is True

        assert cas.get_cas_stats()["total_assets"] == 16
        assert len(list(cas.staging_dir.glob("*.tmp"))) == 0


class TestCorruptStreamChaos:
    """Chaos scenarios: network drops, truncation, cancellation."""

    @pytest.mark.asyncio
    async def test_stream_truncation_mid_flight(self, tmp_path: Path):
        staging_dir = tmp_path / "stg_trunc"
        declared_size = 5 * 1024 * 1024
        sent_size = 1024 * 1024
        sha256 = hashlib.sha256(b"full_untruncated").hexdigest()

        async def trunc_app(scope, receive, send):
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/octet-stream"),
                    (b"content-length", str(declared_size).encode()),
                    (b"x-asset-sha256", sha256.encode()),
                ],
            })
            # Prematurely end stream after 1 chunk
            await send({
                "type": "http.response.body",
                "body": b"X" * sent_size,
                "more_body": False,
            })

        transport = ASGITransport(app=trunc_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://truncworker") as client:
            with pytest.raises(StreamAbortError):
                await download_stream_to_staging(
                    client=client,
                    endpoint_url="http://truncworker",
                    sha256=sha256,
                    staging_dir=staging_dir,
                )

        assert len(list(staging_dir.glob("*.tmp"))) == 0

    @pytest.mark.asyncio
    async def test_cancellation_during_stream(self, tmp_path: Path):
        staging_dir = tmp_path / "stg_cancel"
        staging_dir.mkdir(parents=True, exist_ok=True)
        sha256 = hashlib.sha256(b"cancel_test").hexdigest()

        async def slow_app(scope, receive, send):
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", b"1048576")],
            })
            await send({"type": "http.response.body", "body": b"A" * 1024, "more_body": True})
            await asyncio.sleep(5.0)  # simulate slow/hanging transfer

        transport = ASGITransport(app=slow_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://slowworker") as client:
            task = asyncio.create_task(
                download_stream_to_staging(
                    client=client,
                    endpoint_url="http://slowworker",
                    sha256=sha256,
                    staging_dir=staging_dir,
                )
            )
            await asyncio.sleep(0.05)
            task.cancel()

            with pytest.raises((asyncio.CancelledError, StreamAbortError)):
                await task

        assert len(list(staging_dir.glob("*.tmp"))) == 0
