"""Unit tests for binary asset transfer engine, staging lifecycle, streaming verification, and failover.

File: tests/unit/test_distributed/test_transfer.py
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import os
import tempfile
import time
from pathlib import Path
from typing import AsyncIterator, Callable, Dict, List, Optional, Tuple

import httpx
import pytest
from httpx import ASGITransport

from aidars.distributed.cas_adapter import CASAdapter, LocalCASAdapter
from aidars.distributed.models import (
    CandidateSource,
    LocalityTier,
    TransferResult,
    WorkerCapabilities,
    validate_sha256_hex,
)
from aidars.distributed.transfer import (
    CandidateExhaustedError,
    CASCommitError,
    IntegrityError,
    StagingContext,
    StreamAbortError,
    TransferError,
    WorkerHttpError,
    download_stream_to_staging,
    generate_bounded_chunks,
    parse_byte_range_header,
    transfer_asset_from_candidate,
    transfer_asset_with_failover,
)


# ============================================================================
# Fixtures & Test Helpers
# ============================================================================

@pytest.fixture
def local_cas(tmp_path: Path) -> LocalCASAdapter:
    cas_dir = tmp_path / "cas"
    staging_dir = tmp_path / "staging"
    return LocalCASAdapter(cas_dir=cas_dir, staging_dir=staging_dir, chunk_size=1024 * 1024)


def make_candidate(
    worker_id: str = "w-cand-01",
    endpoint_url: str = "http://127.0.0.1:8000",
    ip: str = "127.0.0.1",
    port: int = 8000,
) -> CandidateSource:
    return CandidateSource(
        worker_id=worker_id,
        endpoint_url=endpoint_url,
        ip_address=ip,
        port=port,
        locality_tier=LocalityTier.LOOPBACK.value,
        estimated_rtt_ms=1.0,
        load_factor=0.1,
    )


# ============================================================================
# 1. StagingContext Lifecycle & Exception Safety
# ============================================================================

class TestStagingContextLifecycle:
    """Verify disk staging context manager guarantees zero temporary file leaks."""

    def test_staging_context_normal_exit_uncommitted_deletes_file(self, tmp_path: Path):
        staging_dir = tmp_path / "staging"
        valid_hash = hashlib.sha256(b"sample data").hexdigest()

        stg_path = None
        with StagingContext(staging_dir, valid_hash) as ctx:
            stg_path = ctx.path
            stg_path.write_bytes(b"sample data")
            assert stg_path.exists()
            assert stg_path.parent == staging_dir.resolve()

        # Without ctx.mark_committed(), file MUST be deleted on block exit
        assert not stg_path.exists()

    def test_staging_context_exception_exit_deletes_file(self, tmp_path: Path):
        staging_dir = tmp_path / "staging"
        valid_hash = hashlib.sha256(b"exception data").hexdigest()

        stg_path = None
        with pytest.raises(RuntimeError, match="Simulated crash"):
            with StagingContext(staging_dir, valid_hash) as ctx:
                stg_path = ctx.path
                stg_path.write_bytes(b"partial downloaded bytes")
                assert stg_path.exists()
                raise RuntimeError("Simulated crash mid-download")

        assert not stg_path.exists()

    def test_staging_context_marked_committed_preserves_file(self, tmp_path: Path):
        staging_dir = tmp_path / "staging"
        valid_hash = hashlib.sha256(b"committed data").hexdigest()

        with StagingContext(staging_dir, valid_hash) as ctx:
            stg_path = ctx.path
            stg_path.write_bytes(b"committed data")
            ctx.mark_committed()

        # File is preserved for atomic rename
        assert stg_path.exists()
        stg_path.unlink()


# ============================================================================
# 2. Range Parsing & Bounded Chunks
# ============================================================================

class TestRangeParsingAndChunking:
    """Test HTTP Range header parsing and generator cleanup."""

    def test_parse_range_none_or_empty(self):
        start, end, length, is_partial = parse_byte_range_header(None, 1000)
        assert (start, end, length, is_partial) == (0, 999, 1000, False)

        start, end, length, is_partial = parse_byte_range_header("", 1000)
        assert (start, end, length, is_partial) == (0, 999, 1000, False)

    def test_parse_range_valid_open_ended(self):
        start, end, length, is_partial = parse_byte_range_header("bytes=200-", 1000)
        assert (start, end, length, is_partial) == (200, 999, 800, True)

    def test_parse_range_valid_closed(self):
        start, end, length, is_partial = parse_byte_range_header("bytes=200-499", 1000)
        assert (start, end, length, is_partial) == (200, 499, 300, True)

    def test_parse_range_zero_byte_file(self):
        start, end, length, is_partial = parse_byte_range_header(None, 0)
        assert (start, end, length, is_partial) == (0, 0, 0, False)

        start, end, length, is_partial = parse_byte_range_header("bytes=0-", 0)
        assert (start, end, length, is_partial) == (0, 0, 0, True)

    def test_parse_range_out_of_bounds_raises_index_error(self):
        with pytest.raises(IndexError):
            parse_byte_range_header("bytes=1000-", 1000)

        with pytest.raises(IndexError):
            parse_byte_range_header("bytes=500-200", 1000)

    def test_parse_range_invalid_syntax_raises_value_error(self):
        with pytest.raises(ValueError):
            parse_byte_range_header("invalid-range", 1000)

    def test_generate_bounded_chunks_exact_and_closes(self):
        data = b"0123456789" * 100
        stream = io.BytesIO(data)
        chunks = list(generate_bounded_chunks(stream, bytes_to_send=len(data), chunk_size=32))
        assert b"".join(chunks) == data
        assert stream.closed is True

    def test_generate_bounded_chunks_closes_on_early_termination(self):
        data = b"0123456789" * 100
        stream = io.BytesIO(data)
        gen = generate_bounded_chunks(stream, bytes_to_send=len(data), chunk_size=32)
        _ = next(gen)
        gen.close()
        assert stream.closed is True


# ============================================================================
# 3. Streaming Download Matrix: Chunk Boundaries & Scaled Payloads
# ============================================================================

class TestStreamingDownloadPayloadMatrix:
    """Test chunked download and incremental verification across varied payload sizes."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "payload_size,label",
        [
            (1, "1_byte"),
            (1024, "1_KiB"),
            (65536, "64_KiB"),
            (1048575, "1_MiB_minus_1B"),
            (1048576, "exact_1_MiB"),
            (1048577, "1_MiB_plus_1B"),
            (3 * 1024 * 1024, "3_MiB"),
        ],
    )
    async def test_download_various_payload_sizes(self, tmp_path: Path, payload_size: int, label: str):
        staging_dir = tmp_path / f"staging_{label}"
        data = os.urandom(payload_size)
        expected_hash = hashlib.sha256(data).hexdigest()

        async def stream_app(scope, receive, send):
            if scope["type"] == "http":
                chunk_size = 1024 * 1024
                await send({
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [
                        (b"content-type", b"application/octet-stream"),
                        (b"content-length", str(len(data)).encode()),
                        (b"x-asset-sha256", expected_hash.encode()),
                    ],
                })
                for i in range(0, len(data), chunk_size):
                    chunk = data[i : i + chunk_size]
                    await send({
                        "type": "http.response.body",
                        "body": chunk,
                        "more_body": (i + chunk_size < len(data)),
                    })

        transport = ASGITransport(app=stream_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testworker") as client:
            stg_path, verified_hash, bytes_received = await download_stream_to_staging(
                client=client,
                endpoint_url="http://testworker",
                sha256=expected_hash,
                staging_dir=staging_dir,
            )

            assert verified_hash == expected_hash
            assert bytes_received == payload_size
            assert stg_path.exists()
            assert stg_path.stat().st_size == payload_size
            assert stg_path.read_bytes() == data
            stg_path.unlink()

    @pytest.mark.asyncio
    async def test_zero_byte_asset_streaming(self, tmp_path: Path):
        staging_dir = tmp_path / "staging_zero"
        data = b""
        expected_hash = hashlib.sha256(data).hexdigest()

        async def stream_app(scope, receive, send):
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/octet-stream"),
                    (b"content-length", b"0"),
                    (b"x-asset-sha256", expected_hash.encode()),
                ],
            })
            await send({"type": "http.response.body", "body": b"", "more_body": False})

        transport = ASGITransport(app=stream_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testworker") as client:
            stg_path, verified_hash, bytes_received = await download_stream_to_staging(
                client=client,
                endpoint_url="http://testworker",
                sha256=expected_hash,
                staging_dir=staging_dir,
            )
            assert verified_hash == expected_hash
            assert bytes_received == 0
            assert stg_path.stat().st_size == 0
            stg_path.unlink()


# ============================================================================
# 4. Corrupt Stream Injections & Zero-Staging-Leak Verification
# ============================================================================

class TestCorruptStreamInjectionRejection:
    """Verify that any corrupted byte stream is rejected and cleaned up without staging leaks."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "corruption_mode",
        ["bit_flip_head", "bit_flip_middle", "bit_flip_tail", "truncated_stream", "extra_bytes_appended"],
    )
    async def test_corrupted_stream_rejection_and_cleanup(self, tmp_path: Path, corruption_mode: str):
        staging_dir = tmp_path / f"stg_corrupt_{corruption_mode}"
        staging_dir.mkdir(parents=True, exist_ok=True)

        original_data = bytearray(os.urandom(2 * 1024 * 1024))  # 2 MiB
        expected_hash = hashlib.sha256(bytes(original_data)).hexdigest()

        if corruption_mode == "bit_flip_head":
            original_data[0] ^= 0xFF
        elif corruption_mode == "bit_flip_middle":
            original_data[1000000] ^= 0x01
        elif corruption_mode == "bit_flip_tail":
            original_data[-1] ^= 0x80
        elif corruption_mode == "truncated_stream":
            original_data = original_data[: 1024 * 1024]
        elif corruption_mode == "extra_bytes_appended":
            original_data.extend(b"EXTRA_TRAILING_BYTES")

        corrupt_payload = bytes(original_data)

        async def stream_app(scope, receive, send):
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/octet-stream"),
                    (b"content-length", str(len(corrupt_payload)).encode()),
                    (b"x-asset-sha256", expected_hash.encode()),
                ],
            })
            chunk_size = 1024 * 1024
            for i in range(0, len(corrupt_payload), chunk_size):
                chunk = corrupt_payload[i : i + chunk_size]
                await send({
                    "type": "http.response.body",
                    "body": chunk,
                    "more_body": (i + chunk_size < len(corrupt_payload)),
                })

        transport = ASGITransport(app=stream_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testworker") as client:
            with pytest.raises((IntegrityError, StreamAbortError)):
                await download_stream_to_staging(
                    client=client,
                    endpoint_url="http://testworker",
                    sha256=expected_hash,
                    staging_dir=staging_dir,
                )

        remaining_tmp = list(staging_dir.glob("*.tmp"))
        assert len(remaining_tmp) == 0, f"Leaked staging files: {remaining_tmp}"

    @pytest.mark.asyncio
    async def test_server_http_error_handling(self, tmp_path: Path):
        staging_dir = tmp_path / "stg_http_err"
        staging_dir.mkdir(parents=True, exist_ok=True)
        valid_hash = hashlib.sha256(b"foo").hexdigest()

        # 404 Not Found
        async def not_found_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 404, "headers": []})
            await send({"type": "http.response.body", "body": b"Asset Not Found", "more_body": False})

        transport_404 = ASGITransport(app=not_found_app)
        async with httpx.AsyncClient(transport=transport_404, base_url="http://testworker") as client:
            with pytest.raises(WorkerHttpError) as exc_info:
                await download_stream_to_staging(
                    client=client,
                    endpoint_url="http://testworker",
                    sha256=valid_hash,
                    staging_dir=staging_dir,
                )
            assert exc_info.value.status_code == 404

        assert len(list(staging_dir.glob("*.tmp"))) == 0


# ============================================================================
# 5. End-to-End Candidate Transfer & Automatic Fail-Over
# ============================================================================

class TestTransferAndFailover:
    """Test transfer_asset_from_candidate and multi-candidate failover orchestration."""

    @pytest.mark.asyncio
    async def test_transfer_asset_from_candidate_success(self, local_cas: LocalCASAdapter):
        payload = b"Valid Payload Content for E2E Transfer Pipeline"
        sha256 = hashlib.sha256(payload).hexdigest()
        cand = make_candidate("cand-01")

        async def ok_app(scope, receive, send):
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/octet-stream"),
                    (b"content-length", str(len(payload)).encode()),
                ],
            })
            await send({"type": "http.response.body", "body": payload, "more_body": False})

        transport = ASGITransport(app=ok_app)
        async with httpx.AsyncClient(transport=transport, base_url="http://cand-01") as client:
            result = await transfer_asset_from_candidate(
                client=client,
                candidate=cand,
                sha256=sha256,
                cas_adapter=local_cas,
            )

            assert result.success is True
            assert result.sha256 == sha256
            assert result.bytes_transferred == len(payload)
            assert result.source_worker_id == "cand-01"
            assert local_cas.has_asset(sha256) is True
            assert local_cas.get_asset_size(sha256) == len(payload)

    @pytest.mark.asyncio
    async def test_transfer_asset_from_candidate_local_cache_hit(self, local_cas: LocalCASAdapter):
        payload = b"Already in local CAS"
        sha256 = local_cas.store_bytes(payload)
        cand = make_candidate("cand-remote")

        async with httpx.AsyncClient() as client:
            result = await transfer_asset_from_candidate(
                client=client,
                candidate=cand,
                sha256=sha256,
                cas_adapter=local_cas,
            )
            assert result.success is True
            assert result.bytes_transferred == 0
            assert result.source_worker_id == "local_cas"

    @pytest.mark.asyncio
    async def test_failover_when_primary_candidate_corrupts_data(self, local_cas: LocalCASAdapter):
        payload = b"Important Asset Needed Across Cluster"
        sha256 = hashlib.sha256(payload).hexdigest()

        cand_bad = make_candidate("cand-bad", endpoint_url="http://worker-bad")
        cand_good = make_candidate("cand-good", endpoint_url="http://worker-good")

        async def bad_app(scope, receive, send):
            corrupt = b"Corrupted bytes"
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", str(len(corrupt)).encode())],
            })
            await send({"type": "http.response.body", "body": corrupt, "more_body": False})

        async def good_app(scope, receive, send):
            await send({
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-length", str(len(payload)).encode())],
            })
            await send({"type": "http.response.body", "body": payload, "more_body": False})

        def dispatch_app(scope, receive, send):
            headers = dict(scope.get("headers", []))
            host = headers.get(b"host", b"").decode()
            if "worker-bad" in host:
                return bad_app(scope, receive, send)
            return good_app(scope, receive, send)

        transport = ASGITransport(app=dispatch_app)
        async with httpx.AsyncClient(transport=transport) as client:
            errors_caught = []

            def on_error(c: CandidateSource, err: Exception):
                errors_caught.append((c.worker_id, err))

            result = await transfer_asset_with_failover(
                client=client,
                candidates=[cand_bad, cand_good],
                sha256=sha256,
                cas_adapter=local_cas,
                on_candidate_error=on_error,
            )

            assert result.success is True
            assert result.source_worker_id == "cand-good"
            assert local_cas.has_asset(sha256) is True
            assert len(errors_caught) == 1
            assert errors_caught[0][0] == "cand-bad"

    @pytest.mark.asyncio
    async def test_failover_exhaustion_raises_error(self, local_cas: LocalCASAdapter):
        sha256 = hashlib.sha256(b"unobtainable").hexdigest()
        cand1 = make_candidate("cand-1", endpoint_url="http://c1")
        cand2 = make_candidate("cand-2", endpoint_url="http://c2")

        async def fail_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 500, "headers": []})
            await send({"type": "http.response.body", "body": b"Server Error", "more_body": False})

        transport = ASGITransport(app=fail_app)
        async with httpx.AsyncClient(transport=transport) as client:
            with pytest.raises(CandidateExhaustedError) as exc_info:
                await transfer_asset_with_failover(
                    client=client,
                    candidates=[cand1, cand2],
                    sha256=sha256,
                    cas_adapter=local_cas,
                )
            assert exc_info.value.candidate_count == 2
