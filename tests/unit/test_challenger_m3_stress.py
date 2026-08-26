from __future__ import annotations
import asyncio
import hashlib
import os
import tracemalloc
from pathlib import Path
from typing import AsyncIterator, List
import httpx
import pytest
from fastapi.testclient import TestClient
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.models import CandidateSource, TransferResult
from aidars.distributed.server import create_worker_app
from aidars.distributed.transfer import (DEFAULT_CHUNK_SIZE, CandidateExhaustedError, IntegrityError, StagingContext, StreamAbortError, WorkerHttpError, download_stream_to_staging, generate_bounded_chunks, parse_byte_range_header, transfer_asset_from_candidate, transfer_asset_with_failover)

class StreamingByteStream(httpx.AsyncByteStream):
    def __init__(self, total_bytes: int, chunk_size: int, pattern: bytes):
        self.total_bytes = total_bytes
        self.chunk_size = chunk_size
        self.pattern = pattern
    async def __aiter__(self) -> AsyncIterator[bytes]:
        remaining = self.total_bytes
        pat_len = len(self.pattern)
        while remaining > 0:
            take = min(remaining, self.chunk_size)
            chunk = self.pattern * (take // pat_len) + self.pattern[:(take % pat_len)]
            remaining -= len(chunk)
            yield chunk

class TrueStreamingTransport(httpx.AsyncBaseTransport):
    def __init__(self, total_bytes: int, chunk_size: int, sha256: str, pattern: bytes):
        self.total_bytes = total_bytes
        self.chunk_size = chunk_size
        self.sha256 = sha256
        self.pattern = pattern
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        stream = StreamingByteStream(self.total_bytes, self.chunk_size, self.pattern)
        headers = {'content-type': 'application/octet-stream', 'content-length': str(self.total_bytes), 'x-asset-sha256': self.sha256}
        return httpx.Response(200, headers=headers, stream=stream)

class TestCorruptDataInjectionAdversarial:
    @pytest.mark.asyncio
    @pytest.mark.parametrize('corrupt_type', ['flip_head', 'flip_mid', 'flip_tail', 'extra_bytes', 'garbage'])
    async def test_corrupt_payload_rejection_and_zero_file_leak(self, tmp_path: Path, corrupt_type: str):
        stg_dir = tmp_path / f'stg_corrupt_{corrupt_type}'
        stg_dir.mkdir(parents=True, exist_ok=True)
        original_data = bytearray(b'CORRUPT_TEST_PAYLOAD_CONTENT_' * 1024)
        expected_sha256 = hashlib.sha256(original_data).hexdigest()
        corrupted = bytearray(original_data)
        declared_length = len(original_data)
        if corrupt_type == 'flip_head': corrupted[0] ^= 0xFF
        elif corrupt_type == 'flip_mid': corrupted[len(corrupted) // 2] ^= 0xFF
        elif corrupt_type == 'flip_tail': corrupted[-1] ^= 0xFF
        elif corrupt_type == 'extra_bytes': corrupted.extend(b'EXTRA_TRAILING_GARBAGE')
        elif corrupt_type == 'garbage': corrupted = bytearray(os.urandom(len(original_data)))
        class CorruptByteStream(httpx.AsyncByteStream):
            def __init__(self, data: bytes): self.data = data
            async def __aiter__(self) -> AsyncIterator[bytes]: yield self.data
        class CorruptTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                s = CorruptByteStream(bytes(corrupted))
                headers = {'content-type': 'application/octet-stream', 'content-length': str(declared_length), 'x-asset-sha256': expected_sha256}
                return httpx.Response(200, headers=headers, stream=s)
        async with httpx.AsyncClient(transport=CorruptTransport(), base_url='http://corrupt') as client:
            with pytest.raises((IntegrityError, StreamAbortError)):
                await download_stream_to_staging(client=client, endpoint_url='http://corrupt', sha256=expected_sha256, staging_dir=stg_dir)
        remaining_tmp = list(stg_dir.glob('*.tmp'))
        assert len(remaining_tmp) == 0, f'Leaked temporary staging files found: {remaining_tmp}'

class TestPrematureEOFAndAborts:
    @pytest.mark.asyncio
    @pytest.mark.parametrize('drop_at_byte', [0, 1, 512, 1024 * 1024 - 1])
    async def test_premature_eof_at_various_offsets(self, tmp_path: Path, drop_at_byte: int):
        stg_dir = tmp_path / f'stg_eof_{drop_at_byte}'
        stg_dir.mkdir(parents=True, exist_ok=True)
        declared_size = 2 * 1024 * 1024
        sha256 = 'a' * 64
        class TruncatedByteStream(httpx.AsyncByteStream):
            def __init__(self, limit: int): self.limit = limit
            async def __aiter__(self) -> AsyncIterator[bytes]:
                if self.limit > 0: yield b'X' * self.limit
        class TruncatedTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
                s = TruncatedByteStream(drop_at_byte)
                headers = {'content-type': 'application/octet-stream', 'content-length': str(declared_size), 'x-asset-sha256': sha256}
                return httpx.Response(200, headers=headers, stream=s)
        async with httpx.AsyncClient(transport=TruncatedTransport(), base_url='http://trunc') as client:
            with pytest.raises(StreamAbortError) as exc_info:
                await download_stream_to_staging(client=client, endpoint_url='http://trunc', sha256=sha256, staging_dir=stg_dir)
        assert 'Stream truncated' in str(exc_info.value)
        assert len(list(stg_dir.glob('*.tmp'))) == 0

class TestRangeHeaderBoundaries:
    def test_range_parsing_matrix(self):
        start, end, length, partial = parse_byte_range_header(None, 1000)
        assert (start, end, length, partial) == (0, 999, 1000, False)
        start, end, length, partial = parse_byte_range_header('bytes=100-', 1000)
        assert (start, end, length, partial) == (100, 999, 900, True)
        start, end, length, partial = parse_byte_range_header('bytes=100-200', 1000)
        assert (start, end, length, partial) == (100, 200, 101, True)
        start, end, length, partial = parse_byte_range_header('bytes=500-2000', 1000)
        assert (start, end, length, partial) == (500, 999, 500, True)
        start, end, length, partial = parse_byte_range_header('bytes=0-', 0)
        assert (start, end, length, partial) == (0, 0, 0, True)
        with pytest.raises(IndexError):
            parse_byte_range_header('bytes=1000-', 1000)
        with pytest.raises(IndexError):
            parse_byte_range_header('bytes=500-100', 1000)
        with pytest.raises(ValueError):
            parse_byte_range_header('bytes=abc-def', 1000)
        with pytest.raises(ValueError):
            parse_byte_range_header('characters=0-100', 1000)
    def test_worker_server_range_endpoint_responses(self, tmp_path: Path):
        cas = LocalCASAdapter(cas_dir=tmp_path / 'srv_cas', staging_dir=tmp_path / 'srv_stg')
        data = b'0123456789' * 100
        sha256 = cas.store_bytes(data)
        app = create_worker_app(cas_adapter=cas, worker_id='w-test')
        client = TestClient(app)
        resp = client.get(f'/api/v1/assets/{sha256}/stream')
        assert resp.status_code == 200
        assert resp.headers['content-length'] == '1000'
        assert resp.content == data
        resp206 = client.get(f'/api/v1/assets/{sha256}/stream', headers={'Range': 'bytes=100-199'})
        assert resp206.status_code == 206
        assert resp206.headers['content-length'] == '100'
        assert resp206.headers['content-range'] == 'bytes 100-199/1000'
        assert resp206.content == data[100:200]
        resp416 = client.get(f'/api/v1/assets/{sha256}/stream', headers={'Range': 'bytes=5000-'})
        assert resp416.status_code == 416
        assert resp416.headers['content-range'] == 'bytes */1000'
        resp_malformed = client.get(f'/api/v1/assets/{sha256}/stream', headers={'Range': 'bytes=bad-input'})
        assert resp_malformed.status_code == 200
        assert resp_malformed.content == data

class TestEmpiricalRAMBoundedness:
    @pytest.mark.asyncio
    @pytest.mark.parametrize('file_size_mb', [50, 100])
    async def test_large_stream_ram_consumption_bounded(self, tmp_path: Path, file_size_mb: int):
        stg_dir = tmp_path / f'stg_ram_{file_size_mb}'
        stg_dir.mkdir(parents=True, exist_ok=True)
        chunk_size = 1024 * 1024
        total_bytes = file_size_mb * chunk_size
        pattern = b'M3_RAM_PROFILE_' + b'Z' * (65536 - 15)
        hasher = hashlib.sha256()
        pat_len = len(pattern)
        remaining = total_bytes
        while remaining > 0:
            take = min(remaining, chunk_size)
            chunk = pattern * (take // pat_len) + pattern[:(take % pat_len)]
            hasher.update(chunk)
            remaining -= len(chunk)
        expected_sha256 = hasher.hexdigest()
        transport = TrueStreamingTransport(total_bytes=total_bytes, chunk_size=chunk_size, sha256=expected_sha256, pattern=pattern)
        tracemalloc.start()
        tracemalloc.reset_peak()
        async with httpx.AsyncClient(transport=transport, base_url='http://streamer') as client:
            stg_path, actual_hash, bytes_received = await download_stream_to_staging(client=client, endpoint_url='http://streamer', sha256=expected_sha256, staging_dir=stg_dir, chunk_size=chunk_size)
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert actual_hash == expected_sha256
        assert bytes_received == total_bytes
        assert stg_path.exists()
        assert stg_path.stat().st_size == total_bytes
        stg_path.unlink()
        peak_mb = peak_bytes / (1024 * 1024)
        print(f'Empirical Peak RAM for {file_size_mb} MB transfer: {peak_mb:.2f} MiB')
        assert peak_mb < 5.0, f'Memory spike detected: {peak_mb:.2f} MiB for {file_size_mb} MB stream'
        assert len(list(stg_dir.glob('*.tmp'))) == 0

class TestAtomicCASCommitConcurrency:
    @pytest.mark.asyncio
    async def test_concurrent_48_staging_commits(self, tmp_path: Path):
        cas = LocalCASAdapter(cas_dir=tmp_path / 'cas_atomic', staging_dir=tmp_path / 'stg_atomic')
        payload = b'ATOMIC_COMMIT_COLLISION_DATA_' * 4096
        sha256 = hashlib.sha256(payload).hexdigest()
        staging_files: List[Path] = []
        for _ in range(48):
            stg = cas.create_staging_file(sha256)
            stg.write_bytes(payload)
            staging_files.append(stg)
        def commit_task(stg_file: Path) -> bool:
            return cas.commit_staged_file(stg_file, sha256)
        loop = asyncio.get_running_loop()
        tasks = [loop.run_in_executor(None, commit_task, sf) for sf in staging_files]
        results = await asyncio.gather(*tasks)
        assert all(results), 'Some concurrent commits failed'
        assert cas.has_asset(sha256) is True
        assert cas.get_cas_stats()['total_assets'] == 1
        assert len(list(cas.staging_dir.glob('*.tmp'))) == 0
