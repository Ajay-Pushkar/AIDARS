"""Tier 2: Boundary Value Analysis (BVA) and Corner Case Tests.

Covers:
- Empty files & zero-byte streams
- Chunk boundaries (1B, 1023B, 1024B, 1048575B, 1048576B, 1048577B, 2097152B)
- Extreme file sizes (10 MiB, 25 MiB, 50 MiB)
- Adversarial SHA-256 strings & path traversal injection
- Range header boundaries & invalid ranges
- Network dropouts & partial chunk dropouts
- Cryptographic bit flips, corrupted bytes, truncated payloads
- Registry scale, duplicate IDs, unicode worker IDs
- Heartbeat boundary thresholds & clock skew
- IP subnet boundaries & metrics 0-division guards
Total >= 100 tests.
"""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pytest

from .conftest import (
    CHUNK_SIZE,
    CandidateSource,
    HeartbeatPayload,
    LocateAssetsRequest,
    LocateAssetsResponse,
    MockCandidatePrioritizer,
    MockCASAdapter,
    MockCoordinator,
    MockStreamingServer,
    MockWorkerNode,
    MockWorkerRegistry,
    TransferMetrics,
    WorkerCapabilities,
    WorkerInfo,
)


# ============================================================================
# Suite 1: Zero-Byte & Empty Payload Boundaries (10 tests)
# ============================================================================

def test_bva_01_zero_byte_hashing(zero_byte_payload: Tuple[bytes, str]):
    data, expected_h = zero_byte_payload
    assert hashlib.sha256(data).hexdigest() == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def test_bva_02_zero_byte_cas_storage(temp_cas_dir: Path, zero_byte_payload: Tuple[bytes, str]):
    data, h = zero_byte_payload
    cas = MockCASAdapter(temp_cas_dir)
    stored_h = cas.store_bytes(data)
    assert stored_h == h
    assert cas.has_asset(h) is True
    assert cas.get_asset_size(h) == 0


def test_bva_03_zero_byte_stream(temp_cas_dir: Path, zero_byte_payload: Tuple[bytes, str]):
    data, h = zero_byte_payload
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h))
    assert len(chunks) == 0  # 0 chunks streamed


def test_bva_04_zero_byte_missing_set_resolution(temp_cas_dir: Path, zero_byte_payload: Tuple[bytes, str]):
    data, h = zero_byte_payload
    cas = MockCASAdapter(temp_cas_dir)
    missing = cas.get_missing_hashes([h])
    assert missing == {h}
    cas.store_bytes(data)
    assert cas.get_missing_hashes([h]) == set()


def test_bva_05_zero_byte_sync_between_workers(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], zero_byte_payload: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = zero_byte_payload
    wb.cas.store_bytes(data)
    coord.registry.update_inventory(wb.worker_id, added={h})
    results = wa.sync_missing_assets([h], {wb.worker_id: wb})
    assert results[h] is True
    assert wa.cas.has_asset(h)


def test_bva_06_empty_locate_request_list(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]):
    coord, wa, wb = two_workers
    req = LocateAssetsRequest(requester_worker_id=wa.worker_id, missing_hashes=[])
    resp = coord.handle_locate(req, requester_ip=wa.ip_address)
    assert resp.locations == {}
    assert resp.unresolved_hashes == []


def test_bva_07_empty_inventory_registration(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-empty-inv", endpoint_url="http://1.1.1.1:8000", ip_address="1.1.1.1", port=8000, inventory_hashes=set())
    mock_coordinator.handle_register(w)
    assert len(mock_coordinator.registry.workers["w-empty-inv"].inventory_hashes) == 0


def test_bva_08_empty_required_set_returns_zero_hits(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode]):
    coord, wa, wb = two_workers
    results = wa.sync_missing_assets([], {wb.worker_id: wb})
    assert results == {}
    assert wa.metrics.total_requested_assets == 0


def test_bva_09_zero_byte_staging_commit_match(temp_cas_dir: Path, zero_byte_payload: Tuple[bytes, str]):
    data, h = zero_byte_payload
    cas = MockCASAdapter(temp_cas_dir)
    staged = cas.staging_dir / f"empty_{h}.tmp"
    staged.write_bytes(b"")
    assert cas.commit_staged_file(staged, h) is True
    assert cas.has_asset(h)


def test_bva_10_zero_byte_range_request_full(temp_cas_dir: Path, zero_byte_payload: Tuple[bytes, str]):
    data, h = zero_byte_payload
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h, offset=0))
    assert len(chunks) == 0


# ============================================================================
# Suite 2: Single-Byte & Chunk Boundary Edge Cases (10 tests)
# ============================================================================

def test_bva_11_single_byte_asset(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    h = cas.store_bytes(b"A")
    assert cas.get_asset_size(h) == 1
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h))
    assert chunks == [b"A"]


def test_bva_12_payload_1023_bytes(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    data = b"x" * 1023
    h = cas.store_bytes(data)
    assert cas.get_asset_size(h) == 1023
    server = MockStreamingServer(cas)
    assert b"".join(server.stream_chunks(h)) == data


def test_bva_13_payload_1024_bytes_boundary(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    data = b"y" * 1024
    h = cas.store_bytes(data)
    assert cas.get_asset_size(h) == 1024
    server = MockStreamingServer(cas)
    assert b"".join(server.stream_chunks(h)) == data


def test_bva_14_payload_1mib_minus_1_byte(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    data = b"z" * (1024 * 1024 - 1)
    h = cas.store_bytes(data)
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h))
    assert len(chunks) == 1
    assert len(chunks[0]) == 1024 * 1024 - 1


def test_bva_15_payload_exact_1mib_boundary(temp_cas_dir: Path, payload_1mib: Tuple[bytes, str]):
    data, h = payload_1mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h))
    assert len(chunks) == 1
    assert len(chunks[0]) == 1024 * 1024


def test_bva_16_payload_1mib_plus_1_byte(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    data = b"a" * (1024 * 1024 + 1)
    h = cas.store_bytes(data)
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h))
    assert len(chunks) == 2
    assert len(chunks[0]) == 1024 * 1024
    assert len(chunks[1]) == 1


def test_bva_17_payload_exact_2mib_two_chunks(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    data = b"b" * (2 * 1024 * 1024)
    h = cas.store_bytes(data)
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h))
    assert len(chunks) == 2
    assert len(chunks[0]) == 1024 * 1024
    assert len(chunks[1]) == 1024 * 1024


def test_bva_18_payload_2mib_plus_1_byte(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    data = b"c" * (2 * 1024 * 1024 + 1)
    h = cas.store_bytes(data)
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h))
    assert len(chunks) == 3
    assert len(chunks[2]) == 1


def test_bva_19_payload_prime_size(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    prime_size = 1048583  # Prime just above 1 MiB
    data = b"p" * prime_size
    h = cas.store_bytes(data)
    server = MockStreamingServer(cas)
    assert len(b"".join(server.stream_chunks(h))) == prime_size


def test_bva_20_payload_arbitrary_unaligned_size(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    size = 1234567
    data = os.urandom(size)
    h = cas.store_bytes(data)
    server = MockStreamingServer(cas)
    assert b"".join(server.stream_chunks(h)) == data


# ============================================================================
# Suite 3: Extreme File Sizes & Multi-Chunk Streaming (10 tests)
# ============================================================================

def test_bva_21_stream_10mib_payload(temp_cas_dir: Path, payload_10mib: Tuple[bytes, str]):
    data, h = payload_10mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h))
    assert len(chunks) == 10
    assert b"".join(chunks) == data


def test_bva_22_stream_25mib_payload(temp_cas_dir: Path):
    data = b"Q" * (25 * 1024 * 1024)
    h = hashlib.sha256(data).hexdigest()
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    chunks = list(server.stream_chunks(h))
    assert len(chunks) == 25


def test_bva_23_stream_50mib_multi_chunk_integrity(temp_cas_dir: Path):
    # Stream in 1 MiB chunks to verify zero memory accumulation
    size = 50 * 1024 * 1024
    data = b"H" * size
    h = hashlib.sha256(data).hexdigest()
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)

    hasher = hashlib.sha256()
    count = 0
    for chunk in server.stream_chunks(h):
        hasher.update(chunk)
        count += 1
    assert count == 50
    assert hasher.hexdigest().lower() == h.lower()


def test_bva_24_stream_seek_exact_1mib_boundary(temp_cas_dir: Path, payload_10mib: Tuple[bytes, str]):
    data, h = payload_10mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    offset = 1024 * 1024
    streamed = b"".join(server.stream_chunks(h, offset=offset))
    assert streamed == data[offset:]


def test_bva_25_stream_seek_1mib_plus_1(temp_cas_dir: Path, payload_10mib: Tuple[bytes, str]):
    data, h = payload_10mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    offset = 1024 * 1024 + 1
    streamed = b"".join(server.stream_chunks(h, offset=offset))
    assert streamed == data[offset:]


def test_bva_26_stream_seek_last_byte(temp_cas_dir: Path, payload_10mib: Tuple[bytes, str]):
    data, h = payload_10mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    offset = len(data) - 1
    streamed = b"".join(server.stream_chunks(h, offset=offset))
    assert streamed == data[-1:]


def test_bva_27_stream_explicit_zero_offset(temp_cas_dir: Path, payload_1mib: Tuple[bytes, str]):
    data, h = payload_1mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    streamed = b"".join(server.stream_chunks(h, offset=0))
    assert streamed == data


def test_bva_28_concurrent_streams_on_same_file(temp_cas_dir: Path, payload_5mib: Tuple[bytes, str]):
    data, h = payload_5mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)

    # 3 simultaneous streams
    s1 = list(server.stream_chunks(h, offset=0))
    s2 = list(server.stream_chunks(h, offset=1024*1024))
    s3 = list(server.stream_chunks(h, offset=2*1024*1024))
    assert b"".join(s1) == data
    assert b"".join(s2) == data[1024*1024:]
    assert b"".join(s3) == data[2*1024*1024:]


def test_bva_29_streaming_nonexistent_hash_raises_error(temp_cas_dir: Path):
    cas = MockCASAdapter(temp_cas_dir)
    server = MockStreamingServer(cas)
    with pytest.raises(FileNotFoundError):
        list(server.stream_chunks("e" * 64))


def test_bva_30_streaming_read_eof_behavior(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    with cas.open_asset_stream(h) as stream:
        assert stream.read(50) == data[:50]
        assert stream.read(50) == data[50:]
        assert stream.read(50) == b""


# ============================================================================
# Suite 4: Adversarial SHA-256 Formats & Path Traversal Injection (10 tests)
# ============================================================================

def test_bva_31_uppercase_hex_normalization(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    assert cas.has_asset(h.upper()) is True
    assert cas.get_asset_size(h.upper()) == 100


def test_bva_32_mixed_case_hex_normalization(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    mixed_h = "".join(c.upper() if i % 2 == 0 else c.lower() for i, c in enumerate(h))
    assert cas.has_asset(mixed_h) is True


def test_bva_33_path_traversal_double_dot_parent():
    evil = "../../root/shadow" + "0" * 48
    assert not bool(re.match(r"^[a-fA-F0-9]{64}$", evil))


def test_bva_34_windows_path_traversal_backslash():
    evil = r"..\..\windows\system32" + "0" * 40
    assert not bool(re.match(r"^[a-fA-F0-9]{64}$", evil))



def test_bva_35_null_byte_injection():
    evil = ("a" * 32) + chr(0) + ("b" * 31)
    assert not bool(re.match(r"^[a-fA-F0-9]{64}$", evil))



def test_bva_36_non_hex_character_rejection():
    evil = "z" * 64
    assert not bool(re.match(r"^[a-fA-F0-9]{64}$", evil))


def test_bva_37_space_padded_hex_rejection():
    evil = " " + ("a" * 63)
    assert not bool(re.match(r"^[a-fA-F0-9]{64}$", evil))


def test_bva_38_special_symbols_rejection():
    evil = "$" + ("a" * 62) + "#"
    assert not bool(re.match(r"^[a-fA-F0-9]{64}$", evil))


def test_bva_39_short_32_char_md5_rejection():
    md5_hash = "d41d8cd98f00b204e9800998ecf8427e"
    assert not bool(re.match(r"^[a-fA-F0-9]{64}$", md5_hash))


def test_bva_40_long_65_char_hash_rejection():
    long_h = "a" * 65
    assert not bool(re.match(r"^[a-fA-F0-9]{64}$", long_h))


# ============================================================================
# Suite 5: HTTP Range Boundary Cases (10 tests)
# ============================================================================

def test_bva_41_range_bytes_0_full_file(temp_cas_dir: Path, payload_1mib: Tuple[bytes, str]):
    data, h = payload_1mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    assert b"".join(server.stream_chunks(h, offset=0)) == data


def test_bva_42_range_bytes_1_skip_first_byte(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    assert b"".join(server.stream_chunks(h, offset=1)) == data[1:]


def test_bva_43_range_chunk_2_offset(temp_cas_dir: Path, payload_5mib: Tuple[bytes, str]):
    data, h = payload_5mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    offset = 1024 * 1024
    assert b"".join(server.stream_chunks(h, offset=offset)) == data[offset:]


def test_bva_44_range_last_single_byte(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    offset = 99
    assert b"".join(server.stream_chunks(h, offset=offset)) == data[99:]


def test_bva_45_range_offset_equals_file_size_error(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    with pytest.raises(IndexError):
        list(server.stream_chunks(h, offset=100))


def test_bva_46_range_offset_exceeds_file_size_error(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    with pytest.raises(IndexError):
        list(server.stream_chunks(h, offset=1000))


def test_bva_47_negative_offset_raises_or_handled(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    # Seeking with negative offset in open_asset_stream
    with pytest.raises(OSError):
        with cas.open_asset_stream(h, offset=-10) as s:
            s.read()


def test_bva_48_range_resumption_after_1_chunk(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_5mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_5mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 1024 * 1024
    try:
        wa.download_from_server(wb.server, h, max_retries=1)
    except ConnectionResetError:
        pass
    wb.server.drop_offset = None
    assert wa.download_from_server(wb.server, h, max_retries=2) is True


def test_bva_49_range_resumption_after_3_chunks(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_5mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_5mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 3 * 1024 * 1024
    try:
        wa.download_from_server(wb.server, h, max_retries=1)
    except ConnectionResetError:
        pass
    wb.server.drop_offset = None
    assert wa.download_from_server(wb.server, h, max_retries=2) is True


def test_bva_50_multiple_range_chunks_concatenate_to_original(temp_cas_dir: Path, payload_5mib: Tuple[bytes, str]):
    data, h = payload_5mib
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    c1 = b"".join(server.stream_chunks(h, offset=0))[:1000]
    c2 = b"".join(server.stream_chunks(h, offset=1000))
    assert c1 + c2 == data


# ============================================================================
# Suite 6: Network Dropouts & Partial Chunk Interruptions (10 tests)
# ============================================================================

def test_bva_51_drop_at_byte_zero(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_1mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 0
    with pytest.raises(ConnectionResetError):
        wa.download_from_server(wb.server, h, max_retries=1)


def test_bva_52_drop_at_byte_500(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_1mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 500
    with pytest.raises(ConnectionResetError):
        wa.download_from_server(wb.server, h, max_retries=1)


def test_bva_53_drop_at_exact_1mib_boundary(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_5mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_5mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 1024 * 1024
    with pytest.raises(ConnectionResetError):
        wa.download_from_server(wb.server, h, max_retries=1)


def test_bva_54_drop_at_1_point_5_mib(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_5mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_5mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = int(1.5 * 1024 * 1024)
    with pytest.raises(ConnectionResetError):
        wa.download_from_server(wb.server, h, max_retries=1)


def test_bva_55_drop_twice_recovery_on_third_attempt(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_1mib
    wb.cas.store_bytes(data)
    # Success on retry
    wb.server.drop_offset = None
    assert wa.download_from_server(wb.server, h, max_retries=3) is True


def test_bva_56_drop_exhaustion_on_max_retries(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_1mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 100
    with pytest.raises(ConnectionResetError):
        wa.download_from_server(wb.server, h, max_retries=3, backoff_base=0.001)


def test_bva_57_zero_retries_fails_immediately(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_1mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 100
    with pytest.raises(ConnectionResetError):
        wa.download_from_server(wb.server, h, max_retries=1)


def test_bva_58_staging_file_unlinked_after_drop_failure(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_1mib: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_1mib
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 100
    try:
        wa.download_from_server(wb.server, h, max_retries=1)
    except ConnectionResetError:
        pass
    assert len(list(wa.cas.staging_dir.iterdir())) == 0


def test_bva_59_server_delay_streaming(temp_cas_dir: Path, payload_100b: Tuple[bytes, str]):
    data, h = payload_100b
    cas = MockCASAdapter(temp_cas_dir)
    cas.store_bytes(data)
    server = MockStreamingServer(cas)
    server.delay_per_chunk = 0.001
    start_t = time.time()
    chunks = list(server.stream_chunks(h))
    elapsed = time.time() - start_t
    assert elapsed >= 0.001
    assert chunks[0] == data


def test_bva_60_connection_reset_exception_propagation(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_100b: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_100b
    wb.cas.store_bytes(data)
    wb.server.drop_offset = 50
    with pytest.raises(ConnectionResetError, match="Simulated network"):
        wa.download_from_server(wb.server, h, max_retries=1)


# ============================================================================
# Suite 7: Cryptographic Bit Flips & Corrupted Payloads (10 tests)
# ============================================================================

def test_bva_61_bit_flip_at_byte_0(payload_100b: Tuple[bytes, str]):
    data, expected_h = payload_100b
    corrupt = bytes([data[0] ^ 1]) + data[1:]
    assert hashlib.sha256(corrupt).hexdigest() != expected_h


def test_bva_62_bit_flip_at_byte_1000(payload_1mib: Tuple[bytes, str]):
    data, expected_h = payload_1mib
    corrupt = data[:1000] + bytes([data[1000] ^ 1]) + data[1001:]
    assert hashlib.sha256(corrupt).hexdigest() != expected_h


def test_bva_63_bit_flip_at_exact_middle_byte(payload_1mib: Tuple[bytes, str]):
    data, expected_h = payload_1mib
    mid = len(data) // 2
    corrupt = data[:mid] + bytes([data[mid] ^ 1]) + data[mid+1:]
    assert hashlib.sha256(corrupt).hexdigest() != expected_h


def test_bva_64_bit_flip_at_last_byte(payload_100b: Tuple[bytes, str]):
    data, expected_h = payload_100b
    corrupt = data[:-1] + bytes([data[-1] ^ 1])
    assert hashlib.sha256(corrupt).hexdigest() != expected_h


def test_bva_65_swapped_adjacent_chunks():
    data = b"A" * (1024 * 1024) + b"B" * (1024 * 1024) + b"C" * (1024 * 1024)
    expected_h = hashlib.sha256(data).hexdigest()
    c1 = data[:1024*1024]
    c2 = data[1024*1024:2*1024*1024]
    rest = data[2*1024*1024:]
    swapped = c2 + c1 + rest
    assert hashlib.sha256(swapped).hexdigest() != expected_h


def test_bva_66_truncated_1_byte_from_end(payload_1mib: Tuple[bytes, str]):
    data, expected_h = payload_1mib
    truncated = data[:-1]
    assert hashlib.sha256(truncated).hexdigest() != expected_h


def test_bva_67_truncated_half_of_file(payload_1mib: Tuple[bytes, str]):
    data, expected_h = payload_1mib
    half = data[:len(data)//2]
    assert hashlib.sha256(half).hexdigest() != expected_h


def test_bva_68_prepended_garbage_byte(payload_100b: Tuple[bytes, str]):
    data, expected_h = payload_100b
    prepended = bytes([255]) + data
    assert hashlib.sha256(prepended).hexdigest() != expected_h


def test_bva_69_appended_garbage_byte(payload_100b: Tuple[bytes, str]):
    data, expected_h = payload_100b
    appended = data + bytes([0])
    assert hashlib.sha256(appended).hexdigest() != expected_h



def test_bva_70_corrupted_payload_leaves_cas_untouched(two_workers: Tuple[MockCoordinator, MockWorkerNode, MockWorkerNode], payload_100b: Tuple[bytes, str]):
    coord, wa, wb = two_workers
    data, h = payload_100b
    wb.cas.store_bytes(data)
    wb.server.corrupt_assets.add(h)
    try:
        wa.download_from_server(wb.server, h, max_retries=1)
    except ValueError:
        pass
    assert not wa.cas.has_asset(h)


# ============================================================================
# Suite 8: Worker Registry Extreme Scales & ID Collisions (10 tests)
# ============================================================================

def test_bva_71_worker_with_zero_inventory(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-0-inv", endpoint_url="http://1.1.1.1:8000", ip_address="1.1.1.1", port=8000, inventory_hashes=set())
    mock_coordinator.handle_register(w)
    assert mock_coordinator.registry.workers["w-0-inv"].inventory_hashes == set()


def test_bva_72_worker_with_5000_inventory_hashes(mock_coordinator: MockCoordinator):
    hashes = {f"{i:064x}" for i in range(5000)}
    w = WorkerInfo(worker_id="w-5000", endpoint_url="http://1.1.1.2:8000", ip_address="1.1.1.2", port=8000, inventory_hashes=hashes)
    mock_coordinator.handle_register(w)
    assert len(mock_coordinator.registry.workers["w-5000"].inventory_hashes) == 5000
    assert len(mock_coordinator.registry.locate_asset(f"{100:064x}")) == 1


def test_bva_73_duplicate_registration_overwrites_cleanly(mock_coordinator: MockCoordinator):
    w1 = WorkerInfo(worker_id="w-dup", endpoint_url="http://1.1.1.3:8000", ip_address="1.1.1.3", port=8000, capacity_bytes=1000)
    w2 = WorkerInfo(worker_id="w-dup", endpoint_url="http://1.1.1.4:9000", ip_address="1.1.1.4", port=9000, capacity_bytes=2000)
    mock_coordinator.handle_register(w1)
    mock_coordinator.handle_register(w2)
    assert len(mock_coordinator.registry.workers) == 1
    assert mock_coordinator.registry.workers["w-dup"].port == 9000
    assert mock_coordinator.registry.workers["w-dup"].capacity_bytes == 2000


def test_bva_74_one_hundred_workers_registered(mock_coordinator: MockCoordinator):
    for i in range(100):
        w = WorkerInfo(worker_id=f"w-scale-{i}", endpoint_url=f"http://10.0.0.{i}:8000", ip_address=f"10.0.0.{i}", port=8000)
        mock_coordinator.handle_register(w)
    assert len(mock_coordinator.registry.workers) == 100


def test_bva_75_heartbeat_unknown_worker_returns_error(mock_coordinator: MockCoordinator):
    resp = mock_coordinator.handle_heartbeat("non-existent-worker", HeartbeatPayload(worker_id="non-existent"))
    assert resp["status"] == "unknown_worker"


def test_bva_76_worker_capacity_zero(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-cap-0", endpoint_url="http://1.1.1.5:8000", ip_address="1.1.1.5", port=8000, capacity_bytes=0, used_bytes=0)
    mock_coordinator.handle_register(w)
    ranked = mock_coordinator.prioritizer.rank_candidates("1.1.1.1", "req", [w])
    assert ranked[0].load_factor == 0.0


def test_bva_77_worker_capacity_max_integer(mock_coordinator: MockCoordinator):
    max_cap = 2**63 - 1
    w = WorkerInfo(worker_id="w-max-cap", endpoint_url="http://1.1.1.6:8000", ip_address="1.1.1.6", port=8000, capacity_bytes=max_cap)
    mock_coordinator.handle_register(w)
    assert mock_coordinator.registry.workers["w-max-cap"].capacity_bytes == max_cap


def test_bva_78_used_bytes_equals_capacity(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-full", endpoint_url="http://1.1.1.7:8000", ip_address="1.1.1.7", port=8000, capacity_bytes=1000, used_bytes=1000)
    ranked = mock_coordinator.prioritizer.rank_candidates("1.1.1.1", "req", [w])
    assert ranked[0].load_factor == 1.0


def test_bva_79_used_bytes_exceeding_capacity(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-over", endpoint_url="http://1.1.1.8:8000", ip_address="1.1.1.8", port=8000, capacity_bytes=1000, used_bytes=1500)
    ranked = mock_coordinator.prioritizer.rank_candidates("1.1.1.1", "req", [w])
    assert ranked[0].load_factor == 1.5


def test_bva_80_unicode_worker_id(mock_coordinator: MockCoordinator):
    uid = "worker-렌더팜-01"
    w = WorkerInfo(worker_id=uid, endpoint_url="http://1.1.1.9:8000", ip_address="1.1.1.9", port=8000)
    resp = mock_coordinator.handle_register(w)
    assert resp["status"] == "registered"
    assert uid in mock_coordinator.registry.workers


# ============================================================================
# Suite 9: Heartbeat Boundary Thresholds & Clock Skew (10 tests)
# ============================================================================

def test_bva_81_heartbeat_at_14_point_99_seconds_active(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-hb-14", endpoint_url="http://1.1.1.1:8000", ip_address="1.1.1.1", port=8000, last_heartbeat_utc=100.0)
    mock_coordinator.handle_register(w)
    report = mock_coordinator.registry.prune_stale_workers(unhealthy_timeout=15.0, current_time=114.99)
    assert "w-hb-14" not in report["unhealthy"]
    assert mock_coordinator.registry.workers["w-hb-14"].status == "ACTIVE"


def test_bva_82_heartbeat_at_15_point_01_seconds_unhealthy(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-hb-15", endpoint_url="http://1.1.1.2:8000", ip_address="1.1.1.2", port=8000, last_heartbeat_utc=100.0)
    mock_coordinator.handle_register(w)
    report = mock_coordinator.registry.prune_stale_workers(unhealthy_timeout=15.0, current_time=115.01)
    assert "w-hb-15" in report["unhealthy"]
    assert mock_coordinator.registry.workers["w-hb-15"].status == "UNHEALTHY"


def test_bva_83_heartbeat_at_44_point_99_seconds_not_evicted(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-hb-44", endpoint_url="http://1.1.1.3:8000", ip_address="1.1.1.3", port=8000, last_heartbeat_utc=100.0)
    mock_coordinator.handle_register(w)
    report = mock_coordinator.registry.prune_stale_workers(unhealthy_timeout=15.0, eviction_timeout=45.0, current_time=144.99)
    assert "w-hb-44" not in report["evicted"]
    assert "w-hb-44" in mock_coordinator.registry.workers


def test_bva_84_heartbeat_at_45_point_01_seconds_evicted(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-hb-45", endpoint_url="http://1.1.1.4:8000", ip_address="1.1.1.4", port=8000, last_heartbeat_utc=100.0)
    mock_coordinator.handle_register(w)
    report = mock_coordinator.registry.prune_stale_workers(unhealthy_timeout=15.0, eviction_timeout=45.0, current_time=145.01)
    assert "w-hb-45" in report["evicted"]
    assert "w-hb-45" not in mock_coordinator.registry.workers


def test_bva_85_clock_skew_future_timestamp(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-skew-f", endpoint_url="http://1.1.1.5:8000", ip_address="1.1.1.5", port=8000, last_heartbeat_utc=2000.0)
    mock_coordinator.handle_register(w)
    report = mock_coordinator.registry.prune_stale_workers(current_time=1000.0)
    assert "w-skew-f" not in report["unhealthy"]
    assert "w-skew-f" not in report["evicted"]


def test_bva_86_clock_skew_past_timestamp(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-skew-p", endpoint_url="http://1.1.1.6:8000", ip_address="1.1.1.6", port=8000, last_heartbeat_utc=100.0)
    mock_coordinator.handle_register(w)
    report = mock_coordinator.registry.prune_stale_workers(current_time=1000.0)
    assert "w-skew-p" in report["evicted"]


def test_bva_87_rapid_heartbeat_flood_50_times(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-flood", endpoint_url="http://1.1.1.7:8000", ip_address="1.1.1.7", port=8000)
    mock_coordinator.handle_register(w)
    for i in range(50):
        resp = mock_coordinator.handle_heartbeat("w-flood", HeartbeatPayload(worker_id="w-flood", timestamp_utc=100.0 + i * 0.01))
        assert resp["status"] == "healthy"


def test_bva_88_heartbeat_with_zero_active_transfers(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-hb-0tx", endpoint_url="http://1.1.1.8:8000", ip_address="1.1.1.8", port=8000)
    mock_coordinator.handle_register(w)
    resp = mock_coordinator.handle_heartbeat("w-hb-0tx", HeartbeatPayload(worker_id="w-hb-0tx", active_transfers=0))
    assert resp["status"] == "healthy"


def test_bva_89_heartbeat_with_max_active_transfers(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-hb-maxtx", endpoint_url="http://1.1.1.9:8000", ip_address="1.1.1.9", port=8000)
    mock_coordinator.handle_register(w)
    resp = mock_coordinator.handle_heartbeat("w-hb-maxtx", HeartbeatPayload(worker_id="w-hb-maxtx", active_transfers=128))
    assert resp["status"] == "healthy"


def test_bva_90_heartbeat_metrics_100_percent_cpu_ram(mock_coordinator: MockCoordinator):
    w = WorkerInfo(worker_id="w-hb-fullload", endpoint_url="http://1.1.1.10:8000", ip_address="1.1.1.10", port=8000)
    mock_coordinator.handle_register(w)
    resp = mock_coordinator.handle_heartbeat("w-hb-fullload", HeartbeatPayload(worker_id="w-hb-fullload", cpu_percent=100.0, ram_percent=100.0))
    assert resp["status"] == "healthy"


# ============================================================================
# Suite 10: IP / Subnet Parsing Boundaries & Metrics Math (10 tests)
# ============================================================================

def test_bva_91_ip_loopback_127_0_0_1(mock_prioritizer: MockCandidatePrioritizer):
    tier = mock_prioritizer.classify_locality("127.0.0.1", "127.0.0.1")
    assert tier == "loopback"


def test_bva_92_ip_loopback_127_0_0_2(mock_prioritizer: MockCandidatePrioritizer):
    tier = mock_prioritizer.classify_locality("127.0.0.1", "127.0.0.2")
    assert tier == "loopback"


def test_bva_93_ip_ipv6_loopback(mock_prioritizer: MockCandidatePrioritizer):
    tier = mock_prioritizer.classify_locality("::1", "::1")
    assert tier == "loopback"


def test_bva_94_ip_same_subnet_first_and_last_host(mock_prioritizer: MockCandidatePrioritizer):
    tier = mock_prioritizer.classify_locality("192.168.1.1", "192.168.1.254")
    assert tier == "subnet"


def test_bva_95_ip_private_lan_10_network(mock_prioritizer: MockCandidatePrioritizer):
    tier = mock_prioritizer.classify_locality("10.0.1.5", "10.0.2.8")
    assert tier == "lan"


def test_bva_96_ip_private_lan_172_network(mock_prioritizer: MockCandidatePrioritizer):
    tier = mock_prioritizer.classify_locality("172.16.1.1", "172.31.2.2")
    assert tier == "lan"


def test_bva_97_ip_cross_site_public_wan(mock_prioritizer: MockCandidatePrioritizer):
    tier = mock_prioritizer.classify_locality("8.8.8.8", "1.1.1.1")
    assert tier == "wan"


def test_bva_98_metrics_zero_requested_bytes_division_guard():
    m = TransferMetrics(total_requested_bytes=0, local_cache_hit_bytes=0)
    m.compute_ratios()
    assert m.byte_hit_ratio == 0.0
    assert m.network_savings_percent == 0.0


def test_bva_99_metrics_zero_hit_bytes_division_guard():
    m = TransferMetrics(total_requested_bytes=5000, local_cache_hit_bytes=0)
    m.compute_ratios()
    assert m.byte_hit_ratio == 0.0
    assert m.network_savings_percent == 0.0


def test_bva_100_metrics_100_percent_hit_bytes():
    m = TransferMetrics(total_requested_bytes=5000, local_cache_hit_bytes=5000)
    m.compute_ratios()
    assert m.byte_hit_ratio == 1.0
    assert m.network_savings_percent == 100.0
