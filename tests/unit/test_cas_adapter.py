"""Unit tests for CASAdapter protocol and LocalCASAdapter implementation."""
from __future__ import annotations

import hashlib
import os
import threading
import time
from pathlib import Path
from typing import Set

import pytest

from aidars.distributed.cas_adapter import CASAdapter, LocalCASAdapter


@pytest.fixture
def temp_cas(tmp_path: Path) -> LocalCASAdapter:
    cas_dir = tmp_path / "cas_root"
    return LocalCASAdapter(cas_dir=cas_dir, chunk_size=4096)


# ============================================================================
# Protocol Conformance & Initialization
# ============================================================================


def test_protocol_conformance(temp_cas: LocalCASAdapter):
    assert isinstance(temp_cas, CASAdapter)


def test_directory_initialization(tmp_path: Path):
    cas_dir = tmp_path / "custom_cas"
    staging_dir = tmp_path / "custom_staging"
    adapter = LocalCASAdapter(cas_dir=cas_dir, staging_dir=staging_dir)

    assert adapter.objects_dir.exists()
    assert adapter.objects_dir.is_dir()
    assert adapter.staging_dir.exists()
    assert adapter.staging_dir.is_dir()


# ============================================================================
# Store & Existence Checks
# ============================================================================


def test_store_bytes_and_has_asset(temp_cas: LocalCASAdapter):
    data = b"Hello AIDAR Distributed CAS Asset!"
    expected_hash = hashlib.sha256(data).hexdigest()

    stored_hash = temp_cas.store_bytes(data)
    assert stored_hash == expected_hash

    assert temp_cas.has_asset(expected_hash) is True
    # Case insensitivity
    assert temp_cas.has_asset(expected_hash.upper()) is True

    # Check non-existent hash
    fake_hash = "0" * 64
    assert temp_cas.has_asset(fake_hash) is False

    # Check invalid hash format
    assert temp_cas.has_asset("invalid-hash") is False
    assert temp_cas.has_asset("../../etc/passwd") is False


def test_zero_byte_asset(temp_cas: LocalCASAdapter):
    data = b""
    expected_hash = hashlib.sha256(data).hexdigest()  # e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855

    stored_hash = temp_cas.store_bytes(data)
    assert stored_hash == expected_hash
    assert temp_cas.has_asset(expected_hash) is True
    assert temp_cas.get_asset_size(expected_hash) == 0


# ============================================================================
# Missing-Set Resolution
# ============================================================================


def test_get_missing_hashes(temp_cas: LocalCASAdapter):
    data_a = b"Asset A payload"
    data_b = b"Asset B payload"
    hash_a = temp_cas.store_bytes(data_a)
    hash_b = temp_cas.store_bytes(data_b)
    hash_c = hashlib.sha256(b"Asset C payload").hexdigest()
    hash_d = hashlib.sha256(b"Asset D payload").hexdigest()

    # All cached
    assert temp_cas.get_missing_hashes([hash_a, hash_b]) == set()

    # Partial missing
    missing = temp_cas.get_missing_hashes([hash_a, hash_b, hash_c, hash_d])
    assert missing == {hash_c, hash_d}

    # Empty list
    assert temp_cas.get_missing_hashes([]) == set()

    # Handles malformed hash safely
    missing_with_invalid = temp_cas.get_missing_hashes([hash_a, "not_a_valid_hash"])
    assert missing_with_invalid == {"not_a_valid_hash"}


# ============================================================================
# Stream Reading & Resumption Offsets
# ============================================================================


def test_open_asset_stream(temp_cas: LocalCASAdapter):
    data = b"0123456789ABCDEF" * 100
    expected_hash = temp_cas.store_bytes(data)

    # Full read
    with temp_cas.open_asset_stream(expected_hash, offset=0) as stream:
        content = stream.read()
        assert content == data

    # Partial read with offset
    offset = 50
    with temp_cas.open_asset_stream(expected_hash, offset=offset) as stream:
        content = stream.read()
        assert content == data[offset:]

    # Offset at exact EOF
    with temp_cas.open_asset_stream(expected_hash, offset=len(data)) as stream:
        content = stream.read()
        assert content == b""

    # Negative offset
    with pytest.raises(ValueError):
        temp_cas.open_asset_stream(expected_hash, offset=-1)

    # Offset exceeds file size
    with pytest.raises(IndexError):
        temp_cas.open_asset_stream(expected_hash, offset=len(data) + 10)

    # Non-existent asset
    with pytest.raises(FileNotFoundError):
        temp_cas.open_asset_stream("f" * 64)


# ============================================================================
# Staging & Atomic Commit
# ============================================================================


def test_commit_staged_file_success(temp_cas: LocalCASAdapter):
    data = b"Streaming binary content for atomic verification"
    expected_hash = hashlib.sha256(data).hexdigest()

    staging_file = temp_cas.create_staging_file(expected_hash, prefix="download")
    assert staging_file.parent == temp_cas.staging_dir
    staging_file.write_bytes(data)

    assert staging_file.exists()
    success = temp_cas.commit_staged_file(staging_file, expected_hash)
    assert success is True

    # Staging file was moved atomically
    assert not staging_file.exists()
    assert temp_cas.has_asset(expected_hash) is True

    # Verify content in CAS
    path = temp_cas.get_asset_path(expected_hash)
    assert path is not None
    assert path.read_bytes() == data


def test_commit_staged_file_corrupt_mismatch(temp_cas: LocalCASAdapter):
    corrupt_data = b"Corrupted chunk data from bad sender"
    expected_hash = hashlib.sha256(b"Legitimate uncorrupted data").hexdigest()

    staging_file = temp_cas.create_staging_file(expected_hash, prefix="corrupt")
    staging_file.write_bytes(corrupt_data)

    success = temp_cas.commit_staged_file(staging_file, expected_hash)
    assert success is False

    # Temporary file must be deleted immediately
    assert not staging_file.exists()
    assert temp_cas.has_asset(expected_hash) is False


def test_commit_nonexistent_file(temp_cas: LocalCASAdapter):
    fake_path = temp_cas.staging_dir / "does_not_exist.tmp"
    success = temp_cas.commit_staged_file(fake_path, "a" * 64)
    assert success is False


def test_commit_invalid_hash_deletes_staging(temp_cas: LocalCASAdapter):
    staging_file = temp_cas.create_staging_file()
    staging_file.write_bytes(b"some data")
    assert staging_file.exists()

    success = temp_cas.commit_staged_file(staging_file, "invalid_hash_string")
    assert success is False
    assert not staging_file.exists()


# ============================================================================
# File Store & Inventory
# ============================================================================


def test_store_file_import(temp_cas: LocalCASAdapter, tmp_path: Path):
    src_file = tmp_path / "source.bin"
    content = b"Content to import from external disk location"
    src_file.write_bytes(content)
    expected_hash = hashlib.sha256(content).hexdigest()

    # Import without explicit hash
    imported_hash = temp_cas.store_file(src_file)
    assert imported_hash == expected_hash
    assert temp_cas.has_asset(expected_hash) is True

    # Import with mismatched expected hash raises ValueError
    with pytest.raises(ValueError):
        temp_cas.store_file(src_file, expected_sha256="1" * 64)

    # Import nonexistent file raises FileNotFoundError
    with pytest.raises(FileNotFoundError):
        temp_cas.store_file(tmp_path / "missing.bin")


def test_inventory_and_stats(temp_cas: LocalCASAdapter):
    hashes: Set[str] = set()
    for i in range(5):
        h = temp_cas.store_bytes(f"Payload number {i}".encode())
        hashes.add(h)

    inventory = temp_cas.get_inventory_hashes()
    assert inventory == hashes
    assert temp_cas.list_cached_hashes() == hashes

    stats = temp_cas.get_cas_stats()
    assert stats["total_assets"] == 5
    assert stats["total_bytes"] > 0
    assert stats["cas_dir"] == str(temp_cas.cas_dir)


def test_delete_asset(temp_cas: LocalCASAdapter):
    data = b"Data to be deleted"
    h = temp_cas.store_bytes(data)
    assert temp_cas.has_asset(h) is True

    assert temp_cas.delete_asset(h) is True
    assert temp_cas.has_asset(h) is False
    assert temp_cas.delete_asset(h) is False
    assert temp_cas.delete_asset("invalid") is False


def test_prune_staging(temp_cas: LocalCASAdapter):
    f1 = temp_cas.create_staging_file()
    f1.write_bytes(b"temp1")
    f2 = temp_cas.create_staging_file()
    f2.write_bytes(b"temp2")

    assert f1.exists() and f2.exists()
    pruned = temp_cas.prune_staging()
    assert pruned == 2
    assert not f1.exists() and not f2.exists()


# ============================================================================
# Security: Path Traversal Prevention
# ============================================================================


def test_path_traversal_prevention(temp_cas: LocalCASAdapter):
    traversal_attacks = [
        "../../../../etc/passwd",
        "..\\..\\..\\windows\\system32\\cmd.exe",
        "0" * 63 + "/",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\x00extra",
        "../../" + "a" * 60,
    ]

    for attack in traversal_attacks:
        assert temp_cas.has_asset(attack) is False
        assert temp_cas.get_asset_path(attack) is None
        assert temp_cas.get_asset_size(attack) is None
        assert temp_cas.delete_asset(attack) is False
        with pytest.raises((ValueError, FileNotFoundError)):
            temp_cas.open_asset_stream(attack)


# ============================================================================
# Concurrency & Multi-threading
# ============================================================================


def test_concurrent_store_and_read(temp_cas: LocalCASAdapter):
    num_threads = 8
    items_per_thread = 10
    hashes = []
    lock = threading.Lock()

    def worker_task(thread_id: int):
        for i in range(items_per_thread):
            data = f"Thread-{thread_id}-Item-{i}".encode() * 50
            h = temp_cas.store_bytes(data)
            with lock:
                hashes.append((h, data))

            # Immediate read verify
            with temp_cas.open_asset_stream(h) as stream:
                read_back = stream.read()
                assert read_back == data

    threads = [threading.Thread(target=worker_task, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(hashes) == num_threads * items_per_thread
    inventory = temp_cas.get_inventory_hashes()
    assert len(inventory) == num_threads * items_per_thread
