"""Empirical Adversarial Test Suite for CASAdapter and Missing-Set Calculation.

Authored by Challenger 1 (Milestone 2 Gate 3).
Covers extreme edge cases:
1. High concurrency and multi-threaded contention on CASAdapter.
2. Empty sets, zero-byte assets, and uninitialized storage states.
3. Scale testing on huge missing sets (20,000+ hashes) and duplicate handling.
4. Malformed hashes, Windows reserved device names, Unicode, null bytes, and traversal.
5. Missing/corrupted local files, external filesystem deletions, directory injections.
6. Windows file locking and active stream concurrency.
"""
from __future__ import annotations

import concurrent.futures
import hashlib
import os
import random
import stat
import string
import tempfile
import threading
import time
from pathlib import Path
from typing import List, Set, Tuple

import pytest

from aidars.distributed.cas_adapter import CASAdapter, LocalCASAdapter
from aidars.distributed.models import validate_sha256_hex


def _generate_sha256(seed: int | str) -> str:
    return hashlib.sha256(f"challenger-m2-seed-{seed}".encode("utf-8")).hexdigest()


# ============================================================================
# 1. Concurrency & Contention Stress
# ============================================================================


class TestConcurrentCASContention:
    """Multi-threaded stress testing CASAdapter operations under extreme contention."""

    def test_concurrent_get_missing_hashes_under_mutations(self, tmp_path: Path):
        """Simultaneous get_missing_hashes calls while assets are being continuously added and deleted."""
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_contention")
        num_readers = 10
        num_writers = 5
        duration_seconds = 2.0

        catalog_data = [f"content-seed-{i}".encode("utf-8") for i in range(100)]
        catalog_hashes = [adapter.store_bytes(data) for data in catalog_data[:30]]
        # Remaining uncommitted hashes
        catalog_hashes += [hashlib.sha256(data).hexdigest() for data in catalog_data[30:]]

        stop_event = threading.Event()
        errors: List[str] = []

        def reader_worker(rid: int):
            while not stop_event.is_set():
                query = random.sample(catalog_hashes, 20)
                try:
                    missing = adapter.get_missing_hashes(query)
                    assert isinstance(missing, set)
                    assert missing.issubset(set(query))
                except Exception as exc:
                    errors.append(f"Reader {rid} error: {exc}")

        def writer_worker(wid: int):
            while not stop_event.is_set():
                h_idx = random.randint(0, 99)
                data = catalog_data[h_idx]
                h = hashlib.sha256(data).hexdigest()
                try:
                    if random.random() < 0.6:
                        adapter.store_bytes(data)
                    else:
                        adapter.delete_asset(h)
                except Exception as exc:
                    errors.append(f"Writer {wid} error: {exc}")

        threads = [threading.Thread(target=reader_worker, args=(i,)) for i in range(num_readers)]
        threads += [threading.Thread(target=writer_worker, args=(i,)) for i in range(num_writers)]

        for t in threads:
            t.start()

        time.sleep(duration_seconds)
        stop_event.set()

        for t in threads:
            t.join(timeout=5.0)

        assert not errors, f"Concurrent contention produced errors: {errors}"

    def test_delete_asset_while_stream_open_windows_lock(self, tmp_path: Path):
        """Deleting an asset while an active stream is reading it should handle OSError/PermissionError."""
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_lock_test")
        payload = b"Payload for file lock test" * 500
        h = adapter.store_bytes(payload)

        # Open stream and keep handle open
        stream = adapter.open_asset_stream(h)
        try:
            # Attempt to delete while stream is open
            # On Windows, deleting an open file raises PermissionError if not handled in delete_asset
            try:
                deleted = adapter.delete_asset(h)
            except OSError as exc:
                # Documented Windows filesystem lock limitation
                deleted = False
        finally:
            stream.close()

        # After closing stream, delete must succeed
        deleted_after = adapter.delete_asset(h)
        assert adapter.has_asset(h) is False


# ============================================================================
# 2. Empty Sets & Boundary Conditions
# ============================================================================


class TestEmptySetsAndBoundaries:
    """Behavior of CASAdapter under empty inputs, zero assets, and empty shards."""

    def test_empty_set_inputs(self, tmp_path: Path):
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_empty")
        assert adapter.get_missing_hashes([]) == set()
        assert adapter.get_missing_hashes(set()) == set()
        assert adapter.get_missing_hashes(tuple()) == set()
        assert adapter.get_missing_hashes(iter([])) == set()
        assert adapter.get_inventory_hashes() == set()
        assert adapter.list_cached_hashes() == set()

        stats = adapter.get_cas_stats()
        assert stats["total_assets"] == 0
        assert stats["total_bytes"] == 0

    def test_empty_and_corrupt_shard_directories(self, tmp_path: Path):
        """CAS directory containing empty shard folders or non-directory junk."""
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_shards")
        # Create empty shard directories
        for i in range(16):
            shard_name = f"{i:02x}"
            (adapter.objects_dir / shard_name).mkdir(parents=True, exist_ok=True)

        # Create junk file in objects root
        (adapter.objects_dir / "junk.txt").write_text("not a shard directory")

        # Create junk directory inside a shard
        (adapter.objects_dir / "00" / "not_a_file_dir").mkdir(parents=True, exist_ok=True)

        # Inventory enumeration should gracefully ignore junk without throwing
        assert adapter.get_inventory_hashes() == set()
        stats = adapter.get_cas_stats()
        assert stats["total_assets"] == 0

    def test_zero_byte_asset_handling(self, tmp_path: Path):
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_zero")
        empty_hash = hashlib.sha256(b"").hexdigest()

        # Store 0-byte file
        stored_h = adapter.store_bytes(b"")
        assert stored_h == empty_hash
        assert adapter.has_asset(empty_hash) is True
        assert adapter.get_asset_size(empty_hash) == 0

        # Stream 0-byte file
        with adapter.open_asset_stream(empty_hash, offset=0) as s:
            assert s.read() == b""

        # Offset 0 is valid, offset 1 raises IndexError
        with pytest.raises(IndexError):
            adapter.open_asset_stream(empty_hash, offset=1)


# ============================================================================
# 3. Huge Missing Sets & Duplicate Scaling
# ============================================================================


class TestHugeMissingSetsAndScaling:
    """Stress testing missing-set resolution at large scale."""

    def test_missing_set_large_duplicate_list(self, tmp_path: Path):
        """50,000 elements with 500 distinct hashes (testing duplicate handling)."""
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_dups")
        cached = []
        for i in range(250):
            h = adapter.store_bytes(f"cached-content-{i}".encode("utf-8"))
            cached.append(h)

        missing = [_generate_sha256(f"missing-{i}") for i in range(250)]
        pool = cached + missing

        # Create 50,000 list by sampling
        large_list = [random.choice(pool) for _ in range(50000)]

        t0 = time.perf_counter()
        result = adapter.get_missing_hashes(large_list)
        duration = time.perf_counter() - t0

        assert result == set(missing)
        assert len(result) == 250
        assert duration < 5.0, f"Large duplicate calculation took {duration:.4f}s"

    def test_all_cached_vs_all_missing_scale(self, tmp_path: Path):
        """500 all missing vs 500 all cached."""
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_extreme")
        cached = []
        for i in range(500):
            h = adapter.store_bytes(f"allcached-content-{i}".encode("utf-8"))
            cached.append(h)

        # All cached
        res_cached = adapter.get_missing_hashes(cached)
        assert res_cached == set()

        # All missing
        missing_all = [_generate_sha256(f"allmissing-{i}") for i in range(500)]
        res_missing = adapter.get_missing_hashes(missing_all)
        assert res_missing == set(missing_all)


# ============================================================================
# 4. Malformed Hashes, Traversal & Security
# ============================================================================


class TestMalformedHashesAndSecurity:
    """Adversarial malformed inputs, control chars, and Windows-specific attack vectors."""

    def test_windows_device_names_and_path_traversal(self, tmp_path: Path):
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_sec")
        bad_hashes = [
            "CON", "PRN", "AUX", "NUL", "COM1", "LPT1",
            "con.txt", "aux.bin", "nul.dat",
            "../CON", "..\\NUL",
            "../" * 10 + "windows/system32/cmd.exe",
            r"..\..\..\..\..\..\boot.ini",
            "0" * 62 + "\r\n",
            "0" * 63 + "\x00",
            "0" * 60 + "\t\n\v\f",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\x00extra",
            "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855/../../evil",
        ]

        for bad in bad_hashes:
            assert adapter.has_asset(bad) is False
            assert adapter.get_asset_path(bad) is None
            assert adapter.get_asset_size(bad) is None
            assert adapter.delete_asset(bad) is False
            with pytest.raises((ValueError, FileNotFoundError)):
                adapter.open_asset_stream(bad)

        # In get_missing_hashes, malformed strings are treated as unresolvable / missing
        missing = adapter.get_missing_hashes(bad_hashes)
        assert len(missing) == len(bad_hashes)

    def test_non_string_types_in_missing_hashes(self, tmp_path: Path):
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_types")
        non_strings = [12345, 99.99, None, True, False]
        # Should gracefully treat them as stringified unresolvable hashes without crashing
        missing = adapter.get_missing_hashes(non_strings)
        assert len(missing) == len(non_strings)


# ============================================================================
# 5. Missing Local Files & Filesystem Corruption
# ============================================================================


class TestMissingLocalFilesAndCorruption:
    """Resilience against filesystem anomalies and missing files."""

    def test_file_deleted_externally_after_creation(self, tmp_path: Path):
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_ghost")
        data = b"Ghost file payload"
        h = adapter.store_bytes(data)
        assert adapter.has_asset(h) is True

        # External actor deletes the file directly from objects folder
        path = adapter.get_asset_path(h)
        assert path is not None and path.exists()
        path.unlink()

        # has_asset must now return False
        assert adapter.has_asset(h) is False
        assert adapter.get_asset_size(h) is None
        with pytest.raises(FileNotFoundError):
            adapter.open_asset_stream(h)

        # get_missing_hashes should now report it as missing
        assert adapter.get_missing_hashes([h]) == {h}

    def test_corrupted_directory_replacing_asset_file(self, tmp_path: Path):
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_dir_inject")
        valid_h = _generate_sha256("dir_replace")
        target_path = adapter._get_path_for_hash(valid_h)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        # Create a directory with the hash name instead of a file
        target_path.mkdir(exist_ok=True)

        assert adapter.has_asset(valid_h) is False
        assert adapter.get_asset_path(valid_h) is None
        assert adapter.get_asset_size(valid_h) is None
        assert adapter.get_missing_hashes([valid_h]) == {valid_h}

    def test_staging_file_missing_on_commit(self, tmp_path: Path):
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_staging_miss")
        h = _generate_sha256("staging_miss")
        staging_file = adapter.create_staging_file(h)
        # Never wrote to staging_file (it doesn't exist)
        assert not staging_file.exists()
        assert adapter.commit_staged_file(staging_file, h) is False

    def test_store_file_missing_source(self, tmp_path: Path):
        adapter = LocalCASAdapter(cas_dir=tmp_path / "cas_store_miss")
        missing_src = tmp_path / "non_existent_source.bin"
        with pytest.raises(FileNotFoundError):
            adapter.store_file(missing_src)
