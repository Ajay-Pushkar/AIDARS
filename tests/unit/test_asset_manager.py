"""Unit tests for AssetManager (src/aidars/core/assets/manager.py).

AssetManager had no prior test coverage. These tests are scoped to the
minimal correctness fix: replacing duck-typed put()/UTF-8-decode logic
with a direct, typed call to LocalCASAdapter.store_bytes().
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from aidars.core.assets.manager import AssetManager
from aidars.distributed.cas_adapter import LocalCASAdapter


@pytest.fixture
def temp_cas(tmp_path: Path) -> LocalCASAdapter:
    return LocalCASAdapter(cas_dir=tmp_path / "cas_root", chunk_size=4096)


@pytest.mark.asyncio
async def test_upload_assets_stores_bytes_in_cas(temp_cas: LocalCASAdapter):
    data = b"AIDAR asset payload"
    manager = AssetManager(temp_cas)

    hashes = await manager.upload_assets({"asset-a": data})

    assert len(hashes) == 1
    stored_hash = next(iter(hashes))
    assert temp_cas.has_asset(stored_hash) is True
    with temp_cas.open_asset_stream(stored_hash) as stream:
        assert stream.read() == data


@pytest.mark.asyncio
async def test_upload_assets_returns_correct_sha256_hash(temp_cas: LocalCASAdapter):
    data = b"content used to verify hash correctness"
    expected_hash = hashlib.sha256(data).hexdigest()
    manager = AssetManager(temp_cas)

    hashes = await manager.upload_assets({"asset-b": data})

    assert hashes == {expected_hash}


@pytest.mark.asyncio
async def test_upload_assets_preserves_binary_non_utf8_bytes(temp_cas: LocalCASAdapter):
    # Bytes that are not valid UTF-8 (lone continuation/start bytes). The
    # previous implementation ran data.decode("utf-8", errors="ignore")
    # before ever writing anything, which would silently drop these bytes.
    data = bytes([0xFF, 0xFE, 0x00, 0x80, 0x81, 0xC0, 0xC1, 0x00, 0x01, 0x02])
    with pytest.raises(UnicodeDecodeError):
        data.decode("utf-8")  # sanity check: this payload really isn't valid UTF-8

    expected_hash = hashlib.sha256(data).hexdigest()
    manager = AssetManager(temp_cas)

    hashes = await manager.upload_assets({"asset-binary": data})

    assert hashes == {expected_hash}
    with temp_cas.open_asset_stream(expected_hash) as stream:
        assert stream.read() == data


@pytest.mark.asyncio
async def test_upload_assets_propagates_cas_failure(temp_cas: LocalCASAdapter, monkeypatch):
    def _raise(_data: bytes) -> str:
        raise RuntimeError("simulated CAS commit failure")

    monkeypatch.setattr(temp_cas, "store_bytes", _raise)
    manager = AssetManager(temp_cas)

    with pytest.raises(RuntimeError, match="simulated CAS commit failure"):
        await manager.upload_assets({"asset-c": b"irrelevant"})
