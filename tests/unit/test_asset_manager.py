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


# ============================================================================
# Phase 4B.1: file-based ingestion of real M4-resolved AssetRecords
# ============================================================================


def _resolve_real_m4_asset_record(tmp_path: Path):
    """Build a real scene with an on-disk referenced texture and resolve it
    through the actual M1-M4 pipeline (SceneEngine.resolve_required_assets,
    added in Phase 4A), returning the matching AssetRecord."""
    import json
    from aidars.adapters.blender.intelligence.scene_engine import SceneEngine
    from aidars.adapters.blender.packaging.models import AssetStatus

    texture_bytes = b"phase-4b1-real-m4-asset-bytes-0123456789"
    (tmp_path / "wood.png").write_bytes(texture_bytes)

    scene = {
        "metadata": {"name": "Phase4B1Scene", "frame_start": 1, "frame_end": 24, "fps": 24},
        "collections": [{"name": "Main", "id": "col-main", "parent": None}],
        "objects": [{
            "name": "Chair", "id": "obj-chair", "type": "MESH", "collection": "col-main",
            "visibility": {"hide_render": False, "hide_viewport": False},
            "materials": [{"name": "WoodMat", "shader": "Principled", "image_textures": ["wood.png"]}],
            "modifiers": [], "constraints": [], "animation": {"is_animated": False, "curves": []},
            "children": [], "referenced_assets": [],
        }],
        "lights": [],
        "materials": [{"name": "WoodMat", "shader": "Principled", "image_textures": ["wood.png"]}],
        "textures": [], "images": [],
    }
    scene_path = tmp_path / "scene.json"
    scene_path.write_text(json.dumps(scene), encoding="utf-8")

    engine = SceneEngine()
    source = engine.load_source(str(scene_path))
    snapshot = engine.analyze(source)
    graph = engine.build_dependency_graph(snapshot)
    records = engine.resolve_required_assets(snapshot, graph, str(scene_path))

    resolved = [r for r in records if r.status == AssetStatus.RESOLVED and r.sha256]
    assert len(resolved) > 0, "expected at least one physically resolved asset"
    # The same physical file can produce more than one AssetRecord (e.g. an
    # "image" node and a dependent "texture" node) -- both share the same
    # source_path/sha256, so any one of them is representative for this test.
    return resolved[0], texture_bytes


def test_upload_asset_file_stores_real_m4_resolved_asset(temp_cas: LocalCASAdapter, tmp_path: Path):
    """A real AssetRecord produced by M4's own resolver (source_path + sha256)
    ends up physically present in CAS via AssetManager.upload_asset_file."""
    record, texture_bytes = _resolve_real_m4_asset_record(tmp_path)
    manager = AssetManager(temp_cas)

    stored_hash = manager.upload_asset_file(record.source_path, record.sha256)

    assert temp_cas.has_asset(record.sha256) is True
    with temp_cas.open_asset_stream(record.sha256) as stream:
        assert stream.read() == texture_bytes


def test_upload_asset_file_returns_hash_matching_m4_sha256(temp_cas: LocalCASAdapter, tmp_path: Path):
    record, _ = _resolve_real_m4_asset_record(tmp_path)
    manager = AssetManager(temp_cas)

    stored_hash = manager.upload_asset_file(record.source_path, record.sha256)

    assert stored_hash == record.sha256


def test_upload_asset_file_propagates_hash_mismatch(temp_cas: LocalCASAdapter, tmp_path: Path):
    """A wrong expected hash must raise, never silently store under the
    wrong identity."""
    record, _ = _resolve_real_m4_asset_record(tmp_path)
    manager = AssetManager(temp_cas)
    wrong_hash = "0" * 64

    with pytest.raises(ValueError, match="checksum mismatch"):
        manager.upload_asset_file(record.source_path, wrong_hash)

    assert temp_cas.has_asset(wrong_hash) is False


def test_upload_asset_file_propagates_missing_file_error(temp_cas: LocalCASAdapter, tmp_path: Path):
    manager = AssetManager(temp_cas)
    missing_path = tmp_path / "does-not-exist.png"

    with pytest.raises(FileNotFoundError):
        manager.upload_asset_file(missing_path, "a" * 64)
