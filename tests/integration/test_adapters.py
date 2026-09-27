import hashlib
import json
import pytest
from pathlib import Path
from aidars.adapters.blender.adapter import BlenderAdapter
from aidars.adapters.llm.adapter import LLMAdapter
from aidars.adapters.ml_training.adapter import MLTrainingAdapter
from aidars.distributed.models import WorkloadSpec

BLENDER_FIXTURE_PATH = str(Path(__file__).resolve().parent.parent / "fixtures" / "blender_adapter_scene.json")


def _scene_with_texture_reference(texture_filename: str) -> dict:
    """A minimal scene (modeled on the PACKAGING_SCENE fixture used by
    tests/test_m4_smart_packaging.py) whose material references an external
    texture file, so PhysicalAssetResolver has something real to resolve."""
    return {
        "metadata": {"name": "AdapterAssetTest", "frame_start": 1, "frame_end": 24, "fps": 24},
        "collections": [{"name": "Main", "id": "col-main", "parent": None}],
        "objects": [{
            "name": "Chair", "id": "obj-chair", "type": "MESH", "collection": "col-main",
            "visibility": {"hide_render": False, "hide_viewport": False},
            "materials": [{"name": "WoodMat", "shader": "Principled", "image_textures": [texture_filename]}],
            "modifiers": [], "constraints": [], "animation": {"is_animated": False, "curves": []},
            "children": [], "referenced_assets": [],
        }],
        "lights": [],
        "materials": [{"name": "WoodMat", "shader": "Principled", "image_textures": [texture_filename]}],
        "textures": [], "images": [],
    }

def test_blender_adapter_produces_generic_workload():
    adapter = BlenderAdapter()
    request = {
        "input_path": BLENDER_FIXTURE_PATH,
        "frame_start": 1,
        "frame_end": 250,
        "requires_gpu": True
    }

    specs = adapter.evaluate_request(request)
    assert len(specs) == 2
    assert isinstance(specs[0], WorkloadSpec)
    assert specs[0].task_type == "blender_render"
    assert specs[0].requires_gpu is True
    assert specs[0].min_ram_bytes > 0

def test_llm_adapter_produces_generic_workload():
    adapter = LLMAdapter()
    request = {
        "model": "llama-2-7b",
        "prompt": "Hello world",
        "max_tokens": 128,
        "requires_gpu": True
    }
    
    specs = adapter.evaluate_request(request)
    assert len(specs) == 1
    assert isinstance(specs[0], WorkloadSpec)
    assert specs[0].task_type == "llm_inference"
    assert specs[0].requires_gpu is True
    assert specs[0].parameters["model"] == "llama-2-7b"

def test_ml_training_adapter_produces_generic_workload():
    adapter = MLTrainingAdapter()
    request = {
        "dataset": "mnist",
        "epochs": 5,
        "batch_size": 64,
        "requires_gpu": True
    }
    
    specs = adapter.evaluate_request(request)
    assert len(specs) == 1
    assert isinstance(specs[0], WorkloadSpec)
    assert specs[0].task_type == "ml_training"
    assert specs[0].requires_gpu is True
    assert specs[0].parameters["epochs"] == 5
    
def test_all_adapters_conform_to_same_contract():
    adapters = [
        (BlenderAdapter(), {"input_path": BLENDER_FIXTURE_PATH}),
        (LLMAdapter(), {"prompt": "b"}),
        (MLTrainingAdapter(), {"dataset": "c"})
    ]
    
    for adapter, req in adapters:
        specs = adapter.evaluate_request(req)
        for spec in specs:
            assert isinstance(spec, WorkloadSpec)
            assert spec.workload_id
            assert spec.task_type in ["blender_render", "llm_inference", "ml_training"]


def test_blender_adapter_populates_input_asset_hashes_for_resolved_asset(tmp_path):
    """Phase 4A: a scene with a real, on-disk referenced asset produces a
    non-empty input_asset_hashes on every resulting WorkloadSpec."""
    texture_bytes = b"fake-png-texture-bytes-for-phase-4a"
    (tmp_path / "wood.png").write_bytes(texture_bytes)
    scene_path = tmp_path / "scene.json"
    scene_path.write_text(json.dumps(_scene_with_texture_reference("wood.png")), encoding="utf-8")

    adapter = BlenderAdapter()
    specs = adapter.evaluate_request({
        "input_path": str(scene_path),
        "frame_start": 1,
        "frame_end": 24,
        "worker_count": 1,
    })

    assert len(specs) == 1
    assert len(specs[0].input_asset_hashes) > 0


def test_blender_adapter_input_asset_hashes_are_real_sha256(tmp_path):
    """Phase 4A: the populated hash is the actual SHA-256 of the resolved
    file on disk, not a placeholder or derived value."""
    texture_bytes = b"another-fake-texture-payload-98765"
    (tmp_path / "wood.png").write_bytes(texture_bytes)
    expected_hash = hashlib.sha256(texture_bytes).hexdigest()

    scene_path = tmp_path / "scene.json"
    scene_path.write_text(json.dumps(_scene_with_texture_reference("wood.png")), encoding="utf-8")

    adapter = BlenderAdapter()
    specs = adapter.evaluate_request({
        "input_path": str(scene_path),
        "frame_start": 1,
        "frame_end": 24,
        "worker_count": 1,
    })

    assert expected_hash in specs[0].input_asset_hashes


def test_blender_adapter_missing_asset_produces_no_fake_hash(tmp_path):
    """Phase 4A: a scene referencing a texture that does NOT exist on disk
    must not fabricate a hash for it, and must not raise."""
    scene_path = tmp_path / "scene.json"
    # wood.png is referenced but intentionally never created on disk.
    scene_path.write_text(json.dumps(_scene_with_texture_reference("wood.png")), encoding="utf-8")

    adapter = BlenderAdapter()
    specs = adapter.evaluate_request({
        "input_path": str(scene_path),
        "frame_start": 1,
        "frame_end": 24,
        "worker_count": 1,
    })

    assert len(specs) == 1
    assert specs[0].input_asset_hashes == set()


def test_blender_adapter_scheduling_unchanged_alongside_asset_hashes(tmp_path):
    """Phase 4A: adding asset-hash resolution must not alter the existing
    chunking/scheduling behavior (chunk count and frame boundaries)."""
    texture_bytes = b"scheduling-regression-check-bytes"
    (tmp_path / "wood.png").write_bytes(texture_bytes)
    scene_path = tmp_path / "scene.json"
    scene_path.write_text(json.dumps(_scene_with_texture_reference("wood.png")), encoding="utf-8")

    adapter = BlenderAdapter()
    specs = adapter.evaluate_request({
        "input_path": str(scene_path),
        "frame_start": 1,
        "frame_end": 250,
        "requires_gpu": True,
        # worker_count intentionally omitted -> must still default to 2
    })

    assert len(specs) == 2
    assert specs[0].parameters["frame_start"] == 1
    assert specs[0].parameters["frame_end"] == 125
    assert specs[1].parameters["frame_start"] == 126
    assert specs[1].parameters["frame_end"] == 250
    assert specs[0].estimated_duration_seconds == 125.0 * 2.0
    assert specs[1].estimated_duration_seconds == 125.0 * 2.0
    # Both chunks share the same whole-scene asset closure.
    assert len(specs[0].input_asset_hashes) > 0
    assert specs[0].input_asset_hashes == specs[1].input_asset_hashes
