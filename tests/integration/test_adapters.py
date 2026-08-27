import pytest
from aidars.adapters.blender.adapter import BlenderAdapter
from aidars.adapters.llm.adapter import LLMAdapter
from aidars.adapters.ml_training.adapter import MLTrainingAdapter
from aidars.distributed.models import WorkloadSpec

def test_blender_adapter_produces_generic_workload():
    adapter = BlenderAdapter()
    request = {
        "input_path": "/test/scene.blend",
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
        (BlenderAdapter(), {"input_path": "a"}),
        (LLMAdapter(), {"prompt": "b"}),
        (MLTrainingAdapter(), {"dataset": "c"})
    ]
    
    for adapter, req in adapters:
        specs = adapter.evaluate_request(req)
        for spec in specs:
            assert isinstance(spec, WorkloadSpec)
            assert spec.workload_id
            assert spec.task_type in ["blender_render", "llm_inference", "ml_training"]
