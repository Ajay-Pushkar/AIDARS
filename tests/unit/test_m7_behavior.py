"""Tests for the M7 Behavior Inference."""

from aidars.m7.features import WorkerFeatureVector, WorkloadFeatureVector
from aidars.m7.behavior import BehaviorInferencer, WorkerState, WorkloadState

def test_infer_worker_overloaded():
    """Test overloaded worker inference."""
    features = WorkerFeatureVector(
        cpu_available_ratio=0.05,
        ram_available_ratio=0.05,
        vram_available_ratio=0.0,
        has_gpu=0.0,
        active_workload_ratio=1.0,
        cache_locality_ratio=0.5,
        heartbeat_stability=1.0,
        recent_failure_rate=0.0,
        recent_latency_normalized=0.1,
        throughput_normalized=0.9
    )
    
    behavior = BehaviorInferencer.infer_worker_behavior("w-1", features)
    assert behavior.state == WorkerState.OVERLOADED
    assert behavior.confidence > 0.5

def test_infer_worker_erratic():
    """Test erratic worker inference."""
    features = WorkerFeatureVector(
        cpu_available_ratio=0.8,
        ram_available_ratio=0.8,
        vram_available_ratio=0.0,
        has_gpu=0.0,
        active_workload_ratio=0.1,
        cache_locality_ratio=0.5,
        heartbeat_stability=0.9,
        recent_failure_rate=0.4,  # High failure rate
        recent_latency_normalized=0.1,
        throughput_normalized=0.9
    )
    
    behavior = BehaviorInferencer.infer_worker_behavior("w-1", features)
    assert behavior.state == WorkerState.ERRATIC

def test_infer_workload_cpu_bound():
    """Test CPU bound workload inference."""
    features = WorkloadFeatureVector(
        required_cpu_ratio=0.8,
        required_ram_ratio=0.2,  # CPU is 4x RAM
        required_vram_ratio=0.0,
        requires_gpu=0.0,
        dependency_count_normalized=0.1,
        dependency_bytes_normalized=0.1,
        priority_normalized=0.5,
        estimated_duration_normalized=0.5
    )
    
    behavior = BehaviorInferencer.infer_workload_behavior("wl-1", features)
    assert behavior.state == WorkloadState.CPU_BOUND

def test_infer_workload_gpu_bound():
    """Test GPU bound workload inference."""
    features = WorkloadFeatureVector(
        required_cpu_ratio=0.1,
        required_ram_ratio=0.1,
        required_vram_ratio=0.5,
        requires_gpu=1.0,  # Needs GPU
        dependency_count_normalized=0.1,
        dependency_bytes_normalized=0.1,
        priority_normalized=0.5,
        estimated_duration_normalized=0.5
    )
    
    behavior = BehaviorInferencer.infer_workload_behavior("wl-1", features)
    assert behavior.state == WorkloadState.GPU_BOUND
