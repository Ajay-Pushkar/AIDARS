"""Tests for M7 Prediction Engine."""

from aidars.m7.prediction import PerformancePredictor
from aidars.m7.features import WorkerFeatureVector, WorkloadFeatureVector
from aidars.m7.behavior import WorkerBehavior, WorkerState, WorkloadBehavior, WorkloadState

def test_predict_ideal_conditions():
    """Test prediction for stable worker and normal workload."""
    wf = WorkerFeatureVector(
        cpu_available_ratio=0.8,
        ram_available_ratio=0.8,
        vram_available_ratio=0.0,
        has_gpu=0.0,
        active_workload_ratio=0.1,
        cache_locality_ratio=0.0,
        heartbeat_stability=1.0,
        recent_failure_rate=0.0,
        recent_latency_normalized=0.1,
        throughput_normalized=0.5
    )
    wlf = WorkloadFeatureVector(
        required_cpu_ratio=0.1,
        required_ram_ratio=0.1,
        required_vram_ratio=0.0,
        requires_gpu=0.0,
        dependency_count_normalized=0.1,
        dependency_bytes_normalized=0.1,
        priority_normalized=0.5,
        estimated_duration_normalized=0.1
    )
    
    wb = WorkerBehavior("w-1", WorkerState.STABLE, confidence=0.9)
    wlb = WorkloadBehavior("wl-1", WorkloadState.MIXED, confidence=0.8)
    
    res = PerformancePredictor.predict(wf, wlf, wb, wlb)
    
    assert res.failure_probability == 0.0
    assert res.predicted_duration_seconds == 10.0
    assert res.predicted_ram_peak_bytes == 1024 * 1024 * 1024
    assert res.confidence == 0.3  # Cold start penalty

def test_predict_gpu_bound_on_cpu_worker():
    """Test prediction properly spikes failure rate if GPU is required but missing."""
    wf = WorkerFeatureVector(
        cpu_available_ratio=1.0, ram_available_ratio=1.0, vram_available_ratio=0.0,
        has_gpu=0.0, active_workload_ratio=0.0, cache_locality_ratio=0.0,
        heartbeat_stability=1.0, recent_failure_rate=0.0, recent_latency_normalized=0.0,
        throughput_normalized=0.0
    )
    wlf = WorkloadFeatureVector(
        required_cpu_ratio=0.1, required_ram_ratio=0.1, required_vram_ratio=0.5,
        requires_gpu=1.0, dependency_count_normalized=0.0, dependency_bytes_normalized=0.0,
        priority_normalized=0.5, estimated_duration_normalized=0.1
    )
    
    wb = WorkerBehavior("w-1", WorkerState.STABLE, confidence=0.9)
    wlb = WorkloadBehavior("wl-1", WorkloadState.GPU_BOUND, confidence=0.9)
    
    res = PerformancePredictor.predict(wf, wlf, wb, wlb)
    
    # Missing GPU causes fatal failure probability spike (+0.9)
    assert res.failure_probability >= 0.9
