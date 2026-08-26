"""Tests for the M7 Feature Extraction."""

from aidars.m7.features import FeatureExtractor, WorkerFeatureVector, WorkloadFeatureVector
from aidars.m7.telemetry import WorkerTemporalState
from aidars.distributed.models import WorkerResourceProfile, WorkloadSpec

def test_extract_worker_features():
    """Test extracting worker features from raw profiles and telemetry."""
    profile = WorkerResourceProfile(
        worker_id="w-1",
        endpoint_url="http://127.0.0.1:8001",
        ip_address="127.0.0.1",
        cpu_cores_total=8,
        cpu_utilization_percent=25.0,
        ram_total_bytes=16000,
        ram_available_bytes=8000,
        gpu_available=False,
        vram_total_bytes=0,
        vram_available_bytes=0,
        active_workload_count=2,
        local_cached_hashes={"hash1", "hash2"}
    )
    
    temporal_state = WorkerTemporalState(worker_id="w-1")
    temporal_state.failure_rate_ema.value = 0.1
    temporal_state.latency_ema.value = 150.0  # 150ms
    
    features = FeatureExtractor.extract_worker_features(profile, temporal_state, network_max_ram=32000)
    
    # 1.0 - 0.25 = 0.75
    assert features.cpu_available_ratio == 0.75
    # 8000 / 16000 = 0.5
    assert features.ram_available_ratio == 0.5
    assert features.has_gpu == 0.0
    
    # Active workload count: 2 / 10 = 0.2
    assert features.active_workload_ratio == 0.2
    # Cached hashes: 2 / 100 = 0.02
    assert features.cache_locality_ratio == 0.02
    
    # Telemetry
    assert features.recent_failure_rate == 0.1
    # 150.0 / 1000.0 = 0.15
    assert features.recent_latency_normalized == 0.15

def test_extract_workload_features():
    """Test extracting workload features from specs."""
    spec = WorkloadSpec(
        workload_id="wl-1",
        task_type="m6-lan-test",
        min_cpu_cores=4,
        min_ram_bytes=4000,
        requires_gpu=True,
        input_asset_hashes={
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            "a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3"
        },
        priority=80,
        estimated_duration_seconds=1800.0
    )
    
    features = FeatureExtractor.extract_workload_features(spec, network_max_ram=16000, network_max_cpu=16)
    
    # 4 / 16 = 0.25
    assert features.required_cpu_ratio == 0.25
    # 4000 / 16000 = 0.25
    assert features.required_ram_ratio == 0.25
    assert features.requires_gpu == 1.0
    
    # 2 assets / 100 = 0.02
    assert features.dependency_count_normalized == 0.02
    # 80 / 100.0 = 0.8
    assert features.priority_normalized == 0.8
    # 1800 / 3600 = 0.5
    assert features.estimated_duration_normalized == 0.5
