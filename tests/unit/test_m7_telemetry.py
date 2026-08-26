"""Tests for the M7 Telemetry Memory Engine."""

import time
from aidars.m7.telemetry import EMA, WorkerTemporalState, WorkloadTemporalState, TelemetryMemory

def test_ema_initialization_and_update():
    """Test that EMA initializes correctly and smooths values."""
    ema = EMA(alpha=0.5)
    
    # First update should initialize exactly to the value
    res = ema.update(10.0)
    assert ema.initialized
    assert res == 10.0
    
    # Second update should smooth: 0.5 * 20 + 0.5 * 10 = 15.0
    res = ema.update(20.0)
    assert res == 15.0
    
    # Third update: 0.5 * 30 + 0.5 * 15 = 22.5
    res = ema.update(30.0)
    assert res == 22.5

def test_worker_temporal_state_trend():
    """Test that trend calculation correctly compares current vs baseline."""
    state = WorkerTemporalState("Worker-A")
    state.cpu_utilization_ema.update(0.5)  # Baseline is 50%
    
    # Trend for 0.75 vs 0.5 baseline should be 1.5 (spiking)
    trend = state.get_trend("cpu_utilization", 0.75)
    assert trend == 1.5
    
    # Trend for 0.25 vs 0.5 baseline should be 0.5 (dropping)
    trend = state.get_trend("cpu_utilization", 0.25)
    assert trend == 0.5
    
    # Trend for uninitialized metric should default to 1.0
    trend = state.get_trend("latency", 100.0)
    assert trend == 1.0

def test_telemetry_memory_ingestion():
    """Test the central memory engine correctly ingests and stores metrics."""
    memory = TelemetryMemory()
    
    # Ingest worker metrics
    memory.ingest_worker_metrics("Worker-A", cpu_ratio=0.8, ram_ratio=0.5, latency=10.0, failed=False)
    state = memory.get_worker_state("Worker-A")
    assert state is not None
    assert state.cpu_utilization_ema.value == 0.8
    assert state.failure_rate_ema.value == 0.0
    
    # Second ingestion with failure
    memory.ingest_worker_metrics("Worker-A", cpu_ratio=0.9, ram_ratio=0.6, latency=15.0, failed=True)
    # Failure rate EMA alpha is 0.1. 0.1 * 1.0 + 0.9 * 0.0 = 0.1
    assert state.failure_rate_ema.value == 0.1
    
    # Ingest workload result (success)
    memory.ingest_workload_result("m6-lan-test", duration=5.0, ram_peak=1024.0, failed=False)
    wl_state = memory.get_workload_state("m6-lan-test")
    assert wl_state is not None
    assert wl_state.duration_ema.value == 5.0
    assert wl_state.failure_rate_ema.value == 0.0
    
    # Ingest workload result (failure)
    # Duration and ram should NOT update on failure
    memory.ingest_workload_result("m6-lan-test", duration=0.1, ram_peak=0.0, failed=True)
    assert wl_state.duration_ema.value == 5.0
    assert wl_state.ram_peak_ema.value == 1024.0
    # Failure rate EMA alpha is 0.1. 0.1 * 1.0 + 0.9 * 0.0 = 0.1
    assert wl_state.failure_rate_ema.value == 0.1
