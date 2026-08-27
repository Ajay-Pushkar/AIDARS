"""M7 Intelligence: Feature Engineering Contracts.

Defines the normalized numerical representations (Feature Vectors) that the
M7 intelligence layer uses to model the distributed environment.
"""
from dataclasses import dataclass
from typing import List
from aidars.m7.contracts import FeatureVector, WorkerFeatureVector, WorkloadFeatureVector

class FeatureExtractor:
    """Translates raw generic structs into normalized numerical vectors."""
    
    @staticmethod
    def extract_worker_features(profile: 'WorkerResourceProfile', temporal_state: 'WorkerTemporalState', spec: 'WorkloadSpec') -> WorkerFeatureVector:
        """Normalizes a worker's resources and historical telemetry into a feature vector."""
        cpu_ratio = max(0.0, 1.0 - (profile.cpu_utilization_percent / 100.0))
        
        # Avoid division by zero
        ram_total = profile.ram_total_bytes if profile.ram_total_bytes > 0 else 1.0
        ram_ratio = profile.ram_available_bytes / ram_total
        
        vram_total = profile.vram_total_bytes if profile.vram_total_bytes > 0 else 1.0
        vram_ratio = profile.vram_available_bytes / vram_total
        
        has_gpu = 1.0 if profile.gpu_available else 0.0
        
        # Telemetry
        heartbeat = 1.0  # Default if no telemetry
        fail_rate = 0.0
        latency_norm = 0.0
        throughput_norm = 0.0
        
        if temporal_state:
            # We assume failure_rate is a probability EMA [0.0, 1.0]
            fail_rate = min(1.0, max(0.0, temporal_state.failure_rate_ema.value))
            # Latency normalized (assuming max expected latency is 1000ms)
            latency_norm = min(1.0, temporal_state.latency_ema.value / 1000.0)
            
        required = set(spec.input_asset_hashes)
        cached = set(profile.local_cached_hashes)
        if not required:
            cache_locality_ratio = 1.0
        else:
            cache_locality_ratio = len(required.intersection(cached)) / len(required)
            
        active_workload_ratio = min(1.0, profile.active_workload_count / profile.max_concurrent_workloads)
        
        return WorkerFeatureVector(
            cpu_available_ratio=cpu_ratio,
            ram_available_ratio=ram_ratio,
            vram_available_ratio=vram_ratio,
            has_gpu=has_gpu,
            active_workload_ratio=active_workload_ratio,
            cache_locality_ratio=cache_locality_ratio,
            heartbeat_stability=heartbeat,
            recent_failure_rate=fail_rate,
            recent_latency_normalized=latency_norm,
            throughput_normalized=throughput_norm
        )

    @staticmethod
    def extract_workload_features(spec: 'WorkloadSpec', network_max_ram: int, network_max_cpu: int, total_dependency_bytes: int = 0) -> WorkloadFeatureVector:
        """Normalizes a workload's requirements into a feature vector."""
        # Normalize against network maximums to keep values [0.0, 1.0]
        req_cpu = min(1.0, spec.min_cpu_cores / (network_max_cpu or 1.0))
        req_ram = min(1.0, spec.min_ram_bytes / (network_max_ram or 1.0))
        req_vram = min(1.0, spec.min_vram_bytes / (network_max_ram or 1.0))  # Assuming vram uses same max scale for now
        
        req_gpu = 1.0 if spec.requires_gpu else 0.0
        
        dep_count_norm = min(1.0, len(spec.input_asset_hashes) / 100.0) # Still arbitrary until asset counts are modeled
        
        # Priorities typically 1-100
        priority_norm = min(1.0, max(0.0, spec.priority / 100.0))
        
        # Duration maxing out at 3600 seconds (1 hour)
        duration_norm = min(1.0, spec.estimated_duration_seconds / 3600.0)
        
        # Dependency bytes
        # max_transfer_size e.g., 50GB
        dep_bytes_norm = min(1.0, total_dependency_bytes / (50 * 1024 * 1024 * 1024))
        
        return WorkloadFeatureVector(
            required_cpu_ratio=req_cpu,
            required_ram_ratio=req_ram,
            required_vram_ratio=req_vram,
            requires_gpu=req_gpu,
            dependency_count_normalized=dep_count_norm,
            dependency_bytes_normalized=dep_bytes_norm,
            priority_normalized=priority_norm,
            estimated_duration_normalized=duration_norm
        )

