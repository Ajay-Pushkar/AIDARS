"""M7.12 and M7.13: Simulated LAN Intelligence and Adaptive Experiment.

This script simulates a long-running LAN environment with two workers:
- w-stable: A reliable LAN worker.
- w-erratic: A worker that experiences severe network degradation and failure spikes midway.

It demonstrates M7's intelligence dynamically adapting to the erratic worker and shifting load away.
"""
import time
from aidars.distributed.models import WorkerResourceProfile, WorkloadSpec
from aidars.distributed.placement import PlacementEngine
from aidars.m7.telemetry import TelemetryMemory
from aidars.m7.controller import M7OrchestratorBridge

def run_simulation():
    print("Starting M7 Long-Running Adaptive Experiment Simulation")
    
    # Initialize M7 Intelligence
    memory = TelemetryMemory()
    m7_bridge = M7OrchestratorBridge(memory)
    
    # Initialize M6 Placement with M7 injected
    placement_engine = PlacementEngine(m7_bridge=m7_bridge)
    
    workers = [
        WorkerResourceProfile(
            worker_id="w-stable",
            endpoint_url="http://10.0.0.1",
            ip_address="10.0.0.1",
            cpu_cores_total=8,
            cpu_utilization_percent=10.0,
            ram_total_bytes=16000,
            ram_available_bytes=14000,
            gpu_available=False
        ),
        WorkerResourceProfile(
            worker_id="w-erratic",
            endpoint_url="http://10.0.0.2",
            ip_address="10.0.0.2",
            cpu_cores_total=16, # Looks better statically!
            cpu_utilization_percent=5.0,
            ram_total_bytes=32000,
            ram_available_bytes=30000,
            gpu_available=False
        )
    ]
    
    workload = WorkloadSpec(
        workload_id="wl-test",
        task_type="simulation",
        min_cpu_cores=2,
        min_ram_bytes=1000,
        requires_gpu=False,
        estimated_duration_seconds=5.0
    )
    
    print("\n--- PHASE 1: Initial State (No Telemetry) ---")
    decision_1 = placement_engine.evaluate(workload, workers)
    print(f"M6 selected: {decision_1.selected_worker_id}")
    print(f"Reason: w-erratic has more resources statically, so M6 naturally prefers it.")
    
    print("\n--- PHASE 2: Simulating 20 minutes of stable execution ---")
    for _ in range(20):
        # Both perform well
        memory.ingest_worker_metrics("w-stable", cpu_ratio=0.8, ram_ratio=0.8, latency=10.0, failed=False)
        memory.ingest_worker_metrics("w-erratic", cpu_ratio=0.9, ram_ratio=0.9, latency=12.0, failed=False)
        
    decision_2 = placement_engine.evaluate(workload, workers)
    print(f"M6 selected: {decision_2.selected_worker_id}")
    print(f"Reason: Both are stable, w-erratic still preferred for higher raw specs.")
    
    print("\n--- PHASE 3: Environmental Degradation (w-erratic starts failing and lagging) ---")
    for _ in range(15):
        # w-stable is fine
        memory.ingest_worker_metrics("w-stable", cpu_ratio=0.8, ram_ratio=0.8, latency=11.0, failed=False)
        # w-erratic suffers a network partition and random crashes
        memory.ingest_worker_metrics("w-erratic", cpu_ratio=0.1, ram_ratio=0.1, latency=800.0, failed=True)
        
    decision_3 = placement_engine.evaluate(workload, workers)
    print(f"M6 selected: {decision_3.selected_worker_id}")
    print(f"Reason: M7 detected severe risk (ERRATIC behavior, high failure) and penalized w-erratic, shifting load to w-stable.")
    
    print("\nM7.12 & M7.13 Simulation Complete.")

if __name__ == "__main__":
    run_simulation()
