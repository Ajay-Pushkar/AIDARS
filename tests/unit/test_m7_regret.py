import random
import statistics
from typing import List, Dict

from aidars.distributed.models import WorkloadSpec, WorkerResourceProfile
from aidars.m7.telemetry import TelemetryMemory
from aidars.m7.controller import M7OrchestratorBridge

class SimulationEnvironment:
    """Simulates a physical LAN environment with hidden worker distributions."""
    
    def __init__(self):
        # Hidden truth that M7 must learn
        self.worker_truth = {
            "w-fast-stable": {"mean_duration": 10.0, "duration_std": 1.0, "fail_prob": 0.01},
            "w-fast-unstable": {"mean_duration": 8.0, "duration_std": 4.0, "fail_prob": 0.20},
            "w-slow-stable": {"mean_duration": 20.0, "duration_std": 0.5, "fail_prob": 0.05}
        }
        
    def simulate_execution(self, worker_id: str) -> dict:
        """Simulates an actual workload execution returning real duration and failure state."""
        truth = self.worker_truth[worker_id]
        
        # Did it fail?
        failed = random.random() < truth["fail_prob"]
        
        if failed:
            duration = random.uniform(1.0, 5.0)  # Fails early or randomly
        else:
            duration = max(1.0, random.gauss(truth["mean_duration"], truth["duration_std"]))
            
        return {
            "duration": duration,
            "failed": failed,
            "cost": 1000.0 if failed else duration  # Cost function
        }

def test_m7_placement_regret_vs_m6():
    """Proves M7 consistently improves expected outcomes and minimizes placement regret vs naive M6."""
    
    # 1. Setup
    env = SimulationEnvironment()
    memory = TelemetryMemory()
    bridge = M7OrchestratorBridge(memory)
    
    workload = WorkloadSpec(
        workload_id="wl-test",
        task_type="m6-lan-test",
        min_cpu_cores=4,
        min_ram_bytes=4000,
        requires_gpu=False,
        estimated_duration_seconds=15.0
    )
    
    candidates = [
        WorkerResourceProfile(worker_id=wid, endpoint_url="http://x", ip_address="127.0.0.1", cpu_cores_total=8, cpu_utilization_percent=0.0, ram_total_bytes=16000, ram_available_bytes=16000, gpu_available=False, active_workload_count=0, max_concurrent_workloads=10)
        for wid in env.worker_truth.keys()
    ]
    
    # 2. Naive M6 baseline scores (all workers identical hardware, so M6 scores them equally e.g. 100)
    m6_scores = {wid: 100.0 for wid in env.worker_truth.keys()}
    
    # Trackers
    m6_total_cost = 0.0
    m7_total_cost = 0.0
    m6_regret = 0.0
    m7_regret = 0.0
    
    # 3. Training / Simulation Loop
    iterations = 100
    
    for i in range(iterations):
        # Determine actual outcomes for all workers in this iteration (Hindsight Oracle)
        outcomes = {wid: env.simulate_execution(wid) for wid in env.worker_truth.keys()}
        best_possible_cost = min(out.get('cost', 1000.0) for out in outcomes.values())
        
        # M6 Decision (always picks the first one or random, since scores are equal. We'll simulate random tie-break or just pick fast-unstable if M6 likes it due to slightly lower CPU initially. Let's assume M6 picks randomly among equal scores)
        m6_chosen = random.choice(list(env.worker_truth.keys()))
        m6_cost = outcomes[m6_chosen]["cost"]
        m6_total_cost += m6_cost
        m6_regret += (m6_cost - best_possible_cost)
        
        # M7 Decision
        intelligence = bridge.evaluate_candidates(workload, candidates)
        adjusted_list = bridge.adjust_ranking(m6_scores, intelligence)
        m7_chosen = adjusted_list[0] if adjusted_list else list(env.worker_truth.keys())[0]
        m7_cost = outcomes[m7_chosen]["cost"]
        m7_total_cost += m7_cost
        m7_regret += (m7_cost - best_possible_cost)
        
        # Only M7 learns (simulating feedback loop)
        for wid, outcome in outcomes.items():
             # Ingest worker metrics (heartbeats)
             memory.ingest_worker_metrics(wid, cpu_ratio=0.5, ram_ratio=0.5, latency=5.0, failed=False)
             # Ingest workload outcomes
             memory.ingest_workload_result("m6-lan-test", duration=outcome["duration"], ram_peak=1024, failed=outcome["failed"])
             # Update worker temporal failures
             if outcome["failed"]:
                 memory.ingest_worker_metrics(wid, cpu_ratio=0.5, ram_ratio=0.5, latency=5.0, failed=True)

    print(f"M6 Regret: {m6_regret:.2f}, M7 Regret: {m7_regret:.2f}")
    assert m7_regret < (m6_regret * 0.8), "M7 should significantly reduce placement regret compared to M6"
