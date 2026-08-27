import time
from aidars.m7.telemetry import TelemetryMemory
from aidars.m7.behavior import BehaviorInferencer
from aidars.m7.contracts import WorkerFeatureVector

mem = TelemetryMemory()
worker_a_id = "worker-a-failing"

for i in range(100):
    mem.ingest_worker_metrics(
        worker_a_id,
        cpu_ratio=1.0,
        ram_ratio=1.0,
        latency=5.0,
        failed=True
    )

worker_state = mem.workers[worker_a_id]
features = WorkerFeatureVector(
    cpu_available_ratio=1.0 - worker_state.cpu_utilization_ema.value,
    ram_available_ratio=1.0 - worker_state.ram_utilization_ema.value,
    vram_available_ratio=1.0,
    has_gpu=0.0,
    active_workload_ratio=0.0,
    cache_locality_ratio=0.0,
    heartbeat_stability=1.0,
    recent_failure_rate=worker_state.failure_rate_ema.value,
    recent_latency_normalized=min(1.0, worker_state.latency_ema.value / 100.0),
    throughput_normalized=0.5
)
print("Features:", features)
behavior = BehaviorInferencer.infer_worker_behavior(worker_a_id, features)
print("Behavior:", behavior)
