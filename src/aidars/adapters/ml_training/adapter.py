import json
import hashlib
from typing import Any, List, Dict
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec

class MLTrainingAdapter(ApplicationAdapter):
    """ML Training adapter implementing the generic contract."""
    
    def evaluate_request(self, request: dict) -> List[WorkloadSpec]:
        """Parse ML training request, calculate requirements, and output WorkloadSpec."""
        dataset = request.get("dataset", "mnist")
        epochs = request.get("epochs", 10)
        batch_size = request.get("batch_size", 32)
        
        # CPU/RAM/GPU/VRAM requirements estimation
        requires_gpu = request.get("requires_gpu", True)
        min_vram_bytes = 8 * 1024 * 1024 * 1024 if requires_gpu else 0 # 8GB VRAM
        min_ram_bytes = 16 * 1024 * 1024 * 1024 # 16GB RAM
        
        spec = WorkloadSpec(
            workload_id=f"ml-train-{hashlib.md5(dataset.encode()).hexdigest()[:8]}",
            task_type="ml_training",
            input_asset_hashes=set(),  # Dataset and initial model checkpoint hashes
            min_cpu_cores=8,
            min_ram_bytes=min_ram_bytes,
            requires_gpu=requires_gpu,
            min_vram_bytes=min_vram_bytes,
            estimated_duration_seconds=float(epochs) * 60.0, # ~1 min per epoch
            parameters={
                "dataset": dataset,
                "epochs": epochs,
                "batch_size": batch_size
            }
        )
        return [spec]
        
    def collect_outputs(self, spec: WorkloadSpec, workspace: Any) -> Any:
        """Interpret workload outputs from the generic runtime."""
        return {
            "result": f"ML workload {spec.workload_id} outputs interpreted."
        }
