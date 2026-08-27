import json
import hashlib
from typing import Any, List, Dict
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec

class LLMAdapter(ApplicationAdapter):
    """LLM inference adapter implementing the generic contract."""
    
    def evaluate_request(self, request: dict) -> List[WorkloadSpec]:
        """Parse LLM request, calculate requirements, and output WorkloadSpec."""
        model_name = request.get("model", "llama-2-7b")
        prompt = request.get("prompt", "")
        max_tokens = request.get("max_tokens", 256)
        
        # RAM/VRAM estimation (dummy logic for simulation)
        # e.g., 7B model requires ~14GB VRAM
        requires_gpu = request.get("requires_gpu", True)
        min_vram_bytes = 14 * 1024 * 1024 * 1024 if requires_gpu else 0
        min_ram_bytes = 16 * 1024 * 1024 * 1024
        
        # Create a single workload for inference
        spec = WorkloadSpec(
            workload_id=f"llm-infer-{hashlib.md5(prompt.encode()).hexdigest()[:8]}",
            task_type="llm_inference",
            input_asset_hashes=set(),  # In reality, this would contain the model weights hash
            min_cpu_cores=4,
            min_ram_bytes=min_ram_bytes,
            requires_gpu=requires_gpu,
            min_vram_bytes=min_vram_bytes,
            estimated_duration_seconds=float(max_tokens) * 0.05, # ~20 tok/sec
            parameters={
                "model": model_name,
                "prompt": prompt,
                "max_tokens": max_tokens
            }
        )
        return [spec]
        
    def collect_outputs(self, spec: WorkloadSpec, workspace: Any) -> Any:
        """Interpret workload outputs from the generic runtime."""
        return {
            "result": f"LLM workload {spec.workload_id} outputs interpreted."
        }
