import hashlib
import json
import uuid
import os
from typing import Any, List, Dict, Optional
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec, ExecutionSpec, OutputVerificationPolicy


class LLMAdapter(ApplicationAdapter):
    """LLM inference adapter implementing the generic contract.
    
    Targets llama-cli (llama.cpp) as the local inference engine.
    """
    
    def __init__(self, asset_manager=None) -> None:
        self.asset_manager = asset_manager

    def validate(self, request: dict) -> bool:
        """Validate LLM specific request."""
        if "prompt" not in request:
            return False
        return True

    def evaluate_request(self, request: dict) -> List[WorkloadSpec]:
        """Parse LLM request, calculate requirements, and output WorkloadSpec."""
        model_identifier = request.get("model") or request.get("model_identifier") or request.get("model_path") or "default-model"
        model_path = request.get("model_path")
        prompt = request.get("prompt", "")
        max_tokens = request.get("max_tokens", 256)
        requires_gpu = request.get("requires_gpu", True)
        
        # M4: Physical asset resolution
        input_asset_hashes = set()
        model_hash = None
        if model_path and os.path.exists(model_path) and os.path.isfile(model_path):
            hasher = hashlib.sha256()
            with open(model_path, "rb") as f:
                while chunk_data := f.read(65536):
                    hasher.update(chunk_data)
            model_hash = hasher.hexdigest()
            input_asset_hashes.add(model_hash)
            if self.asset_manager:
                self.asset_manager.upload_asset_file(model_path, model_hash)

        # Protect prompt from broad logs: use a generic prefix instead of embedding prompt text
        prompt_hash = hashlib.md5(prompt.encode()).hexdigest()[:8]
        job_id = f"llm-job-{uuid.uuid4().hex[:16]}"
        
        # Estimate requirements based on parameters
        min_vram_bytes = 4 * 1024 * 1024 * 1024 if requires_gpu else 0
        min_ram_bytes = 8 * 1024 * 1024 * 1024
        
        spec = WorkloadSpec(
            workload_id=f"{job_id}-{prompt_hash}",
            job_id=job_id,
            task_type="llm_inference",
            input_asset_hashes=input_asset_hashes,
            min_cpu_cores=4,
            min_ram_bytes=min_ram_bytes,
            requires_gpu=requires_gpu,
            min_vram_bytes=min_vram_bytes,
            estimated_duration_seconds=float(max_tokens) * 0.5, # ~2 tok/sec fallback estimate
            parameters={
                "model": model_identifier,
                "model_identifier": model_identifier,
                "model_hash": model_hash,
                "prompt": prompt,
                "max_tokens": max_tokens,
            }
        )
        spec.execution_spec = self.build_execution_spec(spec)
        return [spec]
        
    def build_execution_spec(self, workload: WorkloadSpec) -> ExecutionSpec:
        model_hash = workload.parameters.get("model_hash")
        model = workload.parameters.get("model") or workload.parameters.get("model_identifier") or "default-model"
        prompt = workload.parameters.get("prompt", "")
        max_tokens = workload.parameters.get("max_tokens", 256)
        
        model_arg = f"inputs/{model_hash}" if model_hash else str(model)
            
        args = [
            "-m", model_arg,
            "-p", prompt,
            "-n", str(max_tokens),
            "-o", "./outputs/response.txt"
        ]
        return ExecutionSpec(
            executable="llama-cli", # Real local inference engine
            args=args,
            env={},
            cwd=None,
            timeout_seconds=workload.estimated_duration_seconds * 3.0
        )

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        return OutputVerificationPolicy(
            expected_output_count=1 # Expect one text output file
        )
