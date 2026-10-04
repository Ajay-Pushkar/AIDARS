import os
import hashlib
import uuid
import sys
from typing import List
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec, ExecutionSpec, OutputVerificationPolicy

class CustomScriptAdapter(ApplicationAdapter):
    """Adapter for executing arbitrary user-provided scripts (Python/Bash).
    
    Demonstrates platform expansion by enabling arbitrary workloads.
    """
    
    def __init__(self, asset_manager=None) -> None:
        self.asset_manager = asset_manager

    def validate(self, request: dict) -> bool:
        """Validate custom script request."""
        if "script_path" not in request:
            return False
        return True

    def evaluate_request(self, request: dict) -> List[WorkloadSpec]:
        """Parse request, calculate requirements, and output WorkloadSpec."""
        script_path = request.get("script_path")
        args = request.get("args", [])
        expected_output_count = request.get("expected_output_count", 0)
        
        # Physical asset resolution
        input_asset_hashes = set()
        script_hash = None
        
        if script_path and os.path.exists(script_path) and os.path.isfile(script_path):
            hasher = hashlib.sha256()
            with open(script_path, "rb") as f:
                while chunk_data := f.read(65536):
                    hasher.update(chunk_data)
            script_hash = hasher.hexdigest()
            input_asset_hashes.add(script_hash)
            
            if self.asset_manager:
                self.asset_manager.upload_asset_file(script_path, script_hash)
                
        job_id = f"custom-{uuid.uuid4().hex[:16]}"
        
        spec = WorkloadSpec(
            workload_id=f"{job_id}-chunk-0",
            job_id=job_id,
            task_type="custom_script",
            input_asset_hashes=input_asset_hashes,
            min_cpu_cores=1,
            min_ram_bytes=256 * 1024 * 1024, # 256MB default
            requires_gpu=False,
            min_vram_bytes=0,
            estimated_duration_seconds=30.0,
            parameters={
                "script_hash": script_hash,
                "script_args": args,
                "expected_output_count": expected_output_count
            }
        )
        return [spec]
        
    def build_execution_spec(self, workload: WorkloadSpec) -> ExecutionSpec:
        script_hash = workload.parameters.get("script_hash")
        script_args = workload.parameters.get("script_args", [])
        
        if not script_hash:
            raise ValueError("Custom workload missing script_hash")
            
        args = [f"inputs/{script_hash}"] + script_args
        
        return ExecutionSpec(
            executable=sys.executable, # Assume Python script for safety/portability
            args=args,
            env={},
            cwd=None,
            timeout_seconds=workload.estimated_duration_seconds * 3.0
        )

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        return OutputVerificationPolicy(
            expected_output_count=workload.parameters.get("expected_output_count", 0)
        )
