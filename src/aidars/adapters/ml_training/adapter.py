import json
import hashlib
import os
import uuid
from typing import Any, List, Dict, Optional
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec, ExecutionSpec, OutputVerificationPolicy


class MLTrainingAdapter(ApplicationAdapter):
    """ML Training adapter implementing the generic contract.
    
    Executes a real Python ML script (e.g. PyTorch) using physical
    dataset and script assets staged via CAS.
    
    NOTE: This adapter orchestrates independent training workloads.
    Distributed model training (e.g. DistributedDataParallel) across
    multiple machines is DEFERRED / NOT IMPLEMENTED in Phase 13.
    """
    
    def __init__(self, asset_manager=None) -> None:
        self.asset_manager = asset_manager

    def validate(self, request: dict) -> bool:
        """Validate ML training request."""
        if "script_path" not in request and "script" not in request and "dataset" not in request and "dataset_path" not in request:
            return False
        return True

    def evaluate_request(self, request: dict) -> List[WorkloadSpec]:
        """Parse ML training request, calculate requirements, and output WorkloadSpec."""
        script_path = request.get("script_path") or request.get("script")
        dataset_path = request.get("dataset_path") or request.get("dataset")
        epochs = request.get("epochs", 1)
        batch_size = request.get("batch_size", 32)
        requires_gpu = request.get("requires_gpu", False)
        
        # Physical asset resolution
        input_asset_hashes = set()
        script_hash = None
        dataset_hash = None
        
        for path_var, name in [(script_path, "script"), (dataset_path, "dataset")]:
            if path_var and os.path.exists(path_var) and os.path.isfile(path_var):
                hasher = hashlib.sha256()
                with open(path_var, "rb") as f:
                    while chunk_data := f.read(65536):
                        hasher.update(chunk_data)
                h = hasher.hexdigest()
                input_asset_hashes.add(h)
                if name == "script":
                    script_hash = h
                else:
                    dataset_hash = h
                
                if self.asset_manager:
                    self.asset_manager.upload_asset_file(path_var, h)
                    
        job_id = f"ml-train-{uuid.uuid4().hex[:16]}"
        
        # CPU/RAM/GPU/VRAM requirements estimation
        min_vram_bytes = 4 * 1024 * 1024 * 1024 if requires_gpu else 0
        min_ram_bytes = 8 * 1024 * 1024 * 1024
        
        spec = WorkloadSpec(
            workload_id=f"{job_id}-chunk-0",
            job_id=job_id,
            task_type="ml_training",
            input_asset_hashes=input_asset_hashes,
            min_cpu_cores=4,
            min_ram_bytes=min_ram_bytes,
            requires_gpu=requires_gpu,
            min_vram_bytes=min_vram_bytes,
            estimated_duration_seconds=float(epochs) * 30.0,
            parameters={
                "script": script_path or "train.py",
                "dataset": dataset_path or "dataset",
                "script_hash": script_hash,
                "dataset_hash": dataset_hash,
                "epochs": epochs,
                "batch_size": batch_size
            }
        )
        spec.execution_spec = self.build_execution_spec(spec)
        return [spec]
        
    def build_execution_spec(self, workload: WorkloadSpec) -> ExecutionSpec:
        script_hash = workload.parameters.get("script_hash")
        dataset_hash = workload.parameters.get("dataset_hash")
        script = workload.parameters.get("script") or "train.py"
        dataset = workload.parameters.get("dataset") or "dataset"
        epochs = workload.parameters.get("epochs", 1)
        batch_size = workload.parameters.get("batch_size", 32)
        
        script_arg = f"inputs/{script_hash}" if script_hash else str(script)
        dataset_arg = f"inputs/{dataset_hash}" if dataset_hash else str(dataset)
            
        output_path = workload.parameters.get("output", "./outputs/model.pt")
        args = [
            script_arg,
            "--dataset", dataset_arg,
            "--epochs", str(epochs),
            "--batch-size", str(batch_size),
            "--output", output_path,
        ]
        return ExecutionSpec(
            executable="python",
            args=args,
            env={},
            cwd=None,
            timeout_seconds=workload.estimated_duration_seconds * 3.0
        )

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        return OutputVerificationPolicy(
            expected_output_count=1
        )
