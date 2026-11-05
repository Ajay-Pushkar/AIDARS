"""M22: Power-Proportional Dataset Sharding & Low-RAM Auto-Tuned ML Training Adapter.

Executes real Python ML scripts (PyTorch, scikit-learn, Voicebox/RVC, custom training loops)
with power-proportional dataset/batch sharding across any N heterogeneous PCs,
zero hardcoded worker IDs, and automatic low-RAM (<3.5GB) micro-batching.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import uuid
from typing import Any, Dict, List, Optional, Sequence, Union

from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import (
    ExecutionSpec,
    OutputVerificationPolicy,
    WorkerInfo,
    WorkerResourceProfile,
    WorkerStatus,
    WorkloadSpec,
)
from aidars.distributed.placement import compute_worker_throughput_score

logger = logging.getLogger(__name__)


class MLTrainingAdapter(ApplicationAdapter):
    """Universal ML Training adapter implementing the generic contract.

    Executes Python ML scripts (PyTorch, scikit-learn, etc.) across N heterogeneous PCs:
    - Proportionally splits datasets/batches across workers based on live compute power score.
    - Locks CPU thread pools to each worker's optimal thread capacity.
    - Auto-tunes low-RAM (<3.5GB) nodes with micro-batching and zero worker overhead.
    """

    def __init__(self, asset_manager: Optional[Any] = None) -> None:
        self.asset_manager = asset_manager

    def validate(self, request: dict) -> bool:
        """Validate ML training request."""
        if (
            "script_path" not in request
            and "script" not in request
            and "dataset" not in request
            and "dataset_path" not in request
        ):
            return False
        return True

    def plan_shards(
        self,
        job_spec: Union[dict, WorkloadSpec],
        active_workers: Sequence[Any],
    ) -> List[WorkloadSpec]:
        """Split dataset/batches proportionally across active workers using continuous throughput scores."""
        # 1. Normalize request parameters
        if isinstance(job_spec, WorkloadSpec):
            req_params = dict(job_spec.parameters or {})
            requires_gpu = job_spec.requires_gpu
            job_id = job_spec.job_id
            input_asset_hashes = set(job_spec.input_asset_hashes)
        else:
            req_params = dict(job_spec)
            requires_gpu = bool(job_spec.get("requires_gpu", False))
            job_id = f"ml-train-{uuid.uuid4().hex[:16]}"
            input_asset_hashes = set()

        script_path = req_params.get("script_path") or req_params.get("script") or "train.py"
        dataset_path = req_params.get("dataset_path") or req_params.get("dataset") or "dataset"
        epochs = int(req_params.get("epochs", 1))
        batch_size = int(req_params.get("batch_size", 32))

        # Asset resolution if not yet hashed
        script_hash = req_params.get("script_hash")
        dataset_hash = req_params.get("dataset_hash")
        for path_var, name in [(script_path, "script"), (dataset_path, "dataset")]:
            if path_var and os.path.exists(path_var) and os.path.isfile(path_var):
                hasher = hashlib.sha256()
                with open(path_var, "rb") as f:
                    while chunk := f.read(65536):
                        hasher.update(chunk)
                h = hasher.hexdigest()
                input_asset_hashes.add(h)
                if name == "script":
                    script_hash = h
                else:
                    dataset_hash = h
                if self.asset_manager:
                    self.asset_manager.upload_asset_file(path_var, h)

        # 2. Filter active workers capable of running Python/ML workloads
        capable_workers: List[Any] = []
        for w in active_workers:
            if w is None:
                capable_workers.append(None)
                continue
            # Check worker status if WorkerInfo
            if hasattr(w, "status") and w.status in (WorkerStatus.OFFLINE, WorkerStatus.DRAINING):
                continue
            if hasattr(w, "can_execute_workloads") and not w.can_execute_workloads:
                continue
            # Check capabilities if list/set/tuple of strings
            caps = getattr(w, "capabilities", None)
            if isinstance(caps, (list, set, tuple)):
                if not any(c in caps for c in ["python", "ml_training", "custom_script"]):
                    continue
            # Check tags if dict
            tags = getattr(w, "tags", None)
            if isinstance(tags, dict):
                if tags.get("cap:python") == "false" and tags.get("cap:pytorch") == "false":
                    continue
            capable_workers.append(w)

        if not capable_workers:
            capable_workers = [None]

        num_workers = len(capable_workers)

        # 3. Compute continuous compute power scores
        scores: List[float] = []
        for w in capable_workers:
            if w is not None:
                prof = getattr(w, "resource_profile", None)
                if prof is not None and isinstance(prof, WorkerResourceProfile):
                    scores.append(compute_worker_throughput_score(prof, requires_gpu=requires_gpu))
                else:
                    scores.append(4.0)
            else:
                scores.append(4.0)

        total_score = sum(scores) or float(num_workers)
        shares = [s / total_score for s in scores]

        # 4. Assign contiguous dataset slices [start_ratio_i, end_ratio_i]
        specs: List[WorkloadSpec] = []
        curr_ratio = 0.0

        for i, (w, score, share) in enumerate(zip(capable_workers, scores, shares)):
            start_ratio = curr_ratio
            end_ratio = 1.0 if i == num_workers - 1 else round(curr_ratio + share, 4)
            curr_ratio = end_ratio

            # Calculate optimal thread count and memory tuning
            worker_id = f"worker-{i}"
            optimal_threads = 4
            low_ram_mode = "0"

            if w is not None:
                prof = getattr(w, "resource_profile", None)
                worker_id = getattr(w, "worker_id", worker_id)
                if prof is not None:
                    cpus = getattr(prof, "cpu_cores_logical", 0) or getattr(prof, "cpu_cores_total", 0) or 4
                    optimal_threads = max(1, cpus - 2) if cpus >= 8 else max(1, cpus - 1)
                    free_ram_gb = (getattr(prof, "ram_available_bytes", 0) or 0) / (1024**3)
                    low_ram_mode = "1" if free_ram_gb < 3.5 else "0"

            spec = WorkloadSpec(
                workload_id=f"{job_id}-chunk-{i}",
                job_id=job_id,
                task_type="ml_training",
                input_asset_hashes=input_asset_hashes,
                min_cpu_cores=optimal_threads,
                min_ram_bytes=4 * 1024**3,
                requires_gpu=requires_gpu,
                estimated_duration_seconds=float(epochs) * 30.0,
                parameters={
                    "script": script_path,
                    "dataset": dataset_path,
                    "script_hash": script_hash,
                    "dataset_hash": dataset_hash,
                    "epochs": epochs,
                    "batch_size": batch_size,
                    "shard_index": i,
                    "total_shards": num_workers,
                    "start_ratio": start_ratio,
                    "end_ratio": end_ratio,
                    "throughput_score": score,
                    "optimal_threads": optimal_threads,
                    "low_ram_mode": low_ram_mode,
                    "worker_id": worker_id,
                },
            )
            spec.execution_spec = self.build_execution_spec(spec)
            specs.append(spec)

        return specs

    def evaluate_request(
        self,
        request: dict,
        active_workers: Optional[Sequence[Any]] = None,
    ) -> List[WorkloadSpec]:
        """Parse ML training request, calculate requirements, and output WorkloadSpec(s)."""
        workers = active_workers or request.get("active_workers")
        worker_count = int(request.get("worker_count", 1))

        if workers or worker_count > 1:
            worker_list = list(workers) if workers else [None] * worker_count
            return self.plan_shards(request, worker_list)

        # Single-worker default execution
        script_path = request.get("script_path") or request.get("script") or "train.py"
        dataset_path = request.get("dataset_path") or request.get("dataset") or "dataset"
        epochs = int(request.get("epochs", 1))
        batch_size = int(request.get("batch_size", 32))
        requires_gpu = bool(request.get("requires_gpu", False))

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
                "script": script_path,
                "dataset": dataset_path,
                "script_hash": script_hash,
                "dataset_hash": dataset_hash,
                "epochs": epochs,
                "batch_size": batch_size,
                "shard_index": 0,
                "total_shards": 1,
                "start_ratio": 0.0,
                "end_ratio": 1.0,
                "optimal_threads": 4,
                "low_ram_mode": "0",
                "worker_id": "worker-0",
            },
        )
        spec.execution_spec = self.build_execution_spec(spec)
        return [spec]

    def build_execution_spec(self, workload: WorkloadSpec) -> ExecutionSpec:
        params = workload.parameters or {}
        script_hash = params.get("script_hash")
        dataset_hash = params.get("dataset_hash")
        script = params.get("script") or "train.py"
        dataset = params.get("dataset") or "dataset"
        epochs = params.get("epochs", 1)
        batch_size = params.get("batch_size", 32)

        script_arg = f"inputs/{script_hash}" if script_hash else str(script)
        dataset_arg = f"inputs/{dataset_hash}" if dataset_hash else str(dataset)
        output_path = params.get("output", "./outputs/model.pt")

        args = [
            script_arg,
            "--dataset", dataset_arg,
            "--epochs", str(epochs),
            "--batch-size", str(batch_size),
            "--output", output_path,
        ]

        env = {
            "AIDAR_ADAPTER": "ml_training",
            "AIDAR_SHARD_START_RATIO": str(params.get("start_ratio", "0.0")),
            "AIDAR_SHARD_END_RATIO": str(params.get("end_ratio", "1.0")),
            "AIDAR_OPTIMAL_THREADS": str(params.get("optimal_threads", "4")),
            "AIDAR_LOW_RAM_MODE": str(params.get("low_ram_mode", "0")),
            "AIDAR_WORKER_ID": str(params.get("worker_id", "")),
            "PYTHONPATH": f".{os.pathsep}{os.environ.get('PYTHONPATH', '')}".rstrip(os.pathsep),
        }

        return ExecutionSpec(
            command="python",
            executable="python",
            args=args,
            env=env,
            cwd=None,
            timeout_seconds=workload.estimated_duration_seconds * 3.0,
        )

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        return OutputVerificationPolicy(
            expected_output_count=1,
        )
