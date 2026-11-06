"""M23: ComfyUI Platform Adapter.

Strict Honesty:
ComfyUI node DAG workflows, image generation batches, and conditioning passes
are strictly dispatched as TASK_PARALLEL across cluster nodes. AIDAR explicitly
reports task-parallel batch throughput rather than claiming collaborative model training.
"""
from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional, Sequence

from aidars.adapters.base import ApplicationAdapter
from aidars.adapters.ml_training.lifecycle import DistributionStrategy
from aidars.distributed.models import (
    ExecutionSpec,
    OutputVerificationPolicy,
    WorkloadSpec,
)

logger = logging.getLogger("aidars.adapters.ml_training.platforms.comfyui")


class ComfyUIAdapter(ApplicationAdapter):
    """Adapter for distributed ComfyUI DAG execution and batch generation."""

    def __init__(self, asset_manager: Optional[Any] = None) -> None:
        self.asset_manager = asset_manager

    def validate(self, request: dict) -> bool:
        return "workflow" in request or "workflow_path" in request

    def evaluate_request(
        self,
        request: dict,
        active_workers: Optional[Sequence[Any]] = None,
    ) -> List[WorkloadSpec]:
        workers = list(active_workers or request.get("active_workers") or [None])
        num_workers = len(workers)
        job_id = f"comfyui-{uuid.uuid4().hex[:12]}"

        batch_count = int(request.get("batch_count", 1))
        base_seed = int(request.get("seed", 0))
        all_seeds = [base_seed + i for i in range(batch_count)]
        workflow = request.get("workflow", {})

        per_worker = batch_count // num_workers if num_workers > 0 else batch_count
        remainder = batch_count % num_workers if num_workers > 0 else 0

        specs = []
        for rank, w in enumerate(workers):
            wid = getattr(w, "worker_id", f"worker-{rank}") if w else f"worker-{rank}"
            start_idx = rank * per_worker + min(rank, remainder)
            end_idx = start_idx + per_worker + (1 if rank < remainder else 0)
            assigned_seeds = all_seeds[start_idx:end_idx]

            spec = WorkloadSpec(
                workload_id=f"{job_id}-rank-{rank}",
                job_id=job_id,
                task_type="comfyui_generation",
                pinned_worker_id=wid if wid != f"worker-{rank}" else None,
                parameters={
                    "platform": "comfyui",
                    "execution_paradigm": DistributionStrategy.TASK_PARALLEL.value,
                    "worker_id": wid,
                    "workflow": workflow,
                    "seeds": assigned_seeds,
                    "batch_size": len(assigned_seeds),
                },
            )
            spec.execution_spec = self.build_execution_spec(spec)
            specs.append(spec)

        return specs

    def build_execution_spec(self, workload: WorkloadSpec) -> ExecutionSpec:
        p = workload.parameters or {}
        args = ["-m", "aidars.adapters.ml_training.platforms.comfyui_runner"]
        return ExecutionSpec(command="python", args=args, env={"AIDAR_PLATFORM": "comfyui"})

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        p = workload.parameters or {}
        seeds = p.get("seeds", [])
        return OutputVerificationPolicy(expected_output_count=max(1, len(seeds)))

