"""M23: Retrieval-based Voice Conversion (RVC) Platform Adapter.

Strict Honesty & Stage Partitioning:
- Stages 1-3 (Audio Slicing, RMVPE/Harvest F0 Extraction, HuBERT Embedding Extraction):
  Distributed as TASK_PARALLEL across all active PCs; outputs merged into CAS.
- Stage 4 (VITS/NSF Generator Training):
  Distributed as DATA_PARALLEL with synchronized checkpoints and power-weighted dataset sharding.
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
from aidars.distributed.placement import compute_worker_throughput_score

logger = logging.getLogger("aidars.adapters.ml_training.platforms.rvc")


class RVCAdapter(ApplicationAdapter):
    """Adapter for distributed RVC audio preprocessing and voice model training."""

    def __init__(self, asset_manager: Optional[Any] = None) -> None:
        self.asset_manager = asset_manager

    def validate(self, request: dict) -> bool:
        return "stage" in request or "audio_files" in request

    def evaluate_request(
        self,
        request: dict,
        active_workers: Optional[Sequence[Any]] = None,
    ) -> List[WorkloadSpec]:
        workers = list(active_workers or request.get("active_workers") or [None])
        num_workers = len(workers)
        stage = request.get("stage", "extract_f0")
        job_id = f"rvc-{stage}-{uuid.uuid4().hex[:12]}"

        # Stages 1-3: Preprocessing (TASK_PARALLEL)
        if stage in ("slice", "extract_f0", "hubert", "preprocess"):
            audio_files = list(request.get("audio_files", []))
            n_files = len(audio_files)
            chunk_size = max(1, (n_files + num_workers - 1) // num_workers) if num_workers > 0 else n_files

            specs = []
            for rank, w in enumerate(workers):
                wid = getattr(w, "worker_id", f"worker-{rank}") if w else f"worker-{rank}"
                start_idx = rank * chunk_size
                end_idx = min(n_files, (rank + 1) * chunk_size)
                assigned_files = audio_files[start_idx:end_idx]

                spec = WorkloadSpec(
                    workload_id=f"{job_id}-rank-{rank}",
                    job_id=job_id,
                    task_type="rvc_preprocessing",
                    pinned_worker_id=wid if wid != f"worker-{rank}" else None,
                    parameters={
                        "platform": "rvc",
                        "stage": stage,
                        "execution_paradigm": DistributionStrategy.TASK_PARALLEL.value,
                        "worker_id": wid,
                        "audio_files": assigned_files,
                        "f0_method": request.get("f0_method", "rmvpe"),
                    },
                )
                spec.execution_spec = self.build_execution_spec(spec)
                specs.append(spec)
            return specs

        # Stage 4: Generator Training (DATA_PARALLEL)
        scores = [4.0] * num_workers
        for i, w in enumerate(workers):
            prof = getattr(w, "resource_profile", None)
            if prof:
                scores[i] = compute_worker_throughput_score(prof, requires_gpu=True)

        total_score = sum(scores) or float(num_workers)
        shares = [s / total_score for s in scores]

        specs = []
        curr = 0.0
        for rank, (w, s, share) in enumerate(zip(workers, scores, shares)):
            start_r = curr
            end_r = 1.0 if rank == num_workers - 1 else round(curr + share, 4)
            curr = end_r
            wid = getattr(w, "worker_id", f"worker-{rank}") if w else f"worker-{rank}"

            spec = WorkloadSpec(
                workload_id=f"{job_id}-rank-{rank}",
                job_id=job_id,
                task_type="rvc_training",
                pinned_worker_id=wid if wid != f"worker-{rank}" else None,
                parameters={
                    "platform": "rvc",
                    "stage": stage,
                    "execution_paradigm": DistributionStrategy.DATA_PARALLEL.value,
                    "worker_id": wid,
                    "rank": rank,
                    "world_size": num_workers,
                    "throughput_score": s,
                    "start_ratio": start_r,
                    "end_ratio": end_r,
                    "epochs": int(request.get("epochs", 20)),
                    "batch_size": int(request.get("batch_size", 4)),
                },
            )
            spec.execution_spec = self.build_execution_spec(spec)
            specs.append(spec)

        return specs

    def build_execution_spec(self, workload: WorkloadSpec) -> ExecutionSpec:
        p = workload.parameters or {}
        stage = p.get("stage", "extract_f0")
        args = ["-m", f"aidars.adapters.ml_training.platforms.rvc_runner", f"--stage={stage}"]
        return ExecutionSpec(command="python", args=args, env={"AIDAR_PLATFORM": "rvc"})

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        return OutputVerificationPolicy(expected_output_count=1)

