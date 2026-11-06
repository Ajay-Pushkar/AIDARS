"""M23: Voicebox Speech Synthesis Platform Adapter.

Separates phoneme/spectrogram preparation (TASK_PARALLEL) from synchronized
acoustic flow-matching model training (FEDERATED_LEARNING / DATA_PARALLEL).
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

logger = logging.getLogger("aidars.adapters.ml_training.platforms.voicebox")


class VoiceboxAdapter(ApplicationAdapter):
    """Adapter for distributed Voicebox audio feature prep and acoustic model training."""

    def __init__(self, asset_manager: Optional[Any] = None) -> None:
        self.asset_manager = asset_manager

    def validate(self, request: dict) -> bool:
        return "stage" in request or "audio_files" in request or "dataset_path" in request

    def evaluate_request(
        self,
        request: dict,
        active_workers: Optional[Sequence[Any]] = None,
    ) -> List[WorkloadSpec]:
        workers = list(active_workers or request.get("active_workers") or [None])
        num_workers = len(workers)
        stage = request.get("stage", "feature_prep")
        job_id = f"voicebox-{stage}-{uuid.uuid4().hex[:12]}"

        # Stage 1: Feature / Phoneme / Spectrogram Prep (TASK_PARALLEL)
        if stage in ("feature_prep", "preprocess", "phoneme", "spectrogram"):
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
                    task_type="voicebox_preprocessing",
                    pinned_worker_id=wid if wid != f"worker-{rank}" else None,
                    parameters={
                        "platform": "voicebox",
                        "stage": stage,
                        "execution_paradigm": DistributionStrategy.TASK_PARALLEL.value,
                        "worker_id": wid,
                        "audio_files": assigned_files,
                    },
                )
                spec.execution_spec = self.build_execution_spec(spec)
                specs.append(spec)
            return specs

        # Stage 2: Acoustic Model Training (FEDERATED_LEARNING)
        scores = [4.0] * num_workers
        for i, w in enumerate(workers):
            prof = getattr(w, "resource_profile", None)
            if prof:
                scores[i] = compute_worker_throughput_score(prof)

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
                task_type="voicebox_training",
                pinned_worker_id=wid if wid != f"worker-{rank}" else None,
                parameters={
                    "platform": "voicebox",
                    "stage": stage,
                    "execution_paradigm": DistributionStrategy.FEDERATED_LEARNING.value,
                    "worker_id": wid,
                    "rank": rank,
                    "world_size": num_workers,
                    "throughput_score": s,
                    "start_ratio": start_r,
                    "end_ratio": end_r,
                    "dataset_path": request.get("dataset_path", "./data/speech"),
                },
            )
            spec.execution_spec = self.build_execution_spec(spec)
            specs.append(spec)

        return specs

    def build_execution_spec(self, workload: WorkloadSpec) -> ExecutionSpec:
        p = workload.parameters or {}
        stage = p.get("stage", "feature_prep")
        args = ["-m", "aidars.adapters.ml_training.platforms.voicebox_runner", f"--stage={stage}"]
        return ExecutionSpec(command="python", args=args, env={"AIDAR_PLATFORM": "voicebox"})

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        return OutputVerificationPolicy(expected_output_count=1)

