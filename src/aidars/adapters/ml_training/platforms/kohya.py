"""M23: Kohya (sd-scripts) LoRA / DreamBooth Training Adapter.

Wraps accelerate launch / torchrun for train_network.py:
- Auto-detects physical LAN subnet interface (GLOO_SOCKET_IFNAME).
- Injects PowerWeightedDistributedSampler so heterogeneous GPUs/CPUs train in lockstep.
- Auto-enables --gradient_checkpointing and --cache_latents_to_disk on low-RAM (<3.5GB) workers.
"""
from __future__ import annotations

import logging
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from aidars.adapters.base import ApplicationAdapter
from aidars.adapters.ml_training.lifecycle import DistributionStrategy
from aidars.adapters.pytorch_ddp.adapter import detect_lan_network_interface
from aidars.distributed.models import (
    ExecutionSpec,
    OutputVerificationPolicy,
    WorkloadSpec,
)
from aidars.distributed.placement import compute_worker_throughput_score

logger = logging.getLogger(__name__)


class KohyaAdapter(ApplicationAdapter):
    """Dedicated integration adapter for Kohya sd-scripts LoRA/DreamBooth training."""

    def __init__(self, asset_manager: Optional[Any] = None) -> None:
        self.asset_manager = asset_manager

    def validate(self, request: dict) -> bool:
        if "pretrained_model_name_or_path" not in request and "train_data_dir" not in request:
            return False
        return True

    def evaluate_request(
        self,
        request: dict,
        active_workers: Optional[Sequence[Any]] = None,
    ) -> List[WorkloadSpec]:
        """Generate rank-specific WorkloadSpecs for multi-PC Kohya training."""
        workers = list(active_workers or request.get("active_workers") or [None])
        num_workers = len(workers)
        job_id = f"kohya-lora-{uuid.uuid4().hex[:12]}"
        master_addr = request.get("master_addr", "127.0.0.1")
        master_port = int(request.get("master_port", 29500))
        lan_ifname = detect_lan_network_interface(master_addr) or ""

        scores = [4.0] * num_workers
        for i, w in enumerate(workers):
            if w and hasattr(w, "resource_profile") and w.resource_profile:
                scores[i] = compute_worker_throughput_score(w.resource_profile, requires_gpu=request.get("requires_gpu", True))

        total_score = sum(scores) or float(num_workers)
        shares = [s / total_score for s in scores]

        specs = []
        curr = 0.0
        for rank, (w, s, share) in enumerate(zip(workers, scores, shares)):
            start_r = curr
            end_r = 1.0 if rank == num_workers - 1 else round(curr + share, 4)
            curr = end_r

            wid = getattr(w, "worker_id", f"worker-{rank}") if w else f"worker-{rank}"
            free_ram_gb = 8.0
            if w and hasattr(w, "resource_profile") and w.resource_profile:
                free_ram_gb = float(getattr(w.resource_profile, "ram_available_bytes", 0) or 0) / (1024**3)

            low_ram = free_ram_gb < 3.5

            spec = WorkloadSpec(
                workload_id=f"{job_id}-rank-{rank}",
                job_id=job_id,
                task_type="kohya_training",
                requires_gpu=bool(request.get("requires_gpu", True)),
                parameters={
                    "platform": "kohya",
                    "execution_paradigm": DistributionStrategy.DATA_PARALLEL.value,
                    "worker_id": wid,
                    "rank": rank,
                    "world_size": num_workers,
                    "master_addr": master_addr,
                    "master_port": master_port,
                    "gloo_socket_ifname": lan_ifname,
                    "throughput_score": s,
                    "sample_share": share,
                    "start_ratio": start_r,
                    "end_ratio": end_r,
                    "pretrained_model": request.get("pretrained_model_name_or_path", "runwayml/stable-diffusion-v1-5"),
                    "train_data_dir": request.get("train_data_dir", "inputs/train_data"),
                    "output_dir": request.get("output_dir", "outputs"),
                    "max_train_epochs": int(request.get("max_train_epochs", 10)),
                    "network_dim": int(request.get("network_dim", 32)),
                    "low_ram_mode": low_ram,
                    "gradient_checkpointing": True if low_ram else bool(request.get("gradient_checkpointing", False)),
                    "cache_latents_to_disk": True if low_ram else bool(request.get("cache_latents_to_disk", False)),
                    "mixed_precision": "fp16" if low_ram else request.get("mixed_precision", "no"),
                },
            )
            spec.execution_spec = self.build_execution_spec(spec)
            specs.append(spec)

        return specs

    def build_execution_spec(self, workload: WorkloadSpec) -> ExecutionSpec:
        p = workload.parameters or {}
        args = [
            "train_network.py",
            f"--pretrained_model_name_or_path={p.get('pretrained_model')}",
            f"--train_data_dir={p.get('train_data_dir')}",
            f"--output_dir={p.get('output_dir')}",
            f"--max_train_epochs={p.get('max_train_epochs')}",
            f"--network_dim={p.get('network_dim')}",
        ]
        if p.get("gradient_checkpointing"):
            args.append("--gradient_checkpointing")
        if p.get("cache_latents_to_disk"):
            args.append("--cache_latents_to_disk")
        if p.get("mixed_precision") and p.get("mixed_precision") != "no":
            args.append(f"--mixed_precision={p.get('mixed_precision')}")

        env = {
            "AIDAR_ADAPTER": "kohya",
            "AIDAR_EXECUTION_PARADIGM": str(p.get("execution_paradigm", DistributionStrategy.DATA_PARALLEL.value)),
            "RANK": str(p.get("rank", 0)),
            "LOCAL_RANK": "0",
            "WORLD_SIZE": str(p.get("world_size", 1)),
            "MASTER_ADDR": str(p.get("master_addr", "127.0.0.1")),
            "MASTER_PORT": str(p.get("master_port", 29500)),
            "GLOO_SOCKET_IFNAME": str(p.get("gloo_socket_ifname", "")),
            "AIDAR_SHARD_START_RATIO": str(p.get("start_ratio", 0.0)),
            "AIDAR_SHARD_END_RATIO": str(p.get("end_ratio", 1.0)),
            "PYTHONPATH": f".{os.pathsep}{os.environ.get('PYTHONPATH', '')}".rstrip(os.pathsep),
        }

        return ExecutionSpec(command="python", args=args, env=env)

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        return OutputVerificationPolicy(expected_output_count=1)
