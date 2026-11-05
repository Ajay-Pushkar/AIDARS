"""M22: Windows-Safe Heterogeneous PyTorch DDP Adapter.

Supports DistributedDataParallel across heterogeneous N-PC clusters:
1. Automatically detects the physical LAN network interface matching the Coordinator's
   subnet and sets GLOO_SOCKET_IFNAME / TP_SOCKET_IFNAME to avoid binding to virtual WSL/Hyper-V switches.
2. PowerWeightedDistributedSampler assigns mini-batch slices proportional to each worker's
   compute_worker_throughput_score so fast PCs never stall at gradient barriers waiting for slow PCs.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import random
import socket
import sys
import uuid
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import psutil

from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import ExecutionSpec, OutputVerificationPolicy, WorkerInfo, WorkerResourceProfile, WorkloadSpec
from aidars.distributed.placement import compute_worker_throughput_score

logger = logging.getLogger(__name__)


def detect_lan_network_interface(target_host: Optional[str] = None) -> Optional[str]:
    """Detect the active physical LAN interface to bind Gloo/PyTorch sockets."""
    # 1. If target_host is provided, try to find interface that routes to it
    if target_host:
        try:
            target_ip = socket.gethostbyname(target_host.replace("http://", "").replace("https://", "").split(":")[0])
            target_octets = target_ip.split(".")[:3]
            for ifname, addrs in psutil.net_if_addrs().items():
                if any(v in ifname.lower() for v in ["wsl", "hyper-v", "vethernet", "loopback", "virtualbox", "vmware"]):
                    continue
                for addr in addrs:
                    if addr.family == socket.AF_INET and addr.address.split(".")[:3] == target_octets:
                        return ifname
        except Exception:
            pass

    # 2. General scan: find first active, non-virtual IPv4 interface
    stats = psutil.net_if_stats()
    for ifname, addrs in psutil.net_if_addrs().items():
        if any(v in ifname.lower() for v in ["wsl", "hyper-v", "vethernet", "loopback", "virtualbox", "vmware"]):
            continue
        st = stats.get(ifname)
        if st and st.isup:
            for addr in addrs:
                if addr.family == socket.AF_INET and not addr.address.startswith("127."):
                    return ifname

    # 3. Fallback to any non-loopback interface
    for ifname, addrs in psutil.net_if_addrs().items():
        for addr in addrs:
            if addr.family == socket.AF_INET and not addr.address.startswith("127."):
                return ifname

    return None


class PowerWeightedDistributedSampler:
    """Distributed Sampler that assigns mini-batch slices proportional to each rank's throughput score.

    Ensures that heterogeneous PCs (e.g. 16-core workstation vs 4-core laptop) process
    proportional data volumes and finish their mini-batches concurrently without stalling
    at gradient synchronization barriers.
    """

    def __init__(
        self,
        dataset: Any,
        rank: int,
        world_size: int,
        throughput_scores: Sequence[float],
        shuffle: bool = True,
        seed: int = 0,
        drop_last: bool = False,
    ) -> None:
        self.dataset = dataset
        self.rank = rank
        self.world_size = world_size
        self.scores = [max(0.1, float(s)) for s in throughput_scores]
        if len(self.scores) < world_size:
            self.scores.extend([1.0] * (world_size - len(self.scores)))
        self.shuffle = shuffle
        self.seed = seed
        self.drop_last = drop_last
        self.epoch = 0

        # Compute normalized shares sum(share_i) = 1.0
        total_score = sum(self.scores[:world_size]) or float(world_size)
        self.shares = [s / total_score for s in self.scores[:world_size]]

    def __iter__(self) -> Iterator[int]:
        n = len(self.dataset)
        indices = list(range(n))
        if self.shuffle:
            g = random.Random(self.seed + self.epoch)
            g.shuffle(indices)

        # Partition indices proportionally across ranks
        start_idx = int(round(n * sum(self.shares[:self.rank])))
        end_idx = int(round(n * sum(self.shares[:self.rank + 1])))
        start_idx = max(0, min(n, start_idx))
        end_idx = max(start_idx, min(n, end_idx))
        if end_idx == start_idx and n > 0:
            end_idx = min(n, start_idx + 1)

        rank_indices = indices[start_idx:end_idx]
        return iter(rank_indices)

    def __len__(self) -> int:
        n = len(self.dataset)
        start_idx = int(round(n * sum(self.shares[:self.rank])))
        end_idx = int(round(n * sum(self.shares[:self.rank + 1])))
        return max(1 if n > 0 else 0, end_idx - start_idx)

    def set_epoch(self, epoch: int) -> None:
        self.epoch = epoch


class PyTorchDDPAdapter(ApplicationAdapter):
    """Adapter for multi-PC PyTorch DistributedDataParallel workloads."""

    def __init__(self, asset_manager: Optional[Any] = None) -> None:
        self.asset_manager = asset_manager

    def validate(self, request: dict) -> bool:
        if "script_path" not in request and "script" not in request:
            return False
        return True

    def evaluate_request(
        self,
        request: dict,
        active_workers: Optional[Sequence[Any]] = None,
    ) -> List[WorkloadSpec]:
        """Generate rank-specific WorkloadSpecs for heterogeneous N-PC DDP execution."""
        script_path = request.get("script_path") or request.get("script") or "train.py"
        dataset_path = request.get("dataset_path") or request.get("dataset")
        epochs = int(request.get("epochs", 1))
        batch_size = int(request.get("batch_size", 32))
        requires_gpu = bool(request.get("requires_gpu", False))
        master_addr = request.get("master_addr") or "127.0.0.1"
        master_port = int(request.get("master_port", 29500))

        # Filter and assess active workers
        workers = list(active_workers or request.get("active_workers") or [])
        if not workers:
            # Fallback to worker_count specification
            w_count = int(request.get("worker_count", 1))
            workers = [None] * max(1, w_count)

        world_size = len(workers)
        scores: List[float] = []
        for w in workers:
            if w is not None:
                prof = getattr(w, "resource_profile", None)
                if prof is not None and isinstance(prof, WorkerResourceProfile):
                    scores.append(compute_worker_throughput_score(prof, requires_gpu=requires_gpu))
                else:
                    scores.append(4.0)
            else:
                scores.append(4.0)

        total_score = sum(scores) or float(world_size)
        shares = [s / total_score for s in scores]

        # Ingest assets into CAS if asset_manager supplied
        input_asset_hashes = set()
        script_hash = None
        dataset_hash = None
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

        lan_ifname = detect_lan_network_interface(master_addr) or ""
        job_id = f"ddp-job-{uuid.uuid4().hex[:16]}"
        specs: List[WorkloadSpec] = []

        curr_ratio = 0.0
        for rank, (w, score, share) in enumerate(zip(workers, scores, shares)):
            start_ratio = curr_ratio
            end_ratio = 1.0 if rank == world_size - 1 else round(curr_ratio + share, 4)
            curr_ratio = end_ratio

            # Calculate node optimal threads and low RAM mode
            optimal_threads = 4
            low_ram_mode = "0"
            worker_id = f"worker-{rank}"
            if w is not None:
                prof = getattr(w, "resource_profile", None)
                worker_id = getattr(w, "worker_id", worker_id)
                if prof is not None:
                    cpus = getattr(prof, "cpu_cores_logical", 0) or 4
                    optimal_threads = max(1, cpus - 2) if cpus >= 8 else max(1, cpus - 1)
                    free_ram_gb = (getattr(prof, "ram_available_bytes", 0) or 0) / (1024**3)
                    low_ram_mode = "1" if free_ram_gb < 3.5 else "0"

            spec = WorkloadSpec(
                workload_id=f"{job_id}-rank-{rank}",
                job_id=job_id,
                task_type="pytorch_ddp",
                input_asset_hashes=input_asset_hashes,
                min_cpu_cores=optimal_threads,
                min_ram_bytes=4 * 1024**3,
                requires_gpu=requires_gpu,
                estimated_duration_seconds=float(epochs) * 30.0,
                parameters={
                    "script": script_path,
                    "dataset": dataset_path or "dataset",
                    "script_hash": script_hash,
                    "dataset_hash": dataset_hash,
                    "rank": rank,
                    "world_size": world_size,
                    "master_addr": master_addr,
                    "master_port": master_port,
                    "gloo_socket_ifname": lan_ifname,
                    "throughput_score": score,
                    "start_ratio": start_ratio,
                    "end_ratio": end_ratio,
                    "optimal_threads": optimal_threads,
                    "low_ram_mode": low_ram_mode,
                    "worker_id": worker_id,
                    "epochs": epochs,
                    "batch_size": batch_size,
                },
            )
            spec.execution_spec = self.build_execution_spec(spec)
            specs.append(spec)

        return specs

    def build_execution_spec(self, workload: WorkloadSpec) -> ExecutionSpec:
        params = workload.parameters or {}
        script_hash = params.get("script_hash")
        dataset_hash = params.get("dataset_hash")
        script = params.get("script", "train.py")
        dataset = params.get("dataset", "dataset")

        script_arg = f"inputs/{script_hash}" if script_hash else str(script)
        dataset_arg = f"inputs/{dataset_hash}" if dataset_hash else str(dataset)

        args = [
            script_arg,
            "--dataset", dataset_arg,
            "--epochs", str(params.get("epochs", 1)),
            "--batch-size", str(params.get("batch_size", 32)),
            "--rank", str(params.get("rank", 0)),
            "--world-size", str(params.get("world_size", 1)),
        ]

        lan_ifname = str(params.get("gloo_socket_ifname", ""))
        env = {
            "RANK": str(params.get("rank", 0)),
            "LOCAL_RANK": "0",
            "WORLD_SIZE": str(params.get("world_size", 1)),
            "MASTER_ADDR": str(params.get("master_addr", "127.0.0.1")),
            "MASTER_PORT": str(params.get("master_port", 29500)),
            "GLOO_SOCKET_IFNAME": lan_ifname,
            "TP_SOCKET_IFNAME": lan_ifname,
            "AIDAR_SHARD_START_RATIO": str(params.get("start_ratio", "0.0")),
            "AIDAR_SHARD_END_RATIO": str(params.get("end_ratio", "1.0")),
            "AIDAR_OPTIMAL_THREADS": str(params.get("optimal_threads", "4")),
            "AIDAR_LOW_RAM_MODE": str(params.get("low_ram_mode", "0")),
            "AIDAR_WORKER_ID": str(params.get("worker_id", "")),
            "PYTHONPATH": f".{os.pathsep}{os.environ.get('PYTHONPATH', '')}".rstrip(os.pathsep),
        }

        return ExecutionSpec(
            command="python",
            args=args,
            env=env,
            timeout_seconds=workload.estimated_duration_seconds * 3.0,
        )

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        return OutputVerificationPolicy(expected_output_count=1)

