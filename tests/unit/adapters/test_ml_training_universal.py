"""Unit tests for Universal Multi-Low-End-PC ML Training Engine.

Tests:
1. Power-proportional dataset sharding and low-RAM auto-tuner (MLTrainingAdapter.plan_shards).
2. Transparent sitecustomize.py auto-sharding hook, thread pool locking, and micro-batching.
3. Universal Federated Model Merger (FedAvg / DiLoCo Model Soup) for .pt, .npz, .json, and Voicebox/RVC features.
4. Windows-Safe Heterogeneous PyTorch DDP Adapter and PowerWeightedDistributedSampler.
5. Drop-in CLI (aidars-ml-run) workflow.
"""
from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List
import unittest.mock as mock

import numpy as np
import pytest

from aidars.adapters.ml_training.adapter import MLTrainingAdapter
from aidars.adapters.ml_training.merger import merge_worker_ml_outputs, _normalize_weights
from aidars.adapters.ml_training.sitecustomize_hook import (
    configure_thread_pools,
    generate_sitecustomize_code,
    get_shard_range,
    materialize_script_for_execution,
    patch_torch_dataloader,
    shard_dataset,
)
from aidars.adapters.pytorch_ddp.adapter import (
    PowerWeightedDistributedSampler,
    PyTorchDDPAdapter,
    detect_lan_network_interface,
)
from aidars.cli.ml_run import run_ml_distributed
from aidars.distributed.models import WorkerInfo, WorkerResourceProfile, WorkloadSpec


# ============================================================================
# 1. Power-Proportional Dataset Sharding & Low-RAM Auto-Tuner Tests
# ============================================================================

def test_ml_training_power_proportional_sharding_heterogeneous_cluster():
    """Verify MLTrainingAdapter.plan_shards proportionally divides datasets and auto-tunes threads/memory."""
    # Worker 0: Strong workstation (16 cores, 32GB RAM, 12GB VRAM GPU)
    p0 = WorkerResourceProfile(
        worker_id="node-strong-gpu",
        endpoint_url="http://192.168.1.10:8001",
        ip_address="192.168.1.10",
        cpu_cores_total=16,
        cpu_cores_logical=16,
        cpu_cores_physical=8,
        cpu_freq_mhz=3600.0,
        cpu_utilization_percent=10.0,
        ram_total_bytes=32 * 1024**3,
        ram_available_bytes=24 * 1024**3,
        has_gpu=True,
        gpu_available=True,
        vram_available_bytes=12 * 1024**3,
    )
    w0 = WorkerInfo(worker_id="node-strong-gpu", endpoint_url="http://192.168.1.10:8001", ip_address="192.168.1.10", port=8001, resource_profile=p0)

    # Worker 1: Mid-tier desktop (8 cores, 16GB RAM, no GPU)
    p1 = WorkerResourceProfile(
        worker_id="node-mid-cpu",
        endpoint_url="http://192.168.1.11:8001",
        ip_address="192.168.1.11",
        cpu_cores_total=8,
        cpu_cores_logical=8,
        cpu_cores_physical=4,
        cpu_freq_mhz=3200.0,
        cpu_utilization_percent=20.0,
        ram_total_bytes=16 * 1024**3,
        ram_available_bytes=10 * 1024**3,
        has_gpu=False,
    )
    w1 = WorkerInfo(worker_id="node-mid-cpu", endpoint_url="http://192.168.1.11:8001", ip_address="192.168.1.11", port=8001, resource_profile=p1)

    # Worker 2: Low-end laptop (4 cores, 4GB RAM, 2GB available < 3.5GB -> Low-RAM Mode)
    p2 = WorkerResourceProfile(
        worker_id="node-low-laptop",
        endpoint_url="http://192.168.1.12:8001",
        ip_address="192.168.1.12",
        cpu_cores_total=4,
        cpu_cores_logical=4,
        cpu_cores_physical=2,
        cpu_freq_mhz=2400.0,
        cpu_utilization_percent=15.0,
        ram_total_bytes=4 * 1024**3,
        ram_available_bytes=2 * 1024**3, # < 3.5 GB free
        has_gpu=False,
    )
    w2 = WorkerInfo(worker_id="node-low-laptop", endpoint_url="http://192.168.1.12:8001", ip_address="192.168.1.12", port=8001, resource_profile=p2)

    adapter = MLTrainingAdapter()
    req = {
        "script": "train_model.py",
        "dataset": "dataset.csv",
        "epochs": 5,
        "batch_size": 64,
        "requires_gpu": True,
    }

    specs = adapter.plan_shards(req, [w0, w1, w2])
    assert len(specs) == 3

    # Check contiguous sharding from 0.0 to 1.0
    s0, s1, s2 = specs[0], specs[1], specs[2]
    assert s0.parameters["start_ratio"] == 0.0
    assert s0.parameters["end_ratio"] == s1.parameters["start_ratio"]
    assert s1.parameters["end_ratio"] == s2.parameters["start_ratio"]
    assert s2.parameters["end_ratio"] == 1.0

    # Strong node gets strictly larger share than low-end laptop
    share0 = s0.parameters["end_ratio"] - s0.parameters["start_ratio"]
    share1 = s1.parameters["end_ratio"] - s1.parameters["start_ratio"]
    share2 = s2.parameters["end_ratio"] - s2.parameters["start_ratio"]
    assert share0 > share1 > share2

    # Thread auto-tuning: 16 cores -> 14 threads (16 - 2), 8 cores -> 6 threads (8 - 2), 4 cores -> 3 threads (4 - 1)
    assert s0.parameters["optimal_threads"] == 14
    assert s1.parameters["optimal_threads"] == 6
    assert s2.parameters["optimal_threads"] == 3

    # Low-RAM detection: Laptop has 2GB < 3.5GB -> low_ram_mode = "1"
    assert s0.parameters["low_ram_mode"] == "0"
    assert s1.parameters["low_ram_mode"] == "0"
    assert s2.parameters["low_ram_mode"] == "1"

    # Environment variables in ExecutionSpec
    assert s2.execution_spec is not None
    env2 = s2.execution_spec.env
    assert env2["AIDAR_LOW_RAM_MODE"] == "1"
    assert env2["AIDAR_OPTIMAL_THREADS"] == "3"
    assert float(env2["AIDAR_SHARD_START_RATIO"]) == s2.parameters["start_ratio"]
    assert float(env2["AIDAR_SHARD_END_RATIO"]) == 1.0
    assert "PYTHONPATH" in env2


def test_ml_training_adapter_evaluate_request_single_and_multi_worker():
    """Verify evaluate_request conforms to contract and handles single & multi-worker requests."""
    adapter = MLTrainingAdapter()
    req = {"script": "train.py", "dataset": "data.bin", "epochs": 2}
    specs = adapter.evaluate_request(req)
    assert len(specs) == 1
    assert specs[0].parameters["start_ratio"] == 0.0
    assert specs[0].parameters["end_ratio"] == 1.0
    assert specs[0].parameters["optimal_threads"] == 4
    assert specs[0].execution_spec is not None

    # Multi-worker request
    req_multi = {"script": "train.py", "dataset": "data.bin", "worker_count": 4}
    specs_multi = adapter.evaluate_request(req_multi)
    assert len(specs_multi) == 4
    assert specs_multi[0].parameters["start_ratio"] == 0.0
    assert specs_multi[-1].parameters["end_ratio"] == 1.0


# ============================================================================
# 2. sitecustomize_hook.py Auto-Sharding & Low-RAM Micro-Batching Tests
# ============================================================================

def test_sitecustomize_thread_pool_configuration():
    """Verify configure_thread_pools locks OMP/MKL/OpenBLAS/NumExpr env vars."""
    old_env = os.environ.get("AIDAR_OPTIMAL_THREADS")
    try:
        os.environ["AIDAR_OPTIMAL_THREADS"] = "6"
        t = configure_thread_pools()
        assert t == 6
        assert os.environ["OMP_NUM_THREADS"] == "6"
        assert os.environ["MKL_NUM_THREADS"] == "6"
        assert os.environ["OPENBLAS_NUM_THREADS"] == "6"
        assert os.environ["NUMEXPR_NUM_THREADS"] == "6"
    finally:
        if old_env is not None:
            os.environ["AIDAR_OPTIMAL_THREADS"] = old_env
        else:
            os.environ.pop("AIDAR_OPTIMAL_THREADS", None)


def test_sitecustomize_get_shard_range_and_dataset_slicing():
    """Verify get_shard_range and shard_dataset transparently slice collections."""
    items = list(range(100))
    s, e = get_shard_range(100, start_ratio=0.0, end_ratio=0.25)
    assert (s, e) == (0, 25)

    s, e = get_shard_range(100, start_ratio=0.25, end_ratio=0.75)
    assert (s, e) == (25, 75)

    s, e = get_shard_range(100, start_ratio=0.75, end_ratio=1.0)
    assert (s, e) == (75, 100)

    # Shard slicing using environment variables
    old_s = os.environ.get("AIDAR_SHARD_START_RATIO")
    old_e = os.environ.get("AIDAR_SHARD_END_RATIO")
    try:
        os.environ["AIDAR_SHARD_START_RATIO"] = "0.5"
        os.environ["AIDAR_SHARD_END_RATIO"] = "1.0"
        sliced = shard_dataset(items)
        assert sliced == list(range(50, 100))
    finally:
        if old_s: os.environ["AIDAR_SHARD_START_RATIO"] = old_s
        else: os.environ.pop("AIDAR_SHARD_START_RATIO", None)
        if old_e: os.environ["AIDAR_SHARD_END_RATIO"] = old_e
        else: os.environ.pop("AIDAR_SHARD_END_RATIO", None)


def test_sitecustomize_torch_dataloader_patching():
    """Verify DataLoader monkey-patch automatically shards dataset and micro-batches on low RAM."""
    pytest.importorskip("torch")
    import torch
    import torch.utils.data

    patch_torch_dataloader()

    dataset = list(range(100))
    old_s = os.environ.get("AIDAR_SHARD_START_RATIO")
    old_e = os.environ.get("AIDAR_SHARD_END_RATIO")
    old_ram = os.environ.get("AIDAR_LOW_RAM_MODE")

    try:
        # Shard slice [0.0, 0.4] (40 items), Low RAM = True
        os.environ["AIDAR_SHARD_START_RATIO"] = "0.0"
        os.environ["AIDAR_SHARD_END_RATIO"] = "0.4"
        os.environ["AIDAR_LOW_RAM_MODE"] = "1"

        loader = torch.utils.data.DataLoader(dataset, batch_size=16, pin_memory=True, num_workers=2)
        # Verify dataset was wrapped in Subset with 40 elements
        assert len(loader.dataset) == 40
        # Verify low RAM halved batch size: 16 // 2 = 8
        assert loader.batch_size == 8
        # Verify pin_memory disabled and num_workers=0
        assert loader.pin_memory is False
        assert loader.num_workers == 0
    finally:
        if old_s: os.environ["AIDAR_SHARD_START_RATIO"] = old_s
        else: os.environ.pop("AIDAR_SHARD_START_RATIO", None)
        if old_e: os.environ["AIDAR_SHARD_END_RATIO"] = old_e
        else: os.environ.pop("AIDAR_SHARD_END_RATIO", None)
        if old_ram: os.environ["AIDAR_LOW_RAM_MODE"] = old_ram
        else: os.environ.pop("AIDAR_LOW_RAM_MODE", None)


def test_materialize_script_for_execution(tmp_path):
    """Verify user script and sitecustomize.py are written to disk for Windows spawn safety."""
    code = "import sitecustomize\nprint('Training script executed')"
    workdir = str(tmp_path / "sandbox")
    target = materialize_script_for_execution(code, workdir)

    assert os.path.exists(target)
    assert os.path.basename(target) == "_user_ml_train.py"
    assert os.path.exists(os.path.join(workdir, "sitecustomize.py"))

    with open(os.path.join(workdir, "sitecustomize.py"), "r", encoding="utf-8") as f:
        site_content = f.read()
    assert "configure_thread_pools" in site_content
    assert "patch_torch_dataloader" in site_content


# ============================================================================
# 3. Universal Federated Model Merger (FedAvg / DiLoCo) Tests
# ============================================================================

def test_fedavg_pytorch_checkpoint_merger(tmp_path):
    """Verify Sample-Weighted Federated Averaging across multiple PyTorch checkpoints."""
    pytest.importorskip("torch")
    import torch

    w1_dir = tmp_path / "worker_1"
    w2_dir = tmp_path / "worker_2"
    w1_dir.mkdir()
    w2_dir.mkdir()

    # Worker 1: trained on 60 samples, weight = [1.0, 2.0]
    sd1 = {
        "linear.weight": torch.tensor([1.0, 2.0]),
        "linear.bias": torch.tensor([0.5]),
        "num_batches_tracked": torch.tensor(10),
    }
    torch.save(sd1, str(w1_dir / "model.pt"))

    # Worker 2: trained on 40 samples, weight = [3.0, 4.0]
    sd2 = {
        "linear.weight": torch.tensor([3.0, 4.0]),
        "linear.bias": torch.tensor([1.5]),
        "num_batches_tracked": torch.tensor(15),
    }
    torch.save(sd2, str(w2_dir / "model.pt"))

    # Normalized weights: 60/100 = 0.6, 40/100 = 0.4
    # Expected weight: 0.6 * [1.0, 2.0] + 0.4 * [3.0, 4.0] = [0.6+1.2, 1.2+1.6] = [1.8, 2.8]
    # Expected bias: 0.6 * 0.5 + 0.4 * 1.5 = 0.3 + 0.6 = 0.9
    # Expected batches tracked: 10 + 15 = 25
    dest_dir = tmp_path / "merged"
    merged_results = merge_worker_ml_outputs(
        {"worker_1": w1_dir, "worker_2": w2_dir},
        dest_dir,
        worker_weights={"worker_1": 60.0, "worker_2": 40.0},
    )

    assert "model.pt" in merged_results
    merged_path = merged_results["model.pt"]
    merged_sd = torch.load(merged_path, map_location="cpu", weights_only=False)

    assert torch.allclose(merged_sd["linear.weight"], torch.tensor([1.8, 2.8]), atol=1e-5)
    assert torch.allclose(merged_sd["linear.bias"], torch.tensor([0.9]), atol=1e-5)
    assert merged_sd["num_batches_tracked"].item() == 25


def test_fedavg_npz_checkpoint_merger(tmp_path):
    """Verify Sample-Weighted Federated Averaging across NumPy .npz checkpoints."""
    w1_dir = tmp_path / "w1"
    w2_dir = tmp_path / "w2"
    w1_dir.mkdir()
    w2_dir.mkdir()

    arr1 = np.array([2.0, 4.0], dtype=np.float32)
    arr2 = np.array([6.0, 8.0], dtype=np.float32)

    np.savez_compressed(str(w1_dir / "weights.npz"), W=arr1)
    np.savez_compressed(str(w2_dir / "weights.npz"), W=arr2)

    # Weights 50/50: 0.5 * [2, 4] + 0.5 * [6, 8] = [4.0, 6.0]
    dest_dir = tmp_path / "merged_npz"
    merged_results = merge_worker_ml_outputs([w1_dir, w2_dir], dest_dir)

    assert "weights.npz" in merged_results
    with np.load(merged_results["weights.npz"]) as npz:
        assert np.allclose(npz["W"], np.array([4.0, 6.0], dtype=np.float32))


def test_fedavg_json_weights_merger(tmp_path):
    """Verify Federated Averaging across JSON weight dicts."""
    w1_dir = tmp_path / "w1"
    w2_dir = tmp_path / "w2"
    w1_dir.mkdir()
    w2_dir.mkdir()

    with open(w1_dir / "params.json", "w") as f:
        json.dump({"lr": 0.01, "loss": 0.4, "coefs": [1.0, 2.0]}, f)
    with open(w2_dir / "params.json", "w") as f:
        json.dump({"lr": 0.01, "loss": 0.2, "coefs": [3.0, 4.0]}, f)

    dest_dir = tmp_path / "merged_json"
    merged_results = merge_worker_ml_outputs([w1_dir, w2_dir], dest_dir)
    assert "params.json" in merged_results
    with open(merged_results["params.json"], "r") as f:
        data = json.load(f)
    assert data["loss"] == pytest.approx(0.3)
    assert data["coefs"] == pytest.approx([2.0, 3.0])


def test_voicebox_rvc_feature_concatenation(tmp_path):
    """Verify feature matrices (total_fea.npy) are concatenated along axis 0 for Voicebox/RVC."""
    w1_dir = tmp_path / "w1"
    w2_dir = tmp_path / "w2"
    w1_dir.mkdir()
    w2_dir.mkdir()

    fea1 = np.ones((10, 256), dtype=np.float32)
    fea2 = np.ones((15, 256), dtype=np.float32) * 2.0

    np.save(str(w1_dir / "total_fea.npy"), fea1)
    np.save(str(w2_dir / "total_fea.npy"), fea2)

    # Audio slices
    (w1_dir / "slice_0.wav").write_text("wav0")
    (w2_dir / "slice_1.wav").write_text("wav1")

    dest_dir = tmp_path / "merged_rvc"
    merged_results = merge_worker_ml_outputs([w1_dir, w2_dir], dest_dir)

    assert "total_fea.npy" in merged_results
    total_fea = np.load(merged_results["total_fea.npy"])
    assert total_fea.shape == (25, 256)
    assert total_fea[:10].mean() == 1.0
    assert total_fea[10:].mean() == 2.0

    assert "slice_0.wav" in merged_results
    assert "slice_1.wav" in merged_results


# ============================================================================
# 4. Windows-Safe Heterogeneous PyTorch DDP Adapter Tests
# ============================================================================

def test_pytorch_ddp_power_weighted_distributed_sampler():
    """Verify PowerWeightedDistributedSampler partitions indices proportionally across ranks."""
    dataset = list(range(100))
    # 3 ranks with throughput scores: 10.0 (50%), 5.0 (25%), 5.0 (25%)
    scores = [10.0, 5.0, 5.0]

    s0 = PowerWeightedDistributedSampler(dataset, rank=0, world_size=3, throughput_scores=scores, shuffle=False)
    s1 = PowerWeightedDistributedSampler(dataset, rank=1, world_size=3, throughput_scores=scores, shuffle=False)
    s2 = PowerWeightedDistributedSampler(dataset, rank=2, world_size=3, throughput_scores=scores, shuffle=False)

    idx0 = list(s0)
    idx1 = list(s1)
    idx2 = list(s2)

    assert len(idx0) == 50
    assert len(idx1) == 25
    assert len(idx2) == 25
    assert idx0 == list(range(0, 50))
    assert idx1 == list(range(50, 75))
    assert idx2 == list(range(75, 100))


def test_pytorch_ddp_adapter_evaluate_request():
    """Verify PyTorchDDPAdapter produces rank specs with GLOO_SOCKET_IFNAME and rendezvous env."""
    adapter = PyTorchDDPAdapter()
    req = {
        "script": "train_ddp.py",
        "dataset": "mnist",
        "master_addr": "192.168.1.50",
        "master_port": 29500,
        "worker_count": 2,
    }
    specs = adapter.evaluate_request(req)
    assert len(specs) == 2
    r0, r1 = specs[0], specs[1]

    assert r0.parameters["rank"] == 0
    assert r1.parameters["rank"] == 1
    assert r0.parameters["world_size"] == 2
    assert r0.parameters["master_addr"] == "192.168.1.50"
    assert r0.parameters["master_port"] == 29500

    assert r0.execution_spec is not None
    env0 = r0.execution_spec.env
    assert env0["RANK"] == "0"
    assert env0["WORLD_SIZE"] == "2"
    assert env0["MASTER_ADDR"] == "192.168.1.50"
    assert env0["MASTER_PORT"] == "29500"
    assert "GLOO_SOCKET_IFNAME" in env0


# ============================================================================
# 5. CLI aidars-ml-run Tests
# ============================================================================

@pytest.mark.asyncio
async def test_aidars_ml_run_cli_offline_flow(tmp_path):
    """Verify aidars-ml-run handles standalone / local script execution cleanly."""
    script_file = tmp_path / "mock_train.py"
    script_file.write_text("print('Mock training completed')", encoding="utf-8")
    out_dir = tmp_path / "output"

    exit_code = await run_ml_distributed(
        script_path=str(script_file),
        coordinator_url="http://127.0.0.1:9999", # unreachable mock coordinator
        output_dir=str(out_dir),
        poll_interval=0.1,
        timeout=2.0,
    )
    assert exit_code == 0
    assert out_dir.exists()
