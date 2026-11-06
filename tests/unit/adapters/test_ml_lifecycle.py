"""Unit tests for the 10-Step Distributed ML Lifecycle, Mandates, Platform Adapters, and SDK.

Tests:
1. Zero Artificial Metric Bonuses (Hard Prohibition) on Real Multimodal Datasets.
2. Real Multimodal Dataset Generators (Acoustic and 3D Geometry) with strict 80/20 split.
3. Coordinated Synchronization Rounds & Weight Delta Aggregation (Anti-Blind Averaging).
4. Strict Honesty in Execution Paradigms (DATA_PARALLEL, MODEL_PARALLEL, FEDERATED_LEARNING, TASK_PARALLEL).
5. 10-Step Lifecycle Orchestration (Detect, Inspect, Select, Partition, Dispatch, Constraints, Sync, Recover, Aggregate, Validate).
6. NaN / Inf Update Rejection & Degradation Fallback.
7. Worker Failure Shard Recovery.
8. Platform Adapters: RVC, Kohya, ComfyUI, Voicebox.
9. Custom Python ML SDK (aidars.sdk.ml): @distributed_train, DataLoader, Pipeline, Gate.
"""
from __future__ import annotations

import copy
import json
import math
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List
import unittest.mock as mock

import numpy as np
import pytest

from aidars.adapters.ml_training.lifecycle import (
    DistributionStrategy,
    InspectionResult,
    MLLifecycleOrchestrator,
    ValidationReport,
    WorkloadDetectionResult,
    create_3d_geometry_multimodal_dataset,
    create_acoustic_multimodal_dataset,
)
from aidars.adapters.ml_training.platforms.comfyui import ComfyUIAdapter
from aidars.adapters.ml_training.platforms.kohya import KohyaAdapter
from aidars.adapters.ml_training.platforms.rvc import RVCAdapter
from aidars.adapters.ml_training.platforms.voicebox import VoiceboxAdapter
from aidars.distributed.models import WorkerInfo, WorkerResourceProfile, WorkloadSpec
from aidars.sdk.ml import (
    AidarMLSDK,
    AidarModelParallelPipeline,
    AidarSynchronizedDataLoader,
    AidarValidationGate,
    distributed_train,
)


# ============================================================================
# Test Fixtures & Utilities
# ============================================================================

def make_test_worker(
    worker_id: str,
    ip: str = "192.168.1.10",
    port: int = 8001,
    cores: int = 8,
    ram_gb: float = 16.0,
    has_gpu: bool = False,
    vram_gb: float = 0.0,
) -> WorkerInfo:
    """Helper to construct valid WorkerInfo instances with full profiles."""
    prof = WorkerResourceProfile(
        worker_id=worker_id,
        endpoint_url=f"http://{ip}:{port}",
        ip_address=ip,
        cpu_cores_total=cores,
        cpu_cores_logical=cores,
        cpu_cores_physical=max(1, cores // 2),
        cpu_freq_mhz=3200.0,
        cpu_utilization_percent=15.0,
        ram_total_bytes=int(ram_gb * 1024**3),
        ram_available_bytes=int(ram_gb * 0.8 * 1024**3),
        has_gpu=has_gpu,
        gpu_available=has_gpu,
        vram_available_bytes=int(vram_gb * 1024**3),
    )
    return WorkerInfo(
        worker_id=worker_id,
        endpoint_url=f"http://{ip}:{port}",
        ip_address=ip,
        port=port,
        resource_profile=prof,
        tags={"cap:python": "3.11", "cap:ml": "torch"},
    )


# ============================================================================
# 1. Real Multimodal Dataset Generators Tests
# ============================================================================

def test_acoustic_multimodal_dataset_generation():
    """Verify acoustic dataset generates realistic multi-harmonic spectrums with strict 80/20 split."""
    sample_count = 120
    n_features = 32
    train_data, val_data = create_acoustic_multimodal_dataset(sample_count=sample_count, n_features=n_features, random_seed=42)

    X_train, y_train = train_data
    X_val, y_val = val_data

    # Strict 80% Train, 20% Held-Out Val
    assert len(X_train) == 96  # 80% of 120
    assert len(X_val) == 24    # 20% of 120
    assert len(y_train) == 96
    assert len(y_val) == 24

    assert X_train.shape == (96, n_features)
    assert X_val.shape == (24, n_features)
    assert X_train.dtype == np.float32
    assert y_train.dtype == np.float32

    # Verify acoustic properties: finite values, non-zero energy
    assert np.isfinite(X_train).all()
    assert np.isfinite(X_val).all()
    assert float(np.mean(X_train)) > 0.0


def test_3d_geometry_multimodal_dataset_generation():
    """Verify 3D geometry dataset generates surface coordinates, unit normals, and radiance targets."""
    sample_count = 100
    train_data, val_data = create_3d_geometry_multimodal_dataset(sample_count=sample_count, random_seed=123)

    X_train, y_train = train_data
    X_val, y_val = val_data

    assert len(X_train) == 80
    assert len(X_val) == 20
    assert X_train.shape == (80, 8)  # [x, y, z, nx, ny, nz, roughness, reflectance]
    assert X_val.shape == (20, 8)

    # Normals (columns 3, 4, 5) must be unit vectors
    normals = X_train[:, 3:6]
    norms = np.linalg.norm(normals, axis=1)
    np.testing.assert_allclose(norms, 1.0, atol=1e-5)

    # Targets must be valid bounded radiance values
    assert (y_train >= 0.0).all()
    assert (y_val >= 0.0).all()


# ============================================================================
# 2. Zero Artificial Metric Bonuses & Validation Tests
# ============================================================================

def test_zero_artificial_metric_bonuses_mathematical_precision():
    """Verify that validate_against_baseline applies ZERO artificial multipliers or bonuses.

    Every metric MUST be the exact mathematical forward-pass MSE on held-out validation samples.
    """
    orchestrator = MLLifecycleOrchestrator()
    train_data, val_data = create_acoustic_multimodal_dataset(sample_count=100, n_features=16, random_seed=99)
    X_val, y_val = val_data

    # Define simple weight checkpoints
    n_feat = 16
    baseline_cp = {"weight": np.ones((n_feat,), dtype=np.float32) * 0.1, "bias": np.float32(0.0)}
    worker_0_cp = {"weight": np.ones((n_feat,), dtype=np.float32) * 0.15, "bias": np.float32(0.05)}
    worker_1_cp = {"weight": np.ones((n_feat,), dtype=np.float32) * 0.12, "bias": np.float32(0.02)}
    merged_cp = {"weight": np.ones((n_feat,), dtype=np.float32) * 0.135, "bias": np.float32(0.035)}

    report = orchestrator.validate_against_baseline(
        merged_checkpoint=merged_cp,
        baseline_checkpoint=baseline_cp,
        worker_checkpoints=[worker_0_cp, worker_1_cp],
        val_dataset=val_data,
        strategy=DistributionStrategy.FEDERATED_LEARNING,
    )

    # Compute expected mathematical MSE directly
    expected_baseline_loss = float(np.mean(((X_val @ baseline_cp["weight"] + baseline_cp["bias"]) - y_val) ** 2))
    expected_merged_loss = float(np.mean(((X_val @ merged_cp["weight"] + merged_cp["bias"]) - y_val) ** 2))
    expected_w0_loss = float(np.mean(((X_val @ worker_0_cp["weight"] + worker_0_cp["bias"]) - y_val) ** 2))
    expected_w1_loss = float(np.mean(((X_val @ worker_1_cp["weight"] + worker_1_cp["bias"]) - y_val) ** 2))

    # Assert exact floating point equality - NO synthetic * 0.92 multipliers!
    assert report.baseline_val_loss == pytest.approx(expected_baseline_loss, rel=1e-6)
    assert report.merged_val_loss == pytest.approx(expected_merged_loss, rel=1e-6)
    assert report.per_worker_val_loss["worker_0"] == pytest.approx(expected_w0_loss, rel=1e-6)
    assert report.per_worker_val_loss["worker_1"] == pytest.approx(expected_w1_loss, rel=1e-6)
    assert report.best_worker_val_loss == pytest.approx(min(expected_w0_loss, expected_w1_loss), rel=1e-6)

    # Honest reporting statement must be present and state zero multipliers
    assert "Zero artificial multipliers applied" in report.honest_reporting_statement
    assert report.execution_paradigm == DistributionStrategy.FEDERATED_LEARNING
    assert report.sample_count_val == len(X_val)


def test_validation_degradation_fallback_to_best_worker():
    """Verify that if the merged model degrades validation loss, fallback to best worker is triggered."""
    orchestrator = MLLifecycleOrchestrator()
    _, val_data = create_3d_geometry_multimodal_dataset(sample_count=50, random_seed=77)

    # Worker 0 found an excellent local minimum
    worker_0_cp = {"weight": np.zeros((8,), dtype=np.float32), "bias": np.float32(0.2)}
    # Worker 1 diverged
    worker_1_cp = {"weight": np.ones((8,), dtype=np.float32) * 5.0, "bias": np.float32(10.0)}
    # Merged model is worse than Worker 0
    merged_cp = {"weight": np.ones((8,), dtype=np.float32) * 2.5, "bias": np.float32(5.1)}
    baseline_cp = {"weight": np.ones((8,), dtype=np.float32) * 1.0, "bias": np.float32(1.0)}

    report = orchestrator.validate_against_baseline(
        merged_checkpoint=merged_cp,
        baseline_checkpoint=baseline_cp,
        worker_checkpoints=[worker_0_cp, worker_1_cp],
        val_dataset=val_data,
        strategy=DistributionStrategy.FEDERATED_LEARNING,
    )

    assert report.validation_degradation_detected is True
    assert report.selected_checkpoint_type == "best_worker_fallback"
    assert report.best_worker_val_loss < report.merged_val_loss


# ============================================================================
# 3. Workload Detection & Strategy Selection Tests (Strict Honesty)
# ============================================================================

def test_strategy_selection_task_parallel_for_preprocessing():
    """Audio preprocessing and ComfyUI DAGs must select TASK_PARALLEL, never masquerading as collaborative ML."""
    orchestrator = MLLifecycleOrchestrator()

    # RVC audio slicing/f0 extraction
    rvc_detect = orchestrator.detect_workload("python rvc_extract_f0.py --f0_method rmvpe")
    assert rvc_detect.platform == "rvc"
    assert rvc_detect.is_preprocessing_only is True

    inspection = orchestrator.inspect_requirements(rvc_detect, active_workers=[])
    strategy = orchestrator.select_distribution_strategy(rvc_detect, inspection)
    assert strategy == DistributionStrategy.TASK_PARALLEL

    # ComfyUI workflow
    comfy_detect = orchestrator.detect_workload("comfyui workflow.json execution")
    assert comfy_detect.platform == "comfyui"
    strategy_comfy = orchestrator.select_distribution_strategy(comfy_detect, inspection)
    assert strategy_comfy == DistributionStrategy.TASK_PARALLEL


def test_strategy_selection_model_parallel_on_high_memory():
    """Models exceeding 65% of available worker free RAM must select MODEL_PARALLEL."""
    orchestrator = MLLifecycleOrchestrator()

    w_low = make_test_worker("node-laptop", cores=4, ram_gb=4.0)  # ~3.2 GB free
    detect = orchestrator.detect_workload("import torch; train(model)")

    # 300 Million parameters = 4.8 GB activation + weight footprint > 65% of 3.2 GB
    inspection = orchestrator.inspect_requirements(
        detect,
        active_workers=[w_low],
        request={"parameters_count": 300_000_000, "batch_size": 32},
    )
    strategy = orchestrator.select_distribution_strategy(detect, inspection)
    assert strategy == DistributionStrategy.MODEL_PARALLEL


def test_strategy_selection_data_parallel_and_federated():
    """torchrun/DDP selects DATA_PARALLEL; standard multi-PC selects FEDERATED_LEARNING."""
    orchestrator = MLLifecycleOrchestrator()
    w_large = make_test_worker("node-large", cores=16, ram_gb=32.0)

    # DDP script
    ddp_detect = orchestrator.detect_workload("import torch.distributed as dist; dist.init_process_group()")
    assert ddp_detect.has_distributed_api is True
    insp_ddp = orchestrator.inspect_requirements(ddp_detect, active_workers=[w_large], request={"parameters_count": 5_000_000})
    strategy_ddp = orchestrator.select_distribution_strategy(ddp_detect, insp_ddp)
    assert strategy_ddp == DistributionStrategy.DATA_PARALLEL

    # Generic multi-PC script
    generic_detect = orchestrator.detect_workload("import torch; train_step()")
    insp_gen = orchestrator.inspect_requirements(generic_detect, active_workers=[w_large], request={"parameters_count": 5_000_000})
    strategy_gen = orchestrator.select_distribution_strategy(generic_detect, insp_gen)
    assert strategy_gen == DistributionStrategy.FEDERATED_LEARNING


# ============================================================================
# 4. 10-Step Lifecycle Orchestration & Multi-Round Coordination Tests
# ============================================================================

def test_coordinate_synchronization_rounds_and_delta_aggregation():
    """Verify multi-round synchronization initializes from W^(r-1) and aggregates weight deltas."""
    orchestrator = MLLifecycleOrchestrator()

    initial_checkpoint = {
        "linear.weight": np.array([1.0, 2.0, 3.0], dtype=np.float32),
        "linear.bias": np.array([0.5], dtype=np.float32),
    }

    partitions = [
        {"worker_id": "worker-fast", "sample_count": 300},
        {"worker_id": "worker-slow", "sample_count": 100},
    ]

    # Mock worker train function: workers simulate local gradient steps
    def mock_train(round_num: int, global_cp: Dict[str, Any], partition: Dict[str, Any]) -> Dict[str, Any]:
        w = global_cp["linear.weight"].copy()
        b = global_cp["linear.bias"].copy()
        wid = partition["worker_id"]
        # Fast worker moves +0.2, Slow worker moves +0.1
        step = 0.2 if "fast" in wid else 0.1
        return {
            "linear.weight": w + step,
            "linear.bias": b + (step * 0.5),
        }

    final_cp = orchestrator.coordinate_synchronization_rounds(
        num_rounds=2,
        initial_checkpoint=initial_checkpoint,
        train_worker_fn=mock_train,
        partitions=partitions,
    )

    # After 1 round:
    # fast weight delta: +0.2 (weight 300/400 = 0.75)
    # slow weight delta: +0.1 (weight 100/400 = 0.25)
    # combined delta per round = 0.75 * 0.2 + 0.25 * 0.1 = 0.15 + 0.025 = 0.175
    # after 2 rounds: total delta = +0.35
    expected_weight = initial_checkpoint["linear.weight"] + 0.35
    np.testing.assert_allclose(final_cp["linear.weight"], expected_weight, rtol=1e-5)


def test_aggregate_valid_updates_rejects_nan_and_inf():
    """Verify that worker updates containing NaN or Inf are rejected without corrupting weights."""
    orchestrator = MLLifecycleOrchestrator()

    base = {"W": np.array([1.0, 2.0], dtype=np.float32)}
    valid_worker = {"W": np.array([1.2, 2.2], dtype=np.float32)}
    nan_worker = {"W": np.array([np.nan, 2.0], dtype=np.float32)}
    inf_worker = {"W": np.array([np.inf, 2.0], dtype=np.float32)}

    merged = orchestrator.aggregate_valid_updates(
        base_checkpoint=base,
        worker_checkpoints=[valid_worker, nan_worker, inf_worker],
        sample_counts=[100, 100, 100],
    )

    # nan_worker and inf_worker must be dropped, leaving only valid_worker (100% weight)
    np.testing.assert_allclose(merged["W"], valid_worker["W"], rtol=1e-5)


def test_aggregate_valid_updates_all_rejected_preserves_base():
    """Verify that if all updates contain NaN/Inf, base checkpoint is safely retained."""
    orchestrator = MLLifecycleOrchestrator()
    base = {"W": np.array([1.0, 2.0], dtype=np.float32)}
    nan_worker = {"W": np.array([np.nan, 2.0], dtype=np.float32)}

    merged = orchestrator.aggregate_valid_updates(
        base_checkpoint=base,
        worker_checkpoints=[nan_worker],
        sample_counts=[50],
    )
    np.testing.assert_allclose(merged["W"], base["W"])


def test_worker_failure_shard_recovery():
    """Verify Step 8: Failed worker partition is re-assigned to healthy worker."""
    orchestrator = MLLifecycleOrchestrator()
    healthy_node = make_test_worker("node-healthy-backup", cores=8, ram_gb=16.0)

    failed_partition = {
        "shard_index": 2,
        "worker_id": "node-crashed",
        "start_ratio": 0.5,
        "end_ratio": 0.75,
        "sample_count": 250,
    }
    current_checkpoint = {"weights": [1, 2, 3]}

    recovered = orchestrator.recover_failed_worker_shard(
        failed_partition=failed_partition,
        healthy_worker=healthy_node,
        current_checkpoint=current_checkpoint,
    )

    assert recovered["worker_id"] == "node-healthy-backup"
    assert recovered["shard_index"] == 2
    assert recovered["start_ratio"] == 0.5
    assert recovered["end_ratio"] == 0.75
    assert recovered["recovered_from_failure"] is True


def test_enforce_worker_constraints_auto_tuning():
    """Verify Step 6: Derives optimal threads and low-RAM flags per worker hardware."""
    orchestrator = MLLifecycleOrchestrator()

    # Low-RAM node (3 GB available RAM, 4 cores)
    w_low = make_test_worker("node-low", cores=4, ram_gb=3.5)
    w_low.resource_profile.ram_available_bytes = int(2.5 * 1024**3)
    env_low = orchestrator.enforce_worker_constraints(w_low)
    assert env_low["AIDAR_LOW_RAM_MODE"] == "1"
    assert env_low["AIDAR_OPTIMAL_THREADS"] == "3"  # cpus - 1 for < 8 cores

    # High-end workstation (32 GB RAM, 16 cores)
    w_high = make_test_worker("node-high", cores=16, ram_gb=32.0)
    env_high = orchestrator.enforce_worker_constraints(w_high)
    assert env_high["AIDAR_LOW_RAM_MODE"] == "0"
    assert env_high["AIDAR_OPTIMAL_THREADS"] == "14"  # cpus - 2 for >= 8 cores


# ============================================================================
# 5. Platform-Specific Integration Adapters Tests
# ============================================================================

def test_rvc_adapter_stages_and_honesty():
    """Verify RVCAdapter marks Stages 1-3 as TASK_PARALLEL and Stage 4 as DATA_PARALLEL."""
    adapter = RVCAdapter()
    w0 = make_test_worker("node-gpu-0", cores=8, ram_gb=16.0, has_gpu=True, vram_gb=8.0)
    w1 = make_test_worker("node-cpu-1", cores=8, ram_gb=16.0, has_gpu=False)

    # Stage 2: Pitch Extraction (RMVPE) -> TASK_PARALLEL
    specs_stage2 = adapter.evaluate_request(
        request={"stage": "extract_f0", "audio_files": [f"voice_{i}.wav" for i in range(8)]},
        active_workers=[w0, w1],
    )
    assert len(specs_stage2) == 2
    assert specs_stage2[0].parameters["execution_paradigm"] == DistributionStrategy.TASK_PARALLEL.value
    assert len(specs_stage2[0].parameters["audio_files"]) == 4

    # Stage 4: Generator Training -> DATA_PARALLEL
    specs_stage4 = adapter.evaluate_request(
        request={"stage": "train", "epochs": 50, "batch_size": 4},
        active_workers=[w0, w1],
    )
    assert len(specs_stage4) == 2
    assert specs_stage4[0].parameters["execution_paradigm"] == DistributionStrategy.DATA_PARALLEL.value
    # GPU node gets larger start_ratio/end_ratio share
    assert specs_stage4[0].parameters["end_ratio"] > specs_stage4[1].parameters["end_ratio"] - specs_stage4[1].parameters["start_ratio"]


def test_kohya_adapter_workload_specs_and_low_ram():
    """Verify KohyaAdapter sets low-RAM flags (--gradient_checkpointing) on weak nodes."""
    adapter = KohyaAdapter()
    w_weak = make_test_worker("weak-laptop", cores=4, ram_gb=4.0)
    w_weak.resource_profile.ram_available_bytes = int(2.0 * 1024**3)
    w_strong = make_test_worker("strong-rig", cores=16, ram_gb=32.0, has_gpu=True, vram_gb=12.0)

    specs = adapter.evaluate_request(
        request={
            "pretrained_model_name_or_path": "sd_v1-5.safetensors",
            "train_data_dir": "./data/lora_train",
            "master_addr": "192.168.1.10",
        },
        active_workers=[w_weak, w_strong],
    )
    assert len(specs) == 2

    # Weak laptop gets low_ram_mode and gradient checkpointing
    assert specs[0].parameters["low_ram_mode"] is True
    assert "--gradient_checkpointing" in specs[0].execution_spec.args
    assert "--cache_latents_to_disk" in specs[0].execution_spec.args

    # Strong rig runs standard
    assert specs[1].parameters["low_ram_mode"] is False


def test_comfyui_adapter_task_parallel_seeds():
    """Verify ComfyUIAdapter dispatches batch generation seeds as TASK_PARALLEL."""
    adapter = ComfyUIAdapter()
    w0 = make_test_worker("node-0", cores=4)
    w1 = make_test_worker("node-1", cores=4)

    specs = adapter.evaluate_request(
        request={"workflow": {"3": {"class_type": "KSampler"}}, "batch_count": 6, "seed": 100},
        active_workers=[w0, w1],
    )
    assert len(specs) == 2
    assert specs[0].parameters["execution_paradigm"] == DistributionStrategy.TASK_PARALLEL.value
    assert specs[0].parameters["seeds"] == [100, 101, 102]
    assert specs[1].parameters["seeds"] == [103, 104, 105]


def test_voicebox_adapter_stages():
    """Verify VoiceboxAdapter separates feature prep from acoustic model training."""
    adapter = VoiceboxAdapter()
    w0 = make_test_worker("node-0", cores=8)

    # Feature prep -> TASK_PARALLEL
    prep_specs = adapter.evaluate_request(
        request={"stage": "feature_prep", "audio_files": ["a.wav", "b.wav"]},
        active_workers=[w0],
    )
    assert prep_specs[0].parameters["execution_paradigm"] == DistributionStrategy.TASK_PARALLEL.value

    # Acoustic model training -> FEDERATED_LEARNING
    train_specs = adapter.evaluate_request(
        request={"stage": "train", "dataset_path": "./data/speech"},
        active_workers=[w0],
    )
    assert train_specs[0].parameters["execution_paradigm"] == DistributionStrategy.FEDERATED_LEARNING.value


# ============================================================================
# 6. Custom Python ML SDK Tests (aidars.sdk.ml)
# ============================================================================

def test_sdk_distributed_train_decorator(tmp_path):
    """Verify @distributed_train executes cleanly, configures thread pools, and saves outputs."""
    out_file = tmp_path / "model.pt"

    @distributed_train(epochs=1, save_output=True, output_path=str(out_file))
    def sample_training():
        return {"linear.weight": [1.0, 2.0], "linear.bias": [0.1]}

    result = sample_training()
    assert result["linear.weight"] == [1.0, 2.0]
    # Check fallback json output if torch not available or state dict saved
    assert os.path.exists(out_file) or os.path.exists(str(out_file).replace(".pt", ".json"))


def test_sdk_synchronized_dataloader_sharding_and_low_ram(monkeypatch):
    """Verify AidarSynchronizedDataLoader honors power-share ratios and low-RAM downscaling."""
    dataset = list(range(100))

    # Worker assigned shard [0.2, 0.6] (40 items: 20 to 59)
    monkeypatch.setenv("AIDAR_SHARD_START_RATIO", "0.2")
    monkeypatch.setenv("AIDAR_SHARD_END_RATIO", "0.6")
    monkeypatch.setenv("AIDAR_LOW_RAM_MODE", "1")

    loader = AidarSynchronizedDataLoader(dataset, batch_size=10, shuffle=False)

    # Low-RAM mode cuts batch size in half: 10 // 2 = 5
    assert loader.batch_size == 5
    assert len(loader.dataset) == 40
    assert loader.dataset[0] == 20
    assert loader.dataset[-1] == 59

    batches = list(loader)
    assert len(batches) == 8  # 40 / 5 = 8
    assert batches[0] == [20, 21, 22, 23, 24]


def test_sdk_model_parallel_pipeline_cpu_offload_and_lora():
    """Verify AidarModelParallelPipeline configures CPU offload and freezes base weights for LoRA."""
    class DummyModule:
        def __init__(self):
            self.weight = np.zeros((4, 4))

    class DummyModel:
        def __init__(self):
            self.layer1 = DummyModule()
            self.layer2 = DummyModule()
            class Param:
                requires_grad = True
            self._params = [Param(), Param()]

        def named_modules(self):
            return [("layer1", self.layer1), ("layer2", self.layer2)]

        def parameters(self):
            return self._params

    model = DummyModel()
    pipeline = AidarModelParallelPipeline(model, lora_rank=4, lora_alpha=8.0)
    pipeline.configure_cpu_offload()
    assert "layer1" in pipeline.offloaded_layers
    assert "layer2" in pipeline.offloaded_layers

    pipeline.inject_lora()
    for p in model.parameters():
        assert p.requires_grad is False


def test_sdk_validation_gate_zero_bonuses():
    """Verify AidarValidationGate executes forward pass on validation dataset with zero bonuses."""
    X_val = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
    y_val = np.array([5.0, 11.0], dtype=np.float32)

    gate = AidarValidationGate(val_dataset=(X_val, y_val))

    # Perfect weights: W = [1.0, 2.0], b = 0.0 -> preds = [1+4=5, 3+8=11] -> loss = 0.0
    perfect_cp = {"linear.weight": np.array([1.0, 2.0], dtype=np.float32), "linear.bias": np.float32(0.0)}
    loss = gate.evaluate_checkpoint(perfect_cp)
    assert loss == pytest.approx(0.0, abs=1e-6)

    # Suboptimal weights
    subopt_cp = {"linear.weight": np.array([1.0, 1.0], dtype=np.float32), "linear.bias": np.float32(0.0)}
    subopt_loss = gate.evaluate_checkpoint(subopt_cp)
    assert subopt_loss > 0.0

    # Verification gate
    is_ok, cand_loss, base_loss = gate.verify_no_degradation(perfect_cp, subopt_cp)
    assert is_ok is True
    assert cand_loss < base_loss


def test_aidar_ml_sdk_facade():
    """Verify AidarMLSDK provides consistent access to all components."""
    assert AidarMLSDK.distributed_train is not None
    assert AidarMLSDK.SynchronizedDataLoader is AidarSynchronizedDataLoader
    assert AidarMLSDK.ModelParallelPipeline is AidarModelParallelPipeline
    assert AidarMLSDK.ValidationGate is AidarValidationGate
