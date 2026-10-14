"""Unit tests for M1 Cluster Sizer: Minimum-Sufficient-Resource Sizing.

Validates that AIDAR selects the smallest suitable subset of workers to satisfy
workload performance objectives without greedily recruiting every idle PC.
"""

import copy
import time
import pytest
from pydantic import ValidationError

from aidars.distributed.models import WorkerResourceProfile, WorkerStatus
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.sizing import (
    ClusterSizer,
    GroupSizingEvaluation,
    SizingDecision,
    SizingFeasibilityStatus,
    SizingPolicy,
    SizingRequest,
    WorkerSizingDetail,
)


def make_profile(
    worker_id: str,
    *,
    status: WorkerStatus = WorkerStatus.ACTIVE,
    can_execute: bool = True,
    cpu_cores: int = 8,
    cpu_util: float = 10.0,
    ram_total_mb: int = 8192,
    ram_avail_mb: int = 4096,
    gpu_available: bool = False,
    vram_total_mb: int = 0,
    vram_avail_mb: int = 0,
    active_workloads: int = 0,
    cached_hashes: set = None,
    timestamp_utc: float = None,
    endpoint_url: str = None,
    cost_per_hour: float = 0.0,
) -> WorkerResourceProfile:
    """Helper to construct a valid WorkerResourceProfile."""
    now = time.time()
    ts = now if timestamp_utc is None else timestamp_utc
    url = endpoint_url or f"http://127.0.0.1:{8000 + int(hash(worker_id) % 1000)}"
    return WorkerResourceProfile(
        worker_id=worker_id,
        endpoint_url=url,
        ip_address="127.0.0.1",
        status=status,
        can_execute_workloads=can_execute,
        cpu_cores_total=cpu_cores,
        cpu_utilization_percent=cpu_util,
        ram_total_bytes=ram_total_mb * 1024 * 1024,
        ram_available_bytes=ram_avail_mb * 1024 * 1024,
        gpu_available=gpu_available,
        vram_total_bytes=vram_total_mb * 1024 * 1024,
        vram_available_bytes=vram_avail_mb * 1024 * 1024,
        active_workload_count=active_workloads,
        local_cached_hashes=cached_hashes or set(),
        timestamp_utc=ts,
        cost_per_hour=cost_per_hour,
    )


class TestM1ClusterSizer:
    """Unit tests for M1 adaptive cluster sizing."""

    def test_single_worker_sufficient_when_1_pc_meets_deadline(self):
        """When 1 worker alone satisfies deadline with safety margin, use 1 worker and leave others idle."""
        now = time.time()
        p1 = make_profile("w-1", timestamp_utc=now)
        p2 = make_profile("w-2", timestamp_utc=now)
        p3 = make_profile("w-3", timestamp_utc=now)

        sizer = ClusterSizer()
        # 4 units, each takes 10s. 1 worker takes 40s.
        # Deadline is 60s (safety target 60 * 0.90 = 54s). 40s <= 54s.
        req = SizingRequest(
            task_type="blender_render",
            total_units=4,
            unit_workload_seconds=10.0,
            deadline_seconds=60.0,
        )

        decision = sizer.size_cluster(req, [p1, p2, p3], now_utc=now)

        assert decision.recommended_worker_count == 1
        assert decision.selected_worker_ids == ["w-1"]
        assert set(decision.spare_worker_ids) == {"w-2", "w-3"}
        assert decision.policy_applied == SizingPolicy.SINGLE_WORKER_SUFFICIENT
        assert decision.feasibility_status == SizingFeasibilityStatus.FEASIBLE
        assert decision.deadline_achievable is True
        assert len(decision.worker_details) == 1
        assert decision.worker_details[0].allocated_units == 4

    def test_smallest_group_meets_deadline(self):
        """When 1 worker is too slow to meet deadline, recruit smallest group that satisfies it."""
        now = time.time()
        p1 = make_profile("w-1", timestamp_utc=now)
        p2 = make_profile("w-2", timestamp_utc=now)
        p3 = make_profile("w-3", timestamp_utc=now)
        p4 = make_profile("w-4", timestamp_utc=now)

        sizer = ClusterSizer()
        # 12 units, each 10s. Total work = 120s.
        # 1 PC = 120s.
        # 2 PCs = 6 units * 10s = 60s + 1s coord = 61s.
        # 3 PCs = 4 units * 10s = 40s + 2s coord = 42s.
        # Deadline is 50s. Safety target = 50 * 0.90 = 45s.
        # 2 PCs take 61s > 45s (misses).
        # 3 PCs take 42s <= 45s (meets deadline with safety buffer).
        req = SizingRequest(
            task_type="compute",
            total_units=12,
            unit_workload_seconds=10.0,
            deadline_seconds=50.0,
        )

        decision = sizer.size_cluster(req, [p1, p2, p3, p4], now_utc=now)

        assert decision.recommended_worker_count == 3
        assert decision.selected_worker_ids == ["w-1", "w-2", "w-3"]
        assert decision.spare_worker_ids == ["w-4"]
        assert decision.policy_applied == SizingPolicy.DEADLINE_DRIVEN_MINIMAL
        assert decision.feasibility_status == SizingFeasibilityStatus.FEASIBLE
        assert decision.deadline_achievable is True

    def test_heterogeneous_worker_speeds(self):
        """Worker speed multipliers result in proportional unit allocation and balanced load."""
        now = time.time()
        p_fast = make_profile("w-fast", timestamp_utc=now)
        p_slow = make_profile("w-slow", timestamp_utc=now)

        sizer = ClusterSizer()
        # 10 units, unit workload 10s.
        # Fast PC has speed 2.0x, slow PC has speed 0.5x.
        # Total speed = 2.5x.
        # Fast quota = 10 * (2.0 / 2.5) = 8 units.
        # Slow quota = 10 * (0.5 / 2.5) = 2 units.
        req = SizingRequest(
            total_units=10,
            unit_workload_seconds=10.0,
            deadline_seconds=50.0,
            worker_speed_multipliers={"w-fast": 2.0, "w-slow": 0.5},
        )

        decision = sizer.size_cluster(req, [p_fast, p_slow], now_utc=now)

        assert decision.recommended_worker_count == 2
        alloc_map = {d.worker_id: d.allocated_units for d in decision.worker_details}
        assert alloc_map["w-fast"] == 8
        assert alloc_map["w-slow"] == 2

        # Verify compute time for both is balanced:
        # Fast: 8 * 10 / 2.0 = 40.0s
        # Slow: 2 * 10 / 0.5 = 40.0s
        compute_map = {d.worker_id: d.estimated_compute_seconds for d in decision.worker_details}
        assert compute_map["w-fast"] == pytest.approx(40.0)
        assert compute_map["w-slow"] == pytest.approx(40.0)

    def test_transfer_overhead_prevents_unnecessary_recruitment(self):
        """When an extra worker has high asset transfer overhead that worsens wall clock time, halt expansion."""
        now = time.time()
        h1 = "a" * 64
        # w-cached already has the 1 GB asset cached
        p_cached = make_profile("w-cached", cached_hashes={h1}, timestamp_utc=now)
        # w-uncached must download the 1 GB asset over a 10 MB/s link (100 seconds transfer)
        p_uncached = make_profile("w-uncached", cached_hashes=set(), timestamp_utc=now)

        sizer = ClusterSizer()
        # 2 units, 30s each.
        # w-cached alone: transfer = 0s, compute = 60s -> total = 60s.
        # w-cached + w-uncached:
        #   transfer w-uncached = 1,000,000,000 / (10 * 1024 * 1024) ≈ 95.36s.
        #   compute = 1 unit each * 30s = 30s.
        #   w-uncached finish = 95.36 + 30 ≈ 125.36s + 1s coord = 126.36s > 60s!
        req = SizingRequest(
            total_units=2,
            unit_workload_seconds=30.0,
            input_asset_hashes={h1},
            asset_sizes_bytes={h1: 1000 * 1024 * 1024},  # 1000 MB
            network_bandwidth_bytes_per_sec=10.0 * 1024 * 1024,  # 10 MB/s
        )

        decision = sizer.size_cluster(req, [p_cached, p_uncached], now_utc=now)

        assert decision.recommended_worker_count == 1
        assert decision.selected_worker_ids == ["w-cached"]
        assert decision.spare_worker_ids == ["w-uncached"]
        assert decision.policy_applied == SizingPolicy.TRANSFER_OVERHEAD_HALT

    def test_diminishing_returns_halts_expansion(self):
        """In throughput mode (no deadline), halt expansion when marginal speedup falls below threshold."""
        now = time.time()
        p1 = make_profile("w-1", timestamp_utc=now)
        p2 = make_profile("w-2", timestamp_utc=now)
        p3 = make_profile("w-3", timestamp_utc=now)

        # Set coordination overhead to 4.0s to cause diminishing returns on worker 3
        sizer = ClusterSizer(
            default_coordination_overhead_per_worker_seconds=4.0,
            marginal_benefit_threshold=0.15,
        )
        # 24 units, 5s each = 120s total compute.
        # k=1: 120s
        # k=2: 12 * 5 = 60s + 4s coord = 64s. Marginal gain = (120 - 64) / 120 = 46.7% (>= 15%)
        # k=3: 8 * 5 = 40s + 8s coord = 48s. Marginal gain = (64 - 48) / 64 = 25% (>= 15%)
        # Now suppose coordination overhead is 8.0s:
        # k=1: 120s
        # k=2: 60s + 8s = 68s. Marginal = (120 - 68) / 120 = 43.3%
        # k=3: 40s + 16s = 56s. Marginal = (68 - 56) / 68 = 17.6%
        # If coordination overhead is 12.0s:
        # k=1: 120s
        # k=2: 60s + 12s = 72s. Marginal = (120 - 72)/120 = 40%
        # k=3: 40s + 24s = 64s. Marginal = (72 - 64)/72 = 11.1% (< 15%)!
        req = SizingRequest(
            total_units=24,
            unit_workload_seconds=5.0,
            coordination_overhead_per_worker_seconds=12.0,
            marginal_benefit_threshold=0.15,
        )

        decision = sizer.size_cluster(req, [p1, p2, p3], now_utc=now)

        assert decision.recommended_worker_count == 2
        assert decision.selected_worker_ids == ["w-1", "w-2"]
        assert decision.spare_worker_ids == ["w-3"]
        assert decision.policy_applied == SizingPolicy.DIMINISHING_RETURNS

    def test_missing_estimates_fallback(self):
        """When unit_workload_seconds is None, sizer falls back gracefully and reports UNKNOWN_ESTIMATE."""
        now = time.time()
        p1 = make_profile("w-1", timestamp_utc=now)
        p2 = make_profile("w-2", timestamp_utc=now)

        sizer = ClusterSizer()
        req = SizingRequest(
            total_units=10,
            unit_workload_seconds=None,
        )

        decision = sizer.size_cluster(req, [p1, p2], now_utc=now)

        assert decision.recommended_worker_count >= 1
        assert decision.confidence_score == 0.5
        assert decision.feasibility_status == SizingFeasibilityStatus.UNKNOWN_ESTIMATE

    def test_non_partitionable_selects_single_worker(self):
        """Non-partitionable workloads always recruit exactly 1 worker."""
        now = time.time()
        p1 = make_profile("w-1", timestamp_utc=now)
        p2 = make_profile("w-2", timestamp_utc=now)

        sizer = ClusterSizer()
        req = SizingRequest(
            total_units=20,
            unit_workload_seconds=2.0,
            partitionable=False,
        )

        decision = sizer.size_cluster(req, [p1, p2], now_utc=now)

        assert decision.recommended_worker_count == 1
        assert decision.policy_applied == SizingPolicy.NON_PARTITIONABLE
        assert decision.selected_worker_ids == ["w-1"]
        assert decision.spare_worker_ids == ["w-2"]
        assert decision.worker_details[0].allocated_units == 20

    def test_technical_eligibility_enforced_via_placement_engine(self):
        """PlacementEngine technical gatekeeping is strictly enforced without duplicate checks."""
        now = time.time()
        p_healthy_gpu = make_profile(
            "w-gpu",
            gpu_available=True,
            vram_total_mb=8192,
            vram_avail_mb=8192,
            timestamp_utc=now,
        )
        p_no_gpu = make_profile("w-cpu", gpu_available=False, timestamp_utc=now)
        p_stale = make_profile("w-stale", gpu_available=True, timestamp_utc=now - 500)

        sizer = ClusterSizer()
        req = SizingRequest(
            requires_gpu=True,
            min_vram_bytes=4 * 1024 * 1024 * 1024,
            total_units=10,
            unit_workload_seconds=5.0,
        )

        decision = sizer.size_cluster(req, [p_healthy_gpu, p_no_gpu, p_stale], now_utc=now)

        assert decision.selected_worker_ids == ["w-gpu"]
        assert decision.recommended_worker_count == 1
        assert "w-cpu" in decision.rejection_diagnostics
        assert "REQUIRED_GPU_UNAVAILABLE" in decision.rejection_diagnostics["w-cpu"]
        assert "w-stale" in decision.rejection_diagnostics
        assert "STALE_TELEMETRY" in decision.rejection_diagnostics["w-stale"]

    def test_no_arbitrary_ram_floor_rejection(self):
        """Worker with small RAM is not rejected when min_ram_bytes is not specified."""
        now = time.time()
        # 64 MB RAM worker
        p_low_ram = make_profile(
            "w-low-ram",
            ram_total_mb=64,
            ram_avail_mb=32,
            timestamp_utc=now,
        )

        sizer = ClusterSizer()
        # min_ram_bytes is None (unknown RAM requirement)
        req = SizingRequest(
            total_units=2,
            unit_workload_seconds=5.0,
            min_ram_bytes=None,
        )

        decision = sizer.size_cluster(req, [p_low_ram], now_utc=now)

        assert decision.recommended_worker_count == 1
        assert decision.selected_worker_ids == ["w-low-ram"]
        assert "w-low-ram" not in decision.rejection_diagnostics

    def test_infeasible_deadline(self):
        """When even all available workers cannot meet deadline, flag INFEASIBLE_DEADLINE."""
        now = time.time()
        p1 = make_profile("w-1", timestamp_utc=now)
        p2 = make_profile("w-2", timestamp_utc=now)

        sizer = ClusterSizer()
        # 100 units * 10s = 1000s of work.
        # Even 2 workers take ~500s.
        # Deadline is 5 seconds!
        req = SizingRequest(
            total_units=100,
            unit_workload_seconds=10.0,
            deadline_seconds=5.0,
        )

        decision = sizer.size_cluster(req, [p1, p2], now_utc=now)

        assert decision.deadline_achievable is False
        assert decision.feasibility_status == SizingFeasibilityStatus.INFEASIBLE_DEADLINE

    def test_invalid_inputs(self):
        """Validation errors are raised for invalid request constraints."""
        with pytest.raises(ValidationError):
            SizingRequest(total_units=0)

        with pytest.raises(ValidationError):
            SizingRequest(unit_workload_seconds=-10.0)

        with pytest.raises(ValidationError):
            SizingRequest(min_cpu_cores=0)

    def test_immutability_of_inputs(self):
        """Worker profiles and request objects are never mutated during sizing."""
        now = time.time()
        p1 = make_profile("w-1", timestamp_utc=now)
        p2 = make_profile("w-2", timestamp_utc=now)
        profiles = [p1, p2]
        profiles_copy = copy.deepcopy(profiles)

        req = SizingRequest(
            total_units=10,
            unit_workload_seconds=5.0,
            deadline_seconds=60.0,
        )
        req_copy = req.model_copy(deep=True)

        sizer = ClusterSizer()
        sizer.size_cluster(req, profiles, now_utc=now)

        assert profiles == profiles_copy
        assert req == req_copy

    def test_spare_workers_tracking(self):
        """Eligible unselected workers are preserved in spare_worker_ids in ranked order."""
        now = time.time()
        p1 = make_profile("w-1", timestamp_utc=now)
        p2 = make_profile("w-2", timestamp_utc=now)
        p3 = make_profile("w-3", timestamp_utc=now)
        p4 = make_profile("w-4", timestamp_utc=now)

        sizer = ClusterSizer()
        req = SizingRequest(
            total_units=10,
            unit_workload_seconds=10.0,
            deadline_seconds=60.0,
        )

        decision = sizer.size_cluster(req, [p1, p2, p3, p4], now_utc=now)

        assert decision.recommended_worker_count == 2
        assert decision.selected_worker_ids == ["w-1", "w-2"]
        assert decision.spare_worker_ids == ["w-3", "w-4"]

    def test_unschedulable_constraints_zero_eligible(self):
        """When all workers are rejected by technical checks, report UNSCHEDULABLE_CONSTRAINTS."""
        now = time.time()
        # w-1 has 2 cores, request requires 16
        p1 = make_profile("w-1", cpu_cores=2, timestamp_utc=now)

        sizer = ClusterSizer()
        req = SizingRequest(
            min_cpu_cores=16,
            total_units=10,
            unit_workload_seconds=5.0,
        )

        decision = sizer.size_cluster(req, [p1], now_utc=now)

        assert decision.recommended_worker_count == 0
        assert decision.selected_worker_ids == []
        assert decision.feasibility_status == SizingFeasibilityStatus.UNSCHEDULABLE_CONSTRAINTS
        assert decision.deadline_achievable is False
        assert "INSUFFICIENT_CPU" in decision.rejection_diagnostics.get("w-1", [])
