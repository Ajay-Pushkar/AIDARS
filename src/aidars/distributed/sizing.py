"""M1 Cluster Sizer: Minimum-Sufficient-Resource Sizing for Distributed Workloads.

Determines the smallest suitable group of worker PCs needed to satisfy a workload's
performance objective (deadline, throughput) efficiently, rather than greedily
recruiting every available machine.

Delegates technical eligibility enforcement directly to PlacementEngine.
"""

from enum import Enum
import math
import time
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

from pydantic import BaseModel, Field

from aidars.distributed.models import WorkerResourceProfile, WorkloadSpec
from aidars.distributed.placement import PlacementEngine


class SizingPolicy(str, Enum):
    """Policy applied by the cluster sizer to make its recommendation."""

    SINGLE_WORKER_SUFFICIENT = "single_worker_sufficient"
    DEADLINE_DRIVEN_MINIMAL = "deadline_driven_minimal"
    EFFICIENCY_THROUGHPUT = "efficiency_throughput"
    TRANSFER_OVERHEAD_HALT = "transfer_overhead_halt"
    DIMINISHING_RETURNS = "diminishing_returns"
    NON_PARTITIONABLE = "non_partitionable"


class SizingFeasibilityStatus(str, Enum):
    """Feasibility status of the sizing recommendation against workload constraints."""

    FEASIBLE = "feasible"
    INFEASIBLE_DEADLINE = "infeasible_deadline"
    UNSCHEDULABLE_CONSTRAINTS = "unschedulable_constraints"
    UNKNOWN_ESTIMATE = "unknown_estimate"


class SizingRequest(BaseModel):
    """Workload performance objective and technical resource constraints."""

    request_id: str = Field(default_factory=lambda: f"size-{uuid4().hex[:12]}")
    task_type: str = "compute"
    total_units: int = Field(default=1, ge=1)
    unit_workload_seconds: Optional[float] = Field(default=None, gt=0.0)
    deadline_seconds: Optional[float] = Field(default=None, gt=0.0)
    deadline_at_utc: Optional[float] = Field(default=None, gt=0.0)
    partitionable: bool = True
    min_units_per_worker: int = Field(default=1, ge=1)
    max_workers_limit: Optional[int] = Field(default=None, ge=1)

    # Technical constraints passed to WorkloadSpec probe for PlacementEngine evaluation
    min_cpu_cores: int = Field(default=1, ge=1)
    min_ram_bytes: Optional[int] = Field(default=None, ge=1)
    requires_gpu: bool = False
    min_vram_bytes: int = Field(default=0, ge=0)
    required_gpu_vendor: Optional[str] = None
    required_gpu_model: Optional[str] = None
    min_compute_capability: Optional[str] = None
    required_driver_version: Optional[str] = None
    required_runtime_compatibility: Optional[str] = None
    required_tags: Dict[str, str] = Field(default_factory=dict)
    required_worker_tags: Dict[str, str] = Field(default_factory=dict)
    preferred_worker_tags: Dict[str, str] = Field(default_factory=dict)
    execution_spec: Optional[Any] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    affinity_group_id: Optional[str] = None
    affinity_mode: str = "none"
    affinity_tag_key: Optional[str] = None
    affinity_hard: bool = True
    max_cost_per_hour: Optional[float] = Field(default=None, ge=0.0)
    input_asset_hashes: Set[str] = Field(default_factory=set)
    asset_sizes_bytes: Dict[str, int] = Field(default_factory=dict)

    # Heterogeneous performance / overhead overrides
    worker_speed_multipliers: Dict[str, float] = Field(default_factory=dict)
    network_bandwidth_bytes_per_sec: Optional[float] = Field(default=None, gt=0.0)
    coordination_overhead_per_worker_seconds: Optional[float] = Field(default=None, ge=0.0)
    marginal_benefit_threshold: Optional[float] = Field(default=None, gt=0.0, le=1.0)


class WorkerSizingDetail(BaseModel):
    """Detailed per-worker allocation and execution projection."""

    worker_id: str
    relative_speed: float
    missing_asset_bytes: int
    transfer_time_seconds: float
    allocated_units: int
    estimated_compute_seconds: float
    estimated_total_seconds: float


class GroupSizingEvaluation(BaseModel):
    """Projection for a candidate cluster group size."""

    group_size: int
    worker_ids: List[str]
    aggregate_speed: float
    max_transfer_seconds: float
    compute_seconds: float
    coordination_seconds: float
    wall_clock_seconds: float
    speedup_over_single: float
    marginal_speedup: float
    meets_deadline: bool


class SizingDecision(BaseModel):
    """Adaptive sizing recommendation for a workload."""

    request_id: str
    policy_applied: SizingPolicy
    feasibility_status: SizingFeasibilityStatus
    recommended_worker_count: int
    selected_worker_ids: List[str]
    spare_worker_ids: List[str]
    estimated_wall_clock_seconds: float
    deadline_seconds: Optional[float] = None
    deadline_achievable: bool = True
    confidence_score: float = 1.0
    worker_details: List[WorkerSizingDetail] = Field(default_factory=list)
    evaluations_by_group_size: List[GroupSizingEvaluation] = Field(default_factory=list)
    explanation: str = ""
    rejection_diagnostics: Dict[str, List[str]] = Field(default_factory=dict)
    evaluated_at_utc: float = Field(default_factory=time.time)


class ClusterSizer:
    """Calculates the minimum sufficient worker subset for distributed execution."""

    def __init__(
        self,
        placement_engine: Optional[PlacementEngine] = None,
        default_network_bandwidth_bytes_per_sec: float = 50.0 * 1024 * 1024,  # 50 MB/s
        default_coordination_overhead_per_worker_seconds: float = 1.0,  # 1.0s per worker
        marginal_benefit_threshold: float = 0.15,  # 15% marginal speedup threshold
        safety_margin: float = 0.10,  # 10% safety buffer for deadline feasibility
    ):
        self.placement_engine = placement_engine or PlacementEngine()
        self.default_network_bandwidth = default_network_bandwidth_bytes_per_sec
        self.default_coordination_overhead = default_coordination_overhead_per_worker_seconds
        self.marginal_benefit_threshold = marginal_benefit_threshold
        self.safety_margin = safety_margin

    def build_probe_spec(self, request: SizingRequest, now_utc: float) -> WorkloadSpec:
        """Construct a probe WorkloadSpec to delegate technical eligibility to PlacementEngine.

        When request.min_ram_bytes is None (unknown RAM requirement), sets min_ram_bytes=1
        to satisfy WorkloadSpec schema (ge=1) without inventing an artificial memory floor.
        """
        deadline_at_utc = request.deadline_at_utc
        if deadline_at_utc is None and request.deadline_seconds is not None:
            deadline_at_utc = now_utc + request.deadline_seconds

        if request.unit_workload_seconds is not None:
            est_duration = max(0.1, float(request.total_units * request.unit_workload_seconds))
        else:
            est_duration = 10.0

        ram_bytes = request.min_ram_bytes if request.min_ram_bytes is not None else 1

        return WorkloadSpec(
            workload_id=f"probe-{request.request_id}",
            task_type=request.task_type,
            input_asset_hashes=request.input_asset_hashes,
            min_cpu_cores=request.min_cpu_cores,
            min_ram_bytes=ram_bytes,
            requires_gpu=request.requires_gpu,
            min_vram_bytes=request.min_vram_bytes,
            estimated_duration_seconds=est_duration,
            deadline_at_utc=deadline_at_utc,
            required_gpu_vendor=request.required_gpu_vendor,
            required_gpu_model=request.required_gpu_model,
            min_compute_capability=request.min_compute_capability,
            required_driver_version=request.required_driver_version,
            required_runtime_compatibility=request.required_runtime_compatibility,
            required_worker_tags={**request.required_worker_tags, **request.required_tags},
            preferred_worker_tags=request.preferred_worker_tags,
            affinity_group_id=request.affinity_group_id,
            affinity_mode=request.affinity_mode,
            affinity_tag_key=request.affinity_tag_key,
            affinity_hard=request.affinity_hard,
            max_cost_per_hour=request.max_cost_per_hour,
            execution_spec=request.execution_spec,
            parameters=request.parameters,
        )

    def size_cluster(
        self,
        request: SizingRequest,
        profiles: List[WorkerResourceProfile],
        worker_tags: Optional[Dict[str, Dict[str, str]]] = None,
        *,
        group_workers: Optional[Dict[str, Set[str]]] = None,
        now_utc: Optional[float] = None,
    ) -> SizingDecision:
        """Evaluate worker profiles and determine the minimum sufficient worker subset."""
        now = time.time() if now_utc is None else now_utc

        bandwidth = (
            request.network_bandwidth_bytes_per_sec
            if request.network_bandwidth_bytes_per_sec is not None
            else self.default_network_bandwidth
        )
        coord_overhead = (
            request.coordination_overhead_per_worker_seconds
            if request.coordination_overhead_per_worker_seconds is not None
            else self.default_coordination_overhead
        )
        marginal_thresh = (
            request.marginal_benefit_threshold
            if request.marginal_benefit_threshold is not None
            else self.marginal_benefit_threshold
        )
        effective_deadline = request.deadline_seconds
        if effective_deadline is None and request.deadline_at_utc is not None:
            effective_deadline = max(0.0, request.deadline_at_utc - now)

        # 1. Step 1: Delegate technical eligibility directly to PlacementEngine
        probe_spec = self.build_probe_spec(request, now)
        self.placement_engine.evaluate(
            probe_spec,
            profiles,
            worker_tags=worker_tags,
            group_workers=group_workers,
            now_utc=now,
            asset_sizes_bytes=request.asset_sizes_bytes,
        )

        last_eval = self.placement_engine.last_evaluation or {}
        eligible_worker_ids = set(last_eval.get("eligible_worker_ids", []))
        rejection_diagnostics: Dict[str, List[str]] = {}
        for cand in last_eval.get("candidates", []):
            wid = cand.get("worker_id")
            if wid and not cand.get("eligible", False):
                rejection_diagnostics[wid] = cand.get("rejection_reasons", [])

        eligible_profiles = [p for p in profiles if p.worker_id in eligible_worker_ids]

        if not eligible_profiles:
            return SizingDecision(
                request_id=request.request_id,
                policy_applied=SizingPolicy.SINGLE_WORKER_SUFFICIENT,
                feasibility_status=SizingFeasibilityStatus.UNSCHEDULABLE_CONSTRAINTS,
                recommended_worker_count=0,
                selected_worker_ids=[],
                spare_worker_ids=[],
                estimated_wall_clock_seconds=0.0,
                deadline_seconds=effective_deadline,
                deadline_achievable=False,
                confidence_score=1.0,
                explanation="No workers meet technical placement eligibility constraints.",
                rejection_diagnostics=rejection_diagnostics,
                evaluated_at_utc=now,
            )

        # Step 2: Handle duration estimates and confidence
        has_estimate = request.unit_workload_seconds is not None
        confidence = 1.0 if has_estimate else 0.5
        unit_seconds = request.unit_workload_seconds if has_estimate else 10.0

        def get_worker_missing_bytes(p: WorkerResourceProfile) -> int:
            missing = request.input_asset_hashes - p.local_cached_hashes
            return sum(request.asset_sizes_bytes.get(h, 0) for h in missing)

        def get_worker_transfer_time(p: WorkerResourceProfile) -> float:
            missing_bytes = get_worker_missing_bytes(p)
            if bandwidth <= 0:
                return 0.0
            return missing_bytes / bandwidth

        def get_worker_speed(p: WorkerResourceProfile) -> float:
            return max(0.01, request.worker_speed_multipliers.get(p.worker_id, 1.0))

        # Sort eligible workers by single-worker wall time ascending
        sorted_profiles = sorted(
            eligible_profiles,
            key=lambda p: (
                get_worker_transfer_time(p) + (request.total_units * unit_seconds) / get_worker_speed(p),
                p.worker_id,
            ),
        )

        # Step 3: Handle non-partitionable or single-unit workloads
        if not request.partitionable or request.total_units <= 1:
            best_p = sorted_profiles[0]
            transfer_t = get_worker_transfer_time(best_p)
            compute_t = (request.total_units * unit_seconds) / get_worker_speed(best_p)
            total_t = transfer_t + compute_t
            detail = WorkerSizingDetail(
                worker_id=best_p.worker_id,
                relative_speed=get_worker_speed(best_p),
                missing_asset_bytes=get_worker_missing_bytes(best_p),
                transfer_time_seconds=transfer_t,
                allocated_units=request.total_units,
                estimated_compute_seconds=compute_t,
                estimated_total_seconds=total_t,
            )
            meets_deadline = True if effective_deadline is None else (total_t <= effective_deadline)
            if not meets_deadline:
                feasibility = SizingFeasibilityStatus.INFEASIBLE_DEADLINE
                achievable = False
            elif not has_estimate:
                feasibility = SizingFeasibilityStatus.UNKNOWN_ESTIMATE
                achievable = True
            else:
                feasibility = SizingFeasibilityStatus.FEASIBLE
                achievable = True

            spares = [p.worker_id for p in sorted_profiles[1:]]
            group_eval = GroupSizingEvaluation(
                group_size=1,
                worker_ids=[best_p.worker_id],
                aggregate_speed=get_worker_speed(best_p),
                max_transfer_seconds=transfer_t,
                compute_seconds=compute_t,
                coordination_seconds=0.0,
                wall_clock_seconds=total_t,
                speedup_over_single=1.0,
                marginal_speedup=0.0,
                meets_deadline=meets_deadline,
            )
            return SizingDecision(
                request_id=request.request_id,
                policy_applied=SizingPolicy.NON_PARTITIONABLE,
                feasibility_status=feasibility,
                recommended_worker_count=1,
                selected_worker_ids=[best_p.worker_id],
                spare_worker_ids=spares,
                estimated_wall_clock_seconds=total_t,
                deadline_seconds=effective_deadline,
                deadline_achievable=achievable,
                confidence_score=confidence,
                worker_details=[detail],
                evaluations_by_group_size=[group_eval],
                explanation="Workload is non-partitionable or contains a single unit; allocated to single best worker.",
                rejection_diagnostics=rejection_diagnostics,
                evaluated_at_utc=now,
            )

        # Step 4: Evaluate candidate group sizes k = 1 .. max_k
        max_possible_k = min(len(sorted_profiles), max(1, request.total_units // request.min_units_per_worker))
        if request.max_workers_limit is not None:
            max_possible_k = min(max_possible_k, request.max_workers_limit)
        max_possible_k = max(1, max_possible_k)

        evaluations: List[GroupSizingEvaluation] = []
        allocations_by_k: Dict[int, List[WorkerSizingDetail]] = {}

        t_wall_1 = 0.0

        for k in range(1, max_possible_k + 1):
            sub_profiles = sorted_profiles[:k]
            sub_speeds = [get_worker_speed(p) for p in sub_profiles]
            total_speed = sum(sub_speeds)

            # Integer unit allocation using largest-remainder method
            quotas = [request.total_units * (s / total_speed) for s in sub_speeds]
            floored = [int(math.floor(q)) for q in quotas]
            remainder = request.total_units - sum(floored)
            fractions = [(quotas[i] - floored[i], i) for i in range(k)]
            fractions.sort(key=lambda x: x[0], reverse=True)
            for r in range(remainder):
                idx = fractions[r][1]
                floored[idx] += 1

            worker_details_k: List[WorkerSizingDetail] = []
            transfer_times = []
            compute_times = []
            worker_finish_times = []

            for i, p in enumerate(sub_profiles):
                u_count = floored[i]
                t_trans = get_worker_transfer_time(p)
                t_comp = (u_count * unit_seconds) / sub_speeds[i] if u_count > 0 else 0.0
                t_fin = t_trans + t_comp
                transfer_times.append(t_trans)
                compute_times.append(t_comp)
                worker_finish_times.append(t_fin)
                worker_details_k.append(
                    WorkerSizingDetail(
                        worker_id=p.worker_id,
                        relative_speed=sub_speeds[i],
                        missing_asset_bytes=get_worker_missing_bytes(p),
                        transfer_time_seconds=t_trans,
                        allocated_units=u_count,
                        estimated_compute_seconds=t_comp,
                        estimated_total_seconds=t_fin,
                    )
                )

            max_trans = max(transfer_times) if transfer_times else 0.0
            max_comp = max(compute_times) if compute_times else 0.0
            t_coord = (k - 1) * coord_overhead
            wall_clock = max(worker_finish_times) + t_coord

            if k == 1:
                t_wall_1 = wall_clock
                speedup = 1.0
                marginal_speedup = 0.0
            else:
                speedup = t_wall_1 / wall_clock if wall_clock > 0 else 1.0
                prev_wall = evaluations[-1].wall_clock_seconds
                marginal_speedup = (prev_wall - wall_clock) / prev_wall if prev_wall > 0 else 0.0

            meets_deadline = True if effective_deadline is None else (wall_clock <= effective_deadline)

            evaluations.append(
                GroupSizingEvaluation(
                    group_size=k,
                    worker_ids=[p.worker_id for p in sub_profiles],
                    aggregate_speed=total_speed,
                    max_transfer_seconds=max_trans,
                    compute_seconds=max_comp,
                    coordination_seconds=t_coord,
                    wall_clock_seconds=wall_clock,
                    speedup_over_single=speedup,
                    marginal_speedup=marginal_speedup,
                    meets_deadline=meets_deadline,
                )
            )
            allocations_by_k[k] = worker_details_k

        # Step 5: Decision Policy Selection
        chosen_k = 1
        chosen_policy = SizingPolicy.SINGLE_WORKER_SUFFICIENT
        deadline_achievable = True

        if effective_deadline is not None:
            # Deadline mode: find smallest group meeting deadline with safety buffer
            safety_target = effective_deadline * (1.0 - self.safety_margin)

            # Check if 1 worker meets deadline with safety buffer
            if evaluations[0].wall_clock_seconds <= safety_target:
                # 1 worker meets deadline with safety buffer.
                # Only consider 2nd worker if marginal speedup is significant AND transfer overhead doesn't hurt.
                if len(evaluations) > 1 and evaluations[1].marginal_speedup >= marginal_thresh and evaluations[1].wall_clock_seconds < evaluations[0].wall_clock_seconds:
                    # Even with significant speedup, if 1 worker is sufficient, does AIDAR recruit?
                    # By AIDAR core objective: "If one PC meets the deadline, use one!"
                    # However, if marginal gain is < threshold, Policy A says definitely 1 worker.
                    chosen_k = 1
                    chosen_policy = SizingPolicy.SINGLE_WORKER_SUFFICIENT
                else:
                    chosen_k = 1
                    chosen_policy = SizingPolicy.SINGLE_WORKER_SUFFICIENT
            else:
                # 1 worker cannot meet the safety target.
                # Find smallest k that meets safety target
                k_found = None
                for ev in evaluations:
                    if ev.wall_clock_seconds <= safety_target:
                        k_found = ev.group_size
                        break

                # If no k meets safety target, check if any k meets strict deadline
                if k_found is None:
                    for ev in evaluations:
                        if ev.wall_clock_seconds <= effective_deadline:
                            k_found = ev.group_size
                            break

                if k_found is not None:
                    chosen_k = k_found
                    chosen_policy = SizingPolicy.DEADLINE_DRIVEN_MINIMAL
                else:
                    # Infeasible deadline: no group size can meet deadline
                    # Pick group size with lowest wall clock time
                    best_ev = min(evaluations, key=lambda ev: ev.wall_clock_seconds)
                    chosen_k = best_ev.group_size
                    chosen_policy = SizingPolicy.DEADLINE_DRIVEN_MINIMAL
                    deadline_achievable = False
        else:
            # No deadline specified: Efficiency / Throughput mode
            chosen_k = 1
            chosen_policy = SizingPolicy.EFFICIENCY_THROUGHPUT

            for ev in evaluations[1:]:
                # If adding this worker increases wall clock time, halt expansion immediately
                if ev.wall_clock_seconds >= evaluations[ev.group_size - 2].wall_clock_seconds:
                    chosen_policy = SizingPolicy.TRANSFER_OVERHEAD_HALT
                    break

                # If marginal speedup falls below threshold, halt expansion
                if ev.marginal_speedup < marginal_thresh:
                    chosen_policy = SizingPolicy.DIMINISHING_RETURNS
                    break

                chosen_k = ev.group_size

        # Check if chosen_k is 1 due to transfer overhead penalty from worker 2
        if (
            chosen_k == 1
            and len(evaluations) > 1
            and evaluations[1].wall_clock_seconds >= evaluations[0].wall_clock_seconds
            and chosen_policy not in (SizingPolicy.SINGLE_WORKER_SUFFICIENT, SizingPolicy.NON_PARTITIONABLE)
        ):
            chosen_policy = SizingPolicy.TRANSFER_OVERHEAD_HALT

        chosen_eval = evaluations[chosen_k - 1]
        selected_ids = chosen_eval.worker_ids
        spare_ids = [p.worker_id for p in sorted_profiles if p.worker_id not in selected_ids]

        if not deadline_achievable:
            feasibility = SizingFeasibilityStatus.INFEASIBLE_DEADLINE
        elif not has_estimate:
            feasibility = SizingFeasibilityStatus.UNKNOWN_ESTIMATE
        else:
            feasibility = SizingFeasibilityStatus.FEASIBLE

        explanation = (
            f"Policy {chosen_policy.value} selected {chosen_k} worker(s) out of {len(eligible_profiles)} "
            f"eligible (estimated wall clock: {chosen_eval.wall_clock_seconds:.2f}s, "
            f"aggregate speed: {chosen_eval.aggregate_speed:.2f}x)."
        )

        return SizingDecision(
            request_id=request.request_id,
            policy_applied=chosen_policy,
            feasibility_status=feasibility,
            recommended_worker_count=chosen_k,
            selected_worker_ids=selected_ids,
            spare_worker_ids=spare_ids,
            estimated_wall_clock_seconds=chosen_eval.wall_clock_seconds,
            deadline_seconds=effective_deadline,
            deadline_achievable=deadline_achievable,
            confidence_score=confidence,
            worker_details=allocations_by_k.get(chosen_k, []),
            evaluations_by_group_size=evaluations,
            explanation=explanation,
            rejection_diagnostics=rejection_diagnostics,
            evaluated_at_utc=now,
        )
