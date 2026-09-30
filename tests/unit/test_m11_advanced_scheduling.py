"""Focused M11 contracts: explainability, locality, GPU, queue, affinity, deadlines."""
import asyncio
import sqlite3
import time

import pytest
from types import SimpleNamespace

from aidars.distributed.models import (PlacementDecision, WorkloadExecutionResult, WorkerInfo,
                                       WorkerResourceProfile, WorkerStatus, WorkloadSpec)
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.workload_registry import WorkloadRecord, WorkloadRegistry, WorkloadState


H1 = "a" * 64
H2 = "b" * 64


def profile(worker_id="w-1", **overrides):
    values = dict(worker_id=worker_id, endpoint_url=f"http://{worker_id}", ip_address="127.0.0.1",
                  cpu_cores_total=8, cpu_utilization_percent=0, ram_total_bytes=16_000,
                  ram_available_bytes=16_000, gpu_available=True, gpu_vendor="NVIDIA",
                  gpu_model="RTX Test", gpu_device_name="RTX Test", gpu_compute_capability="8.9",
                  gpu_driver_version="550.54", vram_total_bytes=8_000, vram_available_bytes=8_000,
                  active_workload_count=0, max_concurrent_workloads=2,
                  local_cached_hashes=set(), timestamp_utc=time.time())
    values.update(overrides)
    return WorkerResourceProfile(**values)


def spec(**overrides):
    values = dict(workload_id="wl", task_type="generic", min_cpu_cores=1,
                  min_ram_bytes=100, estimated_duration_seconds=10)
    values.update(overrides)
    return WorkloadSpec(**values)


def test_candidate_explanation_keeps_rejections_and_m6_score_separate():
    engine = PlacementEngine()
    result = engine.evaluate(spec(min_cpu_cores=5, min_ram_bytes=20_000),
                             [profile("rejected", cpu_cores_total=4, ram_available_bytes=100)])
    assert result is None
    candidate = engine.last_evaluation["candidates"][0]
    assert candidate["eligible"] is False
    assert "INSUFFICIENT_CPU" in candidate["rejection_reasons"]
    assert "INSUFFICIENT_RAM" in candidate["rejection_reasons"]


@pytest.mark.parametrize(("overrides", "requirement", "reason"), [
    ({"cpu_cores_total": 1}, {"min_cpu_cores": 2}, "INSUFFICIENT_CPU"),
    ({"ram_available_bytes": 10}, {"min_ram_bytes": 20}, "INSUFFICIENT_RAM"),
    ({"gpu_available": False, "gpu_vendor": None, "gpu_model": None}, {"requires_gpu": True}, "REQUIRED_GPU_UNAVAILABLE"),
    ({"vram_available_bytes": 1}, {"min_vram_bytes": 2}, "INSUFFICIENT_VRAM"),
    ({"timestamp_utc": time.time() - 20}, {}, "STALE_TELEMETRY"),
    ({"active_workload_count": 2}, {}, "CONCURRENCY_LIMIT_REACHED"),
])
def test_hard_rejections_have_factual_reason(overrides, requirement, reason):
    engine = PlacementEngine()
    assert engine.evaluate(spec(**requirement), [profile(**overrides)]) is None
    assert reason in engine.last_evaluation["candidates"][0]["rejection_reasons"]


def test_gpu_strict_fields_require_known_matching_capabilities():
    engine = PlacementEngine()
    required = spec(required_gpu_vendor="nvidia", required_gpu_model="RTX Test",
                    min_compute_capability="8.0", required_driver_version="550.54")
    assert engine.evaluate(required, [profile()]) is not None
    assert engine.evaluate(spec(min_compute_capability="8.0"),
                           [profile(gpu_compute_capability=None)]) is None
    assert engine.evaluate(spec(required_gpu_vendor="AMD"), [profile()]) is None
    assert engine.evaluate(spec(required_gpu_model="Other"), [profile()]) is None
    assert engine.evaluate(spec(required_driver_version="old-driver"), [profile()]) is None
    assert engine.evaluate(spec(required_runtime_compatibility="cuda-12"), [profile()]) is None


def test_m7_only_reorders_m6_eligible_candidates_and_scores_stay_separate():
    class Bridge:
        def evaluate_candidates(self, workload, candidates):
            self.ids = {candidate.worker_id for candidate in candidates}
            return {candidate.worker_id: SimpleNamespace(total_risk=0.9 if candidate.worker_id == "fast" else 0.0)
                    for candidate in candidates}

        def adjust_ranking(self, scores, risks, risk_weight):
            return sorted(scores, key=lambda wid: scores[wid] - risk_weight * risks[wid].total_risk, reverse=True)

    bridge = Bridge()
    engine = PlacementEngine(m7_bridge=bridge)
    result = engine.evaluate(spec(min_cpu_cores=2), [profile("fast"), profile("safe"), profile("invalid", cpu_cores_total=1)])
    assert bridge.ids == {"fast", "safe"}
    assert result.selected_worker_id == "safe"
    fast = next(c for c in result.candidate_explanations if c["worker_id"] == "fast")
    assert fast["m7_risk_score"] == 0.9
    assert fast["m6_score"] == fast["m6_score_breakdown"]["compute"] + fast["m6_score_breakdown"]["memory"] + \
        2.0 * fast["m6_score_breakdown"]["gpu"] + 2.0 * fast["m6_score_breakdown"]["locality"] + \
        fast["m6_score_breakdown"]["latency"] - 0.5 * fast["m6_score_breakdown"]["queue"]
    assert "INSUFFICIENT_CPU" in next(c for c in result.candidate_explanations if c["worker_id"] == "invalid")["rejection_reasons"]


def test_locality_distinguishes_all_partial_none_and_uses_known_bytes():
    engine = PlacementEngine()
    workload = spec(input_asset_hashes={H1, H2})
    candidates = [profile("small", local_cached_hashes={H2}),
                  profile("large", local_cached_hashes={H1}),
                  profile("none")]
    result = engine.evaluate(workload, candidates, asset_sizes_bytes={H1: 1000, H2: 1})
    assert result.selected_worker_id == "large"
    assert result.locality_details["locality_class"] == "partial"
    assert result.locality_details["known_byte_fraction"] == 1000 / 1001
    all_local = engine.evaluate(spec(input_asset_hashes={H1}), [profile(local_cached_hashes={H1})])
    assert all_local.locality_details["locality_class"] == "all_local"
    none_local = engine.evaluate(spec(input_asset_hashes={H1}), [profile()])
    assert none_local.locality_details["locality_class"] == "none"


def test_unknown_size_fallback_is_explicit_and_deterministic():
    engine = PlacementEngine()
    workload = spec(input_asset_hashes={H1, H2})
    result = engine.evaluate(workload, [profile(local_cached_hashes={H1})])
    assert result.locality_details["known_byte_fraction"] is None
    assert result.locality_details["unknown_size_count"] == 2
    assert result.locality_details["ranking_basis"] == "asset_count_fallback_incomplete_or_zero_sizes"
    assert result.locality_details["asset_count_fraction"] == 0.5
    mixed = engine.evaluate(workload, [profile(local_cached_hashes={H1})], asset_sizes_bytes={H1: 999})
    assert mixed.locality_details["unknown_size_count"] == 1
    assert mixed.locality_details["ranking_fraction"] == 0.5
    assert mixed.locality_details["ranking_basis"] == "asset_count_fallback_incomplete_or_zero_sizes"


def test_locality_never_overrides_hard_resources_and_missing_runtime_is_ineligible():
    engine = PlacementEngine()
    result = engine.evaluate(spec(input_asset_hashes={H1}, min_cpu_cores=4),
                             [profile(local_cached_hashes={H1}, cpu_cores_total=2), profile("ok")],
                             asset_sizes_bytes={H1: 10})
    assert result.selected_worker_id == "ok"


def test_deadline_and_soft_affinity_cannot_override_m6_hard_eligibility():
    engine = PlacementEngine()
    invalid = profile("invalid", cpu_cores_total=1, local_cached_hashes={H1})
    result = engine.evaluate(spec(min_cpu_cores=4, input_asset_hashes={H1},
                                  preferred_worker_tags={"domain": "preferred"},
                                  deadline_at_utc=time.time() + 1),
                             [invalid], worker_tags={"invalid": {"domain": "preferred"}})
    assert result is None
    assert "INSUFFICIENT_CPU" in engine.last_evaluation["candidates"][0]["rejection_reasons"]


def test_affinity_tag_and_group_constraints_are_explicit():
    engine = PlacementEngine()
    workload = spec(required_worker_tags={"locality_domain": "lab-a"},
                    preferred_worker_tags={"gpu_class": "large"},
                    affinity_group_id="g1", affinity_mode="different_worker", affinity_hard=True)
    result = engine.evaluate(workload, [profile("used"), profile("match")],
                             worker_tags={"used": {"locality_domain": "lab-a", "gpu_class": "large"},
                                          "match": {"locality_domain": "lab-a", "gpu_class": "small"}},
                             group_workers={"g1": {"used"}})
    assert result.selected_worker_id == "match"
    assert "AFFINITY_CONSTRAINT_NOT_MET" in result.candidate_explanations[0]["rejection_reasons"]
    assert result.candidate_explanations[1]["checks"]["preferred_worker_tags"]["result"] is False


def test_affinity_requires_explicit_group_and_missing_tag_is_not_a_match():
    with pytest.raises(ValueError):
        spec(affinity_mode="same_worker")
    engine = PlacementEngine()
    workload = spec(required_worker_tags={"domain": "lab-a"})
    assert engine.evaluate(workload, [profile()], worker_tags={"w-1": {}}) is None
    assert "REQUIRED_WORKER_TAG_MISMATCH" in engine.last_evaluation["candidates"][0]["rejection_reasons"]


def test_group_affinity_can_use_existing_worker_tag_domain():
    engine = PlacementEngine()
    workload = spec(affinity_group_id="g1", affinity_mode="same_tag", affinity_tag_key="locality_domain")
    result = engine.evaluate(workload, [profile("lab-a"), profile("lab-b")],
                             worker_tags={"lab-a": {"locality_domain": "a"},
                                          "lab-b": {"locality_domain": "b"}},
                             group_workers={"g1": {"lab-a"}})
    assert result.selected_worker_id == "lab-a"
    assert "AFFINITY_CONSTRAINT_NOT_MET" in result.candidate_explanations[1]["rejection_reasons"]


def test_placement_decision_additive_explanation_deserializes_legacy_record():
    legacy = {"workload_id": "w", "selected_worker_id": "n", "placement_score": 1.0,
              "score_breakdown": {}, "missing_assets_on_worker": [], "execution_tier": "lan"}
    decision = PlacementDecision.model_validate(legacy)
    assert decision.candidate_explanations == []
    assert decision.m7_ranking_adjustments == {}


def test_m11_contract_and_placement_explanation_round_trip(tmp_path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    workload = spec(deadline_at_utc=1_900_000_000,
                    required_gpu_vendor="NVIDIA", affinity_group_id="batch",
                    affinity_mode="same_worker", required_worker_tags={"domain": "a"})
    record = WorkloadRecord(workload)
    record.placement_explanation = {"eligible_worker_ids": ["w-1"], "candidates": []}
    record.placement_decision = PlacementEngine().evaluate(
        workload,
        [profile()],
        worker_tags={"w-1": {"domain": "a"}},
        asset_sizes_bytes={H1: 5},
    )
    assert record.placement_decision is not None
    store.save_workload(record)
    store.save_worker(WorkerInfo(worker_id="w-1", endpoint_url="http://w-1", ip_address="127.0.0.1",
                                 port=8000, resource_profile=profile()))
    restored = store.load_workloads()[0]
    restored_worker = store.load_workers()[0]
    assert restored.spec.deadline_at_utc == workload.deadline_at_utc
    assert restored.spec.affinity_group_id == "batch"
    assert restored.placement_explanation == record.placement_explanation
    assert restored.placement_decision == record.placement_decision
    assert restored.placement_decision.candidate_explanations
    assert restored_worker.resource_profile.gpu_vendor == "NVIDIA"
    assert restored_worker.resource_profile.gpu_compute_capability == "8.9"
    store.close()


def test_old_workloads_table_gets_only_additive_explanation_column(tmp_path):
    path = tmp_path / "old.db"
    connection = sqlite3.connect(path)
    connection.execute("""CREATE TABLE workloads (
        workload_id TEXT PRIMARY KEY, spec_json TEXT NOT NULL, state TEXT NOT NULL,
        placement_json TEXT, execution_result_json TEXT, error_message TEXT,
        submitted_at REAL NOT NULL, completed_at REAL, updated_at REAL NOT NULL
    )""")
    connection.commit()
    connection.close()
    store = CoordinatorStateStore(path)
    columns = {row[1] for row in store._conn.execute("PRAGMA table_info(workloads)").fetchall()}
    assert "placement_explanation_json" in columns
    assert "priority" not in columns  # M11 metadata remains in existing JSON records.
    store.close()


@pytest.mark.asyncio
async def test_queue_orders_by_priority_age_and_keeps_temporary_capacity_pending():
    registry = WorkloadRegistry()
    orchestrator = WorkloadOrchestrator(WorkerRegistry(), registry, max_dispatch_tasks=1)
    try:
        old = registry.add_workload(spec(workload_id="old", priority=0))
        old.submitted_at = time.time() - 700
        registry.add_workload(spec(workload_id="new", priority=100))
        assert orchestrator.queue_snapshot()[0]["workload_id"] == "old"
        assert orchestrator.queue_snapshot()[0]["starvation_override"] is True
        await orchestrator.submit_workload(spec(workload_id="waiting"))
        await asyncio.sleep(0.05)
        record = registry.get_workload("waiting")
        assert record.state == WorkloadState.SUBMITTED
        assert record.placement_explanation
        assert "waiting" in {r["workload_id"] for r in orchestrator.queue_snapshot()}
    finally:
        await orchestrator.stop_queue()


@pytest.mark.asyncio
async def test_queued_workload_is_reconsidered_when_worker_becomes_available():
    registry = WorkerRegistry()
    workloads = WorkloadRegistry()
    orchestrator = WorkloadOrchestrator(registry, workloads)

    class Response:
        def json(self):
            return WorkloadExecutionResult(workload_id="later", worker_id="w-1", success=True,
                                           output_asset_hashes=set(), execution_duration_seconds=0.1).model_dump(mode="json")

        def raise_for_status(self):
            return None

    async def fake_post(*args, **kwargs):
        return Response()

    orchestrator.http_client = SimpleNamespace(post=fake_post)
    submitted = time.time()
    try:
        await orchestrator.submit_workload(spec(workload_id="later"))
        await asyncio.sleep(0.05)
        assert workloads.get_workload("later").state == WorkloadState.SUBMITTED
        registry.register_worker(WorkerInfo(worker_id="w-1", endpoint_url="http://w-1",
            ip_address="127.0.0.1", port=8000, status=WorkerStatus.ACTIVE, resource_profile=profile()))
        deadline = time.monotonic() + 2.5
        while workloads.get_workload("later").state != WorkloadState.COMPLETED and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
        record = workloads.get_workload("later")
        assert record.state == WorkloadState.COMPLETED
        assert record.submitted_at >= submitted
        assert record.placement_decision.selected_worker_id == "w-1"
    finally:
        await orchestrator.stop_queue()


@pytest.mark.asyncio
async def test_admission_queue_bounds_dispatch_tasks():
    workloads = WorkloadRegistry()
    orchestrator = WorkloadOrchestrator(WorkerRegistry(), workloads, max_dispatch_tasks=2)
    active = peak = 0
    processed = []

    async def fake_process(workload_id):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        processed.append(workload_id)
        await asyncio.sleep(0.03)
        workloads.update_state(workload_id, WorkloadState.COMPLETED)
        active -= 1

    orchestrator._process_workload = fake_process
    try:
        for index in range(5):
            workload_id = f"bounded-{index}"
            workloads.add_workload(spec(workload_id=workload_id, priority=100 - index))
            orchestrator.enqueue_workload(workload_id)
        deadline = time.monotonic() + 2
        while len(processed) < 5 and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert len(processed) == 5
        assert peak <= 2
    finally:
        await orchestrator.stop_queue()


@pytest.mark.asyncio
async def test_unsupported_gpu_runtime_requirement_is_explicitly_unschedulable():
    orchestrator = WorkloadOrchestrator(WorkerRegistry(), WorkloadRegistry())
    record = orchestrator.workload_registry.add_workload(
        spec(workload_id="unsupported", required_runtime_compatibility="cuda-12"))
    orchestrator.registry.register_worker(WorkerInfo(worker_id="w-1", endpoint_url="http://w-1",
        ip_address="127.0.0.1", port=8000, status=WorkerStatus.ACTIVE, resource_profile=profile()))
    await orchestrator._process_workload(record.spec.workload_id)
    assert record.state == WorkloadState.UNSCHEDULABLE
    assert "runtime compatibility" in record.error_message


def test_deadline_expiry_does_not_change_worker_eligibility_or_timeout_contract():
    engine = PlacementEngine()
    decision = engine.evaluate(spec(deadline_at_utc=time.time() - 100), [profile()])
    assert decision is not None
    assert decision.deadline_details["state"] == "expired"
    assert decision.deadline_details["ranking_scope"] == "workload_admission_only"


def test_deadline_urgency_uses_m7_history_when_present_and_fallback_when_missing():
    memory = SimpleNamespace(get_workload_state=lambda task: SimpleNamespace(
        duration_ema=SimpleNamespace(initialized=True, value=50.0)))
    bridge = SimpleNamespace(memory=memory)
    orchestrator = WorkloadOrchestrator(WorkerRegistry(), WorkloadRegistry(), m7_bridge=bridge)
    estimated = spec(workload_id="estimated", priority=20, estimated_duration_seconds=5,
                     deadline_at_utc=time.time() + 20)
    predicted = spec(workload_id="predicted", priority=20, estimated_duration_seconds=5,
                     deadline_at_utc=time.time() + 20)
    records = [WorkloadRecord(estimated), WorkloadRecord(predicted)]
    records[0].submitted_at = records[1].submitted_at
    assert orchestrator._predicted_duration(predicted) == (50.0, 50.0)
    assert orchestrator._deadline_priority_bonus(predicted, time.time()) > 90
    assert orchestrator._deadline_state(predicted, time.time()) == "approaching"
    assert orchestrator._deadline_priority_bonus(spec(deadline_at_utc=None), time.time()) == 0
    fallback = WorkloadOrchestrator(WorkerRegistry(), WorkloadRegistry())
    assert fallback._predicted_duration(spec(estimated_duration_seconds=12)) == (None, 12.0)


def test_equal_priority_ties_use_submitted_time_then_workload_id():
    workloads = WorkloadRegistry()
    late_id = workloads.add_workload(spec(workload_id="z-last", priority=50))
    early_id = workloads.add_workload(spec(workload_id="a-first", priority=50))
    late_id.submitted_at = early_id.submitted_at = 1234.0
    orchestrator = WorkloadOrchestrator(WorkerRegistry(), workloads)
    assert [row["workload_id"] for row in orchestrator.queue_snapshot()] == ["a-first", "z-last"]
