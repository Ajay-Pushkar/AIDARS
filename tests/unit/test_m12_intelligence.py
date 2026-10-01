"""M12 projections and advisory views stay truthful and replayable."""
import time

from fastapi.testclient import TestClient

from aidars.distributed.attempt import AttemptRecord, AttemptStatus, make_attempt_id
from aidars.distributed.auth import CredentialStore
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import (
    PlacementDecision, WorkloadExecutionResult, WorkloadSpec,
)
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.workload_registry import WorkloadRecord
from aidars.m12.history import HistoricalObservationProjector
from aidars.m12.models import DataQualityState, EstimateKind, OutcomeSemantics
from aidars.m12.service import DistributedIntelligence
from aidars.m7.telemetry import TelemetryMemory


def _record(wid="w-1", task="render"):
    return WorkloadRecord(WorkloadSpec(workload_id=wid, task_type=task))


def _attempt(wid, number=1, *, outcome="success", duration=2.0, checkpoint=False):
    now = time.time()
    a = AttemptRecord(attempt_id=make_attempt_id(wid, number), workload_id=wid,
        attempt_number=number, status=AttemptStatus.SUCCEEDED if outcome != "lost" else AttemptStatus.LOST,
        worker_id="worker-1", queued_at=now - 3, assigned_at=now - 2,
        started_at=now - 2, finished_at=now - 1)
    if outcome == "success":
        a.execution_result = WorkloadExecutionResult(workload_id=wid, worker_id="worker-1",
            success=True, output_asset_hashes=set(), execution_duration_seconds=duration,
            was_checkpointed=checkpoint)
    elif outcome == "failure":
        a.status = AttemptStatus.FAILED
        a.failure_reason = "explicit failure"
    elif outcome == "lost":
        a.status = AttemptStatus.LOST
        a.failure_reason = "worker disappeared"
    return a


def test_history_projects_success_failure_lost_and_checkpoint_without_invented_usage():
    record = _record()
    attempts = [_attempt("w-1", 1), _attempt("w-1", 2, outcome="failure"),
                _attempt("w-1", 3, outcome="lost"), _attempt("w-1", 4, checkpoint=True)]
    projection = HistoricalObservationProjector.project([record], attempts)
    by_id = {o.attempt_id: o for o in projection.observations}
    assert by_id["w-1#1"].outcome == OutcomeSemantics.SUCCESS
    assert by_id["w-1#2"].outcome == OutcomeSemantics.FAILURE
    assert by_id["w-1#3"].data_quality == DataQualityState.CENSORED
    assert by_id["w-1#4"].outcome == OutcomeSemantics.CHECKPOINTED
    assert by_id["w-1#1"].actual_resources.cpu_cores is None


def test_duplicate_attempt_identity_is_quarantined_not_double_counted():
    attempt = _attempt("w-1")
    projection = HistoricalObservationProjector.project([_record()], [attempt, attempt])
    assert projection.duplicate_count == 1
    assert all(o.data_quality == DataQualityState.INVALID for o in projection.observations)


def test_invalid_timestamp_negative_duration_and_missing_worker_are_quarantined():
    record = _record()
    bad_time = _attempt("w-1")
    bad_time.queued_at = float("nan")
    bad_duration = _attempt("w-1", 2, duration=-1)
    missing_worker = _attempt("w-1", 3)
    missing_worker.worker_id = None
    projection = HistoricalObservationProjector.project([record], [bad_time, bad_duration, missing_worker])
    assert all(o.data_quality == DataQualityState.INVALID for o in projection.observations)
    assert "INVALID_QUEUED_TIMESTAMP" in projection.observations[0].validation_errors
    assert any("INVALID_EXECUTION_DURATION" in o.validation_errors for o in projection.observations)
    assert any("TERMINAL_ATTEMPT_MISSING_WORKER_ID" in o.validation_errors for o in projection.observations)


def test_duration_fallback_is_not_mislabeled_as_prediction_and_resources_unknown():
    service = DistributedIntelligence(TelemetryMemory(), WorkerRegistry())
    view = service.workload_view(_record())
    assert view.duration.kind == EstimateKind.DECLARED_FALLBACK
    assert view.duration.value is None
    assert view.duration.fallback_value == 10.0
    assert all(v is None for v in view.resources.predictions.values())


def test_replay_is_idempotent_and_lost_or_checkpointed_attempts_do_not_train_m7():
    service = DistributedIntelligence(TelemetryMemory(), WorkerRegistry())
    record = _record()
    attempts = [_attempt("w-1", 1), _attempt("w-1", 2, outcome="lost"),
                _attempt("w-1", 3, checkpoint=True)]
    service.rebuild_history([record], attempts)
    first = service.memory.get_workload_state("render").duration_ema.value
    service.rebuild_history([record], attempts)
    assert service.memory.get_workload_state("render").duration_ema.value == first == 2.0


def test_observed_successes_enable_median_after_minimum_support():
    service = DistributedIntelligence(TelemetryMemory(), WorkerRegistry())
    record = _record()
    service.rebuild_history([record], [_attempt("w-1", n, duration=d) for n, d in enumerate((1, 2, 20), 1)])
    estimate = service.workload_view(record).duration
    assert estimate.kind == EstimateKind.PREDICTED
    assert estimate.value == 2
    assert estimate.sample_count == 3


def test_failure_rate_uses_explicit_outcomes_and_excludes_lost_attempts():
    service = DistributedIntelligence(TelemetryMemory(), WorkerRegistry())
    record = _record()
    attempts = [_attempt("w-1", 1), _attempt("w-1", 2, outcome="failure"),
                _attempt("w-1", 3, outcome="failure"), _attempt("w-1", 4, outcome="lost")]
    service.rebuild_history([record], attempts)
    prediction = service.workload_view(record).failure
    assert prediction.probability == 2 / 3
    assert prediction.sample_count == 3
    assert prediction.failure_count == 2


def test_duration_anomaly_requires_prior_support_and_reports_baseline():
    service = DistributedIntelligence(TelemetryMemory(), WorkerRegistry())
    record = _record()
    attempts = [_attempt("w-1", n, duration=(20 if n == 6 else 2)) for n in range(1, 7)]
    service.rebuild_history([record], attempts)
    anomalies = service.anomalies()
    assert len(anomalies) == 1
    assert anomalies[0].category == "DURATION_DEVIATION"
    assert anomalies[0].observed_value == 20
    assert anomalies[0].expected_value == 2
    assert anomalies[0].support_count == 5


def test_intelligence_routes_are_admin_only_and_redact_worker_network_identity():
    service = CoordinatorService(credential_store=CredentialStore(admin_tokens={"admin-test"}, insecure_mode=False))
    service.workload_registry.add_workload(WorkloadSpec(workload_id="visible", task_type="render"))
    with TestClient(service.app) as client:
        assert client.get("/api/v1/intelligence/workloads/visible").status_code == 401
        response = client.get("/api/v1/intelligence/workloads/visible",
            headers={"Authorization": "Bearer admin-test"})
        assert response.status_code == 200
        assert response.json()["resources"]["predictions"]["ram_peak_bytes"] is None
        assert client.get("/api/v1/intelligence/capacity",
            headers={"Authorization": "Bearer admin-test"}).status_code == 200


def test_attempt_scoped_placement_provenance_survives_coordinator_replay(tmp_path):
    from aidars.distributed.attempt import AttemptRegistry
    from aidars.distributed.state_store import CoordinatorStateStore

    db = tmp_path / "m12.db"
    store = CoordinatorStateStore(db)
    record = _record()
    store.save_workload(record)
    registry = AttemptRegistry(store)
    decision = PlacementDecision(workload_id="w-1", selected_worker_id="worker-1",
        placement_score=0.8, score_breakdown={"compute": 0.8}, missing_assets_on_worker=set(),
        execution_tier="local", candidate_explanations=[{"worker_id": "worker-1", "eligible": True}])
    attempt = registry.create_attempt("w-1", placement_decision=decision)
    registry.mark_assigned(attempt.attempt_id, "worker-1")
    registry.mark_running(attempt.attempt_id)
    registry.mark_succeeded(attempt.attempt_id, WorkloadExecutionResult(
        workload_id="w-1", worker_id="worker-1", success=True,
        output_asset_hashes=set(), execution_duration_seconds=2.5))
    store.close()
    recovered_store = CoordinatorStateStore(db)
    loaded = recovered_store.load_attempts()[0]
    assert loaded.attempt_id == attempt.attempt_id
    assert loaded.placement_decision.candidate_explanations[0]["eligible"] is True
    service = CoordinatorService(state_store=recovered_store,
        credential_store=CredentialStore(insecure_mode=False))
    service._restore_persisted_state()
    first = service.m7_memory.get_workload_state("render").duration_ema.value
    service._restore_persisted_state()
    assert service.m7_memory.get_workload_state("render").duration_ema.value == first == 2.5
    assert service.m12_intelligence.observations[0].placement_context["selected_worker_id"] == "worker-1"
