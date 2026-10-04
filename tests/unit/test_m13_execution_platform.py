"""Focused M13 generic execution-shape, dependency, and worker-group tests."""
from __future__ import annotations

import json
import time

import httpx
import pytest
from httpx import ASGITransport

from aidars.distributed.auth import CredentialStore
from aidars.distributed.attempt import AttemptRegistry, AttemptStatus
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.execution import ExecutionManager
from aidars.distributed.job_registry import JobRegistry
from aidars.distributed.models import (
    ExecutionDescription,
    ExecutionGroup,
    ExecutionShape,
    FailureCategory,
    PlacementDecision,
    WorkerInfo,
    WorkerResourceProfile,
    WorkerStatus,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.placement import PlacementEngine
from aidars.distributed.registry import WorkerRegistry
from aidars.distributed.runtime import RuntimeAdapter
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload import WorkloadOrchestrator
from aidars.distributed.workload_registry import WorkloadRecord, WorkloadRegistry, WorkloadState


def _spec(workload_id: str, **kwargs) -> WorkloadSpec:
    values = dict(workload_id=workload_id, task_type="generic", min_ram_bytes=10,
                  min_cpu_cores=1, estimated_duration_seconds=10)
    values.update(kwargs)
    return WorkloadSpec(**values)


def _profile(worker_id: str, **kwargs) -> WorkerResourceProfile:
    values = dict(worker_id=worker_id, endpoint_url=f"http://{worker_id}", ip_address="127.0.0.1",
                  cpu_cores_total=8, cpu_utilization_percent=0, ram_total_bytes=1000,
                  ram_available_bytes=1000, timestamp_utc=time.time(), max_concurrent_workloads=10)
    values.update(kwargs)
    return WorkerResourceProfile(**values)


def _worker(worker_id: str, profile: WorkerResourceProfile) -> WorkerInfo:
    return WorkerInfo(
        worker_id=worker_id, endpoint_url=profile.endpoint_url, ip_address="127.0.0.1",
        port=8000, status=WorkerStatus.ACTIVE, capacity_bytes=10000, used_bytes=0,
        resource_profile=profile,
    )


def test_execution_description_models_single_machine_task_split_and_pipeline():
    single = ExecutionDescription(execution_shape=ExecutionShape.SINGLE_MACHINE,
                                  workloads=[_spec("single")])
    split = ExecutionDescription(execution_shape=ExecutionShape.TASK_SPLIT,
                                  workloads=[_spec("frame-1"), _spec("frame-2")])
    pipeline = ExecutionDescription(execution_shape=ExecutionShape.PIPELINE, workloads=[
        _spec("decode"), _spec("process", depends_on=["decode"]),
        _spec("encode", depends_on=["process"]),
    ])
    assert single.execution_shape == ExecutionShape.SINGLE_MACHINE
    assert len(split.workloads) == 2
    assert pipeline.workloads[-1].depends_on == ["process"]
    with pytest.raises(ValueError, match="cycles"):
        ExecutionDescription(execution_shape=ExecutionShape.PIPELINE,
                             workloads=[_spec("a", depends_on=["b"]), _spec("b", depends_on=["a"])])


def test_single_machine_still_requires_one_worker_to_pass_m6_hard_gates():
    engine = PlacementEngine()
    assert engine.evaluate(_spec("one", min_cpu_cores=4), [_profile("too-small", cpu_cores_total=2)]) is None
    decision = engine.evaluate(_spec("one", min_cpu_cores=4), [_profile("enough")])
    assert decision is not None
    assert decision.selected_worker_id == "enough"


def test_pipeline_dependency_waits_unlocks_on_success_and_fails_on_upstream_failure():
    registry = WorkloadRegistry()
    upstream = registry.add_workload(_spec("upstream"))
    downstream = registry.add_workload(_spec("downstream", depends_on=["upstream"]))
    orchestrator = WorkloadOrchestrator(WorkerRegistry(), registry)
    assert orchestrator._dependency_state(downstream) == "waiting"
    registry.update_state(upstream.spec.workload_id, WorkloadState.COMPLETED)
    assert orchestrator._dependency_state(downstream) == "ready"

    failed_registry = WorkloadRegistry()
    failed_registry.add_workload(_spec("failed-upstream"))
    failed_downstream = failed_registry.add_workload(
        _spec("blocked", depends_on=["failed-upstream"]),
    )
    failed_registry.update_state("failed-upstream", WorkloadState.FAILED)
    failed_orchestrator = WorkloadOrchestrator(WorkerRegistry(), failed_registry)
    assert failed_orchestrator._dependency_state(failed_downstream) == "failed"
    assert failed_downstream.state == WorkloadState.FAILED


def test_pipeline_stage_receives_upstream_output_assets_as_inputs():
    registry = WorkloadRegistry()
    upstream = registry.add_workload(_spec("producer"))
    output_hash = "a" * 64
    registry.set_result("producer", WorkloadExecutionResult(
        workload_id="producer", worker_id="worker", success=True,
        output_asset_hashes={output_hash}, execution_duration_seconds=1,
    ))
    downstream = registry.add_workload(_spec("consumer", depends_on=["producer"]))
    orchestrator = WorkloadOrchestrator(WorkerRegistry(), registry)
    staged_spec = orchestrator._spec_with_dependency_outputs(downstream)
    assert staged_spec.input_asset_hashes == {output_hash}
    assert downstream.spec.input_asset_hashes == set()


def test_distributed_group_placement_uses_distinct_hard_eligible_workers():
    engine = PlacementEngine()
    decisions = engine.evaluate_group(
        _spec("collective", min_cpu_cores=2),
        [_profile("bad", cpu_cores_total=1), _profile("good-a"), _profile("good-b")],
        2,
    )
    assert decisions is not None
    selected = [decision.selected_worker_id for decision in decisions]
    assert len(set(selected)) == 2
    assert "bad" not in selected
    assert all(decision.candidate_explanations for decision in decisions)


def test_distributed_group_state_and_job_shape_round_trip(tmp_path):
    store = CoordinatorStateStore(tmp_path / "m13.db")
    jobs = JobRegistry(WorkloadRegistry(), store)
    jobs.create_job("job", {"collective"}, execution_shape=ExecutionShape.DISTRIBUTED_NATIVE,
                    required_worker_count=2)
    decision = PlacementDecision(
        workload_id="collective", selected_worker_id="worker-a", placement_score=1,
        score_breakdown={"compute": 1}, missing_assets_on_worker=set(), execution_tier="lan",
    )
    second_decision = decision.model_copy(update={"selected_worker_id": "worker-b"})
    group = ExecutionGroup(execution_id="exec-1", worker_ids=["worker-a", "worker-b"],
                           required_worker_count=2, placement_decisions=[decision, second_decision])
    attempts = AttemptRegistry(store)
    attempts.create_attempt("collective", placement_decision=decision, execution_group=group)
    attempts.mark_assigned("collective#1", "worker-a")
    attempts.mark_running("collective#1")
    store.close()

    restored_store = CoordinatorStateStore(tmp_path / "m13.db")
    restored_job = restored_store.load_jobs()[0]
    restored_attempt = restored_store.load_attempts()[0]
    assert restored_job.execution_shape == ExecutionShape.DISTRIBUTED_NATIVE
    assert restored_job.required_worker_count == 2
    assert restored_attempt.execution_group.worker_ids == ["worker-a", "worker-b"]
    assert restored_attempt.execution_group.state == "running"
    assert len(restored_attempt.to_summary_dict()["execution_group"]["placement_decisions"]) == 2
    restored_store.close()


@pytest.mark.asyncio
async def test_distributed_native_runtime_receives_group_context_and_all_members_must_succeed():
    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    attempts = AttemptRegistry()
    jobs = JobRegistry(workload_registry)
    orchestrator = WorkloadOrchestrator(registry, workload_registry,
                                        job_registry=jobs, attempt_registry=attempts)
    for name in ("worker-a", "worker-b"):
        profile = _profile(name)
        registry.register_worker(_worker(name, profile).model_copy(
            update={"endpoint_url": f"http://user:secret@{name}?token=secret"},
        ))
    workload = _spec("collective", job_id="job-distributed")
    workload_registry.add_workload(workload)
    jobs.create_job("job-distributed", {"collective"},
                    execution_shape=ExecutionShape.DISTRIBUTED_NATIVE, required_worker_count=2)
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert b"secret" not in request.content
        payload = __import__("json").loads(request.content)
        context = payload["execution_context"]
        assert all("secret" not in worker["endpoint_url"] for worker in context["workers"])
        seen.append(context)
        return httpx.Response(200, json=WorkloadExecutionResult(
            workload_id="collective", worker_id=context["worker_id"], success=True,
            output_asset_hashes=set(), execution_duration_seconds=0.5,
        ).model_dump(mode="json"))

    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await orchestrator._process_workload("collective")
    assert len(seen) == 2
    assert {item["rank"] for item in seen} == {0, 1}
    assert all(item["world_size"] == 2 for item in seen)
    assert attempts.get_latest_attempt("collective").status == AttemptStatus.SUCCEEDED
    assert attempts.get_latest_attempt("collective").execution_group.state == "completed"
    api_group = attempts.get_latest_attempt("collective").to_summary_dict()["execution_group"]
    assert set(api_group["worker_ids"]) == {"worker-a", "worker-b"}
    assert len(api_group["placement_decisions"]) == 2


@pytest.mark.asyncio
async def test_distributed_group_worker_loss_is_not_reported_as_success():
    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    attempts = AttemptRegistry()
    jobs = JobRegistry(workload_registry)
    orchestrator = WorkloadOrchestrator(registry, workload_registry,
                                        job_registry=jobs, attempt_registry=attempts,
                                        max_attempts=1)
    for name in ("worker-a", "worker-b"):
        registry.register_worker(_worker(name, _profile(name)))
    workload_registry.add_workload(_spec("collective", job_id="job-loss"))
    jobs.create_job("job-loss", {"collective"}, execution_shape=ExecutionShape.DISTRIBUTED_NATIVE,
                    required_worker_count=2)

    def handler(request: httpx.Request) -> httpx.Response:
        if "worker-b" in str(request.url):
            raise httpx.ConnectError("worker vanished", request=request)
        return httpx.Response(200, json=WorkloadExecutionResult(
            workload_id="collective", worker_id="worker-a", success=True,
            output_asset_hashes=set(), execution_duration_seconds=0.5,
        ).model_dump(mode="json"))

    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await orchestrator._process_workload("collective")
    attempt = attempts.get_latest_attempt("collective")
    assert attempt.status == AttemptStatus.LOST
    assert attempt.execution_group.state == "lost"
    assert workload_registry.get_workload("collective").state != WorkloadState.COMPLETED


@pytest.mark.asyncio
async def test_heartbeat_loss_cannot_be_overwritten_by_late_group_success_responses():
    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    attempts = AttemptRegistry()
    jobs = JobRegistry(workload_registry)
    orchestrator = WorkloadOrchestrator(registry, workload_registry,
                                        job_registry=jobs, attempt_registry=attempts,
                                        max_attempts=1)
    for name in ("worker-a", "worker-b"):
        registry.register_worker(_worker(name, _profile(name)))
    workload_registry.add_workload(_spec("collective", job_id="job-late-success"))
    jobs.create_job("job-late-success", {"collective"},
                    execution_shape=ExecutionShape.DISTRIBUTED_NATIVE, required_worker_count=2)

    def handler(request: httpx.Request) -> httpx.Response:
        orchestrator.handle_worker_lost("worker-b")
        context = __import__("json").loads(request.content)["execution_context"]
        return httpx.Response(200, json=WorkloadExecutionResult(
            workload_id="collective", worker_id=context["worker_id"], success=True,
            output_asset_hashes=set(), execution_duration_seconds=0.5,
        ).model_dump(mode="json"))

    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await orchestrator._process_workload("collective")
    attempt = attempts.get_latest_attempt("collective")
    assert attempt.status == AttemptStatus.LOST
    assert attempt.execution_group.state == "lost"
    assert workload_registry.get_workload("collective").state == WorkloadState.FAILED


def test_execution_metadata_does_not_persist_runtime_endpoints_or_secrets(tmp_path):
    store = CoordinatorStateStore(tmp_path / "secret-check.db")
    decision = PlacementDecision(workload_id="wl", selected_worker_id="w", placement_score=1,
                                 score_breakdown={}, missing_assets_on_worker=set(), execution_tier="lan")
    group = ExecutionGroup(execution_id="exec", worker_ids=["w", "x"], required_worker_count=2,
                           placement_decisions=[decision, decision.model_copy(update={"selected_worker_id": "x"})])
    attempts = AttemptRegistry(store)
    attempts.create_attempt("wl", execution_group=group)
    spec = _spec("wl", parameters={
        "tile_count": 6,
        "nested": {"password": "do-not-store-this"},
        "api_token": "also-do-not-store-this",
        "command": "tool --password=inline-secret",
    })
    store.save_workload(WorkloadRecord(spec))
    store.close()
    raw = (tmp_path / "secret-check.db").read_bytes()
    assert b"do-not-store-this" not in raw
    assert b"also-do-not-store-this" not in raw
    assert b"inline-secret" not in raw
    assert b"tile_count" in raw
    assert b"6" in raw
    assert b"endpoint_url" not in raw


@pytest.mark.asyncio
async def test_worker_metadata_redacts_secret_parameters_but_keeps_safe_metadata(tmp_path):
    class MetadataInspectingRuntime(RuntimeAdapter):
        async def execute(self, spec, workdir):
            metadata = json.loads((tmp_path / "workloads" / spec.workload_id / "metadata.json").read_text())
            assert metadata["parameters"]["tile_count"] == 6
            assert metadata["parameters"]["password"] == "[REDACTED]"
            assert "inline-secret" not in metadata["parameters"]["command"]
            assert "live-secret" not in json.dumps(metadata)
            return True, None, None

        async def checkpoint(self):
            return None

        async def cancel(self):
            pass

    spec = _spec("metadata-check", parameters={
        "tile_count": 6,
        "password": "live-secret",
        "command": "tool --password=inline-secret",
    })
    manager = ExecutionManager(LocalCASAdapter(tmp_path / "cas"), str(tmp_path / "workloads"))
    result = await manager.execute_workload(spec, "worker", MetadataInspectingRuntime())
    assert result.success
    # Redaction is for stored metadata only; the current execution input is intact.
    assert spec.parameters["password"] == "live-secret"


@pytest.mark.asyncio
async def test_direct_workload_submission_rejects_dependency_edges():
    service = CoordinatorService(credential_store=CredentialStore(insecure_mode=True))
    async with httpx.AsyncClient(
        transport=ASGITransport(app=service.app), base_url="http://coordinator",
    ) as client:
        response = await client.post(
            "/api/v1/workloads/submit",
            json=_spec("orphan", depends_on=["missing"]).model_dump(mode="json"),
        )
    assert response.status_code == 400
    assert service.workload_registry.get_workload("orphan") is None


@pytest.mark.asyncio
async def test_group_drain_from_any_member_marks_attempt_lost_and_checkpoints_all_members():
    workers = WorkerRegistry()
    workloads = WorkloadRegistry()
    attempts = AttemptRegistry()
    orchestrator = WorkloadOrchestrator(workers, workloads, attempt_registry=attempts)
    for name in ("worker-a", "worker-b"):
        workers.register_worker(_worker(name, _profile(name)))
    workloads.add_workload(_spec("collective"))
    group = ExecutionGroup(execution_id="exec-drain", worker_ids=["worker-a", "worker-b"],
                           required_worker_count=2)
    attempt = attempts.create_attempt("collective", worker_id="worker-a", execution_group=group)
    attempts.mark_assigned(attempt.attempt_id, "worker-a")
    attempts.mark_running(attempt.attempt_id)
    checkpoint_urls = []

    def handler(request: httpx.Request) -> httpx.Response:
        checkpoint_urls.append(str(request.url))
        return httpx.Response(200, json={"status": "checkpointing"})

    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await orchestrator.drain_worker("worker-b")

    restored_attempt = attempts.get_attempt(attempt.attempt_id)
    assert restored_attempt.status == AttemptStatus.LOST
    assert restored_attempt.execution_group.state == "lost"
    assert restored_attempt.execution_group.unavailable_worker_ids == {"worker-b"}
    assert {url.split("/")[2] for url in checkpoint_urls} == {"worker-a", "worker-b"}
    assert all(url.endswith("/api/v1/workloads/collective/checkpoint") for url in checkpoint_urls)


@pytest.mark.asyncio
async def test_group_retry_excludes_member_reporting_worker_unavailable():
    registry = WorkerRegistry()
    workload_registry = WorkloadRegistry()
    attempts = AttemptRegistry()
    jobs = JobRegistry(workload_registry)
    orchestrator = WorkloadOrchestrator(registry, workload_registry,
                                        job_registry=jobs, attempt_registry=attempts,
                                        max_attempts=2)
    for name in ("worker-a", "worker-b", "worker-c"):
        registry.register_worker(_worker(name, _profile(name)))
    workload_registry.add_workload(_spec("collective", job_id="job-member-unavailable"))
    jobs.create_job("job-member-unavailable", {"collective"},
                    execution_shape=ExecutionShape.DISTRIBUTED_NATIVE, required_worker_count=2)
    first_unavailable = None
    first_group_workers = set()
    second_group_workers = set()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal first_unavailable
        worker_id = request.url.host
        if first_unavailable is None:
            first_unavailable = worker_id
        if len(attempts.list_attempts_for_workload("collective")) <= 1:
            first_group_workers.add(worker_id)
            unavailable = worker_id == first_unavailable
        else:
            second_group_workers.add(worker_id)
            unavailable = False
        context = json.loads(request.content)["execution_context"]
        result = WorkloadExecutionResult(
            workload_id="collective", worker_id=context["worker_id"], success=not unavailable,
            output_asset_hashes=set(), execution_duration_seconds=0.1,
            failure_category=FailureCategory.WORKER_UNAVAILABLE if unavailable else None,
        )
        return httpx.Response(200, json=result.model_dump(mode="json"))

    orchestrator.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await orchestrator._process_workload("collective")

    assert len(first_group_workers) == 2
    assert first_unavailable in first_group_workers
    assert len(second_group_workers) == 2
    assert first_unavailable not in second_group_workers
    assert len(attempts.list_attempts_for_workload("collective")) == 2


def test_legacy_one_workload_job_loads_as_single_machine(tmp_path):
    store = CoordinatorStateStore(tmp_path / "legacy-shape.db")
    jobs = JobRegistry(WorkloadRegistry(), state_store=store)
    jobs.create_job("legacy-one", {"one"}, execution_shape=ExecutionShape.TASK_SPLIT)
    restored = store.load_jobs()[0]
    assert restored.workload_ids == {"one"}
    assert restored.execution_shape == ExecutionShape.SINGLE_MACHINE
    store.close()
