"""M8: /api/v1/jobs/submit and /api/v1/jobs/{job_id} REST endpoints,
mirroring the existing /workloads endpoints' TestClient pattern from
tests/unit/test_coordinator.py.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import WorkerRegistrationPayload


@pytest.fixture
def coordinator_service() -> CoordinatorService:
    return CoordinatorService(coordinator_id="test-coord-m8", eviction_interval_seconds=1.0)


@pytest.fixture
def client(coordinator_service: CoordinatorService) -> TestClient:
    return TestClient(coordinator_service.app)


def test_submit_job_endpoint_returns_job_id(client: TestClient):
    resp = client.post("/api/v1/jobs/submit", json={
        "specs": [
            {"workload_id": "c0", "task_type": "test", "min_ram_bytes": 1024},
            {"workload_id": "c1", "task_type": "test", "min_ram_bytes": 1024},
        ],
    })
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "submitted"
    assert data["job_id"]


def test_get_job_status_endpoint_reports_aggregate(client: TestClient, coordinator_service: CoordinatorService):
    resp = client.post("/api/v1/jobs/submit", json={
        "specs": [{"workload_id": "c0", "task_type": "test", "min_ram_bytes": 1024}],
    })
    job_id = resp.json()["job_id"]

    status_resp = client.get(f"/api/v1/jobs/{job_id}")
    assert status_resp.status_code == 200
    data = status_resp.json()
    assert data["job_id"] == job_id
    assert data["workload_ids"] == ["c0"]
    assert data["completion_policy"] == "all_required"
    assert data["total"] == 1


def test_get_unknown_job_returns_404(client: TestClient):
    resp = client.get("/api/v1/jobs/does-not-exist")
    assert resp.status_code == 404


def test_submit_job_endpoint_accepts_completion_policy_and_threshold(client: TestClient):
    resp = client.post("/api/v1/jobs/submit", json={
        "specs": [{"workload_id": "c0", "task_type": "test", "min_ram_bytes": 1024}],
        "completion_policy": "threshold",
        "threshold": 1,
    })
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    status_resp = client.get(f"/api/v1/jobs/{job_id}")
    data = status_resp.json()
    assert data["completion_policy"] == "threshold"
    assert data["threshold"] == 1


# ============================================================================
# Correction pass: /jobs/submit must convert expected client-input
# ValueErrors into proper HTTP 4xx responses, not an unhandled 500.
# ============================================================================


def test_submit_job_empty_specs_returns_400_not_500(client: TestClient):
    resp = client.post("/api/v1/jobs/submit", json={"specs": []})
    assert resp.status_code == 400
    assert "at least one" in resp.json()["detail"].lower()


def test_submit_job_threshold_policy_without_threshold_returns_400_not_500(client: TestClient):
    resp = client.post("/api/v1/jobs/submit", json={
        "specs": [{"workload_id": "c0", "task_type": "test", "min_ram_bytes": 1024}],
        "completion_policy": "threshold",
        # threshold intentionally omitted
    })
    assert resp.status_code == 400
    assert "threshold" in resp.json()["detail"].lower()


def test_submit_job_conflicting_job_ids_across_specs_returns_400_not_500(client: TestClient):
    resp = client.post("/api/v1/jobs/submit", json={
        "specs": [
            {"workload_id": "x", "job_id": "job-x", "task_type": "test", "min_ram_bytes": 1024},
            {"workload_id": "y", "job_id": "job-y", "task_type": "test", "min_ram_bytes": 1024},
        ],
    })
    assert resp.status_code == 400
    assert "job_id" in resp.json()["detail"]


def test_submit_job_invalid_completion_policy_value_returns_422(client: TestClient):
    """Pydantic-level validation (unknown enum value) -- FastAPI's own
    422, distinct from the application-level 400s above; documents the
    existing boundary rather than something this pass changes."""
    resp = client.post("/api/v1/jobs/submit", json={
        "specs": [{"workload_id": "c0", "task_type": "test", "min_ram_bytes": 1024}],
        "completion_policy": "not_a_real_policy",
    })
    assert resp.status_code == 422


def test_submit_job_workload_id_conflict_returns_409(client: TestClient):
    """The exact audit-reproduced case, at the /jobs/submit boundary."""
    first = client.post("/api/v1/jobs/submit", json={
        "specs": [{"workload_id": "dup", "job_id": "job-A", "task_type": "test", "min_ram_bytes": 1024}],
    })
    assert first.status_code == 202

    second = client.post("/api/v1/jobs/submit", json={
        "specs": [{"workload_id": "dup", "job_id": "job-B", "task_type": "test", "min_ram_bytes": 2048}],
    })
    assert second.status_code == 409
    assert "dup" in second.json()["detail"]

    # The original submission (job-A) must remain exactly as it was --
    # never silently replaced.
    status_resp = client.get("/api/v1/jobs/job-A")
    assert status_resp.status_code == 200
    assert status_resp.json()["workload_ids"] == ["dup"]
