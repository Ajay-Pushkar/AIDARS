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
