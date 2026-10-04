import pytest
from fastapi.testclient import TestClient
from aidars.distributed.models import WorkloadSpec
import time
import asyncio
from typing import AsyncGenerator

from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.cas_adapter import LocalCASAdapter
import tempfile

@pytest.fixture
def coordinator_app():
    cas = LocalCASAdapter(tempfile.mkdtemp())
    coord = CoordinatorService(cas)
    return coord.app

@pytest.fixture
def api_client(coordinator_app):
    return TestClient(coordinator_app)

@pytest.mark.asyncio
async def test_job_pagination_and_cancellation(api_client):
    headers = {"Authorization": "Bearer admin_token"}

    # 1. Submit a job with multiple workloads
    specs = [
        WorkloadSpec(workload_id=f"wl-{i}", task_type="test", priority=50).model_dump(mode="json")
        for i in range(5)
    ]
    req = {
        "specs": specs
    }
    resp = api_client.post("/api/v1/jobs/submit", json=req, headers=headers)
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]

    # 2. Test pagination of workloads
    resp_wl = api_client.get(f"/api/v1/jobs/{job_id}/workloads?offset=0&limit=3", headers=headers)
    assert resp_wl.status_code == 200
    data_wl = resp_wl.json()
    assert data_wl["total"] == 5
    assert len(data_wl["items"]) == 3
    assert data_wl["has_more"] is True

    # 3. Test pagination of artifacts (currently empty)
    resp_art = api_client.get(f"/api/v1/jobs/{job_id}/artifacts?offset=0&limit=10", headers=headers)
    assert resp_art.status_code == 200
    data_art = resp_art.json()
    assert data_art["total"] == 0
    assert len(data_art["items"]) == 0

    # 4. Cancel the job
    resp_cancel = api_client.post(f"/api/v1/jobs/{job_id}/cancel", headers=headers)
    assert resp_cancel.status_code == 200
    
    # 5. Verify states are updated to cancelled
    resp_wl2 = api_client.get(f"/api/v1/jobs/{job_id}/workloads?offset=0&limit=5", headers=headers)
    data_wl2 = resp_wl2.json()
    for item in data_wl2["items"]:
        assert item["state"] == "cancelled"

    # 6. Verify job status reflects cancelled (handled via WorkloadState.CANCELLED mapping to FAILED/PENDING usually, depending on aggregate)
    # The aggregate state logic treats CANCELLED as failed.
    resp_job = api_client.get(f"/api/v1/jobs/{job_id}", headers=headers)
    assert resp_job.status_code == 200
    assert resp_job.json()["failed"] == 5

@pytest.mark.asyncio
async def test_get_attempt(api_client):
    headers = {"Authorization": "Bearer admin_token"}
    
    # Attempt lookup needs an actual attempt. We will just verify 404 for missing.
    resp = api_client.get("/api/v1/attempts/nonexistent_attempt", headers=headers)
    assert resp.status_code == 404
