"""M10.17/M10.19: M9 security regression coverage for the new M10 API
surface (attempt/retry/timing exposure on GET /workloads/{id}).

Confirms: M9 auth still gates every route (including the newly-added
`attempts` field on the workload-status response), worker credentials
are never exposed through it, and unauthorized access is still denied
-- same posture as test_m9_adversarial.py, scoped to what M10 actually
added rather than re-testing everything M9 already covers.
"""
from __future__ import annotations

import httpx
import pytest
from httpx import ASGITransport

from aidars.distributed.auth import CredentialStore
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import WorkloadExecutionResult, WorkloadSpec
from aidars.distributed.workload_registry import WorkloadState

ADMIN_TOKEN = "m10-sec-admin"
BOOTSTRAP_SECRET = "m10-sec-bootstrap"


@pytest.fixture
def secure_service() -> CoordinatorService:
    return CoordinatorService(credential_store=CredentialStore(
        admin_tokens={ADMIN_TOKEN}, bootstrap_secret=BOOTSTRAP_SECRET, insecure_mode=False,
    ))


@pytest.mark.asyncio
async def test_workload_status_with_attempts_requires_admin_auth(secure_service: CoordinatorService):
    transport = ASGITransport(app=secure_service.app)
    client = httpx.AsyncClient(transport=transport, base_url="http://coordinator")

    spec = WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024)
    secure_service.workload_registry.add_workload(spec)

    # No credential at all -> 401, same as every other admin-gated route.
    resp = await client.get("/api/v1/workloads/task-1")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_workload_status_with_attempts_succeeds_with_admin_token(secure_service: CoordinatorService):
    transport = ASGITransport(app=secure_service.app)
    client = httpx.AsyncClient(transport=transport, base_url="http://coordinator")

    spec = WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024)
    secure_service.workload_registry.add_workload(spec)
    attempt = secure_service.attempt_registry.create_attempt("task-1")
    secure_service.attempt_registry.mark_assigned(attempt.attempt_id, "w-1")

    resp = await client.get(
        "/api/v1/workloads/task-1", headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "attempts" in body
    assert body["attempt_count"] == 1
    assert body["attempts"][0]["worker_id"] == "w-1"


@pytest.mark.asyncio
async def test_worker_credential_never_appears_in_workload_status_response(secure_service: CoordinatorService):
    """The attempt summary carries worker_id only -- never the M9
    per-worker bearer credential issued at registration."""
    transport = ASGITransport(app=secure_service.app)
    client = httpx.AsyncClient(transport=transport, base_url="http://coordinator")

    from aidars.distributed.models import WorkerRegistrationPayload
    reg_resp = secure_service.register_worker_sync(WorkerRegistrationPayload(
        worker_id="w-1", endpoint_url="http://127.0.0.1:8080", ip_address="127.0.0.1", port=8080,
    ))
    issued_credential = reg_resp.worker_credential
    assert issued_credential

    spec = WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024)
    secure_service.workload_registry.add_workload(spec)
    attempt = secure_service.attempt_registry.create_attempt("task-1")
    secure_service.attempt_registry.mark_assigned(attempt.attempt_id, "w-1")
    secure_service.attempt_registry.mark_running(attempt.attempt_id)
    secure_service.attempt_registry.mark_succeeded(attempt.attempt_id, WorkloadExecutionResult(
        workload_id="task-1", worker_id="w-1", success=True,
        output_asset_hashes={"a" * 64}, execution_duration_seconds=1.0,
    ))

    resp = await client.get(
        "/api/v1/workloads/task-1", headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
    )
    assert resp.status_code == 200
    assert issued_credential not in resp.text


@pytest.mark.asyncio
async def test_worker_bearer_token_cannot_read_workload_attempts(secure_service: CoordinatorService):
    """GET /workloads/{id} is admin-only -- a worker's own credential
    (valid for worker-scoped routes) must not authorize it, matching
    the existing M9 authorization matrix (unchanged by M10)."""
    from aidars.distributed.models import WorkerRegistrationPayload
    reg_resp = secure_service.register_worker_sync(WorkerRegistrationPayload(
        worker_id="w-1", endpoint_url="http://127.0.0.1:8080", ip_address="127.0.0.1", port=8080,
    ))

    transport = ASGITransport(app=secure_service.app)
    client = httpx.AsyncClient(transport=transport, base_url="http://coordinator")
    spec = WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024)
    secure_service.workload_registry.add_workload(spec)

    resp = await client.get(
        "/api/v1/workloads/task-1",
        headers={"Authorization": f"Bearer {reg_resp.worker_credential}"},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_bootstrap_secret_cannot_read_workload_attempts(secure_service: CoordinatorService):
    """The bootstrap/join secret authorizes ONLY registration -- it must
    never authorize the new attempt-exposing route either."""
    transport = ASGITransport(app=secure_service.app)
    client = httpx.AsyncClient(transport=transport, base_url="http://coordinator")
    spec = WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024)
    secure_service.workload_registry.add_workload(spec)

    resp = await client.get(
        "/api/v1/workloads/task-1", headers={"Authorization": f"Bearer {BOOTSTRAP_SECRET}"},
    )
    assert resp.status_code == 401


def test_coordinator_state_store_lock_error_message_never_contains_secrets(tmp_path):
    """CoordinatorAlreadyRunningError's message references only the file
    path -- never any credential."""
    from aidars.distributed.state_store import CoordinatorAlreadyRunningError, CoordinatorStateStore

    db_path = tmp_path / "state.db"
    store1 = CoordinatorStateStore(db_path)
    try:
        with pytest.raises(CoordinatorAlreadyRunningError) as exc_info:
            CoordinatorStateStore(db_path)
        message = str(exc_info.value)
        assert "secret" not in message.lower()
        assert "token" not in message.lower()
        assert "credential" not in message.lower() or "credential material" not in message.lower()
    finally:
        store1.close()


@pytest.mark.asyncio
async def test_m9_insecure_mode_still_bypasses_the_new_attempts_field_consistently(monkeypatch):
    """Regression: AIDAR_INSECURE_MODE's bypass behavior (established in
    M9) must apply uniformly -- it shouldn't accidentally leave the new
    route more OR less protected than the rest of the API."""
    service = CoordinatorService(credential_store=CredentialStore(insecure_mode=True))
    transport = ASGITransport(app=service.app)
    client = httpx.AsyncClient(transport=transport, base_url="http://coordinator")

    spec = WorkloadSpec(workload_id="task-1", task_type="test", min_ram_bytes=1024)
    service.workload_registry.add_workload(spec)

    resp = await client.get("/api/v1/workloads/task-1")  # no credential at all
    assert resp.status_code == 200
    assert "attempts" in resp.json()
