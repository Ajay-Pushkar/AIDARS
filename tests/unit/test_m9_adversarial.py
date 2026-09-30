"""M9: security and adversarial test suite.

Follows the existing test_m1..m7_adversarial* naming convention. Every
test in this file constructs a CoordinatorService with an EXPLICIT
CredentialStore(insecure_mode=False) -- overriding the session-wide
AIDAR_INSECURE_MODE default set in tests/conftest.py -- so these tests
always exercise the real, secure, fail-closed authentication path
regardless of the test session's default posture.

Covers the PRD's eight required adversarial scenarios (invalid token,
expired/invalid credential, unauthorized artifact retrieval, unauthorized
workload inspection, malformed recovery state, replayed request,
duplicate submission, worker impersonation attempt) plus the sixteen
additional scenarios from the M9 implementation brief. Duplicate
submission's full behavior matrix already lives in
test_workload_id_collision.py; here it is only re-verified through the
new authenticated boundary, not re-tested exhaustively.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from aidars.distributed.artifact import Artifact, ArtifactLifecycleState, ArtifactVerificationState
from aidars.distributed.auth import CredentialStore, is_insecure_mode_enabled
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.job_registry import CompletionPolicy, JobRecord
from aidars.distributed.state_store import CoordinatorStateStore
from aidars.distributed.workload_registry import WorkloadRecord, WorkloadState
from aidars.distributed.models import WorkloadSpec

ADMIN_TOKEN = "test-admin-token"
BOOTSTRAP_SECRET = "test-bootstrap-secret"


def _secure_store() -> CredentialStore:
    return CredentialStore(
        admin_tokens={ADMIN_TOKEN},
        bootstrap_secret=BOOTSTRAP_SECRET,
        insecure_mode=False,
    )


@pytest.fixture
def secure_service() -> CoordinatorService:
    return CoordinatorService(credential_store=_secure_store())


@pytest.fixture
def client(secure_service: CoordinatorService) -> TestClient:
    return TestClient(secure_service.app, raise_server_exceptions=False)


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _register_worker(client: TestClient, worker_id: str = "w-1") -> str:
    resp = client.post(
        "/api/v1/workers/register",
        json={"worker_id": worker_id, "endpoint_url": f"http://{worker_id}", "ip_address": "127.0.0.1", "port": 8001},
        headers=_auth(BOOTSTRAP_SECRET),
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["worker_credential"]


# ============================================================================
# 1. invalid token
# ============================================================================


def test_invalid_token_rejected(client: TestClient):
    resp = client.get("/api/v1/workers", headers=_auth("not-a-real-token"))
    assert resp.status_code == 401


# ============================================================================
# 2. expired/invalid credential (a superseded worker credential)
# ============================================================================


def test_superseded_worker_credential_becomes_invalid(client: TestClient):
    old_credential = _register_worker(client, "w-1")
    # Worker reconnects/re-registers (e.g. after its own restart) and is
    # issued a NEW credential, replacing the old one.
    new_credential = _register_worker(client, "w-1")
    assert old_credential != new_credential

    stale = client.post("/api/v1/workers/w-1/heartbeat", json={"worker_id": "w-1"}, headers=_auth(old_credential))
    assert stale.status_code == 401

    fresh = client.post("/api/v1/workers/w-1/heartbeat", json={"worker_id": "w-1"}, headers=_auth(new_credential))
    assert fresh.status_code == 200


# ============================================================================
# 3. unauthorized artifact retrieval
# ============================================================================


def test_unauthorized_artifact_retrieval_rejected(client: TestClient, secure_service: CoordinatorService):
    secure_service.artifact_registry.record_artifacts("w-1", "job-1", {"a" * 64})
    [artifact] = secure_service.artifact_registry.list_artifacts()

    no_auth = client.get(f"/api/v1/artifacts/{artifact.artifact_id}")
    assert no_auth.status_code == 401

    worker_credential = _register_worker(client)
    worker_auth = client.get(f"/api/v1/artifacts/{artifact.artifact_id}", headers=_auth(worker_credential))
    assert worker_auth.status_code == 401  # artifacts are admin/client only

    admin_auth = client.get(f"/api/v1/artifacts/{artifact.artifact_id}", headers=_auth(ADMIN_TOKEN))
    assert admin_auth.status_code == 200
    assert admin_auth.json()["content_hash"] == "a" * 64


def test_unknown_artifact_id_is_404_not_500(client: TestClient):
    resp = client.get("/api/v1/artifacts/does-not-exist", headers=_auth(ADMIN_TOKEN))
    assert resp.status_code == 404


# ============================================================================
# 4. unauthorized workload inspection
# ============================================================================


def test_unauthorized_workload_inspection_rejected(client: TestClient):
    submit = client.post(
        "/api/v1/workloads/submit",
        json={"workload_id": "task-1", "task_type": "test", "min_ram_bytes": 1024},
        headers=_auth(ADMIN_TOKEN),
    )
    assert submit.status_code == 202

    no_auth = client.get("/api/v1/workloads/task-1")
    assert no_auth.status_code == 401

    worker_credential = _register_worker(client)
    worker_auth = client.get("/api/v1/workloads/task-1", headers=_auth(worker_credential))
    assert worker_auth.status_code == 401

    admin_auth = client.get("/api/v1/workloads/task-1", headers=_auth(ADMIN_TOKEN))
    assert admin_auth.status_code == 200


# ============================================================================
# 5 & 23. malformed recovery state / malformed persisted state fails explicitly
# ============================================================================


def test_malformed_persisted_worker_row_is_skipped_and_logged(tmp_path: Path, caplog):
    store = CoordinatorStateStore(tmp_path / "state.db")
    conn = sqlite3.connect(str(tmp_path / "state.db"))
    conn.execute(
        "INSERT INTO workers (worker_id, info_json, last_heartbeat_utc, updated_at) VALUES (?, ?, ?, ?)",
        ("corrupt-worker", "{not valid json", 0.0, 0.0),
    )
    conn.commit()
    conn.close()

    with caplog.at_level(logging.ERROR):
        loaded = store.load_workers()

    assert loaded == []  # malformed row excluded, not crashed
    assert any("corrupt-worker" in rec.message for rec in caplog.records)


def test_malformed_persisted_state_does_not_prevent_other_rows_loading(tmp_path: Path):
    store = CoordinatorStateStore(tmp_path / "state.db")
    from aidars.distributed.models import WorkerInfo, WorkerStatus
    store.save_worker(WorkerInfo(
        worker_id="good", endpoint_url="http://good", ip_address="127.0.0.1", port=8001,
        capacity_bytes=100, used_bytes=0,
    ))
    conn = sqlite3.connect(str(tmp_path / "state.db"))
    conn.execute(
        "INSERT INTO jobs (job_id, workload_ids_json, completion_policy, threshold, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        ("corrupt-job", "NOT A JSON ARRAY", "bogus_policy_value", None, 0.0, 0.0),
    )
    conn.commit()
    conn.close()

    assert len(store.load_workers()) == 1
    assert store.load_jobs() == []  # corrupted, excluded -- does not raise


def test_coordinator_startup_survives_malformed_persisted_row(tmp_path: Path):
    """Corrupted state must not crash coordinator startup -- a single bad
    row would otherwise be a denial-of-service vector."""
    store = CoordinatorStateStore(tmp_path / "state.db")
    conn = sqlite3.connect(str(tmp_path / "state.db"))
    conn.execute(
        "INSERT INTO artifacts (artifact_id, content_hash, producer_workload_id, producer_job_id, "
        "created_at, verification_state, lifecycle_state, storage_location, size_bytes, updated_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("bad", "a" * 64, "w1", None, 0.0, "not_a_real_state", "available", "cas", 0.0, 0.0),
    )
    conn.commit()
    conn.close()

    service = CoordinatorService(state_store=store, credential_store=_secure_store())
    pending = service._restore_persisted_state()  # must not raise
    assert pending == []
    assert service.artifact_registry.list_artifacts() == []


# ============================================================================
# 6. replayed request
# ============================================================================


def test_replayed_request_with_same_nonce_rejected(client: TestClient):
    body = {"workload_id": "task-replay", "task_type": "test", "min_ram_bytes": 1024}
    headers = {**_auth(ADMIN_TOKEN), "X-Request-Nonce": "nonce-123"}

    first = client.post("/api/v1/workloads/submit", json=body, headers=headers)
    assert first.status_code == 202

    replayed_body = {"workload_id": "task-replay-2", "task_type": "test", "min_ram_bytes": 1024}
    second = client.post("/api/v1/workloads/submit", json=replayed_body, headers=headers)
    assert second.status_code == 409


def test_fresh_nonce_each_request_is_not_a_replay(client: TestClient):
    for i in range(3):
        headers = {**_auth(ADMIN_TOKEN), "X-Request-Nonce": f"unique-{i}"}
        resp = client.post(
            "/api/v1/workloads/submit",
            json={"workload_id": f"task-{i}", "task_type": "test", "min_ram_bytes": 1024},
            headers=headers,
        )
        assert resp.status_code == 202


def test_no_nonce_header_is_not_treated_as_a_replay(client: TestClient):
    """Additive design: omitting the optional nonce header doesn't itself
    fail the request -- ordinary auth still applies."""
    resp = client.post(
        "/api/v1/workloads/submit",
        json={"workload_id": "task-no-nonce", "task_type": "test", "min_ram_bytes": 1024},
        headers=_auth(ADMIN_TOKEN),
    )
    assert resp.status_code == 202


# ============================================================================
# 7. duplicate submission (re-verified through the authenticated boundary;
# full behavior matrix already in test_workload_id_collision.py)
# ============================================================================


def test_duplicate_submission_conflict_still_enforced_when_authenticated(client: TestClient):
    first = client.post(
        "/api/v1/workloads/submit",
        json={"workload_id": "dup", "job_id": "job-A", "task_type": "test", "min_ram_bytes": 1024},
        headers=_auth(ADMIN_TOKEN),
    )
    assert first.status_code == 202

    conflicting = client.post(
        "/api/v1/workloads/submit",
        json={"workload_id": "dup", "job_id": "job-B", "task_type": "test", "min_ram_bytes": 2048},
        headers=_auth(ADMIN_TOKEN),
    )
    assert conflicting.status_code == 409

    identical_replay = client.post(
        "/api/v1/workloads/submit",
        json={"workload_id": "dup", "job_id": "job-A", "task_type": "test", "min_ram_bytes": 1024},
        headers=_auth(ADMIN_TOKEN),
    )
    assert identical_replay.status_code == 202  # idempotent, unchanged M8 behavior


# ============================================================================
# 8. worker impersonation attempt
# ============================================================================


def test_worker_impersonation_rejected(client: TestClient):
    credential_a = _register_worker(client, "worker-a")
    _register_worker(client, "worker-b")

    # worker-a's credential must not authenticate AS worker-b.
    impersonation = client.post(
        "/api/v1/workers/worker-b/heartbeat", json={"worker_id": "worker-b"}, headers=_auth(credential_a),
    )
    assert impersonation.status_code == 401

    legit = client.post(
        "/api/v1/workers/worker-a/heartbeat", json={"worker_id": "worker-a"}, headers=_auth(credential_a),
    )
    assert legit.status_code == 200


def test_worker_id_in_path_alone_is_never_sufficient_identity(client: TestClient):
    """A worker_id that was never registered at all -- no credential could
    possibly exist for it -- must still be rejected, not treated as
    'unknown worker, allow by default'."""
    resp = client.post(
        "/api/v1/workers/never-registered/heartbeat",
        json={"worker_id": "never-registered"},
        headers=_auth("some-random-token"),
    )
    assert resp.status_code == 401


# ============================================================================
# 9. bootstrap secret cannot perform admin operations
# ============================================================================


@pytest.mark.parametrize("method,path,body", [
    ("get", "/api/v1/workers", None),
    ("get", "/api/v1/cluster/stats", None),
    ("post", "/api/v1/jobs/submit", {"specs": [{"workload_id": "x", "task_type": "t"}]}),
    ("post", "/api/v1/workloads/submit", {"workload_id": "x", "task_type": "t"}),
])
def test_bootstrap_secret_cannot_perform_admin_operations(client: TestClient, method, path, body):
    call = getattr(client, method)
    resp = call(path, json=body, headers=_auth(BOOTSTRAP_SECRET)) if body is not None else call(path, headers=_auth(BOOTSTRAP_SECRET))
    assert resp.status_code == 401


def test_bootstrap_secret_only_authorizes_registration(client: TestClient):
    resp = client.post(
        "/api/v1/workers/register",
        json={"worker_id": "w-x", "endpoint_url": "http://w-x", "ip_address": "127.0.0.1", "port": 8001},
        headers=_auth(BOOTSTRAP_SECRET),
    )
    assert resp.status_code == 200


# ============================================================================
# 10. worker cannot access another worker's heartbeat (see also #8)
# 11. worker cannot inspect worker registry
# 12. worker cannot submit jobs
# 13. worker cannot submit workloads
# 14. worker cannot access cluster stats
# ============================================================================


def test_worker_cannot_inspect_worker_registry(client: TestClient):
    credential = _register_worker(client)
    assert client.get("/api/v1/workers", headers=_auth(credential)).status_code == 401
    assert client.get("/api/v1/workers/w-1", headers=_auth(credential)).status_code == 401


def test_worker_cannot_submit_jobs(client: TestClient):
    credential = _register_worker(client)
    resp = client.post(
        "/api/v1/jobs/submit",
        json={"specs": [{"workload_id": "x", "task_type": "t"}]},
        headers=_auth(credential),
    )
    assert resp.status_code == 401


def test_worker_cannot_submit_workloads(client: TestClient):
    credential = _register_worker(client)
    resp = client.post(
        "/api/v1/workloads/submit",
        json={"workload_id": "x", "task_type": "t"},
        headers=_auth(credential),
    )
    assert resp.status_code == 401


def test_worker_cannot_access_cluster_stats(client: TestClient):
    credential = _register_worker(client)
    resp = client.get("/api/v1/cluster/stats", headers=_auth(credential))
    assert resp.status_code == 401


# ============================================================================
# 15. admin/client can perform authorized operations
# ============================================================================


def test_admin_can_perform_all_authorized_operations(client: TestClient):
    assert client.get("/api/v1/workers", headers=_auth(ADMIN_TOKEN)).status_code == 200
    assert client.get("/api/v1/cluster/stats", headers=_auth(ADMIN_TOKEN)).status_code == 200
    assert client.post(
        "/api/v1/workloads/submit",
        json={"workload_id": "admin-task", "task_type": "t", "min_ram_bytes": 1024},
        headers=_auth(ADMIN_TOKEN),
    ).status_code == 202
    assert client.get("/api/v1/workloads/admin-task", headers=_auth(ADMIN_TOKEN)).status_code == 200
    job_resp = client.post(
        "/api/v1/jobs/submit",
        json={"specs": [{"workload_id": "admin-job-task", "task_type": "t", "min_ram_bytes": 1024}]},
        headers=_auth(ADMIN_TOKEN),
    )
    assert job_resp.status_code == 202
    job_id = job_resp.json()["job_id"]
    assert client.get(f"/api/v1/jobs/{job_id}", headers=_auth(ADMIN_TOKEN)).status_code == 200


# ============================================================================
# 16. malformed Authorization header
# 17. missing Authorization header
# ============================================================================


@pytest.mark.parametrize("header_value", [
    "NotBearer sometoken",
    "Bearer",
    "Bearer ",
    "sometoken",
    "Bearer  ",
])
def test_malformed_authorization_header_rejected(client: TestClient, header_value):
    resp = client.get("/api/v1/workers", headers={"Authorization": header_value})
    assert resp.status_code == 401


def test_missing_authorization_header_rejected(client: TestClient):
    resp = client.get("/api/v1/workers")
    assert resp.status_code == 401


# ============================================================================
# 18. oversized metadata
# ============================================================================


def test_oversized_workload_parameters_rejected(client: TestClient):
    resp = client.post(
        "/api/v1/workloads/submit",
        json={
            "workload_id": "oversized",
            "task_type": "t",
            "min_ram_bytes": 1024,
            "parameters": {"blob": "x" * 100_000},
        },
        headers=_auth(ADMIN_TOKEN),
    )
    assert resp.status_code == 422


def test_parameters_just_under_limit_accepted(client: TestClient):
    from aidars.distributed.models import MAX_WORKLOAD_PARAMETERS_BYTES
    # Leave headroom for JSON structure overhead (quotes, braces, key name).
    payload_size = MAX_WORKLOAD_PARAMETERS_BYTES - 100
    resp = client.post(
        "/api/v1/workloads/submit",
        json={
            "workload_id": "just-under",
            "task_type": "t",
            "min_ram_bytes": 1024,
            "parameters": {"blob": "x" * payload_size},
        },
        headers=_auth(ADMIN_TOKEN),
    )
    assert resp.status_code == 202


def test_oversized_job_specs_count_rejected(client: TestClient):
    from aidars.distributed.coordinator import MAX_SPECS_PER_JOB
    specs = [
        {"workload_id": f"spec-{i}", "task_type": "t", "min_ram_bytes": 1024}
        for i in range(MAX_SPECS_PER_JOB + 1)
    ]
    resp = client.post("/api/v1/jobs/submit", json={"specs": specs}, headers=_auth(ADMIN_TOKEN))
    assert resp.status_code == 422


def test_oversized_request_body_rejected_with_413(client: TestClient):
    from aidars.distributed.coordinator import MAX_REQUEST_BODY_BYTES
    huge_payload = {
        "workload_id": "huge",
        "task_type": "t",
        "min_ram_bytes": 1024,
        # A deliberately huge string field the middleware's Content-Length
        # check catches before any body parsing happens. This will also
        # fail the parameters-size validator if it got that far, but the
        # body-size middleware should reject it first.
        "parameters": {"blob": "y" * (MAX_REQUEST_BODY_BYTES + 1000)},
    }
    resp = client.post("/api/v1/workloads/submit", json=huge_payload, headers=_auth(ADMIN_TOKEN))
    assert resp.status_code == 413


# ============================================================================
# 19. malformed payload
# ============================================================================


def test_malformed_payload_missing_required_field(client: TestClient):
    resp = client.post(
        "/api/v1/workloads/submit",
        json={"task_type": "t"},  # missing required workload_id
        headers=_auth(ADMIN_TOKEN),
    )
    assert resp.status_code == 422


def test_malformed_payload_wrong_type(client: TestClient):
    resp = client.post(
        "/api/v1/workloads/submit",
        json={"workload_id": 12345, "task_type": "t"},  # workload_id must be str
        headers=_auth(ADMIN_TOKEN),
    )
    assert resp.status_code == 422


def test_malformed_job_payload_invalid_completion_policy_enum(client: TestClient):
    resp = client.post(
        "/api/v1/jobs/submit",
        json={"specs": [{"workload_id": "x", "task_type": "t"}], "completion_policy": "not_a_policy"},
        headers=_auth(ADMIN_TOKEN),
    )
    assert resp.status_code == 422


# ============================================================================
# 20. credentials never appear in logs/errors/state
# ============================================================================


def test_wrong_token_never_echoed_in_error_response(client: TestClient):
    secret_looking_token = "super-secret-token-value-should-not-leak"
    resp = client.get("/api/v1/workers", headers=_auth(secret_looking_token))
    assert resp.status_code == 401
    assert secret_looking_token not in resp.text


def test_worker_credential_never_logged_during_issuance(client: TestClient, caplog):
    with caplog.at_level(logging.DEBUG):
        resp = client.post(
            "/api/v1/workers/register",
            json={"worker_id": "w-logtest", "endpoint_url": "http://w", "ip_address": "127.0.0.1", "port": 8001},
            headers=_auth(BOOTSTRAP_SECRET),
        )
    issued_credential = resp.json()["worker_credential"]
    for record in caplog.records:
        assert issued_credential not in record.message


def test_admin_and_bootstrap_tokens_never_appear_in_persisted_state(tmp_path: Path):
    """Static schema guarantee: no persisted table has any credential-
    shaped column, so there is no code path that could accidentally
    write a secret to disk."""
    store = CoordinatorStateStore(tmp_path / "state.db")
    conn = sqlite3.connect(str(tmp_path / "state.db"))
    for table in ("workers", "workloads", "jobs", "artifacts"):
        columns = {row[1].lower() for row in conn.execute(f"PRAGMA table_info({table})")}
        for forbidden in ("credential", "token", "secret", "password", "authorization"):
            assert not any(forbidden in col for col in columns), f"{table} has a credential-shaped column: {columns}"
    conn.close()


# ============================================================================
# 21. insecure mode requires explicit opt-in
# ============================================================================


@pytest.mark.parametrize("value,expected", [
    ("1", True), ("true", True), ("True", True), ("YES", True),
    ("0", False), ("false", False), ("", False), ("garbage", False),
])
def test_insecure_mode_only_accepted_values_enable_it(value, expected):
    assert is_insecure_mode_enabled({"AIDAR_INSECURE_MODE": value}) is expected


def test_insecure_mode_unset_defaults_to_secure():
    assert is_insecure_mode_enabled({}) is False


def test_coordinator_is_secure_by_default_outside_test_session_override(monkeypatch):
    """Proves the *production* default is secure -- this test explicitly
    removes the session-wide test override (tests/conftest.py) to check
    the real default a fresh deployment would get."""
    monkeypatch.delenv("AIDAR_INSECURE_MODE", raising=False)
    service = CoordinatorService()  # no credential_store override
    assert service.credential_store.insecure_mode is False

    client = TestClient(service.app, raise_server_exceptions=False)
    resp = client.get("/api/v1/workers")
    assert resp.status_code == 401


def test_missing_credentials_never_silently_activate_insecure_mode(client: TestClient):
    """A request with NO credentials at all must still 401 -- absence of
    a token must never be interpreted as 'insecure mode intended'."""
    resp = client.get("/api/v1/workers")
    assert resp.status_code == 401


# ============================================================================
# 22. worker credential invalid after Coordinator restart
# ============================================================================


def test_worker_credential_invalid_after_coordinator_restart(client: TestClient, secure_service: CoordinatorService):
    credential = _register_worker(client)
    assert client.post(
        "/api/v1/workers/w-1/heartbeat", json={"worker_id": "w-1"}, headers=_auth(credential),
    ).status_code == 200

    # Simulate a coordinator restart: worker credentials are ephemeral and
    # were never persisted, so a fresh CredentialStore (same admin/
    # bootstrap config, empty worker-credential map) is exactly what a
    # real restart produces.
    secure_service.credential_store.invalidate_all_worker_credentials()

    resp = client.post("/api/v1/workers/w-1/heartbeat", json={"worker_id": "w-1"}, headers=_auth(credential))
    assert resp.status_code == 401


# ============================================================================
# 24. TLS verification is not disabled
# ============================================================================


def test_no_source_file_disables_tls_verification():
    """Static, deterministic boundary check (per the locked M9 design: no
    TLS termination in the coordinator, but verification on outbound
    calls must never be explicitly disabled anywhere)."""
    src_root = Path(__file__).resolve().parents[2] / "src" / "aidars"
    offending = []
    for path in src_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "verify=False" in text or "verify = False" in text:
            offending.append(str(path))
    assert offending == [], f"TLS verification disabled in: {offending}"


def test_default_httpx_async_client_verifies_tls_by_default():
    """httpx.AsyncClient()'s own documented default for `verify` is True;
    the coordinator's dispatch code (workload.py) constructs its client
    with no arguments, so it inherits that default rather than
    overriding it -- combined with the previous test (no source file
    passes verify=False anywhere), this is the documented security
    boundary rather than an attempt to re-verify httpx's own internals."""
    import inspect
    import httpx
    sig = inspect.signature(httpx.AsyncClient.__init__)
    assert sig.parameters["verify"].default is True
