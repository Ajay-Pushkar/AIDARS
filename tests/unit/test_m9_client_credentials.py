"""M9: DistributedClient/DistributedWorker credential plumbing, exercised
against a real (secure, non-insecure-mode) CoordinatorService via
httpx.ASGITransport -- the same in-process pattern already established in
test_streaming_client.py's control-plane RPC tests, just with a
CredentialStore that actually enforces authentication instead of relying
on the session-wide insecure-mode default.
"""
from __future__ import annotations

import httpx
import pytest
from httpx import ASGITransport

from aidars.distributed.auth import CredentialStore
from aidars.distributed.client import DistributedClient
from aidars.distributed.coordinator import CoordinatorService
from aidars.distributed.models import HeartbeatPayload, WorkerRegistrationPayload

ADMIN_TOKEN = "client-test-admin"
BOOTSTRAP_SECRET = "client-test-bootstrap"


@pytest.fixture
def secure_coordinator() -> CoordinatorService:
    return CoordinatorService(credential_store=CredentialStore(
        admin_tokens={ADMIN_TOKEN}, bootstrap_secret=BOOTSTRAP_SECRET, insecure_mode=False,
    ))


@pytest.mark.asyncio
async def test_client_with_bootstrap_secret_registers_and_auto_uses_issued_credential(
    secure_coordinator: CoordinatorService,
):
    transport = ASGITransport(app=secure_coordinator.app)
    mock_http = httpx.AsyncClient(transport=transport, base_url="http://coordinator")

    async with DistributedClient(
        coordinator_url="http://coordinator",
        http_client=mock_http,
        bootstrap_secret=BOOTSTRAP_SECRET,
    ) as client:
        reg_resp = await client.register_worker(WorkerRegistrationPayload(
            worker_id="w-cred-01", endpoint_url="http://127.0.0.1:8080",
            ip_address="127.0.0.1", port=8080,
        ))
        assert reg_resp.status == "registered"
        assert reg_resp.worker_credential
        assert client._worker_credential == reg_resp.worker_credential

        # send_heartbeat() must use the auto-stored credential with no
        # further action from the caller.
        hb_resp = await client.send_heartbeat(
            worker_id="w-cred-01",
            payload=HeartbeatPayload(worker_id="w-cred-01"),
        )
        assert hb_resp.status == "healthy"


@pytest.mark.asyncio
async def test_client_without_bootstrap_secret_fails_closed_against_secure_coordinator(
    secure_coordinator: CoordinatorService,
):
    """Preserves the pre-M9 constructor signature's usability, but against
    a real secure coordinator, an unauthenticated client must fail
    closed, not silently succeed."""
    transport = ASGITransport(app=secure_coordinator.app)
    mock_http = httpx.AsyncClient(transport=transport, base_url="http://coordinator")

    async with DistributedClient(
        coordinator_url="http://coordinator",
        http_client=mock_http,
        # no bootstrap_secret
    ) as client:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await client.register_worker(WorkerRegistrationPayload(
                worker_id="w-nocred", endpoint_url="http://127.0.0.1:8081",
                ip_address="127.0.0.1", port=8081,
            ))
        assert exc_info.value.response.status_code == 401


@pytest.mark.asyncio
async def test_client_unregister_uses_stored_worker_credential(secure_coordinator: CoordinatorService):
    transport = ASGITransport(app=secure_coordinator.app)
    mock_http = httpx.AsyncClient(transport=transport, base_url="http://coordinator")

    async with DistributedClient(
        coordinator_url="http://coordinator", http_client=mock_http, bootstrap_secret=BOOTSTRAP_SECRET,
    ) as client:
        await client.register_worker(WorkerRegistrationPayload(
            worker_id="w-unreg", endpoint_url="http://127.0.0.1:8082",
            ip_address="127.0.0.1", port=8082,
        ))
        result = await client.unregister_worker("w-unreg")
        assert result["status"] == "unregistered"


@pytest.mark.asyncio
async def test_bootstrap_secret_never_appears_in_worker_credential(secure_coordinator: CoordinatorService):
    transport = ASGITransport(app=secure_coordinator.app)
    mock_http = httpx.AsyncClient(transport=transport, base_url="http://coordinator")

    async with DistributedClient(
        coordinator_url="http://coordinator", http_client=mock_http, bootstrap_secret=BOOTSTRAP_SECRET,
    ) as client:
        reg_resp = await client.register_worker(WorkerRegistrationPayload(
            worker_id="w-distinct", endpoint_url="http://127.0.0.1:8083",
            ip_address="127.0.0.1", port=8083,
        ))
        assert reg_resp.worker_credential != BOOTSTRAP_SECRET
        assert BOOTSTRAP_SECRET not in reg_resp.worker_credential
