"""M10.13/M10.19: coordinator/worker launcher (cli.py).

Covers argument parsing/defaults (environment-variable-driven, no
hardcoded localhost-only assumptions baked into behavior -- every
default is overridable) and an end-to-end smoke test that constructs
the exact objects run_coordinator()/run_worker() build internally,
verifying they actually interoperate, without invoking uvicorn.run()
itself (which would block/require a real socket).
"""
from __future__ import annotations

import os
from pathlib import Path

import httpx
import pytest
from httpx import ASGITransport

from aidars.distributed.cli import build_coordinator_arg_parser, build_worker_arg_parser


# ============================================================================
# Argument parsing / environment-driven defaults
# ============================================================================


def test_coordinator_parser_has_safe_defaults_with_no_env_set(monkeypatch):
    for var in (
        "AIDAR_COORDINATOR_HOST", "AIDAR_COORDINATOR_PORT", "AIDAR_COORDINATOR_DB_PATH",
        "AIDAR_COORDINATOR_ID", "AIDAR_HEARTBEAT_INTERVAL_SECONDS", "AIDAR_HEARTBEAT_TIMEOUT_SECONDS",
    ):
        monkeypatch.delenv(var, raising=False)
    args = build_coordinator_arg_parser().parse_args([])
    assert args.host == "0.0.0.0"
    assert args.port == 8000
    assert args.db_path == "./data/coordinator_state.db"
    assert args.coordinator_id is None


def test_coordinator_parser_reads_from_environment(monkeypatch):
    monkeypatch.setenv("AIDAR_COORDINATOR_HOST", "10.0.0.5")
    monkeypatch.setenv("AIDAR_COORDINATOR_PORT", "9000")
    monkeypatch.setenv("AIDAR_COORDINATOR_DB_PATH", "/data/custom.db")
    args = build_coordinator_arg_parser().parse_args([])
    assert args.host == "10.0.0.5"
    assert args.port == 9000
    assert args.db_path == "/data/custom.db"


def test_coordinator_cli_flags_override_environment(monkeypatch):
    monkeypatch.setenv("AIDAR_COORDINATOR_PORT", "9000")
    args = build_coordinator_arg_parser().parse_args(["--port", "7777"])
    assert args.port == 7777


def test_empty_db_path_disables_persistence_via_cli():
    args = build_coordinator_arg_parser().parse_args(["--db-path", ""])
    assert args.db_path == ""


def test_worker_parser_has_safe_defaults(monkeypatch):
    for var in (
        "AIDAR_WORKER_ID", "AIDAR_WORKER_BIND_HOST", "AIDAR_WORKER_IP", "AIDAR_WORKER_PORT",
        "AIDAR_COORDINATOR_URL", "AIDAR_WORKER_CAS_DIR", "AIDAR_WORKER_BOOTSTRAP_SECRET",
    ):
        monkeypatch.delenv(var, raising=False)
    args = build_worker_arg_parser().parse_args([])
    assert args.bind_host == "0.0.0.0"
    assert args.port == 8001
    assert args.coordinator_url == "http://localhost:8000"
    assert args.ip_address is None  # auto-detected at run_worker() time, not the parser's job


def test_worker_parser_reads_bootstrap_secret_from_environment(monkeypatch):
    monkeypatch.setenv("AIDAR_WORKER_BOOTSTRAP_SECRET", "s3cr3t")
    args = build_worker_arg_parser().parse_args([])
    assert args.bootstrap_secret == "s3cr3t"


def test_worker_parser_never_hardcodes_a_platform_specific_path():
    """Regression guard against the exact anti-pattern found in this
    repo's pre-existing ad hoc root-level scripts (e.g. start_worker_a.py,
    which hardcodes C:\\AIDAR-M5\\worker-a\\cas) -- every default here
    must be a relative, portable path."""
    args = build_worker_arg_parser().parse_args([])
    assert "C:" not in args.cas_dir
    assert "\\" not in args.cas_dir


# ============================================================================
# End-to-end wiring smoke test (the exact object graph run_worker()/
# run_coordinator() build, minus uvicorn.run() itself)
# ============================================================================


@pytest.mark.asyncio
async def test_coordinator_and_worker_objects_built_the_cli_way_interoperate(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("AIDAR_INSECURE_MODE", "1")  # smoke test only

    from aidars.distributed.cas_adapter import LocalCASAdapter
    from aidars.distributed.coordinator import CoordinatorService
    from aidars.distributed.state_store import CoordinatorStateStore
    from aidars.distributed.worker import DistributedWorker

    # Mirrors run_coordinator()'s construction exactly.
    state_store = CoordinatorStateStore(tmp_path / "coordinator_state.db")
    coordinator = CoordinatorService(state_store=state_store)
    await coordinator.start()

    # Mirrors run_worker()'s construction exactly.
    cas = LocalCASAdapter(cas_dir=tmp_path / "worker_cas")
    worker = DistributedWorker(
        worker_id="cli-smoke-worker", cas_adapter=cas, ip_address="127.0.0.1",
        port=9321, coordinator_url="http://coordinator",
    )
    worker.client.http_client = httpx.AsyncClient(
        transport=ASGITransport(app=coordinator.app), base_url="http://coordinator",
    )

    await worker.start()
    try:
        assert coordinator.registry.has_worker("cli-smoke-worker")

        worker_transport = ASGITransport(app=worker.server.app)
        worker_http = httpx.AsyncClient(transport=worker_transport, base_url="http://worker")
        resp = await worker_http.get("/api/v1/worker/info")
        assert resp.status_code == 200
        assert resp.json()["worker_id"] == "cli-smoke-worker"
    finally:
        await worker.stop()
        await coordinator.stop()
        state_store.close()
