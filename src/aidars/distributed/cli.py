"""M10.13: coordinator/worker process launchers.

Fills a gap that already existed before M10: the Makefile declares
`start-coordinator`/`start-worker` targets (see Makefile) with no
recipe bodies, and no `__main__`/`main.py`/entrypoint module existed
anywhere in this package to run a CoordinatorService or a distributed
worker as an actual long-lived process. This module is that entrypoint
-- it wires the SAME classes every test already uses
(CoordinatorService, DistributedWorker/WorkerServer, CoordinatorStateStore,
CredentialStore, LocalCASAdapter) into two `argparse`-driven processes,
serving each over uvicorn. It introduces no new architecture: everything
here is configuration and process wiring around existing code.

Every setting is environment-variable/CLI-flag driven -- no hardcoded
localhost-only assumptions, no developer-specific paths, no baked-in
secrets. Defaults are safe for local development (SQLite under ./data,
0.0.0.0 bind, AIDAR_INSECURE_MODE left OFF unless the environment sets
it) but every value is overridable for a real deployment.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import Optional, Sequence

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid float in %s=%r; using default %s", name, raw, default)
        return default


def _configure_logging() -> None:
    level_name = os.environ.get("AIDAR_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


# ============================================================================
# Coordinator
# ============================================================================


def build_coordinator_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aidars-coordinator",
        description="Run the AIDAR distributed coordinator control-plane service.",
    )
    parser.add_argument(
        "--host", default=os.environ.get("AIDAR_COORDINATOR_HOST", "0.0.0.0"),
        help="Bind host (default: $AIDAR_COORDINATOR_HOST or 0.0.0.0).",
    )
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("AIDAR_COORDINATOR_PORT", "8000")),
        help="Bind port (default: $AIDAR_COORDINATOR_PORT or 8000).",
    )
    parser.add_argument(
        "--db-path", default=os.environ.get("AIDAR_COORDINATOR_DB_PATH", "./data/coordinator_state.db"),
        help="SQLite state file path. Empty string disables persistence "
             "(pure in-memory, matching the pre-persistence default). "
             "(default: $AIDAR_COORDINATOR_DB_PATH or ./data/coordinator_state.db)",
    )
    parser.add_argument(
        "--coordinator-id", default=os.environ.get("AIDAR_COORDINATOR_ID"),
        help="Stable coordinator identity (default: $AIDAR_COORDINATOR_ID, or a random id).",
    )
    parser.add_argument(
        "--heartbeat-interval-seconds", type=float,
        default=_env_float("AIDAR_HEARTBEAT_INTERVAL_SECONDS", 5.0),
    )
    parser.add_argument(
        "--heartbeat-timeout-seconds", type=float,
        default=_env_float("AIDAR_HEARTBEAT_TIMEOUT_SECONDS", 15.0),
    )
    parser.add_argument(
        "--log-level", default=os.environ.get("AIDAR_LOG_LEVEL", "INFO"),
    )
    return parser


def run_coordinator(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for the `aidars-coordinator` console script."""
    args = build_coordinator_arg_parser().parse_args(argv)
    os.environ.setdefault("AIDAR_LOG_LEVEL", args.log_level)
    _configure_logging()

    import uvicorn
    from aidars.distributed.coordinator import CoordinatorService
    from aidars.distributed.state_store import CoordinatorStateStore

    state_store = None
    if args.db_path:
        # Same CoordinatorStateStore every test already exercises --
        # includes the M10.15 single-writer flock safety mechanism, so a
        # second coordinator process accidentally pointed at the same
        # db_path fails fast at startup instead of corrupting state.
        state_store = CoordinatorStateStore(args.db_path)

    # CredentialStore is intentionally NOT constructed here -- omitting
    # it lets CoordinatorService's own default (CredentialStore()) read
    # AIDAR_ADMIN_TOKENS / AIDAR_WORKER_BOOTSTRAP_SECRET / AIDAR_INSECURE_MODE
    # from the environment exactly as documented in auth.py, so this
    # launcher adds no second place that credential configuration lives.
    service = CoordinatorService(
        coordinator_id=args.coordinator_id,
        heartbeat_interval_seconds=args.heartbeat_interval_seconds,
        heartbeat_timeout_seconds=args.heartbeat_timeout_seconds,
        state_store=state_store,
    )

    logger.info(
        "Starting AIDAR coordinator %s on %s:%d (persistence=%s, insecure_mode=%s)",
        service.coordinator_id, args.host, args.port,
        "on" if state_store else "off", service.credential_store.insecure_mode,
    )
    uvicorn.run(service.app, host=args.host, port=args.port, log_level=args.log_level.lower())


# ============================================================================
# Worker
# ============================================================================


def _detect_local_ip() -> str:
    """Best-effort IP auto-detection for --ip-address's default. Used only
    when the operator doesn't supply one explicitly -- DistributedWorker's
    WorkerRegistrationPayload.ip_address is strictly validated as a real
    IP address (validate_ip_address in models.py), not a DNS hostname, so
    a container-network deployment (e.g. Docker Compose) that can't route
    via 127.0.0.1 between containers MUST pass --ip-address / $AIDAR_WORKER_IP
    explicitly rather than rely on this fallback.
    """
    import socket
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"


def build_worker_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="aidars-worker",
        description="Run an AIDAR distributed worker node (data plane + execution).",
    )
    parser.add_argument(
        "--worker-id", default=os.environ.get("AIDAR_WORKER_ID"),
        help="Stable worker identity (default: $AIDAR_WORKER_ID, or a generated id).",
    )
    parser.add_argument(
        "--bind-host", default=os.environ.get("AIDAR_WORKER_BIND_HOST", "0.0.0.0"),
        help="Address uvicorn binds to (default: $AIDAR_WORKER_BIND_HOST or 0.0.0.0).",
    )
    parser.add_argument(
        "--ip-address", default=os.environ.get("AIDAR_WORKER_IP"),
        help="The routable IP other workers/the coordinator use to reach this "
             "worker (must be a real IP, not a hostname -- see docstring on "
             "_detect_local_ip). Default: $AIDAR_WORKER_IP, or best-effort "
             "auto-detection; ALWAYS set this explicitly in a container/NAT "
             "deployment.",
    )
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("AIDAR_WORKER_PORT", "8001")),
    )
    parser.add_argument(
        "--coordinator-url", default=os.environ.get("AIDAR_COORDINATOR_URL", "http://localhost:8000"),
        help="Coordinator base URL to register/heartbeat against "
             "(default: $AIDAR_COORDINATOR_URL or http://localhost:8000).",
    )
    parser.add_argument(
        "--cas-dir", default=os.environ.get("AIDAR_WORKER_CAS_DIR", "./data/worker_cas"),
    )
    parser.add_argument(
        "--staging-dir", default=os.environ.get("AIDAR_WORKER_STAGING_DIR"),
        help="Default: <cas-dir>/../worker_staging (LocalCASAdapter's own default).",
    )
    parser.add_argument(
        "--capacity-bytes", type=int,
        default=int(os.environ.get("AIDAR_WORKER_CAPACITY_BYTES", str(100 * 1024 * 1024 * 1024))),
    )
    parser.add_argument(
        "--bootstrap-secret", default=os.environ.get("AIDAR_WORKER_BOOTSTRAP_SECRET"),
        help="M9 bootstrap/join secret presented on registration. Must match "
             "the coordinator's AIDAR_WORKER_BOOTSTRAP_SECRET unless the "
             "coordinator is running with AIDAR_INSECURE_MODE.",
    )
    parser.add_argument(
        "--heartbeat-interval-seconds", type=float,
        default=_env_float("AIDAR_HEARTBEAT_INTERVAL_SECONDS", 5.0),
    )
    parser.add_argument("--log-level", default=os.environ.get("AIDAR_LOG_LEVEL", "INFO"))
    return parser


def run_worker(argv: Optional[Sequence[str]] = None) -> None:
    """Entry point for the `aidars-worker` console script."""
    args = build_worker_arg_parser().parse_args(argv)
    os.environ.setdefault("AIDAR_LOG_LEVEL", args.log_level)
    _configure_logging()

    import uvicorn
    from aidars.distributed.cas_adapter import LocalCASAdapter
    from aidars.distributed.worker import DistributedWorker

    worker_id = args.worker_id or f"worker-{os.getpid()}"
    ip_address = args.ip_address or _detect_local_ip()

    cas_kwargs = {"cas_dir": args.cas_dir}
    if args.staging_dir:
        cas_kwargs["staging_dir"] = args.staging_dir
    cas = LocalCASAdapter(**cas_kwargs)

    # DistributedWorker builds its own WorkerServer/ExecutionManager
    # internally from cas_adapter/ip_address/port -- this launcher does
    # not duplicate that wiring, only supplies the configuration.
    distributed_worker = DistributedWorker(
        worker_id=worker_id,
        cas_adapter=cas,
        ip_address=ip_address,
        port=args.port,
        coordinator_url=args.coordinator_url,
        capacity_bytes=args.capacity_bytes,
        heartbeat_interval_seconds=args.heartbeat_interval_seconds,
        bootstrap_secret=args.bootstrap_secret,
    )

    async def _run() -> None:
        await distributed_worker.start()
        config = uvicorn.Config(
            distributed_worker.server.app,
            host=args.bind_host, port=args.port, log_level=args.log_level.lower(),
        )
        server = uvicorn.Server(config)
        try:
            await server.serve()
        finally:
            await distributed_worker.stop()

    logger.info(
        "Starting AIDAR worker %s bound on %s:%d (advertised endpoint=%s, coordinator=%s)",
        worker_id, args.bind_host, args.port, distributed_worker.endpoint_url, args.coordinator_url,
    )
    asyncio.run(_run())


def main() -> None:
    """`python -m aidars.distributed.cli {coordinator|worker} ...`"""
    if len(sys.argv) < 2 or sys.argv[1] not in ("coordinator", "worker"):
        print("Usage: python -m aidars.distributed.cli {coordinator|worker} [options]", file=sys.stderr)
        sys.exit(2)
    role, rest = sys.argv[1], sys.argv[2:]
    if role == "coordinator":
        run_coordinator(rest)
    else:
        run_worker(rest)


if __name__ == "__main__":
    main()
