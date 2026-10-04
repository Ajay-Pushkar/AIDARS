"""M10.7/M10.19: checkpoint capability model.

Covers three layers:
  1. runtime.py's honest supports_checkpointing flag.
  2. execution.py's capability-gated handling -- a checkpoint request
     against a runtime that doesn't support it must produce a genuine,
     retryable FAILED result, never a fabricated success.
  3. checkpoint.py's validate_checkpoint() metadata validation.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

import pytest

from aidars.distributed.attempt import AttemptRecord
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.checkpoint import CURRENT_CHECKPOINT_FORMAT_VERSION, validate_checkpoint
from aidars.distributed.execution import ExecutionManager
from aidars.distributed.models import FailureCategory, WorkloadSpec
from aidars.distributed.runtime import GenericSubprocessRuntime, RuntimeAdapter


# ============================================================================
# Capability declaration
# ============================================================================


def test_runtime_adapter_base_defaults_to_no_checkpoint_support():
    assert RuntimeAdapter.supports_checkpointing is False


def test_generic_subprocess_runtime_explicitly_declares_no_checkpoint_support():
    assert GenericSubprocessRuntime.supports_checkpointing is False
    assert GenericSubprocessRuntime().supports_checkpointing is False


class _FakeCheckpointCapableRuntime(RuntimeAdapter):
    """Test double for a hypothetical runtime that genuinely supports
    checkpointing -- exercises the "capability declared True" path
    without requiring a real such runtime to exist yet in this codebase."""

    supports_checkpointing = True

    def __init__(self) -> None:
        self._checkpoint_requested = False

    async def execute(self, spec: WorkloadSpec, workdir: str) -> Tuple[bool, Optional[str], Optional[str]]:
        # Write one output file so ingestion has something to hash.
        outputs_dir = Path(workdir) / "outputs"
        (outputs_dir / "result.bin").write_bytes(b"checkpoint payload")
        return True, "ok", None

    async def checkpoint(self) -> None:
        self._checkpoint_requested = True

    async def cancel(self) -> None:
        pass


# ============================================================================
# execution.py capability gating
# ============================================================================


@pytest.mark.asyncio
async def test_unsupported_checkpoint_request_is_a_genuine_retryable_failure(tmp_path):
    """The core M10.7 honesty requirement: requesting a checkpoint from a
    runtime that can't safely produce one must never be reported as a
    successful migration."""
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    spec = WorkloadSpec(
        workload_id="task-checkpoint-unsupported", task_type="test",
        parameters={"command": "python3 -c \"import time; time.sleep(5)\""},
    )
    runtime = GenericSubprocessRuntime()
    assert runtime.supports_checkpointing is False

    import asyncio

    async def _request_checkpoint_shortly():
        await asyncio.sleep(0.05)
        await runtime.checkpoint()

    checkpoint_task = asyncio.create_task(_request_checkpoint_shortly())
    result = await manager.execute_workload(spec, "w-1", runtime)
    await checkpoint_task

    assert result.was_checkpointed is False
    assert result.success is False
    assert result.failure_category == FailureCategory.WORKER_UNAVAILABLE
    assert result.checkpoint_hash is None
    assert "does not support checkpointing" in (result.stderr_snippet or "")


@pytest.mark.asyncio
async def test_supported_checkpoint_request_is_honored_with_full_metadata(tmp_path):
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    spec = WorkloadSpec(workload_id="task-checkpoint-supported", task_type="test")
    runtime = _FakeCheckpointCapableRuntime()
    runtime._checkpoint_requested = True  # simulate a checkpoint having been requested

    result = await manager.execute_workload(spec, "w-1", runtime)

    assert result.was_checkpointed is True
    assert result.success is True
    assert result.checkpoint_hash is not None
    assert result.checkpoint_runtime_type == "_FakeCheckpointCapableRuntime"
    assert result.checkpoint_format_version == CURRENT_CHECKPOINT_FORMAT_VERSION
    assert result.failure_category is None


@pytest.mark.asyncio
async def test_no_checkpoint_requested_is_unaffected_by_capability_flag(tmp_path):
    """Normal (non-checkpointed) execution behaves identically regardless
    of supports_checkpointing -- the gating only applies when a
    checkpoint was actually requested."""
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    spec = WorkloadSpec(workload_id="task-normal", task_type="test", parameters={"command": "true"})
    runtime = GenericSubprocessRuntime()

    result = await manager.execute_workload(spec, "w-1", runtime)

    assert result.was_checkpointed is False
    assert result.success is True
    assert result.checkpoint_hash is None


# ============================================================================
# checkpoint.py metadata validation
# ============================================================================


def _make_attempt(checkpoint_hash=None, checkpoint_format_version=None) -> AttemptRecord:
    return AttemptRecord(
        attempt_id="w-1#1", workload_id="w-1", attempt_number=1,
        checkpoint_hash=checkpoint_hash, checkpoint_format_version=checkpoint_format_version,
    )


def test_validate_checkpoint_no_checkpoint_recorded():
    attempt = _make_attempt(checkpoint_hash=None)
    valid, reason = validate_checkpoint(attempt, cas_has_hash_fn=lambda h: True)
    assert valid is False
    assert "no recorded checkpoint" in reason


def test_validate_checkpoint_incompatible_format_version():
    attempt = _make_attempt(checkpoint_hash="a" * 64, checkpoint_format_version=999)
    valid, reason = validate_checkpoint(attempt, cas_has_hash_fn=lambda h: True)
    assert valid is False
    assert "incompatible" in reason


def test_validate_checkpoint_hash_not_found_in_cluster():
    attempt = _make_attempt(
        checkpoint_hash="a" * 64, checkpoint_format_version=CURRENT_CHECKPOINT_FORMAT_VERSION,
    )
    valid, reason = validate_checkpoint(attempt, cas_has_hash_fn=lambda h: False)
    assert valid is False
    assert "not found" in reason


def test_validate_checkpoint_success():
    attempt = _make_attempt(
        checkpoint_hash="a" * 64, checkpoint_format_version=CURRENT_CHECKPOINT_FORMAT_VERSION,
    )
    valid, reason = validate_checkpoint(attempt, cas_has_hash_fn=lambda h: h == "a" * 64)
    assert valid is True
    assert reason is None


def test_validate_checkpoint_reuses_caller_supplied_lookup_not_a_new_identity_scheme():
    """The lookup function is a plain callable -- validate_checkpoint()
    itself has no CAS/registry dependency, confirming it doesn't invent a
    second identity/query mechanism (see module docstring)."""
    calls = []

    def tracking_lookup(h: str) -> bool:
        calls.append(h)
        return True

    attempt = _make_attempt(
        checkpoint_hash="b" * 64, checkpoint_format_version=CURRENT_CHECKPOINT_FORMAT_VERSION,
    )
    validate_checkpoint(attempt, cas_has_hash_fn=tracking_lookup)
    assert calls == ["b" * 64]
