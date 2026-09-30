"""M8.6: output verification -- a successful process exit must not by
itself mean the workload produced its expected output.

Uses the real ExecutionManager + GenericSubprocessRuntime + LocalCASAdapter
(same pattern as tests/unit/test_m6_compute.py's runtime-failure/timeout
tests), not a mock, since the whole point is verifying the real
ingestion/verification code path in distributed/execution.py.
"""
from __future__ import annotations

import pytest

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.execution import ExecutionManager
from aidars.distributed.models import WorkloadSpec
from aidars.distributed.runtime import GenericSubprocessRuntime


def _manager(tmp_path):
    cas = LocalCASAdapter(cas_dir=str(tmp_path / "cas"))
    return ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))


# ============================================================================
# Successful process + valid output
# ============================================================================


@pytest.mark.asyncio
async def test_successful_process_with_expected_output_succeeds(tmp_path):
    manager = _manager(tmp_path)
    spec = WorkloadSpec(
        workload_id="task-valid", task_type="test",
        parameters={
            "command": "echo frame1 > outputs/frame1.txt",
            "expected_output_count": 1,
        },
    )

    result = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())

    assert result.success is True
    assert len(result.output_asset_hashes) == 1
    assert result.error_message is None


# ============================================================================
# Successful process + missing output -- the core M8.6 gap this closes
# ============================================================================


@pytest.mark.asyncio
async def test_successful_process_with_missing_output_fails_verification(tmp_path):
    """The process exits 0 but writes nothing -- before M8.6 this was
    reported as success=True, output_asset_hashes=set(), indistinguishable
    from a legitimate empty-output success. With expected_output_count
    declared, this must now be reported as a failure."""
    manager = _manager(tmp_path)
    spec = WorkloadSpec(
        workload_id="task-missing", task_type="test",
        parameters={
            "command": "true",  # exits 0, writes nothing
            "expected_output_count": 1,
        },
    )

    result = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())

    assert result.success is False
    assert len(result.output_asset_hashes) == 0
    assert "Output verification failed" in (result.stderr_snippet or "")


@pytest.mark.asyncio
async def test_partial_output_below_expected_count_fails_verification(tmp_path):
    manager = _manager(tmp_path)
    spec = WorkloadSpec(
        workload_id="task-partial", task_type="test",
        parameters={
            "command": "echo one > outputs/f1.txt",  # only 1 file
            "expected_output_count": 3,  # but 3 were expected
        },
    )

    result = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())

    assert result.success is False
    assert len(result.output_asset_hashes) == 1  # the one file that WAS produced is still hashed/reported


# ============================================================================
# Execution failure (process itself fails) -- verification is irrelevant
# ============================================================================


@pytest.mark.asyncio
async def test_execution_failure_is_reported_regardless_of_expected_output_count(tmp_path):
    manager = _manager(tmp_path)
    spec = WorkloadSpec(
        workload_id="task-exec-fail", task_type="test",
        parameters={"command": "exit 1", "expected_output_count": 1},
    )

    result = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())

    assert result.success is False
    assert len(result.output_asset_hashes) == 0


# ============================================================================
# Empty output behavior: backward-compatible opt-in default
# ============================================================================


@pytest.mark.asyncio
async def test_empty_output_without_declared_expectation_is_unchanged(tmp_path):
    """No expected_output_count key at all (the default for any spec that
    doesn't opt in, including every pre-M8 caller/test) -- zero output
    files remains a legitimate success, exactly as before this change."""
    manager = _manager(tmp_path)
    spec = WorkloadSpec(
        workload_id="task-no-expectation", task_type="test",
        parameters={"command": "true"},  # no expected_output_count key
    )

    result = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())

    assert result.success is True
    assert result.output_asset_hashes == set()


@pytest.mark.asyncio
async def test_expected_output_count_of_zero_is_satisfied_by_no_output(tmp_path):
    manager = _manager(tmp_path)
    spec = WorkloadSpec(
        workload_id="task-zero-expected", task_type="test",
        parameters={"command": "true", "expected_output_count": 0},
    )

    result = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())

    assert result.success is True


# ============================================================================
# output_asset_sizes is populated for downstream Artifact metadata (M8.8)
# ============================================================================


@pytest.mark.asyncio
async def test_output_asset_sizes_populated_alongside_hashes(tmp_path):
    manager = _manager(tmp_path)
    spec = WorkloadSpec(
        workload_id="task-sizes", task_type="test",
        parameters={"command": "printf '12345' > outputs/f1.txt"},
    )

    result = await manager.execute_workload(spec, "w1", GenericSubprocessRuntime())

    assert result.success is True
    [only_hash] = list(result.output_asset_hashes)
    assert result.output_asset_sizes[only_hash] == 5
