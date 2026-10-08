import asyncio
import sys
import time
from pathlib import Path

import pytest

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.execution import ExecutionManager
from aidars.distributed.models import (
    FailureCategory,
    WorkloadSpec,
    ExecutionSpec,
)
from aidars.distributed.runtime import GenericSubprocessRuntime

pytestmark = pytest.mark.asyncio

async def test_explicit_timeout_overrides_estimated_duration(tmp_path: Path):
    """
    If estimated_duration_seconds is tiny, the * 3.0 fallback would normally
    kill this task immediately. But with an explicit timeout_seconds=90, it
    should easily finish a 0.2 second sleep.
    """
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    
    spec = WorkloadSpec(
        workload_id="task-1",
        task_type="test",
        estimated_duration_seconds=0.01,  # fallback would be 0.03s
        execution_spec=ExecutionSpec(
            executable=sys.executable,
            args=["-c", "import time; time.sleep(0.2)"],
            timeout_seconds=90.0,
        )
    )
    
    t0 = time.time()
    result = await manager.execute_workload(spec, "w-1", GenericSubprocessRuntime())
    t1 = time.time()
    
    assert result.success is True, result.stderr_snippet
    assert t1 - t0 >= 0.2
    assert result.failure_category is None

async def test_omitted_timeout_seconds_runs_to_completion(tmp_path: Path):
    """An omitted execution timeout means natural completion, regardless of estimate."""
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    
    spec = WorkloadSpec(
        workload_id="task-2",
        task_type="test",
        estimated_duration_seconds=0.05,
        execution_spec=ExecutionSpec(
            executable=sys.executable,
            args=["-c", "import time; time.sleep(0.5)"],
            timeout_seconds=None,
        )
    )
    
    result = await manager.execute_workload(spec, "w-1", GenericSubprocessRuntime())
    
    assert result.success is True
    assert result.failure_category is None

async def test_timeout_expiry_with_explicit_timeout(tmp_path: Path):
    """
    Verify that if execution_spec.timeout_seconds is explicitly small, it correctly
    times out and reports EXECUTION_TIMEOUT.
    """
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    
    spec = WorkloadSpec(
        workload_id="task-3",
        task_type="test",
        estimated_duration_seconds=100.0,  # Fallback would be 300s
        execution_spec=ExecutionSpec(
            executable=sys.executable,
            args=["-c", "import time; time.sleep(2.0)"],
            timeout_seconds=0.1,  # Override to 0.1s
        )
    )
    
    runtime = GenericSubprocessRuntime()
    result = await manager.execute_workload(spec, "w-1", runtime)
    
    assert result.success is False
    assert result.failure_category == FailureCategory.EXECUTION_TIMEOUT
    assert "timed out after 0.1 seconds" in (result.stderr_snippet or "")
    assert runtime._process.returncode is not None

async def test_legacy_workload_without_execution_spec_runs_to_completion(tmp_path: Path):
    """Legacy command workloads also have no implicit execution deadline."""
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    
    spec = WorkloadSpec(
        workload_id="task-4",
        task_type="test",
        estimated_duration_seconds=0.05,
        parameters={"command": f"{sys.executable} -c \"import time; time.sleep(0.5)\""}
    )
    
    result = await manager.execute_workload(spec, "w-1", GenericSubprocessRuntime())
    
    assert result.success is True
    assert result.failure_category is None

async def test_cancellation_still_works_with_explicit_timeout(tmp_path: Path):
    """
    Verify that an explicit cancellation request properly kills a workload governed
    by a custom execution_spec.timeout_seconds.
    """
    cas = LocalCASAdapter(cas_dir=tmp_path / "cas")
    manager = ExecutionManager(cas_adapter=cas, workloads_dir=str(tmp_path / "workloads"))
    runtime = GenericSubprocessRuntime()
    
    spec = WorkloadSpec(
        workload_id="task-5",
        task_type="test",
        estimated_duration_seconds=10.0,
        execution_spec=ExecutionSpec(
            executable=sys.executable,
            args=["-c", "import time; time.sleep(5.0)"],
            timeout_seconds=90.0,
        )
    )
    
    # Start execution in background
    task = asyncio.create_task(manager.execute_workload(spec, "w-1", runtime))
    
    # Wait briefly for process to start
    await asyncio.sleep(0.2)
    
    # Request cancellation
    await runtime.cancel()
    
    result = await task
    
    assert result.success is False
    # Existing (unchanged) classification for a cancelled subprocess is
    # EXECUTION_FAILURE. The key property: it must NOT be EXECUTION_TIMEOUT,
    # i.e. the 90s explicit timeout did not fire and cancellation won.
    assert result.failure_category == FailureCategory.EXECUTION_FAILURE
    assert result.failure_category != FailureCategory.EXECUTION_TIMEOUT
    assert "timed out" not in (result.stderr_snippet or "").lower()
