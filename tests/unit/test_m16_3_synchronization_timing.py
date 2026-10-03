import pytest
from unittest.mock import AsyncMock, MagicMock

from aidars.distributed.models import (
    FailureCategory,
    WorkloadExecutionResult,
    WorkloadSpec,
)
from aidars.distributed.worker import DistributedWorker

def test_m16_3_total_duration_includes_transfer():
    """Verify total_duration_seconds exactly includes transfer_duration_seconds."""
    res = WorkloadExecutionResult(
        workload_id="w-1",
        worker_id="worker-1",
        success=True,
        output_asset_hashes=set(),
        staging_duration_seconds=1.0,
        execution_duration_seconds=2.0,
        output_ingestion_duration_seconds=3.0,
        verification_duration_seconds=4.0,
        transfer_duration_seconds=5.0,
    )
    assert res.total_duration_seconds == 15.0


def test_m16_3_backward_compatible_default_workload_execution_result():
    """Verify older constructions of WorkloadExecutionResult without transfer_duration_seconds still work."""
    res = WorkloadExecutionResult(
        workload_id="w-1",
        worker_id="worker-1",
        success=True,
        output_asset_hashes=set(),
        execution_duration_seconds=2.0,
    )
    assert res.transfer_duration_seconds == 0.0
    assert res.total_duration_seconds == 2.0


@pytest.mark.asyncio
async def test_m16_3_worker_no_dependencies_zero_transfer():
    """Verify worker execution handles no dependencies gracefully with valid transfer timing."""
    worker = DistributedWorker(cas_dir="/tmp/cas", worker_id="worker-1", http_client=AsyncMock())
    worker.execution_manager = AsyncMock()
    worker.execution_manager.execute_workload.return_value = WorkloadExecutionResult(
        workload_id="w-1",
        worker_id="worker-1",
        success=True,
        output_asset_hashes=set(),
        execution_duration_seconds=1.5,
    )
    spec = WorkloadSpec(workload_id="w-1", task_type="test", action="test", input_asset_hashes=set())
    
    result = await worker.execute_workload(spec)
    
    assert result.success is True
    assert result.transfer_duration_seconds >= 0.0


@pytest.mark.asyncio
async def test_m16_3_worker_successful_transfer():
    """Verify worker tracks transfer latency across success."""
    worker = DistributedWorker(cas_dir="/tmp/cas", worker_id="worker-1", http_client=AsyncMock())
    worker.execution_manager = AsyncMock()
    worker.execution_manager.execute_workload.return_value = WorkloadExecutionResult(
        workload_id="w-1",
        worker_id="worker-1",
        success=True,
        output_asset_hashes=set(),
        execution_duration_seconds=1.5,
    )
    
    class MockResult:
        success = True
    
    worker.single_flight = AsyncMock()
    h1 = "a" * 64
    worker.single_flight.run.return_value = {h1: MockResult()}
    
    spec = WorkloadSpec(workload_id="w-1", task_type="test", action="test", input_asset_hashes={h1})
    
    result = await worker.execute_workload(spec)
    
    assert result.success is True
    assert result.transfer_duration_seconds >= 0.0
    assert worker.single_flight.run.called


@pytest.mark.asyncio
async def test_m16_3_worker_transfer_failure_propagation():
    """Verify worker transfer failure correctly sets ASSET_TRANSFER_FAILURE and returns early."""
    worker = DistributedWorker(cas_dir="/tmp/cas", worker_id="worker-1", http_client=AsyncMock())
    worker.execution_manager = AsyncMock()
    
    class MockResult:
        success = False
        error_message = "Network timeout"
    
    worker.single_flight = AsyncMock()
    h1 = "b" * 64
    worker.single_flight.run.return_value = {h1: MockResult()}
    
    spec = WorkloadSpec(workload_id="w-1", task_type="test", action="test", input_asset_hashes={h1})
    
    result = await worker.execute_workload(spec)
    
    assert result.success is False
    assert result.failure_category == FailureCategory.ASSET_TRANSFER_FAILURE
    assert result.error_message == f"Failed to sync dependency {h1}: Network timeout"
    assert result.transfer_duration_seconds >= 0.0
    
    assert not worker.execution_manager.execute_workload.called
