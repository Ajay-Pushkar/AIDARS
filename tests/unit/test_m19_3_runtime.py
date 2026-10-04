import asyncio
import os
import pytest
from aidars.distributed.models import WorkloadSpec, ExecutionSpec
from aidars.distributed.runtime import GenericSubprocessRuntime
import tempfile
import sys

@pytest.fixture
def temp_workdir():
    with tempfile.TemporaryDirectory() as td:
        yield td

def create_spec(exec_spec: ExecutionSpec) -> WorkloadSpec:
    return WorkloadSpec(
        workload_id="test-wl",
        task_type="test",
        execution_spec=exec_spec
    )

@pytest.mark.asyncio
async def test_normal_execution(temp_workdir):
    exec_spec = ExecutionSpec(
        executable=sys.executable,
        args=["-c", "print('hello')"]
    )
    spec = create_spec(exec_spec)
    runtime = GenericSubprocessRuntime()
    success, stdout, stderr = await runtime.execute(spec, temp_workdir)
    assert success is True
    assert "hello" in stdout

@pytest.mark.asyncio
async def test_arguments_containing_spaces(temp_workdir):
    exec_spec = ExecutionSpec(
        executable=sys.executable,
        args=["-c", "import sys; print(sys.argv[1])", "hello world spaces"]
    )
    spec = create_spec(exec_spec)
    runtime = GenericSubprocessRuntime()
    success, stdout, stderr = await runtime.execute(spec, temp_workdir)
    assert success is True
    assert "hello world spaces" in stdout

@pytest.mark.asyncio
async def test_arguments_containing_shell_metacharacters(temp_workdir):
    exec_spec = ExecutionSpec(
        executable=sys.executable,
        args=["-c", "import sys; print(sys.argv[1])", "hello && echo bypass"]
    )
    spec = create_spec(exec_spec)
    runtime = GenericSubprocessRuntime()
    success, stdout, stderr = await runtime.execute(spec, temp_workdir)
    assert success is True
    assert "hello && echo bypass" in stdout
    # Shell should not have executed 'echo bypass'

@pytest.mark.asyncio
async def test_environment_variables(temp_workdir):
    exec_spec = ExecutionSpec(
        executable=sys.executable,
        args=["-c", "import os; print(os.environ.get('MY_TEST_VAR'))"],
        env={"MY_TEST_VAR": "secret123"}
    )
    spec = create_spec(exec_spec)
    runtime = GenericSubprocessRuntime()
    success, stdout, stderr = await runtime.execute(spec, temp_workdir)
    assert success is True
    assert "secret123" in stdout

@pytest.mark.asyncio
async def test_non_zero_exit(temp_workdir):
    exec_spec = ExecutionSpec(
        executable=sys.executable,
        args=["-c", "import sys; sys.exit(1)"]
    )
    spec = create_spec(exec_spec)
    runtime = GenericSubprocessRuntime()
    success, stdout, stderr = await runtime.execute(spec, temp_workdir)
    assert success is False

@pytest.mark.asyncio
async def test_missing_executable(temp_workdir):
    exec_spec = ExecutionSpec(
        executable="non_existent_executable_123456",
        args=[]
    )
    spec = create_spec(exec_spec)
    runtime = GenericSubprocessRuntime()
    success, stdout, stderr = await runtime.execute(spec, temp_workdir)
    assert success is False
    assert "Execution failed" in stderr

@pytest.mark.asyncio
async def test_invalid_working_directory():
    # Only available if we specify absolute invalid dir that raises FileNotFoundError
    # On Windows, missing directory might cause a different exception
    exec_spec = ExecutionSpec(
        executable=sys.executable,
        args=["-c", "print('hello')"],
        cwd="C:\\invalid_directory_123456\\does_not_exist"
    )
    spec = create_spec(exec_spec)
    runtime = GenericSubprocessRuntime()
    success, stdout, stderr = await runtime.execute(spec, ".")
    assert success is False
    assert "Execution failed" in stderr

@pytest.mark.asyncio
async def test_cancellation(temp_workdir):
    exec_spec = ExecutionSpec(
        executable=sys.executable,
        args=["-c", "import time; time.sleep(10)"]
    )
    spec = create_spec(exec_spec)
    runtime = GenericSubprocessRuntime()
    
    # Run in background
    task = asyncio.create_task(runtime.execute(spec, temp_workdir))
    
    # Wait for it to start
    await asyncio.sleep(0.5)
    
    # Cancel it
    await runtime.cancel()
    
    # Wait for completion
    success, stdout, stderr = await task
    
    assert success is False
    assert "Cancelled" in stderr or "Execution cancelled by request" in stderr
