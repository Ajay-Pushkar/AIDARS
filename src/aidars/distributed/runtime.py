"""Execution runtime abstractions.

Defines the interface for running computational workloads inside isolated sandboxes.
"""

import abc
import asyncio
import json
import os
import psutil
import platform
from typing import Any, Dict, Optional, Tuple

from aidars.distributed.models import RuntimeExecutionContext, WorkloadSpec


class RuntimeAdapter(abc.ABC):
    """Abstract base class for workload execution runtimes.

    M10.7: supports_checkpointing is an explicit, honest capability flag.
    A subclass MUST NOT override it to True unless checkpoint() actually
    preserves enough state to make a later resume semantically correct --
    the default (False) means "this runtime cannot safely checkpoint",
    and ExecutionManager.execute_workload() trusts that declaration
    rather than assuming universal support. Requesting a checkpoint from
    a runtime that doesn't support it still aborts the run (checkpoint()
    is always safe to call), but is surfaced as a genuine, retryable
    FAILED attempt rather than a fabricated successful migration -- see
    execution.py.
    """

    supports_checkpointing: bool = False

    @abc.abstractmethod
    async def execute(
        self, spec: WorkloadSpec, workdir: str
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """Execute the workload within the specified working directory.

        Args:
            spec: The workload specification.
            workdir: Absolute path to the isolated working directory.

        Returns:
            Tuple of (success, stdout_snippet, stderr_snippet).
        """
    @abc.abstractmethod
    async def checkpoint(self) -> None:
        """Trigger a graceful checkpoint and halt execution."""
        pass

    @abc.abstractmethod
    async def cancel(self) -> None:
        """Forcefully or gracefully terminate the running execution."""
        pass

    async def execute_with_context(
        self, spec: WorkloadSpec, workdir: str,
        context: Optional[RuntimeExecutionContext] = None,
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """Context-aware additive boundary; legacy runtimes remain compatible."""
        return await self.execute(spec, workdir)


class GenericSubprocessRuntime(RuntimeAdapter):
    """A generic runtime that executes a script or command using structured execution.

    SECURITY NOTE: This runtime provides OS-level process execution, not full container isolation.
    While it uses structured argv execution to mitigate shell injection, it does not sandbox the
    underlying file system, network, or kernel resources.

    supports_checkpointing is explicitly False: checkpoint() below only
    terminates the subprocess -- it does not preserve any process state
    (memory, open files, partial output) that a resume could use. Prior
    to M10 this was silently treated as a successful checkpoint anyway;
    that was dishonest and is now corrected (see execution.py).
    """

    supports_checkpointing: bool = False

    def __init__(self):
        self._process: Optional[asyncio.subprocess.Process] = None
        self._checkpoint_requested: bool = False
        self._cancel_requested: bool = False

    async def execute(
        self, spec: WorkloadSpec, workdir: str
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        return await self._execute(spec, workdir, None)

    async def execute_with_context(
        self, spec: WorkloadSpec, workdir: str,
        context: Optional[RuntimeExecutionContext] = None,
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        return await self._execute(spec, workdir, context)

    async def _execute(self, spec: WorkloadSpec, workdir: str,
                       context: Optional[RuntimeExecutionContext]) -> Tuple[bool, Optional[str], Optional[str]]:
        """Executes a command using structured argv or legacy shell string."""
        
        environment = None
        if context is not None:
            environment = os.environ.copy()
            environment["AIDAR_EXECUTION_CONTEXT"] = json.dumps(context.model_dump(mode="json"))
            
        try:
            if spec.execution_spec:
                # M19.3: Secure structured execution
                exec_spec = spec.execution_spec
                env = os.environ.copy() if environment is None else environment
                if exec_spec.env:
                    env.update(exec_spec.env)
                
                cwd = exec_spec.cwd or workdir
                self._process = await asyncio.create_subprocess_exec(
                    exec_spec.executable,
                    *exec_spec.args,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=cwd,
                    env=env,
                )
            else:
                # Legacy fallback
                command = spec.parameters.get("command")
                if not command:
                    return False, None, "No execution_spec or command specified in workload parameters"

                self._process = await asyncio.create_subprocess_shell(
                    command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=workdir,
                    env=environment,
                )
            
            # Wait for completion, enforcing the hard timeout at the ExecutionManager level
            stdout_bytes, stderr_bytes = await self._process.communicate()
            
            stdout = stdout_bytes.decode(errors="replace")
            stderr = stderr_bytes.decode(errors="replace")
            success = self._process.returncode == 0 or self._checkpoint_requested
            
            # Limit snippet size
            stdout_snippet = stdout[-1000:] if stdout else None
            stderr_snippet = stderr[-1000:] if stderr else None
            
            if self._cancel_requested:
                return False, stdout_snippet, "Execution cancelled by request"
                
            return success, stdout_snippet, stderr_snippet
            
        except Exception as exc:
            if self._checkpoint_requested:
                return True, None, "Checkpointed"
            if self._cancel_requested:
                return False, None, "Cancelled"
            return False, None, f"Execution failed: {exc}"
            
    async def checkpoint(self) -> None:
        """Terminate the subprocess gracefully for checkpointing."""
        self._checkpoint_requested = True
        self._terminate_process_tree()

    async def cancel(self) -> None:
        """Cancel the subprocess gracefully then forcefully if needed."""
        self._cancel_requested = True
        self._terminate_process_tree()

    def _terminate_process_tree(self):
        if self._process and self._process.returncode is None:
            try:
                pid = self._process.pid
                parent = psutil.Process(pid)
                children = parent.children(recursive=True)
                for child in children:
                    child.terminate()
                parent.terminate()
            except (psutil.NoSuchProcess, ProcessLookupError):
                pass
