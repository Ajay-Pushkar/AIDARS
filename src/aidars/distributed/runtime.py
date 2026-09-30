"""Execution runtime abstractions.

Defines the interface for running computational workloads inside isolated sandboxes.
"""

import abc
import asyncio
from typing import Any, Dict, Optional, Tuple

from aidars.distributed.models import WorkloadSpec


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


class GenericSubprocessRuntime(RuntimeAdapter):
    """A generic runtime that executes a script or command.

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

    async def execute(
        self, spec: WorkloadSpec, workdir: str
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """Executes a command based on the task_type or parameters."""
        command = spec.parameters.get("command")
        if not command:
            return False, None, "No command specified in parameters"

        try:
            self._process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=workdir,
            )
            
            # Wait for completion, enforcing the hard timeout at the ExecutionManager level
            stdout_bytes, stderr_bytes = await self._process.communicate()
            
            stdout = stdout_bytes.decode(errors="replace")
            stderr = stderr_bytes.decode(errors="replace")
            success = self._process.returncode == 0 or self._checkpoint_requested
            
            # Limit snippet size
            stdout_snippet = stdout[-1000:] if stdout else None
            stderr_snippet = stderr[-1000:] if stderr else None
            
            return success, stdout_snippet, stderr_snippet
            
        except Exception as exc:
            if self._checkpoint_requested:
                return True, None, "Checkpointed"
            return False, None, f"Execution failed: {exc}"
            
    async def checkpoint(self) -> None:
        """Terminate the subprocess gracefully for checkpointing."""
        self._checkpoint_requested = True
        if self._process and self._process.returncode is None:
            try:
                self._process.terminate()
            except ProcessLookupError:
                pass
