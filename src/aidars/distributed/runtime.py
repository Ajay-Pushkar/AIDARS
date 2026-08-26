"""Execution runtime abstractions.

Defines the interface for running computational workloads inside isolated sandboxes.
"""

import abc
import asyncio
from typing import Any, Dict, Optional, Tuple

from aidars.distributed.models import WorkloadSpec


class RuntimeAdapter(abc.ABC):
    """Abstract base class for workload execution runtimes."""

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
        pass


class GenericSubprocessRuntime(RuntimeAdapter):
    """A generic runtime that executes a script or command."""

    async def execute(
        self, spec: WorkloadSpec, workdir: str
    ) -> Tuple[bool, Optional[str], Optional[str]]:
        """Executes a command based on the task_type or parameters."""
        command = spec.parameters.get("command")
        if not command:
            return False, None, "No command specified in parameters"

        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=workdir,
            )
            
            # Wait for completion, enforcing the hard timeout at the ExecutionManager level
            stdout_bytes, stderr_bytes = await process.communicate()
            
            stdout = stdout_bytes.decode(errors="replace")
            stderr = stderr_bytes.decode(errors="replace")
            success = process.returncode == 0
            
            # Limit snippet size
            stdout_snippet = stdout[-1000:] if stdout else None
            stderr_snippet = stderr[-1000:] if stderr else None
            
            return success, stdout_snippet, stderr_snippet
            
        except Exception as exc:
            return False, None, f"Execution failed: {exc}"
