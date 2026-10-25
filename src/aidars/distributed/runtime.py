"""Execution runtime abstractions.

Defines the interface for running computational workloads inside isolated sandboxes.
"""

import abc
import asyncio
import glob
import json
import os
import psutil
import platform
import shutil
import sys
from typing import Any, Dict, Optional, Tuple

from aidars.distributed.models import RuntimeExecutionContext, WorkloadSpec


def _resolve_blender_executable(executable: str) -> str:
    """Resolve 'blender' to AIDAR_BLENDER_EXECUTABLE or standard Windows install path."""
    env_bin = os.environ.get("AIDAR_BLENDER_EXECUTABLE", "").strip()
    if env_bin and os.path.exists(env_bin):
        return env_bin
    found = shutil.which(executable)
    if found:
        return found
    if os.name == "nt":
        candidates = sorted(
            glob.glob(r"C:\Program Files\Blender Foundation\Blender*\blender.exe"),
            reverse=True,
        )
        if candidates:
            return candidates[0]
    return executable


def _resolve_ffmpeg_executable(executable: str) -> str:
    """Resolve 'ffmpeg' to AIDAR_FFMPEG_EXECUTABLE, PATH, or WinGet install path."""
    env_bin = os.environ.get("AIDAR_FFMPEG_EXECUTABLE", "").strip()
    if env_bin and os.path.exists(env_bin):
        return env_bin
    found = shutil.which(executable)
    if found:
        return found
    if os.name == "nt":
        local_appdata = os.environ.get("LOCALAPPDATA", "")
        search_patterns = [
            os.path.join(
                local_appdata,
                "Microsoft",
                "WinGet",
                "Packages",
                "Gyan.FFmpeg*",
                "**",
                "bin",
                "ffmpeg.exe",
            ),
            r"C:\ffmpeg\bin\ffmpeg.exe",
            r"C:\Program Files\ffmpeg\bin\ffmpeg.exe",
        ]
        for pattern in search_patterns:
            matches = sorted(glob.glob(pattern, recursive=True), reverse=True)
            if matches:
                return matches[0]
    return executable




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
            environment["AIDAR_RANK"] = str(context.rank)
            environment["AIDAR_WORLD_SIZE"] = str(context.world_size)

            from urllib.parse import urlsplit

            execution_context = context
            env = environment
            env["RANK"] = str(execution_context.rank)
            env["LOCAL_RANK"] = "0"
            env["WORLD_SIZE"] = str(execution_context.world_size)
            if execution_context.workers:
                leader_entry = next(
                    (w for w in execution_context.workers if w.get("rank") == 0),
                    execution_context.workers[0],
                )
                leader_url = str(leader_entry.get("endpoint_url", ""))
                parsed_leader = urlsplit(leader_url)
                master_host = parsed_leader.hostname or "127.0.0.1"
                master_port = str((parsed_leader.port or 8000) + 10000)
                env.setdefault("AIDAR_MASTER_ADDR", master_host)
                env.setdefault("AIDAR_MASTER_PORT", master_port)
                env.setdefault("MASTER_ADDR", master_host)
                env.setdefault("MASTER_PORT", master_port)
                peer_ips = [
                    urlsplit(str(w.get("endpoint_url", ""))).hostname or "127.0.0.1"
                    for w in execution_context.workers
                ]
                env.setdefault("AIDAR_PEER_IPS", ",".join(peer_ips))
            
        try:
            if spec.execution_spec:
                # M19.3: Secure structured execution
                exec_spec = spec.execution_spec
                env = os.environ.copy() if environment is None else environment
                if exec_spec.env:
                    env.update(exec_spec.env)
                
                cwd = exec_spec.cwd or workdir
                executable = exec_spec.executable
                args = list(exec_spec.args)
                if executable in ("python", "python3"):
                    executable = sys.executable

                if os.path.basename(executable).lower() in ("blender", "blender.exe"):
                    executable = _resolve_blender_executable(executable)
                    # Real Blender requires the input scene path on the CLI to end with '.blend'.
                    # Create a '.blend' alias inside inputs/ for any extensionless CAS scene argument
                    # while preserving the original ExecutionSpec contract for unit tests.
                    working_dir = getattr(context, "working_directory", None) or workdir
                    resolved_args: list[str] = []
                    for arg in args:
                        if (
                            isinstance(arg, str)
                            and arg.startswith("inputs/")
                            and not arg.endswith((".blend", ".py"))
                        ):
                            abs_src = os.path.join(str(working_dir), arg)
                            abs_blend = f"{abs_src}.blend"
                            if os.path.exists(abs_src) and not os.path.exists(abs_blend):
                                try:
                                    os.link(abs_src, abs_blend)
                                except OSError:
                                    shutil.copy2(abs_src, abs_blend)
                            if os.path.exists(abs_blend):
                                resolved_args.append(f"{arg}.blend")
                                continue
                        resolved_args.append(arg)
                    args = resolved_args

                if os.path.basename(executable).lower() in ("ffmpeg", "ffmpeg.exe"):
                    executable = _resolve_ffmpeg_executable(executable)

                self._process = await asyncio.create_subprocess_exec(
                    executable,
                    *args,
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

                if command.startswith("python ") or command.startswith("python3 "):
                    parts = command.split(" ", 1)
                    py_exe = f'"{sys.executable}"' if " " in sys.executable else sys.executable
                    command = f"{py_exe} {parts[1]}"
                elif command.startswith("blender ") or command.startswith("blender.exe "):
                    parts = command.split(" ", 1)
                    blender_exe = _resolve_blender_executable("blender")
                    b_exe = f'"{blender_exe}"' if " " in blender_exe else blender_exe
                    command = f"{b_exe} {parts[1]}"
                elif command.startswith("ffmpeg ") or command.startswith("ffmpeg.exe "):
                    parts = command.split(" ", 1)
                    ffmpeg_exe = _resolve_ffmpeg_executable("ffmpeg")
                    f_exe = f'"{ffmpeg_exe}"' if " " in ffmpeg_exe else ffmpeg_exe
                    command = f"{f_exe} {parts[1]}"

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
        """Terminate the subprocess tree and wait for it to exit.

        This is intentionally bounded control cleanup, not a workload
        execution timeout. It exists so ExecutionManager can guarantee that
        an explicit execution timeout or cancellation does not leave an
        orphan process behind while the workspace is being removed.
        """
        self._cancel_requested = True
        self._terminate_process_tree()
        process = self._process
        if process is None or process.returncode is not None:
            return
        try:
            await asyncio.wait_for(process.wait(), timeout=5.0)
        except asyncio.TimeoutError:
            try:
                proc = psutil.Process(process.pid)
                for child in proc.children(recursive=True):
                    try:
                        child.kill()
                    except psutil.NoSuchProcess:
                        pass
                try:
                    proc.kill()
                except psutil.NoSuchProcess:
                    pass
            except psutil.NoSuchProcess:
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=1.0)
            except (asyncio.TimeoutError, ProcessLookupError):
                pass

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
