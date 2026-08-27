"""Workload supervisor and workspace isolation.

Manages the sandbox lifecycle, dependency staging, timeout enforcement,
and CAS artifact ingestion for a workload execution.
"""

import asyncio
import json
import logging
import os
import shutil
import time
from typing import Dict, Set

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.models import WorkloadExecutionResult, WorkloadSpec
from aidars.distributed.runtime import RuntimeAdapter

logger = logging.getLogger(__name__)


class ExecutionManager:
    """Supervises workload execution in an isolated sandbox."""

    def __init__(self, cas_adapter: LocalCASAdapter, workloads_dir: str) -> None:
        self.cas = cas_adapter
        self.workloads_dir = os.path.abspath(workloads_dir)
        self.active_runtimes: Dict[str, RuntimeAdapter] = {}
        os.makedirs(self.workloads_dir, exist_ok=True)

    async def execute_workload(
        self, spec: WorkloadSpec, worker_id: str, runtime: RuntimeAdapter
    ) -> WorkloadExecutionResult:
        """Execute a workload from start to finish."""
        workload_id = spec.workload_id
        workdir = os.path.join(self.workloads_dir, workload_id)
        
        inputs_dir = os.path.join(workdir, "inputs")
        outputs_dir = os.path.join(workdir, "outputs")
        logs_dir = os.path.join(workdir, "logs")

        # 1. Isolate Workspace
        try:
            if os.path.exists(workdir):
                shutil.rmtree(workdir)
            os.makedirs(inputs_dir)
            os.makedirs(outputs_dir)
            os.makedirs(logs_dir)
        except Exception as exc:
            return WorkloadExecutionResult(
                workload_id=workload_id,
                worker_id=worker_id,
                success=False,
                output_asset_hashes=set(),
                execution_duration_seconds=0.0,
                error_message=f"Failed to create workspace: {exc}",
            )

        # Write metadata
        with open(os.path.join(workdir, "metadata.json"), "w", encoding="utf-8") as f:
            f.write(spec.model_dump_json(indent=2))

        # 2. Stage Dependencies
        # (This assumes the coordinator/client has already fetched missing hashes to CAS)
        missing_local = []
        for h in spec.input_asset_hashes:
            if not self.cas.has_asset(h):
                missing_local.append(h)
            else:
                asset_path = self.cas.get_asset_path(h)
                dest_path = os.path.join(inputs_dir, h)
                try:
                    # In a real environment, hardlink or symlink to save space.
                    # Copying for Windows compatibility fallback.
                    try:
                        os.link(asset_path, dest_path)
                    except OSError:
                        shutil.copy2(asset_path, dest_path)
                except Exception as exc:
                    return WorkloadExecutionResult(
                        workload_id=workload_id,
                        worker_id=worker_id,
                        success=False,
                        output_asset_hashes=set(),
                        execution_duration_seconds=0.0,
                        error_message=f"Failed to stage dependency {h}: {exc}",
                    )
        
        if missing_local:
            return WorkloadExecutionResult(
                workload_id=workload_id,
                worker_id=worker_id,
                success=False,
                output_asset_hashes=set(),
                execution_duration_seconds=0.0,
                error_message=f"Missing dependencies locally: {missing_local}",
            )

        # 3. Execute with Timeout
        timeout_seconds = spec.estimated_duration_seconds * 3.0
        start_time = time.time()
        
        self.active_runtimes[workload_id] = runtime
        
        try:
            success, stdout_snip, stderr_snip = await asyncio.wait_for(
                runtime.execute(spec, workdir), timeout=timeout_seconds
            )
        except asyncio.TimeoutError:
            success = False
            stdout_snip = None
            stderr_snip = f"Execution timed out after {timeout_seconds} seconds"
        except Exception as exc:
            success = False
            stdout_snip = None
            stderr_snip = f"Runtime error: {exc}"
        finally:
            self.active_runtimes.pop(workload_id, None)
            
        duration = time.time() - start_time

        # 4. Ingest Outputs to CAS
        output_hashes: Set[str] = set()
        if success:
            try:
                for root, _, files in os.walk(outputs_dir):
                    for filename in files:
                        filepath = os.path.join(root, filename)
                        with open(filepath, "rb") as f:
                            data = f.read()
                        
                        # Use CAS staging for atomic commit and hashing
                        try:
                            h = self.cas.store_bytes(data)
                            output_hashes.add(h)
                        except Exception as e:
                            logger.error(f"Failed to store {filename}: {e}")
                            raise e
            except Exception as exc:
                success = False
                stderr_snip = (stderr_snip or "") + f"\nOutput ingestion failed: {exc}"

        # 5. Cleanup
        try:
            shutil.rmtree(workdir, ignore_errors=True)
        except Exception as e:
            logger.warning(f"Failed to cleanup workdir {workdir}: {e}")

        was_checkpointed = getattr(runtime, "_checkpoint_requested", False)
        
        return WorkloadExecutionResult(
            workload_id=workload_id,
            worker_id=worker_id,
            success=success or was_checkpointed,
            output_asset_hashes=output_hashes,
            execution_duration_seconds=duration,
            error_message=None if (success or was_checkpointed) else "Execution failed",
            stdout_snippet=stdout_snip,
            stderr_snippet=stderr_snip,
            was_checkpointed=was_checkpointed,
            checkpoint_hash=next(iter(output_hashes)) if (was_checkpointed and output_hashes) else None
        )

    async def checkpoint_workload(self, workload_id: str) -> bool:
        """Trigger a checkpoint for a running workload."""
        runtime = self.active_runtimes.get(workload_id)
        if runtime:
            await runtime.checkpoint()
            return True
        return False
