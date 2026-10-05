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
from typing import Dict, Optional, Set

from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.checkpoint import CURRENT_CHECKPOINT_FORMAT_VERSION
from aidars.distributed.models import FailureCategory, RuntimeExecutionContext, WorkloadExecutionResult, WorkloadSpec
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
        self, spec: WorkloadSpec, worker_id: str, runtime: RuntimeAdapter,
        execution_context: Optional[RuntimeExecutionContext] = None,
    ) -> WorkloadExecutionResult:
        """Execute a workload from start to finish."""
        workload_id = spec.workload_id
        workdir = os.path.join(self.workloads_dir, workload_id)
        
        inputs_dir = os.path.join(workdir, "inputs")
        outputs_dir = os.path.join(workdir, "outputs")
        logs_dir = os.path.join(workdir, "logs")

        # 0. Abort any existing execution of this workload
        existing_runtime = self.active_runtimes.get(workload_id)
        if existing_runtime:
            logger.warning(f"Workload {workload_id} is already running. Aborting old execution.")
            try:
                await existing_runtime.checkpoint()
                # Wait briefly for it to terminate
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"Failed to abort old execution for {workload_id}: {e}")

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
                failure_category=FailureCategory.ASSET_STAGING_FAILURE,
            )

        # Write metadata
        with open(os.path.join(workdir, "metadata.json"), "w", encoding="utf-8") as f:
            f.write(spec.redacted_metadata_json(indent=2))

        # 2. Stage Dependencies
        # (This assumes the coordinator/client has already fetched missing hashes to CAS)
        staging_start = time.time()
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
                        staging_duration_seconds=time.time() - staging_start,
                        error_message=f"Failed to stage dependency {h}: {exc}",
                        failure_category=FailureCategory.ASSET_STAGING_FAILURE,
                    )

        if missing_local:
            return WorkloadExecutionResult(
                workload_id=workload_id,
                worker_id=worker_id,
                success=False,
                output_asset_hashes=set(),
                execution_duration_seconds=0.0,
                staging_duration_seconds=time.time() - staging_start,
                error_message=f"Missing dependencies locally: {missing_local}",
                # Treated as a sync-timing issue (dependencies not yet
                # propagated to this worker's CAS) rather than a genuinely
                # invalid dependency reference -- retryable.
                failure_category=FailureCategory.TEMPORARY_CAS_FAILURE,
            )
        staging_duration = time.time() - staging_start

        # 3. Execute with Timeout
        if spec.execution_spec and spec.execution_spec.timeout_seconds is not None:
            timeout_seconds = float(spec.execution_spec.timeout_seconds)
        else:
            timeout_seconds = spec.estimated_duration_seconds * 3.0
        start_time = time.time()

        self.active_runtimes[workload_id] = runtime

        timed_out = False
        runtime_exception: Optional[BaseException] = None
        try:
            success, stdout_snip, stderr_snip = await asyncio.wait_for(
                runtime.execute_with_context(spec, workdir, execution_context), timeout=timeout_seconds
            )
        except asyncio.TimeoutError:
            success = False
            stdout_snip = None
            stderr_snip = f"Execution timed out after {timeout_seconds} seconds"
            timed_out = True
        except Exception as exc:
            success = False
            stdout_snip = None
            stderr_snip = f"Runtime error: {exc}"
            runtime_exception = exc
        finally:
            self.active_runtimes.pop(workload_id, None)

        duration = time.time() - start_time
        checkpoint_requested = getattr(runtime, "_checkpoint_requested", False)
        runtime_supports_checkpointing = getattr(runtime, "supports_checkpointing", False)

        # M10.7: a checkpoint is only honored as real when the runtime
        # explicitly declares it can safely produce one. If checkpointing
        # was requested (e.g. for draining) but the runtime doesn't
        # support it, the abort is a genuine failure -- never a fabricated
        # successful "migration" (the pre-M10 behavior here treated ANY
        # checkpoint request as success regardless of runtime capability).
        was_checkpointed = checkpoint_requested and runtime_supports_checkpointing
        checkpoint_unsupported_abort = checkpoint_requested and not runtime_supports_checkpointing

        if checkpoint_unsupported_abort:
            success = False
            stderr_snip = (stderr_snip or "") + (
                "\nCheckpoint was requested but this runtime "
                f"({type(runtime).__name__}) does not support checkpointing "
                "(supports_checkpointing=False); execution was aborted and "
                "must be retried as a fresh attempt rather than resumed."
            )

        # 4. Ingest Outputs to CAS
        output_hashes: Set[str] = set()
        output_sizes: Dict[str, int] = {}
        ingestion_start = time.time()
        ingestion_failed = False
        if success:
            try:
                for root, _, files in os.walk(outputs_dir):
                    for filename in files:
                        filepath = os.path.join(root, filename)
                        # Use chunked CAS staging via thread pool to avoid blocking the event loop
                        try:
                            h = await asyncio.to_thread(self.cas.store_file, filepath)
                            output_hashes.add(h)
                            output_sizes[h] = os.path.getsize(filepath)
                        except Exception as e:
                            logger.error(f"Failed to store {filename}: {e}")
                            raise e
            except Exception as exc:
                success = False
                ingestion_failed = True
                stderr_snip = (stderr_snip or "") + f"\nOutput ingestion failed: {exc}"
        output_ingestion_duration = time.time() - ingestion_start

        # 4b. Output verification (M8.6): a successful process exit does not
        # by itself mean the workload produced its expected output -- e.g. a
        # renderer that exits 0 having written nothing. An adapter may
        # declare a minimum expected output-file count via
        # spec.parameters["expected_output_count"] (BlenderAdapter sets this
        # to the chunk's frame count). When the key is absent, no minimum is
        # enforced and behavior is unchanged from before this check existed
        # -- this keeps the change additive/opt-in for every existing caller
        # and test that never set it. A checkpointed execution is exempt: it
        # isn't expected to have produced final outputs yet.
        verification_start = time.time()
        verification_failed = False
        if success and not was_checkpointed and execution_context is None:
            expected_output_count = spec.parameters.get("expected_output_count")
            if expected_output_count is not None and len(output_hashes) < expected_output_count:
                success = False
                verification_failed = True
                stderr_snip = (stderr_snip or "") + (
                    f"\nOutput verification failed: expected at least "
                    f"{expected_output_count} output file(s), found {len(output_hashes)}."
                )
        verification_duration = time.time() - verification_start

        # M10.5: classify the terminal outcome. Order matters -- more
        # specific/earlier-detected causes take precedence over the
        # generic non-zero-exit default.
        failure_category: Optional[FailureCategory] = None
        final_success = success or was_checkpointed
        if not final_success:
            if checkpoint_unsupported_abort:
                failure_category = FailureCategory.WORKER_UNAVAILABLE
            elif timed_out:
                failure_category = FailureCategory.EXECUTION_TIMEOUT
            elif ingestion_failed:
                failure_category = FailureCategory.TEMPORARY_CAS_FAILURE
            elif verification_failed:
                failure_category = FailureCategory.ARTIFACT_VERIFICATION_FAILURE
            else:
                # Generic runtime exception or non-zero exit: no better
                # signal is available at this layer, so this is
                # conservatively classified as an application-level
                # failure (non-retryable) rather than assumed transient.
                failure_category = FailureCategory.EXECUTION_FAILURE

        # 5. Cleanup
        try:
            shutil.rmtree(workdir, ignore_errors=True)
        except Exception as e:
            logger.warning(f"Failed to cleanup workdir {workdir}: {e}")

        return WorkloadExecutionResult(
            workload_id=workload_id,
            worker_id=worker_id,
            success=final_success,
            output_asset_hashes=output_hashes,
            output_asset_sizes=output_sizes,
            execution_duration_seconds=duration,
            staging_duration_seconds=staging_duration,
            output_ingestion_duration_seconds=output_ingestion_duration,
            verification_duration_seconds=verification_duration,
            error_message=None if final_success else "Execution failed",
            stdout_snippet=stdout_snip,
            stderr_snippet=stderr_snip,
            was_checkpointed=was_checkpointed,
            checkpoint_hash=next(iter(output_hashes)) if (was_checkpointed and output_hashes) else None,
            checkpoint_runtime_type=type(runtime).__name__ if was_checkpointed else None,
            checkpoint_format_version=CURRENT_CHECKPOINT_FORMAT_VERSION if was_checkpointed else None,
            failure_category=failure_category,
        )

    async def checkpoint_workload(self, workload_id: str) -> bool:
        """Trigger a checkpoint for a running workload."""
        runtime = self.active_runtimes.get(workload_id)
        if runtime:
            await runtime.checkpoint()
            return True
        return False

    async def cancel_workload(self, workload_id: str) -> bool:
        """Forcefully cancel a running workload."""
        runtime = self.active_runtimes.get(workload_id)
        if runtime:
            await runtime.cancel()
            return True
        return False

