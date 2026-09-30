import abc
from typing import Any, List, Dict
from aidars.distributed.models import WorkloadSpec

class ApplicationAdapter(abc.ABC):
    """Generic contract for bridging domain-specific requests to M7/M6/M5.
    
    Application -> Adapter -> WorkloadSpec -> M7 -> M6 -> M5 -> Worker
    """
    
    @abc.abstractmethod
    def evaluate_request(self, request: Any) -> list[WorkloadSpec]:
        """Parse an app request, discover dependencies, and output WorkloadSpecs."""
        pass
        
    @abc.abstractmethod
    def collect_outputs(self, spec: WorkloadSpec, workspace: Any) -> Any:
        """Interpret workload outputs from the generic runtime.

        M8 architecture note: this method is NOT called by the live
        distributed execution pipeline. ExecutionManager.execute_workload()
        (worker-side) performs its own generic, adapter-unaware output
        ingestion (walk outputs_dir, hash + commit to CAS) because
        ApplicationAdapter instances are constructed Master/submission-side
        (e.g. BlenderAdapter needs SceneEngine/AssetManager, which are
        Master-side concepts) and have no reachable instance on the worker
        without an RPC round-trip that doesn't exist. M8's output
        verification (spec.parameters["expected_output_count"], checked
        generically in execution.py) and frame coverage
        (adapters/blender/frame_coverage.py, computed from persisted
        WorkloadSpec/WorkloadExecutionResult facts after the fact) are the
        two mechanisms that actually run today; this method is kept as
        part of the published adapter contract but is otherwise dead code
        along the execution path.
        """
        pass
