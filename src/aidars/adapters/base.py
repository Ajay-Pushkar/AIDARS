import abc
from typing import Any, List, Dict
from aidars.distributed.models import ExecutionDescription, ExecutionShape, WorkloadSpec, ExecutionSpec, OutputVerificationPolicy

class ApplicationAdapter(abc.ABC):
    """Generic contract for bridging domain-specific requests to M7/M6/M5.
    
    Application -> Adapter -> WorkloadSpec -> M7 -> M6 -> M5 -> Worker
    """
    
    @abc.abstractmethod
    def validate(self, request: Any) -> bool:
        """Validate application-specific inputs."""
        pass

    @abc.abstractmethod
    def evaluate_request(self, request: Any) -> list[WorkloadSpec]:
        """Parse an app request, discover dependencies, and output WorkloadSpecs."""
        pass

    def describe_request(self, request: Any) -> ExecutionDescription:
        """Additive execution-plan boundary for adapters.

        Existing adapters keep returning WorkloadSpecs. They therefore retain
        the established defaults, while adapters that need a pipeline or a
        distributed-native group can override this method and declare it.
        """
        workloads = self.evaluate_request(request)
        if any(workload.depends_on for workload in workloads):
            shape = ExecutionShape.PIPELINE
        elif len(workloads) > 1:
            shape = ExecutionShape.TASK_SPLIT
        else:
            shape = ExecutionShape.SINGLE_MACHINE
        return ExecutionDescription(execution_shape=shape, workloads=workloads)
        
    @abc.abstractmethod
    def build_execution_spec(self, workload: WorkloadSpec) -> ExecutionSpec:
        """Construct the secure, structured parameters (executable, argv) for the Runtime."""
        pass

    @abc.abstractmethod
    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        """Define verification rules independent of execution."""
        pass
