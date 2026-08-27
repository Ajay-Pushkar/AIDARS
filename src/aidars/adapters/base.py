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
        """Interpret workload outputs from the generic runtime."""
        pass
