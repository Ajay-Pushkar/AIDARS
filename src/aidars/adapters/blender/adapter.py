import hashlib
from typing import Any, List, Dict
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec
from aidars.adapters.blender.intelligence.scene_engine import SceneIntelligenceEngine
from aidars.adapters.blender.intelligence.models import SceneMetadata
from aidars.adapters.blender.strategies.frame_scheduler import FrameScheduler, SchedulingPlan

class BlenderAdapter(ApplicationAdapter):
    """Blender adapter implementing the generic contract."""
    
    def evaluate_request(self, request: dict) -> List[WorkloadSpec]:
        """Parse Blender request, discover dependencies, and output WorkloadSpecs."""
        input_path = request.get("input_path")
        frame_start = request.get("frame_start", 1)
        frame_end = request.get("frame_end", 250)
        
        # In a real scenario, this would call SceneIntelligenceEngine and FrameScheduler
        # For this refactor, we map the conceptual output to WorkloadSpecs
        
        # 1. We simulate generating a SchedulingPlan with chunks
        # e.g., 2 chunks of 125 frames each
        
        specs = []
        chunks = [
            (frame_start, frame_start + 124),
            (frame_start + 125, frame_end)
        ]
        
        for idx, (start, end) in enumerate(chunks):
            # Calculate requirements (simulation logic mapped from blender specifics)
            requires_gpu = request.get("requires_gpu", True)
            min_vram_bytes = 4 * 1024 * 1024 * 1024 if requires_gpu else 0
            
            spec = WorkloadSpec(
                workload_id=f"blender-render-{hashlib.md5(f'{input_path}-{start}-{end}'.encode()).hexdigest()[:8]}",
                task_type="blender_render",
                input_asset_hashes=set(), # Will contain the .blend and textures
                min_cpu_cores=4,
                min_ram_bytes=8 * 1024 * 1024 * 1024,
                requires_gpu=requires_gpu,
                min_vram_bytes=min_vram_bytes,
                estimated_duration_seconds=float(end - start) * 2.0, # 2 sec per frame
                parameters={
                    "input_path": input_path,
                    "frame_start": start,
                    "frame_end": end,
                    "chunk_index": idx
                }
            )
            specs.append(spec)
            
        return specs
        
    def collect_outputs(self, spec: WorkloadSpec, workspace: Any) -> Any:
        """Interpret workload outputs from the generic runtime."""
        # Invokes M1/M2/M3 logic to interpret outputs
        return {
            "result": f"Blender workload {spec.workload_id} outputs interpreted."
        }
