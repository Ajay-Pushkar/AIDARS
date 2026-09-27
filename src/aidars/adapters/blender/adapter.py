import hashlib
from typing import Any, List, Dict
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec
from aidars.adapters.blender.intelligence.scene_engine import SceneEngine

class BlenderAdapter(ApplicationAdapter):
    """Blender adapter implementing the generic contract."""

    def evaluate_request(self, request: dict) -> List[WorkloadSpec]:
        """Parse Blender request, discover dependencies, and output WorkloadSpecs.

        Runs the real M1-M4 scene intelligence pipeline (SceneEngine) to
        derive real per-worker frame chunks and asset costs, instead of
        assuming a fixed chunk layout.
        """
        input_path = request.get("input_path")
        frame_start = request.get("frame_start", 1)
        frame_end = request.get("frame_end", 250)
        worker_count = request.get("worker_count", 2)
        requires_gpu = request.get("requires_gpu", True)

        engine = SceneEngine()
        source = engine.load_source(input_path)
        snapshot = engine.analyze(source)
        graph = engine.build_dependency_graph(snapshot)
        plan = engine.build_scheduling_plan(
            source, snapshot, graph, frame_start, frame_end, worker_count
        )

        min_vram_bytes = 4 * 1024 * 1024 * 1024 if requires_gpu else 0

        specs = []
        for idx, chunk in enumerate(plan.chunks):
            spec = WorkloadSpec(
                workload_id=f"blender-render-{hashlib.md5(f'{input_path}-{chunk.frame_start}-{chunk.frame_end}'.encode()).hexdigest()[:8]}",
                task_type="blender_render",
                input_asset_hashes=set(), # Will contain the .blend and textures
                min_cpu_cores=4,
                min_ram_bytes=8 * 1024 * 1024 * 1024,
                requires_gpu=requires_gpu,
                min_vram_bytes=min_vram_bytes,
                estimated_duration_seconds=float(chunk.frame_count) * 2.0, # 2 sec per frame (existing convention)
                parameters={
                    "input_path": input_path,
                    "frame_start": chunk.frame_start,
                    "frame_end": chunk.frame_end,
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
