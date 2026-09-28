import hashlib
from typing import Any, List, Dict, Optional
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec
from aidars.adapters.blender.intelligence.scene_engine import SceneEngine
from aidars.core.assets.manager import AssetManager

class BlenderAdapter(ApplicationAdapter):
    """Blender adapter implementing the generic contract."""

    def __init__(self, asset_manager: Optional[AssetManager] = None) -> None:
        # Optional: bound to the Master's CAS via the existing AssetManager
        # abstraction (never a raw LocalCASAdapter -- see core/assets/manager.py).
        # When absent, evaluate_request() still computes real M4 hashes for
        # WorkloadSpec.input_asset_hashes; it just has no legitimate CAS to
        # ingest into, so it doesn't fabricate one.
        self.asset_manager = asset_manager

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

        # M4: resolve real physical assets and their SHA-256 hashes. Only
        # successfully resolved assets contribute a hash; embedded/missing
        # assets are never fabricated (AssetRecord.sha256 stays None for
        # them).
        asset_records = engine.resolve_required_assets(snapshot, graph, input_path)
        resolved_paths_by_hash = {
            record.sha256: record.source_path
            for record in asset_records
            if record.sha256 and record.source_path
        }
        input_asset_hashes = set(resolved_paths_by_hash.keys())

        # If a Master-side AssetManager was supplied, physically ingest every
        # resolved asset into CAS now, so the hashes above are guaranteed to
        # be fetchable by a worker (Phase 4B.1-4B.3) rather than merely
        # computed. When no AssetManager is available, this adapter has no
        # legitimate CAS to ingest into, so it deliberately does nothing
        # rather than fabricating one.
        if self.asset_manager is not None:
            for sha256, source_path in resolved_paths_by_hash.items():
                self.asset_manager.upload_asset_file(source_path, sha256)

        min_vram_bytes = 4 * 1024 * 1024 * 1024 if requires_gpu else 0

        specs = []
        for idx, chunk in enumerate(plan.chunks):
            spec = WorkloadSpec(
                workload_id=f"blender-render-{hashlib.md5(f'{input_path}-{chunk.frame_start}-{chunk.frame_end}'.encode()).hexdigest()[:8]}",
                task_type="blender_render",
                input_asset_hashes=input_asset_hashes,
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
