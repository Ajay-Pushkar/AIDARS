import hashlib
import os
import shutil
import uuid
from typing import Any, List, Dict, Optional
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec, ExecutionSpec, OutputVerificationPolicy
from aidars.adapters.blender.intelligence.scene_engine import SceneEngine
from aidars.core.assets.manager import AssetManager

BLENDER_UNIVERSAL_PREPASS_CODE = (
    "import bpy, os, sys\n"
    "scene = bpy.context.scene\n"
    "total_cpus = os.cpu_count() or 4\n"
    "cpu_threads = max(1, total_cpus - 2) if total_cpus >= 8 else max(1, total_cpus - 1)\n"
    "scene.render.threads_mode = 'FIXED'\n"
    "scene.render.threads = cpu_threads\n"
    "scene.render.use_persistent_data = False\n"
    "free_ram_gb = 8.0\n"
    "try:\n"
    "    import psutil\n"
    "    free_ram_gb = psutil.virtual_memory().available / float(1024**3)\n"
    "except Exception:\n"
    "    pass\n"
    "adaptive_tile = 256 if free_ram_gb < 5.0 else (512 if free_ram_gb < 12.0 else 1024)\n"
    "if hasattr(scene, 'cycles'):\n"
    "    if hasattr(scene.cycles, 'use_auto_tile'):\n"
    "        scene.cycles.use_auto_tile = True\n"
    "    if hasattr(scene.cycles, 'tile_size'):\n"
    "        scene.cycles.tile_size = adaptive_tile\n"
    "scene.render.use_simplify = False\n"
    "if scene.render.resolution_x * (scene.render.resolution_percentage / 100.0) < 1024:\n"
    "    scene.render.resolution_percentage = 100\n"
    "if hasattr(scene.render, 'use_sequencer'):\n"
    "    scene.render.use_sequencer = False\n"
    "scene.render.image_settings.file_format = 'PNG'\n"
    "scene.render.use_file_extension = True\n"
    "if hasattr(scene, 'cycles') and hasattr(scene.cycles, 'denoising_prefilter'):\n"
    "    scene.cycles.denoising_prefilter = 'ACCURATE'\n"
    "if hasattr(scene, 'cycles') and hasattr(scene.cycles, 'use_light_tree'):\n"
    "    scene.cycles.use_light_tree = False\n"
    "if scene.world and hasattr(scene.world, 'cycles') and hasattr(scene.world.cycles, 'sampling_method'):\n"
    "    scene.world.cycles.sampling_method = 'NONE'\n"
    "if scene.world and scene.world.node_tree:\n"
    "    for node in scene.world.node_tree.nodes:\n"
    "        if node.type in ('TEX_IMAGE', 'TEX_ENVIRONMENT'):\n"
    "            img = getattr(node, 'image', None)\n"
    "            if img is None or img.size[0] == 0 or not getattr(img, 'has_data', True):\n"
    "                node.mute = True\n"
    "for obj in scene.objects:\n"
    "    if obj.type == 'MESH':\n"
    "        obj.visible_shadow = True\n"
    "        obj.visible_glossy = True\n"
    "        obj.visible_diffuse = True\n"
    "if hasattr(scene, 'cycles'):\n"
    "    rank = int(os.environ.get('AIDAR_SAMPLE_FUSION_RANK', -1))\n"
    "    scene.cycles.seed = (1001 + rank * 997) if rank >= 0 else 0\n"
    "    scene.cycles.use_animated_seed = False\n"
    "    orig_samples = getattr(scene.cycles, 'samples', 64)\n"
    "    scene.cycles.samples = min(orig_samples, 64) if orig_samples > 0 else 64\n"
    "    if hasattr(scene.cycles, 'use_adaptive_sampling'):\n"
    "        scene.cycles.use_adaptive_sampling = True\n"
    "    if hasattr(scene.cycles, 'adaptive_threshold'):\n"
    "        scene.cycles.adaptive_threshold = 0.02\n"
    "if scene.camera is None:\n"
    "    found_cam = next((s.camera for s in bpy.data.scenes if s.camera), None)\n"
    "    if not found_cam:\n"
    "        found_cam = next((obj for obj in bpy.data.objects if obj.type == 'CAMERA'), None)\n"
    "    if found_cam:\n"
    "        scene.camera = found_cam\n"
    "    else:\n"
    "        cam_data = bpy.data.cameras.new('FallbackCamera')\n"
    "        cam_obj = bpy.data.objects.new('FallbackCamera', cam_data)\n"
    "        scene.collection.objects.link(cam_obj)\n"
    "        scene.camera = cam_obj\n"
)


def prepare_self_contained_blend(blend_path: str) -> str:
    """Pack all external textures/dependencies into the .blend file so every remote worker receives 100% self-contained assets."""
    if not blend_path or not os.path.exists(blend_path) or not os.path.isfile(blend_path):
        return blend_path
    try:
        import bpy
        bpy.ops.wm.open_mainfile(filepath=blend_path)
        bpy.ops.file.make_paths_absolute()
        bpy.ops.object.make_local(type='ALL')
        bpy.ops.file.pack_all()
        packed_path = f"{os.path.splitext(blend_path)[0]}_packed.blend"
        bpy.ops.wm.save_as_mainfile(filepath=packed_path)
        return packed_path
    except Exception:
        pass

    try:
        blender_bin = os.environ.get("AIDAR_BLENDER_EXECUTABLE") or shutil.which("blender")
        if blender_bin and os.path.exists(blender_bin):
            packed_path = f"{os.path.splitext(blend_path)[0]}_packed.blend"
            shutil.copy2(blend_path, packed_path)
            cmd = [
                blender_bin, "-b", packed_path,
                "--python-expr",
                "import bpy; bpy.ops.file.make_paths_absolute(); bpy.ops.object.make_local(type='ALL'); bpy.ops.file.pack_all(); bpy.ops.wm.save_mainfile()"
            ]
            import subprocess
            subprocess.run(cmd, capture_output=True, timeout=60.0)
            if os.path.exists(packed_path) and os.path.getsize(packed_path) > 0:
                return packed_path
    except Exception:
        pass

    return blend_path


def fuse_sample_buffers(buffers: List[Any], weights: Optional[List[float]] = None) -> Any:
    """Cooperative N-PC Monte Carlo Sample Fusion. Fuses float32 image buffers weighted by throughput."""
    if not buffers:
        return None
    try:
        import numpy as np
        if weights is None or len(weights) != len(buffers):
            weights = [1.0 / len(buffers)] * len(buffers)
        total_weight = sum(weights)
        norm_weights = [w / total_weight for w in weights]
        fused = np.zeros_like(buffers[0], dtype=np.float32)
        for buf, w in zip(buffers, norm_weights):
            fused += np.asarray(buf, dtype=np.float32) * float(w)
        return fused
    except Exception:
        return buffers[0]


class BlenderAdapter(ApplicationAdapter):
    """Blender adapter implementing the generic contract."""

    def __init__(self, asset_manager: Optional[AssetManager] = None) -> None:
        # Optional: bound to the Master's CAS via the existing AssetManager
        # abstraction (never a raw LocalCASAdapter -- see core/assets/manager.py).
        # When absent, evaluate_request() still computes real M4 hashes for
        # WorkloadSpec.input_asset_hashes; it just has no legitimate CAS to
        # ingest into, so it doesn't fabricate one.
        self.asset_manager = asset_manager

    def validate(self, request: dict) -> bool:
        """Validate Blender specific request."""
        input_path = request.get("input_path")
        if not input_path:
            return False
        return True

    def evaluate_request(self, request: dict) -> List[WorkloadSpec]:
        """Parse Blender request, discover dependencies, and output WorkloadSpecs.

        Runs the real M1-M4 scene intelligence pipeline (SceneEngine) to
        derive real per-worker frame chunks and asset costs, instead of
        assuming a fixed chunk layout.
        """
        input_path = request.get("input_path")
        frame_start = request.get("frame_start", 1)
        frame_end = request.get("frame_end", 250)
        worker_count_param = request.get("worker_count")
        chunk_count_param = request.get("chunk_count")
        requires_gpu = request.get("requires_gpu", False)
        min_cpu_cores = request.get("min_cpu_cores", 1)
        estimated_frame_duration = float(request.get("estimated_frame_duration_seconds", 2.0))

        if worker_count_param is None:
            effective_workers = 2
            adaptive = False
        elif worker_count_param == "auto":
            effective_workers = 1
            adaptive = True
        else:
            effective_workers = max(1, int(worker_count_param))
            adaptive = False

        if chunk_count_param is not None:
            chunk_count = int(chunk_count_param)
            adaptive = False
        else:
            chunk_count = None

        engine = SceneEngine()
        source = engine.load_source(input_path)
        snapshot = engine.analyze(source)
        graph = engine.build_dependency_graph(snapshot)
        plan = engine.build_scheduling_plan(
            source,
            snapshot,
            graph,
            frame_start,
            frame_end,
            effective_workers,
            chunk_count=chunk_count,
            adaptive=adaptive,
            estimated_frame_duration_seconds=estimated_frame_duration,
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
        
        # M14.6: Ensure the primary input file is hashed and included.
        # This is critical so the worker downloads the .blend file itself, not just its dependencies.
        input_file_hash = None
        staged_blend_path = input_path
        if input_path:
            if os.path.exists(input_path) and os.path.isfile(input_path):
                staged_blend_path = prepare_self_contained_blend(input_path)
                hasher = hashlib.sha256()
                with open(staged_blend_path, "rb") as f:
                    while chunk_data := f.read(65536):
                        hasher.update(chunk_data)
                input_file_hash = hasher.hexdigest()
                resolved_paths_by_hash[input_file_hash] = staged_blend_path

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

        total_asset_bytes = sum(record.size_bytes or 0 for record in asset_records)
        base_runtime_ram = 256 * 1024 * 1024
        evidenced_ram = int(base_runtime_ram + total_asset_bytes * 1.5)
        min_ram_bytes = request.get(
            "min_ram_bytes",
            evidenced_ram if total_asset_bytes > 0 else 256 * 1024 * 1024,
        )
        min_vram_bytes = request.get(
            "min_vram_bytes",
            2 * 1024 * 1024 * 1024 if requires_gpu else 0,
        )

        # M8.1/workload-identity: one job_id per evaluate_request() call,
        # shared across every chunk it returns, and used as the workload_id
        # prefix.
        job_id = f"job-{uuid.uuid4().hex[:16]}"

        remap_script_hash_param = request.get("remap_script_hash")
        if remap_script_hash_param:
            input_asset_hashes.add(str(remap_script_hash_param))

        total_frames = max(1, frame_end - frame_start + 1)
        cooperative_fusion = (
            request.get("cooperative_sample_fusion") is True
            or (request.get("sample_fusion") is True and effective_workers > 1)
            or (total_frames < effective_workers and effective_workers > 1 and request.get("worker_count") is not None and not adaptive)
        )

        specs = []
        if cooperative_fusion:
            for rank in range(effective_workers):
                params = {
                    "input_path": input_path,
                    "frame_start": frame_start,
                    "frame_end": frame_end,
                    "chunk_index": rank,
                    "sample_fusion_rank": rank,
                    "sample_fusion_worker_count": effective_workers,
                    "expected_output_count": total_frames,
                    "job_frame_start": frame_start,
                    "job_frame_end": frame_end,
                }
                if remap_script_hash_param:
                    params["remap_script_hash"] = str(remap_script_hash_param)
                if "engine" in request:
                    params["engine"] = request["engine"]
                if "blender_executable" in request:
                    params["blender_executable"] = request["blender_executable"]

                if input_file_hash:
                    params["input_file_hash"] = input_file_hash
                    params["command"] = (
                        f"blender -b inputs/{input_file_hash} "
                        f"-o //../outputs/frame_#### "
                        f"-s {frame_start} -e {frame_end} -a"
                    )

                spec = WorkloadSpec(
                    workload_id=f"{job_id}-chunk-{rank}",
                    job_id=job_id,
                    task_type="blender_render",
                    input_asset_hashes=input_asset_hashes,
                    min_cpu_cores=min_cpu_cores,
                    min_ram_bytes=min_ram_bytes,
                    requires_gpu=requires_gpu,
                    min_vram_bytes=min_vram_bytes,
                    estimated_duration_seconds=float(total_frames) * estimated_frame_duration,
                    parameters=params,
                )
                if input_file_hash:
                    try:
                        spec.execution_spec = self.build_execution_spec(spec)
                    except ValueError:
                        pass
                specs.append(spec)
        else:
            for idx, chunk in enumerate(plan.chunks):
                params = {
                    "input_path": input_path,
                    "frame_start": chunk.frame_start,
                    "frame_end": chunk.frame_end,
                    "chunk_index": idx,
                    # M8.6: one output file expected per frame in this chunk.
                    "expected_output_count": chunk.frame_count,
                    # M8.7: the job's overall requested range, so a
                    # frame-coverage check can be done per-chunk without
                    # needing the original request dict.
                    "job_frame_start": frame_start,
                    "job_frame_end": frame_end,
                }
                if remap_script_hash_param:
                    params["remap_script_hash"] = str(remap_script_hash_param)
                if "engine" in request:
                    params["engine"] = request["engine"]
                if "blender_executable" in request:
                    params["blender_executable"] = request["blender_executable"]

                if "min_ram_bytes" not in request and total_asset_bytes == 0:
                    params["resource_requirement_confidence"] = "unverified"
                    params["ram_requirement_state"] = "undeclared_unconstrained"

                # Store hash for execution spec builder
                if input_file_hash:
                    params["input_file_hash"] = input_file_hash
                    # M14.6: Populate the 'command' parameter for GenericSubprocessRuntime.
                    # Kept for backward compatibility until M19.3 migrates to ExecutionSpec.
                    params["command"] = (
                        f"blender -b inputs/{input_file_hash} "
                        f"-o //../outputs/frame_#### "
                        f"-s {chunk.frame_start} -e {chunk.frame_end} -a"
                    )

                spec = WorkloadSpec(
                    workload_id=f"{job_id}-chunk-{idx}",
                    job_id=job_id,
                    task_type="blender_render",
                    input_asset_hashes=input_asset_hashes,
                    min_cpu_cores=min_cpu_cores,
                    min_ram_bytes=min_ram_bytes,
                    requires_gpu=requires_gpu,
                    min_vram_bytes=min_vram_bytes,
                    estimated_duration_seconds=float(chunk.frame_count) * estimated_frame_duration,
                    parameters=params,
                )
                if input_file_hash:
                    try:
                        spec.execution_spec = self.build_execution_spec(spec)
                    except ValueError:
                        pass
                specs.append(spec)

        return specs
        
    def build_execution_spec(self, spec: WorkloadSpec) -> ExecutionSpec:
        scene_hash = (
            spec.parameters.get("input_file_hash")
            or spec.parameters.get("scene_hash")
        )
        if not scene_hash:
            raise ValueError("Blender workload missing input_file_hash")

        start_frame = spec.parameters.get("frame_start", 1)
        end_frame = spec.parameters.get("frame_end", 1)

        blender_exec = (
            spec.parameters.get("blender_executable")
            or os.environ.get("AIDAR_BLENDER_EXECUTABLE")
            or "blender"
        )
        remap_script_hash = spec.parameters.get("remap_script_hash")
        engine = spec.parameters.get("engine")

        args = ["-b", f"inputs/{scene_hash}"]
        if engine:
            args.extend(["-E", str(engine)])
        if remap_script_hash:
            args.extend(["--python", f"inputs/{remap_script_hash}"])
        args.extend(["--python-expr", BLENDER_UNIVERSAL_PREPASS_CODE])
        args.extend([
            "-o", "//../outputs/frame_######",
            "-s", str(start_frame),
            "-e", str(end_frame),
            "-a",
        ])
        env = {}
        if "sample_fusion_rank" in spec.parameters:
            env["AIDAR_SAMPLE_FUSION_RANK"] = str(spec.parameters["sample_fusion_rank"])

        timeout_seconds = (
            float(spec.parameters.get("timeout_seconds", 14400.0))
            if "timeout_seconds" in spec.parameters
            else 14400.0
        )
        return ExecutionSpec(
            command=str(blender_exec),
            executable=str(blender_exec),
            args=args,
            env=env,
            cwd=None,
            timeout_seconds=timeout_seconds,
        )

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        return OutputVerificationPolicy(
            expected_output_count=workload.parameters.get("expected_output_count")
        )
