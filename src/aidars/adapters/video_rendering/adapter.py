import os
import hashlib
import uuid
from typing import List, Optional, Any
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec, ExecutionSpec, OutputVerificationPolicy


def resolve_davinci_or_video_input(input_path: str) -> str:
    """Resolve a DaVinci Resolve .drp project file or .dra archive directory
    to the underlying playable media clip or image sequence."""
    if not input_path:
        return input_path

    # Directory archive .dra
    if os.path.isdir(input_path) or input_path.lower().endswith(".dra"):
        media_exts = (".mp4", ".mov", ".mkv", ".mxf", ".avi", ".exr", ".dpx", ".png")
        for root, _, files in os.walk(input_path):
            for file in sorted(files):
                if file.lower().endswith(media_exts):
                    return os.path.join(root, file)
        return input_path

    # Project file .drp (ZIP archive containing project.db and media references)
    if input_path.lower().endswith(".drp") and os.path.isfile(input_path):
        import zipfile
        try:
            if zipfile.is_zipfile(input_path):
                with zipfile.ZipFile(input_path, "r") as zf:
                    media_exts = (".mp4", ".mov", ".mkv", ".mxf", ".avi")
                    for name in sorted(zf.namelist()):
                        if name.lower().endswith(media_exts):
                            extract_dir = os.path.join(os.path.dirname(input_path), "_drp_extracted")
                            os.makedirs(extract_dir, exist_ok=True)
                            return zf.extract(name, extract_dir)
        except Exception:
            pass

    return input_path


class FFmpegAdapter(ApplicationAdapter):
    """FFmpeg video processing adapter implementing the generic contract.
    
    Executes real FFmpeg binary workloads, physically tracking video
    assets through CAS.
    """
    
    def __init__(self, asset_manager=None) -> None:
        self.asset_manager = asset_manager

    def validate(self, request: dict) -> bool:
        """Validate FFmpeg specific request."""
        if "input_video_path" not in request:
            return False
        return True

    def evaluate_request(self, request: dict) -> List[WorkloadSpec]:
        """Parse FFmpeg request, calculate requirements, and output WorkloadSpec."""
        raw_video_path = request.get("input_video_path")
        input_video_path = resolve_davinci_or_video_input(raw_video_path) if raw_video_path else None
        output_format = request.get("output_format", "mp4")
        requires_gpu = request.get("requires_gpu", False)
        
        # Physical asset resolution
        input_asset_hashes = set()
        video_hash = None
        
        if input_video_path and os.path.exists(input_video_path) and os.path.isfile(input_video_path):
            hasher = hashlib.sha256()
            with open(input_video_path, "rb") as f:
                while chunk_data := f.read(65536):
                    hasher.update(chunk_data)
            video_hash = hasher.hexdigest()
            input_asset_hashes.add(video_hash)
            
            if self.asset_manager:
                self.asset_manager.upload_asset_file(input_video_path, video_hash)
                
        job_id = f"ffmpeg-{uuid.uuid4().hex[:16]}"
        
        # CPU/RAM/GPU/VRAM requirements estimation
        min_vram_bytes = 2 * 1024 * 1024 * 1024 if requires_gpu else 0
        min_ram_bytes = int(request.get("min_ram_bytes", 4 * 1024 * 1024 * 1024))
        min_cpu_cores = int(request.get("min_cpu_cores", 4))

        params = {
            "input_video_path": input_video_path or "input.mp4",
            "video_hash": video_hash,
            "output_format": output_format,
        }
        for k, v in request.items():
            if k not in params and k not in ("min_ram_bytes", "min_cpu_cores", "requires_gpu"):
                params[k] = v

        worker_count = int(request.get("worker_count", 1))
        workers = request.get("workers") or []
        total_duration = float(request.get("duration", request.get("total_duration", 60.0)))

        if (worker_count > 1 or len(workers) > 1) and total_duration > 0:
            from aidars.distributed.placement import compute_worker_throughput_score
            n_workers = len(workers) if len(workers) > 1 else worker_count
            scores = []
            if workers:
                for w in workers:
                    prof = getattr(w, "resource_profile", w)
                    scores.append(compute_worker_throughput_score(prof, requires_gpu=requires_gpu))
            else:
                scores = [1.0] * n_workers

            total_score = sum(scores) or 1.0
            durations = [total_duration * (s / total_score) for s in scores]

            specs = []
            curr_start = 0.0
            for idx, dur in enumerate(durations):
                chunk_params = dict(params)
                chunk_params["start_time"] = round(curr_start, 3)
                chunk_params["duration"] = round(dur, 3)
                chunk_params["chunk_index"] = idx
                spec = WorkloadSpec(
                    workload_id=f"{job_id}-chunk-{idx}",
                    job_id=job_id,
                    task_type="video_transcoding",
                    input_asset_hashes=input_asset_hashes,
                    min_cpu_cores=min_cpu_cores,
                    min_ram_bytes=min_ram_bytes,
                    requires_gpu=requires_gpu,
                    min_vram_bytes=min_vram_bytes,
                    estimated_duration_seconds=float(dur) * 0.5,
                    parameters=chunk_params,
                )
                spec.execution_spec = self.build_execution_spec(spec)
                specs.append(spec)
                curr_start += dur
            return specs
        
        spec = WorkloadSpec(
            workload_id=f"{job_id}-chunk-0",
            job_id=job_id,
            task_type="video_transcoding",
            input_asset_hashes=input_asset_hashes,
            min_cpu_cores=min_cpu_cores,
            min_ram_bytes=min_ram_bytes,
            requires_gpu=requires_gpu,
            min_vram_bytes=min_vram_bytes,
            estimated_duration_seconds=float(request.get("estimated_duration_seconds", 30.0)),
            parameters=params,
        )
        spec.execution_spec = self.build_execution_spec(spec)
        return [spec]
        
    def build_execution_spec(self, spec: WorkloadSpec) -> ExecutionSpec:
        """Build the generic subprocess execution specification for FFmpeg."""
        input_hashes = sorted(spec.input_asset_hashes)
        input_hash = input_hashes[0] if input_hashes else "missing_input"
        codec = str(spec.parameters.get("codec", "libx264"))
        resolution = str(spec.parameters.get("resolution", "1920x1080"))
        output_format = str(spec.parameters.get("output_format", "mp4"))
        ffmpeg_exec = str(
            spec.parameters.get("ffmpeg_executable")
            or os.environ.get("AIDAR_FFMPEG_EXECUTABLE")
            or "ffmpeg"
        )

        start_time = spec.parameters.get("start_time")
        duration = spec.parameters.get("duration")

        args: List[str] = ["-y"]
        if start_time is not None:
            args.extend(["-ss", str(start_time)])

        args.extend(["-i", f"inputs/{input_hash}"])

        if duration is not None and float(duration) > 0:
            args.extend(["-t", str(duration)])

        args.extend([
            "-c:v", codec,
            "-vf", f"scale={resolution}",
            f"outputs/output_{spec.workload_id}.{output_format}",
        ])

        return ExecutionSpec(
            command=ffmpeg_exec,
            executable=ffmpeg_exec,
            args=args,
            env={"AIDAR_ADAPTER": "ffmpeg"},
        )

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        return OutputVerificationPolicy(
            expected_output_count=1 # Expect transcoded video output
        )
