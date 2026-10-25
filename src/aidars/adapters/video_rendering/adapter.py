import os
import hashlib
import uuid
from typing import List
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec, ExecutionSpec, OutputVerificationPolicy

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
        input_video_path = request.get("input_video_path")
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
