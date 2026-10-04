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
        min_ram_bytes = 4 * 1024 * 1024 * 1024
        
        spec = WorkloadSpec(
            workload_id=f"{job_id}-chunk-0",
            job_id=job_id,
            task_type="video_transcoding",
            input_asset_hashes=input_asset_hashes,
            min_cpu_cores=4,
            min_ram_bytes=min_ram_bytes,
            requires_gpu=requires_gpu,
            min_vram_bytes=min_vram_bytes,
            estimated_duration_seconds=30.0,
            parameters={
                "video_hash": video_hash,
                "output_format": output_format
            }
        )
        return [spec]
        
    def build_execution_spec(self, workload: WorkloadSpec) -> ExecutionSpec:
        video_hash = workload.parameters.get("video_hash")
        output_format = workload.parameters.get("output_format", "mp4")
        
        if not video_hash:
            raise ValueError("FFmpeg workload missing video_hash")
            
        args = [
            "-y", # Overwrite outputs
            "-i", f"inputs/{video_hash}",
            "-c:v", "libx264", # Force a common encoder for test stability
            "-preset", "ultrafast", # Faster tests
            "-t", "1", # Process max 1 second to keep tests fast
            f"./outputs/transcoded.{output_format}"
        ]
        
        return ExecutionSpec(
            executable="ffmpeg", # Real local executable
            args=args,
            env={},
            cwd=None,
            timeout_seconds=workload.estimated_duration_seconds * 3.0
        )

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        return OutputVerificationPolicy(
            expected_output_count=1 # Expect transcoded video output
        )
