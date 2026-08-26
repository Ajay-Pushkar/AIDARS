"""Hardware resource profiling for distributed workers.

Collects CPU, RAM, GPU, and VRAM metrics to build the WorkerResourceProfile.
"""

import socket
import time
from typing import Optional, Set

import psutil

from aidars.distributed.models import WorkerResourceProfile

# Try to import pynvml for NVIDIA GPU profiling. If unavailable, GPUs are marked unavailable.
try:
    import pynvml
    _HAS_PYNVML = True
except ImportError:
    _HAS_PYNVML = False


class WorkerResourceMonitor:
    """Collects hardware metrics from the local machine."""

    def __init__(self, worker_id: str, endpoint_url: str, ip_address: str) -> None:
        self.worker_id = worker_id
        self.endpoint_url = endpoint_url
        self.ip_address = ip_address
        self._gpu_initialized = False

        if _HAS_PYNVML:
            try:
                pynvml.nvmlInit()
                self._gpu_initialized = True
            except Exception:
                self._gpu_initialized = False

    def get_profile(
        self, active_workload_count: int = 0, local_cached_hashes: Optional[Set[str]] = None
    ) -> WorkerResourceProfile:
        """Capture a real-time snapshot of the worker's hardware resources."""
        cpu_cores = psutil.cpu_count(logical=True) or 1
        cpu_util = psutil.cpu_percent(interval=None)

        mem = psutil.virtual_memory()
        ram_total = mem.total
        ram_avail = mem.available

        gpu_available = False
        gpu_name = None
        vram_total = 0
        vram_avail = 0

        if self._gpu_initialized:
            try:
                # Just query the first GPU for now
                handle = pynvml.nvmlDeviceGetHandleByIndex(0)
                gpu_name = pynvml.nvmlDeviceGetName(handle)
                if isinstance(gpu_name, bytes):
                    gpu_name = gpu_name.decode("utf-8")
                
                info = pynvml.nvmlDeviceGetMemoryInfo(handle)
                vram_total = info.total
                vram_avail = info.free
                gpu_available = True
            except Exception:
                # Fallback if GPU querying fails
                pass

        return WorkerResourceProfile(
            worker_id=self.worker_id,
            endpoint_url=self.endpoint_url,
            ip_address=self.ip_address,
            cpu_cores_total=cpu_cores,
            cpu_utilization_percent=cpu_util,
            ram_total_bytes=ram_total,
            ram_available_bytes=ram_avail,
            gpu_available=gpu_available,
            gpu_device_name=gpu_name,
            vram_total_bytes=vram_total,
            vram_available_bytes=vram_avail,
            active_workload_count=active_workload_count,
            local_cached_hashes=local_cached_hashes or set(),
            timestamp_utc=time.time(),
        )

    def shutdown(self) -> None:
        """Clean up resources."""
        if self._gpu_initialized:
            try:
                pynvml.nvmlShutdown()
            except Exception:
                pass
