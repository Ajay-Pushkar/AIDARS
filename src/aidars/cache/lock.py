import os
import sys
import time
from pathlib import Path
from typing import Union, Optional


class ProcessLock:
    """A reliable cross-process file lock for quota and cache coordination."""
    
    def __init__(self, lock_file: Union[Path, str], timeout_seconds: float = 60.0) -> None:
        self.lock_file = Path(lock_file)
        self.timeout_seconds = float(timeout_seconds)
        self.fd: Optional[int] = None

    def __enter__(self) -> "ProcessLock":
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(str(self.lock_file), os.O_RDWR | os.O_CREAT)
        start_time = time.time()
        while True:
            try:
                self._try_lock()
                return self
            except (BlockingIOError, PermissionError, OSError):
                if time.time() - start_time >= self.timeout_seconds:
                    if self.fd is not None:
                        os.close(self.fd)
                        self.fd = None
                    raise TimeoutError(f"Could not acquire lock on {self.lock_file} within {self.timeout_seconds}s")
                time.sleep(0.05)

    def _try_lock(self) -> None:
        if os.name == "nt":
            import msvcrt
            os.lseek(self.fd, 0, os.SEEK_SET)
            # Use non-blocking LK_NBLCK so the outer loop enforces self.timeout_seconds
            msvcrt.locking(self.fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.fd is not None:
            if os.name == "nt":
                import msvcrt
                try:
                    os.lseek(self.fd, 0, os.SEEK_SET)
                    msvcrt.locking(self.fd, msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            else:
                import fcntl
                try:
                    fcntl.flock(self.fd, fcntl.LOCK_UN)
                except OSError:
                    pass
            os.close(self.fd)
            self.fd = None
