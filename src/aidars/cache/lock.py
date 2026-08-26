import os
import sys
from pathlib import Path

class ProcessLock:
    """A boring, reliable cross-process file lock for quota coordination."""
    
    def __init__(self, lock_file: Path | str) -> None:
        self.lock_file = Path(lock_file)
        self.fd = None

    def __enter__(self) -> "ProcessLock":
        self.lock_file.parent.mkdir(parents=True, exist_ok=True)
        self.fd = os.open(str(self.lock_file), os.O_RDWR | os.O_CREAT)
        if sys.platform == "win32":
            import msvcrt
            msvcrt.locking(self.fd, msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(self.fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.fd is not None:
            if sys.platform == "win32":
                import msvcrt
                os.lseek(self.fd, 0, os.SEEK_SET)
                msvcrt.locking(self.fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.fd, fcntl.LOCK_UN)
            os.close(self.fd)
            self.fd = None
