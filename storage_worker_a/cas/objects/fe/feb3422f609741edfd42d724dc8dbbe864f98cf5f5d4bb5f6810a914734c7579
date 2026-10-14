import time
from pathlib import Path
import socket

print("AIDAR_LONG_TEST_STARTED", flush=True)

for i in range(60):
    print(f"RUNNING {i+1}/60", flush=True)
    time.sleep(1)

output = Path("outputs/failure_recovery_marker.txt")
output.parent.mkdir(parents=True, exist_ok=True)
output.write_text(
    "AIDAR_FAILURE_RECOVERY_TEST_COMPLETED\n"
    f"hostname={socket.gethostname()}\n",
    encoding="utf-8",
)

print("AIDAR_LONG_TEST_COMPLETED", flush=True)
