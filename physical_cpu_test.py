from pathlib import Path
import socket

output = Path("outputs/physical_worker_marker.txt")
output.parent.mkdir(parents=True, exist_ok=True)

output.write_text(
    "AIDAR_PHYSICAL_TEST_OK\n"
    f"hostname={socket.gethostname()}\n"
    "execution=remote-worker\n",
    encoding="utf-8",
)

print("AIDAR physical CPU test completed")
