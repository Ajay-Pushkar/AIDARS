import time
import socket
from pathlib import Path
from datetime import datetime, timezone


def now():
    return datetime.now(timezone.utc).isoformat()


print(f"TEST_START {now()}", flush=True)
print(f"HOSTNAME {socket.gethostname()}", flush=True)

for i in range(1, 91):
    print(f"TEST_SECOND {i} {now()}", flush=True)
    time.sleep(1)

output = Path("outputs/timeout_investigation_result.txt")
output.parent.mkdir(parents=True, exist_ok=True)

output.write_text(
    "AIDAR_TIMEOUT_INVESTIGATION_COMPLETED\n"
    f"hostname={socket.gethostname()}\n"
    f"time={now()}\n",
    encoding="utf-8",
)

print(f"TEST_COMPLETED {now()}", flush=True)