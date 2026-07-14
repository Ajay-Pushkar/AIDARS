from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List


@dataclass(slots=True)
class Task:
    """A placeholder task for future scheduling work."""

    task_id: str
    payload: dict[str, Any] = field(default_factory=dict)


class TaskQueue:
    """A simple in-memory queue for future distributed scheduling."""

    def __init__(self) -> None:
        self._tasks: List[Task] = []

    def enqueue(self, task: Task) -> None:
        self._tasks.append(task)

    def dequeue(self) -> Task | None:
        if not self._tasks:
            return None
        return self._tasks.pop(0)

    def size(self) -> int:
        return len(self._tasks)
