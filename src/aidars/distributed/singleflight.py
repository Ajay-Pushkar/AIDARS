"""Single-flight request deduplication for asynchronous operations.

Ensures that concurrent requests for the same key (e.g., downloading the same hash)
are coalesced into a single operation, returning the same result to all awaiters.
"""

import asyncio
from typing import Awaitable, Callable, Dict, TypeVar

T = TypeVar("T")


class SingleFlight:
    """Coalesces concurrent identical in-flight asynchronous operations."""

    def __init__(self) -> None:
        self._flights: Dict[str, asyncio.Future[T]] = {}
        self._lock = asyncio.Lock()

    async def run(self, key: str, operation: Callable[[], Awaitable[T]]) -> T:
        """Execute the operation or wait for an existing in-flight operation.

        Args:
            key: The unique identifier for the operation.
            operation: The async function to execute if the key is not in-flight.

        Returns:
            The result of the operation.
        """
        async with self._lock:
            if key in self._flights:
                future = self._flights[key]
            else:
                loop = asyncio.get_running_loop()
                future = loop.create_future()
                self._flights[key] = future
                asyncio.create_task(self._execute(key, operation, future))

        return await asyncio.shield(future)

    async def _execute(
        self, key: str, operation: Callable[[], Awaitable[T]], future: asyncio.Future[T]
    ) -> None:
        """Internal executor that runs the operation and resolves the future."""
        try:
            result = await operation()
            future.set_result(result)
        except Exception as exc:
            future.set_exception(exc)
        finally:
            async with self._lock:
                self._flights.pop(key, None)
