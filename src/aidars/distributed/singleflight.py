"""SingleFlight duplicate suppression for concurrent asynchronous operations."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Coroutine, Dict, Tuple

logger = logging.getLogger(__name__)


class SingleFlight:
    """Coalesces concurrent asynchronous calls for the same key into a single execution."""

    def __init__(self) -> None:
        self._flights: Dict[str, asyncio.Future[Any]] = {}
        self._lock = asyncio.Lock()

    async def run(
        self,
        key: str,
        operation: Callable[[], Coroutine[Any, Any, Any]],
    ) -> Any:
        """Execute `operation` once for concurrent callers sharing `key`.

        Returns the result of the operation.
        """
        res, _ = await self.run_shared(key, operation)
        return res

    async def run_shared(
        self,
        key: str,
        operation: Callable[[], Coroutine[Any, Any, Any]],
    ) -> Tuple[Any, bool]:
        """Execute `operation` once for concurrent callers sharing `key`.

        Returns `(result, is_shared)` where `is_shared` is `False` for the leader
        caller that triggered execution and `True` for coalesced followers.
        """
        async with self._lock:
            existing = self._flights.get(key)
            if existing is not None and not existing.done():
                is_owner = False
                fut = existing
            else:
                loop = asyncio.get_running_loop()
                fut = loop.create_future()
                self._flights[key] = fut
                is_owner = True

        if not is_owner:
            result = await asyncio.shield(fut)
            return result, True

        return await self._execute(key, fut, operation)

    async def _execute(
        self,
        key: str,
        fut: asyncio.Future[Any],
        operation: Callable[[], Coroutine[Any, Any, Any]],
    ) -> Tuple[Any, bool]:
        try:
            result = await operation()
            if not fut.done():
                fut.set_result(result)
            return result, False
        except BaseException as exc:
            if not fut.done():
                if isinstance(exc, asyncio.CancelledError):
                    fut.cancel()
                else:
                    fut.set_exception(exc)
            raise
        finally:
            async with self._lock:
                if self._flights.get(key) is fut:
                    del self._flights[key]