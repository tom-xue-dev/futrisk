"""Background-task helpers of sim-exchange: supervision and an order-preserving delay line."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Generic, TypeVar

from loguru import logger

T = TypeVar("T")

#: pause before a crashed background loop is restarted
RESTART_DELAY_S = 0.5


async def supervised(name: str, run: Callable[[], Awaitable[None]], *, restart_delay: float = RESTART_DELAY_S) -> None:
    """Run ``run()``; if it raises, log the exception and start it again.

    Background loops (heartbeats, timers, the engine round loop) must never die
    silently: a crash is logged with its traceback and the loop restarts. A normal
    return ends the supervision; cancellation propagates.
    """
    while True:
        try:
            await run()
            return
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("sim-exchange background task {} crashed; restarting in {}s", name, restart_delay)
            await asyncio.sleep(restart_delay)


def spawn(name: str, run: Callable[[], Awaitable[None]]) -> asyncio.Task[None]:
    """``asyncio.create_task(supervised(name, run))`` with a task name."""
    return asyncio.create_task(supervised(name, run), name=name)


class DelayLine(Generic[T]):
    """FIFO delay: every item reaches ``sink`` ``delay`` seconds after :meth:`put`, in order.

    Used for the SimBroker's ``ack_delay_ms`` / ``fill_delay_ms`` so the delay never
    blocks the broker worker, and - unlike independent ``call_later`` timers, whose
    order is not guaranteed when the coarse Windows clock gives them equal deadlines -
    items are never reordered. ``delay <= 0`` hands items over synchronously.
    """

    def __init__(self, name: str, delay_s: float, sink: Callable[[T], None]) -> None:
        self.name = name
        self.delay_s = max(0.0, delay_s)
        self._sink = sink
        self._queue: asyncio.Queue[tuple[float, T]] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self.delay_s > 0 and self._task is None:
            self._task = spawn(self.name, self._run)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def put(self, item: T) -> None:
        if self.delay_s <= 0:
            self._sink(item)
        else:
            self._queue.put_nowait((time.monotonic() + self.delay_s, item))

    async def _run(self) -> None:
        while True:
            due, item = await self._queue.get()
            wait = due - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self._sink(item)
