"""The clearing-worker's process pool (01 §8.12: asyncio + 写库线程 + 进程池（结算、报表、回测）).

* Jobs are submitted from a helper thread: ``ProcessPoolExecutor.submit``
  starts worker processes on the calling thread (``spawn`` on Windows runs
  ``CreateProcess`` and writes the pickled preparation data to a pipe the
  child must read), which must never stall the event loop that answers
  commands such as ``ctl.shutdown``.
* A pool broken by a dead worker (:class:`BrokenProcessPool`) is replaced for
  the next job instead of failing every later EOD.
* :meth:`WorkerPool.close` cancels queued jobs, gives running ones a short
  grace and then terminates the workers, so a long backtest can never hold
  up a shutdown. Interrupted work is recovered at the next start: the EOD
  saga resumes from ``eod_runs`` (a settle or report step simply runs again)
  and backtests are marked FAILED.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import time
from collections.abc import Callable
from concurrent.futures import Executor, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from typing import Any, TypeVar

from loguru import logger

T = TypeVar("T")


def shutdown_pool(ex: Executor, grace_s: float = 1.0) -> list[int]:
    """Stop ``ex`` without waiting for long jobs; returns the pids of workers that had to be terminated.

    Queued jobs are cancelled; running ones get ``grace_s`` seconds, then their
    workers are terminated (``ProcessPoolExecutor.shutdown`` itself would wait
    for them).
    """
    procs: list[Any] = []
    if isinstance(ex, ProcessPoolExecutor):  # shutdown() drops the process handles: take them first
        procs = list((getattr(ex, "_processes", None) or {}).values())
    ex.shutdown(wait=False, cancel_futures=True)
    deadline = time.monotonic() + max(0.0, grace_s)
    killed: list[int] = []
    for p in procs:
        p.join(max(0.0, deadline - time.monotonic()))
        if p.is_alive():
            logger.warning("clearing.pool_worker_terminated", pid=p.pid)
            p.terminate()
            killed.append(int(p.pid))
    for p in procs:
        if p.is_alive():
            p.join(2.0)
    return killed


class WorkerPool:
    """Lazily created ``spawn`` process pool (or an injected executor, e.g. shared by tests)."""

    def __init__(self, workers: int, *, executor: Executor | None = None) -> None:
        self._workers = max(1, workers)
        self._executor = executor
        self._owned = executor is None
        self._closed = False

    @property
    def started(self) -> bool:
        return self._executor is not None

    def _get(self) -> Executor:
        if self._closed:
            raise RuntimeError("clearing worker pool is closed")
        if self._executor is None:
            self._executor = ProcessPoolExecutor(
                max_workers=self._workers, mp_context=multiprocessing.get_context("spawn")
            )
        return self._executor

    async def run(self, fn: Callable[..., T], *args: Any) -> T:
        """Run ``fn(*args)`` in a worker process (top-level, picklable function and arguments)."""
        ex = self._get()
        cfut = await asyncio.to_thread(ex.submit, fn, *args)
        try:
            return await asyncio.wrap_future(cfut)
        except BrokenProcessPool:
            if self._owned and self._executor is ex and not self._closed:
                logger.error("clearing.pool_broken", detail="a worker died; the next job gets a new pool")
                self._executor = None
                ex.shutdown(wait=False, cancel_futures=True)
            raise

    async def close(self, grace_s: float) -> list[int]:
        """Stop the pool (owned pools only): cancel queued jobs, terminate busy workers after ``grace_s``."""
        self._closed = True
        ex, self._executor = self._executor, None
        if ex is None or not self._owned:
            return []
        return await asyncio.to_thread(shutdown_pool, ex, grace_s)


__all__ = ["WorkerPool", "shutdown_pool"]
