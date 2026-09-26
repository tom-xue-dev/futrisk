"""Helpers for the runtime tests: a business-thread harness for SyncServiceRuntime, polling waits."""

from __future__ import annotations

import asyncio
import inspect
import queue
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from typing import Any, TypeVar

from sqlalchemy import MetaData, Table

from futrisk.bus.rpc_client import Completion
from futrisk.db import add_common_tables
from futrisk.db.types import id_col, int_col, text_col
from futrisk.runtime import SyncServiceRuntime

T = TypeVar("T")


def td_schema() -> MetaData:
    md = MetaData()
    add_common_tables(md, fact_cursors=False)
    Table("fills", md, id_col("k", primary_key=True))
    return md


def cw_schema() -> MetaData:
    md = MetaData()
    add_common_tables(md)
    Table("copies", md, text_col("k", primary_key=True), int_col("v", nullable=False))
    return md


class SyncHarness:
    """Runs a SyncServiceRuntime loop on its own 'business' thread; ``call`` executes code there."""

    def __init__(self, rt: SyncServiceRuntime) -> None:
        self.rt = rt
        self._q: queue.SimpleQueue[tuple[Callable[[], Any], Future[Any]]] = queue.SimpleQueue()
        self.completions: list[Completion] = []
        self.error: BaseException | None = None
        self.started = threading.Event()
        self.stopped = threading.Event()
        self._t = threading.Thread(target=self._main, daemon=True, name=f"business:{rt.instance}")

    def start(self) -> None:
        self._t.start()
        assert self.started.wait(10)
        if self.error is not None:
            raise self.error

    def _main(self) -> None:
        try:
            self.rt.start()
            self.started.set()
            self.rt.run(self._step, on_completion=self.completions.append, max_wait_ms=5)
        except BaseException as exc:  # surfaced to the test
            self.error = exc
            self.started.set()
        finally:
            self.rt.stop()
            self.stopped.set()

    def _step(self) -> None:
        while True:
            try:
                fn, fut = self._q.get_nowait()
            except queue.Empty:
                return
            try:
                fut.set_result(fn())
            except BaseException as exc:
                fut.set_exception(exc)

    def call(self, fn: Callable[[], T], timeout: float = 10.0) -> T:
        fut: Future[T] = Future()
        self._q.put((fn, fut))
        return fut.result(timeout)

    async def acall(self, fn: Callable[[], T], timeout: float = 10.0) -> T:
        fut: Future[T] = Future()
        self._q.put((fn, fut))
        return await asyncio.wait_for(asyncio.wrap_future(fut), timeout)

    def stop(self, timeout: float = 10.0) -> None:
        if not self.stopped.is_set():
            self.rt.request_stop("test teardown")
            self.stopped.wait(timeout)


async def eventually(pred: Callable[[], Any], timeout: float = 5.0, step: float = 0.01) -> None:
    """Await until ``pred()`` (sync or async) is truthy."""
    deadline = time.monotonic() + timeout
    while True:
        res = pred()
        if inspect.isawaitable(res):
            res = await res
        if res:
            return
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(step)


def wait_sync(pred: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not pred():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.005)
