"""Shutdown behaviour: ``ctl.shutdown`` is answered at once, busy pool workers are terminated after a short
grace, and work interrupted in the pool resumes correctly after a restart (02 §6.1, §8.4; 01 §8.12)."""

from __future__ import annotations

import asyncio
import dataclasses
import sqlite3
import time
from collections.abc import Callable
from concurrent.futures import Executor, Future, ProcessPoolExecutor
from pathlib import Path
from typing import Any

import psutil
from clearing_support import CW, FAST, NEXT, Api, D, Harness, eventually, market_clock_at

from futrisk.common.config import FullConfig
from futrisk.common.ids import uuid7
from futrisk.messages.common import Ping, Pong, ShutdownRequest
from futrisk.messages.enums import BacktestState
from futrisk.messages.eod import BacktestRunRequest, BacktestStatus, BacktestStatusRequest
from futrisk.services.clearing import jobs
from futrisk.services.clearing.main import serve


def _alive(pids: set[int]) -> list[int]:
    out = []
    for pid in pids:
        try:
            if psutil.Process(pid).status() != psutil.STATUS_ZOMBIE:
                out.append(pid)
        except psutil.NoSuchProcess:
            continue
    return out


class TestShutdown:
    async def test_ctl_shutdown_replies_at_once_with_busy_pool(self, tmp_path: Path, bus_config: FullConfig) -> None:
        me = psutil.Process()
        before = {c.pid for c in me.children(recursive=True)}
        settings = dataclasses.replace(
            FAST, backtest_runner="fake_backtest:slow_job", pool_workers=1, pool_stop_grace_s=0.5
        )
        task = asyncio.create_task(
            serve(home=tmp_path / "cw", config=bus_config, market_clock=market_clock_at("2026-09-28 16:00:00"),
                  setup_logs=False, settings=settings)
        )
        api, cli, sup = Api(bus_config), Api(bus_config, identity="cli"), Api(bus_config, identity="supervisor")
        try:
            async def running() -> bool:
                r = await cli.client.request(CW, "ctl.ping", Ping(), timeout_ms=500, retries=0)
                return bool(r.ok and r.open_as(Pong).state == "RUNNING")

            await eventually(running, timeout=15)
            r = await api.call("backtest.run", BacktestRunRequest(command_id=uuid7(), config_name="slow", actor="ops"))
            run_id = r.open_as(BacktestStatus).run_id

            async def busy() -> bool:
                st = (await api.call("backtest.status", BacktestStatusRequest(run_id=run_id))).open_as(BacktestStatus)
                return bool(st.state == BacktestState.RUNNING and st.progress > 0)  # the worker runs the job
            await eventually(busy, timeout=20)
            workers = {c.pid for c in me.children(recursive=True)} - before
            assert workers  # the service's own spawn pool is busy

            t0 = time.perf_counter()
            reply = await sup.client.request(
                CW, "ctl.shutdown", ShutdownRequest(reason="test", deadline_ms=10_000), timeout_ms=1_000, retries=0
            )
            answered = time.perf_counter() - t0
            assert reply.ok and answered < 1.0, (reply, answered)
            code = await asyncio.wait_for(task, 15)
            stopped = time.perf_counter() - t0
            assert code == 0 and stopped < 5.0, stopped
            await eventually(lambda: not _alive(workers), timeout=5)  # busy worker terminated after the grace
            con = sqlite3.connect(tmp_path / "cw" / "db" / "clearing.db")
            try:
                state, error = con.execute("SELECT state, error FROM backtest_runs WHERE run_id=?", (run_id,)).fetchone()
                assert con.execute("SELECT clean_shutdown FROM db_meta").fetchone()[0] == 1
            finally:
                con.close()
            assert state == "FAILED" and "interrupted" in error
        finally:
            if not task.done():
                task.cancel()
            for a in (api, cli, sup):
                await a.close()


class _HangingSettle(Executor):
    """Delegates to the real pool but never finishes ``settle_job`` (a step interrupted in the pool)."""

    def __init__(self, inner: Executor) -> None:
        self.inner = inner
        self.held: list[Future[Any]] = []

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future[Any]:
        if fn is jobs.settle_job:
            fut: Future[Any] = Future()
            self.held.append(fut)
            return fut
        return self.inner.submit(fn, *args, **kwargs)


class TestInterruptedPoolStep:
    async def test_settle_step_interrupted_in_pool_resumes_after_restart(
        self, tmp_path: Path, bus_config: FullConfig, pool: ProcessPoolExecutor
    ) -> None:
        hanging = _HangingSettle(pool)
        h = Harness(bus_config, tmp_path, executor=hanging)
        try:
            await h.start_stubs()
            await h.start_clearing()
            await h.seed()
            await h.load_day()
            await h.wait_copies()
            assert (await h.api.settle_run()).ok
            await eventually(lambda: bool(hanging.held), timeout=15)  # step 5 is computing in the pool
            t0 = time.perf_counter()
            assert h.rt is not None
            reply = await h.api.to(CW, "ctl.ping", Ping())
            assert reply.ok and time.perf_counter() - t0 < 1.0  # the loop stays responsive meanwhile
            await h.stop_clearing()
            con = sqlite3.connect(tmp_path / "cw" / "db" / "clearing.db")
            try:
                assert con.execute(f"SELECT phase FROM eod_runs WHERE trading_day='{D}'").fetchall() == [("PRICES_FIXED",)]
                assert con.execute("SELECT count(*) FROM account_settlements").fetchone()[0] == 0
            finally:
                con.close()
            h.executor = pool  # restart with a healthy pool: the run resumes at step 5
            await h.start_clearing()
            done = await h.wait_phase("DONE")
            assert done.version == 1
            assert all(s.rolled.get(NEXT) == 1 for s in h.shards)
        finally:
            for f in hanging.held:
                f.cancel()
            await h.close()
