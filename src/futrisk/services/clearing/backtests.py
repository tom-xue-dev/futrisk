"""Backtest jobs hosted by the clearing-worker (01 §5.3.4 Backtest, §8.16; 02 §6.5 ``backtest.*``; 03 §4.4, §5.2).

``backtest.run`` records a ``backtest_runs`` row (``QUEUED``, idempotent by
``command_id``) and queues the job in the service's process pool. The job calls
``futrisk.backtest.runner.run_job(config_name, overrides, out_dir, progress)``
(written by the backtester; the entry point is configurable). While it runs,
the worker process reports progress through ``<out_dir>/progress.json``
(:class:`futrisk.services.clearing.jobs.FileProgress`); the service polls it and
publishes ``backtest.status`` (LV) and ``backtest_runs.progress``.

Request: ``overrides`` is passed to the runner unchanged; ``overrides["seed"]``,
when present, must be an integer (``BAD_REQUEST`` otherwise) and is recorded in
``backtest_runs.seed`` until the runner reports the seed it used.

Runner result contract: a ``dict`` (``None`` counts as empty, any other type fails
the job); every key is optional and a key of the wrong type is ignored with a
WARNING instead of failing a finished job:

==================  ===========================  ==============================================
key                 type                         use
==================  ===========================  ==============================================
``summary``         ``dict`` (JSON-able)         ``backtest_runs.summary`` / ``BacktestStatus``
                                                 (non-finite floats become ``null``)
``config_hash``     ``str``                      64 hex chars kept, other text hashed (SHA-256)
``data_hash``       ``str``                      same as ``config_hash``
``seed``            ``int``                      the seed actually used
``git_sha``         ``str`` (<= 64 chars)        commit of the code that ran
``report_path``     ``str``                      the HTML report, relative to ``out_dir`` or
                                                 absolute; it must lie inside ``out_dir``
                                                 (default ``<out_dir>/report.html``)
==================  ===========================  ==============================================

When the job finishes, ``backtest_runs`` gets the summary and the reproducibility
fields; the report file is registered as a ``BACKTEST_HTML`` report (``reports``
+ ``report.ready``). A missing backtester module fails the job with a clear
error (the module lookup runs in a worker thread, never importing on the event
loop). Jobs interrupted by a restart are marked FAILED at start-up.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import importlib.util
import json
import math
import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

import msgspec
from loguru import logger
from sqlalchemy import Connection

from futrisk.common.ids import uuid7
from futrisk.db import Database
from futrisk.messages import codes
from futrisk.messages.enums import BacktestState, ReportType
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.eod import BacktestRunRequest, BacktestStatus, ReportReady
from futrisk.messages.topics import BACKTEST_STATUS
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.clearing import jobs, store
from futrisk.services.clearing.pool import WorkerPool
from futrisk.services.clearing.reports.gather import ReportService
from futrisk.services.clearing.settings import ClearingSettings

ZERO_HASH = "0" * 64


def git_sha(root: Path | None = None) -> str:
    """Current commit, or ``nogit`` without a repository (03 §4.4 ``backtest_runs.git_sha``).

    Frozen exe: no repository ships with it; the SHA-256 of the executable bundle
    (:func:`futrisk.common.paths.code_identity`, 64 hex chars) identifies the code instead.
    """
    from futrisk.common.paths import code_identity

    ident = code_identity()
    if ident:
        return ident
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(root) if root else None,
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return "nogit"
    sha = out.stdout.strip()
    return sha if out.returncode == 0 and sha else "nogit"


def jsonable(value: Any) -> Any:
    """JSON-safe copy of a runner result: non-finite floats become ``None``, unknown objects ``str``."""
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [jsonable(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if value is None or isinstance(value, bool | int | str):
        return value
    return str(value)


def _hash_or_zero(value: Any) -> str:
    text = str(value or "")
    if len(text) == 64 and all(c in "0123456789abcdef" for c in text):
        return text
    return hashlib.sha256(text.encode("utf-8")).hexdigest() if text else ZERO_HASH


def result_fields(result: Any, run_id: str = "") -> dict[str, Any]:
    """Validated fields of a runner result (see the module docstring); wrong types are dropped with a WARNING."""
    if not isinstance(result, dict):
        logger.warning("backtest.result_not_dict", run_id=run_id, type=type(result).__name__)
        return {}
    out: dict[str, Any] = {}

    def bad(key: str, value: Any) -> None:
        logger.warning("backtest.result_field_ignored", run_id=run_id, key=key, type=type(value).__name__)

    summary = result.get("summary")
    if summary is not None:
        if isinstance(summary, dict):
            out["summary"] = jsonable(summary)
        else:
            bad("summary", summary)
    for key in ("config_hash", "data_hash"):
        value = result.get(key)
        if value is not None:
            if isinstance(value, str) and value:
                out[key] = _hash_or_zero(value)
            else:
                bad(key, value)
    seed = result.get("seed")
    if seed is not None:
        if isinstance(seed, int) and not isinstance(seed, bool):
            out["seed"] = seed
        else:
            bad("seed", seed)
    sha = result.get("git_sha")
    if sha is not None:
        if isinstance(sha, str) and 0 < len(sha) <= 64:
            out["git_sha"] = sha
        else:
            bad("git_sha", sha)
    report = result.get("report_path")
    if report is not None:
        if isinstance(report, str) and report:
            out["report_path"] = report
        else:
            bad("report_path", report)
    return out


def report_inside(out_dir: Path, report_path: str | None) -> Path | None:
    """The report file inside ``out_dir`` (``None`` for a path that escapes it)."""
    base = out_dir.resolve()
    candidate = Path(report_path) if report_path else Path("report.html")
    full = (candidate if candidate.is_absolute() else base / candidate).resolve()
    if full != base and base not in full.parents:
        return None
    return full


class BacktestManager:
    """Queues backtest jobs in the process pool and tracks their state."""

    def __init__(
        self,
        rt: AsyncServiceRuntime,
        db: Database,
        settings: ClearingSettings,
        *,
        pool: WorkerPool,
        reports: ReportService,
        trading_day: Callable[[], str],
    ) -> None:
        self._rt = rt
        self._db = db
        self._settings = settings
        self._pool = pool
        self._reports = reports
        self._trading_day = trading_day
        self._sem = asyncio.Semaphore(max(1, settings.backtest_concurrency))
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._git_sha: str | None = None

    @property
    def backtests_dir(self) -> Path:
        assert self._rt.layout is not None
        return self._rt.layout.backtests_dir

    def runner_available(self) -> str | None:
        """``None`` when the runner module can be found, else the reason."""
        module = self._settings.backtest_runner.partition(":")[0]
        try:
            found = importlib.util.find_spec(module) is not None
        except (ImportError, ValueError):
            found = False
        return None if found else f"backtest engine not available: module {module} not found"

    # --------------------------------------------------------------- startup
    async def recover(self) -> int:
        """Mark jobs left QUEUED/RUNNING by a previous process as FAILED."""
        stale = await self._db.run_read(
            lambda c: store.backtests_in_states(c, [BacktestState.QUEUED, BacktestState.RUNNING])
        )
        now = self._rt.market_clock.now_ns()
        ids = [st.run_id for st in stale]

        def tx(conn: Connection) -> None:
            for run_id in ids:
                store.update_backtest(
                    conn, run_id, state=BacktestState.FAILED, finished_ns=now, error="interrupted by a clearing-worker restart"
                )

        if ids:
            await self._db.run_write(tx)
        return len(ids)

    async def stop(self) -> None:
        """Cancel running jobs and mark them FAILED now (the pool terminates their workers next)."""
        running = dict(self._tasks)
        for t in running.values():
            t.cancel()
        for t in running.values():
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        if not running:
            return
        now = self._rt.market_clock.now_ns()
        ids = set(running)

        def tx(conn: Connection) -> None:
            for st in store.backtests_in_states(conn, [BacktestState.QUEUED, BacktestState.RUNNING]):
                if st.run_id in ids:
                    store.update_backtest(
                        conn,
                        st.run_id,
                        state=BacktestState.FAILED,
                        finished_ns=now,
                        error="interrupted by a clearing-worker shutdown",
                    )

        with contextlib.suppress(Exception):
            await self._db.run_write(tx)

    # ---------------------------------------------------------------- submit
    async def submit(self, req: BacktestRunRequest, *, requester: str, received_ns: int) -> Reply:
        """``backtest.run``: idempotent by ``command_id`` (``rt.idempotent``: check + insert + record in one job)."""
        if not req.config_name.strip():
            return err_reply(codes.BAD_REQUEST, "config_name must not be empty")
        overrides = dict(req.overrides or {})
        raw_seed = overrides.get("seed", 0)
        if raw_seed is None:
            raw_seed = 0
        if not isinstance(raw_seed, int) or isinstance(raw_seed, bool):
            return err_reply(codes.BAD_REQUEST, f"overrides.seed must be an integer, got {raw_seed!r}")
        seed = int(raw_seed)
        now = self._rt.market_clock.now_ns()
        if self._git_sha is None:
            self._git_sha = await asyncio.to_thread(git_sha, Path(__file__).resolve().parents[3])
        run_id = uuid7()
        cfg = self._rt.config
        status = BacktestStatus(run_id=run_id, name=req.config_name, state=BacktestState.QUEUED, progress=0.0, submitted_ns=now)
        td = self._trading_day()
        created: list[bool] = []

        def handler(conn: Connection) -> Reply:
            store.insert_backtest(
                conn,
                run_id=run_id,
                name=req.config_name,
                config_hash=cfg.config_hash or ZERO_HASH,
                data_hash=ZERO_HASH,
                seed=seed,
                git_sha=self._git_sha or "nogit",
                state=BacktestState.QUEUED.value,
                progress=0.0,
                submitted_by=req.actor,
                submitted_ns=now,
                started_ns=None,
                finished_ns=None,
                report_id=None,
                summary=None,
                error=None,
            )
            created.append(True)
            return ok_reply(status)

        reply = await self._rt.idempotent(
            self._db, "backtest.run", requester, req.command_id, req, handler, trading_day=td, received_ns=received_ns
        )
        if created:
            self._publish(status)
            task = asyncio.create_task(self._run(status, req.overrides), name=f"clearing:backtest:{run_id}")
            self._tasks[run_id] = task
            task.add_done_callback(lambda _t: self._tasks.pop(run_id, None))
        return reply

    def _publish(self, status: BacktestStatus) -> None:
        with contextlib.suppress(RuntimeError):  # no PUB endpoint
            self._rt.publish(BACKTEST_STATUS, status, corr_id=status.run_id)

    async def _update(self, status: BacktestStatus, **values: Any) -> BacktestStatus:
        new = msgspec.structs.replace(status, **values)
        db_values = {k: v for k, v in values.items() if k in {"state", "progress", "started_ns", "finished_ns", "report_id", "summary", "error"}}
        await self._db.run_write(lambda c: store.update_backtest(c, status.run_id, **db_values))
        self._publish(new)
        return new

    # ------------------------------------------------------------------- run
    async def _run(self, status: BacktestStatus, overrides: dict[str, Any] | None) -> None:
        out_dir = self.backtests_dir / status.run_id
        reason = await asyncio.to_thread(self.runner_available)  # find_spec imports parent packages
        if reason is not None:
            logger.warning("backtest.unavailable", run_id=status.run_id, reason=reason)
            await self._update(status, state=BacktestState.FAILED, finished_ns=self._rt.market_clock.now_ns(), error=reason)
            return
        async with self._sem:
            status = await self._update(status, state=BacktestState.RUNNING, started_ns=self._rt.market_clock.now_ns())
            poller = asyncio.create_task(self._poll_progress(status, out_dir))
            try:
                result = await self._pool.run(
                    jobs.backtest_job, self._settings.backtest_runner, status.name, overrides, str(out_dir)
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.opt(exception=exc).warning("backtest.failed", run_id=status.run_id)
                await self._update(
                    status,
                    state=BacktestState.FAILED,
                    finished_ns=self._rt.market_clock.now_ns(),
                    error=f"{type(exc).__name__}: {exc}",
                )
                return
            finally:
                poller.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await poller
            try:
                await self._finish(status, result, out_dir)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # never leave a finished job RUNNING
                logger.opt(exception=exc).error("backtest.finish_failed", run_id=status.run_id)
                await self._update(
                    status,
                    state=BacktestState.FAILED,
                    finished_ns=self._rt.market_clock.now_ns(),
                    error=f"result handling failed: {type(exc).__name__}: {exc}",
                )

    async def _poll_progress(self, status: BacktestStatus, out_dir: Path) -> None:
        path = out_dir / "progress.json"
        last = -1.0
        while True:
            await asyncio.sleep(self._settings.progress_poll_s)
            try:
                data = json.loads(await asyncio.to_thread(path.read_text, encoding="utf-8"))
            except (OSError, ValueError):
                continue
            value = float(data.get("progress") or 0.0)
            if value > last:
                last = value
                status = await self._update(status, progress=value)

    async def _finish(self, status: BacktestStatus, result: Any, out_dir: Path) -> None:
        now = self._rt.market_clock.now_ns()
        fields = result_fields(result, status.run_id)
        summary = fields.get("summary")
        extra = {k: fields[k] for k in ("config_hash", "data_hash", "seed", "git_sha") if k in fields}
        if extra:
            await self._db.run_write(lambda c: store.update_backtest(c, status.run_id, **extra))
        report_id: str | None = None
        report_path = report_inside(out_dir, fields.get("report_path"))
        if report_path is None:
            logger.warning(
                "backtest.report_path_outside_out_dir", run_id=status.run_id, report_path=fields.get("report_path")
            )
        elif await asyncio.to_thread(report_path.is_file):
            rel = report_path.relative_to(self._reports.home.resolve()).as_posix()

            def link(conn: Connection, report: ReportReady) -> None:
                store.update_backtest(conn, status.run_id, report_id=report.report_id)

            report = await self._reports.register(ReportType.BACKTEST_HTML, rel, trading_day=None, extra_tx=link)
            report_id = report.report_id
        await self._update(
            status,
            state=BacktestState.DONE,
            progress=1.0,
            finished_ns=now,
            report_id=report_id,
            summary=summary if isinstance(summary, dict) else None,
        )
        logger.info("backtest.done", run_id=status.run_id, report_id=report_id)


__all__ = ["BacktestManager", "git_sha"]
