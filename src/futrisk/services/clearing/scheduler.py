"""EOD scheduler: start the saga at ``app.eod_start_at`` market time (01 §5.3.4, §8.2; 02 §8.8).

Market time comes from the runtime's :class:`~futrisk.common.clock.MarketClock`
(``sys.clock``), so demo clock jumps trigger the EOD exactly like production.
Every ``scheduler_interval_s`` the scheduler computes the latest trading day
whose ``eod_start_at`` has passed (``due``) and looks at the newest SOD
snapshot day ``L`` (the day the system currently stands on):

* ``L > due`` - nothing to do yet;
* ``L`` already has a run (in progress, DONE or FAILED) - nothing to do: runs
  in progress are driven by the saga, FAILED needs an operator (``settle.run``);
* otherwise a run for ``L`` is created (``triggered_by = scheduler``). When the
  worker was down for several days (clock jump, holiday) this settles the
  skipped days one by one, strictly in order: each run creates the SOD of the
  next day, which a later tick picks up, and a day's run only starts once the
  previous day's run is ROLLED (``decide_run_tx`` answers
  ``PRECONDITION_FAILED`` until then and the scheduler simply retries). The
  shards, already PROVISIONAL on a later day, answer these catch-up pushes with
  ``CATCH_UP`` and the push of the day they stand on with ``REBASE``.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from datetime import date, datetime, time

from loguru import logger

from futrisk.common.clock import ns_to_bj, parse_td, td_str
from futrisk.common.config import FullConfig


def parse_hhmm(text: str) -> time:
    hh, mm = text.strip().split(":")[:2]
    return time(int(hh), int(mm))


def due_trading_day(cfg: FullConfig, now_ns: int) -> str | None:
    """The latest trading day ``D`` with ``D@eod_start_at <= now`` (market time, Beijing)."""
    cal = cfg.calendar
    start = parse_hhmm(cfg.app.eod_start_at)
    now: datetime = ns_to_bj(now_ns)
    today: date = now.date()
    try:
        if cal.is_trading_day(today) and now.time() >= start:
            return td_str(today)
        return td_str(cal.prev_trading_day(today))
    except LookupError:
        return None


class EodScheduler:
    """Periodic check that starts the EOD saga of the due trading day."""

    def __init__(
        self,
        *,
        config: Callable[[], FullConfig],
        now_ns: Callable[[], int],
        latest_sod_day: Callable[[], Awaitable[str | None]],
        has_run: Callable[[str], Awaitable[bool]],
        start_run: Callable[[str], Awaitable[bool]],
        interval_s: float,
    ) -> None:
        self._config = config
        self._now_ns = now_ns
        self._latest_sod_day = latest_sod_day
        self._has_run = has_run
        self._start_run = start_run
        self._interval = interval_s
        self._task: asyncio.Task[None] | None = None
        self.last_started: str | None = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._loop(), name="clearing:scheduler")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("scheduler.tick_failed")
            await asyncio.sleep(self._interval)

    async def tick(self) -> str | None:
        """One scheduling decision; returns the trading day whose run was started (if any)."""
        cfg = self._config()
        due = due_trading_day(cfg, self._now_ns())
        if due is None:
            return None
        day = await self._latest_sod_day()
        if day is None or day > due:
            return None
        if not cfg.calendar.is_trading_day(parse_td(day)):
            logger.warning("scheduler.sod_day_not_trading", trading_day=day)
            return None
        if await self._has_run(day):
            return None
        if not await self._start_run(day):
            return None
        logger.info("scheduler.started_eod", trading_day=day, due=due, eod_start_at=cfg.app.eod_start_at)
        self.last_started = day
        return day


__all__ = ["EodScheduler", "due_trading_day", "parse_hhmm"]
