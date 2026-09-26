"""Read-model repair: FACT gaps and silent LV topics (02 §4.5, §4.6; review M3).

The read model subscribes to FACT topics (``oms.order.*``, ``risk.event``,
``risk.state.*``, ...) through the Clone/LV path: a skipped seq is applied
("newest wins"), which is fine for state but loses events. :class:`ReadModelRepair`
watches every applied envelope:

* **FACT gaps** -- per ``(src, topic)`` it tracks ``(stream, next seq)``; a
  jump (or a new stream: the publisher's database was replaced) triggers the
  repair of that topic:

  ===========================  =================================================
  topic                        repair
  ===========================  =================================================
  ``oms.order.{acct}``         ``risk.query.orders`` of the account's trading day,
                               merged; fresh ``orders:{acct}`` snapshot to WS
  ``risk.event``               ``risk.query.alerts`` of the shard, merged; fresh
                               ``events`` snapshot to WS
  ``risk.state.*`` and other   ``ctl.snapshot`` of the publisher (Clone resync:
  FACT topics                  latest item of every topic)
  ``risk.cash.*``,             counted only (not shown by the dashboard)
  ``td.exec.*``
  ===========================  =================================================

  Repairs are coalesced per topic, retried a few times, and a resync of one
  publisher happens at most every ``resync_min_interval_s``.
* **Silent LV topics** -- topics published at a fixed rate (``md.status``,
  ``td.status``, ``risk.firm``: 1 Hz) that stay silent for ``silence_s``
  (30 s) while their publisher's heartbeat is up trigger a ``ctl.snapshot``
  of that publisher (02 §4.5: "epoch change or long silence -> ctl.snapshot").
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

from futrisk.common.clock import Clock
from futrisk.messages.envelope import Envelope
from futrisk.messages.risk import OrderBatch, QueryAlerts, QueryOrders, RiskEventBatch
from futrisk.services.api_gateway.readmodel import ReadModel
from futrisk.services.api_gateway.router import CommandRouter, UnknownAccount
from futrisk.services.api_gateway.wshub import WsHub

SILENCE_S = 30.0
RESYNC_MIN_INTERVAL_S = 5.0
REPAIR_ATTEMPTS = 3
_COUNT_ONLY = ("risk.cash.", "td.exec.")

Spawn = Callable[[Awaitable[Any], str], Any]


class ReadModelRepair:
    """Detects FACT gaps / silent LV topics in the read-model feed and repairs them."""

    def __init__(
        self,
        *,
        readmodel: ReadModel,
        router: CommandRouter,
        hub: WsHub,
        clock: Clock,
        trading_day: Callable[[], str],
        resync: Callable[[str], None] | None = None,
        spawn: Spawn | None = None,
        silence_s: float = SILENCE_S,
        resync_min_interval_s: float = RESYNC_MIN_INTERVAL_S,
        retry_s: float = 1.0,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    ) -> None:
        self._rm = readmodel
        self._router = router
        self._hub = hub
        self._clock = clock
        self._trading_day = trading_day
        self._resync = resync
        self._spawn_fn = spawn
        self._silence_ns = int(silence_s * 1e9)
        self._resync_gap_ns = int(resync_min_interval_s * 1e9)
        self._retry_s = retry_s
        self._sleep = sleep
        self._pos: dict[tuple[str, str], tuple[str, int]] = {}
        self._watched: set[tuple[str, str]] = set()
        self._last_seen: dict[tuple[str, str], int] = {}
        self._last_resync: dict[str, int] = {}
        self._running: set[str] = set()
        self._again: set[str] = set()
        self._tasks: set[asyncio.Task[Any]] = set()
        self.gaps = 0
        self.repairs = 0
        self.repair_failures = 0
        self.resyncs = 0

    # ----------------------------------------------------------------- input
    def watch_periodic(self, publisher: str, topic: str) -> None:
        """Expect ``topic`` from ``publisher`` at a steady rate (silence -> resync)."""
        self._watched.add((publisher, topic))

    def observe(self, env: Envelope) -> None:
        """Every envelope applied to the read model (Clone snapshot items and live messages)."""
        key = (env.src, env.topic)
        if env.stream is None:
            if key in self._watched:
                self._last_seen[key] = self._clock.monotonic_ns()
            return
        rec = self._pos.get(key)
        if rec is None:
            self._pos[key] = (env.stream, env.seq + 1)
            return
        stream, expected = rec
        if env.stream != stream:
            if env.stream < stream:
                return  # stale stream (the Clone drops these anyway)
            self._pos[key] = (env.stream, env.seq + 1)
            self._gap(env, None)
            return
        if env.seq < expected:
            return
        self._pos[key] = (stream, env.seq + 1)
        if env.seq > expected:
            self._gap(env, env.seq - expected)

    def _gap(self, env: Envelope, skipped: int | None) -> None:
        self.gaps += 1
        topic, src = env.topic, env.src
        logger.warning("readmodel.fact_gap", src=src, topic=topic, seq=env.seq, skipped=skipped,
                       stream_changed=skipped is None)
        if topic.startswith("oms.order."):
            acct = topic[len("oms.order."):]
            self._schedule(f"orders:{acct}", lambda: self.repair_orders(acct))
        elif topic == "risk.event":
            self._schedule(f"events:{src}", lambda: self.repair_events(src))
        elif topic.startswith(_COUNT_ONLY):
            return
        else:
            self.request_resync(src, f"gap on {topic}")

    # --------------------------------------------------------------- repairs
    def _spawn(self, coro: Awaitable[Any], name: str) -> None:
        if self._spawn_fn is not None:
            self._spawn_fn(coro, name)
            return
        task = asyncio.ensure_future(coro)
        task.set_name(name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _schedule(self, key: str, factory: Callable[[], Awaitable[bool]]) -> None:
        """Run ``factory`` for ``key``; a request while it runs makes it run once more."""
        if key in self._running:
            self._again.add(key)
            return
        self._running.add(key)

        async def run() -> None:
            try:
                while True:
                    self._again.discard(key)
                    for attempt in range(1, REPAIR_ATTEMPTS + 1):
                        try:
                            if await factory():
                                self.repairs += 1
                                break
                        except Exception:
                            logger.exception("readmodel.repair_crashed", key=key)
                        if attempt < REPAIR_ATTEMPTS:
                            await self._sleep(self._retry_s * attempt)
                    else:
                        self.repair_failures += 1
                        logger.error("readmodel.repair_failed", key=key)
                    if key not in self._again:
                        return
            finally:
                self._running.discard(key)

        self._spawn(run(), f"repair-{key}")

    async def repair_orders(self, account_id: str) -> bool:
        """Re-query the account's orders of its trading day and re-snapshot ``orders:{acct}``."""
        td = self._rm.account_trading_day(account_id) or self._trading_day()
        try:
            reply = await self._router.to_account(account_id, "risk.query.orders",
                                                  QueryOrders(account_id=account_id, trading_day=td),
                                                  timeout_ms=3000, retries=1)
        except UnknownAccount:
            return True
        if not reply.ok:
            logger.warning("readmodel.repair_orders_failed", account=account_id, code=reply.code)
            self._hub.invalidate_orders(account_id)  # the next subscription asks the shard again
            return False
        batch = reply.open_as(OrderBatch)
        merged = self._rm.merge_orders(account_id, batch.items if batch else [], trading_day=td)
        self._hub.resnapshot(f"orders:{account_id}")
        logger.info("readmodel.orders_repaired", account=account_id, merged=merged)
        return True

    async def repair_events(self, shard: str) -> bool:
        """Re-query the shard's risk events of the day and re-snapshot ``events``."""
        td = self._trading_day()
        reply = await self._router.request(shard, "risk.query.alerts", QueryAlerts(trading_day=td),
                                           timeout_ms=3000, retries=1)
        if not reply.ok:
            logger.warning("readmodel.repair_events_failed", shard=shard, code=reply.code)
            return False
        batch = reply.open_as(RiskEventBatch)
        added = self._rm.merge_risk_events(batch.items if batch else [])
        self._hub.resnapshot("events")
        logger.info("readmodel.events_repaired", shard=shard, added=added)
        return True

    def request_resync(self, publisher: str, reason: str) -> bool:
        """``ctl.snapshot`` of ``publisher`` (at most every ``resync_min_interval_s``)."""
        if self._resync is None:
            return False
        now = self._clock.monotonic_ns()
        last = self._last_resync.get(publisher)
        if last is not None and now - last < self._resync_gap_ns:
            return False
        self._last_resync[publisher] = now
        self.resyncs += 1
        logger.warning("readmodel.resync", publisher=publisher, reason=reason)
        try:
            self._resync(publisher)
        except Exception:
            logger.exception("readmodel.resync_failed", publisher=publisher)
            return False
        return True

    # ------------------------------------------------------------- watchdog
    def check_silence(self, alive: Callable[[str], bool]) -> list[str]:
        """Resync publishers whose steady LV topics are silent while they are up; returns them."""
        now = self._clock.monotonic_ns()
        out: list[str] = []
        for key in sorted(self._watched):
            publisher, topic = key
            last = self._last_seen.get(key)
            if not alive(publisher) or last is None:
                self._last_seen[key] = now  # start counting once the publisher is up
                continue
            if now - last > self._silence_ns:
                self._last_seen[key] = now  # next resync after another silent period at the earliest
                if self.request_resync(publisher, f"no {topic} for {self._silence_ns / 1e9:g}s") \
                        and publisher not in out:
                    out.append(publisher)
        return out

    def stats(self) -> dict[str, int]:
        return {"gaps": self.gaps, "repairs": self.repairs, "repair_failures": self.repair_failures,
                "resyncs": self.resyncs}

    async def close(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)


__all__ = ["RESYNC_MIN_INTERVAL_S", "SILENCE_S", "ReadModelRepair"]
