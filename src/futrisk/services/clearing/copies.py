"""Local copies of trades and cash movements (01 §5.3.4 本地副本, 02 §4.1, §4.5, §4.6, §8.2; 03 §4.4, §7).

The clearing-worker consumes two FACT streams with persisted cursors and the
``FROM_BEGINNING`` start policy (02 §4.5):

* ``td.exec.*`` from trade-gateway: only ``Trade`` envelopes are copied
  (``BrokerOrderUpdate`` just advances the cursor), keyed by the trade key;
* ``risk.cash.*`` from every risk-engine shard, keyed by ``movement_id``.

Each message is applied in ONE writer transaction together with its cursor
(``fact_cursors``). ``apply`` is synchronous and never swallows errors: the
write is queued with ``submit_write`` (single writer keeps the order; a failed
commit stops the service). Copies are idempotent by business key, so replays,
stream restarts and domain-query repairs may deliver a row twice.

When a replay range was purged (``ctl.replay`` NOT_FOUND) or the publisher's
stream changed (database recreated / restored), the affected account's data of
the unsettled trading days is pulled with ``td.query.trades`` /
``risk.query.cash`` before the tracker resumes (02 §4.6, §8.2
``domain_fallback``). Whatever is still missing is caught by the three-way
digest of EOD step 3.

A new row for a day whose run already SETTLED is a *late fact*: the settlement
does not contain it, so the same transaction flags the run with a ``LATE_FACT``
WARN (:func:`futrisk.services.clearing.eodwarn.flag_late_facts`). New rows of a
day still being settled are caught by step 5, which re-checks the copy against
the digests pinned in step 3.

The service subscribes only once the market clock is known (``sys.clock``):
cursor times and late-fact WARNs carry market time.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable

from loguru import logger
from sqlalchemy import Connection

from futrisk.bus.fact import CursorUpdate, FactKey
from futrisk.db import Database, FactCursors
from futrisk.messages.base import decode
from futrisk.messages.enums import FactStartPolicy
from futrisk.messages.envelope import Envelope
from futrisk.messages.orders import Trade, TradeBatch, TradeQuery
from futrisk.messages.risk import CashBatch, CashMovement, QueryCash
from futrisk.messages.topics import RISK_CASH, TD_EXEC, key_of
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.clearing import eodwarn, store
from futrisk.services.clearing.settings import ClearingSettings

TRADE_GATEWAY = "trade-gateway"
DaysFn = Callable[[], Awaitable[list[str]]]


class LocalCopies:
    """Subscribes the copy streams, applies them to clearing.db and repairs expired ranges."""

    def __init__(
        self,
        rt: AsyncServiceRuntime,
        db: Database,
        settings: ClearingSettings,
        *,
        repair_days: DaysFn,
    ) -> None:
        self._rt = rt
        self._db = db
        self._settings = settings
        self._cursors = FactCursors(db)
        self._repair_days = repair_days
        self._repairs: set[asyncio.Task[None]] = set()
        self.trades_applied = 0
        self.cash_applied = 0
        self.repaired_rows = 0

    async def start(self, risk_instances: list[str]) -> None:
        """Subscribe ``td.exec.`` (trade-gateway) and ``risk.cash.`` (each shard) from the persisted cursors."""
        cursors = await self._db.run_read(self._cursors.load_all)
        self._rt.subscribe_fact(
            TRADE_GATEWAY,
            [TD_EXEC],
            policy=FactStartPolicy.FROM_BEGINNING,
            apply=self._apply_exec,
            cursors=cursors,
            on_expired=self._on_expired,
            on_stream_change=self._on_stream_change,
        )
        for inst in risk_instances:
            self._rt.subscribe_fact(
                inst,
                [RISK_CASH],
                policy=FactStartPolicy.FROM_BEGINNING,
                apply=self._apply_cash,
                cursors=cursors,
                on_expired=self._on_expired,
                on_stream_change=self._on_stream_change,
            )

    async def stop(self) -> None:
        for t in list(self._repairs):
            t.cancel()
        for t in list(self._repairs):
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t

    # ------------------------------------------------------------------ apply
    def _apply_exec(self, env: Envelope, cur: CursorUpdate) -> None:
        trade: Trade | None = decode(env.payload, Trade) if env.type == "Trade" else None
        ts = self._rt.market_clock.now_ns()
        rt = self._rt

        def tx(conn: Connection) -> None:
            if trade is not None:
                new = store.copy_trades(conn, [trade])
                if new:
                    eodwarn.flag_late_facts(conn, rt, trades=new, now_ns=ts)
            cur.save(conn, self._cursors, updated_ns=ts)

        self._rt.submit_write(self._db, tx)
        if trade is not None:
            self.trades_applied += 1

    def _apply_cash(self, env: Envelope, cur: CursorUpdate) -> None:
        movement: CashMovement | None = decode(env.payload, CashMovement) if env.type == "CashMovement" else None
        ts = self._rt.market_clock.now_ns()
        src = env.src
        rt = self._rt

        def tx(conn: Connection) -> None:
            if movement is not None:
                new = store.copy_cash(conn, [movement], src)
                if new:
                    eodwarn.flag_late_facts(conn, rt, cash=new, now_ns=ts)
            cur.save(conn, self._cursors, updated_ns=ts)

        self._rt.submit_write(self._db, tx)
        if movement is not None:
            self.cash_applied += 1

    # --------------------------------------------------------------- repairs
    def _on_expired(self, key: FactKey, lo: int, hi: int, done: Callable[[], None]) -> None:
        # 02 §4.6：重放区间已超出窗口 → 领域查询补齐（剩余差异由 EOD 第 3 步兜底）
        logger.warning("copies.replay_expired", src=key[0], topic=key[1], lo=lo, hi=hi)
        self._start_repair(key, done)

    def _on_stream_change(self, key: FactKey, old: str | None, new: str, next_seq: int, done: Callable[[], None]) -> None:
        # 发布方的库重建或从备份恢复：用领域查询核对（02 §8.2 restart_stream）
        logger.warning("copies.stream_changed", src=key[0], topic=key[1], old=old, new=new, next_seq=next_seq)
        self._start_repair(key, done)

    def _start_repair(self, key: FactKey, done: Callable[[], None]) -> None:
        task = asyncio.create_task(self._repair(key, done), name=f"clearing:repair:{key[1]}")
        self._repairs.add(task)
        task.add_done_callback(self._repairs.discard)

    async def _repair(self, key: FactKey, done: Callable[[], None]) -> None:
        src, topic = key
        try:
            days = await self._repair_days()
            if topic.startswith(TD_EXEC):
                n = await self.pull_trades(days, key_of(topic, TD_EXEC))
            elif topic.startswith(RISK_CASH):
                n = await self.pull_cash(src, days, key_of(topic, RISK_CASH))
            else:
                n = 0
            self.repaired_rows += n
            logger.info("copies.repaired", src=src, topic=topic, days=days, rows=n)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("copies.repair_failed", src=src, topic=topic)
        finally:
            done()

    async def pull_trades(self, days: list[str], account_id: str) -> int:
        """``td.query.trades`` for one account over ``days`` into the copy; returns new rows."""
        inserted = 0
        for td in days:
            cursor: int | None = None
            for _ in range(10_000):
                reply = await self._rt.request(
                    TRADE_GATEWAY,
                    "td.query.trades",
                    TradeQuery(trading_day=td, account_id=account_id, from_trade_seq=cursor, limit=500),
                    timeout_ms=self._settings.query_timeout_ms,
                    retries=self._settings.query_retries,
                )
                if not reply.ok:
                    logger.warning("copies.pull_trades_failed", trading_day=td, account_id=account_id, code=reply.code)
                    break
                batch = reply.open_as(TradeBatch)
                rows = [t for t in batch.trades if t.trading_day == td]
                if rows:
                    inserted += await self._db.run_write(_insert_trades_job(self._rt, rows))
                if batch.complete or not batch.trades or batch.next_trade_seq == cursor:
                    break
                cursor = batch.next_trade_seq
        return inserted

    async def pull_cash(self, shard: str, days: list[str], account_id: str) -> int:
        """``risk.query.cash`` of ``shard`` for one account over ``days`` into the copy; returns new rows."""
        inserted = 0
        for td in days:
            reply = await self._rt.request(
                shard,
                "risk.query.cash",
                QueryCash(trading_day=td, account_id=account_id),
                timeout_ms=self._settings.query_timeout_ms,
                retries=self._settings.query_retries,
            )
            if not reply.ok:
                logger.warning("copies.pull_cash_failed", shard=shard, trading_day=td, code=reply.code)
                continue
            batch = reply.open_as(CashBatch)
            items = [c for c in batch.items if c.trading_day == td]
            if items:
                inserted += await self._db.run_write(_insert_cash_job(self._rt, items, shard))
        return inserted


def _insert_trades_job(rt: AsyncServiceRuntime, rows: list[Trade]) -> Callable[[Connection], int]:
    """Writer job: copy pulled trades; new rows of an already settled day are flagged (LATE_FACT)."""
    now = rt.market_clock.now_ns()

    def job(conn: Connection) -> int:
        new = store.copy_trades(conn, rows)
        if new:
            eodwarn.flag_late_facts(conn, rt, trades=new, now_ns=now)
        return len(new)

    return job


def _insert_cash_job(rt: AsyncServiceRuntime, items: list[CashMovement], src: str) -> Callable[[Connection], int]:
    """Writer job: copy pulled cash movements; new rows of an already settled day are flagged (LATE_FACT)."""
    now = rt.market_clock.now_ns()

    def job(conn: Connection) -> int:
        new = store.copy_cash(conn, items, src)
        if new:
            eodwarn.flag_late_facts(conn, rt, cash=new, now_ns=now)
        return len(new)

    return job


__all__ = ["TRADE_GATEWAY", "LocalCopies"]
