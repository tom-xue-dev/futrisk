"""EngineCore driven in-process, every Persist applied to the REAL risk-{shard}.db schema (RiskStore) and to an
independent in-memory reference (:class:`RefDb`, the semantics of ENGINE.md §6), so tests can compare both."""

from __future__ import annotations

import itertools
import json
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import msgspec

from futrisk.common.clock import SimClock, bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.common.decimals import D
from futrisk.common.ids import SeqIdGen
from futrisk.core.effects import CommandCtx, Effect, Persist, ReplyTo, RowOp, is_ordered
from futrisk.core.engine import EngineCore
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.risk_account import Lifecycle
from futrisk.db import Database, Outbox
from futrisk.messages.base import Msg
from futrisk.messages.enums import BrokerOrderStatus, Direction, Offset, OrderType, SourceId
from futrisk.messages.envelope import Reply, ok_reply
from futrisk.messages.eod import SodSnapshot
from futrisk.messages.md import Tick
from futrisk.messages.orders import BrokerOrderUpdate, OrderRequest, RouteAck, Trade
from futrisk.services.risk_engine.schema import METADATA, MIGRATIONS
from futrisk.services.risk_engine.store import RiskStore, table_rows

from .re_support import MARKET_OPEN, PRE, TD


class RefDb:
    """Applies Persist rows with the documented op semantics (reference for the SQL writer)."""

    AUTO = {"order_events": "event_id", "audit_events": "audit_id", "firm_controls": "control_id", "fills": "fill_seq"}

    def __init__(self) -> None:
        self.tables: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self._seq: dict[str, itertools.count[int]] = defaultdict(lambda: itertools.count(1))
        self.ops: set[tuple[str, RowOp]] = set()

    @staticmethod
    def _match(row: dict[str, Any], key: tuple[str, ...], values: dict[str, Any]) -> bool:
        return all(row.get(k) == values.get(k) for k in key)

    def apply(self, p: Persist) -> None:
        for r in p.rows:
            self.ops.add((r.table, r.op))
            vals = dict(r.values)
            rows = self.tables[r.table]
            if r.op in (RowOp.INSERT, RowOp.INSERT_IGNORE):
                if r.op == RowOp.INSERT_IGNORE and any(self._match(x, r.key, vals) for x in rows):
                    continue
                auto = self.AUTO.get(r.table)
                if auto is not None and auto not in vals:
                    vals[auto] = next(self._seq[r.table])
                rows.append(vals)
            elif r.op == RowOp.UPSERT:
                for x in rows:
                    if self._match(x, r.key, vals):
                        x.update(vals)
                        break
                else:
                    rows.append(vals)
            elif r.op == RowOp.UPDATE:
                for x in rows:
                    if self._match(x, r.key, vals):
                        x.update({k: v for k, v in vals.items() if k not in r.key})
            elif r.op == RowOp.DELETE:
                self.tables[r.table] = [x for x in rows if not self._match(x, r.key, vals)]


def norm(value: Any) -> Any:
    """Comparable form of a column value (JSON round trip, bools as ints, floats rounded)."""
    if isinstance(value, bool):
        return float(value)  # BOOL columns come back as 0/1
    if isinstance(value, float) or (isinstance(value, int) and abs(value) < 2**52):
        return round(float(value), 6)  # REAL columns may receive ints (0) from the core
    if isinstance(value, dict | list | tuple):
        return json.loads(msgspec.json.encode(value))
    if isinstance(value, bytes | bytearray):
        return bytes(value)
    if hasattr(value, "value") and isinstance(getattr(value, "value", None), str):
        return value.value
    return value


class CoreHarness:
    """One EngineCore (SimClock, SeqIdGen), a real risk DB and a reference DB."""

    def __init__(self, cfg: FullConfig, db_path: Path, *, shard: int = 1, start_ns: int | None = None,
                 coordinator: bool = False) -> None:
        self.cfg = cfg
        self.clock = SimClock(start_ns if start_ns is not None else bj_to_ns(MARKET_OPEN))
        self.ids = SeqIdGen("id")
        self.registry = InstrumentRegistry(cfg.products, cfg.calendar)
        self.core = EngineCore(shard=shard, registry=self.registry, rules=cfg.risk_rules, accounts=cfg.accounts,
                               clock=self.clock, ids=self.ids, calendar=cfg.calendar, coordinator=coordinator,
                               config_hash="cfg-hash")
        self.db = Database(db_path, METADATA, migrations=MIGRATIONS, synchronous="NORMAL")
        self.db.open()
        self.outbox = Outbox(self.db, instance=f"risk-engine-{shard}")
        self.store = RiskStore(self.outbox, epoch="test-epoch", instance=f"risk-engine-{shard}")
        self.ref = RefDb()
        self.effects: list[Effect] = []
        self.facts: list[tuple[str, Msg]] = []
        self.volume: dict[str, int] = defaultdict(int)
        self._coid = itertools.count(1)
        self._trade = itertools.count(1)
        self._bseq = itertools.count(1)

    def close(self) -> None:
        self.db.close()

    # ------------------------------------------------------------------ plumbing
    def run(self, effects: list[Effect]) -> list[Effect]:
        assert is_ordered(effects), [type(e).__name__ for e in effects]
        for e in effects:
            if isinstance(e, Persist):
                self.db.write(lambda c, p=e: self.store.apply_persist(c, p, now_ns=self.clock.now_ns()),
                              critical=e.critical)
                self.ref.apply(e)
                self.facts.extend((m.topic, m.payload) for m in e.outbox)
        self.effects.extend(effects)
        return effects

    def advance(self, ms: int) -> None:
        self.clock.advance(ms * 1_000_000)

    def load(self, sod: SodSnapshot, lifecycle: Lifecycle = Lifecycle.RUNNING) -> None:
        self.run(self.core.load_sod(sod))
        self.run(self.core.set_lifecycle(lifecycle))

    def db_rows(self, table: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = self.db.read(lambda c: table_rows(c, table))
        return rows

    # ------------------------------------------------------------------ inputs
    def tick(self, prices: dict[str, float], *, trading_day: str = TD) -> list[Effect]:
        for inst, px in prices.items():
            self.volume[inst] += 1
            info = self.registry.get(inst)
            pre = D(PRE.get(inst, px))
            up, lo = self.registry.limit_prices(inst, pre, trading_day)
            now = self.clock.now_ns()
            self.core.apply_mark(Tick(instrument_id=inst, exchange=info.exchange, trading_day=trading_day,
                                      action_day=trading_day, exch_ts_ns=now, last=px, volume=self.volume[inst],
                                      turnover=0.0, open_interest=0, upper_limit=float(up), lower_limit=float(lo),
                                      pre_settle=float(pre), source=SourceId.FEED_A, recv_ns=now))
        return self.run(self.core.evaluate_dirty())

    def cmd(self, command: str, payload: Msg) -> tuple[Reply, list[Effect]]:
        effs = self.run(self.core.handle(payload, CommandCtx(command, token=command, requester="test",
                                                             received_ns=self.clock.now_ns())))
        replies = [e.reply for e in effs if isinstance(e, ReplyTo)]
        assert replies, f"{command} produced no reply"
        return replies[-1], effs

    def submit(self, account: str, inst: str, direction: Direction, offset: Offset, qty: int, price: float | None,
               *, coid: str | None = None) -> tuple[Reply, list[Effect]]:
        req = OrderRequest(client_order_id=coid or f"o{next(self._coid)}", account_id=account, instrument_id=inst,
                           direction=direction, offset=offset, order_type=OrderType.LIMIT, price=price, qty=qty,
                           sent_ns=self.clock.now_ns())
        return self.cmd("order.submit", req)

    def route_ok(self, coid: str) -> list[Effect]:
        return self.run(self.core.on_route_ack(coid, ok_reply(RouteAck(client_order_id=coid,
                                                                        status=BrokerOrderStatus.SUBMITTED,
                                                                        broker_order_ref=f"ref-{coid}"))))

    def broker(self, coid: str, status: BrokerOrderStatus, *, filled: int = 0, remaining: int = 0) -> list[Effect]:
        o = self.core.orders.get(coid)
        u = BrokerOrderUpdate(client_order_id=coid, account_id=o.account_id, instrument_id=o.instrument_id,
                              broker_order_ref=f"ref-{coid}", status=status, filled_qty=filled, remaining_qty=remaining,
                              broker_seq=next(self._bseq), trading_day=o.trading_day, updated_ns=self.clock.now_ns())
        return self.run(self.core.apply_exec(u))

    def trade(self, account: str, inst: str, direction: Direction, offset: Offset, px: float, qty: int, *,
              coid: str | None = None, trading_day: str = TD) -> Trade:
        n = next(self._trade)
        return Trade(trading_day=trading_day, exchange=self.registry.get(inst).exchange, trade_id=f"T{n}",
                     direction=direction, account_id=account, instrument_id=inst, client_order_id=coid,
                     broker_order_ref=f"ref-{coid or n}", offset=offset, price=px, qty=qty,
                     trade_ts_ns=self.clock.now_ns(), broker_seq=next(self._bseq), trade_seq=n,
                     received_ns=self.clock.now_ns())

    def fill(self, coid: str, px: float | None = None, qty: int | None = None) -> list[Effect]:
        o = self.core.orders.get(coid)
        t = self.trade(o.account_id, o.instrument_id, o.direction, o.offset, px if px is not None else float(o.price or 0),
                       qty if qty is not None else o.remaining, coid=coid, trading_day=o.trading_day)
        return self.run(self.core.apply_exec(t))

    def facts_of(self, topic_prefix: str) -> list[Msg]:
        return [m for t, m in self.facts if t.startswith(topic_prefix)]


def compare_tables(h: CoreHarness, tables: Iterable[str]) -> dict[str, int]:
    """Assert the real DB equals the reference for ``tables``; returns row counts."""
    counts: dict[str, int] = {}
    for table in tables:
        real = h.db_rows(table)
        ref = h.ref.tables.get(table, [])
        assert len(real) == len(ref), (table, len(real), len(ref))
        cols = sorted({k for r in ref for k in r})

        def key(r: dict[str, Any], cols: list[str] = cols) -> str:
            return json.dumps([norm(r.get(c)) for c in cols], sort_keys=True, default=str)

        real_n = sorted(key({c: r.get(c) for c in cols}) for r in real)
        ref_n = sorted(key(r) for r in ref)
        assert real_n == ref_n, f"{table} differs:\nreal={real_n[:3]}\nref ={ref_n[:3]}"
        counts[table] = len(real)
    return counts


__all__ = ["CoreHarness", "RefDb", "compare_tables", "norm"]
