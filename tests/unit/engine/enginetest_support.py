"""Test harness for the EngineCore: SimClock, SeqIdGen, an in-memory DB that applies Persist rows, event helpers."""

from __future__ import annotations

import itertools
from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import datetime
from functools import lru_cache
from typing import Any

from futrisk.common.clock import BJ, SimClock, bj_to_ns
from futrisk.common.config import FullConfig, load_default_config
from futrisk.common.config.risk_rules import RiskRulesConfig, RuleDef
from futrisk.common.decimals import D
from futrisk.common.ids import SeqIdGen
from futrisk.core.effects import (
    CancelRoute,
    CommandCtx,
    Effect,
    Persist,
    Publish,
    ReplyTo,
    Request,
    ResolveRoute,
    Route,
    RowOp,
    is_ordered,
)
from futrisk.core.engine import EngineCore
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.risk_account import Lifecycle
from futrisk.messages.base import Msg
from futrisk.messages.enums import (
    BrokerOrderStatus,
    Direction,
    MdQuality,
    Offset,
    OrderType,
    RiskEventKind,
    Role,
    SourceId,
    TradeOrigin,
)
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.eod import SodAccount, SodLot, SodSnapshot
from futrisk.messages.md import InstrumentStatus, MdStatus, Tick
from futrisk.messages.orders import BrokerOrderUpdate, OrderRequest, RouteAck, Trade
from futrisk.messages.risk import AccountStateChanged, RiskEvent

TD = "20260929"  # Tuesday; its night session starts on Monday 2026-09-28 21:00
NEXT_TD = "20260930"
PRE = {"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0, "IF2612": 4500.0, "cu2611": 80000.0, "j2701": 1800.0}


@lru_cache(maxsize=1)
def config() -> FullConfig:
    return load_default_config()


@lru_cache(maxsize=1)
def registry() -> InstrumentRegistry:
    cfg = config()
    return InstrumentRegistry(cfg.products, cfg.calendar)


def ts(text: str) -> int:
    """Beijing ``YYYY-MM-DD HH:MM[:SS]`` -> UTC ns."""
    return bj_to_ns(datetime.fromisoformat(text).replace(tzinfo=BJ))


def rules_only(*rule_ids: str, **overrides: Any) -> RiskRulesConfig:
    """The default risk rules restricted to ``rule_ids`` (field overrides per rule id: ``daily_loss={...}``)."""
    base = config().risk_rules
    rules: list[RuleDef] = []
    for rid in rule_ids:
        r = base.rule(rid)
        assert r is not None, rid
        if rid in overrides:
            r = RuleDef.model_validate({**r.model_dump(), **overrides[rid]})
        rules.append(r)
    return RiskRulesConfig(rules=rules, pretrade=base.pretrade, system=base.system)


def rules_with(rules: Sequence[RuleDef], **pretrade: Any) -> RiskRulesConfig:
    base = config().risk_rules
    pre = base.pretrade.model_copy(update=pretrade) if pretrade else base.pretrade
    return RiskRulesConfig(rules=list(rules), pretrade=pre, system=base.system)


def sod_snapshot(
    capital: dict[str, str] | None = None,
    *,
    trading_day: str = TD,
    version: int = 1,
    lots: dict[str, list[SodLot]] | None = None,
    pre_settle: dict[str, float] | None = None,
) -> SodSnapshot:
    capital = capital or {"ACC3": "10000000", "ACC4": "10000000", "ACC5": "5000000"}
    lots = lots or {}
    return SodSnapshot(
        trading_day=trading_day,
        version=version,
        accounts=[
            SodAccount(account_id=a, pre_balance=f"{D(c):.2f}", initial_capital=f"{D(c):.2f}", cum_net_deposit="0.00",
                       hwm_equity=f"{D(c):.2f}", lots=lots.get(a, []))
            for a, c in sorted(capital.items())
        ],
        pre_settle={k: str(v) for k, v in (pre_settle or PRE).items()},
    )


class FakeDb:
    """Applies Persist rows like the shell would (enough semantics for restore tests)."""

    AUTO = {"order_events": "event_id", "audit_events": "audit_id", "firm_controls": "control_id", "fills": "fill_seq"}

    def __init__(self) -> None:
        self.tables: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.outbox: list[Any] = []
        self._seq: dict[str, itertools.count[int]] = defaultdict(lambda: itertools.count(1))
        self.critical_labels: list[str] = []

    @staticmethod
    def _match(row: dict[str, Any], key: tuple[str, ...], values: dict[str, Any]) -> bool:
        return all(row.get(k) == values.get(k) for k in key)

    def apply(self, p: Persist) -> None:
        if p.critical:
            self.critical_labels.append(p.label)
        for r in p.rows:
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
        self.outbox.extend(p.outbox)

    def snapshot(self) -> dict[str, list[dict[str, Any]]]:
        return {k: [dict(x) for x in v] for k, v in self.tables.items()}


class Harness:
    """One engine, its clock, ids and fake database; every effect list goes through :meth:`run`."""

    def __init__(
        self,
        rules: RiskRulesConfig | None = None,
        *,
        shard: int = 1,
        coordinator: bool = False,
        start: str = "2026-09-29 09:30",
        sod: SodSnapshot | None = None,
        load: bool = True,
        lifecycle: Lifecycle = Lifecycle.RUNNING,
        ids_prefix: str = "id",
    ) -> None:
        cfg = config()
        self.clock = SimClock(ts(start))
        self.ids = SeqIdGen(ids_prefix)
        self.rules = rules if rules is not None else cfg.risk_rules
        self.eng = EngineCore(
            shard=shard,
            registry=registry(),
            rules=self.rules,
            accounts=cfg.accounts,
            clock=self.clock,
            ids=self.ids,
            calendar=cfg.calendar,
            coordinator=coordinator,
            config_hash="cfg-hash",
        )
        self.db = FakeDb()
        self.effects: list[Effect] = []
        self.volume: dict[str, int] = defaultdict(int)
        self._coid = itertools.count(1)
        self._trade = itertools.count(1)
        self._bseq = itertools.count(1)
        self.last_px: dict[str, float] = dict(PRE)
        if load:
            self.run(self.eng.load_sod(sod or sod_snapshot()))
            self.run(self.eng.set_lifecycle(lifecycle))

    # ------------------------------------------------------------- plumbing
    def run(self, effects: list[Effect]) -> list[Effect]:
        assert is_ordered(effects), [type(e).__name__ for e in effects]
        assert sum(isinstance(e, Persist) for e in effects) <= 1
        for e in effects:
            if isinstance(e, Persist):
                self.db.apply(e)
        self.effects.extend(effects)
        return effects

    def advance(self, ms: int) -> None:
        self.clock.advance(ms * 1_000_000)

    def at(self, text: str) -> None:
        self.clock.set(ts(text))

    def timer(self) -> list[Effect]:
        return self.run(self.eng.on_timer(self.clock.now_ns()))

    def evaluate(self) -> list[Effect]:
        return self.run(self.eng.evaluate_dirty())

    # ---------------------------------------------------------- market data
    def make_tick(self, inst: str, px: float, *, trading_day: str = TD, flags: int = 0) -> Tick:
        self.volume[inst] += 1
        info = registry().get(inst)
        pre = D(PRE.get(inst, px))
        up, lo = registry().limit_prices(inst, pre, trading_day)
        return Tick(instrument_id=inst, exchange=info.exchange, trading_day=trading_day, action_day=trading_day,
                    exch_ts_ns=self.clock.now_ns(), last=px, volume=self.volume[inst], turnover=0.0, open_interest=0,
                    bid1=px - float(info.tick_size), ask1=px + float(info.tick_size), upper_limit=float(up),
                    lower_limit=float(lo), pre_settle=float(pre), source=SourceId.FEED_A, recv_ns=self.clock.now_ns(),
                    flags=flags)

    def tick(self, inst: str, px: float, *, evaluate: bool = True, **kw: Any) -> list[Effect]:
        self.last_px[inst] = px
        self.eng.apply_mark(self.make_tick(inst, px, **kw))
        return self.evaluate() if evaluate else []

    def ticks(self, prices: dict[str, float]) -> list[Effect]:
        """One market state: several instruments move in the same loop iteration."""
        for inst, px in prices.items():
            self.last_px[inst] = px
            self.eng.apply_mark(self.make_tick(inst, px))
        return self.evaluate()

    def md_status(self, qualities: dict[str, MdQuality]) -> list[Effect]:
        st = MdStatus(as_of_ns=self.clock.now_ns(), trading_day=TD, sources=[],
                      instruments=[InstrumentStatus(instrument_id=i, quality=q) for i, q in qualities.items()])
        self.eng.apply_md_status(st)
        return self.evaluate()

    # ------------------------------------------------------------- commands
    def cmd(self, command: str, payload: Msg, *, token: Any = None) -> tuple[Reply, list[Effect]]:
        effs = self.run(self.eng.handle(payload, CommandCtx(command, token=token or command, requester="test",
                                                            received_ns=self.clock.now_ns())))
        replies = [e.reply for e in effs if isinstance(e, ReplyTo)]
        assert replies, f"{command} produced no reply"
        return replies[-1], effs

    def submit(
        self,
        account: str,
        inst: str,
        direction: Direction,
        offset: Offset,
        qty: int,
        price: float | None = None,
        *,
        order_type: OrderType = OrderType.LIMIT,
        coid: str | None = None,
    ) -> tuple[Reply, list[Effect]]:
        if price is None and order_type == OrderType.LIMIT:
            price = self.last_px[inst]
        req = OrderRequest(client_order_id=coid or f"o{next(self._coid)}", account_id=account, instrument_id=inst,
                           direction=direction, offset=offset, order_type=order_type, price=price, qty=qty,
                           sent_ns=self.clock.now_ns())
        return self.cmd("order.submit", req)

    def route_ok(self, coid: str) -> list[Effect]:
        return self.run(self.eng.on_route_ack(coid, ok_reply(RouteAck(client_order_id=coid, status=BrokerOrderStatus.RECEIVED,
                                                                        broker_order_ref=f"ref-{coid}"))))

    def route_err(self, coid: str, code: str) -> list[Effect]:
        return self.run(self.eng.on_route_ack(coid, err_reply(code, data=RouteAck(client_order_id=coid, status=BrokerOrderStatus.EXPIRED))))

    def broker(self, coid: str, status: BrokerOrderStatus, *, filled: int = 0, remaining: int = 0,
               code: str | None = None) -> list[Effect]:
        o = self.eng.orders.get(coid)
        assert o is not None
        u = BrokerOrderUpdate(client_order_id=coid, account_id=o.account_id, instrument_id=o.instrument_id,
                              broker_order_ref=f"ref-{coid}", status=status, filled_qty=filled, remaining_qty=remaining,
                              broker_code=code, broker_seq=next(self._bseq), trading_day=TD, updated_ns=self.clock.now_ns())
        return self.run(self.eng.apply_exec(u))

    def trade(self, account: str, inst: str, direction: Direction, offset: Offset, px: float, qty: int, *,
              coid: str | None = None, origin: TradeOrigin = TradeOrigin.INTERNAL, trading_day: str = TD,
              trade_id: str | None = None) -> Trade:
        n = next(self._trade)
        info = registry().get(inst)
        return Trade(trading_day=trading_day, exchange=info.exchange, trade_id=trade_id or f"T{n}", direction=direction,
                     account_id=account, instrument_id=inst, client_order_id=coid, origin=origin,
                     broker_order_ref=f"ref-{coid or n}", offset=offset, price=px, qty=qty,
                     trade_ts_ns=self.clock.now_ns(), broker_seq=next(self._bseq), trade_seq=n,
                     received_ns=self.clock.now_ns())

    def fill(self, coid: str, px: float | None = None, qty: int | None = None) -> list[Effect]:
        o = self.eng.orders.get(coid)
        assert o is not None
        t = self.trade(o.account_id, o.instrument_id, o.direction, o.offset, px if px is not None else (o.price or 0.0),
                       qty if qty is not None else o.remaining, coid=coid)
        return self.run(self.eng.apply_exec(t))

    def open_position(self, account: str, inst: str, direction: Direction, qty: int, px: float | None = None) -> str:
        """Submit, route, accept and fill an opening order at ``px`` (default: the last price)."""
        reply, _ = self.submit(account, inst, direction, Offset.OPEN, qty, px)
        assert reply.ok, (reply.code, reply.open())
        coid = reply.open().client_order_id  # type: ignore[union-attr]
        self.route_ok(coid)
        self.broker(coid, BrokerOrderStatus.ACCEPTED, remaining=qty)
        self.fill(coid)
        self.evaluate()
        return str(coid)

    # -------------------------------------------------------------- queries
    def outbox_payloads(self, effects: Iterable[Effect] | None = None) -> list[Msg]:
        out: list[Msg] = []
        for e in self.effects if effects is None else effects:
            if isinstance(e, Persist):
                out.extend(m.payload for m in e.outbox)
        return out

    def events(self, kind: RiskEventKind | None = None, effects: Iterable[Effect] | None = None) -> list[RiskEvent]:
        evs = [p for p in self.outbox_payloads(effects) if isinstance(p, RiskEvent)]
        return [e for e in evs if kind is None or e.kind == kind]

    def state_changes(self, account: str | None = None, effects: Iterable[Effect] | None = None) -> list[AccountStateChanged]:
        out = [p for p in self.outbox_payloads(effects) if isinstance(p, AccountStateChanged)]
        return [p for p in out if account is None or p.account_id == account]

    @staticmethod
    def of_type(effects: Iterable[Effect], typ: type[Any]) -> list[Any]:
        return [e for e in effects if isinstance(e, typ)]

    def cancels(self, effects: Iterable[Effect]) -> list[CancelRoute]:
        return self.of_type(effects, CancelRoute)

    def routes(self, effects: Iterable[Effect]) -> list[Route]:
        return self.of_type(effects, Route)

    def requests(self, effects: Iterable[Effect]) -> list[Request]:
        return self.of_type(effects, Request)

    def resolves(self, effects: Iterable[Effect]) -> list[ResolveRoute]:
        return self.of_type(effects, ResolveRoute)

    def publishes(self, effects: Iterable[Effect]) -> list[Publish]:
        return self.of_type(effects, Publish)


RM = Role.RISK_MANAGER
ADMIN = Role.ADMIN
B, S = Direction.BUY, Direction.SELL
O, C, CT, CY = Offset.OPEN, Offset.CLOSE, Offset.CLOSE_TODAY, Offset.CLOSE_YESTERDAY  # noqa: E741
