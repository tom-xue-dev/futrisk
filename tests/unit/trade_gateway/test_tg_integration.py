"""trade-gateway end to end with the real sim-exchange SimBroker and a real sync runtime (02 §6.4, §8.3, §9.4).

A DEALER client with identity ``risk-engine-1`` routes orders, ``api-gateway`` flips the kill switch, a SUB
client watches ``td.exec.*``; FACT sequences are read back with ``ctl.replay``. A fake coordinator drives the
``risk.control.firm`` fallbacks (replay window exceeded, stream change) and a fake clearing-worker the
``sys.eod`` maintenance trigger.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import zmq
from sqlalchemy import MetaData, Table
from tgint_support import (
    TG,
    FakeCoordinator,
    FakePublisher,
    LoopThread,
    SimThread,
    TgThread,
    body,
    client,
    exec_facts,
    wait_for,
    with_broker_endpoint,
)

from futrisk.bus import BlockingRpcClient, Subscriber
from futrisk.common.clock import MarketClock
from futrisk.common.config import FullConfig
from futrisk.common.ids import uuid7
from futrisk.db import add_common_tables
from futrisk.db.types import id_col
from futrisk.messages import codes, topics
from futrisk.messages.common import SnapshotBatch, SnapshotRequest, StatusRequest
from futrisk.messages.enums import (
    BrokerConnState,
    BrokerOrderStatus,
    ControlOrigin,
    Direction,
    EodPhase,
    Exchange,
    FirmAction,
    Offset,
    OrderType,
    PosDirection,
    RestrictionKind,
    Role,
    ServiceState,
    TimeInForce,
    TradeOrigin,
)
from futrisk.messages.envelope import Envelope, decode_envelope
from futrisk.messages.eod import EodPhaseEvent
from futrisk.messages.orders import (
    BrokerOrderBatch,
    BrokerOrderUpdate,
    BrokerPositionBatch,
    BrokerStatus,
    DigestRequest,
    OrderQuery,
    PositionQuery,
    RouteAck,
    RouteCancel,
    RouteOrder,
    RouteResolve,
    Trade,
    TradeBatch,
    TradeDigest,
    TradeQuery,
    compute_account_digest,
)
from futrisk.messages.risk import FirmControl, KillSwitchRequest, Restriction
from futrisk.messages.system import Heartbeat
from futrisk.runtime import SyncServiceRuntime
from futrisk.runtime.testing import config_with_free_ports
from futrisk.sim.exchange.testing import MARKET_OPEN, make_test_config, offset_for

pytestmark = pytest.mark.timeout(60)
TD = "20260928"
SEC = 1_000_000_000


@dataclass
class Env:
    sim: SimThread
    tg: TgThread
    cfg: FullConfig
    clock: MarketClock
    home: Path
    rc: BlockingRpcClient  # risk-engine-1
    api: BlockingRpcClient  # api-gateway
    ctx: Any

    def route(
        self,
        coid: str,
        account: str,
        instrument: str,
        exchange: Exchange,
        direction: Direction,
        offset: Offset,
        qty: int,
        *,
        price: float | None = None,
        ttl_ns: int = 2 * SEC,
        now: int | None = None,
    ) -> Any:
        now = self.clock.now_ns() if now is None else now
        if price is None:
            up, lo = self.sim.limits(instrument)
            price = up if direction is Direction.BUY else lo  # always marketable
        req = RouteOrder(client_order_id=coid, account_id=account, instrument_id=instrument, exchange=exchange,
                         direction=direction, offset=offset, order_type=OrderType.LIMIT, tif=TimeInForce.GFD,
                         price=price, qty=qty, trading_day=TD, risk_passed_ns=now, expire_ns=now + ttl_ns,
                         src_instance="risk-engine-1")
        return self.rc.request(TG, "route.order", req, timeout_ms=3000, retries=0)

    def filled(self, account: str, coid: str, timeout: float = 10.0) -> list[Envelope]:
        """Wait until ``coid`` is FILLED on ``td.exec.{account}``; returns the whole stream."""

        def done() -> list[Envelope] | None:
            envs = exec_facts(self.rc, account)
            for e in envs:
                m = body(e)
                if isinstance(m, BrokerOrderUpdate) and m.client_order_id == coid and m.status is BrokerOrderStatus.FILLED:
                    return envs
            return None

        return wait_for(done, timeout, 0.02, f"{coid} filled")  # type: ignore[no-any-return]


def start_env(tmp_path: Path, default_config: FullConfig, sim: SimThread | None = None, **svc_kw: Any) -> Env:
    offset = offset_for(MARKET_OPEN)
    if sim is None:
        sim = SimThread(make_test_config(default_config), tmp_path / "sim", offset)
    cfg = with_broker_endpoint(config_with_free_ports(default_config, heartbeat_ms=100), sim.broker_endpoint)
    clock = MarketClock(mode="DEMO", offset_ns=offset)
    tg = TgThread(cfg, tmp_path / "tg", clock, **{"broker_heartbeat_timeout_ms": 2_500, **svc_kw}).start()
    return Env(sim=sim, tg=tg, cfg=cfg, clock=clock, home=tmp_path, rc=client(cfg, "risk-engine-1"),
               api=client(cfg, "api-gateway"), ctx=zmq.Context())


def close_env(env: Env) -> None:
    env.rc.close()
    env.api.close()
    env.ctx.destroy(linger=0)
    env.tg.stop()
    env.sim.close()


@pytest.fixture
def env(tmp_path: Path, default_config: FullConfig) -> Iterator[Env]:
    e = start_env(tmp_path, default_config)
    try:
        yield e
    finally:
        close_env(e)


def ack(reply: Any) -> RouteAck:
    return reply.open_as(RouteAck)  # type: ignore[no-any-return]


def status(env: Env) -> Heartbeat:
    hb = env.rc.request(TG, "ctl.status", StatusRequest(), timeout_ms=3000).open_as(Heartbeat)
    assert isinstance(hb, Heartbeat)
    return hb


def kill_switch(env: Env, on: bool, role: Role = Role.RISK_MANAGER) -> Any:
    req = KillSwitchRequest(command_id=uuid7(), on=on, reason="drill", actor="rm", role=role)
    return env.api.request(TG, "firm.kill_switch", req, timeout_ms=3000, retries=0)


def rule_control(env: Env, on: bool) -> FirmControl:
    """A legacy RULE kill switch record (KILL_SWITCH_ON / OFF)."""
    return FirmControl(action=FirmAction.KILL_SWITCH_ON if on else FirmAction.KILL_SWITCH_OFF, origin=ControlOrigin.RULE,
                       active=on, trading_day=TD, reason="firm daily loss", ts_ns=env.clock.now_ns())


def rule_state(env: Env, on: bool, restrictions: list[Restriction] | None = None) -> FirmControl:
    """The coordinator's whole RULE layer in one fact (FirmAction.RULE_STATE)."""
    return FirmControl(action=FirmAction.RULE_STATE, origin=ControlOrigin.RULE, active=on,
                       restrictions=list(restrictions or []), trading_day=TD, reason="firm rules", ts_ns=env.clock.now_ns())


BLOCK_OPEN = Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key="firm", source="firm.daily_loss", since_ns=1)


def td_rows(env: Env, sql: str) -> list[tuple[Any, ...]]:
    con = sqlite3.connect(env.home / "tg" / "db" / "td.db")
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


# ------------------------------------------------------------------------------------------------
def test_route_fill_publishes_the_exec_fact_sequence_and_digest(env: Env) -> None:
    hb = wait_for(lambda: (lambda h: h if h.state is ServiceState.RUNNING else None)(status(env)), what="RUNNING")
    assert hb.deps["broker"] == "UP" and hb.detail["kill_switch"] == {"manual": False, "rule": False, "effective": False}
    sub = Subscriber(env.ctx)
    sub.connect(TG, env.cfg.services.instance(TG).pub or "", ["td.exec.ACC3", f"sys.heartbeat.{TG}"])
    assert wait_for(lambda: sub.recv(200), what="SUB connected")  # a heartbeat: the subscription is live
    r = env.route("c-fill-1", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 2)
    assert r.ok, r
    a = ack(r)
    assert a.status is BrokerOrderStatus.SUBMITTED and a.broker_order_ref and a.broker_order_ref.startswith("1:")

    envs = env.filled("ACC3", "c-fill-1")
    assert [e.seq for e in envs] == list(range(1, len(envs) + 1))
    msgs = [body(e) for e in envs]
    assert [e.type for e in envs][0] == "BrokerOrderUpdate" and msgs[0].status is BrokerOrderStatus.ACCEPTED  # type: ignore[attr-defined]
    assert isinstance(msgs[-1], BrokerOrderUpdate) and msgs[-1].status is BrokerOrderStatus.FILLED
    broker_seqs = [m.broker_seq for m in msgs]  # type: ignore[attr-defined]
    assert broker_seqs == sorted(broker_seqs) and len(set(broker_seqs)) == len(broker_seqs)
    trades = [m for m in msgs if isinstance(m, Trade)]
    assert trades and sum(t.qty for t in trades) == 2 and [t.trade_seq for t in trades] == list(range(1, len(trades) + 1))
    for t in trades:
        assert t.origin is TradeOrigin.INTERNAL and t.client_order_id == "c-fill-1" and t.offset is Offset.OPEN
        assert t.broker_order_ref == a.broker_order_ref and t.account_id == "ACC3"
    first_trade = next(i for i, m in enumerate(msgs) if isinstance(m, Trade))
    assert isinstance(msgs[first_trade + 1], BrokerOrderUpdate)  # a fill: RtnTrade first, then the order status

    live: list[Envelope] = []
    wait_for(lambda: live.extend(e for e in sub.drain() if e.topic == "td.exec.ACC3") or len(live) >= len(envs),
             what="live td.exec delivery")
    assert [(e.seq, e.type) for e in live] == [(e.seq, e.type) for e in envs]
    sub.close()

    digest = env.rc.request(TG, "td.digest", DigestRequest(trading_day=TD)).open_as(TradeDigest)
    assert digest.accounts == [compute_account_digest("ACC3", trades)] and digest.total_count == len(trades)
    batch = env.rc.request(TG, "td.query.trades", TradeQuery(trading_day=TD, account_id="ACC3")).open_as(TradeBatch)
    assert batch.complete and [t.trade_id for t in batch.trades] == [t.trade_id for t in trades]

    pos = wait_for(lambda: env.rc.request(TG, "td.query.positions", PositionQuery(trading_day=TD, account_id="ACC3")))
    items = pos.open_as(BrokerPositionBatch).items
    assert [(p.instrument_id, p.direction, p.td_qty, p.yd_qty) for p in items] == [("rb2701", PosDirection.LONG, 2, 0)]

    snap = env.rc.request(TG, "ctl.snapshot", SnapshotRequest(prefixes=["td.status"])).open_as(SnapshotBatch)
    st = body(decode_envelope(snap.items[0].envelope))
    assert isinstance(st, BrokerStatus) and st.state is BrokerConnState.LOGGED_IN and st.broker == "sim"
    orders = env.rc.request(TG, "td.query.orders", OrderQuery(trading_day=TD, client_order_ids=["c-fill-1"]))
    item = orders.open_as(BrokerOrderBatch).items[0]
    assert item.status is BrokerOrderStatus.FILLED and item.filled_qty == 2 and item.broker_seq == broker_seqs[-1]


def test_expired_route_is_rejected_tombstoned_and_never_sent(env: Env) -> None:
    before = env.sim.broker_state()["orders_today"]
    r = env.route("c-exp-1", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1,
                  now=env.clock.now_ns() - 3 * SEC)
    assert r.code == codes.ROUTE_EXPIRED and ack(r).status is BrokerOrderStatus.EXPIRED
    retry = env.route("c-exp-1", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1)
    assert retry.code == codes.ROUTE_EXPIRED and retry.duplicate
    q = env.rc.request(TG, "td.query.orders", OrderQuery(trading_day=TD, client_order_ids=["c-exp-1"]))
    item = q.open_as(BrokerOrderBatch).items[0]
    assert item.status is BrokerOrderStatus.EXPIRED and item.broker_code == codes.ROUTE_EXPIRED
    assert env.sim.broker_state()["orders_today"] == before
    # whitelist (02 §4.11): route.* only from risk-engine-*
    other = client(env.cfg, "api-gateway")
    try:
        denied = other.request(TG, "route.resolve", RouteResolve(client_order_id="x", trading_day=TD), retries=0)
        assert denied.code == codes.FORBIDDEN
    finally:
        other.close()


def test_resolve_of_a_never_received_order_makes_a_late_route_lost(env: Env) -> None:
    rr = env.rc.request(TG, "route.resolve", RouteResolve(client_order_id="c-lost-1", trading_day=TD))
    assert rr.code == codes.ROUTE_LOST and ack(rr).status is BrokerOrderStatus.EXPIRED
    late = env.route("c-lost-1", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1)
    assert late.code == codes.ROUTE_LOST and ack(late).status is BrokerOrderStatus.EXPIRED
    q = env.rc.request(TG, "td.query.orders", OrderQuery(trading_day=TD, client_order_ids=["c-lost-1"]))
    assert q.open_as(BrokerOrderBatch).items == []  # no routed_orders row, only the tombstone


def test_kill_switch_blocks_opens_but_not_closes(env: Env) -> None:
    assert env.route("c-ks-open", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1).ok
    env.filled("ACC3", "c-ks-open")
    on = kill_switch(env, True)
    fc = on.open_as(FirmControl)
    assert on.ok and fc.action is FirmAction.KILL_SWITCH_ON and fc.origin is ControlOrigin.MANUAL and fc.active
    assert kill_switch(env, True, Role.TRADER).code == codes.FORBIDDEN
    impostor = env.rc.request(TG, "firm.kill_switch", KillSwitchRequest(command_id=uuid7(), on=False, reason="x",
                                                                       actor="bot", role=Role.ADMIN), retries=0)
    assert impostor.code == codes.FORBIDDEN  # only api-gateway may send it
    blocked = env.route("c-ks-open-2", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1)
    assert blocked.code == codes.ROUTE_KILL_SWITCH and ack(blocked).status is BrokerOrderStatus.EXPIRED
    close = env.route("c-ks-close", "ACC3", "rb2701", Exchange.SHFE, Direction.SELL, Offset.CLOSE_TODAY, 1)
    assert close.ok and ack(close).status is BrokerOrderStatus.SUBMITTED
    env.filled("ACC3", "c-ks-close")
    assert kill_switch(env, False).ok
    assert env.route("c-ks-open-3", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1).ok


def test_broker_force_close_publishes_an_external_trade(env: Env) -> None:
    assert env.route("c-fc-open", "ACC4", "IF2612", Exchange.CFFEX, Direction.BUY, Offset.OPEN, 2).ok
    n = len(env.filled("ACC4", "c-fc-open"))
    res = env.sim.force_close("ACC4", "IF2612", 1)

    def external() -> list[Envelope] | None:
        envs = exec_facts(env.rc, "ACC4")
        return envs if any(isinstance(body(e), Trade) and body(e).origin is TradeOrigin.EXTERNAL for e in envs) else None  # type: ignore[attr-defined]

    envs = wait_for(external, what="external trade")
    wait_for(lambda: len(exec_facts(env.rc, "ACC4")) >= n + 3, what="force-close order reports")
    new = [body(e) for e in exec_facts(env.rc, "ACC4")[n:]]
    assert [type(m).__name__ for m in new] == ["BrokerOrderUpdate", "Trade", "BrokerOrderUpdate"]
    t = new[1]
    assert isinstance(t, Trade) and t.origin is TradeOrigin.EXTERNAL and t.client_order_id is None
    assert t.offset is Offset.FORCE_CLOSE and t.trade_id == res["trade_id"] and t.qty == 1
    assert t.direction is Direction.SELL and t.broker_order_ref.startswith("1:0:FC")
    assert all(isinstance(m, BrokerOrderUpdate) and m.origin is TradeOrigin.EXTERNAL for m in (new[0], new[2]))
    assert new[2].status is BrokerOrderStatus.FILLED  # type: ignore[union-attr]
    assert envs


def test_restart_resyncs_without_duplicates_and_keeps_trade_seq_continuous(tmp_path: Path, default_config: FullConfig) -> None:
    env = start_env(tmp_path, default_config)
    try:
        assert env.route("c-rs-open", "ACC5", "rb2705", Exchange.SHFE, Direction.BUY, Offset.OPEN, 2).ok
        before = env.filled("ACC5", "c-rs-open")
        n_trades = sum(1 for e in before if e.type == "Trade")
        env.tg.stop()
        assert env.tg.exit_code == 0
        env.sim.force_close("ACC5", "rb2705", 1)  # happens while trade-gateway is down
        con = sqlite3.connect(tmp_path / "tg" / "db" / "td.db")
        con.execute("UPDATE broker_cursors SET last_broker_seq = 0")  # a lagging cursor: everything is fetched again
        con.commit()
        con.close()
        env.rc.close()
        env.api.close()
        cfg2 = with_broker_endpoint(config_with_free_ports(env.cfg, [TG], heartbeat_ms=100), env.sim.broker_endpoint)
        tg2 = TgThread(cfg2, tmp_path / "tg", env.clock, broker_heartbeat_timeout_ms=2_500).start()
        env.tg, env.cfg = tg2, cfg2
        env.rc, env.api = client(cfg2, "risk-engine-1"), client(cfg2, "api-gateway")

        after = wait_for(lambda: (lambda e: e if len(e) >= len(before) + 2 else None)(exec_facts(env.rc, "ACC5")),
                         what="resynced reports")
        assert [e.seq for e in after] == list(range(1, len(after) + 1))  # FACT seq continues across the restart
        assert [(e.seq, e.type) for e in after[: len(before)]] == [(e.seq, e.type) for e in before]
        new = [body(e) for e in after[len(before):]]
        assert [type(m).__name__ for m in new] == ["Trade", "BrokerOrderUpdate"]  # order history holes are expected
        assert isinstance(new[0], Trade) and new[0].origin is TradeOrigin.EXTERNAL and new[0].trade_seq == n_trades + 1
        batch = env.rc.request(TG, "td.query.trades", TradeQuery(trading_day=TD)).open_as(TradeBatch)
        assert [t.trade_seq for t in batch.trades] == list(range(1, n_trades + 2))
        assert len({(t.exchange, t.trade_id, t.direction) for t in batch.trades}) == n_trades + 1
        assert tg2.core.stats.dup_trades == n_trades and tg2.core.stats.dup_orders >= 1
        digest = env.rc.request(TG, "td.digest", DigestRequest(trading_day=TD)).open_as(TradeDigest)
        published = [body(e) for e in after if e.type == "Trade"]
        assert digest.accounts == [compute_account_digest("ACC5", published)]  # type: ignore[arg-type]
    finally:
        close_env(env)


def test_rule_kill_switch_arrives_by_fact_and_or_semantics_hold(env: Env) -> None:
    md = MetaData()
    add_common_tables(md, fact_cursors=False)
    Table("noop", md, id_col("k", primary_key=True))
    coord_rt = SyncServiceRuntime("risk-engine-0", config=env.cfg, home=env.home / "re0", market_clock=env.clock,
                                  setup_logs=False)
    coord = LoopThread(coord_rt).start()
    try:
        db = coord.call(lambda: coord_rt.open_database("risk-0", md, synchronous="FULL"))

        def publish(action: FirmAction) -> None:
            fc = FirmControl(action=action, origin=ControlOrigin.RULE, active=action is FirmAction.KILL_SWITCH_ON,
                             trading_day=TD, reason="firm daily loss", cause_alert_key="k", ts_ns=env.clock.now_ns())
            coord.call(lambda: db.write(lambda c: coord_rt.append_fact(c, "risk.control.firm", fc)))

        publish(FirmAction.KILL_SWITCH_ON)
        wait_for(lambda: env.tg.core.kill.rule, what="rule kill switch applied")
        assert env.route("c-rule-1", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1).code == codes.ROUTE_KILL_SWITCH
        assert kill_switch(env, False).ok  # a manual OFF does not clear the rule flag
        assert env.route("c-rule-2", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1).code == codes.ROUTE_KILL_SWITCH
        assert kill_switch(env, True).ok
        publish(FirmAction.KILL_SWITCH_OFF)
        wait_for(lambda: not env.tg.core.kill.rule, what="rule kill switch cleared")
        assert env.route("c-rule-3", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1).code == codes.ROUTE_KILL_SWITCH
        assert kill_switch(env, False).ok
        assert env.route("c-rule-4", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1).ok
        con = sqlite3.connect(env.home / "tg" / "db" / "td.db")
        try:
            cur = con.execute("SELECT src, topic, last_seq, start_policy FROM fact_cursors").fetchall()
        finally:
            con.close()
        assert cur == [("risk-engine-0", "risk.control.firm", 2, "FROM_BEGINNING")]
    finally:
        coord.stop()


def test_broker_disconnect_is_detected_and_the_session_resynced(env: Env) -> None:
    assert env.route("c-dc-1", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1).ok
    n = len(env.filled("ACC3", "c-dc-1"))
    session = env.tg.core.last_status.session_id
    env.sim.call(env.sim.ex.faults.add("BROKER_DISCONNECT", params={"seconds": 1.0}))
    wait_for(lambda: env.tg.core.last_status.state is not BrokerConnState.LOGGED_IN, 8, what="disconnect detected")
    wait_for(lambda: status(env).state is ServiceState.DEGRADED, what="DEGRADED while the broker is down")
    down = env.route("c-dc-2", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1)
    assert down.code == codes.REJECT_GATEWAY_UNAVAILABLE and ack(down).status is BrokerOrderStatus.REJECTED
    env.sim.force_close("ACC3", "rb2701", 1)  # reported to nobody: no session is logged in
    wait_for(lambda: env.tg.core.ready, 15, what="re-login and resync")
    wait_for(lambda: status(env).state is ServiceState.RUNNING, what="RUNNING again")
    assert env.tg.core.last_status.session_id != session
    wait_for(lambda: len(exec_facts(env.rc, "ACC3")) >= n + 2, what="missed reports resynced")
    types = [e.type for e in exec_facts(env.rc, "ACC3")[n:]]
    assert types[:2] == ["Trade", "BrokerOrderUpdate"]
    assert env.route("c-dc-3", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1).ok


def test_a_cancel_refused_by_a_restarted_broker_is_sent_again(tmp_path: Path, default_config: FullConfig) -> None:
    """HIGH-1: SIM_RESTART forgets the session; the cancel answered BROKER_NOT_LOGGED_IN stays pending.

    A long broker heartbeat timeout makes that refusal the only way the gateway learns about the restart.
    """
    env = start_env(tmp_path, default_config, broker_heartbeat_timeout_ms=60_000)
    try:
        _cancel_across_a_sim_restart(env)
    finally:
        close_env(env)


def _cancel_across_a_sim_restart(env: Env) -> None:
    _, lower = env.sim.limits("rb2701")
    assert env.route("c-sr-1", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1, price=lower).ok  # rests

    def updates() -> list[BrokerOrderUpdate]:
        msgs = [body(e) for e in exec_facts(env.rc, "ACC3")]
        return [m for m in msgs if isinstance(m, BrokerOrderUpdate) and m.client_order_id == "c-sr-1"]

    wait_for(lambda: any(u.status is BrokerOrderStatus.ACCEPTED for u in updates()), what="resting order accepted")
    session = env.tg.core.last_status.session_id
    env.sim.call(env.sim.ex.faults.add("SIM_RESTART"))  # broker sessions forgotten, orders reloaded from sim.db
    c = env.rc.request(TG, "route.cancel", RouteCancel(command_id=uuid7(), client_order_id="c-sr-1", reason="drill"),
                       timeout_ms=3000, retries=0)
    assert c.ok and ack(c).status is BrokerOrderStatus.CANCEL_PENDING

    def canceled() -> list[BrokerOrderUpdate] | None:
        u = updates()
        return u if u and u[-1].status is BrokerOrderStatus.CANCELED else None

    final = wait_for(canceled, 20, 0.05, "cancel delivered after the re-login")
    assert [u.status for u in final] == [BrokerOrderStatus.ACCEPTED, BrokerOrderStatus.CANCELED]  # no restatement
    assert env.tg.core.last_status.session_id != session
    assert env.tg.core.stats.cancels_sent == 2 and env.tg.core.stats.cancels_deferred == 1
    assert env.sim.broker_state()["active_orders"] == 0
    assert td_rows(env, "SELECT status, cancel_requested_ns IS NOT NULL FROM routed_orders WHERE client_order_id='c-sr-1'") == [
        ("CANCELED", 1)]


def test_rule_kill_switch_beyond_the_replay_window_comes_from_a_snapshot(env: Env) -> None:
    coord = FakeCoordinator(env.cfg, env.clock)
    coord.fact(1, rule_state(env, False), stored=False)  # both purged by the coordinator's retention
    coord.fact(2, rule_state(env, True, [BLOCK_OPEN]), stored=False)  # the newest RULE_STATE: the snapshot
    coord.fail_snapshots = 2  # the request and its transport retry fail: the service retries later
    coord.start()
    try:
        wait_for(lambda: env.tg.core.kill.rule, what="RULE kill switch taken from ctl.snapshot")
        assert coord.count("ctl.replay") >= 1 and coord.count("ctl.snapshot") >= 3
        blocked = env.route("c-exp-open", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1)
        assert blocked.code == codes.ROUTE_KILL_SWITCH
        rows = td_rows(env, "SELECT action, origin, active, src_instance, restrictions FROM firm_controls")
        assert [r[:4] for r in rows] == [("RULE_STATE", "RULE", 1, "risk-engine-0")]
        assert [r["kind"] for r in json.loads(rows[0][4])] == ["BLOCK_OPEN"]  # audit copy of the rule layer
    finally:
        coord.close()


def test_rule_kill_switch_after_a_coordinator_stream_change_comes_from_a_snapshot(env: Env) -> None:
    coord = FakeCoordinator(env.cfg, env.clock)
    coord.fact(1, rule_control(env, True))  # stored: delivered by ctl.replay
    coord.start()
    try:
        wait_for(lambda: env.tg.core.kill.rule, what="RULE kill switch replayed")
        assert coord.count("ctl.snapshot") == 0
        # the coordinator database was restored: its state exists only as its newest fact, not as a replayable one
        coord.new_stream(3, rule_state(env, False, [BLOCK_OPEN]))
        wait_for(lambda: not env.tg.core.kill.rule, what="RULE kill switch cleared by the stream-change snapshot")
        assert coord.count("ctl.snapshot") >= 1
        assert env.route("c-sc-open", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1).ok
        assert td_rows(env, "SELECT action, active FROM firm_controls ORDER BY control_id") == [
            ("KILL_SWITCH_ON", 1), ("RULE_STATE", 0)]  # a legacy record, then the new protocol
    finally:
        coord.close()


def test_eod_done_runs_the_td_db_maintenance(env: Env) -> None:
    assert env.route("c-eod-1", "ACC3", "rb2701", Exchange.SHFE, Direction.BUY, Offset.OPEN, 1).ok
    env.filled("ACC3", "c-eod-1")
    clearing = FakePublisher(env.cfg, "clearing-worker")
    try:
        done = EodPhaseEvent(trading_day=TD, version=1, phase=EodPhase.DONE, next_trading_day="20260929",
                             ts_ns=env.clock.now_ns())

        def maintained() -> Any:
            clearing.publish(topics.SYS_EOD, done)  # a plain subscription: repeat until the SUB has joined
            return env.tg.svc.last_maintenance

        res = wait_for(maintained, 15, 0.1, "maintenance after sys.eod DONE")
        backup = env.home / "tg" / "backup" / TD / "td.db"
        assert res.trading_day == TD and res.errors == [] and res.backup == str(backup)
        con = sqlite3.connect(backup)
        try:
            assert con.execute("SELECT status FROM routed_orders WHERE client_order_id='c-eod-1'").fetchall() == [("FILLED",)]
        finally:
            con.close()
        assert status(env).state is ServiceState.RUNNING
    finally:
        clearing.close()
