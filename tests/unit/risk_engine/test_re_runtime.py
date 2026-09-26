"""risk-engine shell on real sockets against fakes of trade-gateway, md-gateway and clearing-worker.

Market time is Monday 2026-09-28 09:35 (demo clock): rb2701 and IF2612 are in their continuous session.
Shard 1 owns ACC3 (300 rb2701 long, 3000 CNY per point), ACC4 (5 IF2612 long) and ACC5 (flat).

Timing: shells are used only once reachable from every client (``Env.shell`` -> ``wait_ready``), commands are
retried like the production clients (``request``), and every step that depends on FACT delivery, timers or a peer
waits for an observable condition (``wait_for``) instead of sleeping.
"""

from __future__ import annotations

import sqlite3
import time
from typing import Any, TypeVar

import pytest

from futrisk.bus import BlockingRpcClient
from futrisk.common.ids import uuid7
from futrisk.messages import codes
from futrisk.messages.base import Msg
from futrisk.messages.enums import (
    AccountState,
    BrokerOrderStatus,
    CashType,
    Direction,
    MdQuality,
    Offset,
    OrderStatus,
    OrderType,
    RiskEventKind,
    Role,
    ServiceState,
    TradingMode,
)
from futrisk.messages.envelope import Reply
from futrisk.messages.orders import CancelRequest, OrderAck, OrderRequest, OrderUpdate, RouteOrder, TradeQuery
from futrisk.messages.risk import (
    AccountSnapshotBatch,
    AccountStateChanged,
    CashBatch,
    CashMovement,
    CashMoveRequest,
    FillBatch,
    OrderBatch,
    PositionBatch,
    QueryAccounts,
    QueryFills,
    QueryOrders,
    QueryPositions,
    RiskEvent,
)
from futrisk.services.risk_engine.service import Phase

from .re_support import NTD, TD, TG, Env, body, lv_seq, replay, request, status, wait_for, wait_ready

pytestmark = pytest.mark.timeout(120)
RE1 = "risk-engine-1"
B, S = Direction.BUY, Direction.SELL
OPEN, CY, CT = Offset.OPEN, Offset.CLOSE_YESTERDAY, Offset.CLOSE_TODAY
M = TypeVar("M", bound=Msg)


def submit(c: BlockingRpcClient, account: str, inst: str, direction: Direction, offset: Offset, qty: int, price: float,
           *, coid: str | None = None, target: str = RE1) -> Reply:
    req = OrderRequest(client_order_id=coid or uuid7(), account_id=account, instrument_id=inst, direction=direction,
                       offset=offset, order_type=OrderType.LIMIT, price=price, qty=qty, sent_ns=time.time_ns())
    return request(c, target, "order.submit", req)


def query(c: BlockingRpcClient, command: str, payload: Msg, typ: type[M], *, target: str = RE1) -> M:
    r = request(c, target, command, payload)
    assert r.ok, r
    got = r.open_as(typ)
    assert isinstance(got, typ), r
    return got


def ack(r: Reply) -> OrderAck:
    a = r.open_as(OrderAck)
    assert isinstance(a, OrderAck), r
    return a


def order_status(env: Env, sh: Any, coid: str) -> OrderStatus | None:
    rec = sh.call(lambda: sh.core.orders.get(coid))
    return rec.status if rec is not None else None


def acc(sh: Any, account: str = "ACC3") -> tuple[AccountState, bool, int]:
    got: tuple[AccountState, bool, int] = sh.call(
        lambda: (lambda ra: (ra.state, ra.halted_latched, ra.state_version))(sh.core.account(account)))
    return got


def raised(c: BlockingRpcClient, account: str = "ACC3", src: str = RE1) -> list[RiskEvent]:
    evs = [body(e) for e in replay(c, src, "risk.event")]
    return [e for e in evs if isinstance(e, RiskEvent) and e.kind == RiskEventKind.ALERT_RAISED and e.account_id == account]


def received(env: Env, command: str, account: str | None = None) -> int:
    return sum(1 for c, p in list(env.tg.received) if c == command and (account is None or getattr(p, "account_id", None) == account))


# ------------------------------------------------------------------------------------------------ start-up
def test_startup_loads_the_sod_catches_up_and_runs(env: Env) -> None:
    from futrisk.messages.risk import MetricSeries, QueryMetrics

    sh = env.shell()
    assert sh.svc.phase == Phase.RUNNING
    assert sh.call(lambda: sh.core.lifecycle.value) == "RUNNING"
    assert [r.version for r in env.cw.requests] == [None] and env.cw.requests[0].trading_day == TD
    assert {p.account_id for c, p in env.tg.received if c == "td.query.trades" and isinstance(p, TradeQuery)} == {
        "ACC3", "ACC4", "ACC5"}
    assert received(env, "td.query.positions") >= 1
    snaps = query(env.api, "risk.query.accounts", QueryAccounts(), AccountSnapshotBatch)
    assert [s.account_id for s in snaps.items] == ["ACC3", "ACC4", "ACC5"]
    assert all(s.state == AccountState.NORMAL and s.trading_day == TD for s in snaps.items)
    hb = wait_for(lambda: (lambda h: h if h.state == ServiceState.RUNNING and h.trading_day == TD
                           and h.deps.get("trade-gateway") == "UP" else None)(status(env.api, RE1)), what="heartbeat")
    assert hb.trading_mode == TradingMode.OPEN and hb.detail["phase"] == "RUNNING"
    series = wait_for(lambda: (lambda m: m if m.points else None)(query(
        env.api, "risk.query.metrics", QueryMetrics(account_id="ACC3", trading_day=TD, step_s=1), MetricSeries)),
        step=0.1, what="metric samples")
    assert series.points[-1].equity == pytest.approx(10_000_000.0) and series.step_s == 1
    # the second start of the same database restores instead of asking for a SOD again
    sh2 = env.restart(sh)
    assert sh2.svc.phase == Phase.RUNNING and len(env.cw.requests) == 1


def test_orders_are_rejected_not_ready_until_caught_up(env: Env) -> None:
    env.tg.silent = True  # td.query.trades unanswered: the shard stays in CATCHUP
    sh = env.shell(phase=Phase.CATCHUP)
    r = submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3100.0)  # the core answers while LOADING, without a row
    assert r.code == codes.REJECT_SYSTEM_NOT_READY and ack(r).reject_detail == {"lifecycle": "LOADING"}
    assert sh.svc.db.read(lambda c: c.exec_driver_sql("SELECT COUNT(*) FROM orders").fetchone()) == (0,)
    snaps = query(env.api, "risk.query.accounts", QueryAccounts(), AccountSnapshotBatch)
    assert len(snaps.items) == 3  # queries are answered while starting
    env.tg.silent = False
    sh.wait_phase(Phase.RUNNING)
    wait_ready(env.clients, RE1)
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3100.0).ok


# ------------------------------------------------------------------------------------------------ orders
def test_order_submit_route_fill_position_and_queries(env: Env) -> None:
    sh = env.shell()
    coid = uuid7()
    r = submit(env.bot, "ACC3", "rb2701", B, OPEN, 2, 3100.0, coid=coid)
    assert r.ok and ack(r).accepted and ack(r).status == OrderStatus.ROUTING
    wait_for(lambda: coid in env.tg.routed(), what="routed")
    wait_for(lambda: order_status(env, sh, coid) == OrderStatus.ACCEPTED, what="accepted")
    fill = env.tg.fill(coid)
    wait_for(lambda: order_status(env, sh, coid) == OrderStatus.FILLED, what="filled")
    pos = query(env.api, "risk.query.positions", QueryPositions(account_id="ACC3"), PositionBatch)
    rb = [p for p in pos.items if p.instrument_id == "rb2701"]
    assert [(p.td_qty, p.yd_qty) for p in rb] == [(2, 300)]
    orders = query(env.api, "risk.query.orders", QueryOrders(account_id="ACC3", trading_day=TD), OrderBatch)
    assert [(o.client_order_id, o.status, o.filled_qty) for o in orders.items] == [(coid, OrderStatus.FILLED, 2)]
    fills = query(env.api, "risk.query.fills", QueryFills(account_id="ACC3", trading_day=TD), FillBatch)
    assert [t.trade_id for t in fills.items] == [fill.trade_id]
    updates = [body(e) for e in replay(env.api, RE1, "oms.order.ACC3")]
    statuses = [u.status for u in updates if isinstance(u, OrderUpdate)]
    # ROUTED is skipped when the broker's ACCEPTED (td.exec) overtakes the RouteAck
    assert statuses[0] == OrderStatus.ROUTING and statuses[-1] == OrderStatus.FILLED and OrderStatus.ACCEPTED in statuses
    # idempotency of order.submit (02 §4.7)
    dup = submit(env.bot, "ACC3", "rb2701", B, OPEN, 2, 3100.0, coid=coid)
    assert dup.ok and dup.duplicate
    other = submit(env.bot, "ACC3", "rb2701", B, OPEN, 3, 3100.0, coid=coid)
    assert other.code == codes.REJECT_DUPLICATE_ORDER_ID
    # quantities 03 §4.3 cannot store are refused without a row and without hurting the shard
    zero = submit(env.bot, "ACC3", "rb2701", B, OPEN, 0, 3100.0)
    assert zero.code == codes.REJECT_INVALID_QTY and not ack(zero).accepted
    huge = submit(env.bot, "ACC3", "rb2701", B, OPEN, 2**40, 3100.0)
    assert huge.code == codes.REJECT_QTY_EXCEEDS_MAX
    assert sh.alive()
    # cancel: CANCEL_PENDING, then CANCELED from td.exec
    rest = uuid7()
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3060.0, coid=rest).ok
    wait_for(lambda: order_status(env, sh, rest) == OrderStatus.ACCEPTED, what="resting")
    cr = request(env.bot, RE1, "order.cancel", CancelRequest(command_id=uuid7(), client_order_id=rest, account_id="ACC3"))
    assert cr.ok and cr.open_as(OrderUpdate).status == OrderStatus.CANCEL_PENDING
    wait_for(lambda: order_status(env, sh, rest) == OrderStatus.CANCELED, what="cancelled")


# ------------------------------------------------------------------------------------------------ acceptance
def test_acceptance_acc3_breaker_latches_and_survives_a_crash(env: Env) -> None:
    """QS-03 / QS-04: -2% ALERT, -2.5% RESTRICT, -3% HALTED (pending, then latched); kill; still HALTED, no duplicates."""
    sh = env.shell()
    rest_open, rest_close = uuid7(), uuid7()
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3050.0, coid=rest_open).ok
    assert submit(env.bot, "ACC3", "rb2701", S, CY, 5, 3150.0, coid=rest_close).ok
    wait_for(lambda: order_status(env, sh, rest_open) == order_status(env, sh, rest_close) == OrderStatus.ACCEPTED,
             what="resting orders accepted")

    env.md.tick("rb2701", 3030.0)  # -2.1%
    wait_for(lambda: acc(sh)[0] == AccountState.WARNING, what="WARNING")
    env.md.tick("rb2701", 3015.0)  # -2.55%
    wait_for(lambda: acc(sh)[0] == AccountState.RESTRICTED, what="RESTRICTED")
    env.md.tick("rb2701", 2995.0)  # -3.15%: first breaching state -> HALTED, pending confirmation
    wait_for(lambda: acc(sh)[0] == AccountState.HALTED, what="HALTED")
    wait_for(lambda: order_status(env, sh, rest_open) == OrderStatus.CANCELED, what="resting open cancelled")
    assert order_status(env, sh, rest_close) == OrderStatus.ACCEPTED  # closes are not cancelled
    # (a cancel re-sent after a transport retry carries the same order and reason)
    assert {(p.client_order_id, p.reason) for c, p in list(env.tg.received) if c == "route.cancel"} == {
        (rest_open, "CIRCUIT_BREAK_CANCEL")}
    r = submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 2995.0)
    assert r.code == codes.REJECT_ACCOUNT_HALTED
    close = submit(env.bot, "ACC3", "rb2701", S, CY, 5, 2995.0)
    assert close.ok, close
    env.md.tick("rb2701", 2994.0)  # a second distinct LIVE state confirms (so does the 1 s confirmation timer)
    wait_for(lambda: acc(sh)[1], what="latched")
    events = raised(env.api)
    assert sorted((e.rule_id, e.level, e.episode) for e in events) == [
        ("daily_loss", "halt", 1), ("daily_loss", "restrict", 1), ("daily_loss", "warn", 1)]
    halt = [e for e in events if e.level == "halt"][0]
    assert halt.latched and halt.alert_key == f"{TD}|daily_loss|halt|acct:ACC3|1"
    states = [s for s in (body(e) for e in replay(env.api, RE1, "risk.state.ACC3")) if isinstance(s, AccountStateChanged)]
    halted = [s for s in states if s.to_state == AccountState.HALTED]
    assert not halted[0].halted_latched and halted[-1].halted_latched  # pending first, latched once confirmed
    assert states[-1] == halted[-1]
    version_before = acc(sh)[2]
    n_events_before = len(replay(env.api, RE1, "risk.event"))

    # ---- crash (fatal path: databases closed unclean) and restart on the same home
    sh2 = env.restart(sh, crash=True)
    assert sh.exit_code == 1
    assert sh2.svc.db is not None and sh2.svc.db.info.previous_clean_shutdown is False
    state, latched, version = acc(sh2)
    assert (state, latched) == (AccountState.HALTED, True) and version >= version_before
    # timers and snapshot rounds on the replayed market state (every account is re-published at least once a second)
    seq = lv_seq(env.api, RE1, "risk.snapshot.ACC3")
    wait_for(lambda: lv_seq(env.api, RE1, "risk.snapshot.ACC3") >= seq + 2, step=0.05, what="two snapshot rounds")
    assert sorted((e.rule_id, e.level, e.episode) for e in raised(env.api)) == [
        ("daily_loss", "halt", 1), ("daily_loss", "restrict", 1), ("daily_loss", "warn", 1)]
    after = [body(e) for e in replay(env.api, RE1, "risk.event")][n_events_before:]
    assert [e.kind for e in after if isinstance(e, RiskEvent)] == []  # no new risk event at all
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 2994.0).code == codes.REJECT_ACCOUNT_HALTED
    assert submit(env.bot, "ACC3", "rb2701", S, CY, 1, 2994.0).ok
    snap = query(env.api, "risk.query.accounts", QueryAccounts(account_ids=["ACC3"]), AccountSnapshotBatch)
    assert snap.items[0].state == AccountState.HALTED and snap.items[0].halted_latched


# ------------------------------------------------------------------------------------------------ market data
def test_md_gateway_down_blocks_opens_until_heartbeat_and_snapshot_return(env: Env) -> None:
    sh = env.shell()
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3100.0).ok
    env.md.hb.pause()
    wait_for(lambda: sh.call(lambda: sh.core.quality_of("rb2701")) == MdQuality.STALE, what="md down -> STALE")
    r = submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3100.0)
    assert r.code == codes.REJECT_MD_UNAVAILABLE
    assert submit(env.bot, "ACC3", "rb2701", S, CY, 1, 3110.0).ok  # closes pass (above the resting buy: no self trade)
    wait_for(lambda: (lambda h: h.deps.get("md-gateway") == "DOWN" and h.state == ServiceState.DEGRADED)(
        status(env.api, RE1)), what="DEGRADED heartbeat")
    syncs = sh.call(lambda: sh.svc._md_sub.clone.syncs)
    env.md.hb.resume()
    wait_for(lambda: sh.call(lambda: sh.core.quality_of("rb2701")) == MdQuality.OK, what="md back")
    assert sh.call(lambda: sh.svc._md_sub.clone.syncs) > syncs  # recovery waited for a fresh snapshot (01 8.5)
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3100.0).ok
    wait_for(lambda: status(env.api, RE1).state == ServiceState.RUNNING, what="RUNNING again")


def test_stale_instrument_blocks_opens_only_on_that_instrument(env: Env) -> None:
    sh = env.shell()
    env.md.status({"IF2612": MdQuality.STALE, "rb2701": MdQuality.OK})
    wait_for(lambda: sh.call(lambda: sh.core.quality_of("IF2612")) == MdQuality.STALE, what="IF stale")
    assert submit(env.bot, "ACC4", "IF2612", B, OPEN, 1, 4500.0).code == codes.REJECT_MD_UNAVAILABLE
    assert submit(env.bot, "ACC4", "IF2612", S, Offset.CLOSE, 1, 4500.0).ok
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3100.0)
    env.md.tick("IF2612", 4502.0)  # a LIVE tick alone does not override md.status (the authority on quality)
    wait_for(lambda: sh.call(lambda: sh.core.mark_of("IF2612")) == 4502.0, what="IF tick applied")
    assert sh.call(lambda: sh.core.quality_of("IF2612")) == MdQuality.STALE
    env.md.status({"IF2612": MdQuality.OK, "rb2701": MdQuality.OK})  # md-gateway: IF fresh again
    wait_for(lambda: sh.call(lambda: sh.core.quality_of("IF2612")) == MdQuality.OK, what="IF fresh")
    assert submit(env.bot, "ACC4", "IF2612", B, OPEN, 1, 4499.0).ok  # below the resting sell: no self trade


# ------------------------------------------------------------------------------------------------ trade-gateway
def test_trade_gateway_down_rejects_orders_and_recovers(env: Env) -> None:
    sh = env.shell()
    env.tg.silent = True
    env.tg.hb.pause()
    wait_for(lambda: not sh.call(lambda: sh.core.gateway_up), what="gateway down")
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3100.0).code == codes.REJECT_GATEWAY_UNAVAILABLE
    assert submit(env.bot, "ACC3", "rb2701", S, CY, 1, 3100.0).code == codes.REJECT_GATEWAY_UNAVAILABLE
    env.tg.silent = False
    env.tg.hb.resume()
    wait_for(lambda: sh.call(lambda: sh.core.gateway_up), what="gateway up")
    coid = uuid7()
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3100.0, coid=coid).ok
    wait_for(lambda: order_status(env, sh, coid) == OrderStatus.ACCEPTED, what="routed again")


def test_unanswered_route_becomes_unknown_and_is_resolved_as_lost(env: Env) -> None:
    sh = env.shell()
    env.tg.silent = True  # heartbeats continue: the gateway looks alive but never answers
    coid = uuid7()
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3100.0, coid=coid).ok
    wait_for(lambda: order_status(env, sh, coid) == OrderStatus.UNKNOWN, what="UNKNOWN after expire_ns")
    sent = [p for c, p in list(env.tg.received) if c == "route.order" and isinstance(p, RouteOrder)]
    assert sent and {p.client_order_id for p in sent} == {coid}  # retried with the same id, never a new one
    env.tg.silent = False  # the next route.resolve is answered: never received -> tombstone -> ROUTE_LOST
    wait_for(lambda: order_status(env, sh, coid) == OrderStatus.BROKER_REJECTED, what="resolved as lost")
    rec = sh.call(lambda: sh.core.orders.get(coid))
    assert rec.reject_code == codes.ROUTE_LOST and coid in env.tg.tombstones


# ------------------------------------------------------------------------------------------------ FACT repair
def _accepted_order(env: Env, sh: Any, account: str = "ACC3", qty: int = 2) -> str:
    coid: str = uuid7()
    assert submit(env.bot, account, "rb2701", B, OPEN, qty, 3100.0, coid=coid).ok
    wait_for(lambda: order_status(env, sh, coid) == OrderStatus.ACCEPTED, what="accepted")
    return coid


def _td_qty(sh: Any, account: str = "ACC3", inst: str = "rb2701") -> int:
    def q() -> int:
        return sum(p.td_qty for p in sh.core.account(account).ledger.book.positions(account) if p.instrument_id == inst)

    return int(sh.call(q))


def _yd_qty(sh: Any, account: str = "ACC3") -> int:
    return int(sh.call(lambda: sum(p.yd_qty for p in sh.core.account(account).ledger.book.positions(account))))


def _fill_gap(sh: Any, account: str = "ACC3") -> bool:
    return bool(sh.call(lambda: any(r.source == "SYSTEM:FILL_GAP" for r in sh.core.account(account).restrictions)))


def test_fact_gap_is_replayed_from_the_trade_gateway_outbox(env: Env) -> None:
    sh = env.shell()
    coid = _accepted_order(env, sh)
    ob = env.tg.rt.outbox
    ob.detach_sink()  # the next rows are committed but not sent: the shard misses them
    env.tg.fill(coid)
    env.tg.update(env.tg.orders[coid], BrokerOrderStatus.FILLED)  # one more row on the same topic
    rows = wait_for(lambda: ob.fetch_unpublished() or None, what="unpublished rows")
    assert [r.topic_seq for r in rows][-3:] == [2, 3, 4]
    env.tg.rt.publisher.publish_fact_rows([rows[-1]])  # only the last one reaches the socket: a gap 2..3
    wait_for(lambda: _td_qty(sh) == 2, what="gap repaired by ctl.replay")
    assert sh.call(lambda: sh.rt.fact_tracker.stats.replays_sent) >= 1
    assert order_status(env, sh, coid) == OrderStatus.FILLED
    dups = sh.call(lambda: sh.rt.fact_tracker.stats.duplicates)
    ob.attach_sink(env.tg.rt.publisher.publish_fact_rows).result(5)  # rows 2..3 go out again: duplicates
    wait_for(lambda: sh.call(lambda: sh.rt.fact_tracker.stats.duplicates) >= dups + 2, what="republished rows dropped")
    assert _td_qty(sh) == 2 and not _fill_gap(sh)


def test_stream_change_is_reconciled_by_domain_queries(env: Env) -> None:
    """trade-gateway's td.db is recreated: a new stream (seq from 1) that its heartbeat announces (the authority)."""
    from futrisk.messages.enums import TradeOrigin

    sh = env.shell()
    coid = _accepted_order(env, sh)
    env.tg.fill(coid)
    wait_for(lambda: _td_qty(sh) == 2 and order_status(env, sh, coid) == OrderStatus.FILLED, what="fill")
    tracker = sh.rt.fact_tracker
    old = sh.call(lambda: tracker.stream_of(TG, "td.exec.ACC3"))
    changes = sh.call(lambda: tracker.stats.stream_changes)
    queried = received(env, "td.query.orders", "ACC3")
    # the broker still knows a trade the shard never received; then the gateway comes back on a new database
    t = env.tg.trade("ACC3", "rb2701", S, CY, 3099.0, 1, origin=TradeOrigin.EXTERNAL, publish=False)
    new = env.tg.new_stream()
    assert old is not None and new != old
    wait_for(lambda: sh.call(lambda: tracker.stream_of(TG, "td.exec.ACC3")) == new, what="switched by the heartbeat")
    assert sh.call(lambda: tracker.stats.stream_changes) > changes
    # 02 §8.2 domain fallback: all trades of the day, then the broker state of the account's orders
    wait_for(lambda: _yd_qty(sh) == 299, what="reconciled by td.query.trades")
    wait_for(lambda: received(env, "td.query.orders", "ACC3") > queried, what="then td.query.orders")
    wait_for(lambda: not _fill_gap(sh), what="FILL_GAP lifted")
    fills = query(env.api, "risk.query.fills", QueryFills(account_id="ACC3", trading_day=TD), FillBatch)
    assert t.trade_id in {f.trade_id for f in fills.items}
    # the new stream is followed from its first seq: later facts are applied normally
    env.tg.update(env.tg.orders[coid], BrokerOrderStatus.FILLED)
    assert env.tg.rt.outbox.last_seq("td.exec.ACC3") == 1
    wait_for(lambda: sh.call(lambda: tracker.expected(TG, "td.exec.ACC3")) == 2, what="new-stream fact applied")
    assert _yd_qty(sh) == 299 and _td_qty(sh) == 2


def test_expired_replay_after_restart_falls_back_to_td_query_trades(env: Env) -> None:
    from futrisk.messages.enums import ReconBreakKind, TradeOrigin

    sh = env.shell()
    coid = _accepted_order(env, sh)
    env.tg.fill(coid)
    wait_for(lambda: _td_qty(sh) == 2, what="fill")
    sh.stop()
    t = env.tg.trade("ACC3", "rb2701", B, OPEN, 3100.0, 3, origin=TradeOrigin.EXTERNAL)  # while the shard is down
    ob = env.tg.rt.outbox
    wait_for(lambda: ob.count_unpublished() == 0, what="published")
    assert env.tg.db is not None
    env.tg.db.write(lambda c: ob.purge_published_before(c, 2**62))  # the replay window is gone
    sh2 = env.shell(home=sh.home)
    wait_for(lambda: _td_qty(sh2) == 5, what="missed trade applied")

    def expired() -> list[Any]:
        return [b for b in sh2.call(lambda: list(sh2.core.breaks.values())) if b.kind == ReconBreakKind.REPLAY_EXPIRED]

    brk = wait_for(expired, what="REPLAY_EXPIRED break")[0]
    assert not brk.blocking and brk.account_id == "ACC3"
    fills = query(env.api, "risk.query.fills", QueryFills(account_id="ACC3", trading_day=TD), FillBatch)
    assert sorted(f.trade_id for f in fills.items) == sorted({x.trade_id for x in env.tg.trades_of("ACC3")})
    assert t.trade_id in {f.trade_id for f in fills.items}


# ------------------------------------------------------------------------------------------------ cash
def test_cash_move_is_idempotent_by_movement_id(env: Env) -> None:
    env.shell()
    mid = uuid7()

    def req(amount: str, ns: int, role: Role = Role.ADMIN, movement_id: str | None = None) -> CashMoveRequest:
        return CashMoveRequest(movement_id=movement_id or mid, account_id="ACC3", type=CashType.DEPOSIT, amount=amount,
                               reason="top up", actor="admin", role=role, requested_ns=ns)

    first = request(env.api, RE1, "cash.move", req("100000.00", 1))
    assert first.ok and not first.duplicate
    cm = first.open_as(CashMovement)
    assert (cm.trading_day, cm.amount) == (TD, "100000.00")
    again = request(env.api, RE1, "cash.move", req("100000.00", 2))  # retry with a fresh requested_ns
    assert again.ok and again.duplicate and again.open_as(CashMovement) == cm
    assert request(env.api, RE1, "cash.move", req("5.00", 3)).code == codes.CONFLICT
    assert request(env.api, RE1, "cash.move", req("1.00", 4, Role.TRADER, uuid7())).code == codes.FORBIDDEN
    assert request(env.bot, RE1, "cash.move", req("1.00", 5, movement_id=uuid7())).code == codes.FORBIDDEN
    facts = [body(e) for e in replay(env.api, RE1, "risk.cash.ACC3")]
    assert [f.movement_id for f in facts if isinstance(f, CashMovement)] == [mid]


# ------------------------------------------------------------------------------------------------ EOD
def _cash(env: Env, account: str, kind: CashType, amount: str) -> CashMovement:
    r = request(env.api, RE1, "cash.move", CashMoveRequest(movement_id=uuid7(), account_id=account, type=kind,
                                                           amount=amount, reason="eod test", actor="admin",
                                                           role=Role.ADMIN))
    assert r.ok, r
    cm = r.open_as(CashMovement)
    assert isinstance(cm, CashMovement)
    return cm


def _signed_cash_digest(items: list[CashMovement], account: str) -> tuple[int, str]:
    from decimal import Decimal

    mine = [c for c in items if c.account_id == account]
    total = sum((Decimal(c.amount) if c.type == CashType.DEPOSIT else -Decimal(c.amount) for c in mine), Decimal(0))
    return len(mine), f"{total.quantize(Decimal('0.01')):f}"


def _freeze_reply(env: Env, td: str = TD, version: int = 1) -> Reply:
    from futrisk.messages.eod import EodFreezeRequest

    return request(env.clr, RE1, "eod.freeze", EodFreezeRequest(trading_day=td, version=version), timeout_ms=30_000)


def _freeze(env: Env, td: str = TD, version: int = 1) -> Any:
    from futrisk.messages.eod import EodFreezeAck

    r = _freeze_reply(env, td, version)
    assert r.ok, r
    a = r.open_as(EodFreezeAck)
    assert isinstance(a, EodFreezeAck)
    return a


def _rollover(env: Env, td: str, ntd: str, version: int, target: str = RE1) -> Reply:
    from futrisk.messages.eod import EodRolloverRequest

    return request(env.clr, target, "eod.rollover", EodRolloverRequest(trading_day=td, next_trading_day=ntd,
                                                                       sod_version=version), timeout_ms=30_000)


def test_eod_freeze_digests_and_idempotent_rollover_across_a_restart(env: Env) -> None:
    from futrisk.messages.enums import DayRollMode
    from futrisk.messages.eod import DayRoll
    from futrisk.messages.orders import compute_account_digest
    from futrisk.messages.risk import QueryAlerts, QueryCash, RiskEventBatch

    from .re_support import ALL_ACCOUNTS, lot, sod, yd_positions

    sh = env.shell()
    a3 = _accepted_order(env, sh, "ACC3", 2)
    env.tg.fill(a3)
    close = uuid7()
    assert submit(env.bot, "ACC3", "rb2701", S, CT, 2, 3105.0, coid=close).ok
    wait_for(lambda: order_status(env, sh, close) == OrderStatus.ACCEPTED, what="close accepted")
    env.tg.fill(close, price=3105.0)
    a4 = uuid7()
    assert submit(env.bot, "ACC4", "IF2612", B, OPEN, 1, 4500.0, coid=a4).ok
    wait_for(lambda: order_status(env, sh, a4) == OrderStatus.ACCEPTED, what="IF accepted")
    env.tg.fill(a4)
    wait_for(lambda: order_status(env, sh, close) == order_status(env, sh, a4) == OrderStatus.FILLED, what="fills")
    moves = [_cash(env, "ACC5", CashType.DEPOSIT, "1000.00"), _cash(env, "ACC3", CashType.WITHDRAW, "250.50")]

    ack1 = _freeze(env)
    expected = [compute_account_digest(a, env.tg.trades_of(a)) for a in ("ACC3", "ACC4", "ACC5")]
    assert ack1.accounts == expected and ack1.shard == 1
    assert [(c.account_id, c.count, c.amount_sum) for c in ack1.cash] == [
        (a, *_signed_cash_digest(moves, a)) for a in ("ACC3", "ACC4", "ACC5")]
    assert [(c.count, c.amount_sum) for c in ack1.cash] == [(1, "-250.50"), (0, "0.00"), (1, "1000.00")]
    assert _freeze(env).accounts == expected  # repeatable, recomputed
    frozen_out = uuid7()
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3100.0, coid=frozen_out).code == codes.REJECT_EOD_IN_PROGRESS
    late = _cash(env, "ACC5", CashType.DEPOSIT, "7.00")
    assert late.trading_day == NTD  # frozen, not rolled: next trading day (02 §6.3)

    # the settled SOD of NTD (net positions after the day) and the trade-gateway's view of yesterday
    after = dict(ALL_ACCOUNTS)
    after["ACC4"] = ("10000000", [lot("IF2612", 5, 4500.0, n=1), lot("IF2612", 1, 4500.0, n=2)])
    env.cw.add(sod(after, trading_day=NTD, version=1))
    r = _rollover(env, TD, NTD, 1)
    assert r.ok, r
    dr = r.open_as(DayRoll)
    assert (dr.mode, dr.from_trading_day, dr.to_trading_day, dr.sod_version) == (DayRollMode.ROLL, TD, NTD, 1)
    again = _rollover(env, TD, NTD, 1)  # re-push while alive
    assert again.ok and again.duplicate and again.open_as(DayRoll) == dr
    assert sh.call(lambda: (sh.core.trading_day, sh.core.sod_version)) == (NTD, 1)
    assert [c.trading_day for c in sh.call(lambda: list(sh.core.account("ACC5").cash))] == [NTD]  # moved in at the roll

    # history of the rolled day comes from risk.db
    orders = query(env.api, "risk.query.orders", QueryOrders(account_id="ACC3", trading_day=TD), OrderBatch)
    assert {o.client_order_id: o.status for o in orders.items} == {
        a3: OrderStatus.FILLED, close: OrderStatus.FILLED, frozen_out: OrderStatus.REJECTED}  # pre-trade rejects are rows too
    fills = query(env.api, "risk.query.fills", QueryFills(account_id="ACC4", trading_day=TD), FillBatch)
    assert [f.trade_id for f in fills.items] == [t.trade_id for t in env.tg.trades_of("ACC4")]
    cash = query(env.clr, "risk.query.cash", QueryCash(trading_day=TD), CashBatch)
    assert sorted(c.movement_id for c in cash.items) == sorted(m.movement_id for m in moves)
    query(env.api, "risk.query.alerts", QueryAlerts(trading_day=TD), RiskEventBatch)
    assert _freeze(env).accounts == expected  # a re-settlement freeze of the rolled day: digests from risk.db

    # restart: the re-pushed rollover is still a duplicate with the real DayRoll (from the outbox)
    env.tg.trading_day = NTD
    env.tg.yd = yd_positions(after)
    sh2 = env.restart(sh, crash=True)
    third = _rollover(env, TD, NTD, 1)
    assert third.ok and third.duplicate and third.open_as(DayRoll) == dr
    assert sh2.call(lambda: sh2.core.trading_day) == NTD
    # a newer settlement version of the same day rebases
    env.cw.add(sod(after, trading_day=NTD, version=2))
    reb = _rollover(env, TD, NTD, 2)
    assert reb.ok and reb.open_as(DayRoll).mode == DayRollMode.REBASE
    assert sh2.call(lambda: sh2.core.sod_version) == 2
    rolls = [body(e) for e in replay(env.api, RE1, "sys.dayroll")]
    assert [(d.mode, d.sod_version) for d in rolls if isinstance(d, DayRoll)] == [
        (DayRollMode.ROLL, 1), (DayRollMode.REBASE, 2)]


def test_next_day_fills_buffered_before_a_crash_are_recovered_at_the_roll(env: Env) -> None:
    from futrisk.messages.enums import TradeOrigin
    from futrisk.messages.eod import DayRoll

    from .re_support import ALL_ACCOUNTS, sod

    sh = env.shell()
    # a night-session trade of NTD reaches the shard before its roll: buffered in memory only (ENGINE.md §12)
    t = env.tg.trade("ACC5", "rb2701", B, OPEN, 3100.0, 2, origin=TradeOrigin.EXTERNAL, trading_day=NTD)
    wait_for(lambda: sh.call(lambda: sh.rt.fact_tracker.expected(TG, "td.exec.ACC5")) == 2, what="applied")
    sh2 = env.restart(sh, crash=True)
    assert _td_qty(sh2, "ACC5") == 0
    _freeze(env)
    env.cw.add(sod(ALL_ACCOUNTS, trading_day=NTD, version=1))
    r = _rollover(env, TD, NTD, 1)
    assert r.ok and r.open_as(DayRoll).to_trading_day == NTD
    wait_for(lambda: _td_qty(sh2, "ACC5") == 2, what="next-day fill recovered by td.query.trades")
    fills = query(env.api, "risk.query.fills", QueryFills(account_id="ACC5", trading_day=NTD), FillBatch)
    assert [f.trade_id for f in fills.items] == [t.trade_id]


def test_eod_freeze_is_held_while_catching_up_then_answered(env: Env) -> None:
    import threading

    from futrisk.messages.eod import EodFreezeAck

    env.tg.silent = True
    sh = env.shell(phase=Phase.CATCHUP)
    result: dict[str, Reply] = {}
    th = threading.Thread(target=lambda: result.update(reply=_freeze_reply(env)), daemon=True)
    th.start()
    wait_for(lambda: sh.call(lambda: [d.command for d in sh.svc._deferred]) == ["eod.freeze"], what="freeze held")
    assert "reply" not in result and sh.svc.phase == Phase.CATCHUP  # held, not refused (clearing fails fast on retryable)
    env.tg.silent = False
    th.join(40)
    r = result["reply"]
    assert r.ok, r
    assert r.open_as(EodFreezeAck).trading_day == TD and sh.svc.phase in (Phase.WARMUP, Phase.RUNNING)


# ------------------------------------------------------------------------------------------------ coordinator
def test_coordinator_aggregates_shards_and_firm_controls_reach_every_shard(env_factory: Any) -> None:
    from futrisk.messages.common import SnapshotBatch, SnapshotRequest
    from futrisk.messages.envelope import decode_envelope
    from futrisk.messages.risk import FirmControl, FirmSnapshot, KillSwitchRequest

    from .re_support import daily_loss_rule, firm_loss_rule

    env = env_factory(rules=[daily_loss_rule(), firm_loss_rule(-0.003).model_copy(update={"hold_ms": 200})])
    sh1 = env.shell(1)
    sh0 = env.shell(0)
    assert sh0.svc.is_coordinator and not sh1.svc.is_coordinator
    wait_for(lambda: len(sh0.call(lambda: sh0.core.aggregator.snapshots())) == 5, what="5 account snapshots (ACC5 quiet)")

    def firm() -> FirmSnapshot | None:
        r = request(env.api, "risk-engine-0", "ctl.snapshot", SnapshotRequest(prefixes=["risk.firm"]))
        items = r.open_as(SnapshotBatch).items if r.ok else []
        got = body(decode_envelope(items[0].envelope)) if items else None
        return got if isinstance(got, FirmSnapshot) else None

    fs = wait_for(lambda: (lambda f: f if f is not None and f.equity > 54_000_000 else None)(firm()), what="risk.firm of 2 shards")
    assert fs.equity == pytest.approx(55_000_000, rel=1e-3) and sum(fs.accounts_by_state.values()) == 5
    assert {s.shard for s in fs.shards} == {0, 1}

    env.md.tick("rb2701", 3040.0)  # ACC3 -1.8% (no account alert); firm -0.33% <= -0.3%
    wait_for(lambda: sh1.call(lambda: sh1.core.kill_switch_effective()), what="RULE kill switch on shard 1")
    assert sh0.call(lambda: sh0.core.kill_switch_effective())
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3040.0).code == codes.REJECT_FIRM_KILL_SWITCH
    assert submit(env.bot, "ACC3", "rb2701", S, CY, 1, 3040.0).ok
    assert submit(env.bot, "ACC1", "rb2701", B, OPEN, 1, 3040.0, target="risk-engine-0").code == codes.REJECT_FIRM_KILL_SWITCH
    controls = [body(e) for e in replay(env.api, "risk-engine-0", "risk.control.firm")]
    # the coordinator publishes its whole RULE layer as one fact (RULE_STATE: active = the rule kill switch)
    assert [(c.action.value, c.origin.value, c.active) for c in controls if isinstance(c, FirmControl)] == [
        ("RULE_STATE", "RULE", True)]

    # the manual switch is fanned out by api-gateway to every shard; a rule OFF never clears it
    for target in ("risk-engine-0", "risk-engine-1"):
        r = request(env.api, target, "firm.kill_switch", KillSwitchRequest(command_id="ks-1", on=True, reason="drill",
                                                                           actor="rm", role=Role.RISK_MANAGER))
        assert r.ok and r.open_as(FirmControl).origin.value == "MANUAL"
    env.md.tick("rb2701", 3100.0)  # firm loss back inside: the RULE flag clears after hold_ms
    wait_for(lambda: not sh1.call(lambda: sh1.core.firm.rule_kill), what="RULE off on shard 1")
    assert sh1.call(lambda: sh1.core.kill_switch_effective())  # manual still on
    for target in ("risk-engine-0", "risk-engine-1"):
        assert request(env.api, target, "firm.kill_switch", KillSwitchRequest(command_id="ks-2", on=False, reason="done",
                                                                               actor="rm", role=Role.RISK_MANAGER)).ok
    wait_for(lambda: not sh1.call(lambda: sh1.core.kill_switch_effective()), what="kill switch off")
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3030.0).ok  # below the resting close: no self trade
    rows = sh1.svc.db.read(lambda c: c.exec_driver_sql(
        "SELECT action, origin, active, src_instance FROM firm_controls ORDER BY control_id").fetchall())
    assert [tuple(r) for r in rows] == [
        ("RULE_STATE", "RULE", 1, "risk-engine-0"), ("KILL_SWITCH_ON", "MANUAL", 1, "api-gateway"),
        ("RULE_STATE", "RULE", 0, "risk-engine-0"), ("KILL_SWITCH_OFF", "MANUAL", 0, "api-gateway")]


# ------------------------------------------------------------------------------------------------ config
def test_config_reload_commits_new_rules_immediately(env: Env) -> None:
    import shutil

    import yaml

    from futrisk.common.config import load_config_dir
    from futrisk.common.paths import bundled_config_dir
    from futrisk.messages.risk import QueryRules, RuleSet
    from futrisk.messages.system import ConfigCommit, ConfigPrepare, ConfigPrepared

    from .re_support import daily_loss_rule

    sh = env.shell()

    def version_dir(n: int, rules: list[dict[str, Any]]) -> Any:
        d = env.home / "re1" / "config" / "versions" / str(n)
        shutil.copytree(bundled_config_dir(), d)
        (d / "services.yaml").write_text(yaml.safe_dump(env.cfg.services.model_dump(mode="json")), encoding="utf-8")
        rr = env.cfg.risk_rules.model_dump(mode="json")
        rr["rules"] = rules
        (d / "risk_rules.yaml").write_text(yaml.safe_dump(rr, allow_unicode=True), encoding="utf-8")
        return load_config_dir(d, n)

    rule = daily_loss_rule().model_dump(mode="json")
    rule["levels"][0]["threshold"] = -0.015
    new = version_dir(2, [rule])
    p = query(env.api, "ctl.config.prepare", ConfigPrepare(config_version=2, config_hash=new.config_hash,
                                                           path=str(new.root), files=new.files, requested_by="test"),
              ConfigPrepared)
    assert p.ok, p.errors
    assert p.effects["risk_rules"].value == "IMMEDIATE"
    assert request(env.api, RE1, "ctl.config.commit", ConfigCommit(config_version=2)).ok
    rs = query(env.api, "risk.query.rules", QueryRules(), RuleSet)
    assert (rs.config_version, rs.config_hash) == (2, new.config_hash)
    assert rs.rules[0]["levels"][0]["threshold"] == -0.015
    env.md.tick("rb2701", 3050.0)  # -1.5%: only the new warn line is crossed
    wait_for(lambda: acc(sh)[0] == AccountState.WARNING, what="new threshold applied")
    audit = sh.svc.db.read(lambda c: c.exec_driver_sql("SELECT action FROM audit_events").fetchall())
    assert ("CONFIG_COMMIT",) in [tuple(a) for a in audit]
    # a broken rule set is voted down at prepare
    bad = dict(rule, id="broken", metric="account.no_such_metric")
    cfg3 = version_dir(3, [rule, bad])
    p3 = query(env.api, "ctl.config.prepare", ConfigPrepare(config_version=3, config_hash=cfg3.config_hash,
                                                            path=str(cfg3.root), files=cfg3.files, requested_by="test"),
               ConfigPrepared)
    assert not p3.ok and any("no_such_metric" in e for e in p3.errors)


# ------------------------------------------------------------------------------------------------ clock
def test_clock_jumps_forward_confirm_timers_and_enter_provisional(env: Env) -> None:
    from datetime import datetime

    from futrisk.common.clock import BJ
    from futrisk.messages.enums import DayRollMode
    from futrisk.messages.eod import DayRoll, ForceUnfreezeRequest

    from .re_support import jump

    sh = env.shell()
    env.md.tick("rb2701", 2995.0)  # one breaching state: HALTED pending (confirm: 2 states or 1 s)
    wait_for(lambda: acc(sh)[0] == AccountState.HALTED, what="pending halt")
    jump(env.clock, datetime(2026, 9, 28, 14, 58, tzinfo=BJ))  # to the close: the 1 s confirmation is overdue
    wait_for(lambda: acc(sh)[1], what="latched by the confirm timer after the jump")
    assert sh.call(lambda: sh.core.trading_day) == TD and sh.svc.phase == Phase.RUNNING
    # across the session boundary: the night session of NTD starts at 21:00 -> PROVISIONAL (02 §8.4)
    jump(env.clock, datetime(2026, 9, 28, 21, 5, tzinfo=BJ))
    wait_for(lambda: sh.call(lambda: (sh.core.trading_day, sh.core.trading_mode)) == (NTD, TradingMode.PROVISIONAL),
             what="provisional SOD")
    assert env.md.settle_requests and {r.trading_day for r in env.md.settle_requests} == {TD}
    rolls = [body(e) for e in replay(env.api, RE1, "sys.dayroll")]
    assert [(d.mode, d.to_trading_day) for d in rolls if isinstance(d, DayRoll)] == [(DayRollMode.PROVISIONAL, NTD)]
    assert acc(sh)[0] == AccountState.RESTRICTED and not acc(sh)[1]  # latch released by the day roll
    blocked = submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3000.0)
    assert blocked.code == codes.REJECT_ACCOUNT_BLOCKED and ack(blocked).reject_detail["source"] == "SYSTEM:EOD_NOT_SETTLED"
    assert submit(env.bot, "ACC3", "rb2701", S, CY, 1, 3000.0).ok  # closes pass
    frozen = _freeze(env)  # clearing continues its saga: digests of T only, no mode change
    assert frozen.trading_day == TD and sh.call(lambda: sh.core.trading_mode) == TradingMode.PROVISIONAL
    unfreeze = ForceUnfreezeRequest(command_id=uuid7(), trading_day=TD, reason="audited drill", actor="rm",
                                    role=Role.RISK_MANAGER)
    assert request(env.api, RE1, "eod.force_unfreeze", unfreeze).ok
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 2990.0).ok  # opens allowed now (below the resting close)
    kinds = [e.kind for e in (body(x) for x in replay(env.api, RE1, "risk.event")) if isinstance(e, RiskEvent)]
    assert RiskEventKind.EOD_FORCE_UNFREEZE in kinds
    from .re_support import ALL_ACCOUNTS, sod

    env.cw.add(sod(ALL_ACCOUNTS, trading_day=NTD, version=1))
    r = _rollover(env, TD, NTD, 1)
    assert r.ok and r.open_as(DayRoll).mode == DayRollMode.REBASE
    assert sh.call(lambda: sh.core.trading_mode) == TradingMode.OPEN


# ------------------------------------------------------------------------------------------------ shutdown / maintenance
def test_ctl_shutdown_replies_at_once_and_exits_cleanly(env: Env) -> None:
    from futrisk.messages.common import ShutdownRequest

    from .re_support import client

    sh = env.shell()
    sup = client(env.cfg, "supervisor")
    try:
        wait_ready([sup], RE1)  # the supervisor's connection is up, as it is when the supervisor stops a service
        t0 = time.monotonic()
        r = sup.request(RE1, "ctl.shutdown", ShutdownRequest(reason="test"), timeout_ms=2000, retries=0)
        replied = time.monotonic() - t0
        assert r.ok and replied < 1.0, (r, replied)
        sh.join(10)
        exited = time.monotonic() - t0
        assert not sh.alive() and sh.exit_code == 0
        assert exited < 5.0, exited  # well inside the supervisor's shutdown_grace_s (10 s)
    finally:
        sup.close()
    con = sqlite3.connect(env.home / "re1" / "db" / "risk-1.db")
    try:
        assert con.execute("SELECT clean_shutdown FROM db_meta").fetchone() == (1,)
    finally:
        con.close()


def test_maintenance_runs_after_sys_eod_done(env: Env) -> None:
    from datetime import datetime

    from futrisk.common.clock import BJ
    from futrisk.messages.enums import EodPhase

    from .re_support import jump

    sh = env.shell()
    _cash(env, "ACC3", CashType.DEPOSIT, "1.00")
    assert not sh.call(lambda: sh.svc.market_quiet())  # 09:35: a session is open, maintenance would wait
    jump(env.clock, datetime(2026, 9, 28, 15, 40, tzinfo=BJ))  # after the close, hours before the night session
    assert sh.call(lambda: sh.svc.market_quiet())

    def done() -> Any:
        env.cw.eod(TD, EodPhase.DONE)  # repeated: the first ones may beat the subscription (slow joiner)
        return sh.svc.maintenance_reports or None

    reports = wait_for(done, step=0.1, what="maintenance report")
    rep = reports[0]
    assert rep.errors == [] and rep.trading_day == TD
    backup = env.home / "re1" / "backup" / TD / "risk-1.db"
    assert backup.exists() and rep.backup == str(backup)
    con = sqlite3.connect(backup)
    try:
        assert con.execute("SELECT COUNT(*) FROM cash_movements").fetchone() == (1,)
    finally:
        con.close()
    # once per trading day: a repeated DONE of the same day starts nothing
    assert sh.call(lambda: sh.svc.start_maintenance(TD)) is False
    assert len(sh.svc.maintenance_reports) == 1


# ------------------------------------------------------------------------------------------------ multi-day catch-up
def test_two_missed_eods_are_caught_up_and_rebased_like_a_shard_that_never_missed(env: Env) -> None:
    """D and D+1 missed (two PROVISIONAL rolls), then clearing settles D (CATCH_UP) and D+1 (REBASE on D+2)."""
    from datetime import datetime

    from futrisk.common.clock import BJ
    from futrisk.messages.enums import DayRollMode
    from futrisk.messages.eod import DayRoll
    from futrisk.messages.orders import compute_account_digest

    from .re_support import ALL_ACCOUNTS, jump, lot, sod

    d0, d1, d2 = TD, NTD, "20260930"
    sh = env.shell()
    o1 = _accepted_order(env, sh, "ACC3", 2)  # D: +2 rb
    env.tg.fill(o1)
    wait_for(lambda: _td_qty(sh) == 2, what="D fill")
    jump(env.clock, datetime(2026, 9, 28, 21, 5, tzinfo=BJ))  # D+1 night session: first provisional roll
    wait_for(lambda: sh.call(lambda: sh.core.trading_day) == d1, what="provisional D+1")
    env.tg.trading_day = d1
    close = uuid7()  # D+1: one yesterday lot closed (closes pass on a provisional SOD)
    assert submit(env.bot, "ACC3", "rb2701", S, CY, 1, 3100.0, coid=close).ok
    wait_for(lambda: order_status(env, sh, close) == OrderStatus.ACCEPTED, what="close accepted")
    env.tg.fill(close)
    wait_for(lambda: order_status(env, sh, close) == OrderStatus.FILLED, what="D+1 fill")
    jump(env.clock, datetime(2026, 9, 29, 21, 5, tzinfo=BJ))  # D+2 night session: second provisional roll
    wait_for(lambda: sh.call(lambda: sh.core.trading_day) == d2, what="provisional D+2")

    # clearing catches up: official SODs of D+1 (after D) and D+2 (after D+1)
    sod1 = dict(ALL_ACCOUNTS, ACC3=("10000000", [lot("rb2701", 300, 3100.0, n=1), lot("rb2701", 2, 3100.0, n=2)]))
    sod2 = dict(ALL_ACCOUNTS, ACC3=("10000000", [lot("rb2701", 299, 3100.0, n=1), lot("rb2701", 2, 3100.0, n=2)]))
    env.cw.add(sod(sod1, trading_day=d1, version=1))
    env.cw.add(sod(sod2, trading_day=d2, version=1))
    f0 = _freeze(env, d0)
    assert f0.accounts == [compute_account_digest(a, env.tg.trades_of(a, d0)) for a in ("ACC3", "ACC4", "ACC5")]
    r1 = _rollover(env, d0, d1, 1)
    assert r1.ok and r1.open_as(DayRoll).mode == DayRollMode.CATCH_UP
    dup = _rollover(env, d0, d1, 1)
    assert dup.ok and dup.duplicate and dup.open_as(DayRoll).mode == DayRollMode.CATCH_UP
    f1 = _freeze(env, d1)
    assert f1.accounts == [compute_account_digest(a, env.tg.trades_of(a, d1)) for a in ("ACC3", "ACC4", "ACC5")]
    r2 = _rollover(env, d1, d2, 1)
    assert r2.ok and r2.open_as(DayRoll).mode == DayRollMode.REBASE
    # the final state is the one of a shard that rolled on time: D+2 on the official SOD, no provisional left
    assert sh.call(lambda: (sh.core.trading_day, sh.core.trading_mode, sh.core.sod_version)) == (d2, TradingMode.OPEN, 1)
    pos = sh.call(lambda: sorted((p.instrument_id, p.td_qty, p.yd_qty)
                                  for p in sh.core.account("ACC3").ledger.book.positions("ACC3") if p.qty))
    assert pos == [("rb2701", 0, 301)]
    assert not any(r.source == "SYSTEM:EOD_NOT_SETTLED" for r in sh.call(lambda: sh.core.account("ACC3").restrictions))


def test_fill_gap_restored_after_a_crash_is_lifted_once_caught_up(env: Env) -> None:
    sh = env.shell()
    sh.call(lambda: sh.svc._set_gap("ACC3", "repair", True))  # a replay failed: opens blocked (02 §8.2)
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3100.0).code == codes.REJECT_ACCOUNT_BLOCKED
    sh2 = env.restart(sh, crash=True)
    assert not _fill_gap(sh2)
    assert submit(env.bot, "ACC3", "rb2701", B, OPEN, 1, 3100.0).ok
