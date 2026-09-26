"""Regressions of the independent review of the risk-engine shell (its probes P1-P11, the P5 crash matrix, L3, L4, L7).

Each test pins down one finding with the reviewer's scenario on the shared fakes. Commands that must be handled in
ONE pump batch are sent back to back on one DEALER connection (:class:`Pipeline`) while the business loop is held
(:func:`hold_loop`): the ROUTER reads a connection in order, so the batch is deterministic.
"""

from __future__ import annotations

import dataclasses
import shutil
import sqlite3
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future
from datetime import datetime
from typing import Any

import pytest
import zmq

from futrisk.bus.rpc_common import request_envelope
from futrisk.bus.sockets import make_dealer
from futrisk.common.clock import BJ, bj_to_ns
from futrisk.common.config import FullConfig, RiskRulesConfig, RuleDef
from futrisk.common.config.risk_rules import RuleLevel
from futrisk.common.ids import uuid7
from futrisk.core.effects import Persist
from futrisk.messages import codes
from futrisk.messages.base import Msg, decode
from futrisk.messages.common import ShutdownRequest, SnapshotBatch, SnapshotRequest
from futrisk.messages.enums import (
    AccountState,
    BrokerOrderStatus,
    ConfigEffect,
    Direction,
    FirmAction,
    MdQuality,
    Offset,
    OrderStatus,
    OrderType,
    ReconBreakKind,
    ReconResolution,
    RiskEventKind,
    Role,
    TradeOrigin,
    TradingMode,
)
from futrisk.messages.envelope import Reply, decode_envelope
from futrisk.messages.eod import DayRoll, EodFreezeAck, EodFreezeRequest, EodRolloverRequest
from futrisk.messages.orders import OrderAck, OrderRequest
from futrisk.messages.risk import (
    AccountStateChanged,
    FirmControl,
    HaltRequest,
    ReconResolveRequest,
    ResetRequest,
    RiskEvent,
)
from futrisk.services.risk_engine.service import Phase

from .re_support import (
    NTD,
    TD,
    TG,
    Env,
    Shell,
    all_sod,
    body,
    daily_loss_rule,
    jump,
    replay,
    request,
    wait_for,
    wait_ready,
)

pytestmark = pytest.mark.timeout(180)
RE1 = "risk-engine-1"
B, S = Direction.BUY, Direction.SELL
OPEN, CY = Offset.OPEN, Offset.CLOSE_YESTERDAY


# ---------------------------------------------------------------------------------------------------- helpers
def order(account: str, inst: str, direction: Direction, offset: Offset, qty: int, price: float,
          coid: str | None = None) -> OrderRequest:
    return OrderRequest(client_order_id=coid or uuid7(), account_id=account, instrument_id=inst, direction=direction,
                        offset=offset, order_type=OrderType.LIMIT, price=price, qty=qty, sent_ns=time.time_ns())


def submit(env: Env, account: str, inst: str, direction: Direction, offset: Offset, qty: int, price: float,
           *, coid: str | None = None, target: str = RE1) -> Reply:
    return request(env.bot, target, "order.submit", order(account, inst, direction, offset, qty, price, coid))


def acc(sh: Shell, account: str = "ACC3") -> tuple[AccountState, bool, int]:
    got: tuple[AccountState, bool, int] = sh.call(
        lambda: (lambda ra: (ra.state, ra.halted_latched, ra.state_version))(sh.core.account(account)))
    return got


def events(env: Env, src: str = RE1) -> list[RiskEvent]:
    return [e for e in (body(x) for x in replay(env.api, src, "risk.event")) if isinstance(e, RiskEvent)]


def halt(env: Env, sh: Shell) -> None:
    r = request(env.api, RE1, "risk.halt", HaltRequest(command_id=uuid7(), account_id="ACC3", reason="drill",
                                                       actor="rm", role=Role.RISK_MANAGER))
    assert r.ok, r
    wait_for(lambda: acc(sh)[:2] == (AccountState.HALTED, True), what="manual halt latched")


def freeze(env: Env, td: str = TD) -> EodFreezeAck:
    r = request(env.clr, RE1, "eod.freeze", EodFreezeRequest(trading_day=td, version=1), timeout_ms=30_000)
    assert r.ok, r
    a = r.open_as(EodFreezeAck)
    assert isinstance(a, EodFreezeAck)
    return a


def hold_loop(sh: Shell, seconds: float = 15.0, *, collect: int = 0) -> threading.Event:
    """Hold the shell's business thread (at the end of a step) until the returned event is set.

    With ``collect`` the held thread then moves that many requests from its ROUTER into the RPC backlog before it
    goes on (waiting for them to arrive): the next pump handles all of them as ONE batch, whatever the transport
    timing of the connections that sent them.
    """
    release, holding = threading.Event(), threading.Event()

    def blocker() -> None:
        holding.set()
        release.wait(seconds)
        server = sh.rt.server
        end = time.monotonic() + 10
        while collect and server is not None and server.backlog_size < collect and time.monotonic() < end:
            if not server.receive():
                server.socket.poll(5)

    sh._q.put((blocker, Future()))
    assert holding.wait(20), "the business loop did not pick up the blocker"
    return release


def in_thread(fn: Callable[[], Any]) -> tuple[threading.Thread, dict[str, Any]]:
    out: dict[str, Any] = {}

    def run() -> None:
        try:
            out["result"] = fn()
        except BaseException as exc:
            out["error"] = exc

    th = threading.Thread(target=run, daemon=True)
    th.start()
    return th, out


def fail_commits(sh: Shell, when: Callable[[Persist], bool]) -> list[str]:
    """Make the risk.db commits of matching Persists fail (the job raises inside the writer transaction)."""
    store = sh.svc.store
    assert store is not None
    orig = store.apply_persist
    failed: list[str] = []

    def flaky(conn: Any, p: Persist | None, **kw: Any) -> None:
        if p is not None and when(p):
            failed.append(p.label)
            raise sqlite3.OperationalError("database is locked (injected)")
        orig(conn, p, **kw)

    store.apply_persist = flaky
    return failed


def drop_connection(c: Any, target: str) -> None:
    """Close a client's DEALER to ``target`` (it would take its identity back from a :class:`Pipeline` when it
    reconnects: ROUTER_HANDOVER); the client opens a new one on its next request."""
    peer = c._peers.pop(target, None)
    if peer is not None:
        peer[0].close(0)


class Pipeline:
    """Requests sent back to back on ONE DEALER connection (identity ``identity``); replies matched by corr_id.

    The DEALER takes over the identity's connection at the ROUTER (``ROUTER_HANDOVER``): the environment's own
    client of that identity reconnects when this one is closed.
    """

    def __init__(self, cfg: FullConfig, identity: str, target: str) -> None:
        self.ctx: zmq.Context[Any] = zmq.Context()
        router = cfg.services.instance(target).router
        assert router is not None
        self.sock = make_dealer(self.ctx, router, identity=identity)
        self.identity = identity
        self.epoch = uuid7()
        self.sent: list[str] = []
        assert self.sock.poll(10_000, zmq.POLLOUT), f"{identity} -> {target} not connected"

    def send(self, command: str, payload: Msg) -> str:
        corr: str = uuid7()
        self.sock.send(request_envelope(command, payload, src=self.identity, epoch=self.epoch, ts_ns=time.time_ns(),
                                        corr_id=corr), zmq.NOBLOCK)
        self.sent.append(corr)
        return corr

    def replies(self, timeout: float = 20.0, *, peer_gone: Callable[[], bool] | None = None) -> dict[str, Reply]:
        """Replies received within ``timeout``. Read while the peer is up: with ``IMMEDIATE`` a DEALER drops what it
        has not read yet when the peer's connection closes. ``peer_gone`` ends the wait shortly after it is true."""
        out: dict[str, Reply] = {}
        end = time.monotonic() + timeout
        gone_at: float | None = None
        while len(out) < len(self.sent):
            now = time.monotonic()
            if peer_gone is not None and gone_at is None and peer_gone():
                gone_at = now
            if now > end or (gone_at is not None and now - gone_at > 0.5):
                break
            if self.sock.poll(50, zmq.POLLIN):
                env = decode_envelope(self.sock.recv_multipart()[-1])
                if env.corr_id is not None:
                    out[env.corr_id] = decode(env.payload, Reply)
        return out

    def close(self) -> None:
        self.sock.close(0)
        self.ctx.term()


def count_heartbeats(sh: Shell, peer: str) -> Callable[[], int]:
    """Counter of ``peer``'s heartbeats received by the shell from now on."""
    seen = [0]

    def on_hb(hb: Any) -> None:
        if hb.instance == peer:
            seen[0] += 1

    sh.call(lambda: sh.rt.on_heartbeat(on_hb, [peer]))
    return lambda: seen[0]


# ---------------------------------------------------------------------------------------------------- C1 / P1
def test_p1_a_failed_commit_stops_every_command_of_its_batch(env: Env) -> None:
    """risk.reset's commit fails; the open order right behind it (same batch) is never routed; exit 1; still HALTED."""
    sh = env.shell()
    halt(env, sh)
    failed = fail_commits(sh, lambda p: p.label == "risk.reset")
    coid = uuid7()
    drop_connection(env.api, RE1)
    pipe = Pipeline(env.cfg, "api-gateway", RE1)
    try:
        release = hold_loop(sh, collect=2)
        c_reset = pipe.send("risk.reset", ResetRequest(command_id=uuid7(), account_id="ACC3", reason="drill over",
                                                       actor="rm", role=Role.RISK_MANAGER))
        c_order = pipe.send("order.submit", order("ACC3", "rb2701", B, OPEN, 1, 3100.0, coid))
        release.set()
        replies = pipe.replies(peer_gone=lambda: not sh.alive())
    finally:
        pipe.close()
    sh.join()
    assert failed == ["risk.reset"] and not sh.alive() and sh.exit_code == 1
    # nothing about state that was never written is answered or sent: both were in the failed batch (UNAVAILABLE; a
    # reply lost with the closing socket is fine)
    assert all(r.code == codes.UNAVAILABLE for r in replies.values()), replies
    assert {c_reset, c_order} >= set(replies)
    assert coid not in env.tg.routed()
    sh2 = env.shell(home=sh.home)
    assert acc(sh2)[:2] == (AccountState.HALTED, True)
    audit = sh2.svc.db.read(lambda c: [r[0] for r in c.exec_driver_sql("SELECT action FROM audit_events").fetchall()])
    assert "RISK_RESET" not in audit
    assert sh2.svc.db.read(lambda c: c.exec_driver_sql(
        "SELECT COUNT(*) FROM orders WHERE client_order_id = ?", (coid,)).fetchone()) == (0,)
    assert submit(env, "ACC3", "rb2701", B, OPEN, 1, 3100.0, coid=coid).code == codes.REJECT_ACCOUNT_HALTED


def test_c1_a_failed_commit_in_a_reply_batch_answers_held_commands_and_stops(env: Env) -> None:
    """Catch-up replies applied in one batch fail to commit: the held eod.freeze is never answered from memory."""
    env.tg.trade("ACC5", "rb2701", B, OPEN, 3100.0, 1, origin=TradeOrigin.EXTERNAL, publish=False)  # found by query
    env.tg.silent_for.add("td.query.trades")
    sh = env.shell(phase=Phase.CATCHUP)
    th, out = in_thread(lambda: env.clr.request(RE1, "eod.freeze", EodFreezeRequest(trading_day=TD, version=1),
                                                timeout_ms=15_000, retries=0))
    wait_for(lambda: sh.call(lambda: [d.command for d in sh.svc._deferred]) == ["eod.freeze"], what="freeze held")
    failed = fail_commits(sh, lambda p: True)
    env.tg.silent_for.clear()  # the next td.query.trades retry is answered: its trade is applied in a reply batch
    sh.join(30)
    th.join(30)
    assert failed and not sh.alive() and sh.exit_code == 1 and sh.svc.poisoned is not None
    r = out.get("result")
    assert r is not None and not r.ok and r.code in (codes.UNAVAILABLE, codes.TIMEOUT), out
    sh2 = env.shell(home=sh.home)  # the durable state is intact: the trade comes back with the catch-up
    wait_for(lambda: sh2.call(lambda: sum(p.td_qty for p in sh2.core.account("ACC5").ledger.book.positions("ACC5"))) == 1,
             what="trade applied after the restart")


# ---------------------------------------------------------------------------------------------------- M7 / P1b
def _record_commands(sh: Shell) -> list[tuple[str, str, bool, Any]]:
    """Spy on the shell's command entry: (command, client_order_id, the runtime still running, the handler result)."""
    seen: list[tuple[str, str, bool, Any]] = []
    orig = sh.svc._on_command

    def spy(command: str, payload: Msg, ctx: Any) -> Any:
        running = sh.rt.running
        res = orig(command, payload, ctx)
        seen.append((command, getattr(payload, "client_order_id", ""), running, res))
        return res

    sh.svc._on_command = spy
    sh.rt.add_handler("order.submit", spy, payload_type=OrderRequest)  # (the ROUTER holds the bound method)
    return seen


def test_p1b_orders_behind_ctl_shutdown_are_refused_not_routed(env: Env) -> None:
    """ctl.shutdown and three orders in one batch (two connections, fair-queued): an order handled after the stop
    request is answered REJECT_SYSTEM_NOT_READY at once and never routed; one handled before it is processed."""
    sh = env.shell()
    drop_connection(env.bot, RE1)  # the pipeline below takes over the strategy-sim identity alone
    seen = sh.call(lambda: _record_commands(sh))
    sup = Pipeline(env.cfg, "supervisor", RE1)
    bot = Pipeline(env.cfg, "strategy-sim", RE1)
    coids = [uuid7() for _ in range(3)]
    try:
        release = hold_loop(sh, collect=4)
        sup.send("ctl.shutdown", ShutdownRequest(reason="test"))
        for c in coids:
            bot.send("order.submit", order("ACC5", "rb2701", B, OPEN, 1, 3100.0, c))
        release.set()
        sh.join()
    finally:
        sup.close()
        bot.close()
    assert not sh.alive() and sh.exit_code == 0
    assert sh.core.lifecycle.value == "STOPPING"  # (an order after the stop was refused: the core stops too)
    handled = {coid: (running, res) for command, coid, running, res in seen if command == "order.submit"}
    assert set(handled) == set(coids)  # all four requests were in one batch
    for coid, (running, res) in handled.items():
        if running:  # ahead of ctl.shutdown in the fair queue: a normal order
            assert coid in env.tg.routed()
        else:  # behind it: refused with an immediate reply, never routed
            assert isinstance(res, Reply) and res.code == codes.REJECT_SYSTEM_NOT_READY
            assert res.open_as(OrderAck).accepted is False and coid not in env.tg.routed()
    # one message per connection and round (fair queue): at most one order is ahead of ctl.shutdown
    assert sum(1 for running, _res in handled.values() if not running) >= 2


# ---------------------------------------------------------------------------------------------------- M4 / P2
def test_p2_an_expired_replay_range_is_not_expired_again_at_every_restart(env: Env) -> None:
    sh = env.shell()
    sh.stop()
    env.tg.trade("ACC3", "rb2701", B, OPEN, 3100.0, 3, origin=TradeOrigin.EXTERNAL)  # while the shard is down
    ob = env.tg.rt.outbox
    wait_for(lambda: ob.count_unpublished() == 0, what="published")
    assert env.tg.db is not None
    env.tg.db.write(lambda c: ob.purge_published_before(c, 2**62))  # the replay window is gone
    head = ob.last_seq("td.exec.ACC3")

    def gap_events() -> int:
        return sum(1 for e in events(env) if e.kind == RiskEventKind.FILL_GAP)

    def order_queries() -> int:
        return sum(1 for c, _p in list(env.tg.received) if c == "td.query.orders")

    def expired(s: Shell) -> int:
        return sum(1 for b in s.call(lambda: list(s.core.breaks.values())) if b.kind == ReconBreakKind.REPLAY_EXPIRED)

    def fill_gap(s: Shell) -> bool:
        return bool(s.call(lambda: any(r.source == "SYSTEM:FILL_GAP" for r in s.core.account("ACC3").restrictions)))

    sh2 = env.shell(home=sh.home)
    wait_for(lambda: expired(sh2) == 1 and order_queries() >= 1 and not fill_gap(sh2), what="first fallback done")
    wait_for(lambda: sh2.svc.db.read(lambda c: c.exec_driver_sql(
        "SELECT last_seq FROM fact_cursors WHERE topic = 'td.exec.ACC3'").fetchone()) == (head,),
        what="cursor persisted at the end of the expired range")
    counts = (gap_events(), order_queries())
    prev = sh2
    for _ in range(2):
        prev = env.restart(prev, crash=True)
        _no_second_fallback(prev, head, counts, (gap_events, order_queries), expired, fill_gap)


def _no_second_fallback(sh: Shell, head: int, counts: tuple[int, int],
                        counters: tuple[Callable[[], int], Callable[[], int]], expired: Callable[[Shell], int],
                        fill_gap: Callable[[Shell], bool]) -> None:
    beats = count_heartbeats(sh, TG)
    wait_for(lambda: beats() >= 5, what="trade-gateway heartbeats (tail checks) after the restart")
    assert sh.call(lambda: sh.rt.fact_tracker.expected(TG, "td.exec.ACC3")) == head + 1
    assert sh.call(lambda: sh.rt.fact_tracker.stats.expired) == 0
    assert (counters[0](), counters[1]()) == counts and expired(sh) == 1 and not fill_gap(sh)


# ---------------------------------------------------------------------------------------------------- H2 / P7
def _firm_rule() -> RuleDef:
    return RuleDef(id="firm_loss", metric="firm.daily_pnl_pct_init", op="<=", scope="firm", levels=[
        RuleLevel(name="restrict", threshold=-0.002, action="RESTRICT", params={"max_order_factor": 0.5},
                  confirm={"states": 1, "ms": 0}),
        RuleLevel(name="kill", threshold=-0.003, action="CIRCUIT_BREAK", confirm={"states": 1, "ms": 0}),
    ])


def test_p7_the_firm_snapshot_fallback_restores_the_whole_rule_layer(env_factory: Any) -> None:
    env = env_factory(rules=[daily_loss_rule(), _firm_rule()])
    sh1 = env.shell(1)
    sh0 = env.shell(0)
    wait_for(lambda: len(sh0.call(lambda: sh0.core.aggregator.snapshots())) == 5, what="coordinator sees both shards")
    env.md.tick("rb2701", 3040.0)  # firm about -0.33%: restrict (-0.2%) and kill (-0.3%) in the same tick
    wait_for(lambda: sh1.call(lambda: sh1.core.firm.rule_kill), what="shard 1 got the RULE kill switch live")
    snap = request(env.api, "risk-engine-0", "ctl.snapshot", SnapshotRequest(prefixes=["risk.control.firm"]))
    items = snap.open_as(SnapshotBatch).items
    newest = decode(decode_envelope(items[0].envelope).payload, FirmControl)
    assert newest.action == FirmAction.RULE_STATE and newest.active and newest.restrictions  # ONE message: all of it
    # shard 1 loses its database while the coordinator's risk.control.firm rows are past the replay window
    sh1.stop()
    ob = sh0.rt.outbox
    wait_for(lambda: ob.count_unpublished() == 0, what="coordinator outbox published")
    sh0.svc.db.write(lambda c: ob.purge_published_before(c, 2**62))
    shutil.rmtree(sh1.home / "db")
    sh1b = env.shell(1, home=sh1.home)  # fresh database: FROM_BEGINNING replay -> NOT_FOUND -> ctl.snapshot fallback
    wait_for(lambda: sh1b.call(lambda: sh1b.core.firm.rule_kill), what="RULE kill switch restored by the fallback")
    restr = sh1b.call(lambda: sorted(r.scope_key for r in sh1b.core.firm.restrictions))
    assert restr == sorted(r.scope_key for r in newest.restrictions)
    assert sh1b.call(lambda: sh1b.core.kill_switch_effective())
    assert submit(env, "ACC3", "rb2701", B, OPEN, 1, 3040.0).code == codes.REJECT_FIRM_KILL_SWITCH


# ---------------------------------------------------------------------------------------------------- H3 / P8
def test_p8_rule_reload_effects_reach_the_database_and_survive_a_restart(env: Env) -> None:
    sh = env.shell()
    env.md.tick("rb2701", 3030.0)  # -2.1%: daily_loss/warn ACTIVE
    wait_for(lambda: acc(sh)[0] == AccountState.WARNING, what="WARNING")
    n_events = len(events(env))
    rr = env.cfg.risk_rules
    new = dataclasses.replace(env.cfg, risk_rules=RiskRulesConfig(rules=[], pretrade=rr.pretrade, system=rr.system),
                              config_version=2)
    sh.call(lambda: sh.svc._on_config_commit(new, {"risk_rules": ConfigEffect.IMMEDIATE}))
    state, _latched, version = acc(sh)
    assert state == AccountState.NORMAL  # the rule is gone: its alert is closed and the state refreshed
    db_alerts = sh.svc.db.read(lambda c: [tuple(r) for r in c.exec_driver_sql("SELECT status FROM alerts").fetchall()])
    db_state = sh.svc.db.read(lambda c: tuple(c.exec_driver_sql(
        "SELECT state, state_version FROM account_states WHERE account_id = 'ACC3'").fetchone()))
    assert db_alerts == [("CLEARED",)] and db_state == ("NORMAL", version)
    assert RiskEventKind.ALERT_CLEARED in [e.kind for e in events(env)[n_events:]]
    last = [s for s in (body(e) for e in replay(env.api, RE1, "risk.state.ACC3")) if isinstance(s, AccountStateChanged)][-1]
    assert (last.to_state, last.state_version) == (AccountState.NORMAL, version)
    # restart on the committed configuration: memory rebuilt from the database agrees, nothing is raised again
    n_events = len(events(env))
    sh.crash()
    sh2 = Shell(new, sh.home, env.clock, 1)
    env.shells.append(sh2)
    sh2.start()
    wait_ready(env.clients, RE1)
    assert acc(sh2)[0] == AccountState.NORMAL
    assert sh2.call(lambda: [a.status for a in sh2.core.alerts.values()]) == ["CLEARED"]
    assert events(env)[n_events:] == []


# ---------------------------------------------------------------------------------------------------- H1 / P9
def test_p9_snapshot_ticks_never_override_md_status(env: Env) -> None:
    env.md.status({"IF2612": MdQuality.STALE})  # IF2612's feed died after its last tick (still in the LV cache)
    sh = env.shell()  # snapshot: md.status + the old IF tick; ACC4 holds IF2612
    assert sh.call(lambda: sh.core.quality_of("IF2612")) == MdQuality.STALE
    assert "IF2612" not in sh.svc._live_ticks  # a snapshot price is never a fresh mark for the warm-up
    assert submit(env, "ACC4", "IF2612", B, OPEN, 1, 4500.0).code == codes.REJECT_MD_UNAVAILABLE
    # md-gateway outage and resync: the snapshot tick does not make IF2612 fresh either
    env.md.hb.pause()
    wait_for(lambda: sh.call(lambda: sh.core.quality_of("rb2701")) == MdQuality.STALE, what="md down")
    env.md.hb.resume()
    wait_for(lambda: sh.call(lambda: sh.core.quality_of("rb2701")) == MdQuality.OK, what="md back")
    assert sh.call(lambda: sh.core.quality_of("IF2612")) == MdQuality.STALE
    assert submit(env, "ACC4", "IF2612", B, OPEN, 1, 4499.0).code == codes.REJECT_MD_UNAVAILABLE


# ---------------------------------------------------------------------------------------------------- M1 / P10
def test_p10_a_fresh_database_is_evaluated_only_after_the_catch_up(env: Env) -> None:
    env.tg.trade("ACC3", "rb2701", S, CY, 3100.0, 300, origin=TradeOrigin.EXTERNAL)  # ACC3 flat since this morning
    env.md.tick("rb2701", 2995.0)  # -3.15% on the SOD book: a HALT if the bare SOD were evaluated
    sh = env.shell()
    assert sh.call(lambda: sum(p.qty for p in sh.core.account("ACC3").ledger.book.positions("ACC3"))) == 0
    assert [e.kind for e in events(env) if e.account_id == "ACC3" and e.kind == RiskEventKind.ALERT_RAISED] == []
    states = [s for s in (body(e) for e in replay(env.api, RE1, "risk.state.ACC3")) if isinstance(s, AccountStateChanged)]
    assert {s.to_state for s in states} <= {AccountState.NORMAL, AccountState.RESTRICTED}  # (RESTRICTED: STARTING)
    assert acc(sh)[0] == AccountState.NORMAL


# ---------------------------------------------------------------------------------------------------- H4 / P11
def test_p11_an_order_id_of_an_earlier_day_is_refused_and_its_history_kept(env: Env) -> None:
    sh = env.shell()
    coid = uuid7()
    assert submit(env, "ACC5", "rb2701", B, OPEN, 1, 3100.0, coid=coid).ok
    wait_for(lambda: coid in env.tg.orders, what="routed")
    env.tg.fill(coid)
    wait_for(lambda: sh.call(lambda: sh.core.orders.get(coid).status) == OrderStatus.FILLED, what="filled")
    freeze(env)
    env.cw.add(all_sod(trading_day=NTD, version=1))
    r = request(env.clr, RE1, "eod.rollover", EodRolloverRequest(trading_day=TD, next_trading_day=NTD, sod_version=1),
                timeout_ms=30_000)
    assert r.ok and r.open_as(DayRoll).to_trading_day == NTD
    env.tg.trading_day = NTD

    def row() -> Any:
        return sh_now[0].svc.db.read(lambda c: tuple(c.exec_driver_sql(
            "SELECT trading_day, status, filled_qty FROM orders WHERE client_order_id = ?", (coid,)).fetchone()))

    sh_now = [sh]
    assert row() == (TD, "FILLED", 1)
    routes = sum(1 for c, p in list(env.tg.received) if c == "route.order" and getattr(p, "client_order_id", "") == coid)
    again = submit(env, "ACC5", "rb2701", B, OPEN, 1, 3100.0, coid=coid)
    assert again.code == codes.REJECT_DUPLICATE_ORDER_ID and not again.ok
    assert row() == (TD, "FILLED", 1)
    # after a restart the core no longer knows the id: the shell's primary-key lookup refuses it
    sh_now[0] = env.restart(sh, crash=True)
    again = submit(env, "ACC5", "rb2701", B, OPEN, 1, 3100.0, coid=coid)
    assert again.code == codes.REJECT_DUPLICATE_ORDER_ID and again.open_as(OrderAck).reject_detail == {"trading_day": TD}
    assert row() == (TD, "FILLED", 1)
    assert sum(1 for c, p in list(env.tg.received)
               if c == "route.order" and getattr(p, "client_order_id", "") == coid) == routes


# ---------------------------------------------------------------------------------------------------- P5 crash matrix
def _no_new_alerts(env: Env, before: int) -> None:
    kinds = [e.kind for e in events(env)[before:]]
    assert RiskEventKind.ALERT_RAISED not in kinds and RiskEventKind.MANUAL_HALT not in kinds, kinds


def _still_halted(env: Env, sh: Shell, before: int, *, open_code: str = codes.REJECT_ACCOUNT_HALTED,
                  close: bool = True) -> None:
    assert acc(sh)[:2] == (AccountState.HALTED, True)
    assert submit(env, "ACC3", "rb2701", B, OPEN, 1, 3100.0).code == open_code
    if close:
        c = submit(env, "ACC3", "rb2701", S, CY, 1, 3150.0)
        assert c.ok, c
    _no_new_alerts(env, before)


def test_p5a_crash_between_the_cancel_pending_commit_and_route_cancel(env: Env) -> None:
    sh = env.shell()
    coid = uuid7()
    assert submit(env, "ACC3", "rb2701", B, OPEN, 1, 3050.0, coid=coid).ok
    wait_for(lambda: env.tg.orders.get(coid) is not None and env.tg.orders[coid].status == BrokerOrderStatus.ACCEPTED,
             what="resting open at the broker")
    dropped: list[str] = []
    sh.svc._cancel = lambda e, attempt: dropped.append(e.client_order_id)
    halt(env, sh)  # the halt cancels the resting open: the CancelRoute never leaves the process
    wait_for(lambda: dropped, what="CancelRoute dropped")
    assert sh.call(lambda: sh.core.orders.get(coid).status) == OrderStatus.CANCEL_PENDING
    before = len(events(env))
    sh2 = env.restart(sh, crash=True)
    wait_for(lambda: env.tg.orders[coid].status == BrokerOrderStatus.CANCELED, what="cancel re-sent after the restart")
    wait_for(lambda: sh2.call(lambda: sh2.core.orders.get(coid).status) == OrderStatus.CANCELED, what="CANCELED")
    _still_halted(env, sh2, before)


def test_p5cd_crash_during_catch_up_and_during_warm_up(env: Env) -> None:
    sh = env.shell()
    halt(env, sh)
    before = len(events(env))
    env.tg.silent_for.add("td.query.trades")  # the restart stays in CATCHUP
    sh.crash()
    sh2 = env.shell(home=sh.home, phase=Phase.CATCHUP)
    assert submit(env, "ACC3", "rb2701", S, CY, 1, 3150.0).code == codes.REJECT_SYSTEM_NOT_READY
    assert acc(sh2)[:2] == (AccountState.HALTED, True)
    sh2.crash()
    env.tg.silent_for = {"td.query.positions"}  # the next restart stays in WARMUP (position recon unanswered)
    sh3 = env.shell(home=sh.home, phase=Phase.WARMUP)
    assert sh3.svc.phase == Phase.WARMUP and acc(sh3)[:2] == (AccountState.HALTED, True)
    assert not submit(env, "ACC3", "rb2701", B, OPEN, 1, 3100.0).ok
    sh3.crash()
    env.tg.silent_for = set()
    sh4 = env.shell(home=sh.home)
    _still_halted(env, sh4, before)


def test_p5e_crash_after_eod_freeze(env: Env) -> None:
    sh = env.shell()
    halt(env, sh)
    a1 = freeze(env)
    before = len(events(env))
    sh2 = env.restart(sh, crash=True)
    assert sh2.call(lambda: sh2.core.trading_mode) == TradingMode.EOD_FROZEN
    a2 = freeze(env)
    assert (a2.accounts, a2.cash) == (a1.accounts, a1.cash)
    _still_halted(env, sh2, before, open_code=codes.REJECT_EOD_IN_PROGRESS, close=False)


def test_p5f_crash_while_provisional(env: Env) -> None:
    sh = env.shell()
    jump(env.clock, datetime(2026, 9, 28, 21, 5, tzinfo=BJ))
    wait_for(lambda: sh.call(lambda: sh.core.trading_mode) == TradingMode.PROVISIONAL, what="PROVISIONAL")
    env.md.ticks({"rb2701": 3100.0, "IF2612": 4500.0}, trading_day=NTD)
    halt(env, sh)
    before = len(events(env))
    env.tg.trading_day = NTD
    sh2 = env.restart(sh, crash=True)
    assert sh2.call(lambda: (sh2.core.trading_mode, sh2.core.trading_day)) == (TradingMode.PROVISIONAL, NTD)
    assert acc(sh2)[:2] == (AccountState.HALTED, True)
    assert submit(env, "ACC3", "rb2701", B, OPEN, 1, 3100.0).code in (codes.REJECT_ACCOUNT_HALTED,
                                                                        codes.REJECT_ACCOUNT_BLOCKED)
    _no_new_alerts(env, before)


# ---------------------------------------------------------------------------------------------------- L3
def test_l3_a_next_day_fill_survives_a_double_crash(env: Env) -> None:
    """A night fill of NTD buffered before the roll, lost by crash 1, still pending at the roll's query when crash 2
    hits after later NTD fills: the start-up catch-up asks the whole day, so it comes back."""
    sh = env.shell()
    t1 = env.tg.trade("ACC5", "rb2701", B, OPEN, 3100.0, 2, origin=TradeOrigin.EXTERNAL, trading_day=NTD)
    wait_for(lambda: sh.call(lambda: sh.rt.fact_tracker.expected(TG, "td.exec.ACC5")) == 2, what="buffered")
    sh2 = env.restart(sh, crash=True)  # crash 1: the buffered fill is gone from memory, its fact is behind the cursor
    freeze(env)
    env.cw.add(all_sod(trading_day=NTD, version=1))
    env.tg.silent_for.add("td.query.trades")  # the roll's td.query.trades{NTD} stays unanswered
    r = request(env.clr, RE1, "eod.rollover", EodRolloverRequest(trading_day=TD, next_trading_day=NTD, sod_version=1),
                timeout_ms=30_000)
    assert r.ok
    env.tg.trading_day = NTD
    t2 = env.tg.trade("ACC5", "rb2701", B, OPEN, 3101.0, 3, origin=TradeOrigin.EXTERNAL)  # a later NTD fill, live

    def td_qty(s: Shell) -> int:
        return int(s.call(lambda: sum(p.td_qty for p in s.core.account("ACC5").ledger.book.positions("ACC5"))))

    wait_for(lambda: td_qty(sh2) == 3, what="later fill applied")
    sh3 = env.restart(sh2, crash=True, phase=Phase.CATCHUP)  # crash 2 before the roll's query was answered
    env.tg.silent_for.clear()
    sh3.wait_phase(Phase.RUNNING)
    wait_for(lambda: td_qty(sh3) == 5, what="both NTD fills after the restart")
    fills = sh3.svc.db.read(lambda c: sorted(r[0] for r in c.exec_driver_sql(
        "SELECT trade_id FROM fills WHERE account_id = 'ACC5' AND trading_day = ?", (NTD,)).fetchall()))
    assert fills == sorted([t1.trade_id, t2.trade_id])


# ---------------------------------------------------------------------------------------------------- L4
def test_l4_an_undecodable_fill_blocks_opens_until_resolved(env: Env) -> None:
    sh = env.shell()
    t = env.tg.trade("ACC5", "rb2701", B, OPEN, 3100.0, 2, origin=TradeOrigin.EXTERNAL, publish=False)
    ob, db = env.tg.rt.outbox, env.tg.db
    assert ob is not None and db is not None
    db.write(lambda c: ob.append(c, topic="td.exec.ACC5", type="Trade", v=1, payload=b"\xc1 not msgpack", td=TD,
                                 corr_id=None, epoch=env.tg.rt.epoch, created_ns=env.clock.now_ns()))

    def breaks() -> list[Any]:
        return [b for b in sh.call(lambda: list(sh.core.breaks.values()))
                if b.account_id == "ACC5" and b.kind == ReconBreakKind.POSITION_MISMATCH]

    brk = wait_for(breaks, what="blocking break for the undecodable fill")[0]
    assert brk.blocking and brk.detail["reason"] == "UNDECODABLE_FACT"
    wait_for(lambda: sh.call(lambda: sum(p.td_qty for p in sh.core.account("ACC5").ledger.book.positions("ACC5"))) == 2,
             what="the fill recovered by td.query.trades")
    wait_for(lambda: not sh.call(lambda: any(r.source == "SYSTEM:FILL_GAP" for r in sh.core.account("ACC5").restrictions)),
             what="catch-up done")
    assert t.trade_id in sh.svc.db.read(lambda c: [r[0] for r in c.exec_driver_sql("SELECT trade_id FROM fills").fetchall()])
    assert submit(env, "ACC5", "rb2701", B, OPEN, 1, 3100.0).code == codes.REJECT_ACCOUNT_BLOCKED
    r = request(env.api, RE1, "risk.recon.resolve", ReconResolveRequest(
        command_id=uuid7(), break_id=brk.break_id, resolution=ReconResolution.ACCEPTED, note="fill verified",
        actor="rm", role=Role.RISK_MANAGER))
    assert r.ok, r
    assert submit(env, "ACC5", "rb2701", B, OPEN, 1, 3100.0).ok


# ---------------------------------------------------------------------------------------------------- L7
def test_l7_a_fill_gap_already_in_force_is_not_announced_twice(env: Env) -> None:
    sh = env.shell()

    def gap_events() -> int:
        return sum(1 for e in events(env) if e.kind == RiskEventKind.FILL_GAP and e.account_id == "ACC3")

    n = gap_events()
    sh.call(lambda: sh.svc.execute(sh.core.set_system_restriction("ACC3", "FILL_GAP", True)))  # as restored
    wait_for(lambda: gap_events() == n + 1, what="the restored gap's event")
    sh.call(lambda: sh.svc._set_gap("ACC3", "repair", True))  # a replay failure of the same account
    sh.call(lambda: sh.svc._set_gap("ACC3", "repair", False))
    assert not sh.call(lambda: any(r.source == "SYSTEM:FILL_GAP" for r in sh.core.account("ACC3").restrictions))
    assert gap_events() == n + 1


# ---------------------------------------------------------------------------------------------------- M3 / P3
def test_m3_maintenance_waits_for_a_quiet_market_and_never_blocks_commits(env: Env) -> None:
    sh = env.shell()
    quiet = sh.svc.market_quiet
    assert not sh.call(lambda: quiet(bj_to_ns(datetime(2026, 9, 28, 9, 35, tzinfo=BJ))))  # day session
    assert sh.call(lambda: quiet(bj_to_ns(datetime(2026, 9, 28, 15, 40, tzinfo=BJ))))  # after the close
    assert not sh.call(lambda: quiet(bj_to_ns(datetime(2026, 9, 28, 20, 50, tzinfo=BJ))))  # night auction in 5 min
    assert sh.call(lambda: sh.svc.start_maintenance(TD)) is False  # 09:35: postponed
    for _ in range(3):
        assert submit(env, "ACC5", "rb2701", B, OPEN, 1, 3100.0).ok
    reader = sqlite3.connect(str(sh.svc.db.path), timeout=5.0, isolation_level=None, check_same_thread=False)
    try:
        reader.execute("BEGIN")
        reader.execute("SELECT COUNT(*) FROM orders").fetchone()  # a slow reader pins an old snapshot
        for _ in range(3):  # newer commits: the reader is behind the WAL tail, a TRUNCATE checkpoint would wait for it
            assert submit(env, "ACC5", "rb2701", B, OPEN, 1, 3100.0).ok
        jump(env.clock, datetime(2026, 9, 28, 15, 40, tzinfo=BJ))  # quiet now: the postponed maintenance starts
        rtts: list[float] = []
        for _ in range(5):
            t0 = time.monotonic()
            r = submit(env, "ACC5", "rb2701", B, OPEN, 1, 3100.0)  # (the session is closed: a rejected order row)
            rtts.append(time.monotonic() - t0)
            assert not r.ok and r.code != codes.UNAVAILABLE, r
        reports = wait_for(lambda: sh.svc.maintenance_reports, what="maintenance report")
    finally:
        reader.execute("COMMIT")
        reader.close()
    assert max(rtts) < 2.0, rtts  # commits interleave with the maintenance (TRUNCATE + busy wait took ~4-9 s)
    rep = reports[0]
    assert rep.errors == [] and rep.backup is not None and rep.trading_day == TD


# ---------------------------------------------------------------------------------------------------- L2
def test_l2_the_coordinator_counts_running_shards_alive_from_their_heartbeats(env: Env) -> None:
    sh1 = env.shell(1)
    sh0 = env.shell(0)
    noted: list[int] = []
    orig = sh0.core.note_shard_alive

    def spy(shard: int, ts_ns: int | None = None) -> None:
        noted.append(shard)
        orig(shard, ts_ns)

    sh0.call(lambda: setattr(sh0.core, "note_shard_alive", spy))
    wait_for(lambda: noted.count(1) >= 3, what="heartbeats of risk-engine-1 noted by the coordinator")
    assert set(noted) == {1}
    sh1.stop()  # a stopped (or still starting) shard is not kept alive: its snapshots age out
    n = len(noted)
    beats = count_heartbeats(sh0, "md-gateway")
    wait_for(lambda: beats() >= 5, what="time passing on the coordinator")
    assert len(noted) == n


def test_c1_an_engine_error_poisons_the_shard_and_its_batch(env: Env) -> None:
    """An exception inside the core (not a commit): the command gets INTERNAL, the rest of its batch is not
    executed (UNAVAILABLE), later commands never reach the core, nothing is routed, exit 1."""
    from futrisk.messages.enums import CashType
    from futrisk.messages.risk import CashMoveRequest

    sh = env.shell()
    orig = sh.core.handle

    def broken(payload: Msg, ctx: Any) -> Any:
        if ctx.command == "cash.move":
            raise RuntimeError("engine bug (injected)")
        return orig(payload, ctx)

    sh.call(lambda: setattr(sh.core, "handle", broken))
    first, last = uuid7(), uuid7()
    drop_connection(env.api, RE1)
    pipe = Pipeline(env.cfg, "api-gateway", RE1)
    try:
        release = hold_loop(sh, collect=3)
        c_first = pipe.send("order.submit", order("ACC5", "rb2701", B, OPEN, 1, 3100.0, first))
        c_cash = pipe.send("cash.move", CashMoveRequest(movement_id=uuid7(), account_id="ACC5", type=CashType.DEPOSIT,
                                                        amount="1.00", reason="probe", actor="admin", role=Role.ADMIN))
        c_last = pipe.send("order.submit", order("ACC5", "rb2701", B, OPEN, 1, 3100.0, last))
        release.set()
        replies = pipe.replies(peer_gone=lambda: not sh.alive())
    finally:
        pipe.close()
    sh.join()
    assert not sh.alive() and sh.exit_code == 1
    assert replies.get(c_cash) is None or replies[c_cash].code == codes.INTERNAL
    assert replies.get(c_first) is None or replies[c_first].code == codes.UNAVAILABLE  # handled, never executed
    assert replies.get(c_last) is None or replies[c_last].code == codes.REJECT_SYSTEM_NOT_READY  # never reached it
    assert first not in env.tg.routed() and last not in env.tg.routed()
