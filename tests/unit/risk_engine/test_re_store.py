"""risk-{shard}.db: Persist rows on the real schema, restore round trip, history queries (ENGINE.md §6, §8; 03 §4.3)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from futrisk.common.config import FullConfig
from futrisk.core.alerts import ALERT_ACTIVE
from futrisk.core.effects import CommandCtx, Persist, Request
from futrisk.core.engine import EngineCore
from futrisk.core.risk_account import Lifecycle
from futrisk.messages import codes
from futrisk.messages.enums import (
    AccountState,
    BrokerOrderStatus,
    CashType,
    Direction,
    Offset,
    OrderStatus,
    PosDirection,
    ReconResolution,
    RiskEventKind,
    Role,
)
from futrisk.messages.envelope import ok_reply
from futrisk.messages.eod import DayRoll, EodFreezeRequest, EodRolloverRequest
from futrisk.messages.orders import BrokerPosition, BrokerPositionBatch
from futrisk.messages.risk import (
    AckAlertRequest,
    CashMoveRequest,
    HaltRequest,
    KillSwitchRequest,
    LiquidationConfirm,
    LiquidationPlan,
    ReconResolveRequest,
    ResetRequest,
)
from futrisk.services.risk_engine.schema import BUSINESS_TABLES, METADATA
from futrisk.services.risk_engine.store import RiskStore

from .core_harness import CoreHarness, compare_tables
from .re_support import NTD, TD, daily_loss_rule, make_config, shard1_sod

O, CY = Offset.OPEN, Offset.CLOSE_YESTERDAY  # noqa: E741
B, S = Direction.BUY, Direction.SELL
RM, ADMIN = Role.RISK_MANAGER, Role.ADMIN

P0 = {"rb2701": 3100.0, "IF2612": 4500.0}
P_WARN = {"rb2701": 3030.0}  # 300 lots x 10 x -70 = -210k = -2.1%
P_RESTRICT = {"rb2701": 3015.0}  # -2.55%
P_HALT = {"rb2701": 2995.0}  # -3.15%
P_HALT_2 = {"rb2701": 2994.0}
P_DERISK = {"rb2701": 2930.0}  # -5.1%


@pytest.fixture
def cfg(default_config: FullConfig) -> FullConfig:
    return make_config(default_config, rules=[daily_loss_rule()])


@pytest.fixture
def h(cfg: FullConfig, tmp_path: Path) -> Iterator[CoreHarness]:
    harness = CoreHarness(cfg, tmp_path / "risk-1.db")
    harness.load(shard1_sod())
    harness.tick(P0)
    yield harness
    harness.close()


def book_through_breaker(h: CoreHarness) -> dict[str, str]:
    """Orders, fills, cash, alert -> restrict -> latched halt, manual commands, kill switch, recon break."""
    ids: dict[str, str] = {}
    # a routed + accepted + filled open, a resting open (ACCEPTED), a resting close, a pre-trade reject
    r, _ = h.submit("ACC3", "rb2701", B, O, 2, 3100.0, coid="o-fill")
    assert r.ok, r
    h.route_ok("o-fill")
    h.broker("o-fill", BrokerOrderStatus.ACCEPTED, remaining=2)
    h.fill("o-fill")
    r, _ = h.submit("ACC3", "rb2701", B, O, 1, 3050.0, coid="o-rest")
    assert r.ok, r
    h.route_ok("o-rest")
    h.broker("o-rest", BrokerOrderStatus.ACCEPTED, remaining=1)
    r, _ = h.submit("ACC3", "rb2701", S, CY, 10, 3150.0, coid="o-close")
    assert r.ok, r
    h.route_ok("o-close")
    r, _ = h.submit("ACC3", "rb2701", B, O, 1, 3100.5, coid="o-bad")
    assert r.code == codes.REJECT_PRICE_OFF_TICK
    # cash, then duplicate
    cm = CashMoveRequest(movement_id="m-1", account_id="ACC4", type=CashType.DEPOSIT, amount="1000.50", reason="top up",
                         actor="admin", role=ADMIN)
    assert h.cmd("cash.move", cm)[0].ok
    assert h.cmd("cash.move", cm)[0].duplicate
    # the daily loss path: warn -> restrict -> halt pending -> latched
    h.advance(100)
    h.tick(P_WARN)
    h.advance(100)
    h.tick(P_RESTRICT)
    h.advance(100)
    effs = h.tick(P_HALT)
    assert h.core.account("ACC3").state == AccountState.HALTED and not h.core.account("ACC3").halted_latched
    assert h.core.orders.get("o-rest").status == OrderStatus.CANCEL_PENDING, effs
    h.advance(200)
    h.tick(P_HALT_2)
    assert h.core.account("ACC3").halted_latched
    ids["halt_key"] = f"{TD}|daily_loss|halt|acct:ACC3|1"
    assert h.cmd("risk.ack_alert", AckAlertRequest(command_id="c-ack", alert_key=ids["halt_key"], actor="rm", role=RM))[0].ok
    # manual halt of ACC5 and the firm kill switch
    assert h.cmd("risk.halt", HaltRequest(command_id="c-halt", account_id="ACC5", reason="drill", actor="rm", role=RM))[0].ok
    assert h.cmd("firm.kill_switch", KillSwitchRequest(command_id="c-ks", on=True, reason="drill", actor="rm", role=RM))[0].ok
    # a position recon mismatching twice -> blocking POSITION_MISMATCH break (ACC4 broker says 4 lots)
    for _ in range(2):
        effs = h.run(h.core.request_position_recon())
        req = [e for e in effs if isinstance(e, Request)]
        assert req, effs
        batch = BrokerPositionBatch(trading_day=TD, as_of_ns=h.clock.now_ns(), items=[
            BrokerPosition(account_id="ACC3", instrument_id="rb2701", direction=PosDirection.LONG, td_qty=2, yd_qty=300),
            BrokerPosition(account_id="ACC4", instrument_id="IF2612", direction=PosDirection.LONG, td_qty=0, yd_qty=4),
        ])
        h.advance(6000)
        h.run(h.core.on_request_result(req[0].token, ok_reply(batch)))
    breaks = [b for b in h.core.breaks.values() if b.account_id == "ACC4"]
    assert breaks and breaks[0].blocking
    ids["break_id"] = breaks[0].break_id
    return ids


def test_every_persist_row_lands_in_the_real_schema(h: CoreHarness) -> None:
    ids = book_through_breaker(h)
    # derisk (-5%) creates a liquidation plan; the manager confirms it
    h.advance(200)
    h.tick(P_DERISK)
    plans = list(h.core.plans.values())
    assert plans, "no liquidation plan"
    assert h.cmd("risk.liquidation.confirm", LiquidationConfirm(command_id="c-liq", plan_id=plans[0].plan_id,
                                                                decision="CONFIRM", actor="rm", role=RM))[0].ok
    assert h.cmd("risk.recon.resolve", ReconResolveRequest(command_id="c-rr", break_id=ids["break_id"],
                                                           resolution=ReconResolution.ACCEPTED, note="ok", actor="rm",
                                                           role=RM))[0].ok
    assert h.cmd("risk.reset", ResetRequest(command_id="c-reset", account_id="ACC3", reason="reviewed", actor="rm",
                                            role=RM))[0].ok
    # timers: snapshots + metric samples
    h.advance(1500)
    h.run(h.core.on_timer())
    # EOD: freeze, then the roll (settle.sod_snapshot answered here)
    rep, _ = h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
    assert rep.ok
    effs = h.run(h.core.handle(EodRolloverRequest(trading_day=TD, next_trading_day=NTD, sod_version=1),
                               CommandCtx("eod.rollover", token="roll", requester="clearing-worker")))
    req = [e for e in effs if isinstance(e, Request)][0]
    effs = h.run(h.core.on_request_result(req.token, ok_reply(shard1_sod(trading_day=NTD, version=1))))
    assert any(isinstance(m, DayRoll) for m in h.facts_of("sys.dayroll"))

    counts = compare_tables(h, BUSINESS_TABLES)
    written = {t for t, n in counts.items() if n}
    assert written >= set(BUSINESS_TABLES) - {"retro_breaches"}, sorted(set(BUSINESS_TABLES) - written)
    # every op of ENGINE.md §6 was exercised
    ops = {op.value for _t, op in h.ref.ops}
    assert ops == {"INSERT", "INSERT_IGNORE", "UPSERT", "UPDATE", "DELETE"}
    # JSON and bool columns are stored as JSON text / 0-1 integers
    raw = h.db.read(lambda c: c.exec_driver_sql(
        "SELECT restrictions, halted_latched FROM account_states WHERE account_id='ACC3'").first())
    assert isinstance(raw[0], str) and raw[0].startswith("[") and raw[1] in (0, 1)
    # the outbox received every FACT in order, created_ns = the engine time of the event
    n_facts = h.db.read(lambda c: c.exec_driver_sql("SELECT COUNT(*) FROM outbox").scalar())
    assert n_facts == len(h.facts) and n_facts > 20
    alerts = h.db_rows("alerts")
    assert {a["status"] for a in alerts} >= {"ROLLED"} and any(a["acked_by"] == "rm" for a in alerts)


def test_restore_rebuilds_the_live_state(h: CoreHarness, cfg: FullConfig) -> None:
    book_through_breaker(h)
    live = h.core
    tables, peaks = h.db.read(h.store.load_restore)
    re = EngineCore(shard=1, registry=h.registry, rules=cfg.risk_rules, accounts=cfg.accounts, clock=h.clock,
                    ids=h.ids, calendar=cfg.calendar, config_hash="cfg-hash")
    re.restore(tables, lifecycle=Lifecycle.RUNNING, intraday_peaks=peaks)
    assert re.trading_day == live.trading_day and re.sod_version == live.sod_version
    for a in live.account_ids():
        ra, rb = live.account(a), re.account(a)
        assert (rb.state, rb.halted_latched, rb.state_version) == (ra.state, ra.halted_latched, ra.state_version), a
        assert sorted(map(repr, rb.restrictions)) == sorted(map(repr, ra.restrictions)), a
        assert {k: m.st.persisted() for k, m in rb.machines.items()} == {k: m.st.persisted() for k, m in ra.machines.items()}
        pa = {(p.instrument_id, p.direction, p.td_qty, p.yd_qty) for p in ra.ledger.book.positions(a)}
        pb = {(p.instrument_id, p.direction, p.td_qty, p.yd_qty) for p in rb.ledger.book.positions(a)}
        assert pa == pb, a
        assert [c.movement_id for c in rb.cash] == [c.movement_id for c in ra.cash]
        assert rb.intraday_peak == pytest.approx(ra.intraday_peak) or ra.intraday_peak <= rb.intraday_peak
        assert rb.cancels_today == ra.cancels_today, a  # (only the events entering CANCEL_PENDING are loaded)
    assert live.account("ACC3").cancels_today >= 1  # the breaker's cancel of the resting open
    assert live.account("ACC3").state == AccountState.HALTED and re.account("ACC3").halted_latched
    assert re.account("ACC5").halted_latched  # manual halt is a latched machine
    assert set(re.alerts) == set(live.alerts)
    assert all(r.status == ALERT_ACTIVE for k, r in re.alerts.items() if "halt" in k)
    live_resting = {o.client_order_id: o.status for o in live.orders.resting()}
    re_resting = {o.client_order_id: o.status for o in re.orders.resting()}
    assert re_resting == live_resting and "o-close" in re_resting
    assert (re.firm.manual_kill, re.firm.kill_switch) == (True, True)
    assert {b.break_id for b in re.breaks.values()} == {b.break_id for b in live.breaks.values()}
    # processed commands survive: a duplicate cash.move is still a duplicate
    effs = re.handle(CashMoveRequest(movement_id="m-1", account_id="ACC4", type=CashType.DEPOSIT, amount="1000.50",
                                     reason="top up", actor="admin", role=ADMIN), CommandCtx("cash.move", token="t"))
    assert [e.reply.duplicate for e in effs if hasattr(e, "reply")] == [True]
    # re-evaluating the same market state raises nothing new
    evs = [e for e in (re.post_restore() + re.evaluate_all()) if isinstance(e, Persist)]
    raised = [m.payload for p in evs for m in p.outbox if getattr(m.payload, "kind", None) == RiskEventKind.ALERT_RAISED]
    assert raised == []


def test_history_queries_from_risk_db(h: CoreHarness) -> None:
    book_through_breaker(h)
    h.advance(1500)
    h.run(h.core.on_timer())
    h.advance(1000)
    h.run(h.core.on_timer())
    db, store = h.db, h.store
    orders = db.read(lambda c: store.query_orders(c, "ACC3", TD, None))
    assert {o.client_order_id for o in orders} == {"o-fill", "o-rest", "o-close", "o-bad"}
    assert {o.client_order_id: o.status for o in orders}["o-bad"] == OrderStatus.REJECTED
    only = db.read(lambda c: store.query_orders(c, "ACC3", TD, ["FILLED"]))
    assert [o.client_order_id for o in only] == ["o-fill"]
    fills = db.read(lambda c: store.query_fills(c, "ACC3", TD))
    assert [(f.client_order_id, f.qty, f.price) for f in fills] == [("o-fill", 2, 3100.0)]
    events = db.read(lambda c: store.query_events(c, TD, "ACC3"))
    kinds = [e.kind for e in events]
    assert kinds.count(RiskEventKind.ALERT_RAISED) >= 3
    assert [e.level for e in events if e.kind == RiskEventKind.ALERT_RAISED][:3] == ["warn", "restrict", "halt"]
    cash = db.read(lambda c: store.query_cash(c, TD, None))
    assert [(c.movement_id, c.amount, c.type) for c in cash] == [("m-1", "1000.50", CashType.DEPOSIT)]
    series = db.read(lambda c: store.query_metrics(c, "ACC3", TD, 1))
    assert len(series.points) >= 2 and series.points[-1].state == AccountState.HALTED
    assert any("daily_loss/acct:ACC3" in p.rule_util for p in series.points)
    coarse = db.read(lambda c: store.query_metrics(c, "ACC3", TD, 3600))
    assert len(coarse.points) == 1 and coarse.points[0].ts_ns == series.points[-1].ts_ns
    assert db.read(lambda c: store.query_orders(c, "ACC3", "20200101", None)) == []
    dr = db.read(store.last_dayroll)
    assert dr is None  # no roll yet


def test_row_writer_rejects_unknown_columns(tmp_path: Path, cfg: FullConfig) -> None:
    from futrisk.core.effects import Row, RowOp
    from futrisk.db import Database

    db = Database(tmp_path / "x.db", METADATA)
    db.open()
    try:
        store = RiskStore(None)
        bad = Persist(rows=(Row("orders", RowOp.UPSERT, ("client_order_id",), {"client_order_id": "x", "nope": 1}),))
        with pytest.raises(KeyError):
            db.write(lambda c: store.apply_persist(c, bad, now_ns=0))
        # nothing was committed
        assert db.read(lambda c: c.exec_driver_sql("SELECT COUNT(*) FROM orders").scalar()) == 0
    finally:
        db.close()


def test_liquidation_plan_row_and_plan_payload(h: CoreHarness) -> None:
    book_through_breaker(h)
    h.advance(200)
    h.tick(P_DERISK)
    rows = h.db_rows("liquidation_plans")
    assert len(rows) == 1 and rows[0]["status"] == "PENDING_CONFIRM" and isinstance(rows[0]["steps"], list)
    plan = next(iter(h.core.plans.values()))
    assert isinstance(plan, LiquidationPlan) and rows[0]["trigger_alert_key"] == plan.trigger_alert_key


def test_unstorable_orders_are_answered_without_an_orders_row(h: CoreHarness) -> None:
    """03 §4.3 ``orders.qty`` CHECK(qty>0) and the INTEGER range: the core answers such orders without a row (it
    used to persist REJECT_INVALID_QTY rows with qty 0, a failed commit that would have stopped the shard)."""
    zero, eff0 = h.submit("ACC3", "rb2701", B, O, 0, 3100.0, coid="o-zero")
    huge, eff1 = h.submit("ACC3", "rb2701", B, O, 2**40, 3100.0, coid="o-huge")
    assert (zero.code, huge.code) == (codes.REJECT_INVALID_QTY, codes.REJECT_QTY_EXCEEDS_MAX)
    assert not any(isinstance(e, Persist) for e in eff0 + eff1)
    assert h.db_rows("orders") == []


def test_facts_go_through_the_outbox_with_engine_time(tmp_path: Path) -> None:
    """``append_fact`` = ``Outbox.append_msg``: a sequence per topic, ``created_ns`` = the engine time of the event,
    handed to the publisher only after the commit (a rolled-back job publishes nothing)."""
    from futrisk.core.effects import OutboxMsg
    from futrisk.db import Database, Outbox
    from futrisk.messages.risk import CashMovement
    from futrisk.services.risk_engine.schema import MIGRATIONS

    msgs = [OutboxMsg(topic, CashMovement(movement_id=f"m{i}", account_id="ACC3", trading_day=TD, type=CashType.DEPOSIT,
                                          amount="1.00", actor="a", reason="r", ts_ns=100 + i), corr_id=f"c{i}", td=TD,
                      ts_ns=100 + i)
            for i, topic in enumerate(["risk.cash.ACC3", "risk.cash.ACC4", "risk.cash.ACC3"])]
    db = Database(tmp_path / "o.db", METADATA, migrations=MIGRATIONS)
    db.open()
    try:
        ob = Outbox(db, instance="risk-engine-1")
        sent: list[tuple[str, int]] = []
        ob.attach_sink(lambda rows: sent.extend((r.topic, r.topic_seq) for r in rows)).result(5)
        store = RiskStore(ob, epoch="ep")

        def failing(c: Any) -> None:
            store.append_fact(c, msgs[0], now_ns=0)
            raise RuntimeError("rolled back")

        with pytest.raises(RuntimeError):
            db.write(failing)
        db.write(lambda c: [store.append_fact(c, m, now_ns=0) for m in msgs])
        db.flush()
        rows = db.read(lambda c: [tuple(r) for r in c.exec_driver_sql(
            "SELECT topic, topic_seq, type, epoch, td, corr_id, created_ns FROM outbox ORDER BY outbox_id").fetchall()])
        assert rows == [("risk.cash.ACC3", 1, "CashMovement", "ep", TD, "c0", 100),
                        ("risk.cash.ACC4", 1, "CashMovement", "ep", TD, "c1", 101),
                        ("risk.cash.ACC3", 2, "CashMovement", "ep", TD, "c2", 102)]
        assert sent == [("risk.cash.ACC3", 1), ("risk.cash.ACC4", 1), ("risk.cash.ACC3", 2)]
    finally:
        db.close()


def test_schema_v1_database_is_upgraded_to_the_current_version(tmp_path: Path) -> None:
    """v2 adds the orders cancel columns, v3 rebuilds firm_controls for RULE_STATE (rows and control_ids kept)."""
    import sqlite3

    from futrisk.db import Database
    from futrisk.services.risk_engine.schema import METADATA_V1, MIGRATIONS, SCHEMA_VERSION

    path = tmp_path / "risk-1.db"
    old = Database(path, METADATA_V1, migrations=MIGRATIONS[:1])
    old.open()
    row = ("KILL_SWITCH_ON", "MANUAL", 1, "[]", TD, "rm", "drill", None, "api-gateway", 5)
    ins = ("INSERT INTO firm_controls (action, origin, active, restrictions, trading_day, actor, reason, "
           "cause_alert_key, src_instance, ts_ns) VALUES (?,?,?,?,?,?,?,?,?,?)")
    old.write(lambda c: c.exec_driver_sql(ins, row))
    with pytest.raises(Exception, match="CHECK"):  # v1 does not know RULE_STATE
        old.write(lambda c: c.exec_driver_sql(ins, ("RULE_STATE", *row[1:])))
    old.close()
    db = Database(path, METADATA, migrations=MIGRATIONS)
    info = db.open()
    try:
        assert info.applied_migrations == (2, 3) and info.schema_version == SCHEMA_VERSION == 3
        cols = {r[1] for r in db.read(lambda c: c.exec_driver_sql("PRAGMA table_info(orders)").fetchall())}
        assert {"cancel_command_id", "cancel_sent_ns", "cancel_attempts"} <= cols
        db.write(lambda c: c.exec_driver_sql(ins, ("RULE_STATE", "RULE", 0, "[]", TD, None, "rule", None,
                                                   "risk-engine-0", 6)))
        rows = db.read(lambda c: c.exec_driver_sql(
            "SELECT control_id, action, reason FROM firm_controls ORDER BY control_id").fetchall())
        assert [tuple(r) for r in rows] == [(1, "KILL_SWITCH_ON", "drill"), (2, "RULE_STATE", "rule")]
        idx = db.read(lambda c: c.exec_driver_sql("PRAGMA index_list(firm_controls)").fetchall())
        assert "ix_firm_controls__origin_action" in {r[1] for r in idx}
    finally:
        db.close()
    con = sqlite3.connect(path)
    try:
        assert con.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert con.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        con.close()


def test_released_migrations_never_change() -> None:
    """A database records the checksum of every applied migration and refuses to open when the code's differs
    (03 §9): migration 1 is generated from METADATA_V1, whose enum CHECK lists are frozen (schema.enum_values) and
    whose common tables come from futrisk.db. A change here needs a new migration instead."""
    from futrisk.services.risk_engine.schema import MIGRATIONS

    assert [(m.version, m.checksum) for m in MIGRATIONS] == [
        (1, "157270b37fc64e071541e77558231d9283b2e6c38d31f43ec10b58cd2bbbdac5"),
        (2, "d3f4e1080bfbed8572f58b2f0a8ef1087f964ca4ce5d5196070523b5780860b4"),
        (3, "6fba002705358a1c4617470cdcf837bd3758428438960ae84ecbcacd8efd627a"),
    ]


def test_every_stored_enum_value_is_allowed() -> None:
    """Every value of the message enums stored in risk.db passes the current schema's CHECK lists (a new value
    without a migration would fail the commit and stop the shard)."""
    from futrisk.messages import enums
    from futrisk.services.risk_engine.schema import SCHEMA_VERSION, enum_values

    allowed = enum_values(SCHEMA_VERSION)
    missing = {name: sorted({m.value for m in getattr(enums, name)} - set(vals)) for name, vals in allowed.items()}
    assert {k: v for k, v in missing.items() if v} == {}


def test_cursor_upsert_matches_fact_cursors(tmp_path: Path) -> None:
    from futrisk.bus.fact import CursorUpdate
    from futrisk.db import Database, FactCursors
    from futrisk.messages.enums import FactStartPolicy
    from futrisk.services.risk_engine.schema import MIGRATIONS

    db = Database(tmp_path / "c.db", METADATA, migrations=MIGRATIONS)
    db.open()
    try:
        store = RiskStore(None)
        for seq in (1, 2, 7):
            cur = CursorUpdate("trade-gateway", "td.exec.ACC3", "stream-1", seq, FactStartPolicy.FROM_BEGINNING)
            db.write(lambda c, cur=cur, seq=seq: store.save_cursor(c, cur, now_ns=100 + seq))
        got = FactCursors(db).load_all()[("trade-gateway", "td.exec.ACC3")]
        assert (got.stream, got.last_seq, got.start_policy, got.updated_ns) == (
            "stream-1", 7, FactStartPolicy.FROM_BEGINNING, 107)
    finally:
        db.close()
