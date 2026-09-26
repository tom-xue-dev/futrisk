"""Determinism, liquidation plans, position recon, manual commands, snapshots / samples, queries, restriction kinds."""

from __future__ import annotations

from enginetest_support import (
    CT,
    CY,
    RM,
    TD,
    B,
    C,
    Harness,
    O,
    S,
    config,
    rules_only,
    rules_with,
    sod_snapshot,
)

from futrisk.common.config.risk_rules import RuleDef
from futrisk.core.effects import CancelRoute, Persist, Request, Route
from futrisk.core.risk_account import Lifecycle
from futrisk.messages import codes
from futrisk.messages.enums import (
    AccountState,
    BrokerOrderStatus,
    LiquidationStatus,
    MdQuality,
    Offset,
    PosDirection,
    ReconBreakKind,
    ReconBreakStatus,
    ReconResolution,
    RestrictionKind,
    RiskEventKind,
    Role,
    RuleSmState,
    Severity,
    TradeOrigin,
)
from futrisk.messages.envelope import ok_reply
from futrisk.messages.eod import SodLot
from futrisk.messages.orders import BrokerPosition, BrokerPositionBatch, CancelRequest, TradeBatch
from futrisk.messages.risk import (
    AccountSnapshot,
    AccountSnapshotBatch,
    AccountStateChanged,
    AckAlertRequest,
    CashBatch,
    FillBatch,
    HaltRequest,
    LiquidationConfirm,
    LiquidationPlan,
    LiquidationPlanBatch,
    OrderBatch,
    PositionBatch,
    QueryAccounts,
    QueryAlerts,
    QueryCash,
    QueryFills,
    QueryLiquidationPlans,
    QueryMetrics,
    QueryOrders,
    QueryPositions,
    QueryReconBreaks,
    QueryRules,
    ReconBreakBatch,
    ReconResolveRequest,
    ResetRequest,
    RiskEventBatch,
    RuleSet,
)

P = {"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0, "IF2612": 4500.0}


def yd_lot(inst: str, qty: int, price: str, direction: PosDirection = PosDirection.LONG, no: int = 1) -> SodLot:
    return SodLot(instrument_id=inst, direction=direction, lot_no=no, open_td="20260928", open_ts_ns=no,
                  open_price=price, qty=qty, basis_price=price)


def script(h: Harness) -> None:
    """A deterministic mix of every kind of input."""
    h.ticks(P)
    h.open_position("ACC3", "rb2701", B, 80, 3100.0)
    h.open_position("ACC3", "hc2701", B, 75, 3300.0)
    h.submit("ACC3", "i2701", S, O, 2, 790.0)
    h.ticks({"rb2701": 3000.0, "hc2701": 3180.0})
    h.advance(300)
    h.ticks({"rb2701": 2960.0, "hc2701": 3150.0})
    h.advance(1200)
    h.timer()
    h.cmd("risk.reset", ResetRequest(command_id="r1", account_id="ACC3", reason="ok", actor="rm", role=RM))
    h.advance(600)
    h.timer()


class TestDeterminism:
    def test_same_inputs_same_effects(self) -> None:
        a, b = Harness(), Harness()
        script(a)
        script(b)
        assert a.effects == b.effects
        assert len(a.effects) > 20
        assert a.db.tables == b.db.tables


class TestLiquidation:
    def test_plan_losers_first_confirm_records_only(self) -> None:
        derisk = RuleDef.model_validate({"id": "dl", "metric": "account.daily_pnl_pct_init", "op": "<=", "levels": [
            {"name": "derisk", "threshold": -0.001, "action": "LIQUIDATE", "params": {"step": 0.5, "price_ticks": 3}}]})
        sod = sod_snapshot(lots={"ACC3": [yd_lot("rb2701", 10, "3100"), yd_lot("i2701", 5, "800", no=2)]})
        h = Harness(rules_with([derisk]), sod=sod)
        h.ticks(P)
        h.open_position("ACC3", "rb2701", B, 4, 3100.0)
        effs = h.ticks({"rb2701": 3050.0, "i2701": 790.0})
        ev = h.events(RiskEventKind.LIQUIDATION_PLAN, effs)
        assert len(ev) == 1 and ev[0].alert_key == f"{TD}|dl|derisk|acct:ACC3|1"
        plan = next(iter(h.eng.plans.values()))
        assert plan.status == LiquidationStatus.PENDING_CONFIRM and plan.trigger_alert_key == ev[0].alert_key
        steps = [(s.instrument_id, s.offset, s.qty, s.price_rule) for s in plan.steps]
        # rb 亏损更大排在前面；SHFE 先平昨再平今；按 step 取整
        assert steps == [("rb2701", Offset.CLOSE_YESTERDAY, 7, "对手价-3跳"), ("i2701", Offset.CLOSE, 3, "对手价-3跳")]
        assert h.eng.account("ACC3").state == AccountState.NORMAL  # LIQUIDATE 不单独改变账户状态
        assert h.db.tables["liquidation_plans"][0]["status"] == "PENDING_CONFIRM"
        rep, effs = h.cmd("risk.liquidation.confirm", LiquidationConfirm(command_id="lc", plan_id=plan.plan_id,
                                                                         decision="CONFIRM", actor="rm", role=RM))
        assert rep.ok and rep.open_as(LiquidationPlan).status == LiquidationStatus.CONFIRMED
        assert not [e for e in effs if isinstance(e, Route)]  # 01 TD-08：只记录决定，不自动下单
        assert [r["action"] for r in h.db.tables["audit_events"]][-1] == "LIQUIDATION_CONFIRM"
        rep2, _ = h.cmd("risk.liquidation.confirm", LiquidationConfirm(command_id="lc2", plan_id=plan.plan_id,
                                                                       decision="CANCEL", actor="rm", role=RM))
        assert rep2.code == codes.CONFLICT

    def test_plan_query_and_decision_events(self) -> None:
        derisk = RuleDef.model_validate({"id": "dl", "metric": "account.daily_pnl_pct_init", "op": "<=", "levels": [
            {"name": "derisk", "threshold": -0.001, "action": "LIQUIDATE", "params": {"step": 0.5}}]})
        sod = sod_snapshot(lots={"ACC3": [yd_lot("rb2701", 30, "3100")], "ACC4": [yd_lot("rb2701", 30, "3100")]})
        h = Harness(rules_with([derisk]), sod=sod)
        h.ticks(P)
        h.ticks({"rb2701": 3050.0})
        plans = {p.account_id: p for p in h.eng.plans.values()}
        assert sorted(plans) == ["ACC3", "ACC4"]
        q = lambda **kw: h.cmd("risk.query.liquidation_plans", QueryLiquidationPlans(**kw))[0]  # noqa: E731
        items = q().open_as(LiquidationPlanBatch).items
        assert [p.account_id for p in items] == ["ACC3", "ACC4"]
        assert all(p.status == LiquidationStatus.PENDING_CONFIRM and p.created_ns for p in items)
        _, effs = h.cmd("risk.liquidation.confirm", LiquidationConfirm(command_id="c", plan_id=plans["ACC3"].plan_id,
                                                                       decision="CONFIRM", actor="rm1", role=RM))
        (ev,) = h.events(RiskEventKind.LIQUIDATION_PLAN, effs)
        assert ev.severity == Severity.WARNING and ev.actor == "rm1" and ev.account_id == "ACC3"
        assert ev.detail == {"plan_id": plans["ACC3"].plan_id, "status": "CONFIRMED", "actor": "rm1"}
        _, effs = h.cmd("risk.liquidation.confirm", LiquidationConfirm(command_id="x", plan_id=plans["ACC4"].plan_id,
                                                                       decision="CANCEL", actor="rm2", role=RM))
        (ev,) = h.events(RiskEventKind.LIQUIDATION_PLAN, effs)
        assert ev.severity == Severity.INFO and ev.detail["status"] == "CANCELED"
        done = q(account_id="ACC3").open_as(LiquidationPlanBatch).items
        assert len(done) == 1 and done[0].status == LiquidationStatus.CONFIRMED and done[0].confirmed_by == "rm1"
        assert done[0].confirmed_ns is not None
        assert [p.account_id for p in q(status=LiquidationStatus.CANCELED).open_as(LiquidationPlanBatch).items] == ["ACC4"]
        assert q(status=LiquidationStatus.PENDING_CONFIRM).open_as(LiquidationPlanBatch).items == []
        assert q(trading_day="20260101").code == codes.NOT_FOUND  # other days: answered by the shell from risk.db

    def test_pending_plan_expires_at_the_roll_with_an_event(self) -> None:
        derisk = RuleDef.model_validate({"id": "dl", "metric": "account.daily_pnl_pct_init", "op": "<=", "levels": [
            {"name": "derisk", "threshold": -0.001, "action": "LIQUIDATE"}]})
        h = Harness(rules_with([derisk]), sod=sod_snapshot(lots={"ACC3": [yd_lot("rb2701", 30, "3100")]}))
        h.ticks(P)
        h.ticks({"rb2701": 3050.0})
        effs = h.run(h.eng.enter_provisional())
        (ev,) = h.events(RiskEventKind.LIQUIDATION_PLAN, effs)
        assert ev.detail["status"] == "EXPIRED" and ev.severity == Severity.INFO and ev.trading_day == TD


class TestRecon:
    def _batch(self, td: int, yd: int) -> BrokerPositionBatch:
        return BrokerPositionBatch(trading_day=TD, as_of_ns=1, items=[
            BrokerPosition(account_id="ACC3", instrument_id="rb2701", direction=PosDirection.LONG, td_qty=td, yd_qty=yd)])

    def test_two_mismatches_block_opens_until_resolved(self) -> None:
        h = Harness(rules_only("daily_loss"), sod=sod_snapshot(lots={"ACC3": [yd_lot("rb2701", 10, "3100")]}))
        h.ticks(P)

        def recon(batch: BrokerPositionBatch) -> list[object]:
            req = [e for e in h.run(h.eng.request_position_recon()) if isinstance(e, Request)][0]
            assert req.command == "td.query.positions"
            return h.run(h.eng.on_request_result(req.token, ok_reply(batch)))

        recon(self._batch(0, 12))
        assert not h.eng.breaks  # 第一次不一致只计数
        effs = recon(self._batch(0, 12))
        brk = next(iter(h.eng.breaks.values()))
        assert (brk.kind, brk.blocking, brk.status) == (ReconBreakKind.POSITION_MISMATCH, True, ReconBreakStatus.OPEN)
        assert any(isinstance(e, Request) and e.command == "td.query.trades" for e in effs)  # 自动补拉成交
        rep, _ = h.submit("ACC3", "rb2701", B, O, 1)
        assert rep.code == codes.REJECT_ACCOUNT_BLOCKED and rep.open().reject_detail["source"] == "SYSTEM:RECON_BREAK"
        assert h.submit("ACC3", "rb2701", S, CY, 1, 3100.0)[0].ok  # 平仓照常
        rep, _ = h.cmd("risk.recon.resolve", ReconResolveRequest(command_id="rr", break_id=brk.break_id,
                                                                 resolution=ReconResolution.ACCEPTED, note="broker adj",
                                                                 actor="rm", role=RM))
        assert rep.ok and h.eng.breaks[brk.break_id].status == ReconBreakStatus.RESOLVED
        assert h.submit("ACC3", "rb2701", B, O, 1, 3099.0)[0].ok  # 3100 会与自己的平仓挂单自成交

    def test_periodic_recon_request_from_the_timer(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        assert not [e for e in h.timer() if isinstance(e, Request)]  # 第一次只排期
        h.advance(301_000)
        reqs = [e for e in h.timer() if isinstance(e, Request)]
        assert [(r.command, r.target) for r in reqs] == [("td.query.positions", "trade-gateway")]
        assert list(h.eng.pending_requests().values()) == ["recon_positions"]

    def test_auto_resync_resolves(self) -> None:
        h = Harness(rules_only("daily_loss"), sod=sod_snapshot(lots={"ACC3": [yd_lot("rb2701", 10, "3100")]}))
        h.ticks(P)
        for _ in range(2):
            req = [e for e in h.run(h.eng.request_position_recon()) if isinstance(e, Request)][0]
            effs = h.run(h.eng.on_request_result(req.token, ok_reply(self._batch(0, 8))))
        trades_req = [e for e in effs if isinstance(e, Request) and e.command == "td.query.trades"][0]
        missing = h.trade("ACC3", "rb2701", S, CY, 3100.0, 2)
        h.run(h.eng.on_request_result(trades_req.token, ok_reply(TradeBatch(trades=[missing], next_trade_seq=99, complete=True))))
        h.advance(6000)
        req = [e for e in h.run(h.eng.request_position_recon()) if isinstance(e, Request)][0]
        h.run(h.eng.on_request_result(req.token, ok_reply(self._batch(0, 8))))
        brk = next(iter(h.eng.breaks.values()))
        assert brk.status == ReconBreakStatus.RESOLVED and brk.resolution == ReconResolution.RESYNCED
        assert not any(r.source == "SYSTEM:RECON_BREAK" for r in h.eng.account("ACC3").restrictions)


class TestManualCommands:
    def test_halt_reset_and_role_checks(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        h.submit("ACC3", "rb2701", B, O, 1, 3090.0, coid="rest")
        rep, effs = h.cmd("risk.halt", HaltRequest(command_id="h1", account_id="ACC3", reason="suspicious", actor="rm", role=RM))
        sc = rep.open_as(AccountStateChanged)
        assert sc.to_state == AccountState.HALTED and sc.halted_latched and sc.cause == "MANUAL"
        assert [e.client_order_id for e in h.cancels(effs)] == ["rest"]
        ev = h.events(RiskEventKind.MANUAL_HALT, effs)
        assert ev and ev[0].alert_key == f"{TD}|manual_halt|halt|acct:ACC3|1"
        assert h.cmd("risk.reset", ResetRequest(command_id="r0", account_id="ACC3", reason="x", actor="v", role=Role.VIEWER))[0].code == codes.FORBIDDEN
        assert h.cmd("risk.reset", ResetRequest(command_id="r1", account_id="ACC3", reason=" ", actor="rm", role=RM))[0].code == codes.BAD_REQUEST
        rep, effs = h.cmd("risk.reset", ResetRequest(command_id="r2", account_id="ACC3", reason="cleared", actor="rm", role=RM))
        assert rep.ok and h.eng.account("ACC3").state == AccountState.NORMAL
        assert [e.alert_key for e in h.events(RiskEventKind.RESET, effs)] == [f"{TD}|manual_halt|halt|acct:ACC3|1"]
        assert h.submit("ACC3", "rb2701", B, O, 1)[0].ok
        assert "risk.halt" in h.db.critical_labels and "risk.reset" in h.db.critical_labels

    def test_ack_alert(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        h.open_position("ACC3", "rb2701", B, 300, 3100.0)
        h.ticks({"rb2701": 3030.0})  # −2.1%：warn
        key = f"{TD}|daily_loss|warn|acct:ACC3|1"
        assert key in h.eng.alerts
        rep, effs = h.cmd("risk.ack_alert", AckAlertRequest(command_id="a1", alert_key=key, note="seen", actor="rm", role=RM))
        assert rep.ok and h.eng.alerts[key].acked_by == "rm"
        assert h.events(RiskEventKind.ALERT_ACKED, effs)[0].alert_key == key
        assert h.eng.account("ACC3").state == AccountState.WARNING  # 确认告警不改变状态机
        assert h.cmd("risk.ack_alert", AckAlertRequest(command_id="a2", alert_key="nope", actor="rm", role=RM))[0].code == codes.NOT_FOUND


class TestSnapshotsAndSamples:
    def test_snapshot_throttle_and_metric_samples(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        h.open_position("ACC3", "rb2701", B, 10, 3100.0)
        effs = h.timer()
        snaps = [p for p in h.publishes(effs) if p.topic.startswith("risk.snapshot.")]
        assert sorted(p.topic for p in snaps) == ["risk.snapshot.ACC3", "risk.snapshot.ACC4", "risk.snapshot.ACC5"]
        s3 = [p.payload for p in snaps if p.topic.endswith("ACC3")][0]
        assert isinstance(s3, AccountSnapshot) and s3.positions and s3.rules
        assert s3.state_version == 0  # LOADING -> RUNNING directly (load_sod keeps the lifecycle): no state change yet
        samples = h.db.tables["metric_samples"]
        assert {r["account_id"] for r in samples} == {"ACC3", "ACC4", "ACC5"}
        assert "daily_loss/acct:ACC3" in [k for r in samples for k in r["rule_util"]]
        h.advance(100)
        assert not [p for p in h.publishes(h.timer()) if p.topic.startswith("risk.snapshot.")]  # 2 Hz 节流
        h.tick("rb2701", 3101.0)
        h.advance(500)
        snaps = [p for p in h.publishes(h.timer()) if p.topic.startswith("risk.snapshot.")]
        assert [p.topic for p in snaps] == ["risk.snapshot.ACC3"]  # 只发有变化的账户
        assert snaps[0].payload.version == s3.version + 1  # type: ignore[attr-defined]
        assert h.eng.next_timer_ns() is not None


class TestQueries:
    def test_in_memory_queries(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        h.open_position("ACC3", "rb2701", B, 10, 3100.0)
        h.open_position("ACC4", "IF2612", B, 1, 4500.0)
        assert len(h.cmd("risk.query.accounts", QueryAccounts())[0].open_as(AccountSnapshotBatch).items) == 3
        pos = h.cmd("risk.query.positions", QueryPositions(product="IF"))[0].open_as(PositionBatch).items
        assert [(p.account_id, p.instrument_id) for p in pos] == [("ACC4", "IF2612")]
        pos = h.cmd("risk.query.positions", QueryPositions(sector="black"))[0].open_as(PositionBatch).items
        assert [p.account_id for p in pos] == ["ACC3"]
        orders = h.cmd("risk.query.orders", QueryOrders(account_id="ACC3", trading_day=TD))[0].open_as(OrderBatch).items
        assert len(orders) == 1 and orders[0].status.value == "FILLED"
        assert h.cmd("risk.query.orders", QueryOrders(account_id="ACC3", trading_day="20260101"))[0].code == codes.NOT_FOUND
        fills = h.cmd("risk.query.fills", QueryFills(account_id="ACC3", trading_day=TD))[0].open_as(FillBatch).items
        assert len(fills) == 1 and fills[0].qty == 10
        assert h.cmd("risk.query.cash", QueryCash(trading_day=TD))[0].open_as(CashBatch).items == []
        assert h.cmd("risk.query.recon_breaks", QueryReconBreaks())[0].open_as(ReconBreakBatch).items == []
        assert isinstance(h.cmd("risk.query.alerts", QueryAlerts(trading_day=TD))[0].open_as(RiskEventBatch).items, list)
        assert h.cmd("risk.query.metrics", QueryMetrics(account_id="ACC3", trading_day=TD))[0].code == codes.NOT_FOUND
        rs = h.cmd("risk.query.rules", QueryRules())[0].open_as(RuleSet)
        assert rs.config_hash == "cfg-hash" and [r["id"] for r in rs.rules] == ["daily_loss"]
        rep, effs = h.cmd("risk.query.accounts", QueryAccounts(account_ids=["ACC3"]))
        assert not [e for e in effs if isinstance(e, Persist)]  # 查询不写库


class TestRestrictionKinds:
    def test_block_open_largest_product_and_product_scope(self) -> None:
        h = Harness(rules_only("concentration", "product_exposure"))
        h.ticks(P)
        h.open_position("ACC3", "rb2701", B, 170, 3100.0)  # gross 5.27M / 10M >= min_gross_pct 50%
        h.open_position("ACC3", "hc2701", B, 2, 3300.0)
        ra = h.eng.account("ACC3")
        assert not [r for r in ra.restrictions if r.kind == RestrictionKind.BLOCK_OPEN]  # time-only confirm: 30 s
        h.advance(30_000)
        h.timer()
        blocks = [r for r in ra.restrictions if r.kind == RestrictionKind.BLOCK_OPEN]
        assert [r.scope_key for r in blocks] == ["acct:ACC3/prod:rb"]  # 有效品种数 < 1.5（ACC3）→ 禁止在最大品种上加仓
        assert h.submit("ACC3", "rb2701", B, O, 1)[0].code == codes.REJECT_PRODUCT_BLOCKED
        assert h.submit("ACC3", "hc2701", B, O, 1)[0].ok
        # 单品种敞口超过上限（rb：60% 权益）→ 该品种禁开
        rep, _ = h.submit("ACC4", "IF2612", B, O, 10, 4500.0)  # 13.5M / 10M = 135% > 30% 上限：事前 what-if 拦截
        assert rep.code == codes.REJECT_WHATIF_BREACH and rep.open().reject_detail["rule_id"] == "product_exposure"
        ext = h.trade("ACC4", "IF2612", B, O, 4500.0, 10, origin=TradeOrigin.EXTERNAL)  # 柜台端成交照样入账
        h.run(h.eng.apply_exec(ext))
        h.evaluate()
        codes_ = {r.scope_key: r.source for r in h.eng.account("ACC4").restrictions}
        assert codes_.get("acct:ACC4/prod:IF") == "product_exposure/block"
        assert h.submit("ACC4", "IF2612", B, O, 1, 4500.0)[0].code == codes.REJECT_PRODUCT_BLOCKED
        assert h.submit("ACC4", "IF2612", S, C, 1, 4500.0)[0].ok

    def test_by_instrument_index_limits_dirty_accounts(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        h.open_position("ACC3", "rb2701", B, 2, 3100.0)
        h.submit("ACC4", "rb2701", B, O, 1, 3090.0)  # 在途委托也纳入索引
        assert h.eng.dirty_accounts() == ["ACC4"]  # an order moves the order-rate part of compliance_util
        h.evaluate()
        assert h.eng.holders("rb2701") == ["ACC3", "ACC4"] and h.eng.holders("hc2701") == []
        h.tick("hc2701", 3301.0, evaluate=False)
        assert h.eng.dirty_accounts() == []
        h.tick("rb2701", 3101.0, evaluate=False)
        assert h.eng.dirty_accounts() == ["ACC3", "ACC4"]

    def test_md_unknown_account_block(self) -> None:
        h = Harness(rules_only("daily_loss"), sod=sod_snapshot(lots={"ACC3": [yd_lot("rb2701", 10, "3100")]}))
        h.ticks(P)
        h.md_status({"rb2701": MdQuality.UNKNOWN})
        ra = h.eng.account("ACC3")
        assert any(r.source == "SYSTEM:MD_UNKNOWN" for r in ra.restrictions) and ra.state == AccountState.RESTRICTED
        assert h.submit("ACC3", "hc2701", B, O, 1)[0].code == codes.REJECT_ACCOUNT_BLOCKED
        h.md_status({"rb2701": MdQuality.OK})
        assert not any(r.source == "SYSTEM:MD_UNKNOWN" for r in ra.restrictions)

    def test_hot_reload_of_thresholds(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        h.open_position("ACC3", "rb2701", B, 300, 3100.0)
        h.ticks({"rb2701": 3050.0})  # −1.5%
        assert h.eng.account("ACC3").state == AccountState.NORMAL
        tighter = rules_only("daily_loss", daily_loss={"levels": [{"name": "warn", "threshold": -0.01, "action": "ALERT"}]})
        h.eng.update_rules(tighter, config_hash="v2")
        h.evaluate()
        assert h.eng.account("ACC3").state == AccountState.WARNING
        assert h.db.tables["rule_states"][-1]["config_hash"] == "v2"
        assert config().risk_rules.rule("daily_loss") is not None
        _ = CT


class TestRestoreExtras:
    def test_compliance_counters_are_rebuilt(self) -> None:
        """M3: orders_today / cancels (per instrument, large) come back from orders + order_events."""
        a = Harness(rules_only("daily_loss"))
        a.ticks(P)
        for i in range(4):
            coid = f"c{i}"
            a.submit("ACC3", "IF2612", B, O, 1, 4500.0, coid=coid)
            a.route_ok(coid)
            a.cmd("order.cancel", CancelRequest(command_id=f"k{i}", client_order_id=coid, account_id="ACC3"))
            a.broker(coid, BrokerOrderStatus.CANCELED)
        a.submit("ACC3", "rb2701", B, O, 1, 3100.0, coid="r1")
        a.route_ok("r1")
        a.broker("r1", BrokerOrderStatus.CANCEL_PENDING)  # cancelled from another terminal: counts too
        a.submit("ACC3", "IF2612", B, O, 10**6, 4500.0, coid="big")  # rejected by the pipeline: still an order
        ra = a.eng.account("ACC3")
        before = (ra.orders_today, ra.cancels_today, dict(ra.cancels_by_inst), dict(ra.large_cancels_by_inst))
        assert before[0] == 6 and before[1] == 5 and before[2] == {"IF2612": 4, "rb2701": 1}
        b = Harness(a.rules, load=False)
        b.clock.set(a.clock.now_ns())
        b.eng.restore(a.db.snapshot(), lifecycle=Lifecycle.RUNNING)
        rb = b.eng.account("ACC3")
        assert (rb.orders_today, rb.cancels_today, dict(rb.cancels_by_inst), dict(rb.large_cancels_by_inst)) == before

    def test_restored_peak_ignores_untrusted_samples(self) -> None:
        a = Harness(rules_only("daily_loss"))
        a.ticks(P)
        a.open_position("ACC3", "rb2701", B, 10, 3100.0)
        a.timer()
        snap = a.db.snapshot()
        good = max(r["perf_equity"] for r in snap["metric_samples"] if r["account_id"] == "ACC3")
        base = [r for r in snap["metric_samples"] if r["account_id"] == "ACC3"][-1]
        snap["metric_samples"].append({**base, "ts_ns": 1, "perf_equity": 99e9, "quality": "STALE"})
        snap["metric_samples"].append({**base, "ts_ns": 2, "perf_equity": 10_000_500.0, "quality": "OK"})
        b = Harness(a.rules, load=False)
        b.clock.set(a.clock.now_ns())
        b.eng.restore(snap, lifecycle=Lifecycle.RUNNING)
        assert b.eng.account("ACC3").intraday_peak == max(good, 10_000_500.0)  # the STALE 99e9 sample is ignored


class TestHotReload:
    def _halted(self) -> Harness:
        h = Harness(rules_only("daily_loss", "leverage"))
        h.ticks({"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0})
        h.open_position("ACC3", "rb2701", B, 80, 3100.0)
        h.open_position("ACC3", "hc2701", B, 75, 3300.0)
        h.open_position("ACC3", "i2701", B, 32, 800.0)
        h.ticks({"rb2701": 2970.0, "hc2701": 3170.0, "i2701": 767.0})
        h.advance(200)
        h.ticks({"rb2701": 2969.0})
        assert h.eng.account("ACC3").halted_latched
        return h

    def test_removed_rule_keeps_a_latched_break_until_reset(self) -> None:
        """M5 / p17: a hot reload never silently releases a latched circuit break."""
        h = self._halted()
        ra = h.eng.account("ACC3")
        effs = h.run(h.eng.update_rules(rules_only("leverage"), config_hash="v2", config_version=2))
        h.evaluate()
        assert ra.state == AccountState.HALTED and ra.halted_latched
        audits = [r for r in h.db.tables["audit_events"] if r["action"] == "CONFIG_COMMIT"]
        assert [a["before"]["level"] for a in audits] == ["halt"] and audits[0]["after"]["kept"] is True
        assert any(isinstance(e, Persist) and e.critical for e in effs)
        # the non-latched levels of the removed rule are gone and their alerts closed
        assert {k[1] for k in ra.machines if k[0] == "daily_loss"} == {"halt"}
        statuses = {r["level"]: r["status"] for r in h.db.tables["alerts"] if r["rule_id"] == "daily_loss"}
        assert statuses == {"warn": "CLEARED", "restrict": "CLEARED", "halt": "ACTIVE"}
        assert h.submit("ACC3", "rb2701", B, O, 1, 2969.0)[0].code == codes.REJECT_ACCOUNT_HALTED
        # a restart with the new config keeps it too (rebuilt from the alerts row)
        b = Harness(h.rules, load=False)
        b.eng.update_rules(rules_only("leverage"), config_hash="v2", config_version=2)
        b.clock.set(h.clock.now_ns())
        b.eng.restore(h.db.snapshot(), lifecycle=Lifecycle.RUNNING)
        b.run(b.eng.post_restore())
        rb = b.eng.account("ACC3")
        assert rb.state == AccountState.HALTED and rb.halted_latched
        # risk.reset releases it and drops the orphan
        rep, _ = b.cmd("risk.reset", ResetRequest(command_id="r", account_id="ACC3", reason="ok", actor="rm", role=RM))
        assert rep.ok and rb.state != AccountState.HALTED and not [k for k in rb.machines if k[0] == "daily_loss"]

    def test_removed_restrict_rule_lifts_its_restriction(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks({"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0})
        h.open_position("ACC3", "rb2701", B, 80, 3100.0)
        h.open_position("ACC3", "hc2701", B, 75, 3300.0)
        h.open_position("ACC3", "i2701", B, 32, 800.0)
        h.ticks({"rb2701": 2990.0, "hc2701": 3190.0, "i2701": 772.0})
        ra = h.eng.account("ACC3")
        assert ra.state == AccountState.RESTRICTED
        effs = h.run(h.eng.update_rules(rules_only("leverage"), config_hash="v2"))
        assert ra.state == AccountState.NORMAL and not ra.restrictions  # refreshed at once
        assert sorted(e.level for e in h.events(RiskEventKind.ALERT_CLEARED, effs)) == ["restrict", "warn"]


class TestCauseAndTimers:
    def test_cause_alert_key_is_the_most_severe_active_level(self) -> None:
        h = Harness(rules_only("daily_loss", "leverage"))
        h.ticks({"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0})
        for _ in range(2):
            h.open_position("ACC3", "rb2701", B, 280, 3100.0)
            h.open_position("ACC3", "hc2701", B, 190, 3300.0)
        assert h.eng.snapshot("ACC3").metrics.leverage < 3.0
        # one market state: daily_loss restrict (about −2.5 %) and leverage warn (the equity drop lifts it over 3x)
        effs = h.ticks({"rb2701": 3073.0, "hc2701": 3273.0})
        assert h.eng.snapshot("ACC3").metrics.leverage >= 3.0
        raised = {(e.rule_id, e.level) for e in h.events(RiskEventKind.ALERT_RAISED, effs)}
        assert ("daily_loss", "restrict") in raised and ("leverage", "warn") in raised
        msg = h.state_changes("ACC3", effs)[-1]
        assert msg.to_state == AccountState.RESTRICTED
        assert msg.cause_alert_key == f"{TD}|daily_loss|restrict|acct:ACC3|1"

    def test_next_timer_is_never_a_past_time_without_progress(self) -> None:
        """L6 / p21: quality-gated timers and a provisional trigger waiting for md.settle_inputs are left out."""
        h = Harness(rules_only("daily_loss"))
        h.ticks({"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0})
        h.open_position("ACC3", "rb2701", B, 80, 3100.0)
        h.open_position("ACC3", "hc2701", B, 75, 3300.0)
        h.open_position("ACC3", "i2701", B, 32, 800.0)
        h.ticks({"rb2701": 2970.0, "hc2701": 3170.0, "i2701": 767.0})  # halt PENDING (confirm 1 s)
        h.md_status({"rb2701": MdQuality.STALE})
        for _ in range(3):
            h.advance(2000)
            h.timer()
            nxt = h.eng.next_timer_ns()
            assert nxt is not None and nxt > h.clock.now_ns()
        assert h.eng.account("ACC3").machines[("daily_loss", "halt", "acct:ACC3")].state == RuleSmState.PENDING
        h2 = Harness(rules_only("daily_loss"))
        h2.at("2026-09-29 20:56")
        assert [e for e in h2.timer() if isinstance(e, Request)]  # waiting for md.settle_inputs
        for _ in range(2):
            h2.advance(5000)
            h2.timer()
            nxt = h2.eng.next_timer_ns()
            assert nxt is not None and nxt > h2.clock.now_ns()

    def test_halt_cancels_resting_opens_but_kill_switch_does_not(self) -> None:
        """Kill switch / BLOCK_OPEN reject new opens only (02 §11.2); HALTED also cancels resting opens (01 §8.14)."""
        from futrisk.messages.risk import KillSwitchRequest

        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        h.submit("ACC3", "IF2612", B, O, 1, 4500.0, coid="c1")
        h.route_ok("c1")
        _, effs = h.cmd("firm.kill_switch", KillSwitchRequest(command_id="k", on=True, reason="x", actor="rm", role=RM))
        assert not [e for e in effs if isinstance(e, CancelRoute)]
        _, effs = h.cmd("risk.halt", HaltRequest(command_id="h", account_id="ACC3", reason="x", actor="rm", role=RM))
        assert [e.client_order_id for e in effs if isinstance(e, CancelRoute)] == ["c1"]


class TestShellContract:
    """Items raised by the risk-engine shell author."""

    def test_no_evaluation_and_no_row_before_restore_or_while_stopping(self) -> None:
        h = Harness(rules_only("daily_loss"), load=False)
        rep, effs = h.submit("ACC3", "rb2701", B, O, 1, 3100.0)
        assert rep.code == codes.REJECT_SYSTEM_NOT_READY and not [e for e in effs if isinstance(e, Persist)]
        h2 = Harness(rules_only("daily_loss"))
        h2.ticks(P)
        h2.run(h2.eng.set_lifecycle(Lifecycle.STOPPING))
        h2.eng.account("ACC3").dirty = True
        rep, effs = h2.submit("ACC3", "rb2701", B, O, 1, 3100.0)
        assert rep.code == codes.REJECT_SYSTEM_NOT_READY and not [e for e in effs if isinstance(e, Persist)]
        assert h2.eng.account("ACC3").dirty  # not evaluated

    def test_md_status_does_not_clear_the_heartbeat_down_state(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        h.eng.set_md_down(True)
        h.md_status({"rb2701": MdQuality.OK})
        assert h.eng.quality_of("rb2701") == MdQuality.STALE
        h.eng.set_md_down(False)
        assert h.eng.quality_of("rb2701") == MdQuality.OK

    def test_loop_timers_are_reported_before_the_first_on_timer(self) -> None:
        h = Harness(rules_only("daily_loss"))
        assert h.eng.next_timer_ns() == h.clock.now_ns()
        h.timer()
        nxt = h.eng.next_timer_ns()
        assert nxt is not None and nxt > h.clock.now_ns()
        assert Harness(rules_only("daily_loss"), load=False).eng.next_timer_ns() is None

    def test_fill_gap_restored_from_account_states_can_be_cleared(self) -> None:
        a = Harness(rules_only("daily_loss"))
        a.run(a.eng.set_system_restriction("ACC3", "FILL_GAP", True))
        b = Harness(a.rules, load=False)
        b.clock.set(a.clock.now_ns())
        b.eng.restore(a.db.snapshot(), lifecycle=Lifecycle.RUNNING)
        b.run(b.eng.post_restore())
        assert any(r.source == "SYSTEM:FILL_GAP" for r in b.eng.account("ACC3").restrictions)
        b.run(b.eng.clear_system_restriction(None, "FILL_GAP"))  # after the shell's catch-up completed
        assert not any(r.source == "SYSTEM:FILL_GAP" for r in b.eng.account("ACC3").restrictions)

    def test_open_recon_break_from_the_shell(self) -> None:
        h = Harness(rules_only("daily_loss"))
        effs = h.run(h.eng.open_recon_break("ACC3", ReconBreakKind.POSITION_MISMATCH, {"reason": "REPLAY_EXPIRED"}))
        assert any(isinstance(e, Persist) and e.critical for e in effs)
        brk = [b for b in h.eng.breaks.values() if b.detail.get("reason") == "REPLAY_EXPIRED"]
        assert len(brk) == 1 and brk[0].blocking
        assert any(r.source == "SYSTEM:RECON_BREAK" for r in h.eng.account("ACC3").restrictions)
        assert h.eng.open_recon_break("ACC3", ReconBreakKind.POSITION_MISMATCH, {"reason": "REPLAY_EXPIRED"}) == []

    def test_resting_orders_are_indexed_per_instrument(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        h.submit("ACC3", "rb2701", B, O, 1, 3090.0, coid="r1")
        h.submit("ACC3", "hc2701", B, O, 1, 3290.0, coid="h1")
        h.submit("ACC3", "rb2701", B, O, 1, 3091.0, coid="r2")
        book = h.eng.orders
        assert [o.client_order_id for o in book.resting_on("ACC3", "rb2701")] == ["r1", "r2"]
        h.route_err("r1", codes.ROUTE_KILL_SWITCH)
        assert [o.client_order_id for o in book.resting_on("ACC3", "rb2701")] == ["r2"]
        assert book.resting_on("ACC4", "rb2701") == []
        assert h.submit("ACC3", "rb2701", S, O, 1, 3091.0)[0].code == codes.REJECT_SELF_TRADE


class TestPhase3Shell:
    def test_md_status_is_the_quality_authority(self) -> None:
        """H1 / P9: a snapshot tick (live=False) never upgrades quality; a live tick never above md.status."""
        from futrisk.core.effects import CommandCtx
        from futrisk.messages.orders import OrderRequest

        h = Harness(rules_only("daily_loss"))
        h.ticks({"rb2701": 3100.0})
        h.md_status({"IF2612": MdQuality.STALE})
        h.eng.apply_mark(h.make_tick("IF2612", 4500.0), live=False)  # the LV cache's old tick on (re)snapshot
        assert h.eng.quality_of("IF2612") == MdQuality.STALE
        h.eng.apply_mark(h.make_tick("IF2612", 4501.0))  # even a live tick: md.status says STALE
        assert h.eng.quality_of("IF2612") == MdQuality.STALE
        req = OrderRequest(client_order_id="q1", account_id="ACC4", instrument_id="IF2612", direction=B, offset=O,
                           price=4501.0, qty=1, sent_ns=0)
        rep = h.run(h.eng.handle(req, CommandCtx("order.submit", token="t")))[-1].reply  # type: ignore[union-attr]
        assert rep.code == codes.REJECT_MD_UNAVAILABLE
        h.md_status({"IF2612": MdQuality.OK})
        assert h.eng.quality_of("IF2612") == MdQuality.OK
        h.eng.apply_mark(h.make_tick("hc2701", 3300.0), live=False)  # never ticked live, no status: stays UNKNOWN
        assert h.eng.quality_of("hc2701") == MdQuality.UNKNOWN
        h.eng.apply_mark(h.make_tick("hc2701", 3301.0))
        assert h.eng.quality_of("hc2701") == MdQuality.OK
        assert h.eng.mark_of("hc2701") == 3301.0

    def test_snapshot_tick_is_not_a_new_live_market_state(self) -> None:
        h = Harness(rules_only("daily_loss"))
        h.ticks({"rb2701": 3100.0})
        h.open_position("ACC3", "rb2701", B, 10, 3100.0)
        ra = h.eng.account("ACC3")
        seq = ra.market_seq
        h.eng.apply_mark(h.make_tick("rb2701", 3090.0), live=False)
        assert ra.market_seq == seq and ra.dirty and ra.ledger.mark("rb2701") == 3090.0
        h.eng.apply_mark(h.make_tick("rb2701", 3091.0))
        assert ra.market_seq == seq + 1

    def test_load_sod_does_not_start_evaluation(self) -> None:
        """M1 / P10: the bare SOD is not evaluated before the fact catch-up (lifecycle stays LOADING)."""
        lot = yd_lot("rb2701", 300, "3100")
        h = Harness(rules_only("daily_loss"), load=False)
        effs = h.run(h.eng.load_sod(sod_snapshot(lots={"ACC3": [lot]})))
        assert h.eng.lifecycle.value == "LOADING" and not h.state_changes(effects=effs)
        h.tick("rb2701", 2995.0)  # -3.15 % on the SOD book
        h.advance(1500)
        assert h.run(h.eng.evaluate_dirty()) == [] and h.run(h.eng.on_timer()) == []
        assert not h.events(RiskEventKind.ALERT_RAISED)
        # catch-up: the account sold everything this morning
        t = h.trade("ACC3", "rb2701", S, CY, 3100.0, 300, origin=TradeOrigin.EXTERNAL)
        h.run(h.eng.apply_exec(t))
        h.run(h.eng.set_lifecycle(Lifecycle.WARMUP))
        h.evaluate()
        h.timer()
        assert not h.events(RiskEventKind.ALERT_RAISED)
        assert h.eng.account("ACC3").state == AccountState.RESTRICTED  # SYSTEM:STARTING only
        assert [r.source for r in h.eng.account("ACC3").restrictions] == ["SYSTEM:STARTING"]

    def test_order_row_is_inserted_once_then_updated_with_changed_columns(self) -> None:
        from futrisk.core.effects import RowOp

        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        _, effs = h.submit("ACC3", "IF2612", B, O, 2, 4500.0, coid="c1")
        rows = [r for e in effs if isinstance(e, Persist) for r in e.rows if r.table == "orders"]
        assert [r.op for r in rows] == [RowOp.INSERT] and rows[0].values["status"] == "ROUTING"
        h.advance(1)
        effs = h.route_ok("c1")
        rows = [r for e in effs if isinstance(e, Persist) for r in e.rows if r.table == "orders"]
        assert [r.op for r in rows] == [RowOp.UPDATE] and rows[0].key == ("client_order_id",)
        assert set(rows[0].values) == {"client_order_id", "status", "updated_ns", "broker_order_ref"}
        effs = h.fill("c1", 4500.0, 1)
        vals = [r for e in effs if isinstance(e, Persist) for r in e.rows if r.table == "orders"][0].values
        assert {"filled_qty", "avg_fill_price", "frozen_margin", "status"} <= set(vals) and "qty" not in vals
        db_row = [r for r in h.db.tables["orders"] if r["client_order_id"] == "c1"][0]
        assert (db_row["status"], db_row["filled_qty"], db_row["qty"]) == ("PARTIALLY_FILLED", 1, 2)

    def test_client_order_id_of_an_earlier_day_is_never_reused(self) -> None:
        """P11: after the roll the old order is pruned from memory, its id stays taken (no row overwrite)."""
        from futrisk.messages.envelope import ok_reply as ok
        from futrisk.messages.orders import BrokerOrderUpdate  # noqa: F401

        h = Harness(rules_only("daily_loss"))
        h.ticks(P)
        rep, _ = h.submit("ACC5", "rb2701", B, O, 1, 3100.0, coid="reuse-1")
        assert rep.ok
        h.route_ok("reuse-1")
        h.fill("reuse-1", 3100.0, 1)
        from futrisk.core.effects import CommandCtx
        from futrisk.messages.eod import EodFreezeRequest, EodRolloverRequest

        h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        effs = h.run(h.eng.handle(EodRolloverRequest(trading_day=TD, next_trading_day="20260930", sod_version=1),
                                  CommandCtx("eod.rollover", token="r")))
        req = [e for e in effs if isinstance(e, Request)][0]
        h.run(h.eng.on_request_result(req.token, ok(sod_snapshot(trading_day="20260930", version=1))))
        assert h.eng.orders.get("reuse-1") is None  # pruned
        h.at("2026-09-29 21:05")
        h.tick("rb2701", 3100.0, trading_day="20260930")
        rep, effs = h.submit("ACC5", "rb2701", B, O, 1, 3100.0, coid="reuse-1")
        assert rep.code == codes.REJECT_DUPLICATE_ORDER_ID and not [e for e in effs if isinstance(e, Persist)]
        assert [r["status"] for r in h.db.tables["orders"] if r["client_order_id"] == "reuse-1"] == ["FILLED"]


class TestTimersOff:
    """Periodic loop timers set to 0 are OFF: next_timer_ns() is then only rule / order / provisional timers."""

    @staticmethod
    def _harness() -> Harness:
        from futrisk.common.config.risk_rules import RiskRulesConfig, SystemRiskConfig

        base = rules_only("daily_loss")
        rules = RiskRulesConfig(rules=base.rules, pretrade=base.pretrade,
                                system=SystemRiskConfig(snapshot_hz=0, metric_sample_s=0, recon_interval_s=0))
        h = Harness(rules)
        h.eng.auto_provisional = False  # like the backtest: no provisional trigger
        return h

    def test_nothing_pending_means_no_timer(self) -> None:
        h = self._harness()
        h.ticks({"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0})
        assert h.eng.next_timer_ns() is None and h.eng.next_rule_due_ns() is None
        effs = h.timer()
        assert not h.publishes(effs) and "metric_samples" not in h.db.tables  # no snapshots, no samples
        assert not [e for e in effs if isinstance(e, Request)]  # no position recon
        assert h.eng.next_timer_ns() is None

    def test_confirm_and_hold_timers_drive_time(self) -> None:
        h = self._harness()
        h.ticks({"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0})
        h.open_position("ACC3", "rb2701", B, 80, 3100.0)
        h.open_position("ACC3", "hc2701", B, 75, 3300.0)
        h.open_position("ACC3", "i2701", B, 32, 800.0)
        assert h.eng.next_timer_ns() is None
        h.ticks({"rb2701": 2970.0, "hc2701": 3170.0, "i2701": 767.0})  # halt PENDING, confirm 1 s
        halt = h.eng.account("ACC3").machines[("daily_loss", "halt", "acct:ACC3")]
        assert halt.state == RuleSmState.PENDING and halt.st.pending_since_ns is not None
        due = halt.st.pending_since_ns + 1000 * 1_000_000
        assert h.eng.next_rule_due_ns() == due and h.eng.next_timer_ns() == due
        h.clock.set(due)
        h.timer()
        assert halt.state == RuleSmState.ACTIVE and h.eng.next_timer_ns() is None
        # a hold timer: warn / restrict clear only after hold_ms (3 s) beyond the clear line
        h.cmd("risk.reset", ResetRequest(command_id="r", account_id="ACC3", reason="ok", actor="rm", role=RM))
        h.ticks({"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0})
        restrict = h.eng.account("ACC3").machines[("daily_loss", "restrict", "acct:ACC3")]
        assert restrict.st.clear_since_ns is not None
        hold_due = restrict.st.clear_since_ns + 3000 * 1_000_000
        assert h.eng.next_timer_ns() == min(hold_due, h.eng.next_rule_due_ns() or hold_due) == hold_due
        h.clock.set(hold_due)
        h.timer()
        assert restrict.state == RuleSmState.OK and h.eng.account("ACC3").state == AccountState.NORMAL
        assert h.eng.next_timer_ns() is None

    def test_order_timers_still_count(self) -> None:
        h = self._harness()
        h.ticks({"IF2612": 4500.0})
        rep, _ = h.submit("ACC3", "IF2612", B, O, 1, 4500.0, coid="t1")
        o = h.eng.orders.get("t1")
        assert rep.ok and o is not None and h.eng.next_timer_ns() == o.expire_ns


class TestWatermarkReset:
    """md-gateway's FLAG_WM_RESET: the first LIVE tick after a watermark roll-back may go backwards."""

    def test_flagged_live_tick_with_a_lower_key_is_applied(self) -> None:
        import msgspec

        from futrisk.messages.md import FLAG_WM_RESET

        h = Harness()
        h.tick("rb2701", 3120.0)
        h.tick("rb2701", 3122.0)
        assert h.eng.mark_of("rb2701") == 3122.0
        newest = h.make_tick("rb2701", 3119.0)
        older = msgspec.structs.replace(newest, volume=newest.volume - 5)
        h.eng.apply_mark(older)  # plain tick with a lower key: ignored
        assert h.eng.mark_of("rb2701") == 3122.0
        h.eng.apply_mark(older, live=False)  # a snapshot tick never resets, even when flagged
        h.eng.apply_mark(msgspec.structs.replace(older, flags=FLAG_WM_RESET), live=False)
        assert h.eng.mark_of("rb2701") == 3122.0
        h.eng.apply_mark(msgspec.structs.replace(older, flags=FLAG_WM_RESET))  # live + flagged: applied
        assert h.eng.mark_of("rb2701") == 3119.0
        h.tick("rb2701", 3121.0)  # ordering continues from the reset tick
        assert h.eng.mark_of("rb2701") == 3121.0
