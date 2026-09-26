"""EOD support: freeze digests, cash attribution, rollover / PROVISIONAL / REBASE (01 §6.6, §8.15; 02 §6.3, §8.4)."""

from __future__ import annotations

import msgspec
import pytest
from enginetest_support import (
    ADMIN,
    CY,
    NEXT_TD,
    RM,
    TD,
    B,
    Harness,
    O,
    S,
    rules_only,
    rules_with,
    sod_snapshot,
)

from futrisk.common.config.risk_rules import RuleDef
from futrisk.common.decimals import D
from futrisk.core.effects import CommandCtx, Effect, Request, ResolveRoute
from futrisk.core.eod import build_provisional_sod, cash_from_row, digest_only_ack, trade_from_fill_row
from futrisk.core.risk_account import Lifecycle
from futrisk.messages import codes
from futrisk.messages.enums import (
    AccountState,
    BrokerOrderStatus,
    CashType,
    DayRollMode,
    Exchange,
    LiquidationStatus,
    Offset,
    OrderStatus,
    PosDirection,
    ReconBreakKind,
    RiskEventKind,
    RuleSmState,
    SettleMethod,
    Severity,
    TradeOrigin,
    TradingMode,
)
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.eod import (
    DayRoll,
    EodFreezeAck,
    EodFreezeRequest,
    EodRolloverRequest,
    ForceUnfreezeRequest,
    SodLot,
    SodRequest,
    SodSnapshot,
)
from futrisk.messages.md import SettleInput, SettleInputsBatch
from futrisk.messages.orders import OrderUpdate, RouteAck, Trade, TradeBatch, compute_account_digest
from futrisk.messages.risk import CashMovement, CashMoveRequest, HaltRequest

P = {"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0}


def traded(rules: object = None) -> Harness:
    h = Harness(rules or rules_only("daily_loss"))  # type: ignore[arg-type]
    h.ticks(P)
    h.open_position("ACC3", "rb2701", B, 10, 3100.0)
    h.open_position("ACC3", "i2701", B, 5, 800.0)
    h.open_position("ACC4", "hc2701", B, 3, 3300.0)
    return h


def cash(h: Harness, mid: str, amount: str = "100000.00", kind: CashType = CashType.DEPOSIT, role=ADMIN, account: str = "ACC3"):  # type: ignore[no-untyped-def]
    return h.cmd("cash.move", CashMoveRequest(movement_id=mid, account_id=account, type=kind, amount=amount,
                                               reason="fund", actor="admin", role=role, requested_ns=1))


def rollover(h: Harness, version: int, sod_capital: dict[str, str] | None = None,
             lots: dict[str, list[SodLot]] | None = None) -> tuple[DayRoll, list[object]]:
    rep_effs = h.run(h.eng.handle(EodRolloverRequest(trading_day=TD, next_trading_day=NEXT_TD, sod_version=version),
                                  CommandCtx("eod.rollover", token="roll")))
    req = [e for e in rep_effs if isinstance(e, Request)]
    assert req and req[0].command == "settle.sod_snapshot" and req[0].target == "clearing-worker"
    sreq = req[0].payload
    assert isinstance(sreq, SodRequest) and sreq.version == version and sreq.trading_day == NEXT_TD
    assert sreq.account_ids == ["ACC3", "ACC4", "ACC5"]
    sod = sod_snapshot(sod_capital, trading_day=NEXT_TD, version=version, lots=lots)
    effs = h.run(h.eng.on_request_result(req[0].token, ok_reply(sod)))
    replies = [e.reply for e in effs if hasattr(e, "reply")]
    assert replies and replies[-1].ok
    return replies[-1].open_as(DayRoll), effs


class TestFreeze:
    def test_freeze_digests_and_frozen_mode(self) -> None:
        h = traded()
        cash(h, "m1")
        rep, effs = h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        ack = rep.open_as(EodFreezeAck)
        trades = {a: list(h.eng.account(a).trades) for a in ("ACC3", "ACC4", "ACC5")}
        assert ack.accounts == [compute_account_digest(a, trades[a]) for a in ("ACC3", "ACC4", "ACC5")]
        assert ack.accounts[0].count == 2 and ack.accounts[0].qty_sum == 15
        assert [(c.account_id, c.count, c.amount_sum) for c in ack.cash][0] == ("ACC3", 1, "100000.00")
        assert ack.open_orders == 0 and h.eng.trading_mode == TradingMode.EOD_FROZEN
        row = h.db.tables["eod_states"][0]
        assert (row["status"], row["freeze_version"]) == ("FROZEN", 1) and row["digest"]["accounts"]
        assert "eod.freeze" in h.db.critical_labels
        assert h.submit("ACC3", "rb2701", B, O, 1)[0].code == codes.REJECT_EOD_IN_PROGRESS
        rep2, _ = h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=2))  # 幂等：状态不变，摘要重算
        assert rep2.open_as(EodFreezeAck).accounts == ack.accounts
        rep3, _ = h.cmd("eod.freeze", EodFreezeRequest(trading_day="20260925", version=1))
        assert rep3.code == codes.PRECONDITION_FAILED


class TestCash:
    def test_validation_idempotency_and_attribution(self) -> None:
        h = traded()
        eq0 = h.eng.snapshot("ACC3").funds
        rep, effs = cash(h, "m1")
        cm = rep.open_as(CashMovement)
        assert rep.ok and cm.trading_day == TD and cm.amount == "100000.00"
        f = h.eng.snapshot("ACC3")
        assert f.funds.static_equity == pytest.approx(eq0.static_equity + 100_000)
        assert f.metrics.daily_pnl == pytest.approx(eq0.equity - eq0.static_equity)  # 出入金不算盈亏
        assert [m.topic for p in effs if hasattr(p, "outbox") for m in p.outbox] == ["risk.cash.ACC3"]  # type: ignore[attr-defined]
        assert h.db.tables["cash_movements"][0]["movement_id"] == "m1"
        rep_dup, _ = cash(h, "m1")
        assert rep_dup.duplicate
        rep_conf, _ = cash(h, "m1", amount="5.00")
        assert rep_conf.code == codes.CONFLICT
        assert cash(h, "m2", role=RM)[0].code == codes.FORBIDDEN
        assert cash(h, "m3", amount="1.001")[0].code == codes.BAD_REQUEST
        assert cash(h, "m4", amount="-5")[0].code == codes.BAD_REQUEST
        assert cash(h, "m5", amount="99999999.00", kind=CashType.WITHDRAW)[0].code == codes.PRECONDITION_FAILED
        # 冻结之后、换日之前：记入下一交易日
        h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        rep, _ = cash(h, "m6", amount="50000.00")
        assert rep.open_as(CashMovement).trading_day == NEXT_TD
        assert h.eng.snapshot("ACC3").funds.static_equity == pytest.approx(eq0.static_equity + 100_000)
        # 换日之后：记入新交易日的账本
        rollover(h, 1)
        f = h.eng.snapshot("ACC3").funds
        assert f.net_deposit == pytest.approx(50_000) and f.static_equity == pytest.approx(10_000_000 + 50_000)


class TestRollover:
    def test_roll_resets_state_machines_latches_alerts_plans(self) -> None:
        derisk = RuleDef.model_validate({"id": "dl2", "metric": "account.daily_pnl_pct_init", "op": "<=", "levels": [
            {"name": "derisk", "threshold": -0.001, "action": "LIQUIDATE", "params": {"step": 0.5}}]})
        h = traded(rules_with([*rules_only("daily_loss").rules, derisk]))
        h.cmd("risk.halt", HaltRequest(command_id="h", account_id="ACC3", reason="x", actor="rm", role=RM))
        h.ticks({"rb2701": 3050.0, "i2701": 780.0})  # 小亏损：dl2 生成减仓计划
        assert any(p.status == LiquidationStatus.PENDING_CONFIRM for p in h.eng.plans.values())
        ra = h.eng.account("ACC3")
        assert ra.state == AccountState.HALTED and ra.halted_latched
        h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        dr, effs = rollover(h, 1)
        assert (dr.mode, dr.from_trading_day, dr.to_trading_day, dr.sod_version) == (DayRollMode.ROLL, TD, NEXT_TD, 1)
        assert dr.latches_cleared >= 1
        assert h.eng.trading_day == NEXT_TD and h.eng.trading_mode == TradingMode.OPEN
        ra = h.eng.account("ACC3")
        assert ra.state == AccountState.NORMAL and not ra.halted_latched and ra.machines == {}
        assert all(r["status"] != "ACTIVE" for r in h.db.tables["alerts"])
        assert {r["status"] for r in h.db.tables["alerts"]} >= {"ROLLED"}
        assert all(p.status != LiquidationStatus.PENDING_CONFIRM for p in h.eng.plans.values())
        assert any(isinstance(m.payload, DayRoll) for p in effs if hasattr(p, "outbox") for m in p.outbox)  # type: ignore[attr-defined]
        eod = {r["trading_day"]: r for r in h.db.tables["eod_states"]}
        assert eod[TD]["status"] == "ROLLED" and eod[TD]["sod_version"] == 1 and eod[NEXT_TD]["status"] == "OPEN"
        assert {r["trading_day"] for r in h.db.tables["sod_accounts"]} == {TD, NEXT_TD}
        # 重复推送同一版本：返回首次结果，不再换日
        rep_effs = h.run(h.eng.handle(EodRolloverRequest(trading_day=TD, next_trading_day=NEXT_TD, sod_version=1),
                                      CommandCtx("eod.rollover", token="r2")))
        dup = [e.reply for e in rep_effs if hasattr(e, "reply")][0]
        assert dup.duplicate and dup.open_as(DayRoll) == dr
        # 同一新交易日更大的版本：重新定基
        dr2, _ = rollover(h, 2)
        assert dr2.mode == DayRollMode.REBASE and h.eng.sod_version == 2

    def test_rollover_failure_is_reported_and_state_kept(self) -> None:
        h = traded()
        h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        effs = h.run(h.eng.handle(EodRolloverRequest(trading_day=TD, next_trading_day=NEXT_TD, sod_version=1),
                                  CommandCtx("eod.rollover", token="r")))
        tok = [e for e in effs if isinstance(e, Request)][0].token
        out = h.run(h.eng.on_request_result(tok, err_reply(codes.TIMEOUT)))
        assert [e.reply.code for e in out if hasattr(e, "reply")] == [codes.TIMEOUT]
        assert h.eng.trading_day == TD and h.eng.trading_mode == TradingMode.EOD_FROZEN

    def test_roll_day_direct_for_backtests(self) -> None:
        h = traded()
        effs = h.run(h.eng.roll_day(sod_snapshot(trading_day=NEXT_TD, version=0)))
        assert h.eng.trading_day == NEXT_TD
        assert any(isinstance(m.payload, DayRoll) for p in effs if hasattr(p, "outbox") for m in p.outbox)  # type: ignore[attr-defined]
        assert h.eng.account("ACC3").ledger.book.positions("ACC3") == []


class TestProvisional:
    def test_provisional_then_force_unfreeze_then_rebase(self) -> None:
        h = traded()
        h.ticks({"rb2701": 3080.0, "i2701": 795.0})
        h.at("2026-09-29 20:56")  # 20260930 的第一个交易时段（夜盘集合竞价 20:55）已经开始
        effs = h.timer()
        req = [e for e in effs if isinstance(e, Request)]
        assert req and req[0].command == "md.settle_inputs" and req[0].target == "md-gateway"
        assert h.eng.trading_mode == TradingMode.EOD_FROZEN  # 先自行冻结 T
        assert h.submit("ACC3", "rb2701", B, O, 1)[0].code == codes.REJECT_EOD_IN_PROGRESS
        # 取不到 VWAP：按收盘最新价构建临时快照
        effs = h.run(h.eng.on_request_result(req[0].token, err_reply(codes.TIMEOUT)))
        assert h.eng.trading_day == NEXT_TD and h.eng.trading_mode == TradingMode.PROVISIONAL
        assert h.eng.sod_version == -1 and h.eng.provisional
        dr = [m.payload for p in effs if hasattr(p, "outbox") for m in p.outbox if isinstance(m.payload, DayRoll)]  # type: ignore[attr-defined]
        assert dr and dr[0].mode == DayRollMode.PROVISIONAL
        sod_lots = [r for r in h.db.tables["sod_lots"] if r["trading_day"] == NEXT_TD and r["instrument_id"] == "rb2701"]
        assert sod_lots and sod_lots[0]["basis_price"] == "3080"  # 今仓转昨仓，按临时结算价定基
        acc = [r for r in h.db.tables["sod_accounts"] if r["trading_day"] == NEXT_TD and r["account_id"] == "ACC3"][0]
        assert acc["provisional"] is True and acc["sod_version"] == -1
        assert float(acc["pre_balance"]) == pytest.approx(10_000_000 + (3080 - 3100) * 10 * 10 + (795 - 800) * 100 * 5 - 31.0 - 40.0, abs=0.01)
        # 夜盘开盘：所有账户禁开，平仓照常
        h.at("2026-09-29 21:05")
        for inst, px in {"rb2701": 3080.0, "i2701": 795.0}.items():
            h.tick(inst, px, trading_day=NEXT_TD)
        rep, _ = h.submit("ACC3", "rb2701", B, O, 1)
        assert rep.code == codes.REJECT_ACCOUNT_BLOCKED and rep.open().reject_detail["source"] == "SYSTEM:EOD_NOT_SETTLED"
        rep, _ = h.submit("ACC3", "rb2701", S, CY, 4)
        assert rep.ok, rep.code
        close_id = rep.open().client_order_id
        h.route_ok(close_id)
        t = h.trade("ACC3", "rb2701", S, CY, 3081.0, 4, coid=close_id, trading_day=NEXT_TD)
        h.run(h.eng.apply_exec(t))
        # 临时快照期间收到的 eod.freeze{T}：只返回摘要，不改变交易模式
        rep, _ = h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        assert rep.ok and rep.open_as(EodFreezeAck).accounts[0].count == 2
        assert h.eng.trading_mode == TradingMode.PROVISIONAL
        # 临时快照期间的出入金记入新交易日
        assert cash(h, "m-prov", amount="1000.00")[0].open_as(CashMovement).trading_day == NEXT_TD
        # 审计放行
        assert h.cmd("eod.force_unfreeze", ForceUnfreezeRequest(command_id="fu", trading_day=TD, reason="clearing down",
                                                                actor="rm", role=RM))[0].ok
        assert h.events(RiskEventKind.EOD_FORCE_UNFREEZE)
        assert not any(r.source == "SYSTEM:EOD_NOT_SETTLED" for r in h.eng.account("ACC3").restrictions)
        assert h.submit("ACC3", "i2701", B, O, 1, 795.0)[0].ok
        # 正式快照到达：重新定基（重放当日成交），交易模式恢复
        official = {"ACC3": [
            SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td=TD, open_ts_ns=1,
                   open_price="3100", qty=10, basis_price="3079"),
            SodLot(instrument_id="i2701", direction=PosDirection.LONG, lot_no=1, open_td=TD, open_ts_ns=2,
                   open_price="800", qty=5, basis_price="796"),
        ]}
        dr2, _ = rollover(h, 3, lots=official)
        assert dr2.mode == DayRollMode.REBASE and h.eng.trading_mode == TradingMode.OPEN and h.eng.sod_version == 3
        pos = [p for p in h.eng.snapshot("ACC3").positions if p.instrument_id == "rb2701"][0]
        assert (pos.yd_qty, pos.basis_avg_price) == (6, pytest.approx(3079.0))  # 正式快照 + 重放当日平仓 4 手
        assert not any(b.blocking for b in h.eng.breaks.values())
        eod = {r["trading_day"]: r for r in h.db.tables["eod_states"]}
        assert eod[TD]["status"] == "ROLLED" and eod[TD]["sod_version"] == 3

    def test_clearing_down_for_several_days_rolls_provisionally_again(self) -> None:
        h = traded()
        h.at("2026-09-29 20:56")
        req = [e for e in h.timer() if isinstance(e, Request)][0]
        h.run(h.eng.on_request_result(req.token, err_reply(codes.UNAVAILABLE)))
        assert h.eng.trading_day == NEXT_TD and h.eng.provisional
        # 临时快照所在交易日自己的 EOD 仍可冻结
        rep, _ = h.cmd("eod.freeze", EodFreezeRequest(trading_day=NEXT_TD, version=1))
        assert rep.ok and h.eng.trading_mode == TradingMode.EOD_FROZEN
        # 长假后下一个交易日（20261008，无夜盘）08:55 集合竞价开始：再次进入临时快照
        h.at("2026-10-08 08:56")
        req = [e for e in h.timer() if isinstance(e, Request)][0]
        h.run(h.eng.on_request_result(req.token, err_reply(codes.UNAVAILABLE)))
        assert h.eng.trading_day == "20261008" and h.eng.trading_mode == TradingMode.PROVISIONAL
        pos = [p for p in h.eng.snapshot("ACC3").positions if p.instrument_id == "rb2701"][0]
        assert (pos.td_qty, pos.yd_qty) == (0, 10)

    def test_provisional_uses_vwap_with_exchange_rounding(self) -> None:
        h = traded()
        h.at("2026-09-29 20:56")
        req = [e for e in h.timer() if isinstance(e, Request)][0]
        si = SettleInput(trading_day=TD, instrument_id="rb2701", exchange=Exchange.SHFE, method=SettleMethod.VWAP_FULL_DAY,
                         multiplier=10, tick_size="1", day_volume=100, day_turnover="3050700", vwap_raw="3050.7",
                         pre_settle="3100", computed_ns=1)
        h.run(h.eng.on_request_result(req.token, ok_reply(SettleInputsBatch(items=[si]))))
        lots = [r for r in h.db.tables["sod_lots"] if r["trading_day"] == NEXT_TD and r["instrument_id"] == "rb2701"]
        assert lots[0]["basis_price"] == "3050"  # SHFE 向下取整到 tick
        i_lots = [r for r in h.db.tables["sod_lots"] if r["trading_day"] == NEXT_TD and r["instrument_id"] == "i2701"]
        assert i_lots[0]["basis_price"] == "800"  # 无 VWAP：收盘最新价


class TestRebaseKeepsRuleState:
    def test_rebase_same_day_keeps_episodes(self) -> None:
        h = traded()
        h.ticks({"rb2701": 2000.0, "i2701": 300.0})  # ACC3 大幅亏损：warn/restrict/halt 全部触发
        h.advance(1500)
        h.timer()
        ra = h.eng.account("ACC3")
        before = {k: m.st.persisted() for k, m in ra.machines.items()}
        assert ra.machines[("daily_loss", "halt", "acct:ACC3")].state == RuleSmState.ACTIVE
        raised = len(h.events(RiskEventKind.ALERT_RAISED))
        lots = {"ACC3": []}  # type: ignore[var-annotated]
        h.run(h.eng.roll_day(sod_snapshot(version=5, lots=lots), DayRollMode.REBASE))
        ra = h.eng.account("ACC3")
        assert {k: m.st.persisted() for k, m in ra.machines.items()} == before
        assert len(h.events(RiskEventKind.ALERT_RAISED)) == raised
        assert ra.state == AccountState.HALTED and ra.halted_latched
        assert len(ra.trades) == 2  # 当日成交按原顺序重放


# ---------------------------------------------------------------------------- phase-2 EOD items
D2 = "20261008"  # next trading day after NEXT_TD (National Day holiday)


def restart(a: Harness, **kw: object) -> Harness:
    b = Harness(a.rules, load=False)
    b.clock.set(a.clock.now_ns())
    b.eng.restore(a.db.snapshot(), lifecycle=Lifecycle.RUNNING, **kw)  # type: ignore[arg-type]
    b.db.tables.update({k: [dict(r) for r in v] for k, v in a.db.snapshot().items()})
    return b


def provisional_now(h: Harness) -> list[Effect]:
    req = [e for e in h.timer() if isinstance(e, Request) and e.command == "md.settle_inputs"]
    assert req, "provisional trigger did not fire"
    return h.run(h.eng.on_request_result(req[0].token, err_reply(codes.TIMEOUT)))


def roll(h: Harness, x: str, y: str, sod: SodSnapshot | None, *, token: str = "roll") -> Reply:
    """eod.rollover{x -> y, sod.version}; answers settle.sod_snapshot with ``sod`` when asked."""
    version = sod.version if sod is not None else 1
    effs = h.run(h.eng.handle(EodRolloverRequest(trading_day=x, next_trading_day=y, sod_version=version),
                              CommandCtx("eod.rollover", token=token)))
    req = [e for e in effs if isinstance(e, Request) and e.command == "settle.sod_snapshot"]
    if req:
        assert sod is not None
        effs = h.run(h.eng.on_request_result(req[0].token, ok_reply(sod)))
    replies = [e.reply for e in effs if hasattr(e, "reply")]
    assert replies
    return replies[-1]


class TestPhase2Eod:
    def test_restart_in_provisional_keeps_the_freeze_digests(self) -> None:
        """H3 / p04: after a restart on a provisional SOD, eod.freeze{T} still has T's fills and cash."""
        a = traded()
        cash(a, "m1", "1000.00")
        a.at("2026-09-29 20:56")
        provisional_now(a)
        live = a.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))[0].open_as(EodFreezeAck)
        assert live.accounts[0].count == 2 and live.cash[0].count == 1
        b = restart(a)
        assert b.eng.trading_mode == TradingMode.PROVISIONAL and b.eng.freeze_digest_source(TD) == "MEMORY"
        rep, _ = b.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        ack = rep.open_as(EodFreezeAck)
        assert ack.accounts == live.accounts and ack.cash == live.cash and ack.open_orders == 0
        assert b.eng.trading_mode == TradingMode.PROVISIONAL
        # the digest-only helper over the same rows (what the shell does for older days) agrees
        rows = a.db.snapshot()
        trades = [trade_from_fill_row(r) for r in sorted(rows["fills"], key=lambda r: r["fill_seq"])]
        cm = [cash_from_row(r) for r in rows["cash_movements"]]
        store = digest_only_ack(trading_day=TD, shard=1, account_ids=b.eng.owned, trades=trades, cash=cm,
                                frozen_ns=ack.frozen_ns)
        assert (store.accounts, store.cash, store.open_orders) == (ack.accounts, ack.cash, 0)

    def test_provisional_sod_uses_the_days_last_price(self) -> None:
        """M7 / p11: a next-day tick before md.settle_inputs answers does not replace T's close."""
        h = Harness(rules_only("daily_loss"))
        h.ticks({"rb2701": 3100.0, "i2701": 800.0})
        h.open_position("ACC3", "rb2701", B, 10, 3100.0)
        h.ticks({"rb2701": 3080.0})
        h.at("2026-09-29 20:56")
        req = [e for e in h.timer() if isinstance(e, Request)][0]
        h.tick("rb2701", 3090.0, trading_day=NEXT_TD, evaluate=False)
        h.run(h.eng.on_request_result(req.token, err_reply(codes.TIMEOUT)))
        lot = [r for r in h.db.tables["sod_lots"] if r["trading_day"] == NEXT_TD and r["instrument_id"] == "rb2701"][0]
        assert lot["basis_price"] == "3080"
        h2 = Harness(rules_only("daily_loss"))
        h2.ticks({"rb2701": 3100.0})
        h2.open_position("ACC3", "rb2701", B, 10, 3100.0)
        h2.run(h2.eng.enter_provisional({"rb2701": 3000.0}))
        lot2 = [r for r in h2.db.tables["sod_lots"] if r["trading_day"] == NEXT_TD and r["instrument_id"] == "rb2701"][0]
        assert lot2["basis_price"] == "3000"

    def test_roll_asks_trade_gateway_for_fills_of_the_new_day(self) -> None:
        """L4 / p13: next-day fills buffered in memory are recovered after a restart."""
        a = traded()
        a.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        t = a.trade("ACC3", "rb2701", B, O, 3100.0, 7, trading_day=NEXT_TD, origin=TradeOrigin.EXTERNAL)
        assert a.run(a.eng.apply_exec(t)) == []  # buffered until the roll (memory only)
        b = restart(a)
        effs = b.run(b.eng.handle(EodRolloverRequest(trading_day=TD, next_trading_day=NEXT_TD, sod_version=1),
                                  CommandCtx("eod.rollover", token="r")))
        tok = [e for e in effs if isinstance(e, Request)][0].token
        effs = b.run(b.eng.on_request_result(tok, ok_reply(sod_snapshot(trading_day=NEXT_TD, version=1))))
        reqs = [e for e in effs if isinstance(e, Request) and e.command == "td.query.trades"]
        assert sorted((r.payload.account_id, r.payload.trading_day) for r in reqs) == [  # type: ignore[attr-defined]
            ("ACC3", NEXT_TD), ("ACC4", NEXT_TD), ("ACC5", NEXT_TD)]
        r3 = [r for r in reqs if r.payload.account_id == "ACC3"][0]  # type: ignore[attr-defined]
        b.run(b.eng.on_request_result(r3.token, ok_reply(TradeBatch(trades=[t], next_trade_seq=t.trade_seq + 1,
                                                                     complete=True))))
        pos = [p for p in b.eng.snapshot("ACC3").positions if p.instrument_id == "rb2701"]
        assert [(p.td_qty, p.yd_qty) for p in pos] == [(7, 0)]
        a.run(a.eng.roll_day(sod_snapshot(trading_day=NEXT_TD, version=1)))  # live engine: from memory
        assert [(p.td_qty, p.yd_qty) for p in a.eng.snapshot("ACC3").positions if p.instrument_id == "rb2701"] == [(7, 0)]
        bt = Harness(rules_only("daily_loss"))
        bt.eng.query_trades_on_roll = False  # the backtest engine (auto_provisional=False) does not ask
        assert not [e for e in bt.run(bt.eng.roll_day(sod_snapshot(trading_day=NEXT_TD))) if isinstance(e, Request)]

    def test_duplicate_rollover_after_restart_gets_the_first_dayroll(self) -> None:
        """L5 / p20."""
        a = traded()
        a.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        dr, _ = rollover(a, 1)
        b = restart(a)
        rep = roll(b, TD, NEXT_TD, None, token="dup")
        assert rep.ok and rep.duplicate
        got = rep.open_as(DayRoll)
        assert (got.mode, got.from_trading_day, got.to_trading_day, got.sod_version) == (
            DayRollMode.ROLL, TD, NEXT_TD, 1)
        c = restart(a, dayrolls=[dr])  # the shell passes the sys.dayroll facts of its outbox: exact reply
        assert roll(c, TD, NEXT_TD, None, token="dup2").open_as(DayRoll) == dr

    def test_orders_of_a_closed_day_expire_at_the_roll(self) -> None:
        """M6 / p14: GFD orders left working are CANCELED (SESSION_CLOSE), released and confirmed."""
        lot = SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td="20260928", open_ts_ns=1,
                     open_price="3100", qty=10, basis_price="3100")
        h = Harness(rules_only("daily_loss"), sod=sod_snapshot(lots={"ACC3": [lot]}))
        h.ticks({"rb2701": 3100.0, "hc2701": 3300.0})
        h.submit("ACC3", "rb2701", S, CY, 10, 3150.0, coid="gfd")
        h.route_ok("gfd")
        h.broker("gfd", BrokerOrderStatus.ACCEPTED, remaining=10)
        h.submit("ACC3", "hc2701", B, O, 1, 3300.0, coid="open")
        h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        dr, effs = rollover(h, 1, lots={"ACC3": [lot]})
        assert dr.mode == DayRollMode.ROLL
        for coid in ("gfd", "open"):
            o = h.eng.orders.get(coid)
            assert o is not None and o.status == OrderStatus.CANCELED and o.close_reserved == 0
        ups = [p for p in h.outbox_payloads(effs) if isinstance(p, OrderUpdate)]
        assert {(u.client_order_id, u.reason, u.trading_day) for u in ups} == {("gfd", "SESSION_CLOSE", TD),
                                                                               ("open", "SESSION_CLOSE", TD)}
        assert sorted(e.client_order_id for e in effs if isinstance(e, ResolveRoute)) == ["gfd", "open"]
        f = h.eng.snapshot("ACC3").funds
        assert f.frozen_margin == 0 and f.frozen_fee == 0
        h.at("2026-09-29 21:05")
        h.tick("rb2701", 3100.0, trading_day=NEXT_TD)
        assert h.submit("ACC3", "rb2701", S, CY, 5, 3100.0)[0].ok  # the yd lots are closable again
        # the confirmation: a broker still reporting the order working opens a non-blocking break
        h.run(h.eng.on_resolve_result("gfd", ok_reply(RouteAck(client_order_id="gfd", status=BrokerOrderStatus.ACCEPTED))))
        h.run(h.eng.on_resolve_result("open", ok_reply(RouteAck(client_order_id="open", status=BrokerOrderStatus.CANCELED))))
        brk = [b for b in h.eng.breaks.values() if b.detail.get("reason") == "SESSION_CLOSE_STILL_WORKING"]
        assert len(brk) == 1 and brk[0].detail["client_order_id"] == "gfd" and not brk[0].blocking
        assert h.eng.account("ACC3").state == AccountState.NORMAL

    def test_rollover_sod_is_validated_before_anything_changes(self) -> None:
        h = traded()
        h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        before = h.eng.snapshot("ACC3")
        bad = sod_snapshot({"ACC3": "10000000", "ACC4": "10000000"}, trading_day=NEXT_TD, version=1)  # ACC5 missing
        rep = roll(h, TD, NEXT_TD, bad)
        assert rep.code == codes.PRECONDITION_FAILED and "misses accounts" in (rep.message or "")
        assert h.eng.trading_day == TD and h.eng.trading_mode == TradingMode.EOD_FROZEN
        assert h.eng.snapshot("ACC3").positions == before.positions
        assert roll(h, TD, NEXT_TD, sod_snapshot(trading_day=NEXT_TD, version=1), token="ok").ok

    def test_official_sod_is_cross_checked_against_the_end_of_day_lots(self) -> None:
        """01 §6.6: a SOD lot count that differs from the engine's EOD lots opens a WARN (non-blocking) break."""
        h = traded()
        h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        lots = {"ACC3": [
            SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td=TD, open_ts_ns=1,
                   open_price="3100", qty=10, basis_price="3100"),
            SodLot(instrument_id="i2701", direction=PosDirection.LONG, lot_no=2, open_td=TD, open_ts_ns=2,
                   open_price="800", qty=4, basis_price="800"),
        ]}  # i2701 4 instead of 5, and ACC4's hc2701 missing
        _, effs = rollover(h, 1, lots=lots)
        brk = {b.account_id: b for b in h.eng.breaks.values() if b.detail.get("reason") == "SOD_VS_EOD"}
        assert sorted(brk) == ["ACC3", "ACC4"] and not any(b.blocking for b in brk.values())
        assert brk["ACC3"].detail["diffs"] == [{"instrument_id": "i2701", "direction": "LONG", "engine": 5, "sod": 4}]
        assert brk["ACC4"].detail["diffs"] == [{"instrument_id": "hc2701", "direction": "LONG", "engine": 3, "sod": 0}]
        assert brk["ACC3"].kind == ReconBreakKind.POSITION_MISMATCH and brk["ACC3"].trading_day == NEXT_TD
        ev = [e for e in h.events(RiskEventKind.RECON_BREAK, effs) if e.detail.get("reason") == "SOD_VS_EOD"]
        assert len(ev) == 2 and all(e.severity == Severity.WARNING for e in ev)
        assert h.eng.account("ACC3").state == AccountState.NORMAL

    def test_withdrawal_while_frozen_is_checked(self) -> None:
        """M4 / p12: next-day withdrawals are checked against the current available minus pending ones."""
        h = traded()
        h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        assert cash(h, "w1", "99999999.00", CashType.WITHDRAW)[0].code == codes.PRECONDITION_FAILED
        avail = h.eng.snapshot("ACC3").funds.available
        amount = f"{int(avail * 0.6)}.00"
        rep, _ = cash(h, "w2", amount, CashType.WITHDRAW)
        assert rep.ok and rep.open_as(CashMovement).trading_day == NEXT_TD
        assert cash(h, "w3", amount, CashType.WITHDRAW)[0].code == codes.PRECONDITION_FAILED
        assert cash(h, "d1", "5000000.00")[0].ok  # deposits are never limited


# ---------------------------------------------------------------------------- catch-up EOD
def ext(h: Harness, account: str, inst: str, d: object, off: Offset, px: float, qty: int, td: str, tid: str) -> Trade:
    return h.trade(account, inst, d, off, px, qty, origin=TradeOrigin.EXTERNAL, trading_day=td, trade_id=tid)  # type: ignore[arg-type]


def official(prev: SodSnapshot, trades: list[Trade], prices: dict[str, str], nxt: str, version: int) -> SodSnapshot:
    """An "official" SOD with the clearing formulas (settle_day) at given settlement prices."""
    sod = build_provisional_sod(registry=Harness().eng.registry, sod=prev, trades=trades, cash=[],
                                prices={k: D(v) for k, v in prices.items()}, next_trading_day=nxt)
    return msgspec.structs.replace(sod, version=version)


class TestCatchUp:
    """Two missed EODs: the shard is PROVISIONAL on D+2 when clearing settles D, then D+1 (02 §8.4)."""

    def _trades(self, h: Harness) -> tuple[list[Trade], list[Trade], list[Trade]]:
        d0 = [ext(h, "ACC3", "rb2701", B, O, 3100.0, 10, TD, "D0-1"), ext(h, "ACC3", "i2701", B, O, 800.0, 5, TD, "D0-2"),
              ext(h, "ACC4", "hc2701", B, O, 3300.0, 3, TD, "D0-3")]
        d1 = [ext(h, "ACC3", "rb2701", S, CY, 3081.0, 4, NEXT_TD, "D1-1")]
        d2 = [ext(h, "ACC4", "hc2701", B, O, 3310.0, 2, D2, "D2-1")]
        return d0, d1, d2

    def test_two_day_gap_catch_up_equals_a_shard_that_never_missed(self) -> None:
        a = Harness(rules_only("daily_loss"))  # misses the EODs of TD and NEXT_TD
        b = Harness(rules_only("daily_loss"))  # never misses one
        d0, d1, d2 = self._trades(a)
        sod0 = sod_snapshot()
        sod1 = official(sod0, d0, {"rb2701": "3082", "i2701": "796", "hc2701": "3301"}, NEXT_TD, 1)
        sod2 = official(sod1, d1, {"rb2701": "3079", "i2701": "799", "hc2701": "3305"}, D2, 1)
        # ----- day TD
        for h in (a, b):
            h.ticks({"rb2701": 3100.0, "i2701": 800.0, "hc2701": 3300.0})
            for t in d0:
                h.run(h.eng.apply_exec(t))
            h.evaluate()
        b.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        live_d0 = b.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))[0].open_as(EodFreezeAck)
        assert roll(b, TD, NEXT_TD, sod1).open_as(DayRoll).mode == DayRollMode.ROLL
        a.at("2026-09-29 20:56")
        provisional_now(a)  # clearing is down: PROVISIONAL on NEXT_TD
        # ----- day NEXT_TD
        for h in (a, b):
            h.at("2026-09-30 10:00")
            h.ticks({"rb2701": 3081.0, "i2701": 798.0, "hc2701": 3302.0}) if h is b else [
                h.tick(i, px, trading_day=NEXT_TD) for i, px in {"rb2701": 3081.0, "i2701": 798.0, "hc2701": 3302.0}.items()]
            for t in d1:
                h.run(h.eng.apply_exec(t))
            h.evaluate()
        b.at("2026-09-30 16:00")
        live_d1 = b.cmd("eod.freeze", EodFreezeRequest(trading_day=NEXT_TD, version=1))[0].open_as(EodFreezeAck)
        assert roll(b, NEXT_TD, D2, sod2).open_as(DayRoll).mode == DayRollMode.ROLL
        a.at("2026-10-08 08:56")
        provisional_now(a)  # still down: PROVISIONAL again, now on D2
        assert (a.eng.trading_day, a.eng.trading_mode, a.eng.sod_version) == (D2, TradingMode.PROVISIONAL, -1)
        # ----- day D2 (before clearing comes back)
        for h in (a, b):
            h.at("2026-10-08 09:05")
            for i, px in {"rb2701": 3080.0, "i2701": 799.0, "hc2701": 3308.0}.items():
                h.tick(i, px, trading_day=D2)
            for t in d2:
                h.run(h.eng.apply_exec(t))
            h.evaluate()
        # ----- clearing comes back: contiguity is enforced
        rep = roll(a, NEXT_TD, D2, sod2, token="early")
        assert rep.code == codes.PRECONDITION_FAILED and f"settle {TD} first" in (rep.message or "")
        # freeze(TD): an older unsettled day -> digests only, from the store (the shell uses digest_only_ack)
        assert a.eng.freeze_digest_source(TD) == "STORE" and a.eng.freeze_digest_source(NEXT_TD) == "MEMORY"
        assert a.eng.freeze_digest_source("20261009") is None
        assert a.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))[0].code == codes.NOT_FOUND
        rows = a.db.snapshot()
        fills = [trade_from_fill_row(r) for r in sorted(rows["fills"], key=lambda r: r["fill_seq"])]
        ack0 = digest_only_ack(trading_day=TD, shard=1, account_ids=a.eng.owned, trades=fills,
                               cash=[cash_from_row(r) for r in rows.get("cash_movements", [])], frozen_ns=1)
        assert ack0.accounts == live_d0.accounts and ack0.cash == live_d0.cash
        # rollover(TD -> NEXT_TD): CATCH_UP, the current (provisional) day is untouched
        funds_before = a.eng.snapshot("ACC3").funds
        dr = roll(a, TD, NEXT_TD, sod1).open_as(DayRoll)
        assert (dr.mode, dr.from_trading_day, dr.to_trading_day, dr.sod_version) == (DayRollMode.CATCH_UP, TD, NEXT_TD, 1)
        assert (a.eng.trading_day, a.eng.trading_mode, a.eng.sod_version) == (D2, TradingMode.PROVISIONAL, -1)
        assert a.eng.snapshot("ACC3").funds == funds_before
        acc = {r["account_id"]: r for r in a.db.tables["sod_accounts"] if r["trading_day"] == NEXT_TD}
        assert acc["ACC3"]["sod_version"] == 1 and acc["ACC3"]["provisional"] is False
        eod = {r["trading_day"]: r for r in a.db.tables["eod_states"]}
        assert (eod[TD]["status"], eod[TD]["next_trading_day"], eod[TD]["sod_version"]) == ("ROLLED", NEXT_TD, 1)
        assert a.eng.last_official_day() == NEXT_TD
        dup = roll(a, TD, NEXT_TD, sod1, token="dup")
        assert dup.duplicate and dup.open_as(DayRoll) == dr  # repeats are idempotent by (Y, version)
        # freeze(NEXT_TD): the provisional source day, digests from memory, trading mode unchanged
        ack1 = a.cmd("eod.freeze", EodFreezeRequest(trading_day=NEXT_TD, version=1))[0].open_as(EodFreezeAck)
        assert ack1.accounts == live_d1.accounts and a.eng.trading_mode == TradingMode.PROVISIONAL
        # rollover(NEXT_TD -> D2): REBASE as today
        dr2 = roll(a, NEXT_TD, D2, sod2, token="rebase").open_as(DayRoll)
        assert dr2.mode == DayRollMode.REBASE
        assert (a.eng.trading_day, a.eng.trading_mode, a.eng.sod_version) == (D2, TradingMode.OPEN, 1)
        # final state == the shard that never missed an EOD
        for acct in ("ACC3", "ACC4", "ACC5"):
            sa, sb = a.eng.snapshot(acct), b.eng.snapshot(acct)
            for fld in ("pre_balance", "static_equity", "equity", "margin", "position_pnl", "close_pnl", "fee",
                        "available", "cum_net_deposit"):
                assert getattr(sa.funds, fld) == pytest.approx(getattr(sb.funds, fld)), (acct, fld)
            pa = [(p.instrument_id, p.direction, p.td_qty, p.yd_qty, p.basis_avg_price) for p in sa.positions]
            pb = [(p.instrument_id, p.direction, p.td_qty, p.yd_qty, p.basis_avg_price) for p in sb.positions]
            assert pa == pb, acct
            assert sa.state == sb.state == AccountState.NORMAL and not sa.restrictions
        assert not [b_ for b_ in a.eng.breaks.values() if b_.blocking]

    def test_newer_version_of_a_caught_up_day_supersedes(self) -> None:
        a = Harness(rules_only("daily_loss"))
        d0, _, _ = self._trades(a)
        a.ticks({"rb2701": 3100.0, "i2701": 800.0, "hc2701": 3300.0})
        for t in d0:
            a.run(a.eng.apply_exec(t))
        a.at("2026-09-29 20:56")
        provisional_now(a)
        a.at("2026-10-08 08:56")
        provisional_now(a)
        v1 = official(sod_snapshot(), d0, {"rb2701": "3082", "i2701": "796", "hc2701": "3301"}, NEXT_TD, 1)
        v2 = msgspec.structs.replace(v1, version=2)
        assert roll(a, TD, NEXT_TD, v1).open_as(DayRoll).mode == DayRollMode.CATCH_UP
        assert roll(a, TD, NEXT_TD, v2, token="v2").open_as(DayRoll).mode == DayRollMode.CATCH_UP
        eod = {r["trading_day"]: r for r in a.db.tables["eod_states"]}
        assert eod[TD]["sod_version"] == 2
        assert {r["sod_version"] for r in a.db.tables["sod_accounts"] if r["trading_day"] == NEXT_TD} == {2}
        old = roll(a, TD, NEXT_TD, v1, token="old")
        assert old.code == codes.PRECONDITION_FAILED  # an older version no longer applies
