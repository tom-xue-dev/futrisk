"""The 9-step pre-trade pipeline: every reject code of 02 §11.2 and the close-order exemptions."""

from __future__ import annotations

from typing import Any

import pytest
from enginetest_support import (
    ADMIN,
    CT,
    CY,
    RM,
    TD,
    B,
    C,
    Harness,
    O,
    S,
    rules_only,
    rules_with,
    sod_snapshot,
)

from futrisk.common.config.risk_rules import RuleDef
from futrisk.core.risk_account import Lifecycle
from futrisk.messages import codes
from futrisk.messages.enums import (
    ControlOrigin,
    FirmAction,
    MdQuality,
    Offset,
    OrderStatus,
    OrderType,
    PosDirection,
    RestrictionKind,
    TradeOrigin,
)
from futrisk.messages.eod import EodFreezeRequest, SodLot
from futrisk.messages.orders import CancelRequest, OrderRequest
from futrisk.messages.risk import FirmControl, HaltRequest, KillSwitchRequest, Restriction

P = {"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0, "IF2612": 4500.0}


def lot(inst: str, qty: int, direction: PosDirection = PosDirection.LONG, price: str = "3100", lot_no: int = 1) -> SodLot:
    return SodLot(instrument_id=inst, direction=direction, lot_no=lot_no, open_td="20260928", open_ts_ns=lot_no,
                  open_price=price, qty=qty, basis_price=price)


def base(rules: Any = None, **kw: Any) -> Harness:
    kw.setdefault("sod", sod_snapshot(lots={"ACC3": [lot("rb2701", 10), lot("IF2612", 2, price="4500", lot_no=2)]}))
    h = Harness(rules if rules is not None else rules_only("daily_loss"), **kw)
    h.ticks(P)
    return h


def code(h: Harness, *args: Any, **kw: Any) -> str:
    rep, _ = h.submit(*args, **kw)
    return rep.code


def raw(h: Harness, **fields: Any) -> str:
    req = OrderRequest(**{"client_order_id": f"raw-{len(h.effects)}", "account_id": "ACC3", "instrument_id": "rb2701",
                          "direction": B, "offset": O, "qty": 1, "price": 3100.0, **fields})
    rep, _ = h.cmd("order.submit", req)
    return rep.code


class TestStep1Static:
    def test_static_rejects(self) -> None:
        h = base()
        rows_before = len(h.db.tables["orders"])
        assert code(h, "ACC99", "rb2701", B, O, 1) == codes.REJECT_UNKNOWN_ACCOUNT
        assert code(h, "ACC1", "rb2701", B, O, 1) == codes.NOT_OWNER
        assert raw(h, instrument_id="zz2701") == codes.REJECT_UNKNOWN_INSTRUMENT
        assert len(h.db.tables["orders"]) == rows_before  # 主数据之外的账户 / 合约不落库
        assert raw(h, order_type=OrderType.LIMIT, price=None) == codes.REJECT_INVALID_ORDER
        assert raw(h, offset=Offset.FORCE_CLOSE) == codes.REJECT_INVALID_ORDER
        assert raw(h, price=-1.0) == codes.REJECT_INVALID_ORDER
        assert raw(h, qty=0) == codes.REJECT_INVALID_QTY
        assert raw(h, instrument_id="IF2612", qty=21, price=4500.0) == codes.REJECT_QTY_EXCEEDS_MAX
        assert raw(h, price=3100.5) == codes.REJECT_PRICE_OFF_TICK
        rejected = [r for r in h.db.tables["orders"] if r["status"] == "REJECTED"]
        # qty <= 0 / a non-positive price cannot be stored (03 §4.3 CHECK(qty > 0)): answered without a row
        assert len(rejected) == 4 and {r["reject_code"] for r in rejected} >= {codes.REJECT_PRICE_OFF_TICK}
        assert {r["reject_code"] for r in rejected}.isdisjoint({codes.REJECT_INVALID_QTY})
        assert raw(h, qty=2**31) == codes.REJECT_QTY_EXCEEDS_MAX and len(h.db.tables["orders"]) == rows_before + 4
        assert raw(h, qty=-5) == codes.REJECT_INVALID_QTY and raw(h, price=float("nan")) == codes.REJECT_INVALID_ORDER
        assert len(h.db.tables["orders"]) == rows_before + 4

    def test_not_tradable(self) -> None:
        h = Harness(rules_only("daily_loss"), sod=sod_snapshot(trading_day="20261020"), start="2026-10-20 09:30")
        assert code(h, "ACC3", "rb2610", B, O, 1, 3100.0) == codes.REJECT_NOT_TRADABLE  # 最后交易日 20261015

    def test_duplicate_client_order_id(self) -> None:
        h = base()
        r1, _ = h.submit("ACC3", "rb2701", B, O, 1, coid="dup")
        r2, effs = h.submit("ACC3", "rb2701", B, O, 1, coid="dup")
        assert r1.ok and r2.ok and r2.duplicate and r2.open() == r1.open()
        assert not h.routes(effs)  # 不会重复路由
        r3, _ = h.submit("ACC3", "rb2701", B, O, 2, coid="dup")
        assert r3.code == codes.REJECT_DUPLICATE_ORDER_ID
        assert sum(r["client_order_id"] == "dup" for r in h.db.tables["orders"]) == 1


class TestStep2System:
    def test_loading_and_stopping_reject_everything(self) -> None:
        h = Harness(rules_only("daily_loss"), load=False)
        assert code(h, "ACC3", "rb2701", B, O, 1, 3100.0) == codes.REJECT_SYSTEM_NOT_READY
        h2 = base()
        h2.run(h2.eng.set_lifecycle(Lifecycle.STOPPING))
        assert code(h2, "ACC3", "rb2701", B, O, 1) == codes.REJECT_SYSTEM_NOT_READY
        assert code(h2, "ACC3", "rb2701", S, CY, 1) == codes.REJECT_SYSTEM_NOT_READY  # 停机时平仓也拒

    def test_eod_frozen(self) -> None:
        h = base()
        rep, _ = h.cmd("eod.freeze", EodFreezeRequest(trading_day=TD, version=1))
        assert rep.ok
        assert code(h, "ACC3", "rb2701", B, O, 1) == codes.REJECT_EOD_IN_PROGRESS
        assert code(h, "ACC3", "rb2701", S, CY, 1) == codes.REJECT_EOD_IN_PROGRESS

    def test_session_closed(self) -> None:
        h = base()
        h.at("2026-09-29 12:00")  # 午休
        assert code(h, "ACC3", "rb2701", B, O, 1) == codes.REJECT_SESSION_CLOSED
        assert code(h, "ACC3", "rb2701", S, CY, 1) == codes.REJECT_SESSION_CLOSED
        h.at("2026-09-29 13:10")  # 商品已开盘前 / 股指 13:00 已开
        assert code(h, "ACC3", "IF2612", S, C, 1, 4500.0) == codes.OK
        assert code(h, "ACC3", "rb2701", S, CY, 1) == codes.REJECT_SESSION_CLOSED  # 商品 13:30 开盘

    def test_md_quality_blocks_opens_only(self) -> None:
        h = base()
        h.md_status({"rb2701": MdQuality.STALE})
        assert code(h, "ACC3", "rb2701", B, O, 1) == codes.REJECT_MD_UNAVAILABLE
        assert code(h, "ACC3", "rb2701", S, CY, 1) == codes.OK
        assert code(h, "ACC3", "j2701", B, O, 1, 1800.0) == codes.REJECT_MD_UNAVAILABLE  # 从未收到行情 = UNKNOWN

    def test_kill_switch_blocks_opens_only(self) -> None:
        h = base()
        rep, _ = h.cmd("firm.kill_switch", KillSwitchRequest(command_id="k1", on=True, reason="drill", actor="rm", role=RM))
        assert rep.ok
        assert code(h, "ACC3", "rb2701", B, O, 1) == codes.REJECT_FIRM_KILL_SWITCH
        assert code(h, "ACC3", "rb2701", S, CY, 1) == codes.OK

    def test_gateway_down(self) -> None:
        h = base()
        h.run(h.eng.set_gateway_status(False))
        assert code(h, "ACC3", "rb2701", B, O, 1) == codes.REJECT_GATEWAY_UNAVAILABLE
        assert code(h, "ACC3", "rb2701", S, CY, 1) == codes.REJECT_GATEWAY_UNAVAILABLE


class TestStep3Price:
    def test_limits_and_fat_finger(self) -> None:
        h = base()
        assert code(h, "ACC3", "rb2701", B, O, 1, 3260.0) == codes.REJECT_PRICE_LIMIT  # 涨停 3255
        assert code(h, "ACC3", "rb2701", B, O, 1, 3200.0) == codes.REJECT_FAT_FINGER  # max(50 跳, 3%) = 93
        assert code(h, "ACC3", "rb2701", B, O, 1, 3190.0) == codes.OK
        assert code(h, "ACC3", "rb2701", S, CY, 1, 3000.0) == codes.REJECT_FAT_FINGER  # 平仓同样检查价格


class TestStep4Account:
    def test_halted_blocks_opens_not_closes(self) -> None:
        h = base()
        rep, _ = h.cmd("risk.halt", HaltRequest(command_id="h1", account_id="ACC3", reason="manual", actor="rm", role=RM))
        assert rep.ok and h.eng.account("ACC3").halted_latched
        assert code(h, "ACC3", "rb2701", B, O, 1) == codes.REJECT_ACCOUNT_HALTED
        assert code(h, "ACC3", "rb2701", S, CY, 1) == codes.OK

    def test_block_open_account_and_product_scope(self) -> None:
        h = base()
        h.run(h.eng.set_system_restriction("ACC3", "FILL_GAP", True))
        rep, _ = h.submit("ACC3", "rb2701", B, O, 1)
        assert rep.code == codes.REJECT_ACCOUNT_BLOCKED and rep.open().reject_detail["source"] == "SYSTEM:FILL_GAP"
        assert code(h, "ACC3", "rb2701", S, CY, 1) == codes.OK
        h.run(h.eng.set_system_restriction("ACC3", "FILL_GAP", False))
        ctl = FirmControl(action=FirmAction.SET_RESTRICTIONS, origin=ControlOrigin.RULE, trading_day=TD, ts_ns=1,
                          restrictions=[Restriction(kind=RestrictionKind.BLOCK_OPEN, scope_key="firm/prod:rb",
                                                    source="firm_rb/block")])
        h.run(h.eng.apply_firm_control(ctl))
        assert code(h, "ACC3", "rb2701", B, O, 1) == codes.REJECT_PRODUCT_BLOCKED
        assert code(h, "ACC3", "hc2701", B, O, 1) == codes.OK

    def test_exposure_cap_factor(self) -> None:
        rule = RuleDef.model_validate({"id": "hwm_dd", "metric": "account.hwm_dd", "op": ">=", "levels": [
            {"name": "halve", "threshold": 0.12, "action": "RESTRICT", "params": {"exposure_cap_factor": 0.5}}]})
        sod = sod_snapshot()
        sod.accounts[0].hwm_equity = "12000000.00"  # ACC3: 距高水位回撤 16.7%
        h = base(rules_with([rule]), sod=sod)
        ra = h.eng.account("ACC3")
        assert [r.kind for r in ra.restrictions] == [RestrictionKind.EXPOSURE_CAP_FACTOR]
        # rb 上限 = 3% / 5% = 60% 权益，减半后 30% ≈ 3,000,000 元 ≈ 96 手
        assert code(h, "ACC3", "rb2701", B, O, 100) == codes.REJECT_EXPOSURE_CAP
        assert code(h, "ACC3", "rb2701", B, O, 50) == codes.OK


class TestStep5Throttle:
    def test_token_bucket(self) -> None:
        h = base(rules_with(rules_only("daily_loss").rules, throttle_burst=2, throttle_rate_per_s=0.001))
        assert code(h, "ACC3", "rb2701", B, O, 1) == codes.OK
        assert code(h, "ACC3", "rb2701", B, O, 1) == codes.OK
        assert code(h, "ACC3", "rb2701", B, O, 1) == codes.REJECT_THROTTLED
        assert code(h, "ACC4", "rb2701", B, O, 1) == codes.OK  # 每个账户独立

    def test_open_volume_limit(self) -> None:
        h = base()
        t = h.trade("ACC3", "IF2612", B, O, 4500.0, 490, origin=TradeOrigin.EXTERNAL)
        h.run(h.eng.apply_exec(t))
        rep, _ = h.submit("ACC3", "IF2612", B, O, 11, 4500.0)
        assert rep.code == codes.REJECT_OPEN_VOLUME_LIMIT and rep.open().reject_detail["limit"] == 500

    def test_cancel_limits(self) -> None:
        h = base()
        rep, _ = h.submit("ACC3", "IF2612", B, O, 16, 4500.0)
        coid = rep.open().client_order_id
        ra = h.eng.account("ACC3")
        ra.large_cancels_by_inst["IF2612"] = 100
        rep, _ = h.cmd("order.cancel", CancelRequest(command_id="c1", client_order_id=coid, account_id="ACC3"))
        assert rep.code == codes.REJECT_LARGE_CANCEL_LIMIT
        ra.cancels_by_inst["IF2612"] = 400
        rep, _ = h.cmd("order.cancel", CancelRequest(command_id="c2", client_order_id=coid, account_id="ACC3"))
        assert rep.code == codes.REJECT_CANCEL_LIMIT
        assert h.eng.orders.get(coid).status == OrderStatus.ROUTING


class TestStep6Position:
    def test_shfe_close_today_and_yesterday(self) -> None:
        h = base()
        h.open_position("ACC3", "rb2701", B, 5)  # 今仓 5，昨仓 10（SOD）
        assert code(h, "ACC3", "rb2701", S, CT, 6) == codes.REJECT_CLOSE_TODAY_EXCEEDS
        assert code(h, "ACC3", "rb2701", S, CY, 11) == codes.REJECT_CLOSE_YD_EXCEEDS
        assert code(h, "ACC3", "rb2701", S, C, 11) == codes.REJECT_CLOSE_YD_EXCEEDS  # SHFE 的 CLOSE = 平昨
        rep, effs = h.submit("ACC3", "rb2701", S, C, 8)
        assert rep.ok and h.routes(effs)[0].order.offset == Offset.CLOSE_YESTERDAY  # 已按交易所规则归一
        assert code(h, "ACC3", "rb2701", S, CY, 3) == codes.REJECT_CLOSE_YD_EXCEEDS  # 在途平仓占用可平量
        assert code(h, "ACC3", "rb2701", S, CY, 2) == codes.OK
        assert code(h, "ACC3", "rb2701", S, CT, 5) == codes.OK
        pv = [p for p in h.eng.snapshot("ACC3").positions if p.instrument_id == "rb2701"][0]
        assert (pv.closable_td_qty, pv.closable_yd_qty) == (0, 0)

    def test_close_exceeds_position_non_explicit_exchange(self) -> None:
        h = base()
        assert code(h, "ACC3", "IF2612", S, C, 3, 4500.0) == codes.REJECT_CLOSE_EXCEEDS_POSITION
        rep, effs = h.submit("ACC3", "IF2612", S, CT, 2, 4500.0)
        assert rep.ok and h.routes(effs)[0].order.offset == Offset.CLOSE  # 非 SHFE 归一为 CLOSE
        assert code(h, "ACC3", "hc2701", S, CT, 1) == codes.REJECT_CLOSE_TODAY_EXCEEDS  # 无持仓

    def test_position_limit_counts_in_flight_opens(self) -> None:
        sod = sod_snapshot({"ACC3": "1000000000"}, lots={"ACC3": [lot("IF2612", 4980, price="4500")]})
        h = base(sod=sod)
        assert code(h, "ACC3", "IF2612", B, O, 15, 4500.0) == codes.OK
        rep, _ = h.submit("ACC3", "IF2612", B, O, 6, 4500.0)
        assert rep.code == codes.REJECT_POSITION_LIMIT and rep.open().reject_detail["in_flight"] == 15


class TestStep7Funds:
    def test_insufficient_funds(self) -> None:
        h = base()
        assert code(h, "ACC5", "IF2612", B, O, 20, 4500.0) == codes.OK  # 保证金 3,240,000 ≤ 5,000,000 × 95%
        rep, _ = h.submit("ACC5", "IF2612", B, O, 20, 4500.0)
        assert rep.code == codes.REJECT_INSUFFICIENT_FUNDS
        d = rep.open().reject_detail
        assert d["required"] == pytest.approx(3_240_000 + 621.0, abs=1.0) and d["available"] < d["required"]


class TestStep8WhatIf:
    def test_whatif_breach_on_a_blocking_level(self) -> None:
        h = base(rules_only("leverage"))
        h.open_position("ACC4", "IF2612", B, 20, 4500.0)  # 名义 27,000,000 ≈ 2.7 倍
        assert h.eng.snapshot("ACC4").metrics.leverage == pytest.approx(2.7, rel=1e-3)
        rep, _ = h.submit("ACC4", "IF2612", B, O, 20, 4500.0)
        assert rep.code == codes.REJECT_WHATIF_BREACH
        d = rep.open().reject_detail
        assert (d["rule_id"], d["level"]) == ("leverage", "block") and d["value"] > 4.0
        assert code(h, "ACC4", "IF2612", B, O, 5, 4500.0) == codes.OK  # 3.4 倍 < 4
        assert code(h, "ACC4", "IF2612", S, C, 20, 4500.2) == codes.OK  # 平仓豁免 what-if


class TestStep9SelfTrade:
    def test_self_trade(self) -> None:
        h = base()
        assert code(h, "ACC3", "rb2701", S, CY, 2, 3105.0) == codes.OK
        assert code(h, "ACC3", "rb2701", B, O, 1, 3105.0) == codes.REJECT_SELF_TRADE
        assert code(h, "ACC3", "rb2701", B, O, 1, 3104.0) == codes.OK
        assert code(h, "ACC4", "rb2701", B, O, 1, 3105.0) == codes.OK  # 其他账户不算自成交


class TestCloseExemptions:
    def test_close_passes_every_open_only_block(self) -> None:
        h = base()
        h.cmd("risk.halt", HaltRequest(command_id="h1", account_id="ACC3", reason="x", actor="rm", role=RM))
        h.cmd("firm.kill_switch", KillSwitchRequest(command_id="k1", on=True, reason="x", actor="rm", role=ADMIN))
        h.run(h.eng.set_system_restriction("ACC3", "RECON_BREAK", True))
        h.md_status({"rb2701": MdQuality.UNKNOWN})
        rep, effs = h.submit("ACC3", "rb2701", S, CY, 10)
        assert rep.ok, rep.code
        assert h.routes(effs)
        assert code(h, "ACC3", "rb2701", S, CY, 1) == codes.REJECT_CLOSE_YD_EXCEEDS  # 可平量仍然检查

    def test_close_allowed_with_negative_available(self) -> None:
        sod = sod_snapshot({"ACC3": "100000"}, lots={"ACC3": [lot("IF2612", 2, price="4500")]})
        h = base(sod=sod)
        f = h.eng.snapshot("ACC3").funds
        assert f.available < 0  # 保证金 324,000 > 权益 100,000
        assert code(h, "ACC3", "IF2612", B, O, 1, 4500.0) == codes.REJECT_INSUFFICIENT_FUNDS
        assert code(h, "ACC3", "IF2612", S, C, 2, 4500.0) == codes.OK


class TestPhase2Pretrade:
    def test_whatif_runs_on_last_marks_whatever_their_quality(self) -> None:
        """H2 / p06: a STALE instrument elsewhere in the book no longer switches the what-if off (fail-open)."""
        sod = sod_snapshot(lots={"ACC4": [lot("cu2611", 1, price="80000")]})
        h = Harness(rules_only("leverage"), sod=sod)
        h.ticks({"IF2612": 4500.0, "cu2611": 80000.0})
        h.open_position("ACC4", "IF2612", B, 20, 4500.0)
        rep, _ = h.submit("ACC4", "IF2612", B, O, 20, 4500.0)
        assert rep.code == codes.REJECT_WHATIF_BREACH
        h.md_status({"cu2611": MdQuality.STALE})
        rep, _ = h.submit("ACC4", "IF2612", B, O, 20, 4500.0)
        assert rep.code == codes.REJECT_WHATIF_BREACH and rep.open().reject_detail["rule_id"] == "leverage"
        assert h.submit("ACC4", "IF2612", B, O, 1, 4500.0)[0].ok  # small orders still pass

    def test_close_skips_the_fat_finger_on_a_stale_mark(self) -> None:
        """L7 / p16: a halted account can close at the real market price while the feed is stale; the band holds."""
        h = Harness(rules_only("daily_loss"), sod=sod_snapshot(lots={"ACC3": [lot("cu2611", 5, price="80000")]}))
        h.ticks({"cu2611": 80000.0})
        h.cmd("risk.halt", HaltRequest(command_id="h", account_id="ACC3", reason="x", actor="rm", role=RM))
        h.md_status({"cu2611": MdQuality.STALE})
        up, lo = h.eng.limits_of("cu2611")  # type: ignore[misc]
        assert h.submit("ACC3", "cu2611", S, CY, 1, lo - 10.0)[0].code == codes.REJECT_PRICE_LIMIT
        assert h.submit("ACC3", "cu2611", S, CY, 2, 77000.0)[0].ok
        h.md_status({"cu2611": MdQuality.OK})
        assert h.submit("ACC3", "cu2611", S, CY, 1, 77000.0)[0].code == codes.REJECT_FAT_FINGER  # fresh mark: checked
        assert up > 80000.0
