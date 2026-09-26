"""Account metrics and the metric registry (02 §7.6 AccountMetricsView / ExposureView, 01 §8.14)."""

from __future__ import annotations

import pytest
from enginetest_support import TD, config, registry

from futrisk.core.ledger import IntradayLedger
from futrisk.core.metrics import (
    ComplianceCounters,
    OrderDelta,
    build_valuation,
    compliance_utilization,
    get_metric,
    metric_value,
    registered_metrics,
)
from futrisk.messages.enums import CashType, Exchange, MdQuality, PosDirection
from futrisk.messages.eod import SodAccount, SodLot
from futrisk.messages.risk import CashMovement

OK = MdQuality.OK


def ledger(hwm: str = "10500000.00") -> IntradayLedger:
    sa = SodAccount(
        account_id="ACC3", pre_balance="10000000.00", initial_capital="10000000.00", cum_net_deposit="0.00",
        hwm_equity=hwm,
        lots=[
            SodLot(instrument_id="rb2701", direction=PosDirection.LONG, lot_no=1, open_td="20260925", open_ts_ns=1,
                   open_price="3000", qty=100, basis_price="3100"),
            SodLot(instrument_id="i2701", direction=PosDirection.SHORT, lot_no=1, open_td="20260925", open_ts_ns=2,
                   open_price="810", qty=20, basis_price="800"),
        ],
    )
    led = IntradayLedger(registry(), TD, sa, {"rb2701": "3100", "i2701": "800"})
    led.set_mark("rb2701", 3150.0)
    led.set_mark("i2701", 790.0)
    return led


class TestValuation:
    def test_funds_and_pnl_definitions(self) -> None:
        v = build_valuation(ledger(), registry(), quality={"rb2701": OK, "i2701": OK}, intraday_peak=10_100_000.0)
        m, f = v.metrics, v.funds
        assert f.position_pnl == pytest.approx(70_000.0)
        assert f.equity == pytest.approx(10_070_000.0)
        assert m.daily_pnl == pytest.approx(70_000.0)
        assert m.daily_pnl_pct_init == pytest.approx(0.007)
        assert m.daily_pnl_pct_sod == pytest.approx(0.007)
        assert m.floating_pnl_pct_init == pytest.approx(0.007)
        assert m.perf_equity == pytest.approx(10_070_000.0) and m.nav == pytest.approx(1.007)
        assert m.intraday_peak_equity == pytest.approx(10_100_000.0)
        assert m.intraday_dd == pytest.approx(30_000 / 10_100_000)
        assert m.hwm_equity == pytest.approx(10_500_000.0)
        assert m.hwm_dd == pytest.approx(430_000 / 10_500_000)
        assert v.quality == OK

    def test_cash_is_not_pnl(self) -> None:
        led = ledger()
        led.apply_cash(CashMovement(movement_id="m1", account_id="ACC3", trading_day=TD, type=CashType.DEPOSIT,
                                    amount="1000000.00", actor="a", reason="r", ts_ns=1))
        v = build_valuation(led, registry(), quality={"rb2701": OK, "i2701": OK}, intraday_peak=0.0)
        m, f = v.metrics, v.funds
        assert f.static_equity == pytest.approx(11_000_000.0)
        assert m.daily_pnl == pytest.approx(70_000.0)  # 静态权益已包含出入金，不能再扣一次
        assert m.daily_pnl_pct_sod == pytest.approx(70_000 / 11_000_000)
        assert m.perf_equity == pytest.approx(10_070_000.0) and m.nav == pytest.approx(1.007)

    def test_exposure_metrics(self) -> None:
        v = build_valuation(ledger(), registry(), quality={"rb2701": OK, "i2701": OK}, intraday_peak=0.0)
        m, eq = v.metrics, v.funds.equity
        assert m.gross_notional == pytest.approx(3_150_000 + 1_580_000)
        assert m.net_notional == pytest.approx(3_150_000 - 1_580_000)
        assert m.leverage == pytest.approx(4_730_000 / eq)
        w1, w2 = 3_150_000 / 4_730_000, 1_580_000 / 4_730_000
        assert m.effective_n == pytest.approx(1 / (w1**2 + w2**2))
        assert m.limit_stress == pytest.approx((3_150_000 * 0.05 + 1_580_000 * 0.06) / eq)
        rb = v.exposures["rb"]
        assert (rb.long_lots, rb.short_lots, rb.net_lots, rb.exchange) == (100, 0, 100, Exchange.SHFE)
        assert rb.cap_pct == pytest.approx(0.6) and rb.utilization == pytest.approx(3_150_000 / eq / 0.6)
        i = v.exposures["i"]
        assert i.net_notional == pytest.approx(-1_580_000) and i.net_pct_equity == pytest.approx(1_580_000 / eq)
        assert v.sectors["black"].net_pct_equity == pytest.approx(1_570_000 / eq)
        assert v.largest_product() == "rb"
        assert v.product_pnl("rb", PosDirection.LONG) == pytest.approx(50_000.0)

    def test_quality_is_the_worst_of_the_instruments_involved(self) -> None:
        v = build_valuation(ledger(), registry(), quality={"rb2701": OK}, intraday_peak=0.0)
        assert v.quality == MdQuality.UNKNOWN  # i2701 missing -> UNKNOWN (fail-safe)
        assert v.product_quality == {"rb": OK, "i": MdQuality.UNKNOWN}
        assert v.quality_for("prod", "rb") == OK and v.quality_for("sector", "black") == MdQuality.UNKNOWN
        assert v.unknown_exposure_share() == pytest.approx(1_580_000 / 4_730_000)

    def test_whatif(self) -> None:
        v = build_valuation(ledger(), registry(), quality={"rb2701": OK, "i2701": OK}, intraday_peak=0.0)
        d = OrderDelta(instrument_id="rb2701", product="rb", sector="black", exchange=Exchange.SHFE, qty=10,
                       signed_notional=315_000.0, pnl_delta=-1_000.0, fee=3.16, margin_delta=25_280.0, limit_pct=0.05)
        a = v.whatif(d)
        assert a.metrics.daily_pnl == pytest.approx(70_000 - 1_000 - 3.16)
        assert a.metrics.gross_notional == pytest.approx(4_730_000 + 315_000)
        assert a.funds.margin == pytest.approx(v.funds.margin + 25_280.0)
        assert a.exposures["rb"].net_lots == 110
        assert metric_value("product.utilization", a, "rb") > metric_value("product.utilization", v, "rb")  # type: ignore[operator]
        assert metric_value("account.leverage", a) == pytest.approx(5_045_000 / a.funds.equity)


class TestRegistry:
    def test_every_configured_metric_resolves(self) -> None:
        for r in config().risk_rules.rules:
            md = get_metric(r.metric)
            assert md.name == r.metric
        names = registered_metrics()
        assert {"account.daily_pnl_pct_init", "account.floating_pnl_pct_init", "account.nav", "account.hwm_dd",
                "product.utilization", "sector.net_pct_equity", "account.compliance_util", "firm.daily_pnl_pct_init"} <= set(names)
        with pytest.raises(KeyError):
            get_metric("account.nope")

    def test_not_applicable_metrics_are_none(self) -> None:
        sa = SodAccount(account_id="ACC4", pre_balance="1000.00", initial_capital="1000.00", hwm_equity="1000.00")
        led = IntradayLedger(registry(), TD, sa, {})
        v = build_valuation(led, registry(), quality={}, intraday_peak=0.0)
        assert metric_value("account.effective_n", v) is None  # 空仓：有效品种数不适用
        assert v.metrics.effective_n == 0.0
        assert metric_value("product.utilization", v, "rb") == 0.0
        assert v.quality == OK  # 空仓账户没有行情依赖
        assert get_metric("account.daily_pnl_pct_init").money_base == "initial_capital"

    def test_compliance_utilization(self) -> None:
        c = ComplianceCounters(open_volume={"IF2612": 400}, cancels={"rb2701": 100}, large_cancels={"IF2612": 10}, self_trades=1)
        assert compliance_utilization(registry(), c) == pytest.approx(0.8)  # 400 / 500 IF 当日开仓限制
