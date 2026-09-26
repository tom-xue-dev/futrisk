"""Unit tests for futrisk.core.settle_price: priority, VWAP, rounding, fallback chains (01 §8.15)."""

from __future__ import annotations

from decimal import Decimal

import pytest
from tests.golden.builders import inst, lot_fee, make_registry, margin, product, rate_fee, ts

from futrisk.common.clock import ns_to_bj
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.settle_price import (
    BenchmarkMove,
    HourlyBucket,
    ManualPrice,
    build_settle_input,
    determine_settlement_price,
    first_hour_end_ns,
    hourly_buckets,
    trading_hour_buckets,
    trading_hour_segments,
    trading_hour_windows,
    vwap,
    vwap_from_trades,
)
from futrisk.messages.enums import LimitLock, Rounding, SettleInputQuality, SettleMethod, SettlePriceSource
from futrisk.messages.md import SettleInput

D = Decimal
TD = "20260922"


@pytest.fixture(scope="module")
def reg() -> InstrumentRegistry:
    return make_registry(
        {
            "IF": product("CFFEX", multiplier=300, tick="0.2", limit_pct="0.10", fees=[rate_fee("0.000023")],
                          margins=[margin("0.12")], settle_method="VWAP_LAST_HOUR", settle_window="14:00-15:00",
                          big_side_group="CFFEX:INDEX", physical_delivery=False),
            "T": product("CFFEX", multiplier=10000, tick="0.005", limit_pct="0.02", fees=[lot_fee("3")],
                         margins=[margin("0.02")], settle_method="VWAP_LAST_HOUR", settle_window="14:15-15:15",
                         big_side_group="CFFEX:TREASURY"),
            "m": product("DCE", multiplier=10, tick="1", fees=[lot_fee("1.5")], margins=[margin("0.07")]),
        },
        [inst("IF2612", "IF", "20261218"), inst("T2612", "T", "20261211"), inst("m2701", "m", "20270115")],
    )


def tape(*rows: tuple[str, float, int]) -> list[tuple[int, float, int]]:
    return [(ts(f"2026-09-22 {w}"), p, q) for w, p, q in rows]


class TestHelpers:
    def test_vwap(self) -> None:
        assert vwap(D("79467000"), 60, 300).quantize(D("0.000001")) == D("4414.833333")  # type: ignore[union-attr]
        assert vwap(D("0"), 0, 300) is None

    def test_vwap_from_trades_window_is_inclusive(self) -> None:
        rows = tape(("13:59:59", 100.0, 1), ("14:00:00", 110.0, 1), ("15:00:00", 120.0, 2), ("15:00:01", 999.0, 1))
        vol, turn, avg = vwap_from_trades(rows, start_ns=ts("2026-09-22 14:00"), end_ns=ts("2026-09-22 15:00"))
        assert (vol, turn) == (3, D("350.0"))
        assert avg is not None and avg.quantize(D("0.0001")) == D("116.6667")

    def test_hourly_buckets(self) -> None:
        rows = tape(("13:05", 4400.0, 2), ("13:55", 4410.0, 1), ("14:30", 4420.0, 1))
        buckets = hourly_buckets(rows, 300)
        assert [(b.volume, b.turnover) for b in buckets] == [(3, D("3963000.0")), (1, D("1326000.0"))]
        assert buckets[0].start_ns == ts("2026-09-22 13:00")

    def test_build_settle_input(self, reg: InstrumentRegistry) -> None:
        inp = build_settle_input(reg, "T2612", TD, tape(("10:00", 101.0, 1), ("14:20", 101.5, 2)), pre_settle="101.2")
        assert inp.method == SettleMethod.VWAP_LAST_HOUR
        assert inp.window_start_ns == ts("2026-09-22 14:15")
        # 按交易小时分段（09:45–10:30 段、14:15–15:15 段）
        assert inp.hourly == {str(ts("2026-09-22 09:45")): ["1", "1010000"], str(ts("2026-09-22 14:15")): ["2", "2030000"]}
        assert (inp.day_volume, inp.day_turnover, inp.window_volume, inp.window_turnover) == (3, "3040000", 2, "2030000")
        assert (inp.vwap_raw, inp.last, inp.tick_size, inp.quality) == ("101.5", "101.5", "0.005", SettleInputQuality.OK)
        empty = build_settle_input(reg, "m2701", TD, [], pre_settle="3000")
        assert (empty.quality, empty.vwap_raw, empty.last, empty.window_volume) == (SettleInputQuality.NO_TRADE, None, None, None)


class TestPriority:
    def test_manual_beats_official_beats_vwap(self, reg: InstrumentRegistry) -> None:
        inp = build_settle_input(reg, "IF2612", TD, tape(("14:30", 4414.0, 3)), pre_settle="4400")
        assert determine_settlement_price(inp, reg).source == SettlePriceSource.VWAP
        off = determine_settlement_price(inp, reg, official=D("4415.0"))
        assert (off.price, off.source, off.rounding, off.vwap_raw) == ("4415", SettlePriceSource.OFFICIAL, Rounding.NONE, "4414")
        man = determine_settlement_price(inp, reg, official=D("4415.0"), manual=ManualPrice(D("4416.2"), "u", "r"))
        assert (man.price, man.source, man.operator, man.reason) == ("4416.2", SettlePriceSource.MANUAL, "u", "r")

    def test_manual_must_be_on_tick(self, reg: InstrumentRegistry) -> None:
        with pytest.raises(ValueError, match="tick"):
            determine_settlement_price(None, reg, instrument_id="IF2612", trading_day=TD, pre_settle=D("4400"),
                                       manual=ManualPrice(D("4416.3"), "u", "r"))

    def test_official_may_be_off_tick(self, reg: InstrumentRegistry) -> None:
        v = determine_settlement_price(None, reg, instrument_id="IF2612", trading_day=TD, official=D("4416.37"), version=3)
        assert (v.price, v.source, v.version, v.method, v.input_quality) == ("4416.37", SettlePriceSource.OFFICIAL, 3, SettleMethod.VWAP_LAST_HOUR, None)

    def test_ids_required_without_input(self, reg: InstrumentRegistry) -> None:
        with pytest.raises(ValueError):
            determine_settlement_price(None, reg, official=D("1"))
        with pytest.raises(ValueError, match="pre_settle"):
            determine_settlement_price(None, reg, instrument_id="m2701", trading_day=TD)

    def test_round_tick_for_dce(self, reg: InstrumentRegistry) -> None:
        inp = build_settle_input(reg, "m2701", TD, tape(("09:30", 3000.0, 1), ("10:30", 3001.0, 1)), pre_settle="2990")
        v = determine_settlement_price(inp, reg)
        assert (v.price, v.rounding, v.vwap_raw) == ("3001", Rounding.ROUND_TICK, "3000.5")  # 3000.5 四舍五入

    def test_vwap_raw_used_when_volumes_missing(self, reg: InstrumentRegistry) -> None:
        inp = SettleInput(trading_day=TD, instrument_id="IF2612", exchange="CFFEX", method=SettleMethod.VWAP_LAST_HOUR,
                          multiplier=300, tick_size="0.2", day_volume=5, day_turnover="6600000", vwap_raw="4414.83",
                          pre_settle="4400", computed_ns=0)
        assert determine_settlement_price(inp, reg).price == "4414.8"


class TestCffexFallback:
    def test_hourly_walks_back(self, reg: InstrumentRegistry) -> None:
        rows = tape(("10:10", 4390.0, 1), ("13:10", 4401.0, 1), ("13:20", 4402.0, 2))
        inp = build_settle_input(reg, "IF2612", TD, rows, pre_settle="4400")
        assert inp.window_volume == 0
        v = determine_settlement_price(inp, reg, hourly=hourly_buckets(rows, 300))
        assert (v.price, v.source, v.vwap_raw) == ("4401.6", SettlePriceSource.FALLBACK_HOURLY, "4401.666667")

    def test_hourly_accepts_json_mapping_and_tuples(self, reg: InstrumentRegistry) -> None:
        inp = build_settle_input(reg, "IF2612", TD, tape(("13:10", 4401.0, 1)), pre_settle="4400")
        start = ts("2026-09-22 13:00")
        as_json = {str(start): [1, "1320300"]}
        assert determine_settlement_price(inp, reg, hourly=as_json).price == "4401"
        assert determine_settlement_price(inp, reg, hourly=[(start, 1, "1320300")]).price == "4401"
        assert determine_settlement_price(inp, reg, hourly=[HourlyBucket(start, 1, D("1320300"))]).price == "4401"

    def test_hourly_attribute_on_input_is_picked_up(self, reg: InstrumentRegistry) -> None:
        import msgspec

        base = build_settle_input(reg, "IF2612", TD, tape(("13:10", 4401.0, 1)), pre_settle="4400")
        inp = msgspec.structs.replace(base, hourly={str(ts("2026-09-22 13:00")): ["1", "1320300"]})
        assert determine_settlement_price(inp, reg).source == SettlePriceSource.FALLBACK_HOURLY

    def test_fullday_when_no_hourly_data(self, reg: InstrumentRegistry) -> None:
        import msgspec

        inp = build_settle_input(reg, "IF2612", TD, tape(("10:10", 4390.0, 1), ("13:10", 4401.0, 1)), pre_settle="4400")
        inp = msgspec.structs.replace(inp, hourly=None)
        v = determine_settlement_price(inp, reg)
        assert (v.price, v.source) == ("4395.6", SettlePriceSource.FALLBACK_FULLDAY)  # 4395.5 -> 最近 tick 4395.6

    def test_benchmark_then_pre_settle(self, reg: InstrumentRegistry) -> None:
        inp = build_settle_input(reg, "IF2612", TD, [], pre_settle="4400")
        v = determine_settlement_price(inp, reg, benchmark=BenchmarkMove(settle=D("4431.3"), pre_settle=D("4410")))
        assert (v.price, v.source) == ("4421.4", SettlePriceSource.FALLBACK_BENCHMARK)  # 4400 + 21.3 -> 4421.4
        capped = determine_settlement_price(inp, reg, benchmark=BenchmarkMove(settle=D("6000"), pre_settle=D("4410")))
        assert capped.price == "4840"  # 受涨停价 4840.0 约束
        assert determine_settlement_price(inp, reg).source == SettlePriceSource.PRE_SETTLE

    def test_unknown_fallback_step_rejected(self) -> None:
        bad = make_registry(
            {"m": product("DCE", multiplier=10, tick="1", fees=[lot_fee("1")], margins=[margin("0.1")])},
            [inst("m2701", "m", "20270115")],
            rules={"DCE": {"settle_fallback": ["NOPE"]}},
        )
        with pytest.raises(ValueError, match="NOPE"):
            determine_settlement_price(build_settle_input(bad, "m2701", TD, [], pre_settle="3000"), bad)

    def test_pre_settle_appended_when_chain_omits_it(self) -> None:
        reg2 = make_registry(
            {"m": product("DCE", multiplier=10, tick="1", fees=[lot_fee("1")], margins=[margin("0.1")])},
            [inst("m2701", "m", "20270115")],
            rules={"DCE": {"settle_fallback": ["LIMIT"]}},
        )
        v = determine_settlement_price(build_settle_input(reg2, "m2701", TD, [], pre_settle="3000", limit_lock=LimitLock.NONE), reg2)
        assert (v.price, v.source) == ("3000", SettlePriceSource.PRE_SETTLE)



class TestTradingHours:
    """CFFEX trading-hour windows (结算细则：逐小时向前回溯；最后一笔成交距开盘不足一小时取全天均价)."""

    @staticmethod
    def _hm(windows: list[list[tuple[int, int]]]) -> list[list[tuple[str, str]]]:
        return [[(ns_to_bj(a).strftime("%H:%M"), ns_to_bj(b).strftime("%H:%M")) for a, b in w] for w in windows]

    def test_windows_walk_back_across_breaks(self, reg: InstrumentRegistry) -> None:
        assert self._hm(trading_hour_windows(reg, "IF2612", TD)) == [
            [("14:00", "15:00")],
            [("13:00", "14:00")],
            [("10:30", "11:30")],
            [("09:30", "10:30")],
        ]
        assert self._hm(trading_hour_windows(reg, "T2612", TD)) == [
            [("14:15", "15:15")],
            [("13:15", "14:15")],
            [("10:45", "11:30"), ("13:00", "13:15")],  # 跨午休的一小时
            [("09:45", "10:45")],
            [("09:30", "09:45")],
        ]
        assert first_hour_end_ns(reg, "T2612", TD) == ts("2026-09-22 10:30")
        segs = [(ns_to_bj(a).strftime("%H:%M"), ns_to_bj(b).strftime("%H:%M")) for a, b in trading_hour_segments(reg, "T2612", TD)]
        assert ("09:45", "10:30") in segs and ("10:30", "10:45") in segs

    def test_if_last_trade_in_first_hour_uses_whole_day_vwap(self, reg: InstrumentRegistry) -> None:
        # 最后一笔成交 10:15，距开盘不足一小时：取全天均价 4410，而不是 10 点那个钟点小时的 4420
        rows = tape(("09:40", 4400.0, 1), ("10:15", 4420.0, 1))
        inp = build_settle_input(reg, "IF2612", TD, rows, pre_settle="4390")
        v = determine_settlement_price(inp, reg)
        assert (v.price, v.source) == ("4410", SettlePriceSource.FALLBACK_FULLDAY)
        # 钟点小时的输入也按首小时规则判定；显式的最后成交时间同样生效
        assert determine_settlement_price(inp, reg, hourly=hourly_buckets(rows, 300)).price == "4410"
        assert determine_settlement_price(inp, reg, last_trade_ns=ts("2026-09-22 10:15")).price == "4410"

    def test_t_walks_back_to_the_13_15_hour(self, reg: InstrumentRegistry) -> None:
        # 最后一小时 [14:15,15:15) 无成交；前一交易小时 [13:15,14:15) 只有 13:30 那笔 → 101.2（钟点小时会混入 13:05 得到 101.02）
        rows = tape(("13:05", 101.0, 9), ("13:30", 101.2, 1))
        inp = build_settle_input(reg, "T2612", TD, rows, pre_settle="101.1")
        v = determine_settlement_price(inp, reg)
        assert (v.price, v.source) == ("101.2", SettlePriceSource.FALLBACK_HOURLY)
        clock = determine_settlement_price(inp, reg, hourly=hourly_buckets(rows, 10000))
        assert clock.price == "101.02"  # 钟点小时分桶只能近似（见模块说明）

    def test_hour_spanning_the_lunch_break(self, reg: InstrumentRegistry) -> None:
        rows = tape(("11:00", 101.0, 1), ("13:05", 101.1, 1))
        inp = build_settle_input(reg, "T2612", TD, rows, pre_settle="101.1")
        v = determine_settlement_price(inp, reg)
        assert (v.price, v.source) == ("101.05", SettlePriceSource.FALLBACK_HOURLY)

    def test_trading_hour_buckets_edges(self, reg: InstrumentRegistry) -> None:
        rows = tape(("09:26", 4400.0, 1), ("15:00", 4410.0, 2))  # 集合竞价、收盘时刻
        buckets = trading_hour_buckets(reg, "IF2612", TD, rows)
        assert [(ns_to_bj(b.start_ns).strftime("%H:%M"), b.volume) for b in buckets] == [("09:30", 1), ("14:00", 2)]
