"""Position tracker and pricing helpers (01 §8.15 开平规则; 02 §7.3 Offset, §11.2 steps 1/3/6): pure tests."""

from __future__ import annotations

from decimal import Decimal

import msgspec
import pytest

from futrisk.client.positions import PendingCloses, PositionTracker
from futrisk.client.pricing import ContractTable, aggressive_price, on_tick_grid, passive_price, to_tick
from futrisk.client.testing import make_tick
from futrisk.common.config import FullConfig
from futrisk.messages.enums import Direction, Exchange, Offset, PosDirection
from futrisk.messages.risk import PositionView

BUY, SELL = Direction.BUY, Direction.SELL
LONG, SHORT = PosDirection.LONG, PosDirection.SHORT
T0 = 1_790_559_300_000_000_000


@pytest.fixture
def contracts(default_config: FullConfig) -> ContractTable:
    return ContractTable(default_config.products)


@pytest.fixture
def tracker(contracts: ContractTable) -> PositionTracker:
    return PositionTracker(contracts, trading_day="20260928")


class TestContracts:
    def test_rules_and_caps(self, contracts: ContractTable) -> None:
        rb, i, if_ = contracts.get("rb2701"), contracts.get("i2701"), contracts.get("IF2612")
        assert (rb.exchange, rb.multiplier, rb.tick_size, rb.explicit_close_today) == (Exchange.SHFE, 10, Decimal("1"), True)
        assert (i.exchange, i.explicit_close_today, i.close_style) == (Exchange.DCE, False, "FIFO")
        assert (if_.close_style, if_.max_limit_qty) == ("TODAY_FIRST", 20)
        # 01 §8.14: cap = 3% / limit_pct -> rb 60%, IF 30%, i 50% then 37.5% after the holiday widening
        assert rb.cap_frac("20260928") == pytest.approx(0.6)
        assert if_.cap_frac("20260928") == pytest.approx(0.3)
        assert i.cap_frac("20260928") == pytest.approx(0.5)
        assert i.cap_frac("20260930") == pytest.approx(0.375)
        assert rb.notional(3120.0, 2) == pytest.approx(62_400.0)
        assert not contracts.has("zz9999") and contracts.has("rb2705")


class TestPricing:
    def test_aggressive_crosses_by_ticks(self, contracts: ContractTable) -> None:
        t = make_tick("rb2701", 3120.0, ts_ns=T0, bid=3119.0, ask=3121.0)
        assert aggressive_price(t, BUY, 2, contracts.get("rb2701").tick_size) == 3123.0
        assert aggressive_price(t, SELL, 2, contracts.get("rb2701").tick_size) == 3117.0

    def test_if_tick_grid_is_exact(self, contracts: ContractTable) -> None:
        tick_size = contracts.get("IF2612").tick_size
        t = make_tick("IF2612", 4414.8, ts_ns=T0, bid=4414.6, ask=4414.8)
        px = aggressive_price(t, BUY, 2, tick_size)
        assert px == 4415.2 and on_tick_grid(px, tick_size)
        assert on_tick_grid(aggressive_price(t, SELL, 3, tick_size) or 0, tick_size)
        assert to_tick(4414.83, tick_size, "up") == 4415.0 and to_tick(4414.83, tick_size, "down") == 4414.8

    def test_clamped_to_limits_and_fallbacks(self, contracts: ContractTable) -> None:
        ts = contracts.get("rb2701").tick_size
        up_locked = msgspec.structs.replace(
            make_tick("rb2701", 3276.0, ts_ns=T0, bid=3276.0, upper=3276.0, lower=2964.0), ask1=None)
        assert aggressive_price(up_locked, BUY, 5, ts) == 3276.0  # never above the upper limit
        near_low = make_tick("rb2701", 2965.0, ts_ns=T0, bid=2965.0, ask=2966.0, upper=3276.0, lower=2964.0)
        assert aggressive_price(near_low, SELL, 5, ts) == 2964.0
        assert passive_price(near_low, BUY, ts) == 2965.0 and passive_price(near_low, SELL, ts) == 2966.0


class TestTracker:
    def test_shfe_today_yesterday_offsets(self, tracker: PositionTracker) -> None:
        tracker.apply_fill("ACC3", "rb2701", BUY, Offset.OPEN, 5, 3120.0)
        assert (tracker.leg("ACC3", "rb2701", LONG).td, tracker.leg("ACC3", "rb2701", LONG).yd) == (5, 0)
        assert tracker.roll_to("20260929")
        tracker.apply_fill("ACC3", "rb2701", BUY, Offset.OPEN, 3, 3100.0, trading_day="20260929")
        leg = tracker.leg("ACC3", "rb2701", LONG)
        assert (leg.td, leg.yd, leg.total) == (3, 5, 8)
        assert leg.avg_price == pytest.approx((5 * 3120 + 3 * 3100) / 8)
        # SHFE must name today/yesterday explicitly (01 §8.15): yesterday first by default
        assert tracker.close_plan("ACC3", "rb2701", LONG, 8) == [(Offset.CLOSE_YESTERDAY, 5), (Offset.CLOSE_TODAY, 3)]
        assert tracker.close_plan("ACC3", "rb2701", LONG, 6, prefer_today=True) == [
            (Offset.CLOSE_TODAY, 3), (Offset.CLOSE_YESTERDAY, 3)]
        # working closes are deducted (02 §11.2 step 6 counts frozen closes)
        assert tracker.close_plan("ACC3", "rb2701", LONG, 8, pending=PendingCloses(yd=2, td=1)) == [
            (Offset.CLOSE_YESTERDAY, 3), (Offset.CLOSE_TODAY, 2)]
        assert tracker.close_plan("ACC3", "rb2701", SHORT, 1) == []
        tracker.apply_fill("ACC3", "rb2701", SELL, Offset.CLOSE_TODAY, 2)
        tracker.apply_fill("ACC3", "rb2701", SELL, Offset.CLOSE_YESTERDAY, 5)
        assert (tracker.leg("ACC3", "rb2701", LONG).td, tracker.leg("ACC3", "rb2701", LONG).yd) == (1, 0)
        assert "ACC3" not in tracker.drift_accounts
        tracker.apply_fill("ACC3", "rb2701", SELL, Offset.CLOSE_TODAY, 2)  # one more than held
        assert tracker.net("ACC3", "rb2701") == 0 and "ACC3" in tracker.drift_accounts

    def test_other_exchanges_close_style(self, tracker: PositionTracker) -> None:
        tracker.set_leg("ACC1", "i2701", LONG, td=3, yd=2)
        assert tracker.close_plan("ACC1", "i2701", LONG, 4) == [(Offset.CLOSE, 4)]
        tracker.apply_fill("ACC1", "i2701", SELL, Offset.CLOSE, 3)  # DCE FIFO: yesterday first
        assert (tracker.leg("ACC1", "i2701", LONG).td, tracker.leg("ACC1", "i2701", LONG).yd) == (2, 0)
        tracker.set_leg("ACC4", "IF2612", SHORT, td=3, yd=2)
        tracker.apply_fill("ACC4", "IF2612", BUY, Offset.CLOSE, 3)  # CFFEX: today's opens first
        assert (tracker.leg("ACC4", "IF2612", SHORT).td, tracker.leg("ACC4", "IF2612", SHORT).yd) == (0, 2)
        assert tracker.net("ACC4", "IF2612") == -2
        # a CLOSE_TODAY sent to DCE is folded into CLOSE by the risk-engine; the tracker still copes
        tracker.apply_fill("ACC1", "i2701", SELL, Offset.CLOSE_TODAY, 1)
        assert tracker.leg("ACC1", "i2701", LONG).total == 1

    def test_roll_and_seed(self, tracker: PositionTracker) -> None:
        tracker.apply_fill("ACC5", "rb2705", SELL, Offset.OPEN, 4, 3150.0)
        assert not tracker.roll_to("20260928") and not tracker.roll_to("20260927")
        assert tracker.roll_to("20260929")
        assert tracker.snapshot("ACC5") == {"rb2705": {"SHORT": (0, 4)}}
        tracker.seed("ACC5", [
            PositionView(account_id="ACC5", instrument_id="rb2701", product="rb", exchange=Exchange.SHFE,
                         direction=LONG, td_qty=2, yd_qty=7, closable_qty=9, closable_td_qty=2, closable_yd_qty=7,
                         avg_open_price=3111.0, basis_avg_price=3100.0),
            PositionView(account_id="ACC9", instrument_id="rb2701", product="rb", exchange=Exchange.SHFE,
                         direction=LONG, td_qty=1, yd_qty=0, closable_qty=1, closable_td_qty=1, closable_yd_qty=0,
                         avg_open_price=1.0, basis_avg_price=1.0),
        ])
        assert tracker.snapshot("ACC5") == {"rb2701": {"LONG": (2, 7)}}  # replaced, other accounts ignored
        assert tracker.leg("ACC5", "rb2701", LONG).avg_price == 3111.0
        assert tracker.instruments("ACC5") == ["rb2701"]
