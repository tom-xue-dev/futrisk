"""Strategy logic as pure functions of synthetic tick sequences (01 §7.1, §8.14 sizing limits)."""

from __future__ import annotations

import math

import pytest
from botstest_support import T0, S, bars, context, expected_book, feed, tick

from futrisk.client.positions import PositionTracker
from futrisk.client.pricing import ContractTable
from futrisk.messages import codes
from futrisk.messages.enums import PosDirection
from futrisk.sim.bots.gates import Gates
from futrisk.sim.bots.strategies import (
    BreakoutParams,
    CalendarSpread,
    ChannelBreakout,
    CounterTrendAdd,
    CounterTrendParams,
    IndexDirectional,
    IndexParams,
    MultiTrend,
    SpreadParams,
    TrendParams,
    make_strategy,
)

LONG = PosDirection.LONG


class TestChannelBreakout:
    def make(self) -> ChannelBreakout:
        return ChannelBreakout(BreakoutParams(instruments=["rb2701"], bar_s=30, entry_n=5, exit_n=3), account_id="ACC1")

    def test_breakout_long_exit_and_short(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        s, ctx = self.make(), context("ACC1", contracts, tracker)
        flat = [3100.0, 3110.0, 3105.0, 3102.0, 3108.0, 3104.0]
        assert feed(s, ctx, bars("rb2701", flat)) == {"rb2701": 0}  # warm, no breakout yet
        # close above the 5-bar high -> long 12% of equity, clipped by the rb cap (01 §8.14)
        t = feed(s, ctx, [tick("rb2701", 3150.0, 185)])
        assert t == {"rb2701": math.floor(0.12 * 10_000_000 / (3150 * 10))} == {"rb2701": 38}
        # holds while the price stays above the 3-bar exit channel
        t = feed(s, ctx, bars("rb2701", [3150.0, 3160.0, 3155.0], start_s=210))
        assert t == {"rb2701": 38}
        # below the 3-bar low -> exit
        assert feed(s, ctx, [tick("rb2701", 3140.0, 305)]) == {"rb2701": 0}
        # below the 5-bar low -> short
        t = feed(s, ctx, bars("rb2701", [3140.0, 3120.0, 3110.0, 3100.0], start_s=330) + [tick("rb2701", 3050.0, 460)])
        assert t["rb2701"] < 0

    def test_adopts_existing_position_after_restart(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        tracker.set_leg("ACC1", "rb2701", LONG, yd=20)
        s, ctx = self.make(), context("ACC1", contracts, tracker)
        assert feed(s, ctx, bars("rb2701", [3100.0] * 3)) == {}  # not warm: hold
        assert feed(s, ctx, bars("rb2701", [3100.0] * 3, start_s=90)) == {"rb2701": 20}


class TestMultiTrend:
    def test_crossover_long_then_short(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        s = MultiTrend(TrendParams(instruments=["cu2611", "T2612"], bar_s=30, fast=3, slow=6, band=0.0),
                       account_id="ACC2")
        ctx = context("ACC2", contracts, tracker, equity=20_000_000.0)
        up = [78000.0 + 100 * i for i in range(8)]
        t = feed(s, ctx, bars("cu2611", up))
        assert t == {"cu2611": math.floor(0.10 * 20_000_000 / (78600 * 5))}  # 5 lots
        down = [78700.0 - 150 * i for i in range(8)]
        t = feed(s, ctx, bars("cu2611", down, start_s=240))
        assert t["cu2611"] < 0
        # T: one contract is 1.08M -> at least one lot under its 150% cap
        t = feed(s, ctx, bars("T2612", [108.0 + 0.01 * i for i in range(8)]))
        assert t["T2612"] == 1


class TestCounterTrendAdd:
    PX = {"rb2701": 3120.0, "hc2701": 3260.0, "i2701": 780.0}
    W = CounterTrendParams().weights

    def book(self, contracts: ContractTable, frac: float = 1.0) -> dict[str, int]:
        return expected_book(contracts, self.W, self.PX, frac=frac)

    def warm(self, s: CounterTrendAdd, ctx, t_s: float = 0.0, px: dict[str, float] | None = None) -> dict[str, int]:
        prices = px or self.PX
        return feed(s, ctx, [tick(i, p, t_s + 0.1 * k) for k, (i, p) in enumerate(prices.items())])

    def test_builds_base_book_in_steps(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        s, ctx = CounterTrendAdd(CounterTrendParams(), account_id="ACC3"), context("ACC3", contracts, tracker)
        first = self.warm(s, ctx)
        assert first == self.book(contracts, 0.2) == {"rb2701": 25, "hc2701": 10, "i2701": 5}  # step 1 of 5
        seen = [first]
        for t_s in (60, 120, 180, 240):
            seen.append(self.warm(s, ctx, t_s))
        full = seen[-1]
        assert full == self.book(contracts) == {"rb2701": 125, "hc2701": 52, "i2701": 28}
        notional = sum(full[i] * self.PX[i] * contracts.get(i).multiplier for i in full)
        # ~0.78x equity long book: below the 0.8 black-sector warn line until prices fall
        assert notional == pytest.approx(sum(self.W.values()) * 10_000_000, rel=0.01)
        assert notional < 0.8 * 10_000_000
        for i, n in full.items():  # every product stays under 90% of its cap (rb 60%, hc 60%, i 50%)
            assert n * self.PX[i] * contracts.get(i).multiplier <= 0.9 * contracts.get(i).cap_frac("20260928") * 1e7
        assert [x["rb2701"] for x in seen] == sorted(x["rb2701"] for x in seen)  # monotone build

    def test_aggressive_builds_at_once(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        s = CounterTrendAdd(CounterTrendParams(aggressive=True), account_id="ACC3")
        assert self.warm(s, context("ACC3", contracts, tracker)) == self.book(contracts)

    def complete(self, contracts: ContractTable, tracker: PositionTracker, **kw: object) -> tuple[CounterTrendAdd, object]:
        s = CounterTrendAdd(CounterTrendParams(aggressive=True, **kw), account_id="ACC3")  # type: ignore[arg-type]
        ctx = context("ACC3", contracts, tracker)
        base = self.warm(s, ctx)
        for i, n in base.items():
            tracker.set_leg("ACC3", i, LONG, td=n, avg_price=self.PX[i])
        self.warm(s, ctx, 1.0)
        assert s.build_done and s.ref == self.PX
        return s, ctx

    def test_adds_on_dips_within_caps_and_sells_back(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        s, ctx = self.complete(contracts, tracker)
        base = self.book(contracts)
        p = CounterTrendParams()
        assert (p.dip_step, p.add_frac, p.max_adds) == (0.005, 0.0625, 8)  # +50% spread down to -4%
        t = feed(s, ctx, [tick("rb2701", 3104.0, 5)])  # -0.51%: one ladder level
        assert s.adds["rb2701"] == 1 and t["rb2701"] == math.floor(base["rb2701"] * 1.0625) == 132
        assert t["hc2701"] == base["hc2701"]
        t = feed(s, ctx, [tick("rb2701", 3050.0, 6)])  # -2.24%: four levels, the adds go on below -2%
        assert s.adds["rb2701"] == 4 and t["rb2701"] == math.floor(base["rb2701"] * 1.25) == 156
        t = feed(s, ctx, [tick("rb2701", 2990.0, 6.5)])  # -4.17%: all 8 levels (x1.5 = 187), clipped by the rb cap
        cap = math.floor(0.9 * 0.6 * 10_000_000 / (2990 * 10))
        assert s.adds["rb2701"] == 8 and t["rb2701"] == min(math.floor(base["rb2701"] * 1.5), cap) == cap == 180
        t = feed(s, ctx, [tick("rb2701", 3100.0, 7)])  # rebound to -0.64%: hysteresis keeps level 1
        assert s.adds["rb2701"] == 1
        t = feed(s, ctx, [tick("rb2701", 3114.0, 8)])  # back near the reference: sell the adds back
        assert s.adds["rb2701"] == 0 and t["rb2701"] == base["rb2701"]

    def test_sector_cap_scales_adds(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        s, ctx = self.complete(contracts, tracker)
        shock = {i: p * 0.958 for i, p in self.PX.items()}  # black sector -4.2%: every ladder at max
        t = feed(s, ctx, [tick(i, round(p / float(contracts.get(i).tick_size)) * float(contracts.get(i).tick_size),
                               9 + k * 0.1) for k, (i, p) in enumerate(shock.items())])
        assert all(s.adds[i] == 8 for i in self.PX)
        total = sum(t[i] * ctx.price(i) * contracts.get(i).multiplier for i in t)
        assert total <= 1.1 * 10_000_000 + max(ctx.price(i) * contracts.get(i).multiplier for i in t)
        assert all(t[i] >= s.base[i] for i in t)  # the base book itself is never cut

    def test_halted_trims_once_then_probes(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        s, ctx = self.complete(contracts, tracker)
        base = self.book(contracts)
        ctx.gates.mark_halted(ctx.now_ns)
        t = feed(s, ctx, [tick("rb2701", 3120.0, 10)])
        assert t["rb2701"] == base["rb2701"]  # within the trim delay: nothing changes
        t = feed(s, ctx, [tick("rb2701", 3120.0, 35)])  # 30 s later (latched by then): trim 10%
        trimmed = {i: math.floor(n * 0.9) for i, n in base.items()}
        assert t == trimmed == {"rb2701": 112, "hc2701": 46, "i2701": 25}
        for i, n in t.items():
            tracker.set_leg("ACC3", i, LONG, td=n)
        t = feed(s, ctx, [tick("rb2701", 3120.0, 40)])
        assert t == trimmed  # the trimmed book stays the ceiling: the lots are not bought back
        t = feed(s, ctx, [tick("rb2701", 3050.0, 90)])
        assert t == trimmed and s.adds["rb2701"] > 0  # the ladder may want more; opens are the 1-lot halt probes
        ctx.gates.clear_halted()  # risk.reset: still no rebuild today
        assert s.targets(ctx) == trimmed
        tracker.roll_to("20260929")  # the day roll ends the trim; the new day's build restores the weights
        t = feed(s, ctx, [tick(i, p, 3600 + k, trading_day="20260929") for k, (i, p) in enumerate(self.PX.items())])
        assert not s.trim and s.trim_episode is None and t == base

    def test_realloc_when_a_product_is_blocked(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        gates = Gates("ACC3")
        s = CounterTrendAdd(CounterTrendParams(aggressive=True), account_id="ACC3")
        ctx = context("ACC3", contracts, tracker, gates=gates)
        self.warm(s, ctx)
        tracker.set_leg("ACC3", "rb2701", LONG, td=30)
        for _ in range(2):  # e.g. concentration: block_open_largest_product, twice
            gates.on_reject(codes.REJECT_PRODUCT_BLOCKED, product="rb", instrument_id="rb2701", is_open=True,
                            now_ns=ctx.now_ns)
        t = self.warm(s, ctx, 1.0)
        assert "rb2701" in s.abandoned and t["rb2701"] == 30
        base = self.book(contracts)
        assert t["hc2701"] > base["hc2701"] and t["i2701"] > base["i2701"]  # budget moved to the other products ...
        for i in ("hc2701", "i2701"):  # ... but never beyond their caps
            assert t[i] * self.PX[i] * contracts.get(i).multiplier <= 0.9 * contracts.get(i).cap_frac("20260928") * 1e7
        total = sum(t[i] * self.PX[i] * contracts.get(i).multiplier for i in t)
        assert total == pytest.approx(sum(self.W.values()) * 1e7, rel=0.02)  # same gross budget

    def test_day_roll_keeps_carried_book(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        s, ctx = self.complete(contracts, tracker)
        tracker.roll_to("20260929")
        t = feed(s, ctx, [tick(i, p, 3600 + k, trading_day="20260929") for k, (i, p) in enumerate(self.PX.items())])
        assert s.day == "20260929" and t == self.book(contracts)
        assert s.build_done and s.adds == {"rb2701": 0, "hc2701": 0, "i2701": 0}  # carried book: complete at once


class TestIndexDirectional:
    def run(self, params: IndexParams, contracts: ContractTable, tracker: PositionTracker, drift: float) -> dict[str, int]:
        s = IndexDirectional(params, account_id="ACC4")
        ctx = context("ACC4", contracts, tracker)
        ticks = []
        for k in range(8):
            ticks += bars("IF2612", [4414.8 * (1 + drift * k)], start_s=15 * k, bar_s=15, per_bar=1)
            ticks += bars("IC2612", [6500.0 * (1 + drift * k)], start_s=15 * k + 1, bar_s=15, per_bar=1)
        ticks += [tick("IF2612", 4414.8 * (1 + drift * 8), 125), tick("IC2612", 6500.0 * (1 + drift * 8), 126)]
        return feed(s, ctx, [t for t in ticks])

    def test_direction_and_caps(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        p = IndexParams(bar_s=15, fast=2, slow=4, band=0.0001)
        up = self.run(p, contracts, tracker, 0.002)
        assert up == {"IF2612": 2, "IC2612": 2}  # IF/IC capped at 90% of 30% of equity each
        down = self.run(p, contracts, PositionTracker(contracts, "20260928"), -0.002)
        assert down == {"IF2612": -2, "IC2612": -2}

    def test_leverage_target_without_caps(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        # the defaults (0.3-0.55x) sit under the IF/IC caps; a 1.5-2.5x target shows the sizing when caps are off
        p = IndexParams(bar_s=15, fast=2, slow=4, band=0.0001, leverage_min=1.5, leverage_max=2.5,
                        respect_product_cap=False)
        t = self.run(p, contracts, tracker, 0.002)
        per_lot = {"IF2612": 4414.8 * 1.016 * 300, "IC2612": 6500.0 * 1.016 * 200}
        lev = sum(t[i] * per_lot[i] for i in t) / 1e7
        assert 1.5 * 0.85 <= lev <= 2.5 and all(v > 2 for v in t.values())

    def test_default_leverage_stays_under_the_caps(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        p = IndexParams(bar_s=15, fast=2, slow=4, band=0.0001)
        capped = self.run(p, contracts, tracker, 0.002)
        free = self.run(IndexParams(bar_s=15, fast=2, slow=4, band=0.0001, respect_product_cap=False), contracts,
                        PositionTracker(contracts, "20260928"), 0.002)
        assert capped == free  # ACC4's default target never needs the cap clip (no REJECT_EXPOSURE_CAP noise)


class TestCalendarSpread:
    def test_two_sided_book_entries_and_exits(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        s = CalendarSpread(SpreadParams(sample_s=5, window=10, min_samples=5, entry_z=1.5, exit_z=0.3), account_id="ACC5")
        ctx = context("ACC5", contracts, tracker, equity=5_000_000.0)

        def sample(k: int, spread: float) -> dict[str, int]:
            near = 3120.0
            return feed(s, ctx, [tick("rb2701", near, 5 * k, bid=near - 1, ask=near + 1),
                                 tick("rb2705", near + spread, 5 * k + 0.5, bid=near + spread - 1, ask=near + spread + 1)])

        assert sample(0, 30) == {"rb2701": 20, "rb2705": -20}  # base book opened at once, two-sided
        for k, sp in enumerate([31, 29, 30, 31, 29, 30], start=1):
            t = sample(k, sp)
        assert t == {"rb2701": 20, "rb2705": -20}
        t = sample(7, 40)  # spread rich: long near / short far, bigger
        assert t["rb2701"] > 20 and t["rb2705"] == -t["rb2701"] and s.side == 1
        t = sample(8, 15)  # spread cheap: reverse
        assert s.side == -1 and t["rb2701"] < 0 and t["rb2705"] == -t["rb2701"] and abs(t["rb2701"]) <= 60
        for k in range(9, 30):
            t = sample(k, 30)
        assert abs(t["rb2701"]) == 20 and t["rb2705"] == -t["rb2701"]  # reverted: back to the base, side kept

    def test_adopts_existing_book(self, contracts: ContractTable, tracker: PositionTracker) -> None:
        tracker.set_leg("ACC5", "rb2701", PosDirection.SHORT, yd=30)
        s = CalendarSpread(SpreadParams(), account_id="ACC5")
        t = feed(s, context("ACC5", contracts, tracker, equity=5e6), [tick("rb2701", 3120.0, 0), tick("rb2705", 3150.0, 0.5)])
        assert t == {"rb2701": -30, "rb2705": 30}


def test_registry_validates_params() -> None:
    import random

    s = make_strategy("counter_trend_add", {"build_s": 0}, account_id="ACC3", rng=random.Random(1))
    assert isinstance(s, CounterTrendAdd) and s.instruments == ("rb2701", "hc2701", "i2701")
    with pytest.raises(KeyError):
        make_strategy("nope", {}, account_id="ACC1", rng=random.Random(1))
    with pytest.raises(ValueError):
        make_strategy("multi_trend", {"fast": 30, "slow": 10}, account_id="ACC2", rng=random.Random(1))
    with pytest.raises(ValueError):
        make_strategy("channel_breakout", {"unknown_param": 1}, account_id="ACC1", rng=random.Random(1))
    assert T0 > 0 and S == 1_000_000_000
