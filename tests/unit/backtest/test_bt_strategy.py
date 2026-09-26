"""The dual moving-average strategy: signal, hysteresis, roll rescaling, sizing, and what the breaker blocks."""

from __future__ import annotations

import pytest
from btsupport import DaySpec, PlanSource, base

from futrisk.backtest.config import AccountRun, ArmSpec, BacktestConfig
from futrisk.backtest.data import DailyBar
from futrisk.backtest.replay import ArmResult, Replay
from futrisk.backtest.strategies import DualMA, DualMAParams, Quote, StrategyContext, make_strategy
from futrisk.core.instruments import InstrumentRegistry
from futrisk.messages import codes

RB = "rb2701"


def _bar(close: float, td: str = "20260921", factor: float = 1.0) -> DailyBar:
    return DailyBar(td, RB, close, close, close, close, close, factor, factor != 1.0)


def _ma(**kw: float) -> DualMA:
    return DualMA(DualMAParams(**kw), account_id="ACC3", products=["rb"])  # type: ignore[arg-type]


def test_signal_needs_history_and_uses_the_live_price() -> None:
    s = _ma(fast=2, slow=4, band=0.0)
    assert s.signal("rb", 110.0) == 0  # no history yet: the current regime
    for c in (100.0, 100.0, 100.0):
        s.on_bar("rb", _bar(c))
    assert s.signal("rb", 110.0) == 1  # fast (100 + 110) / 2 = 105 > slow 102.5
    assert s.signal("rb", 90.0) == -1
    flat = DualMA(DualMAParams(fast=2, slow=4, band=0.0, allow_short=False), account_id="A", products=["rb"])
    for c in (100.0, 100.0, 100.0):
        flat.on_bar("rb", _bar(c))
    assert flat.signal("rb", 90.0) == 0


def test_band_holds_the_regime_and_a_roll_rescales_the_history() -> None:
    s = _ma(fast=2, slow=4, band=0.01)
    for c in (100.0, 100.0, 100.0):
        s.on_bar("rb", _bar(c))
    assert s.signal("rb", 101.0) == 0  # fast 100.5 is inside slow 100.25 ± 1 %
    s.on_roll("rb", 1.1)  # the new main trades 10 % higher: the history moves with it, no false cross
    assert list(s.closes["rb"]) == pytest.approx([110.0, 110.0, 110.0])
    assert s.signal("rb", 110.0) == 0


def test_targets_size_on_the_initial_capital_within_the_product_cap() -> None:
    reg = InstrumentRegistry(base().products, base().calendar)
    s = make_strategy("dual_ma", {"fast": 2, "slow": 4, "band": 0.0, "notional_frac": 0.15}, account_id="ACC3",
                      products=["rb"])
    assert isinstance(s, DualMA)
    for c in (2900.0, 2900.0, 2900.0):
        s.on_bar("rb", _bar(c))
    ctx = StrategyContext(account_id="ACC3", trading_day="20260929", now_ns=0, equity=10_000_000.0, registry=reg,
                          quotes={"rb": Quote("rb", RB, 3000.0, 3000.0, True, True)}, positions={})
    assert s.targets(ctx) == {RB: 50}  # 15 % of 10M / (3000 x 10) = 50 lots, under the cap 0.85 x 3 % / 5 %
    with pytest.raises(KeyError):
        make_strategy("channel_breakout", {}, account_id="A", products=["rb"])
    with pytest.raises(ValueError):
        _ma(fast=4, slow=4)


def _flip_day() -> list[DaySpec]:
    """A long book of 16 rb lots (48 % of 1M) falls through the whole ladder, then the averages cross down."""
    return [DaySpec(
        "20260922",
        states=[("2026-09-22 09:30", RB, 3000.0), ("2026-09-22 09:40", RB, 2900.0), ("2026-09-22 09:50", RB, 2840.0),
                ("2026-09-22 10:00", RB, 2810.0), ("2026-09-22 10:00:00.5", RB, 2808.0),
                ("2026-09-22 10:10", RB, 2780.0), ("2026-09-22 10:20", RB, 2790.0)],
        settle={RB: 2790.0},
        pre_settle={RB: 3000.0},
    )]


def _run(tier: str) -> ArmResult:
    cfg = BacktestConfig(name="t", accounts={"ACC3": AccountRun(capital=1_000_000)},
                         arms=[ArmSpec(name=tier, tier=tier)], workers=1)
    strat = _ma(fast=1, slow=2, band=0.002, notional_frac=0.5)
    strat.on_bar("rb", _bar(2800.0, td="20260921"))  # yesterday's close: the day starts in an up regime
    rep = Replay(cfg=cfg, arm=cfg.arms[0], source=PlanSource(_flip_day()), base_rules=base().risk_rules,
                 base_accounts=base().accounts, strategies={"ACC3": strat}, label=f"flip-{tier}")
    return rep.run()


def test_after_the_latch_the_flip_closes_but_cannot_open_the_other_side() -> None:
    free, ruled = _run("none"), _run("block_open")
    moves = [(o["direction"], o["offset"], o["qty"], o["ok"]) for o in free.orders]
    # the flip re-sizes at the current price: 50 % of 1M / (2780 x 10) = 17 lots short
    assert moves == [("BUY", "OPEN", 16, True), ("SELL", "CLOSE_TODAY", 16, True), ("SELL", "OPEN", 17, True)]
    moves = [(o["direction"], o["offset"], o["qty"], o["ok"], o["code"]) for o in ruled.orders]
    assert moves == [("BUY", "OPEN", 16, True, codes.OK), ("SELL", "CLOSE_TODAY", 16, True, codes.OK),
                     ("SELL", "OPEN", 17, False, codes.REJECT_ACCOUNT_HALTED)]
    raised = [(e["level"], e["latched"]) for e in ruled.events if e["kind"] == "ALERT_RAISED"]
    assert raised == [("warn", False), ("restrict", False), ("halt", True)]
    (row,) = ruled.daily
    assert row["halt_latched"] and row["blocked_opens"] == 1 and row["trig_halt"]
    # the short the rule blocked lost (2790 - 2780) x 10 x 17 = 1700 plus its fees in the free arm
    assert free.daily[0]["day_pnl"] < ruled.daily[0]["day_pnl"] - 1700
