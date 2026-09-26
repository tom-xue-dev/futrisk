"""Layer C: a synthetic recording written with the recorder's shard writer, replayed and settled with 01 §8.15."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from btsupport import base, ts

from futrisk.backtest.config import AccountRun, ArmSpec, BacktestConfig
from futrisk.backtest.runner import run_config
from futrisk.backtest.sources import TickSource
from futrisk.common.clock import SimClock
from futrisk.core.instruments import InstrumentRegistry
from futrisk.marketdata.recorder import ParquetShardWriter
from futrisk.marketdata.tickschema import TICK_SCHEMA, tick_row
from futrisk.messages.enums import SettlePriceSource, SourceId, TickMode
from futrisk.messages.md import Tick

TD = "20260929"


def record(home: Path) -> None:
    reg = InstrumentRegistry(base().products, base().calendar)
    w = ParquetShardWriter(home / "ticks" / "raw", "sina", TICK_SCHEMA, prefix="part", epoch_ms=1, clock=SimClock(0),
                           roll_s=1e9)
    tape = {
        # rb2701 (SHFE): 10 @3100 (night), 20 @3110, 10 @3095 -> full-day VWAP 3103.75, floored to 3103
        "rb2701": [("2026-09-28 21:00:05", 3100.0, 10), ("2026-09-29 09:05", 3110.0, 20), ("2026-09-29 14:55", 3095.0, 10)],
        # IF2612 (CFFEX): last hour 14:00-15:00 = 1 @4530 + 3 @4510 -> 4515.0; the whole-day VWAP would be 4512.5
        "IF2612": [("2026-09-29 09:35", 4500.0, 2), ("2026-09-29 13:55", 4520.0, 2), ("2026-09-29 14:05", 4530.0, 1),
                   ("2026-09-29 14:50", 4510.0, 3)],
    }
    pre = {"rb2701": 3100.0, "IF2612": 4500.0}
    rejected = Tick(instrument_id="rb2701", exchange=reg.exchange("rb2701"), trading_day=TD, action_day="20260929",
                    exch_ts_ns=ts("2026-09-29 09:06"), last=9999.0, volume=1, turnover=0.0, open_interest=0,
                    upper_limit=3255.0, lower_limit=2945.0, pre_settle=3100.0, source=SourceId.SINA, recv_ns=ts("2026-09-29 09:06"))
    for inst, rows in tape.items():
        vol, turn = 0, 0.0
        mult = reg.multiplier(inst)
        up, lo = reg.limit_prices(inst, Decimal(str(pre[inst])), TD)
        for t, px, q in rows:
            vol += q
            turn += px * q * mult
            tick = Tick(instrument_id=inst, exchange=reg.exchange(inst), trading_day=TD, action_day=t[:10].replace("-", ""),
                        exch_ts_ns=ts(t), last=px, volume=vol, turnover=turn, open_interest=100, bid1=px - 1, ask1=px + 1,
                        upper_limit=float(up), lower_limit=float(lo), pre_settle=pre[inst], source=SourceId.SINA, recv_ns=ts(t))
            w.append(TD, tick_row(tick, TickMode.LIVE))
    w.append(TD, tick_row(rejected, TickMode.REJECTED, "MD_REJECT_PRICE_LIMIT"))
    w.close()


def test_recorded_ticks_replay_and_settle_with_the_exchange_rules(tmp_path: Path) -> None:
    record(tmp_path)
    src = TickSource(tmp_path / "ticks" / "raw", base().products, base().calendar)
    assert src.days() == [TD] and src.products() == ["IF", "rb"]
    plan = src.plan(TD, {}, set(), {})
    assert [p.kind for p in plan.points] == ["T"] * 7  # the REJECTED row is not replayed
    assert plan.mains == {"rb": "rb2701", "IF": "IF2612"}
    replayed: dict[str, list] = {}
    for p in plan.points:
        replayed.setdefault(p.instrument_id, []).append(p)
    views = src.settle_views(plan, replayed)
    assert Decimal(views["rb2701"].price) == Decimal("3103") and views["rb2701"].source == SettlePriceSource.VWAP
    assert Decimal(views["IF2612"].price) == Decimal("4515") and views["IF2612"].source == SettlePriceSource.VWAP
    assert len(src.file_hashes()) == 1


def test_layer_c_run_config(tmp_path: Path) -> None:
    record(tmp_path)
    cfg = BacktestConfig(name="ticks", kind="arms", layer="C", data={"ticks_home": str(tmp_path)}, products=["rb", "IF"],
                         accounts={"ACC3": AccountRun(strategy="counter_trend_add", products=["rb"],
                                                      params={"weights": {"rb": 0.39}})},
                         arms=[ArmSpec(name="no_rules", tier="none"), ArmSpec(name="block_open", tier="block_open")],
                         workers=1)
    out = run_config(cfg, tmp_path / "out")
    r = out.results[0]
    assert r.days == [TD] and r.stats["ticks"] == 7 and r.stats["settle_failed_checks"] == 0
    (row,) = r.daily
    lots = sum(o["qty"] for o in r.orders if o["direction"] == "BUY")
    assert lots > 0
    # the book was bought at the first rb state (3100) and settled at the computed 3103
    assert row["position_pnl"] == pytest.approx((3103 - 3100) * 10 * lots)


def test_layer_c_main_contract_is_chosen_before_the_open(tmp_path: Path) -> None:
    """No look-ahead: the main of a recorded day is the contract with the largest final volume of the previous
    recorded day (not of the day itself)."""
    reg = InstrumentRegistry(base().products, base().calendar)
    w = ParquetShardWriter(tmp_path / "ticks" / "raw", "sina", TICK_SCHEMA, prefix="part", epoch_ms=1, clock=SimClock(0),
                           roll_s=1e9)
    volumes = {"20260928": {"rb2701": 900, "rb2705": 100}, "20260929": {"rb2701": 200, "rb2705": 5000}}
    for td, per in volumes.items():
        day = f"{td[:4]}-{td[4:6]}-{td[6:]}"
        for inst, vol in per.items():
            up, lo = reg.limit_prices(inst, Decimal("3100"), td)
            tick = Tick(instrument_id=inst, exchange=reg.exchange(inst), trading_day=td, action_day=td,
                        exch_ts_ns=ts(f"{day} 10:00"), last=3100.0, volume=vol, turnover=3100.0 * 10 * vol, open_interest=100,
                        bid1=3099.0, ask1=3101.0, upper_limit=float(up), lower_limit=float(lo), pre_settle=3100.0,
                        source=SourceId.SINA, recv_ns=ts(f"{day} 10:00"))
            w.append(td, tick_row(tick, TickMode.LIVE))
    w.close()
    src = TickSource(tmp_path / "ticks" / "raw", base().products, base().calendar)
    assert src.days() == ["20260928", "20260929"]
    assert src.main_contracts("20260928") == {"rb": "rb2701"}  # first day: no previous day, no daily data -> first id
    # rb2705 trades 5000 lots on 09-29, but that is known only at its end: 09-29's main is still rb2701
    assert src.main_contracts("20260929") == {"rb": "rb2701"}
