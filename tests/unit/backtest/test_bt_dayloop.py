"""The day loop on hand-built days: golden clearing numbers, SOD carry-over, equity continuity.

Golden case 1 of tests/golden/CASES.md (SHFE rb2701, fee 0.0001 on every leg, opening balance
200,000) is replayed through the engine, the in-process broker and settle_day: the settlement
numbers of the three days must be the hand-computed ones. products.yaml charges 8% margin on rb
(the golden case uses 7%), which changes the margin and available funds but not the equity.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from btsupport import DaySpec, scripted_replay

from futrisk.backtest.executor import pos_info
from futrisk.core import InstrumentRegistry, settle_day
from futrisk.core.settlement import seed_sod
from futrisk.messages.enums import PosDirection, SettlePriceSource
from futrisk.messages.eod import SettlementPriceView

RB = "rb2701"


def order(side: str, offset: str, qty: int) -> dict[str, object]:
    return {"instrument": RB, "side": side, "offset": offset, "qty": qty, "label": f"{side}-{offset}-{qty}"}


def golden_days() -> list[DaySpec]:
    t1 = DaySpec(
        "20260922",
        states=[("2026-09-21 21:05", RB, 3100.0), ("2026-09-22 09:40", RB, 3110.0), ("2026-09-22 14:10", RB, 3130.0)],
        settle={RB: 3120.0},
        pre_settle={RB: 3090.0},
        orders={"2026-09-21 21:05": [order("BUY", "OPEN", 10)], "2026-09-22 09:40": [order("BUY", "OPEN", 5)],
                "2026-09-22 14:10": [order("SELL", "CLOSE_TODAY", 3)]},
    )
    t2 = DaySpec(
        "20260923",
        states=[("2026-09-22 21:10", RB, 3115.0), ("2026-09-23 10:00", RB, 3140.0), ("2026-09-23 10:05", RB, 3135.0)],
        settle={RB: 3132.0},
        pre_settle={RB: 3120.0},
        orders={"2026-09-22 21:10": [order("BUY", "OPEN", 4)], "2026-09-23 10:00": [order("SELL", "CLOSE_YESTERDAY", 6)],
                "2026-09-23 10:05": [order("SELL", "CLOSE_TODAY", 2)]},
    )
    t3 = DaySpec(
        "20260924",
        states=[("2026-09-24 09:30", RB, 3100.0)],
        settle={RB: 3095.0},
        pre_settle={RB: 3132.0},
        orders={"2026-09-24 09:30": [order("SELL", "CLOSE", 8)]},
    )
    return [t1, t2, t3]


@pytest.fixture(scope="module")
def golden():  # type: ignore[no-untyped-def]
    return scripted_replay(golden_days(), capital=200_000.0, label="golden")


def test_golden_case_1_settlement_numbers(golden) -> None:  # type: ignore[no-untyped-def]
    _rep, res = golden
    rows = {r["trading_day"]: r for r in res.daily}
    assert [rows[d]["equity"] for d in ("20260922", "20260923", "20260924")] == [202744.06, 205366.49, 202781.69]
    assert [rows[d]["fee"] for d in ("20260922", "20260923", "20260924")] == [55.94, 37.57, 24.80]
    assert [rows[d]["close_pnl"] for d in ("20260922", "20260923", "20260924")] == [900.0, 1600.0, -2560.0]
    assert [rows[d]["position_pnl"] for d in ("20260922", "20260923", "20260924")] == [1900.0, 1060.0, 0.0]
    assert rows["20260922"]["margin"] == pytest.approx(12 * 3120 * 10 * 0.08)  # 8% company rate in products.yaml
    assert res.stats["settle_failed_checks"] == 0 and res.stats["digest_mismatches"] == 0
    assert all(s["code"] is None for s in res.scripted)


def test_settlement_equals_settle_day_computed_directly() -> None:
    """Day 1 again, then settle_day on the broker's trades and the same prices gives identical numbers."""
    days = golden_days()[:1]
    rep, res = scripted_replay(days, capital=200_000.0, label="direct")
    reg = InstrumentRegistry(rep.source.products_cfg, rep.source.calendar)
    sod = seed_sod("20260922", {"ACC3": Decimal("200000")}, version=1, pre_settle={RB: Decimal("3090")})
    view = SettlementPriceView(trading_day="20260922", instrument_id=RB, price="3120", source=SettlePriceSource.OFFICIAL)
    ds = settle_day(trading_day="20260922", version=1, sod=sod, trades=list(rep.broker.trades), cash_movements=[],
                    settle_prices={RB: view}, registry=reg, next_trading_day="20260923")
    a = ds.accounts["ACC3"]
    row = res.daily[0]
    assert (row["equity"], row["day_pnl"], row["fee"], row["margin"]) == (float(a.equity), float(a.day_pnl), float(a.fee),
                                                                        float(a.margin))
    assert len(rep.broker.trades) == 3 and [t.price for t in rep.broker.trades] == [3100.0, 3110.0, 3130.0]


def test_sod_roll_carries_positions_and_equity_is_continuous(golden) -> None:  # type: ignore[no-untyped-def]
    _rep, res = golden
    rows = {r["trading_day"]: r for r in res.daily}
    # 12 lots carried into T2 (7 @3100 + 5 @3110, basis 3120), 8 into T3, none after T3
    assert rows["20260923"]["sod_net"] == '{"rb": 12}'
    assert rows["20260924"]["sod_net"] == '{"rb": 8}'
    for prev, cur in (("20260922", "20260923"), ("20260923", "20260924")):
        assert rows[cur]["pre_balance"] == rows[prev]["equity"]
        # the engine's equity right after the roll (marks = yesterday's settlement) equals the settled equity
        assert rows[cur]["engine_equity_sod"] == pytest.approx(rows[prev]["equity"], abs=1e-6)


def test_engine_book_after_the_roll() -> None:
    days = golden_days()[:2]
    rep, _res = scripted_replay(days, capital=200_000.0, label="book")
    ra = rep.core.account("ACC3")
    pi = pos_info(ra.ledger.book, "ACC3", RB)
    # end of T2: 12 − 6 (close yd) = 6 yesterday lots, 4 − 2 = 2 today lots
    assert (pi.long_qty, pi.long_yd, pi.long_td) == (8, 6, 2)
    lots = ra.ledger.book.find("ACC3", RB, PosDirection.LONG).lots
    assert sum(lot.qty for lot in lots) == 8
