"""Quality gating with a materiality tolerance (``system.quality_stale_tolerance``; 01 §8.14, 04 §3.6 / §14).

An account-scope rule used to take the worst quality of all held instruments: one STALE instrument froze
``daily_loss``, so a -3 % loss on the other instruments could not halt the account. Now, while the non-OK
instruments carry at most ``quality_stale_tolerance`` (default 10 %) of the gross notional, the account quality is
OK and the rules run on the last known marks; above it the machines hold as before. Product / sector scopes keep
their own worst quality; the firm keeps "worst quality of the exposed accounts' held instruments".
"""

from __future__ import annotations

from enginetest_support import PRE, TD, Harness, config, rules_only, sod_snapshot

from futrisk.common.config.risk_rules import RiskRulesConfig
from futrisk.messages.enums import AccountState, MdQuality, PosDirection, RiskEventKind, RuleSmState
from futrisk.messages.eod import SodLot

OK, STALE = MdQuality.OK, MdQuality.STALE
P = {"rb2701": 3100.0, "hc2701": 3300.0, "i2701": 800.0, "cu2611": 80000.0}
LOSS_1 = {"rb2701": 2950.0, "hc2701": 3150.0, "i2701": 775.0}  # ACC3 daily P&L -312,500 = -3.125 %
LOSS_2 = {"rb2701": 2949.0, "hc2701": 3149.0, "i2701": 774.5}  # a second LIVE market state (confirms the halt)


def _lot(inst: str, qty: int, no: int) -> SodLot:
    return SodLot(instrument_id=inst, direction=PosDirection.LONG, lot_no=no, open_td="20260925", open_ts_ns=no,
                  open_price=str(PRE[inst]), qty=qty, basis_price=str(PRE[inst]))


def _book(cu_lots: int, *, tolerance: float | None = None, coordinator: bool = False) -> Harness:
    """ACC3: rb 80 + hc 75 + i 32 (gross 7.515 M) and ``cu_lots`` cu2611 (0.4 M each)."""
    rules = rules_only("daily_loss")
    if tolerance is not None:
        rules = RiskRulesConfig(rules=rules.rules, pretrade=rules.pretrade,
                                system=rules.system.model_copy(update={"quality_stale_tolerance": tolerance}))
    lots = {"ACC3": [_lot("rb2701", 80, 1), _lot("hc2701", 75, 2), _lot("i2701", 32, 3), _lot("cu2611", cu_lots, 4)]}
    h = Harness(rules, sod=sod_snapshot(lots=lots))
    h.ticks(P)
    return h


def test_default_tolerance_is_ten_percent() -> None:
    assert config().risk_rules.system.quality_stale_tolerance == 0.10


def test_a_small_stale_position_does_not_stop_a_real_breach_from_halting() -> None:
    h = _book(cu_lots=1)  # cu = 0.4 M of 7.915 M gross = 5.1 %
    h.md_status({"cu2611": STALE})
    ra = h.eng.account("ACC3")
    val = h.eng.valuation(ra)
    assert val.instrument_quality["cu2611"] == STALE and val.product_quality["cu"] == STALE
    assert val.quality == OK  # account scope: the stale share is immaterial
    h.ticks(LOSS_1)
    assert ra.valuation is not None and ra.valuation.metrics.daily_pnl_pct_init < -0.03
    assert ra.state == AccountState.HALTED  # the circuit break is in force at the first breach (PENDING)
    h.ticks(LOSS_2)
    assert ra.state == AccountState.HALTED and ra.halted_latched  # confirmed and latched
    assert [e.level for e in h.events(RiskEventKind.ALERT_RAISED) if e.rule_id == "daily_loss"][-1] == "halt"


def test_a_large_stale_share_still_holds_the_machines() -> None:
    h = _book(cu_lots=3)  # cu = 1.2 M of 8.715 M gross = 13.8 % > 10 %
    h.md_status({"cu2611": STALE})
    ra = h.eng.account("ACC3")
    assert h.eng.valuation(ra).quality == STALE
    h.ticks(LOSS_1)
    h.ticks(LOSS_2)
    assert ra.valuation is not None and ra.valuation.metrics.daily_pnl_pct_init < -0.03
    assert ra.state == AccountState.NORMAL  # quality gating: the machines hold
    assert all(m.state == RuleSmState.OK for m in ra.machines.values())
    h.md_status({"cu2611": OK})  # quality back: the breach is seen at once
    assert ra.state == AccountState.HALTED


def test_tolerance_zero_keeps_the_worst_quality() -> None:
    h = _book(cu_lots=1, tolerance=0.0)
    h.md_status({"cu2611": STALE})
    ra = h.eng.account("ACC3")
    assert h.eng.valuation(ra).quality == STALE
    h.ticks(LOSS_1)
    assert ra.state == AccountState.NORMAL


def test_the_snapshot_quality_is_the_rule_quality() -> None:
    small = _book(cu_lots=1)
    small.md_status({"cu2611": STALE})
    snap = small.eng.snapshot("ACC3")
    assert snap.quality == OK  # what the account-scope rules use
    by_prod = {ev.product: ev.quality for ev in snap.exposures}
    assert by_prod["cu"] == STALE and by_prod["rb"] == OK  # product scopes keep their own quality
    large = _book(cu_lots=3)
    large.md_status({"cu2611": STALE})
    assert large.eng.snapshot("ACC3").quality == STALE
    large.advance(1000)
    large.timer()  # metric_samples: the quality column is the rule quality too
    rows = [r for r in large.db.tables["metric_samples"] if r["account_id"] == "ACC3"]
    assert rows and rows[-1]["quality"] == "STALE"


def test_the_firm_keeps_the_worst_quality_of_the_held_instruments() -> None:
    h1 = _book(cu_lots=1)
    h1.md_status({"cu2611": STALE})
    snap = h1.eng.snapshot("ACC3")
    assert snap.quality == OK
    h0 = Harness(rules_only("daily_loss"), shard=0, coordinator=True,
                 sod=sod_snapshot({"ACC1": "10000000", "ACC2": "20000000"}))
    h0.ticks(P)
    h0.eng.ingest_snapshot(snap)
    agg = h0.eng.aggregator
    assert agg is not None
    fv = agg.build_valuation(TD, h0.clock.now_ns())
    assert fv.quality == STALE  # firm scopes: worst quality of the exposed accounts' held instruments, as before
    assert fv.product_quality["rb"] == STALE and fv.product_quality["cu"] == STALE
