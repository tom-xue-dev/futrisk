"""Breaker scenario calibration (01 §6.3 QS-03, ``config/scenarios/breaker.yaml``) against the real risk engine.

:mod:`futrisk.sim.bots.calibration` runs the risk-engine core (``EngineCore``: risk_rules.yaml,
fees, P&L) in-process with the real ACC3 bot in scenario mode on the sim's price model. The
acceptance behaviour: one daily-loss level per leg (warn, restrict, halt; >= 30 s apart; the halt
before the scenario's last step) in at least 19 of 20 noisy runs, never within 0.5% of the -5%
derisk level; after restrict the adds are halved (ORDER_SIZE_FACTOR) and rejected
REJECT_ADD_TO_LOSER; once halted the bot knows at once, its opening probes are rejected
REJECT_ACCOUNT_HALTED and its halt trim (closing orders) trades.
"""

from __future__ import annotations

import pytest
from loguru import logger

from futrisk.messages import codes
from futrisk.sim.bots.calibration import BreakerRun, run_breaker, run_many, scenario_spec

pytestmark = pytest.mark.slow


@pytest.fixture(autouse=True)
def _quiet_logs() -> None:
    logger.disable("futrisk")
    yield
    logger.enable("futrisk")


def _table(runs: list[BreakerRun]) -> str:
    return "\n".join(("  ok " if r.one_level_per_leg() else "MISS ") + r.row() for r in runs)


def test_noise_free_path_shows_every_acceptance_behaviour() -> None:
    spec = scenario_spec()
    assert len(spec.legs) == 3 and spec.at_s[1] - spec.at_s[0] >= 30 + spec.ramp_s / 2
    r = run_breaker(spec, seed=None, noise=False)
    assert r.book_ratio >= 0.7 and r.one_level_per_leg(hold_s=30), r.row()
    # the position dimension raises its WARNs first (ladder adds), then the daily-loss levels
    first = [a for _, a in r.alerts[:2]]
    assert set(first) <= {"sector_exposure/warn", "product_exposure/warn"}, r.alerts
    # RESTRICTED: the adds go on at half size and are rejected (NO_ADD_TO_LOSERS)
    assert r.orders[f"open:{codes.REJECT_ADD_TO_LOSER}"] >= 1
    assert r.add_reasons_restricted and all("ORDER_SIZE_FACTOR 0.5" in w for w in r.add_reasons_restricted)
    # HALTED: known at once from risk.state, opening probes rejected every ~20 s, the halt trim trades
    halt = r.level("halt")
    assert halt is not None and r.learned_halt is not None and r.learned_halt - halt <= 1.0
    assert r.orders[f"halt_probe:{codes.REJECT_ACCOUNT_HALTED}"] >= 10
    assert r.orders["close:ok"] == 3
    assert r.worst_pnl > -0.045 and r.level("derisk") is None


def test_twenty_noisy_seeds_one_level_per_leg() -> None:
    runs = run_many(scenario_spec(), range(1, 21))
    ok = sum(r.one_level_per_leg(hold_s=30) for r in runs)
    assert ok >= 19, _table(runs)
    assert min(r.worst_pnl for r in runs) > -0.045, _table(runs)
    assert all(r.level("halt") is not None and r.level("derisk") is None for r in runs), _table(runs)
