"""Layer D: the acceptance scenario QS-03 and the single bad tick, reproduced in-process with exact assertions."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from futrisk.backtest.config import load_backtest_config
from futrisk.backtest.runner import run_config
from futrisk.messages import codes


@pytest.fixture(scope="module")
def breaker(tmp_path_factory: pytest.TempPathFactory) -> Any:
    cfg, path = load_backtest_config("breaker_synthetic", {"workers": 1})
    out = tmp_path_factory.mktemp("breaker")
    return run_config(cfg, out, config_path=path)


def _arm(out: Any, scenario: int, arm: str) -> Any:
    for r in out.results:
        if r.stats.get("group") == f"sc:{scenario}" and r.arm == arm:
            return r
    raise KeyError((scenario, arm))


def test_every_scenario_check_passes(breaker: Any) -> None:
    s = breaker.summary
    assert s["passed"] is True
    assert len(s["checks"]) == 16
    failed = [c for c in s["checks"] if not c["ok"]]
    assert failed == []


def test_acceptance_breaker_exact_events(breaker: Any) -> None:
    r = _arm(breaker, 0, "block_open")
    raised = [(e["level"], e["action"], e["latched"], e["step"]) for e in r.events if e["kind"] == "ALERT_RAISED"]
    assert raised == [("warn", "ALERT", False, "s1"), ("restrict", "RESTRICT", False, "s2"), ("halt", "CIRCUIT_BREAK", True, "s5")]
    values = {e["level"]: e["value"] for e in r.events if e["kind"] == "ALERT_RAISED"}
    assert values["warn"] == pytest.approx(-0.020921, abs=1e-6)  # (3016-3100)*1250 + (3211-3300)*510 + (778.5-800)*2700 - fees
    assert values["restrict"] == pytest.approx(-0.025541, abs=1e-6)
    assert -0.0315 < values["halt"] < -0.030
    states = [(s["step"], s["from_state"], s["to_state"], s["halted_latched"]) for s in r.states if s["step"] is not None]
    assert states == [
        ("s1", "NORMAL", "WARNING", False),
        ("s2", "WARNING", "RESTRICTED", False),
        ("s4", "RESTRICTED", "HALTED", False),  # first breaching state: HALTED at once, not latched (PENDING)
        ("s5", "HALTED", "HALTED", True),  # second distinct LIVE state: confirmed and latched
    ]
    scripted = {o["label"]: o for o in r.scripted}
    assert scripted["add_restricted"]["code"] == codes.REJECT_ADD_TO_LOSER
    assert scripted["open_after_halt"]["code"] == codes.REJECT_ACCOUNT_HALTED
    assert scripted["close_after_halt"]["code"] is None and scripted["close_after_halt"]["statuses"] == ["FILLED"]
    assert all(scripted[f"book:{i}"]["statuses"] == ["FILLED"] for i in ("rb2701", "hc2701", "i2701"))


def test_single_bad_tick_never_latches(breaker: Any) -> None:
    r = _arm(breaker, 1, "block_open")
    assert not [e for e in r.events if e["kind"] == "ALERT_RAISED" and e["level"] == "halt"]
    states = [(s["step"], s["from_state"], s["to_state"], s["halted_latched"]) for s in r.states if s["step"] is not None]
    assert ("s3", "RESTRICTED", "HALTED", False) in states
    assert ("s4", "HALTED", "RESTRICTED", False) in states
    assert not any(latched for _s, _f, _t, latched in states)
    (row,) = r.daily
    assert row["halted"] and not row["halt_latched"] and row["halt_revoked"] == 1
    assert {o["label"]: o["code"] for o in r.scripted}["open_after_revoke"] is None


def test_no_rules_arm_produces_no_risk_events(breaker: Any) -> None:
    for sc in (0, 1):
        r = _arm(breaker, sc, "no_rules")
        assert r.events == []
        assert all(o["ok"] for o in r.orders)
        assert all(s["to_state"] in ("NORMAL", "RESTRICTED") and s["step"] is None for s in r.states)  # start-up only


def test_flatten_tier_closes_every_position_when_the_breaker_latches(breaker: Any) -> None:
    r = _arm(breaker, 0, "flatten")
    acts = sorted((a["instrument_id"], a["offset"], a["qty"], a["accepted"]) for a in r.actions)
    assert acts == [("hc2701", "CLOSE_TODAY", 51, 51), ("i2701", "CLOSE", 27, 27), ("rb2701", "CLOSE_TODAY", 125, 125)]
    tier_orders = [o for o in r.orders if o["purpose"] == "tier:flatten"]
    assert {o["step"] for o in tier_orders} == {"s5"} and all(o["ok"] for o in tier_orders)
    # after the flatten the scripted close has nothing left to close
    assert {o["label"]: o["code"] for o in r.scripted}["close_after_halt"] == codes.REJECT_CLOSE_TODAY_EXCEEDS


def test_outputs_written(breaker: Any) -> None:
    out: Path = breaker.out_dir
    for name in ("report.html", "metrics.csv", "events.parquet", "states.parquet", "daily.parquet", "orders.parquet",
                 "manifest.json", "summary.json"):
        assert (out / name).exists(), name
    html = (out / "report.html").read_text(encoding="utf-8")
    assert "data:image/png;base64," in html and "<link" not in html and "http://" not in html.replace("http://www.w3", "")
