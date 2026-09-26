"""Real data (data/hist): short runs of the default, stress, sweep and minute configs. Slow: not in the fast suite.

The full runs are made with the CLI (``futrisk backtest run default``) and kept under var/backtests/.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from futrisk.backtest.config import load_backtest_config
from futrisk.backtest.runner import run_config
from futrisk.common.paths import repo_root

DATA = repo_root() / "data" / "hist"
pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(not (DATA / "daily" / "rb.parquet").exists(), reason="data/hist not available"),
]


def test_default_config_on_november_2016_deterministic(tmp_path: Path) -> None:
    cfg, path = load_backtest_config("default", {"start": "20161101", "end": "20161130", "workers": 6, "ensemble.paths": 0,
                                                 "sensitivity": [], "analysis.bootstrap_samples": 100})
    out = run_config(cfg, tmp_path, config_path=path)
    s = out.summary
    assert s["model"]["kind"] == "deterministic"
    assert {"no_rules", "alert_only", "block_open", "halt_only", "halve", "flatten", "placebo_halt", "placebo_flatten"} <= set(s["arms"])
    assert s["checks"]["alert_only_equals_no_rules"] is True
    for a in s["arms"].values():
        assert a["run"]["settle_failed_checks"] == 0 and a["run"]["digest_mismatches"] == 0
    blk = next(r for r in out.results if r.arm == "block_open")
    ev = [e for e in blk.events if e["trading_day"] == "20161114" and e["account_id"] == "ACC3" and e["kind"] == "ALERT_RAISED"
          and e["level"] == "halt"]
    assert len(ev) == 1 and ev[0]["latched"] is True
    assert "decomposition" in s["comparisons"] and "ladder" in s["comparisons"]
    assert (tmp_path / "report.html").stat().st_size > 50_000


def test_default_config_ensemble_on_a_short_window(tmp_path: Path) -> None:
    cfg, path = load_backtest_config("default", {"start": "20211008", "end": "20211130", "workers": 8, "ensemble.paths": 2,
                                                 "sensitivity": [{"name": "low_first", "set": {"path.rule": "low_first"}}],
                                                 "warmup_days": 40})
    out = run_config(cfg, tmp_path, config_path=path)
    s = out.summary
    assert s["model"]["kind"] == "ensemble" and s["paths"] == 2 and s["model"]["calibration"]["products"]["rb"]["sigma"] > 0
    d = s["deltas"]["block_open"]["ACC3"]["total_ret"]
    assert d["p5"] <= d["delta"] <= d["p95"] and d["lo"] <= d["hi"]
    rows = s["sensitivity"]["rows"]
    assert {r["group"] for r in rows} == {"main", "var:low_first"}
    assert (tmp_path / "ensemble.parquet").exists()


def test_stress_config_one_window(tmp_path: Path) -> None:
    cfg, path = load_backtest_config("stress", {"workers": 6, "ensemble.paths": 2})
    windows = [w for w in cfg.stress.windows if w.id == "2016_11_black"]  # type: ignore[union-attr]
    cfg = cfg.model_copy(update={"stress": cfg.stress.model_copy(update={"windows": windows})})  # type: ignore[union-attr]
    out = run_config(cfg, tmp_path, config_path=path)
    assert [w["id"] for w in out.summary["windows"]] == ["2016_11_black"]
    rows = [r for r in out.summary["table"] if r["account_id"] == "ACC3"]
    assert {r["arm"] for r in rows} == {"alert_only", "block_open", "halt_only", "halve", "flatten"}
    assert all(r["paths"] == 2 for r in rows)
    assert any(f["trading_day"] == "20161114" for f in out.summary["focus"])


def test_minute_layer_b(tmp_path: Path) -> None:
    cfg, path = load_backtest_config("minute", {"workers": 6, "analysis.bootstrap_samples": 100})
    out = run_config(cfg, tmp_path, config_path=path)
    assert out.results[0].layer == "B" and len(out.results[0].days) > 50
    hr = out.summary["hit_rate"]["by_period"]
    assert 0.5 < hr["60"]["hit_rate"] < 1.0
