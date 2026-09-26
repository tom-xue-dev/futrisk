"""Real data (data/hist): a short window of the default config. Slow: not in the fast suite.

The full run is made with the CLI (``futrisk backtest run default``); the report is kept under docs/backtests/.
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


def test_default_config_on_2016_both_arms_settle_cleanly_and_the_rule_fires(tmp_path: Path) -> None:
    cfg, path = load_backtest_config("default", {"start": "20161101", "end": "20161130", "workers": 2})
    out = run_config(cfg, tmp_path / "a", config_path=path)
    s = out.summary
    assert s["baseline"] == "no_rules" and set(s["arms"]) == {"no_rules", "block_open"}
    assert s["checks"]["digest_mismatches"] == 0 and s["checks"]["settle_failed_checks"] == 0
    assert s["period"]["days"] == 22 and set(s["accounts"]) == {"ACC1", "ACC2", "ACC3", "ACC4", "ACC5"}
    # no rule, no event; with the rule the black-sector books hit the ladder in the November 2016 crash
    assert s["arms"]["no_rules"]["stats"]["TOTAL"]["warn_days"] == 0
    assert s["arms"]["block_open"]["stats"]["TOTAL"]["warn_days"] > 0
    assert (tmp_path / "a" / "report.html").stat().st_size > 20_000
    # the number of workers does not change the result
    one = run_config(cfg.model_copy(update={"workers": 1}), tmp_path / "b", config_path=path)
    assert one.manifest["events_hash"] == out.manifest["events_hash"]
