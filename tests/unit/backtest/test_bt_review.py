"""Review items: look-ahead, DCE roll factors, path independence, pairing invariants, caches, an end-to-end run."""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from btsupport import base, write_hist
from test_bt_hist import D1, D2, D3, FixedTarget, rb_files

from futrisk.backtest.config import AccountRun, ArmSpec, BacktestConfig
from futrisk.backtest.data import DailyBar, HistData
from futrisk.backtest.replay import ArmResult, Replay
from futrisk.backtest.runner import get_hist, load_base, run_config
from futrisk.backtest.sources import DailySource
from futrisk.backtest.strategies import Strategy, StrategyContext

D0 = "20260918"


# ------------------------------------------------------------------ fixtures
def crash_files(root: Path) -> None:
    """rb: a quiet day, then a crash day (-10 % at the low, recovering to -1.7 %), then a quiet day."""
    rows = [
        {"td": D0, "inst": "rb2701", "o": 3000, "h": 3010, "l": 2995, "c": 3000, "s": 3000, "ps": 3000, "cps": 3000},
        {"td": D1, "inst": "rb2701", "o": 3000, "h": 3010, "l": 2700, "c": 2950, "s": 2960, "ps": 3000, "cps": 3000},
        {"td": D2, "inst": "rb2701", "o": 2950, "h": 2980, "l": 2940, "c": 2970, "s": 2965, "ps": 2960, "cps": 2960},
    ]
    write_hist(root, "rb", "SHFE", rows, [dict(r, main=True) for r in rows])


def _cfg(root: Path, **kw: Any) -> BacktestConfig:
    return BacktestConfig(name="t", layer="A", data={"dir": str(root)}, products=["rb"], workers=1, **kw)


def _replay(cfg: BacktestConfig, arm: ArmSpec, strategies: dict[str, Strategy]) -> ArmResult:
    hist = HistData(cfg.data_dir(), ["rb"], base().products)
    src = DailySource(hist, cfg, base().calendar)
    rep = Replay(cfg=cfg, arm=arm, source=src, base_rules=base().risk_rules, base_accounts=base().accounts,
                 strategies=strategies, label=f"t-{arm.name}")
    return rep.run()


class AddOnDips(Strategy):
    """A long book of ``base`` lots bought at the open, plus ``add`` lots for every ``step`` drop from the open
    (at most ``max_adds``): it opens positions during a bad day, which the breaker then blocks."""

    name = "add_on_dips"

    def __init__(self, account_id: str = "ACC3", base: int = 300, add: int = 30, step: float = 0.01,
                 max_adds: int = 5) -> None:
        super().__init__(Strategy.params_model(), account_id=account_id, products=["rb"])
        self.base, self.add, self.step, self.max_adds = base, add, step, max_adds

    def targets(self, ctx: StrategyContext) -> dict[str, int] | None:
        q = ctx.quotes.get("rb")
        if q is None or q.price is None or q.open is None:
            return None
        adds = min(self.max_adds, max(0, int((q.open - q.price) / (q.open * self.step))))
        return {q.instrument_id: self.base + self.add * adds}


def _pnl(r: ArmResult) -> dict[tuple[str, str], float]:
    return {(x["trading_day"], x["account_id"]): x["day_pnl"] for x in r.daily}


# ------------------------------------------------------------------ look-ahead
class Recorder(Strategy):
    """Holds nothing; records which daily bars it has seen whenever it is asked for targets."""

    name = "recorder"

    def __init__(self) -> None:
        super().__init__(Strategy.params_model(), account_id="ACC2", products=["rb"])
        self.seen: list[str] = []
        self.asked: list[tuple[str, tuple[str, ...]]] = []

    def on_bar(self, product: str, bar: DailyBar) -> None:
        self.seen.append(bar.trading_day)

    def targets(self, ctx: StrategyContext) -> dict[str, int] | None:
        self.asked.append((ctx.trading_day, tuple(self.seen)))
        return None


def test_no_look_ahead_a_strategy_sees_a_day_bar_only_after_that_day(tmp_path: Path) -> None:
    rb_files(tmp_path)
    cfg = _cfg(tmp_path, accounts={"ACC2": AccountRun(capital=5_000_000)})
    rec = Recorder()
    _replay(cfg, ArmSpec(name="none", tier="none"), {"ACC2": rec})
    assert rec.asked and rec.seen == [D1, D2, D3]
    for td, seen in rec.asked:
        assert all(d < td for d in seen), (td, seen)  # during day d only bars before d


# ------------------------------------------------------------------ DCE roll factor
def test_dce_roll_factor_uses_the_estimated_previous_settle(tmp_path: Path) -> None:
    rows = [
        {"td": D1, "inst": "i2701", "o": 700, "h": 710, "l": 695, "c": 705, "s": 702, "ps": 698, "cps": 698, "src": "sina"},
        {"td": D2, "inst": "i2705", "o": 660, "h": 668, "l": 655, "c": 664, "s": 662, "ps": 702, "cps": None, "roll": True,
         "src": "sina"},
        {"td": D3, "inst": "i2705", "o": 663, "h": 668, "l": 660, "c": 665, "s": 664, "ps": 662, "cps": 662, "src": "sina"},
    ]
    # DCE per-contract rows carry no previous settle; the new main's row on the day before the roll has no settle
    # either -> (H + L) / 2 = 659 is its estimated settle, i.e. the new main's previous settle on the roll day
    contracts = [dict(r, main=True, ps=None) for r in rows] + [
        {"td": D1, "inst": "i2705", "o": 657, "h": 664, "l": 654, "c": 660, "s": None, "ps": None},
    ]
    write_hist(tmp_path, "i", "DCE", rows, contracts)
    hist = HistData(tmp_path, ["i"], base().products)
    s = hist.series["i"]
    assert s.roll_source(D2) == "estimated"
    assert s.roll_factor(D2) == pytest.approx(659 / 702) and s.roll_factor(D2) != 1.0
    assert s.roll_factor(D1) == 1.0 and s.roll_source(D1) == "none"
    assert [b.roll_factor for b in hist.daily_bars("i")][1] == pytest.approx(659 / 702)


def test_dce_roll_after_the_archive_freeze_uses_the_new_mains_open(tmp_path: Path) -> None:
    rows = [
        {"td": D1, "inst": "i2701", "o": 700, "h": 710, "l": 695, "c": 705, "s": 702, "ps": 698, "cps": 698, "src": "sina"},
        {"td": D2, "inst": "i2705", "o": 660, "h": 668, "l": 655, "c": 664, "s": 662, "ps": 702, "cps": None, "roll": True,
         "src": "sina"},
    ]
    write_hist(tmp_path, "i", "DCE", rows, [dict(r, main=True, ps=None) for r in rows])
    s = HistData(tmp_path, ["i"], base().products).series["i"]
    assert s.roll_source(D2) == "open" and s.roll_factor(D2) == pytest.approx(660 / 702)


# ------------------------------------------------------------------ paths and pairing
def test_a_contracts_path_does_not_depend_on_the_other_contracts_of_the_plan(tmp_path: Path) -> None:
    rb_files(tmp_path)
    rows = [{"td": d, "inst": "hc2701", "o": 3300, "h": 3350 + i, "l": 3250, "c": 3320, "s": 3310, "ps": 3300, "cps": 3300}
            for i, d in enumerate((D1, D2, D3))]
    write_hist(tmp_path, "hc", "SHFE", rows, [dict(r, main=True) for r in rows])
    hist = HistData(tmp_path, ["rb", "hc"], base().products)
    cfg = BacktestConfig(name="t", layer="A", data={"dir": str(tmp_path)}, products=["rb", "hc"], workers=1)
    alone = DailySource(hist, cfg, base().calendar, products=["rb"])
    both = DailySource(hist, cfg, base().calendar, products=["rb", "hc"])
    pa = alone.plan(D2, {"rb2610": 3005.0, "rb2701": 3095.0}, set())
    pb = both.plan(D2, {"rb2610": 3005.0, "rb2701": 3095.0, "hc2701": 3300.0}, set())
    ra = [(p.ts_ns, p.price) for p in pa.points if p.instrument_id == "rb2701"]
    rb = [(p.ts_ns, p.price) for p in pb.points if p.instrument_id == "rb2701"]
    assert ra == rb and ra


def test_accounts_are_independent(tmp_path: Path) -> None:
    crash_files(tmp_path)
    cfg = _cfg(tmp_path, accounts={"ACC3": AccountRun(capital=10_000_000), "ACC2": AccountRun(capital=5_000_000)})
    arm = ArmSpec(name="block_open", tier="block_open")
    together = _replay(cfg, arm, {"ACC3": AddOnDips(), "ACC2": FixedTarget(["rb"], lots=40)})
    alone3 = _replay(cfg, arm, {"ACC3": AddOnDips()})
    alone2 = _replay(cfg, arm, {"ACC2": FixedTarget(["rb"], lots=40)})
    t = _pnl(together)
    assert {k: v for k, v in t.items() if k[1] == "ACC3"} == _pnl(alone3)
    assert {k: v for k, v in t.items() if k[1] == "ACC2"} == _pnl(alone2)
    assert any(e["level"] == "halt" for e in alone3.events)


def test_block_open_does_nothing_to_a_static_book_but_cuts_the_adds(tmp_path: Path) -> None:
    crash_files(tmp_path)
    cfg = _cfg(tmp_path, accounts={"ACC3": AccountRun(capital=10_000_000)})
    static = {a: _replay(cfg, ArmSpec(name=a, tier=a), {"ACC3": AddOnDips(max_adds=0)}) for a in ("none", "block_open")}
    assert _pnl(static["none"]) == _pnl(static["block_open"])  # no intraday opens after the halt: nothing to block
    assert any(e["level"] == "halt" and e["latched"] for e in static["block_open"].events)
    dips = {a: _replay(cfg, ArmSpec(name=a, tier=a), {"ACC3": AddOnDips(max_adds=5)}) for a in ("none", "block_open")}
    assert _pnl(dips["none"]) != _pnl(dips["block_open"])
    assert sum(r["blocked_opens"] + r["blocked_adds"] for r in dips["block_open"].daily) > 0
    assert not dips["none"].events


# ------------------------------------------------------------------ caches and hashes
def test_caches_reload_changed_files_and_hashes_are_taken_at_read(tmp_path: Path) -> None:
    rb_files(tmp_path)
    cfg = _cfg(tmp_path, accounts={"ACC2": AccountRun(capital=5_000_000, products=["rb"])})
    b = load_base(cfg)
    h1 = get_hist(cfg, b)
    path = tmp_path / "daily" / "rb.parquet"
    sha1 = hashlib.sha256(path.read_bytes()).hexdigest()
    assert h1.file_hashes()["daily/rb.parquet"] == sha1 and get_hist(cfg, b) is h1
    df = pd.read_parquet(path)
    df.iloc[:-1].to_parquet(path, index=False)  # the data is refreshed on disk
    sha2 = hashlib.sha256(path.read_bytes()).hexdigest()
    h2 = get_hist(cfg, b)
    assert h2 is not h1 and h2.file_hashes()["daily/rb.parquet"] == sha2 != sha1
    assert h1.file_hashes()["daily/rb.parquet"] == sha1  # what the old instance read
    assert len(h2.series["rb"].days) == len(h1.series["rb"].days) - 1
    shutil.rmtree(tmp_path / "daily")


# ------------------------------------------------------------------ end to end
def test_small_arms_run_end_to_end_and_deterministic(tmp_path: Path) -> None:
    crash_files(tmp_path / "data")
    raw: dict[str, Any] = {
        "name": "small", "kind": "arms", "layer": "A", "data": {"dir": str(tmp_path / "data")}, "products": ["rb"],
        "accounts": {"ACC3": {"strategy": "dual_ma", "products": ["rb"], "capital": 10_000_000,
                              "params": {"fast": 1, "slow": 2, "notional_frac": 0.5}}},
        "workers": 1,
    }
    cfg = BacktestConfig.model_validate(raw)
    a = run_config(cfg, tmp_path / "a")
    b = run_config(cfg.model_copy(update={"workers": 2}), tmp_path / "b")
    s = a.summary
    assert s["kind"] == "arms" and s["baseline"] == "no_rules" and set(s["arms"]) == {"no_rules", "block_open"}
    assert s["period"] == {"start": D0, "end": D2, "days": 3}
    assert s["arms"]["no_rules"]["stats"]["ACC3"]["orders"] > 0  # the strategy traded after its first day of history
    assert s["checks"]["digest_mismatches"] == 0 and s["checks"]["settle_failed_checks"] == 0
    assert a.manifest["events_hash"] == b.manifest["events_hash"]
    assert a.manifest["summary_hash"] == b.manifest["summary_hash"]
    daily = pd.read_csv(tmp_path / "a" / "daily.csv")
    assert len(daily) == 2 * 3 and set(daily["arm"]) == {"no_rules", "block_open"}
    html = (tmp_path / "a" / "report.html").read_text(encoding="utf-8")
    assert "data:image/png;base64," in html and "block_open" in html
