"""Replay throughput benchmark (01 §8.12 performance baseline for the backtester).

Measures ticks per second through the same EngineCore the risk-engine runs:
every market state is applied with ``apply_mark``, dirty accounts are
evaluated (metrics → rules → state machines → actions), the strategies' orders
go through the 9-step pre-trade pipeline, the in-process broker fills them and
the fills come back through ``apply_exec``. ``static=True`` stops the
strategies after the first day, i.e. a pure tick replay over a static book
(evaluation only). With ``profile=True`` the replay runs under cProfile and the
self time is grouped by module.
"""

from __future__ import annotations

import cProfile
import pstats
from typing import Any

from futrisk.backtest.config import AccountRun, ArmSpec, BacktestConfig
from futrisk.backtest.replay import Replay
from futrisk.backtest.runner import build_source, build_strategies, get_hist, load_base
from futrisk.backtest.timing import wall_s

DEMO_ACCOUNTS = {
    "ACC1": AccountRun(strategy="channel_breakout"),
    "ACC2": AccountRun(strategy="multi_trend"),
    "ACC3": AccountRun(strategy="counter_trend_add"),
    "ACC4": AccountRun(strategy="index_directional"),
    "ACC5": AccountRun(strategy="rb_calendar_spread"),
}


class _StaticReplay(Replay):
    """Strategies trade on the first day only; afterwards a pure tick replay of the book."""

    def _day(self, td: str, plan: Any, sod: Any, next_td: str | None) -> Any:
        out = super()._day(td, plan, sod, next_td)
        self.use_strategies = False
        return out


def run_bench(*, start: str = "20160104", days: int = 120, tier: str = "block_open", profile: bool = False,
              static: bool = False) -> dict[str, Any]:
    cfg = BacktestConfig(name="bench", start=start, accounts=dict(DEMO_ACCOUNTS), arms=[ArmSpec.model_validate({"name": tier, "tier": tier})],
                         workers=1, warmup_days=40)
    base = load_base(cfg)
    hist = get_hist(cfg, base)
    tds = hist.trading_days(start, None)[:days]
    if not tds:
        raise ValueError(f"no data from {start}")
    run_cfg = cfg.model_copy(update={"end": tds[-1]})
    source = build_source(run_cfg, base, start=start, end=tds[-1])
    strategies = build_strategies(run_cfg, source.products())
    cls = _StaticReplay if static else Replay
    rep = cls(cfg=run_cfg, arm=run_cfg.arms[0], source=source, base_rules=base.risk_rules, base_accounts=base.accounts,
              strategies=strategies, label="bench")
    prof = cProfile.Profile() if profile else None
    t0 = wall_s()
    if prof is not None:
        prof.enable()
    res = rep.run()
    if prof is not None:
        prof.disable()
    wall = wall_s() - t0
    st = res.stats
    out: dict[str, Any] = {
        "days": st["days"],
        "states": st["states"],
        "ticks": st["ticks"],
        "orders": st["orders"],
        "fills": st["fills"],
        "replay_s": round(st["replay_s"], 3),
        "strategy_and_orders_s": round(st["strategy_s"], 3),
        "settle_s": round(st["settle_s"], 3),
        "plan_s": round(st["plan_s"], 3),
        "wall_s": round(wall, 3),
        "ticks_per_s_replay": round(st["ticks"] / st["replay_s"]) if st["replay_s"] else None,
        "states_per_s_replay": round(st["states"] / st["replay_s"]) if st["replay_s"] else None,
        "ticks_per_s_end_to_end": round(st["ticks"] / wall) if wall else None,
        "us_per_state": round(st["replay_s"] / st["states"] * 1e6, 1) if st["states"] else None,
        "profiled": profile,
    }
    if prof is not None:
        stats = pstats.Stats(prof)
        by_mod: dict[str, float] = {}
        for (filename, _line, _fn), (_cc, _nc, tt, _ct, _callers) in stats.stats.items():  # type: ignore[attr-defined]
            f = filename.replace("\\", "/")
            if "/futrisk/" in f:
                mod = "futrisk." + f.split("/futrisk/", 1)[1].removesuffix(".py").replace("/", ".")
            elif "site-packages/" in f:
                mod = f.split("site-packages/", 1)[1].split("/", 1)[0]
            elif f.startswith("~") or f.startswith("<"):
                mod = "builtins"
            else:
                mod = "stdlib"
            by_mod[mod] = by_mod.get(mod, 0.0) + tt
        out["profile"] = sorted(by_mod.items(), key=lambda kv: -kv[1])[:25]
    return out


__all__ = ["run_bench"]
