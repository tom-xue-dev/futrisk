"""``replay``: backtest replay throughput through the EngineCore (target ≥ 100k ticks/s; known to fall short).

Calls :func:`futrisk.backtest.bench.run_bench` - the same code as ``futrisk backtest bench``: one arm
(``block_open``) of the five demo strategies over historical data, every market state applied with
``apply_mark``, dirty accounts evaluated, the strategies' orders through the 9-step pre-trade pipeline,
filled by the in-process broker and fed back with ``apply_exec``.

* **with strategies** (primary): ticks / replay seconds, as the backtester runs;
* **static book** (``static=True``): strategies only on the first day, afterwards a pure tick replay of
  the resulting book (marks + evaluation only).

The time per market state (all ticks of one timestamp, then evaluation, broker matching, strategies)
comes from a timing wrapper around ``Replay._state`` installed for the duration of the call.
"""

from __future__ import annotations

import cProfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

from futrisk.bench.core import (
    BenchContext,
    BenchResult,
    GcMonitor,
    Target,
    latency_ns,
    now_ns,
    profile_lines,
    scalar,
)

NAME = "replay"
TITLE = "Backtest replay through the EngineCore (in process)"
TARGET = "≥ 100,000 ticks/s"


@contextmanager
def _state_timer() -> Iterator[list[int]]:
    """Time every ``Replay._state`` call (ns); yields the list (empty when the method is not found)."""
    from futrisk.backtest.replay import Replay

    samples: list[int] = []
    orig: Callable[..., Any] | None = getattr(Replay, "_state", None)
    if orig is None:
        yield samples
        return

    def timed(self: Any, *args: Any, **kw: Any) -> Any:
        t0 = now_ns()
        try:
            return orig(self, *args, **kw)
        finally:
            samples.append(now_ns() - t0)

    Replay._state = timed  # type: ignore[method-assign]
    try:
        yield samples
    finally:
        Replay._state = orig  # type: ignore[method-assign]


def run(ctx: BenchContext) -> BenchResult:
    from futrisk.backtest.bench import run_bench

    days = int(ctx.size("days", full=120, quick=10, tiny=1))
    static_days = int(ctx.size("static_days", full=60, quick=10, tiny=1))
    start = str(ctx.size("start", full="20160104", quick="20160104"))
    res = BenchResult(name=NAME, title=TITLE, target=TARGET, quick=ctx.quick,
                      params={"start": start, "days": days, "static_days": static_days, "tier": "block_open",
                              "accounts": "ACC1-ACC5 demo strategies", "code": "futrisk.backtest.bench.run_bench"})
    runs: dict[str, Any] = {}
    gcm = GcMonitor()
    prof = cProfile.Profile() if ctx.profile else None
    with gcm:
        for label, static, n_days in (("strategies", False, days), ("static", True, static_days)):
            ctx.say(f"replay: {n_days} days ({label})")
            with _state_timer() as samples:
                if prof is not None and not static:
                    prof.enable()
                out = run_bench(start=start, days=n_days, tier="block_open", static=static)
                if prof is not None and not static:
                    prof.disable()
            runs[label] = (out, list(samples))
    out, samples = runs["strategies"]
    res.add(scalar("replay rate with strategies (ticks / replay s)", out.get("ticks_per_s_replay"), "ticks/s",
                   n=int(out.get("ticks", 0)), targets=[Target("value", ">=", 100_000, "ticks/s")], primary=True,
                   note=f"{out.get('days')} days, {out.get('states'):,} market states, {out.get('orders'):,} orders, "
                        f"{out.get('fills'):,} fills"))
    res.add(scalar("end-to-end rate incl. settlement / planning", out.get("ticks_per_s_end_to_end"), "ticks/s",
                   n=int(out.get("ticks", 0))))
    res.add(latency_ns("time per market state (with strategies)", samples,
                       note="marks of one timestamp + evaluation + matching + strategies"))
    sout, ssamples = runs["static"]
    res.add(scalar("replay rate, static book (marks + evaluation only)", sout.get("ticks_per_s_replay"), "ticks/s",
                   n=int(sout.get("ticks", 0)), note=f"{sout.get('days')} days"))
    res.add(latency_ns("time per market state (static book)", ssamples))
    res.extra["run_bench"] = {k: {kk: vv for kk, vv in v[0].items() if kk != "profile"} for k, v in runs.items()}
    res.extra["gc"] = gcm.summary()
    if prof is not None:
        res.profile = ["(replay with strategies, whole run_bench call incl. data loading)", *profile_lines(prof)]
    return res


__all__ = ["NAME", "TARGET", "TITLE", "run"]
