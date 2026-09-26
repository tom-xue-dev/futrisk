"""``tick2decision``: cross-process tick -> rule decision in the risk-engine (QS-07: p99 ≤ 3 ms at 1k ticks/s).

Three processes on loopback (see :mod:`futrisk.bench.world`): the fake md-gateway (``fakes`` child)
publishes ``md.tick`` at ``rate`` ticks/s round robin over the 20 instruments; the **real risk-engine**
(``futrisk svc risk-engine --shard 1`` with the timestamp hooks of :mod:`futrisk.bench.rehooks`) owns the
50 accounts of the bench book; the bench process only orchestrates.

For every measured tick: ``perf_counter_ns`` right before the publish (carried in ``corr_id``) ->
the end of the loop step that evaluated it: ``evaluate_dirty`` returned (the decision) and its effects
were executed (persist / publishes). All processes read the same QPC timeline, checked at start-up.
Conflation is part of the design (the shell keeps the newest tick per instrument per loop step): a tick
superseded before its step is measured to the evaluation that covered it (conservative).

The heartbeat's ``latency_us.tick_eval`` (drain of the loop step -> evaluation done, in-process only,
the last 2048 steps) is reported next to it for comparison.
"""

from __future__ import annotations

import time
from typing import Any

from futrisk.bench.core import BenchContext, BenchResult, Target, from_summary, gc_window, latency_ns, scalar
from futrisk.bench.scenario import WorldSpec
from futrisk.bench.world import BenchWorld, ChildError

NAME = "tick2decision"
TITLE = "Cross-process tick -> rule decision (md-gateway -> risk-engine), 1k ticks/s, 50 accounts × 20 instruments"
TARGET = "p99 ≤ 3 ms"


def measure(ctx: BenchContext, spec: WorldSpec, *, rate: float, seconds: float, warmup_s: float,
            label: str = "") -> dict[str, Any]:
    """One world: start, warm up, a measured burst, stop; returns the raw results."""
    with BenchWorld(spec, base=ctx.tmp_root, name=f"{NAME}{label}", keep=ctx.keep, echo=ctx.say) as w:
        w.start_fakes()
        w.start_re(ticks=True, profile="running" if ctx.profile else None)
        w.wait_running()
        ctx.say(f"tick2decision{label}: risk-engine RUNNING, {spec.accounts} accounts × {spec.instruments} instruments,"
                f" {spec.holdings} held each")
        assert w.fakes is not None
        n_warm = max(1, int(rate * warmup_s))
        w.fakes.call("burst", rate=rate, n=n_warm, tag="w", timeout=warmup_s * 5 + 60)
        time.sleep(0.5)
        n = max(1, int(rate * seconds))
        ctx.say(f"tick2decision{label}: {n:,} ticks at {rate:,.0f}/s")
        burst = w.fakes.call("burst", rate=rate, n=n, tag="m", timeout=seconds * 5 + 60)
        time.sleep(1.0)  # the last evaluations (and one heartbeat-fields refresh) complete
        hb = w.status(timeout_ms=3000)
        doc = w.stop_re()
    if doc is None or doc.get("ticks") is None:
        raise ChildError("the risk-engine probe wrote no results")
    return {"burst": burst, "heartbeat": hb, "probe": doc, "n": n}


def run(ctx: BenchContext) -> BenchResult:
    accounts = int(ctx.size("accounts", full=50, quick=50, tiny=10))
    holdings = int(ctx.size("holdings", full=4, quick=4, tiny=2))
    rate = float(ctx.size("rate", full=1000, quick=1000, tiny=300))
    seconds = float(ctx.size("seconds", full=30, quick=6, tiny=1))
    warmup_s = float(ctx.size("warmup_s", full=3, quick=2, tiny=0.5))
    matrix_s = float(ctx.size("matrix_seconds", full=10, quick=0, tiny=0))
    hb_ms = ctx.heartbeat_ms or 1000
    spec = WorldSpec(accounts=accounts, holdings=holdings, heartbeat_ms=hb_ms)
    res = BenchResult(name=NAME, title=TITLE, target=TARGET, quick=ctx.quick,
                      params={"rate_ticks_per_s": rate, "seconds": seconds, "warmup_s": warmup_s, "accounts": accounts,
                              "instruments": spec.instruments, "holdings_per_account": holdings,
                              "holders_per_instrument": accounts * holdings / spec.instruments,
                              "rules": "default risk_rules.yaml", "heartbeat_ms": hb_ms,
                              "processes": "fakes (md publisher) -> risk-engine-1 (production entry + hooks)"})
    out = measure(ctx, spec, rate=rate, seconds=seconds, warmup_s=warmup_s)
    _report(res, out, rate, primary=True)
    if matrix_s > 0:
        mspec = WorldSpec(accounts=accounts, holdings=spec.instruments, heartbeat_ms=hb_ms)
        try:
            mout = measure(ctx, mspec, rate=rate, seconds=matrix_s, warmup_s=warmup_s, label="-matrix")
            _report(res, mout, rate, primary=False, suffix=" [matrix: every account holds all 20]")
        except Exception as exc:  # the main result stands; the variant is informative
            res.notes.append(f"matrix variant failed: {type(exc).__name__}: {exc}")
    return res


def _report(res: BenchResult, out: dict[str, Any], rate: float, *, primary: bool, suffix: str = "") -> None:
    probe = out["probe"]
    samples = probe["ticks"]["samples"]
    n = int(out["n"])
    executed = [int(s[3]) - int(s[0]) for s in samples]
    decided = [int(s[2]) - int(s[0]) for s in samples]
    to_loop = [int(s[1]) - int(s[0]) for s in samples]
    in_loop = [int(s[3]) - int(s[1]) for s in samples]
    burst = out["burst"]
    res.add(latency_ns(f"tick publish -> decision executed (cross-process){suffix}", executed,
                       targets=[Target("p99", "<=", 3, "ms")], primary=primary,
                       note=f"{len(executed):,} of {n:,} measured ticks covered by an evaluation"))
    res.add(latency_ns(f"tick publish -> rules evaluated (evaluate_dirty returned){suffix}", decided))
    res.add(latency_ns(f"  part 1: publish -> drained by the risk-engine loop{suffix}", to_loop,
                       note="transport + time the tick waited while the loop finished its current step"))
    res.add(latency_ns(f"  part 2: drained -> decision executed (same loop step){suffix}", in_loop,
                       note="apply marks + evaluate the dirty accounts + execute the effects"))
    res.add(scalar(f"achieved tick rate{suffix}", burst.get("rate"), "ticks/s", n=int(burst.get("sent", 0)),
                   note=f"target rate {rate:,.0f}/s"))
    evals = int(probe["ticks"]["evaluations"])
    res.add(scalar(f"ticks per evaluation (conflation){suffix}", len(samples) / evals if evals else None, "ticks",
                   kind="ratio", n=evals))
    hb = out["heartbeat"]
    if hb is not None:
        lat = hb.latency_us or {}
        res.add(from_summary(f"heartbeat tick_eval (loop drain -> evaluated, in process){suffix}", lat.get("tick_eval"),
                             note="risk-engine's own window: last 2048 loop steps with marks"))
        key = "heartbeat" + (suffix.strip() or "")
        res.extra[key] = {"latency_us": {k: {"p50": v.p50, "p99": v.p99, "max": v.max, "n": v.n} for k, v in lat.items()},
                          "rates": dict(hb.rates or {}), "queues": dict(hb.queues or {}), "loop_lag_ms": hb.loop_lag_ms}
    if samples:
        t0 = min(int(s[0]) for s in samples)
        t1 = max(int(s[3]) for s in samples)
        res.extra["risk_engine_gc" + (suffix.strip() or "")] = gc_window(probe.get("gc_events", []), t0, t1)
    if probe.get("profile"):
        res.profile += [f"(risk-engine process, from RUNNING to stop){suffix}", *probe["profile"]]


__all__ = ["NAME", "TARGET", "TITLE", "measure", "run"]
