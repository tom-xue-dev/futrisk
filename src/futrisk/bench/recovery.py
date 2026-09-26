"""``recovery``: risk-engine cold start with 10k fills in risk.db (QS-11: < 3 s; QS-04 after a kill).

1. **Load**: a fresh risk-engine (production entry) starts from the SOD of the bench book (50 accounts);
   the bench sends 1-lot orders back to back, the fake trade-gateway fills each one through ``td.exec``
   FACTs, until risk.db holds ``fills`` fills of the day (plus their orders, order events, outbox rows).
   Then ``ctl.shutdown`` (clean close).
2. **Clean cold start** (primary, repeated ``restarts`` times): spawn the process again on the same home and
   time it until the phase is RUNNING: interpreter + imports + CLI, runtime start (config, sockets, risk.db
   open with outbox recovery, core), market clock, **restore from risk.db** (``load_restore`` +
   ``core.restore`` + ``post_restore``), CATCHUP (``td.exec`` subscription, ``td.query.trades`` of every
   account: all of today's trades again, deduplicated), WARMUP (position recon, fresh live marks).
3. **Crash restart**: the same after the process was killed (``TerminateProcess``, risk.db unclean).

Timestamps come from inside the process (``perf_counter_ns`` hooks on the start-up steps) and share the
bench's timeline, so "spawn -> RUNNING" starts at the bench's ``Popen``. The peers (fakes) keep running and
answer at once; with the production heartbeat (1 s) a restart may wait up to one heartbeat interval for a
peer's first heartbeat.
"""

from __future__ import annotations

import time
from typing import Any

from futrisk.bench.core import BenchContext, BenchResult, Target, durations, gc_window, now_ns, scalar
from futrisk.bench.order_rtt import OrderFlow, send_orders
from futrisk.bench.scenario import WorldSpec
from futrisk.bench.world import BenchWorld, ChildError, wait_until

NAME = "recovery"
TITLE = "risk-engine cold-start recovery with 10k fills (restore from risk.db until RUNNING)"
TARGET = "< 3 s"

_STEPS = (
    ("interpreter start", "spawn", "probe"),
    ("imports + CLI", "probe", "start_begin"),
    ("runtime start (config, sockets, risk.db, core)", "start_begin", "start_end"),
    ("market clock (sys.clock snapshot)", "start_end", "restore_begin"),
    ("restore from risk.db (load + core.restore + post_restore)", "restore_begin", "restore_end"),
    ("catch-up (td.exec subscription, td.query.trades)", "restore_end", "WARMUP"),
    ("warm-up (position recon, fresh marks)", "WARMUP", "RUNNING"),
)


def breakdown(doc: dict[str, Any]) -> dict[str, float]:
    """Seconds per start-up step from a probe document (``spawn`` = the bench's Popen)."""
    t: dict[str, int] = {"spawn": int(doc["t_spawn_ns"]), "probe": int(doc["t_probe_start_ns"])}
    t.update({k: int(v) for k, v in doc.get("marks", {}).items()})
    for phase, ts in doc.get("phases", []):
        t.setdefault(str(phase), int(ts))
    out: dict[str, float] = {}
    for name, a, b in _STEPS:
        if a in t and b in t:
            out[name] = round((t[b] - t[a]) / 1e9, 4)
    if "RUNNING" in t:
        out["TOTAL spawn -> RUNNING"] = round((t["RUNNING"] - t["spawn"]) / 1e9, 4)
        if "start_begin" in t:
            out["service start -> RUNNING"] = round((t["RUNNING"] - t["start_begin"]) / 1e9, 4)
        if "restore_begin" in t:
            out["restore begin -> RUNNING"] = round((t["RUNNING"] - t["restore_begin"]) / 1e9, 4)
    return out


def _restart(w: BenchWorld, profile: bool) -> dict[str, Any]:
    w.start_re(profile="startup" if profile else None)
    w.client("cli", fresh=True)  # a new DEALER connects at once (the old one would back off up to 1 s)
    _hb, t_seen = w.wait_running(timeout=120)
    doc = w.stop_re()
    if doc is None:
        raise ChildError("the risk-engine probe wrote no results")
    b = breakdown(doc)
    b["observed by the bench (ctl.status polling)"] = round((t_seen - int(doc["t_spawn_ns"])) / 1e9, 4)
    running = dict((str(p), int(t)) for p, t in doc.get("phases", [])).get("RUNNING")
    events = doc.get("gc_events", [])
    if running is not None:
        g = gc_window(events, int(doc["t_spawn_ns"]), running)
        b["GC pauses until RUNNING"] = round(g["pause_total_ms"] / 1000, 4)
    marks = doc.get("marks", {})
    if "restore_begin" in marks and "restore_end" in marks:
        g = gc_window(events, int(marks["restore_begin"]), int(marks["restore_end"]))
        b["GC pauses during the restore"] = round(g["pause_total_ms"] / 1000, 4)
    return {"breakdown": b, "profile": doc.get("profile") or []}


def run(ctx: BenchContext) -> BenchResult:
    fills = int(ctx.size("fills", full=10_000, quick=2_000, tiny=60))
    restarts = int(ctx.size("restarts", full=3, quick=2, tiny=1))
    crash = bool(ctx.size("crash", full=True, quick=True, tiny=False))
    accounts = int(ctx.size("accounts", full=50, quick=50, tiny=10))
    hb_ms = ctx.heartbeat_ms or 1000
    spec = WorldSpec(accounts=accounts, heartbeat_ms=hb_ms, tg_auto_fill=True, tg_fill_parts=1)
    res = BenchResult(name=NAME, title=TITLE, target=TARGET, quick=ctx.quick,
                      params={"fills": fills, "orders": fills, "accounts": accounts, "restarts": restarts,
                              "crash_restart": crash, "heartbeat_ms": hb_ms,
                              "fill_origin": "1-lot orders through the risk-engine, filled by the fake trade-gateway"})
    clean: list[dict[str, Any]] = []
    crashed: list[dict[str, Any]] = []
    with BenchWorld(spec, base=ctx.tmp_root, name=NAME, keep=ctx.keep, echo=ctx.say) as w:
        w.start_fakes()
        assert w.fakes is not None
        w.start_re()
        w.wait_running()
        ctx.say(f"recovery: loading {fills:,} fills through the risk-engine")
        t0 = now_ns()
        flow = OrderFlow(spec)
        rejected: dict[str, int] = {}
        trades = 0
        for _round in range(5):  # a few orders can be refused (races of the back-to-back flow): top up
            for o in send_orders(w, flow, fills - trades, 0.0):
                if not o["ok"]:
                    rejected[o["code"]] = rejected.get(o["code"], 0) + 1
            trades = int(w.fakes.call("tg_stats")["trades"])
            if trades >= fills:
                break
        wait_until(lambda: w.db_counts(("fills",)).get("fills", 0) >= trades, 300, 0.2,
                   f"{trades:,} fills in risk.db")
        load_s = (now_ns() - t0) / 1e9
        w.stop_re()
        counts = w.db_counts()
        db_mb = sum(p.stat().st_size for p in w.risk_db().parent.glob(w.risk_db().name + "*")) / 2**20
        res.extra["risk_db"] = {**counts, "size_mb_incl_wal": round(db_mb, 1), "load_s": round(load_s, 1),
                                "orders_rejected": rejected}
        ctx.say(f"recovery: risk.db {counts} ({db_mb:.1f} MB); clean restarts")
        for k in range(restarts):
            clean.append(_restart(w, ctx.profile and k == restarts - 1))
        if crash:
            ctx.say("recovery: kill + restart")
            try:
                w.start_re()
                w.client("cli", fresh=True)
                w.wait_running(timeout=120)
                time.sleep(0.5)
                w.kill_re()
                crashed.append(_restart(w, False))
            except Exception as exc:  # the clean restarts stand; the crash variant is informative
                res.notes.append(f"restart after kill failed: {type(exc).__name__}: {str(exc).splitlines()[0]}")
    key = "TOTAL spawn -> RUNNING"
    res.add(durations("cold start after a clean shutdown: spawn -> RUNNING", _values(clean, key),
                      targets=[Target("max", "<", 3, "s")], primary=True,
                      note=f"risk.db: {res.extra['risk_db'].get('fills', 0):,} fills, "
                           f"{res.extra['risk_db'].get('orders', 0):,} orders, "
                           f"{res.extra['risk_db'].get('order_events', 0):,} order events"))
    res.add(durations("in-service part: RiskEngineService.start -> RUNNING",
                      _values(clean, "service start -> RUNNING"), targets=[Target("max", "<", 3, "s")]))
    res.add(durations("restore from risk.db (load_restore + core.restore + post_restore)",
                      _values(clean, _STEPS[4][0])))
    if crashed:
        res.add(durations("restart after kill (unclean risk.db): spawn -> RUNNING", _values(crashed, key),
                          targets=[Target("max", "<", 3, "s")]))
    res.add(scalar("fills in risk.db", res.extra["risk_db"].get("fills"), "fills", kind="count"))
    res.extra["clean_restarts"] = [r["breakdown"] for r in clean]
    res.extra["crash_restarts"] = [r["breakdown"] for r in crashed]
    if clean and clean[-1]["profile"]:
        res.profile = ["(risk-engine process, last clean restart: RiskEngineService.start -> RUNNING)",
                       *clean[-1]["profile"]]
    return res


def _values(runs: list[dict[str, Any]], key: str) -> list[float]:
    return [float(v) for r in runs if (v := r["breakdown"].get(key)) is not None]


__all__ = ["NAME", "TARGET", "TITLE", "breakdown", "run"]
