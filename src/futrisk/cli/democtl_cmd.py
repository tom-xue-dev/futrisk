"""``futrisk fault ... | shock | scenario ... | clock ...``: demo control (02 §9.5, §9.6 ``/demo/*``, §8.8).

Everything goes through api-gateway's ``/demo/*`` proxies (ADMIN key, audited; mutations carry an
``Idempotency-Key``); api-gateway answers ``403 FORBIDDEN`` unless ``app.mode=demo``. Only when an
older api-gateway lacks one of the read routes (``GET /demo/faults``, ``GET /demo/state``, ``GET
/demo/scenarios``) or ``DELETE /demo/scenario`` -- it answers 405 or a router 404 -- does the
command fall back to the sim-exchange control API itself (``http://{sim.control.host}:{port}
/control``, 127.0.0.1 only, demo mode only; 02 §9.5 names the CLI as a caller).

==================================================  ==================================================
command                                             call
==================================================  ==================================================
``fault inject TYPE [--target] [--param k=v]``      ``POST /demo/faults {type, target?, params, duration_s?}``
``fault list [--all]``                              ``GET /demo/faults`` (``{items, history}``)
``fault clear ID | --all``                          ``DELETE /demo/faults/{id}`` (each listed active fault for
                                                    ``--all``: there is no bulk delete)
``shock --sector|--instrument --pct``               ``POST /demo/shock {sector?|instruments?, pct, duration_s, shape}``
``scenario list``                                   ``GET /demo/scenarios`` + ``GET /demo/state`` (the last run)
``scenario run NAME [--var k=v] [--speed]``         ``POST /demo/scenario {name, speed?, vars?}``
``scenario cancel``                                 ``DELETE /demo/scenario``
``clock show``                                      ``GET /system/status`` (``clock``, ``trading_day``)
``clock jump --to TIME|pre-close|next-open``        ``POST /demo/clock {target, at?}``
==================================================  ==================================================

Times are Beijing market time (``Asia/Shanghai``) whatever the machine's time zone; the market
clock only moves forward.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

import typer

from futrisk.cli.toolkit import (
    DEMO_HINT,
    ApiOpt,
    HomeOpt,
    JsonOpt,
    Session,
    as_dict,
    bj_time,
    dicts,
    dump_json,
    fmt,
    kv,
    parse_kv,
    pct,
    session,
    span,
    state_style,
)

fault_app = typer.Typer(help="Inject, list and clear sim-exchange faults (demo mode, ADMIN)", no_args_is_help=True)
scenario_app = typer.Typer(help="Scenario scripts of the virtual exchange (demo mode, ADMIN)", no_args_is_help=True)
clock_app = typer.Typer(help="Show or jump the market clock (jumps: demo mode, ADMIN)", no_args_is_help=True)

#: 02 §9.5 fault table (validated again by sim-exchange): type -> (targets, params)
FAULTS: dict[str, tuple[str, str]] = {
    "FEED_DISCONNECT": ("feed_a | feed_b", "refuse_reconnect_s, mode=close|silent"),
    "FEED_GAP": ("feed_a", "drop_count"),
    "FEED_DUPLICATE": ("feed_a | feed_b", "ratio"),
    "FEED_REORDER": ("feed_a | feed_b", "window"),
    "FEED_LATENCY": ("feed_a | feed_b", "ms"),
    "FEED_FREEZE": ("(none) | feed_a | feed_b", "instrument"),
    "BAD_TICK": ("(none) | feed_a | feed_b", "instrument, offset_pct, with_volume"),
    "MALFORMED": ("feed_b", "ratio, mode=body|record"),
    "SIM_RESTART": ("(none)", "-"),
    "BROKER_FORCE_CLOSE": ("(none)", "account_id, instrument_id, qty, posi_direction"),
    "BROKER_DISCONNECT": ("(none)", "seconds"),
}
CLOCK_KEYWORDS = {"pre-close": "PRE_CLOSE", "pre_close": "PRE_CLOSE", "next-open": "NEXT_OPEN", "next_open": "NEXT_OPEN"}
_HHMM = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$")


def _seg(value: str) -> str:
    from futrisk.cli.apiclient import seg

    return seg(value)


def _demo(home: str | None, api: str | None, as_json: bool) -> Any:
    return session(home, api, as_json, forbidden_hint=DEMO_HINT)


def _fault_types_help() -> str:
    return "; ".join(f"{t} ({targets}) {{{params}}}" for t, (targets, params) in FAULTS.items())


# ====================================================================== faults
@fault_app.command("inject")
def fault_inject(
    fault_type: str = typer.Argument(..., metavar="TYPE", help="Fault type, e.g. FEED_DISCONNECT (all types below)"),
    target: str | None = typer.Option(None, "--target", "-t", help="feed_a | feed_b (per type)"),
    param: list[str] | None = typer.Option(
        None, "--param", "-p", help="Fault parameter KEY=VALUE (repeatable; JSON values: 0.2, true, \"x\")"
    ),
    duration: float | None = typer.Option(None, "--duration", "-d", min=0, help="Seconds (default: until cleared)"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Inject a fault into the virtual exchange (POST /demo/faults).

    TYPE (--target) {params}: FEED_DISCONNECT (feed_a|feed_b) {refuse_reconnect_s, mode=close|silent};
    FEED_GAP (feed_a) {drop_count}; FEED_DUPLICATE (feed_a|feed_b) {ratio}; FEED_REORDER {window};
    FEED_LATENCY {ms}; FEED_FREEZE {instrument}; BAD_TICK {instrument, offset_pct, with_volume};
    MALFORMED (feed_b) {ratio}; SIM_RESTART; BROKER_FORCE_CLOSE {account_id, instrument_id, qty};
    BROKER_DISCONNECT {seconds}. Example: fault inject FEED_LATENCY -t feed_a -p ms=800 -d 60
    """
    kind = fault_type.strip().upper()
    if kind not in FAULTS:
        raise typer.BadParameter(f"unknown fault type {fault_type!r}; known: {_fault_types_help()}", param_hint="TYPE")
    params = parse_kv(param, "--param")
    with _demo(home, api, as_json) as s:
        body: dict[str, Any] = {"type": kind, "params": params}
        if target:
            body["target"] = target.lower()
        if duration is not None:
            body["duration_s"] = duration
        reply = s.api.post("/demo/faults", body)
        if s.out.json:
            s.out.raw(reply.raw)
            return
        data = as_dict(reply.data)
        fault = as_dict(data.get("fault"))
        lasts = f"{fmt(fault.get('duration_s'))}s" if fault.get("duration_s") is not None else "until cleared or done"
        state = data.get("state") or fault.get("state")
        s.out.ok(f"fault {fmt(data.get('fault_id'))} {kind} injected: {fmt(state)}")
        s.out.fields([
            ("target", fault.get("target")),
            ("params", kv(fault.get("params") if isinstance(fault.get("params"), dict) else params)),
            ("lasts", lasts),
        ])


@dataclass
class Proxied:
    """A ``/demo`` answer: the data, the JSON to print with ``--json`` and, for the sim fallback, a note."""

    data: Any
    raw: bytes
    fallback: str | None = None


def proxied(s: Session, method: str, path: str, sim_path: str,
            shape: Callable[[Any], Any] | None = None) -> Proxied:
    """``method /api/v1{path}`` through api-gateway (authenticated, audited). Only an api-gateway
    without the route (older version: 405 or a router 404) makes it call the sim-exchange control
    API ``sim_path`` directly; ``shape`` turns that answer into the gateway's data shape."""
    reply = s.api.get(path, check=False) if method == "GET" else s.api.delete(path, check=False)
    if reply.ok:
        return Proxied(reply.data, reply.raw)
    if not reply.route_missing:
        raise reply.error()
    with s.sim() as sim:
        doc, _raw = sim.call(method, sim_path)
    data = shape(doc) if shape is not None else doc
    note = f"via the sim-exchange control API: this api-gateway has no {method} /api/v1{path}"
    envelope = {"ok": True, "code": "OK", "message": note, "data": data}
    return Proxied(data, dump_json(envelope).encode("utf-8"), note)


def _faults_of_state(doc: Any) -> dict[str, Any]:
    state = as_dict(doc)
    return {"items": dicts(state.get("faults")), "history": dicts(state.get("fault_history"))}


def demo_faults(s: Session) -> Proxied:
    """``GET /demo/faults`` -> ``{items, history}``."""
    return proxied(s, "GET", "/demo/faults", "/state", _faults_of_state)


@fault_app.command("list")
def fault_list(
    show_all: bool = typer.Option(False, "--all", "-a", help="Also ended faults (recent history)"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Active faults of the virtual exchange (GET /demo/faults; demo mode)."""
    with _demo(home, api, as_json) as s:
        got = demo_faults(s)
        if s.out.json:
            s.out.raw(got.raw)
            return
        data = as_dict(got.data)
        faults = dicts(data.get("items")) + (dicts(data.get("history")) if show_all else [])
        s.out.table(
            ["FAULT ID", "TYPE", "TARGET", "PARAMS", "STATE", "REMAINING", "CREATED"],
            [
                [f.get("fault_id"), f.get("type"), f.get("target"),
                 kv(f.get("params") if isinstance(f.get("params"), dict) else None),
                 (f.get("state"), state_style(f.get("state"))),
                 f"{fmt(f.get('remaining_s'))}s" if f.get("remaining_s") is not None else None,
                 bj_time(f.get("created_ns"))]
                for f in faults
            ],
            title=f"faults ({len(faults)}{' incl. ended' if show_all else ' active'})",
        )
        if got.fallback:
            s.out.dim(got.fallback)


@fault_app.command("clear")
def fault_clear(
    fault_id: str | None = typer.Argument(None, help="Fault id (from `fault list`)"),
    clear_all: bool = typer.Option(False, "--all", "-a", help="Clear every active fault"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Remove a fault (DELETE /demo/faults/{id}); --all removes every active one, one by one."""
    if (fault_id is None) == (not clear_all):
        raise typer.BadParameter("give a FAULT_ID or --all (not both)")
    with _demo(home, api, as_json) as s:
        if fault_id is not None:
            reply = s.api.delete(f"/demo/faults/{_seg(fault_id)}")
            if s.out.json:
                s.out.raw(reply.raw)
                return
            s.out.ok(f"fault {fault_id} cleared")
            return
        listed = demo_faults(s)
        if listed.fallback and not s.out.json:
            s.out.dim(listed.fallback)
        active = [str(f["fault_id"]) for f in dicts(as_dict(listed.data).get("items")) if f.get("fault_id")]
        results: list[dict[str, Any]] = []
        for fid in active:
            r = s.api.delete(f"/demo/faults/{_seg(fid)}", check=False)
            results.append({"fault_id": fid, "ok": r.ok, "code": r.code, "message": r.message})
        failed = [r for r in results if not r["ok"]]
        if s.out.json:
            s.out.obj({"ok": not failed, "cleared": [r["fault_id"] for r in results if r["ok"]], "results": results})
        elif not results:
            s.out.line("no active fault")
        else:
            s.out.table(
                ["FAULT ID", "RESULT", "CODE", "MESSAGE"],
                [[r["fault_id"], ("cleared", "green") if r["ok"] else ("FAILED", "bold red"), r["code"], r["message"]]
                 for r in results],
                title=f"cleared {len(results) - len(failed)}/{len(results)} fault(s)",
            )
        if failed:
            raise typer.Exit(1)


# ====================================================================== shock
def _pct_value(text: str) -> float:
    """``-0.03`` (ratio) or ``-3%`` -> -0.03."""
    t = text.strip()
    try:
        value = Decimal(t[:-1]) / 100 if t.endswith("%") else Decimal(t)
    except InvalidOperation:
        raise typer.BadParameter(f"expected a ratio like -0.03 or a percentage like -3%, got {text!r}",
                                 param_hint="--pct") from None
    if not value.is_finite() or value == 0 or value <= -1:
        raise typer.BadParameter("must be a non-zero ratio above -1 (-0.03 = -3%)", param_hint="--pct")
    return float(value)


def shock(
    pct_text: str = typer.Option(..., "--pct", help="Price move as a ratio (-0.03 = -3%) or a percentage (-3%)"),
    sector: str | None = typer.Option(None, "--sector", "-s", help="Sector, e.g. black"),
    instrument: list[str] | None = typer.Option(None, "--instrument", "-i", help="Instrument (repeatable or a,b)"),
    duration: float = typer.Option(0.0, "--duration", "-d", min=0, help="Seconds over which the move unfolds"),
    shape: Literal["STEP", "LINEAR"] = typer.Option("STEP", "--shape", case_sensitive=False, help="STEP | LINEAR"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Price shock on a sector or instruments (POST /demo/shock; demo mode)."""
    instruments = [i.strip() for item in instrument or [] for i in item.split(",") if i.strip()]
    if bool(sector) == bool(instruments):
        raise typer.BadParameter("give either --sector or --instrument", param_hint="--sector/--instrument")
    value = _pct_value(pct_text)
    with _demo(home, api, as_json) as s:
        body: dict[str, Any] = {"pct": value, "duration_s": duration, "shape": shape.upper()}
        if sector:
            body["sector"] = sector
        else:
            body["instruments"] = instruments
        reply = s.api.post("/demo/shock", body)
        if s.out.json:
            s.out.raw(reply.raw)
            return
        data = as_dict(reply.data)
        sh = as_dict(data.get("shock"))
        what = f"sector {sector}" if sector else ", ".join(instruments)
        targets = sh.get("targets")
        count = f" ({len(targets)} instruments)" if isinstance(targets, list) else ""
        s.out.ok(f"shock {fmt(data.get('shock_id'))}: {what}{count} {pct(value)} {shape.upper()} over {duration:g}s")


# ====================================================================== scenarios
@scenario_app.command("list")
def scenario_list(home: str | None = HomeOpt, api: str | None = ApiOpt, as_json: bool = JsonOpt) -> None:
    """Available scenario scripts and the last run (GET /demo/scenarios, GET /demo/state; demo mode)."""
    with _demo(home, api, as_json) as s:
        got = proxied(s, "GET", "/demo/scenarios", "/scenarios")
        if s.out.json:
            s.out.raw(got.raw)
            return
        items = dicts(got.data)
        s.out.table(["NAME", "DESCRIPTION"], [[x.get("name"), x.get("description")] for x in items],
                    title=f"scenarios ({len(items)})")
        run = as_dict(proxied(s, "GET", "/demo/state", "/state").data).get("scenario")
        if got.fallback:
            s.out.dim(got.fallback)
        if isinstance(run, dict):
            err = f", error: {run.get('error')}" if run.get("error") else ""
            s.out.line(
                f"last run: {run.get('name')} {run.get('status')} step {fmt(run.get('step'))}/{fmt(run.get('steps_total'))}"
                f" (run {run.get('run_id')}, started {bj_time(run.get('started_ns'))}){err}",
                state_style(run.get("status")) or None,
            )


@scenario_app.command("run")
def scenario_run(
    name: str = typer.Argument(..., help="Scenario name (config/scenarios/<name>.yaml), e.g. breaker"),
    var: list[str] | None = typer.Option(None, "--var", "-v", help="Scenario variable KEY=VALUE (repeatable)"),
    speed: float | None = typer.Option(None, "--speed", min=0.01, help="Time scale (2 = twice as fast)"),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Start a scenario script (POST /demo/scenario; demo mode)."""
    variables = parse_kv(var, "--var")
    with _demo(home, api, as_json) as s:
        body: dict[str, Any] = {"name": name}
        if speed is not None:
            body["speed"] = speed
        if variables:
            body["vars"] = variables
        reply = s.api.post("/demo/scenario", body)
        if s.out.json:
            s.out.raw(reply.raw)
            return
        data = as_dict(reply.data)
        s.out.ok(f"scenario {fmt(data.get('name') or name)} started (run {fmt(data.get('run_id'))})")


@scenario_app.command("cancel")
def scenario_cancel(home: str | None = HomeOpt, api: str | None = ApiOpt, as_json: bool = JsonOpt) -> None:
    """Cancel the running scenario (DELETE /demo/scenario; demo mode)."""
    with _demo(home, api, as_json) as s:
        got = proxied(s, "DELETE", "/demo/scenario", "/scenario")
        if s.out.json:
            s.out.raw(got.raw)
            return
        if got.fallback:
            s.out.dim(got.fallback)
        run = as_dict(got.data)
        s.out.ok(f"scenario {fmt(run.get('name'))} ({fmt(run.get('run_id'))}): {fmt(run.get('status'))} "
                 f"at step {fmt(run.get('step'))}/{fmt(run.get('steps_total'))}")


# ====================================================================== clock
@clock_app.command("show")
def clock_show(home: str | None = HomeOpt, api: str | None = ApiOpt, as_json: bool = JsonOpt) -> None:
    """Market clock, trading day and app mode (GET /system/status)."""
    with session(home, api, as_json) as s:
        reply = s.api.get("/system/status")
        if s.out.json:
            s.out.raw(reply.raw)
            return
        data = as_dict(reply.data)
        clk = as_dict(data.get("clock"))
        now = clk.get("market_now_ns")
        offset = clk.get("offset_ns")
        wall = int(now) - int(offset) if isinstance(now, int) and isinstance(offset, int) else None
        s.out.fields([
            ("market time", f"{bj_time(now)} (Beijing)"),
            ("trading day", data.get("trading_day")),
            ("trading mode", data.get("trading_mode")),
            ("clock", f"{fmt(clk.get('mode'))} v{fmt(clk.get('version'))}, offset {span(offset)}"),
            ("wall clock", f"{bj_time(wall)} (Beijing)"),
            ("app mode", data.get("mode")),
            ("kill switch", ("ON", "bold red") if data.get("kill_switch") else data.get("kill_switch")),
        ])


def parse_jump(text: str, market_now_ns: int | None) -> tuple[str, str | None]:
    """``--to`` -> ``(target, at)``: keywords, ``YYYY-MM-DD HH:MM[:SS]`` (Beijing), ISO with an offset,
    or ``HH:MM[:SS]`` = the next such time after the market clock."""
    from futrisk.common.clock import BJ, ns_to_bj

    t = text.strip()
    keyword = CLOCK_KEYWORDS.get(t.lower())
    if keyword:
        return keyword, None
    m = _HHMM.match(t)
    if m:
        if market_now_ns is None:
            raise typer.BadParameter("HH:MM needs the market clock", param_hint="--to")
        now = ns_to_bj(market_now_ns)
        try:
            at = now.replace(hour=int(m[1]), minute=int(m[2]), second=int(m[3] or 0), microsecond=0)
        except ValueError:
            raise typer.BadParameter(f"not a time: {text!r}", param_hint="--to") from None
        if at <= now:
            at += timedelta(days=1)
        return "AT", at.isoformat()
    try:
        dt = datetime.fromisoformat(t)
    except ValueError:
        raise typer.BadParameter(
            f"expected 'YYYY-MM-DD HH:MM[:SS]' (Beijing), HH:MM, pre-close or next-open; got {text!r}",
            param_hint="--to",
        ) from None
    dt = dt.replace(tzinfo=BJ) if dt.tzinfo is None else dt
    return "AT", dt.isoformat()


@clock_app.command("jump")
def clock_jump(
    to: str = typer.Option(
        ..., "--to", help="Beijing time 'YYYY-MM-DD HH:MM[:SS]', HH:MM (next one), pre-close or next-open"
    ),
    home: str | None = HomeOpt,
    api: str | None = ApiOpt,
    as_json: bool = JsonOpt,
) -> None:
    """Move the market clock forward (POST /demo/clock; demo mode): pause, move, converge, resume."""
    needs_now = bool(_HHMM.match(to.strip()))
    if not needs_now:
        target, at = parse_jump(to, None)
    with _demo(home, api, as_json) as s:
        if needs_now:
            target, at = parse_jump(to, s.market_now_ns())
        body: dict[str, Any] = {"target": target}
        if at is not None:
            body["at"] = at
        reply = s.api.post("/demo/clock", body, check=False)
        if s.out.json:
            s.out.raw(reply.raw)
            if not reply.ok:
                raise typer.Exit(1)
            return
        data = as_dict(reply.data)
        steps = dicts(data.get("steps"))
        if not reply.ok:
            if steps:
                _steps_table(s, steps)
            raise reply.error()
        clk = as_dict(data.get("clock"))
        s.out.ok(f"market clock -> {bj_time(clk.get('market_now_ns'))} (Beijing), version {fmt(clk.get('version'))}")
        lagging = data.get("lagging") or []
        if lagging:
            s.out.warn(f"not yet on the new clock after {fmt(data.get('waited_ms'))} ms: {', '.join(map(str, lagging))}")
        else:
            s.out.dim(f"every service follows the new clock ({fmt(data.get('waited_ms'))} ms)")


def _steps_table(s: Session, steps: list[dict[str, Any]]) -> None:
    s.out.table(
        ["STEP", "ACTION", "RESULT", "DETAIL"],
        [[x.get("step"), x.get("action"), ("ok", "green") if x.get("ok") else ("FAILED", "bold red"),
          kv(x.get("detail")) if isinstance(x.get("detail"), dict) else x.get("detail")] for x in steps],
        title="clock jump steps",
    )


__all__ = ["FAULTS", "Proxied", "clock_app", "demo_faults", "fault_app", "parse_jump", "proxied", "scenario_app", "shock"]
