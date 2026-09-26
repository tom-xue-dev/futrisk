"""``futrisk fault | shock | scenario | clock`` (02 §9.5, §9.6 ``/demo/*``, §8.8) and demo-only 403 handling."""

from __future__ import annotations

import json
from datetime import datetime

from clitools_support import KEY, Cli, is_uuid

from futrisk.common.clock import BJ, bj_to_ns

MARKET = bj_to_ns(datetime(2026, 9, 28, 14, 58, 0, tzinfo=BJ))
OFFSET = 2 * 86_400 * 10**9 + 5 * 3600 * 10**9
FAULT = {"fault_id": "f-1", "type": "FEED_DISCONNECT", "target": "feed_a",
         "params": {"refuse_reconnect_s": 20.0, "mode": "silent"}, "duration_s": 30.0, "state": "ACTIVE",
         "created_ns": MARKET, "remaining_s": 29.5, "detail": {}}


# ---------------------------------------------------------------------------- faults
def test_fault_inject_sends_typed_params(cli: Cli) -> None:
    cli.api.on("POST", "/demo/faults", {"fault_id": "f-1", "state": "ACTIVE", "fault": FAULT})
    r = cli.invoke("fault", "inject", "feed_disconnect", "--target", "FEED_A", "-p", "refuse_reconnect_s=20",
                   "-p", "mode=silent", "--duration", "30")
    assert r.exit_code == 0, r.output
    req = cli.api.last("POST", "/demo/faults")
    assert req.body == {"type": "FEED_DISCONNECT", "params": {"refuse_reconnect_s": 20, "mode": "silent"},
                        "target": "feed_a", "duration_s": 30.0}
    assert is_uuid(req.idem)
    assert "fault f-1 FEED_DISCONNECT injected: ACTIVE" in r.output and "30.0s" in r.output

    cli.api.on("POST", "/demo/faults", {"fault_id": "f-2", "state": "ACTIVE", "fault": {}})
    r = cli.invoke("fault", "inject", "BAD_TICK", "-p", "instrument=rb2701", "-p", "offset_pct=0.05",
                   "-p", "with_volume=true")
    assert r.exit_code == 0, r.output
    second = cli.api.last("POST", "/demo/faults")
    assert second.body == {"type": "BAD_TICK", "params": {"instrument": "rb2701", "offset_pct": 0.05,
                                                          "with_volume": True}}
    assert second.idem != req.idem


def test_fault_inject_usage_errors(cli: Cli) -> None:
    r = cli.invoke("fault", "inject", "FEED_EXPLODE")
    assert r.exit_code == 2 and "FEED_DISCONNECT" in r.output
    assert cli.invoke("fault", "inject", "FEED_GAP", "-p", "drop_count").exit_code == 2
    assert cli.invoke("fault", "inject", "FEED_GAP", "--duration", "-1").exit_code == 2
    assert cli.api.requests == []


def test_fault_list_and_clear(cli: Cli) -> None:
    f2 = {**FAULT, "fault_id": "f-2", "type": "FEED_LATENCY", "params": {"ms": 800.0}, "duration_s": None,
          "remaining_s": None}
    old = {**FAULT, "fault_id": "f-0", "state": "EXPIRED", "remaining_s": None}
    cli.api.on("GET", "/demo/faults", {"items": [FAULT, f2], "history": [old]})
    r = cli.invoke("fault", "list")
    assert r.exit_code == 0, r.output
    assert "f-1" in r.output and "f-2" in r.output and "f-0" not in r.output
    assert "refuse_reconnect_s=20.0 mode=silent" in r.output and "29.5s" in r.output and "ms=800.0" in r.output
    assert "2026-09-28 14:58:00" in r.output  # created, Beijing time
    assert "sim-exchange control API" not in r.output  # no fallback
    req = cli.api.last("GET", "/demo/faults")
    assert req.headers["x-api-key"] == KEY  # authenticated (and audited by api-gateway)
    assert cli.api.calls("GET", "/control/state") == []
    r = cli.invoke("fault", "list", "--all")
    assert "f-0" in r.output and "EXPIRED" in r.output
    r, doc = cli.json("fault", "list")
    assert [f["fault_id"] for f in doc["data"]["items"]] == ["f-1", "f-2"]  # the gateway envelope as is
    assert [f["fault_id"] for f in doc["data"]["history"]] == ["f-0"]

    cli.api.on("DELETE", "/demo/faults/f-1", None)
    cli.api.on("DELETE", "/demo/faults/f-2", None, code="NOT_FOUND", message="no active fault f-2")
    r = cli.invoke("fault", "clear", "--all")
    assert r.exit_code == 1  # one of the two failed
    assert "cleared 1/2 fault(s)" in r.output and "no active fault f-2" in r.output
    keys = [c.idem for c in cli.api.calls("DELETE")]
    assert len(keys) == 2 and all(is_uuid(k) for k in keys) and keys[0] != keys[1]

    r = cli.invoke("fault", "clear", "f-1")
    assert r.exit_code == 0 and "fault f-1 cleared" in r.output
    assert cli.invoke("fault", "clear").exit_code == 2
    assert cli.invoke("fault", "clear", "f-1", "--all").exit_code == 2
    assert [c for c in cli.api.requests if c.port == 7004] == []  # never the sim directly


# ---------------------------------------------------------------------------- shock / scenarios
def test_shock_bodies_and_validation(cli: Cli) -> None:
    targets = ["rb2701", "rb2705", "hc2701", "i2701", "j2701"]
    cli.api.on("POST", "/demo/shock", {"shock_id": "s-1", "shock": {"targets": targets}})
    r = cli.invoke("shock", "--sector", "black", "--pct", "-3%", "--duration", "60", "--shape", "linear")
    assert r.exit_code == 0, r.output
    req = cli.api.last("POST", "/demo/shock")
    assert req.body == {"pct": -0.03, "duration_s": 60.0, "shape": "LINEAR", "sector": "black"} and is_uuid(req.idem)
    assert "shock s-1: sector black (5 instruments) -3.00% LINEAR over 60s" in r.output

    r = cli.invoke("shock", "-i", "rb2701,hc2701", "-i", "i2701", "--pct", "-0.02")
    assert r.exit_code == 0, r.output
    assert cli.api.last("POST", "/demo/shock").body == {
        "pct": -0.02, "duration_s": 0.0, "shape": "STEP", "instruments": ["rb2701", "hc2701", "i2701"]}

    for bad in (["--pct", "-0.02"], ["--sector", "black", "-i", "rb2701", "--pct", "-0.02"],
                ["--sector", "black", "--pct", "0"], ["--sector", "black", "--pct", "-100%"],
                ["--sector", "black", "--pct", "abc"]):
        assert cli.invoke("shock", *bad).exit_code == 2, bad
    assert len(cli.api.calls("POST", "/demo/shock")) == 2


def test_scenario_list_run_and_cancel(cli: Cli) -> None:
    cli.api.on("GET", "/demo/scenarios", [{"name": "breaker", "description": "ACC3 crosses the daily-loss levels"},
                                          {"name": "feed_outage", "description": "Feed A down"}])
    run = {"run_id": "run-1", "name": "breaker", "status": "RUNNING", "step": 2, "steps_total": 7,
           "started_ns": MARKET, "error": None}
    cli.api.on("GET", "/demo/state", {"faults": [], "scenario": run})
    r = cli.invoke("scenario", "list")
    assert r.exit_code == 0, r.output
    assert "feed_outage" in r.output and "ACC3 crosses the daily-loss levels" in r.output
    assert "last run: breaker RUNNING step 2/7" in r.output
    r, doc = cli.json("scenario", "list")
    assert [x["name"] for x in doc["data"]] == ["breaker", "feed_outage"]

    cli.api.on("POST", "/demo/scenario", {"run_id": "run-2", "name": "breaker"})
    r = cli.invoke("scenario", "run", "breaker", "--var", "leg1_pct=-0.03", "-v", "sector=black", "--speed", "2")
    assert r.exit_code == 0, r.output
    req = cli.api.last("POST", "/demo/scenario")
    assert req.body == {"name": "breaker", "speed": 2.0, "vars": {"leg1_pct": -0.03, "sector": "black"}}
    assert is_uuid(req.idem) and "scenario breaker started (run run-2)" in r.output

    cli.api.on("DELETE", "/demo/scenario", {**run, "status": "CANCELED", "step": 3})
    r = cli.invoke("scenario", "cancel")
    assert r.exit_code == 0 and "breaker (run-1): CANCELED at step 3/7" in r.output
    cancel = cli.api.last("DELETE", "/demo/scenario")
    assert is_uuid(cancel.idem) and cancel.idem != req.idem
    assert [c for c in cli.api.requests if c.port == 7004] == []  # never the sim directly


def test_scenario_cancel_without_a_run(cli: Cli) -> None:
    """A route's own NOT_FOUND is an answer, not a missing route: no fallback to the sim."""
    cli.api.on("DELETE", "/demo/scenario", None, code="NOT_FOUND", message="no scenario has been started")
    r = cli.invoke("scenario", "cancel")
    assert r.exit_code == 1 and "NOT_FOUND (HTTP 404): no scenario has been started" in r.output
    assert cli.api.calls("DELETE", "/control/scenario") == []


def test_an_older_gateway_falls_back_to_the_sim_control_api(cli: Cli) -> None:
    # no GET /demo/faults (router 404 "unknown API path"), DELETE /demo/scenario answered 405
    cli.api.on_sim("GET", "/state", {"faults": [FAULT], "fault_history": [], "scenario": None})
    r = cli.invoke("fault", "list")
    assert r.exit_code == 0, r.output
    assert "f-1" in r.output and "via the sim-exchange control API: this api-gateway has no GET" in r.output
    r, doc = cli.json("fault", "list")
    assert doc["ok"] is True and [f["fault_id"] for f in doc["data"]["items"]] == ["f-1"]
    assert "via the sim-exchange control API" in doc["message"]

    cli.api.on("DELETE", "/demo/faults/f-1", None)
    r = cli.invoke("fault", "clear", "--all")  # the listing falls back, the delete stays on api-gateway
    assert r.exit_code == 0 and "cleared 1/1 fault(s)" in r.output
    assert is_uuid(cli.api.last("DELETE", "/demo/faults/f-1").idem)

    cli.api.on("DELETE", "/demo/scenario", None, code="BAD_REQUEST", status=405, message="Method Not Allowed")
    cli.api.on_sim("DELETE", "/scenario", {"run_id": "run-1", "name": "breaker", "status": "CANCELED", "step": 1,
                                           "steps_total": 7})
    r = cli.invoke("scenario", "cancel")
    assert r.exit_code == 0 and "CANCELED at step 1/7" in r.output
    assert len(cli.api.calls("DELETE", "/control/scenario")) == 1

    cli.api.on_sim("GET", "/scenarios", [{"name": "breaker", "description": "x"}])
    r = cli.invoke("scenario", "list")  # scenarios and state both from the sim
    assert r.exit_code == 0 and "breaker" in r.output


# ---------------------------------------------------------------------------- clock
def test_clock_show(cli: Cli) -> None:
    cli.api.on("GET", "/system/status", {"mode": "demo", "trading_day": "20260928", "trading_mode": "OPEN",
                                         "kill_switch": False,
                                         "clock": {"mode": "DEMO", "offset_ns": OFFSET, "version": 3,
                                                   "market_now_ns": MARKET}})
    r = cli.invoke("clock", "show")
    assert r.exit_code == 0, r.output
    out = r.output
    assert "2026-09-28 14:58:00 (Beijing)" in out  # whatever the machine's time zone (UTC+9 here)
    assert "DEMO v3, offset +2d 05:00:00" in out and "2026-09-26 09:58:00" in out and "20260928" in out


def _jumped(req: object) -> dict[str, object]:
    return {"ok": True, "code": "OK", "message": "", "data": {
        "clock": {"mode": "DEMO", "offset_ns": OFFSET, "version": 4, "market_now_ns": MARKET},
        "steps": [], "waited_ms": 120, "converged": True, "lagging": []}}


def test_clock_jump_targets(cli: Cli) -> None:
    cli.api.on("POST", "/demo/clock", handler=_jumped)
    r = cli.invoke("clock", "jump", "--to", "2026-09-28 14:58")
    assert r.exit_code == 0, r.output
    first = cli.api.last("POST", "/demo/clock")
    assert first.body == {"target": "AT", "at": "2026-09-28T14:58:00+08:00"} and is_uuid(first.idem)
    assert "market clock -> 2026-09-28 14:58:00 (Beijing), version 4" in r.output and "120 ms" in r.output

    assert cli.invoke("clock", "jump", "--to", "pre-close").exit_code == 0
    assert cli.api.last("POST", "/demo/clock").body == {"target": "PRE_CLOSE"}
    assert cli.invoke("clock", "jump", "--to", "NEXT_OPEN").exit_code == 0
    assert cli.api.last("POST", "/demo/clock").body == {"target": "NEXT_OPEN"}

    # HH:MM = the next such Beijing time after the market clock (15:10 -> tomorrow 14:58)
    cli.api.on("GET", "/health", {"trading_day": "20260929", "ts_ns": bj_to_ns(datetime(2026, 9, 28, 15, 10, tzinfo=BJ))})
    assert cli.invoke("clock", "jump", "--to", "14:58").exit_code == 0
    assert cli.api.last("POST", "/demo/clock").body == {"target": "AT", "at": "2026-09-29T14:58:00+08:00"}
    assert cli.invoke("clock", "jump", "--to", "21:00").exit_code == 0
    assert cli.api.last("POST", "/demo/clock").body == {"target": "AT", "at": "2026-09-28T21:00:00+08:00"}

    keys = [c.idem for c in cli.api.calls("POST", "/demo/clock")]
    assert len(set(keys)) == len(keys) == 5
    assert cli.invoke("clock", "jump", "--to", "tomorrow").exit_code == 2
    assert cli.invoke("clock", "jump", "--to", "25:00").exit_code == 2


def test_clock_jump_failure_shows_the_steps(cli: Cli) -> None:
    steps = [{"step": 1, "action": "sim.pause", "ok": True, "detail": None},
             {"step": 2, "action": "svc.clock", "ok": False, "detail": {"code": "CONFLICT", "message": "not after"}},
             {"step": 5, "action": "sim.resume", "ok": True, "detail": None}]
    cli.api.on("POST", "/demo/clock", {"clock": None, "steps": steps, "waited_ms": 0, "converged": True,
                                       "lagging": []}, code="CONFLICT", message="svc.clock failed: not after")
    r = cli.invoke("clock", "jump", "--to", "2026-09-28 09:00")
    assert r.exit_code == 1
    assert "svc.clock" in r.output and "FAILED" in r.output and "CONFLICT (HTTP 409): svc.clock failed" in r.output


# ---------------------------------------------------------------------------- demo-only 403
def test_demo_commands_outside_demo_mode(prod_cli: Cli) -> None:
    r = prod_cli.invoke("fault", "inject", "SIM_RESTART")
    assert r.exit_code == 1
    assert "FORBIDDEN (HTTP 403): demo endpoints are only available in demo mode" in r.output
    assert "hint: demo control needs app.mode=demo" in r.output

    r = prod_cli.invoke("scenario", "run", "breaker", "--json")
    assert r.exit_code == 1
    assert json.loads(r.stdout)["code"] == "FORBIDDEN"

    for args in (("fault", "list"), ("scenario", "list"), ("scenario", "cancel"), ("fault", "clear", "--all")):
        r = prod_cli.invoke(*args)
        assert r.exit_code == 1 and "FORBIDDEN (HTTP 403)" in r.output and "hint: demo control" in r.output, args
    assert [c for c in prod_cli.api.requests if c.port == 7004] == []  # nothing sent to the sim

    prod_cli.api.on("GET", "/system/status", {"mode": "prod", "trading_day": "20260928",
                                              "clock": {"mode": "REAL", "offset_ns": 0, "version": 0,
                                                        "market_now_ns": MARKET}})
    r = prod_cli.invoke("clock", "show")  # not a demo endpoint
    assert r.exit_code == 0 and "REAL v0, offset +00:00:00" in r.output


def test_the_fallback_respects_the_local_mode(prod_cli: Cli) -> None:
    """An older gateway (no /demo/faults route) and a prod home: the sim is not asked."""
    prod_cli.api.demo = True  # the old gateway lets the request through to its router
    r = prod_cli.invoke("fault", "list")
    assert r.exit_code == 1 and "FORBIDDEN" in r.output and "app.mode is 'prod'" in r.output
    assert [c for c in prod_cli.api.requests if c.port == 7004] == []
