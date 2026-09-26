"""Tools CLI plumbing: API key resolution, errors and exit codes, 429 retries, ``--api``, JSON output,
registration in ``futrisk.main`` and the fault table (02 §9.5, §9.6)."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
from clitools_support import KEY, Cli, is_uuid, route_exists, snapshot, tools_app

from futrisk.cli import apiclient, democtl_cmd, toolkit


def test_env_key_takes_precedence_over_the_file(cli: Cli, monkeypatch: pytest.MonkeyPatch) -> None:
    cli.api.on("GET", "/accounts", [])
    monkeypatch.setenv("FUTRISK_API_KEY", "frk_env-key")
    r = cli.invoke("accounts")
    assert r.exit_code == 1 and "UNAUTHORIZED (HTTP 401)" in r.output  # the fake only knows KEY
    assert cli.api.last("GET", "/accounts").headers["x-api-key"] == "frk_env-key"
    monkeypatch.setenv("FUTRISK_API_KEY", KEY)
    assert cli.invoke("accounts").exit_code == 0


def test_missing_key_fails_before_any_request(cli: Cli) -> None:
    (cli.home / "secrets" / "bootstrap_admin.key").unlink()
    r = cli.invoke("accounts")
    assert r.exit_code == 1
    assert "UNAUTHORIZED: no API key: set FUTRISK_API_KEY" in r.output and "bootstrap_admin.key" in r.output
    assert cli.api.requests == []


def test_unreachable_api(cli: Cli, monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    monkeypatch.setattr(apiclient, "TRANSPORT", httpx.MockTransport(refuse))
    r = cli.invoke("positions")
    assert r.exit_code == 1
    assert "UNAVAILABLE: api-gateway unreachable at http://127.0.0.1:18080" in r.output and "futrisk status" in r.output
    r = cli.invoke("positions", "--json")
    assert r.exit_code == 1 and json.loads(r.stdout)["code"] == "UNAVAILABLE"


def test_timeout_is_reported(cli: Cli, monkeypatch: pytest.MonkeyPatch) -> None:
    def slow(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/health"):
            health: httpx.Response = cli.api(request)
            return health
        raise httpx.ReadTimeout("timed out", request=request)

    monkeypatch.setattr(apiclient, "TRANSPORT", httpx.MockTransport(slow))
    r = cli.invoke("risk", "halt", "ACC1", "-r", "x")
    assert r.exit_code == 1 and "TIMEOUT: POST http://127.0.0.1:18080/api/v1/risk/accounts/ACC1/halt" in r.output


def test_busy_answers_are_retried_with_the_same_key(cli: Cli) -> None:
    path = "/risk/accounts/ACC1/halt"
    cli.api.on("POST", path, None, code="RISK_BUSY", message="shard busy", headers={"Retry-After": "2"})
    cli.api.on("POST", path, {"account_id": "ACC1", "from_state": "NORMAL", "to_state": "HALTED",
                              "halted_latched": True})
    r = cli.invoke("risk", "halt", "ACC1", "--reason", "drill")
    assert r.exit_code == 0, r.output
    first, second = cli.api.calls("POST", path)
    assert is_uuid(first.idem) and first.idem == second.idem and first.body == second.body
    assert cli.sleeps == [2.0]


def test_busy_gives_up_after_the_retries(cli: Cli) -> None:
    cli.api.on("POST", "/risk/accounts/ACC1/halt", None, code="BUSY", message="queue full",
               headers={"Retry-After": "600"})
    r = cli.invoke("risk", "halt", "ACC1", "--reason", "drill")
    assert r.exit_code == 1 and "BUSY (HTTP 429): queue full" in r.output
    assert len(cli.api.calls("POST", "/risk/accounts/ACC1/halt")) == 1 + apiclient.BUSY_RETRIES
    assert cli.sleeps == [apiclient.MAX_RETRY_AFTER_S] * apiclient.BUSY_RETRIES  # Retry-After is capped


def test_error_envelope_with_json(cli: Cli) -> None:
    cli.api.on("GET", "/settlement/20260101", None, code="NOT_FOUND", message="no settlement for 20260101")
    r = cli.invoke("settle", "show", "--td", "20260101", "--json")
    assert r.exit_code == 1
    assert json.loads(r.stdout) == {"ok": False, "code": "NOT_FOUND", "message": "no settlement for 20260101",
                                    "data": None}
    r = cli.invoke("settle", "show", "--td", "20260101")
    assert r.exit_code == 1 and "error: NOT_FOUND (HTTP 404): no settlement for 20260101" in r.stderr
    assert r.stdout == ""


def test_non_envelope_error_body(cli: Cli) -> None:
    cli.api.on("GET", "/accounts", raw=b"<html>Bad Gateway</html>", status=502)
    r = cli.invoke("accounts")
    assert r.exit_code == 1 and "INTERNAL (HTTP 502): <html>Bad Gateway</html>" in r.output


def test_api_option_overrides_the_config(cli: Cli, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[httpx.URL] = []

    def other(request: httpx.Request) -> httpx.Response:
        seen.append(request.url)
        return httpx.Response(200, json={"ok": True, "code": "OK", "message": "", "data": [snapshot("ACC1")]})

    monkeypatch.setattr(apiclient, "TRANSPORT", httpx.MockTransport(other))
    r = cli.invoke("accounts", "--api", "http://10.1.2.3:9000/api/v1/")
    assert r.exit_code == 0, r.output
    assert [str(u) for u in seen] == ["http://10.1.2.3:9000/api/v1/health", "http://10.1.2.3:9000/api/v1/accounts"]


def test_bad_config_exits_2(cli: Cli) -> None:
    active = json.loads((cli.home / "config" / "active.json").read_text(encoding="utf-8"))
    (cli.home / active["path"] / "services.yaml").write_text("instances: [oops\n", encoding="utf-8")
    r = cli.invoke("accounts")
    assert r.exit_code == 2 and "configuration error" in r.output


def test_numbers_keep_their_text() -> None:
    raw = b'{"a":1000000.0,"b":0.30000000000000004,"c":"123.40","d":7}'
    doc = toolkit.decode_json(raw)
    assert doc == {"a": Decimal("1000000.0"), "b": Decimal("0.30000000000000004"), "c": "123.40", "d": 7}
    assert [toolkit.fmt(doc[k]) for k in "abcd"] == ["1000000.0", "0.30000000000000004", "123.40", "7"]
    assert json.loads(toolkit.dump_json(doc))["b"] == 0.30000000000000004
    assert "0.30000000000000004" in toolkit.pretty_json(raw)
    assert toolkit.pct(Decimal("-0.0302")) == "-3.02%" and toolkit.pct(None) == "-" and toolkit.pct("0.35") == "35.00%"
    assert toolkit.span(-90 * 10**9) == "-00:01:30" and toolkit.span(86_400 * 10**9) == "+1d 00:00:00"
    assert toolkit.parse_kv(["a=1", "b=x", "c=true", "d=[1,2]", 'e="7"'], "--p") == {
        "a": 1, "b": "x", "c": True, "d": [1, 2], "e": "7"}
    assert apiclient.seg("20260928|r|l|acct:ACC3/rb|1") == "20260928%7Cr%7Cl%7Cacct%3AACC3%2Frb%7C1"


def test_registered_through_futrisk_main() -> None:
    from futrisk import main

    assert "futrisk.cli.tools_cmd" in main.TOPLEVEL
    names = {c.name for c in tools_app().registered_commands} | {g.name for g in tools_app().registered_groups}
    assert {"positions", "accounts", "order", "risk", "settle", "report", "fault", "shock", "scenario", "clock",
            "doctor"} <= names


def test_fault_table_matches_the_sim() -> None:
    from futrisk.sim.exchange.faults import FAULT_TYPES

    assert tuple(democtl_cmd.FAULTS) == FAULT_TYPES


def test_every_path_the_cli_calls_is_a_real_route() -> None:
    """The paths below are the command templates; FakeApi.on checks the faked ones as well."""
    from clitools_support import _api_routes, _sim_routes

    api = [("GET", "/health"), ("GET", "/positions"), ("GET", "/accounts"), ("GET", "/accounts/ACC1/orders"),
           ("POST", "/orders"), ("DELETE", "/orders/o%7C1"), ("POST", "/orders/cancel-all"), ("GET", "/risk/state"),
           ("GET", "/risk/events"), ("POST", "/risk/accounts/ACC1/reset"), ("POST", "/risk/accounts/ACC1/halt"),
           ("POST", "/risk/events/20260928%7Cd%7Ch%7Cacct%3AACC3%2Frb%7C1/ack"), ("POST", "/risk/firm/kill-switch"),
           ("POST", "/settlement/run"), ("GET", "/settlement/20260928/status"), ("GET", "/settlement/20260928"),
           ("GET", "/settlement/days"), ("GET", "/settlement/20260928/statements/ACC1"), ("GET", "/reports"),
           ("GET", "/reports/r-1"), ("POST", "/demo/faults"), ("DELETE", "/demo/faults/f-1"), ("POST", "/demo/shock"),
           ("POST", "/demo/scenario"), ("POST", "/demo/clock"), ("GET", "/system/status"),
           # the sim-exchange reads and the scenario cancel, proxied (authenticated and audited)
           ("GET", "/demo/faults"), ("GET", "/demo/state"), ("GET", "/demo/scenarios"), ("DELETE", "/demo/scenario")]
    for method, path in api:
        assert route_exists(_api_routes(), method, "/api/v1" + path), (method, path)
    # the fallback for an older api-gateway without those four routes
    for method, path in (("GET", "/control/state"), ("GET", "/control/scenarios"), ("DELETE", "/control/scenario")):
        assert route_exists(_sim_routes(), method, path), (method, path)
    # still no bulk delete: `fault clear --all` deletes one fault at a time
    assert not route_exists(_api_routes(), "DELETE", "/api/v1/demo/faults")


def test_sim_control_unreachable_in_the_fallback(cli: Cli, monkeypatch: pytest.MonkeyPatch) -> None:
    def gateway_only(request: httpx.Request) -> httpx.Response:
        if request.url.port == 7004:
            raise httpx.ConnectError("refused", request=request)
        resp: httpx.Response = cli.api(request)  # an older gateway: no GET /demo/faults route
        return resp

    monkeypatch.setattr(apiclient, "TRANSPORT", httpx.MockTransport(gateway_only))
    r = cli.invoke("fault", "list")
    assert r.exit_code == 1 and "sim-exchange control API unreachable at http://127.0.0.1:7004/control" in r.output


def test_futrisk_home_is_the_default_home(tmp_path: Path, cli: Cli, monkeypatch: pytest.MonkeyPatch) -> None:
    cli.api.on("GET", "/accounts", [])
    monkeypatch.setenv("FUTRISK_HOME", str(cli.home))
    r = cli.invoke("accounts", home=False)  # key and api URL from $FUTRISK_HOME
    assert r.exit_code == 0, r.output
    assert cli.api.last("GET", "/accounts").headers["x-api-key"] == KEY


# ---------------------------------------------------------------------------- the home guard
def test_a_gateway_of_another_home_never_gets_this_homes_key(cli: Cli) -> None:
    cli.api.home_id = "0123456789abcdef0123456789abcdef"
    cli.api.on("GET", "/accounts", [])
    r = cli.invoke("accounts")
    assert r.exit_code == 1
    assert "CONFLICT: the api-gateway at http://127.0.0.1:18080 serves another home" in r.output
    assert "API key is not sent there" in r.output and "hint:" in r.output
    assert [(x.method, x.path) for x in cli.api.requests] == [("GET", "/api/v1/health")]
    assert all("x-api-key" not in x.headers for x in cli.api.requests)
    r = cli.invoke("accounts", "--json")
    assert r.exit_code == 1 and json.loads(r.stdout)["code"] == "CONFLICT"


def test_an_explicit_key_or_an_older_gateway_skips_the_home_check(cli: Cli, monkeypatch: pytest.MonkeyPatch) -> None:
    cli.api.on("GET", "/accounts", [])
    cli.api.home_id = "0123456789abcdef0123456789abcdef"  # e.g. --api of a docker deployment
    monkeypatch.setenv("FUTRISK_API_KEY", KEY)
    assert cli.invoke("accounts").exit_code == 0
    assert cli.api.calls("GET", "/health") == []
    monkeypatch.delenv("FUTRISK_API_KEY")
    cli.api.on("GET", "/health", {"status": "ok", "trading_day": "20260928"})  # no home_id: cannot tell
    assert cli.invoke("accounts").exit_code == 0


def test_body_too_large_and_failed_auth_throttle(cli: Cli) -> None:
    cli.api.on("POST", "/demo/scenario", None, code="BAD_REQUEST", status=413,
               message="request body larger than 1048576 bytes")
    r = cli.invoke("scenario", "run", "breaker")
    assert r.exit_code == 1 and "BAD_REQUEST (HTTP 413): request body larger than 1048576 bytes" in r.output
    cli.api.on("GET", "/accounts", None, code="BUSY", message="too many failed authentications from this address",
               headers={"Retry-After": "1"})
    r = cli.invoke("accounts")
    assert r.exit_code == 1 and "BUSY (HTTP 429): too many failed authentications" in r.output
    assert "hint: check the API key" in r.output
    assert len(cli.api.calls("GET", "/accounts")) == 1 + apiclient.BUSY_RETRIES
