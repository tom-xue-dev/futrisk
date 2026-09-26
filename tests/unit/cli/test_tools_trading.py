"""``futrisk positions | accounts | order ... | risk ...`` against the fake api-gateway (02 §9.6)."""

from __future__ import annotations

import json

from clitools_support import KEY, Cli, Req, envelope, is_uuid, position, snapshot


# ---------------------------------------------------------------------------- positions
def test_positions_table_filters_and_missing_accounts(cli: Cli) -> None:
    items = [position("ACC3", "rb2701", notional=998765.43, position_pnl=0.1 + 0.2, pct_equity=0.3512),
             position("ACC3", "hc2701", "SHORT", limit_locked=True)]
    cli.api.on("GET", "/positions", {"group_by": None, "items": items, "groups": [], "accounts_missing": ["ACC5"]})
    r = cli.invoke("positions", "--account", "ACC3", "--sector", "black", "--exchange", "shfe")
    assert r.exit_code == 0, r.output
    req = cli.api.last("GET", "/positions")
    assert req.params == {"account": "ACC3", "sector": "black", "exchange": "SHFE"}
    assert req.headers["x-api-key"] == KEY and req.idem is None
    assert req.port == 18080  # the api-gateway http endpoint of the home's services.yaml
    out = r.output
    assert "rb2701" in out and "hc2701" in out and "SHORT" in out and "LOCKED" in out
    assert "998765.43" in out and "0.30000000000000004" in out  # money exactly as the API wrote it
    assert "3120.5" in out and "35.12%" in out
    assert "not included): ACC5" in out


def test_positions_group_by_and_raw_json(cli: Cli) -> None:
    raw = (b'{"ok":true,"code":"OK","message":"","data":{"group_by":"product","items":[],"groups":[{"group":"rb",'
           b'"positions":2,"long_qty":3,"short_qty":1,"net_qty":2,"gross_notional":1234567.1000000001,'
           b'"net_notional":-0.30000000000000004,"margin":123.4,"position_pnl":-100.5,"accounts":["ACC1","ACC3"],'
           b'"instruments":["rb2701"]}],"accounts_missing":[]}}')
    cli.api.on("GET", "/positions", raw=raw)
    r = cli.invoke("positions", "--group-by", "PRODUCT")
    assert r.exit_code == 0, r.output
    assert cli.api.last("GET", "/positions").params == {"group_by": "product"}
    assert "1234567.1000000001" in r.output and "-0.30000000000000004" in r.output and "ACC1,ACC3" in r.output

    r, doc = cli.json("positions")
    assert r.exit_code == 0
    assert doc["data"]["groups"][0]["group"] == "rb"
    assert "1234567.1000000001" in r.stdout and "-0.30000000000000004" in r.stdout  # untouched numbers

    r = cli.invoke("positions", "--group-by", "desk")
    assert r.exit_code == 2  # usage error, nothing sent
    assert len(cli.api.calls("GET", "/positions")) == 2


def test_accounts_overview(cli: Cli) -> None:
    missing = [{"account_id": a, "name": f"{a} name", "shard": 1, "missing": True, "stale": False, "age_ms": None}
               for a in ("ACC2", "ACC4")]
    missing[1].update(state="HALTED", halted_latched=True, state_version=2)  # a risk.state without a snapshot
    stale = snapshot("ACC3", "HALTED", equity=9698000.0, risk_degree=0.41234) | {"stale": True, "age_ms": 12345}
    cli.api.on("GET", "/accounts", [snapshot("ACC1", equity=10250000.5) | {"missing": False, "stale": False,
                                                                            "age_ms": 800}, stale, *missing])
    r = cli.invoke("accounts")
    assert r.exit_code == 0, r.output
    out = r.output
    assert "ACC3" in out and "HALTED" in out and "NORMAL" in out
    assert "10250000.5" in out and "9698000.0" in out  # equity as given
    assert "-3.02%" in out  # daily P&L % of initial capital
    assert "41.23%" in out and "35.00%" in out  # risk degree
    assert "stale 12.3s" in out  # the owning shard stopped updating ACC3
    assert "(no snapshot)" in out and "HALTED (no snapshot)" in out
    assert "without a snapshot yet: ACC2, ACC4, ACC5" in out  # ACC5: not listed at all (an older gateway)


# ---------------------------------------------------------------------------- orders
def _ack(req: Req) -> dict[str, object]:
    doc: dict[str, object] = envelope({"client_order_id": req.body["client_order_id"], "accepted": True,
                                       "status": "ROUTING", "risk_check_us": 42, "trading_day": "20260928"})
    return doc


def test_order_place_sends_one_fresh_uuid_per_invocation(cli: Cli) -> None:
    cli.api.on("POST", "/orders", handler=_ack)
    args = ("order", "place", "--account", "ACC1", "--instrument", "rb2701", "--side", "BUY",
            "--offset", "close-today", "--qty", "2", "--price", "3120.5")
    r1 = cli.invoke(*args)
    r2 = cli.invoke(*args)
    assert r1.exit_code == 0 and r2.exit_code == 0, r1.output + r2.output
    first, second = cli.api.calls("POST", "/orders")
    assert first.body == {"client_order_id": first.body["client_order_id"], "account_id": "ACC1",
                          "instrument_id": "rb2701", "direction": "BUY", "offset": "CLOSE_TODAY",
                          "order_type": "LIMIT", "qty": 2, "price": 3120.5}
    assert is_uuid(first.idem) and first.idem == first.body["client_order_id"]
    assert is_uuid(second.idem) and second.idem != first.idem
    assert "accepted" in r1.output and "ROUTING" in r1.output and "42 us" in r1.output


def test_order_place_market_with_tif(cli: Cli) -> None:
    cli.api.on("POST", "/orders", handler=_ack)
    r = cli.invoke("order", "place", "-a", "ACC2", "-i", "IF2612", "--side", "sell", "--offset", "open", "-q", "1",
                   "--type", "market", "--tif", "fak")
    assert r.exit_code == 0, r.output
    body = cli.api.last("POST", "/orders").body
    assert body["order_type"] == "MARKET" and body["tif"] == "FAK" and "price" not in body
    assert body["direction"] == "SELL" and body["offset"] == "OPEN"


def test_order_place_reject_prints_code_and_reason(cli: Cli) -> None:
    ack = {"client_order_id": "x", "accepted": False, "status": "REJECTED", "reject_code": "REJECT_ACCOUNT_HALTED",
           "reject_detail": {"state": "HALTED", "rule": "daily_loss"}, "trading_day": "20260928"}
    cli.api.on("POST", "/orders", ack, code="REJECT_ACCOUNT_HALTED", message="ACC3 is halted by daily_loss")
    args = ("order", "place", "--account", "ACC3", "--instrument", "rb2701", "--side", "buy", "--offset", "open",
            "--qty", "1", "--price", "3100")
    r = cli.invoke(*args)
    assert r.exit_code == 1
    assert "REJECTED" in r.output and "REJECT_ACCOUNT_HALTED" in r.output
    assert "ACC3 is halted by daily_loss" in r.output and "rule=daily_loss" in r.output

    r, doc = cli.json(*args)
    assert r.exit_code == 1
    assert doc["ok"] is False and doc["code"] == "REJECT_ACCOUNT_HALTED" and doc["data"]["reject_detail"]

    cli.api.on("POST", "/orders", {"accepted": False, "status": "REJECTED"}, code="ROUTE_KILL_SWITCH",
               message="firm kill switch is on")  # another business prefix, HTTP 200
    r = cli.invoke(*args)
    assert r.exit_code == 1 and "REJECTED: ROUTE_KILL_SWITCH" in r.output and "firm kill switch is on" in r.output


def test_order_place_usage_errors_send_nothing(cli: Cli) -> None:
    base = ("order", "place", "--account", "ACC1", "--instrument", "rb2701", "--side", "buy", "--offset", "open",
            "--qty", "1")
    assert cli.invoke(*base).exit_code == 2  # limit without --price
    assert cli.invoke(*base, "--type", "market", "--price", "1").exit_code == 2
    assert cli.invoke(*base, "--price", "abc").exit_code == 2
    assert cli.invoke(*base, "--price", "-5").exit_code == 2
    assert cli.invoke(*base, "--price", "1", "--offset", "close-all").exit_code == 2
    assert cli.api.requests == []


def test_order_cancel_cancel_all_and_list(cli: Cli) -> None:
    cli.api.on("DELETE", "/orders/abc%7C1", {"client_order_id": "abc|1", "status": "CANCEL_PENDING"})
    r = cli.invoke("order", "cancel", "abc|1", "--account", "ACC1", "--reason", "fat finger")
    assert r.exit_code == 0, r.output
    req = cli.api.last("DELETE", "/orders/abc%7C1")
    assert req.params == {"account": "ACC1", "reason": "fat finger"} and is_uuid(req.idem)
    assert "CANCEL_PENDING" in r.output

    cli.api.on("POST", "/orders/cancel-all", {"count": 3})
    r = cli.invoke("order", "cancel-all", "--account", "ACC1", "--open-only", "--product", "rb")
    assert r.exit_code == 0, r.output
    req = cli.api.last("POST", "/orders/cancel-all")
    assert req.body == {"account_id": "ACC1", "scope": "OPEN_ONLY", "reason": "cli cancel-all", "product": "rb"}
    assert is_uuid(req.idem) and "3 order(s)" in r.output

    orders = [{"client_order_id": "o-2", "instrument_id": "rb2701", "direction": "SELL", "offset": "CLOSE",
               "order_type": "LIMIT", "price": 3121.0, "qty": 2, "filled_qty": 1, "avg_fill_price": 3121.0,
               "status": "PARTIALLY_FILLED", "source": "MANUAL", "created_ns": 1_790_000_100_000_000_000},
              {"client_order_id": "o-1", "instrument_id": "rb2701", "direction": "BUY", "offset": "OPEN",
               "order_type": "LIMIT", "price": 3119.0, "qty": 1, "filled_qty": 0, "status": "ACCEPTED",
               "source": "MANUAL", "created_ns": 1_790_000_000_000_000_000}]
    cli.api.on("GET", "/accounts/ACC1/orders", {"items": orders})
    r = cli.invoke("order", "list", "--account", "ACC1", "--active", "--td", "2026-09-28")
    assert r.exit_code == 0, r.output
    assert cli.api.last("GET", "/accounts/ACC1/orders").params == {
        "td": "20260928", "status": "ROUTING,ROUTED,ACCEPTED,PARTIALLY_FILLED,CANCEL_PENDING,UNKNOWN"}
    out = r.output
    assert out.index("o-1") < out.index("o-2")  # oldest first
    assert "PARTIALLY_FILLED" in out and "3121.0" in out
    assert cli.invoke("order", "list", "--account", "ACC1", "--td", "28/09").exit_code == 2


def test_order_cancel_conflict_is_an_error(cli: Cli) -> None:
    cli.api.on("DELETE", "/orders/o-9", {"status": "FILLED"}, code="CONFLICT", message="order is FILLED")
    r = cli.invoke("order", "cancel", "o-9")
    assert r.exit_code == 1
    assert "CONFLICT (HTTP 409): order is FILLED" in r.output


# ---------------------------------------------------------------------------- risk
def test_risk_state_and_events(cli: Cli) -> None:
    rows = [{"account_id": "ACC1", "shard": 0, "trading_day": "20260928", "state": "NORMAL", "halted_latched": False,
             "state_version": 1, "restrictions": [], "trading_mode": "OPEN", "last_change": None},
            {"account_id": "ACC3", "shard": 1, "trading_day": "20260928", "state": "HALTED", "halted_latched": True,
             "state_version": 4, "restrictions": [{"kind": "BLOCK_OPEN", "scope_key": "acct:ACC3", "source": "x"},
                                                   {"kind": "ORDER_SIZE_FACTOR", "scope_key": "acct:ACC3",
                                                    "value": 0.5, "source": "y"}],
             "trading_mode": "OPEN",
             "last_change": {"from_state": "RESTRICTED", "to_state": "HALTED", "cause": "RULE",
                             "ts_ns": 1_790_000_000_000_000_000, "actor": None}}]
    cli.api.on("GET", "/risk/state", rows[1:])  # api-gateway filters by ?account=
    r = cli.invoke("risk", "state", "--account", "ACC3")
    assert r.exit_code == 0, r.output
    assert cli.api.last("GET", "/risk/state").params == {"account": "ACC3"}
    assert "ACC3" in r.output and "ACC1" not in r.output
    assert "BLOCK_OPEN acct:ACC3; ORDER_SIZE_FACTOR acct:ACC3=0.5" in r.output
    assert "RESTRICTED->HALTED RULE" in r.output
    r, doc = cli.json("risk", "state", "--account", "ACC3")
    assert r.exit_code == 0 and [x["account_id"] for x in doc["data"]] == ["ACC3"]
    cli.api.on("GET", "/risk/state", rows)  # an older gateway ignores ?account=
    r, doc = cli.json("risk", "state", "--account", "ACC3")
    assert r.exit_code == 0 and [x["account_id"] for x in doc["data"]] == ["ACC3"]
    assert "ACC1" not in cli.invoke("risk", "state", "--account", "ACC3").output
    cli.api.on("GET", "/risk/state", None, code="NOT_FOUND", message="unknown account 'ACC9'")
    r = cli.invoke("risk", "state", "--account", "ACC9")
    assert r.exit_code == 1 and "unknown account 'ACC9'" in r.output

    key = "20260928|daily_loss|halt|acct:ACC3|1"
    events = [{"event_id": f"e{i}", "kind": "ALERT_RAISED", "trading_day": "20260928",
               "ts_ns": 1_790_000_000_000_000_000 + i, "account_id": "ACC3", "rule_id": "daily_loss", "level": "halt",
               "action": "CIRCUIT_BREAK", "severity": "CRITICAL", "value": -0.0302, "threshold": -0.03,
               "message": "ACC3 当日亏损 -3.02%，已熔断禁开", "alert_key": key} for i in range(3)]
    cli.api.on("GET", "/risk/events", {"trading_day": "20260928", "items": events, "errors": {"risk-engine-0": "TIMEOUT"}},
               message="partial: risk-engine-0=TIMEOUT")
    r = cli.invoke("risk", "events", "--td", "20260928", "--account", "ACC3", "--limit", "2")
    assert r.exit_code == 0, r.output
    assert cli.api.last("GET", "/risk/events").params == {"td": "20260928", "account": "ACC3"}
    assert key in r.output and "daily_loss/halt" in r.output and "-0.0302" in r.output
    assert "已熔断禁开" in r.output and "newest 2 shown" in r.output
    assert "partial result: risk-engine-0=TIMEOUT" in r.output


def test_risk_actions_send_bodies_and_fresh_keys(cli: Cli) -> None:
    cli.api.on("POST", "/risk/accounts/ACC3/reset", {"account_state": "NORMAL", "rearm_threshold": -0.025},
               message="1 latched level(s) reset")
    cli.api.on("POST", "/risk/accounts/ACC3/halt", {"account_id": "ACC3", "from_state": "NORMAL",
                                                     "to_state": "HALTED", "halted_latched": True})
    key = "20260928|daily_loss|halt|acct:ACC3|1"
    enc = "/risk/events/20260928%7Cdaily_loss%7Chalt%7Cacct%3AACC3%7C1/ack"
    cli.api.on("POST", enc, None)
    targets = [{"target": t, "ok": True, "code": "OK", "message": "", "data": None}
               for t in ("risk-engine-0", "risk-engine-1", "trade-gateway")]
    cli.api.on("POST", "/risk/firm/kill-switch", {"targets": targets, "all_ok": True, "control": None, "retrying": []},
               message="3/3 targets confirmed")

    r = cli.invoke("risk", "reset", "ACC3", "--reason", "reviewed", "--rule", "daily_loss", "--level", "halt")
    assert r.exit_code == 0, r.output
    assert "state NORMAL, re-arm threshold -0.025 (1 latched level(s) reset)" in r.output
    r = cli.invoke("risk", "halt", "ACC3", "-r", "manual stop")
    assert r.exit_code == 0 and "NORMAL -> HALTED (latched)" in r.output
    r = cli.invoke("risk", "ack", key, "--note", "seen")
    assert r.exit_code == 0 and f"acknowledged {key}" in r.output
    r = cli.invoke("risk", "kill-switch", "ON", "--reason", "drill")
    assert r.exit_code == 0, r.output
    assert "trade-gateway" in r.output and "3/3 targets confirmed" in r.output

    reset = cli.api.last("POST", "/risk/accounts/ACC3/reset")
    halt = cli.api.last("POST", "/risk/accounts/ACC3/halt")
    ack = cli.api.last("POST", enc)
    kill = cli.api.last("POST", "/risk/firm/kill-switch")
    assert reset.body == {"reason": "reviewed", "rule_id": "daily_loss", "level": "halt"}
    assert halt.body == {"reason": "manual stop"}
    assert ack.body == {"note": "seen"}
    assert kill.body == {"on": True, "reason": "drill"}
    keys = [x.idem for x in (reset, halt, ack, kill)]
    assert all(is_uuid(k) for k in keys) and len(set(keys)) == 4

    assert cli.invoke("risk", "reset", "ACC3").exit_code == 2  # --reason is required


def test_kill_switch_partial_failure_exits_1(cli: Cli) -> None:
    targets = [{"target": "risk-engine-0", "ok": True, "code": "OK", "message": "", "data": None},
               {"target": "risk-engine-1", "ok": False, "code": "FORBIDDEN", "message": "role", "data": None},
               {"target": "trade-gateway", "ok": False, "code": "TIMEOUT", "message": "no reply", "data": None}]
    cli.api.on("POST", "/risk/firm/kill-switch",  # a fan-out report is always HTTP 200
               {"targets": targets, "all_ok": False, "control": None, "retrying": ["trade-gateway"],
                "failed": ["risk-engine-1"]}, status=200, code="FORBIDDEN",
               message="1/3 targets confirmed; failed: risk-engine-1=FORBIDDEN, trade-gateway=TIMEOUT")
    r = cli.invoke("risk", "kill-switch", "off", "-r", "resume")
    assert r.exit_code == 1
    assert "retrying" in r.output and "keeps retrying: trade-gateway" in r.output
    assert "FAILED" in r.output and "not retried (non-retryable answer or retries used up): risk-engine-1" in r.output
    assert "error: FORBIDDEN: 1/3 targets confirmed" in r.output  # no HTTP status: the report is a 200
    assert cli.api.last("POST", "/risk/firm/kill-switch").body == {"on": False, "reason": "resume"}

    r = cli.invoke("risk", "kill-switch", "off", "-r", "resume", "--json")
    doc = json.loads(r.stdout)
    assert r.exit_code == 1 and doc["data"]["retrying"] == ["trade-gateway"] and doc["data"]["failed"] == ["risk-engine-1"]
