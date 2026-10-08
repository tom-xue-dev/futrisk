"""REST API of api-gateway against a fake bus (02 §9.6): auth, RBAC matrix, Idempotency-Key, error mapping,
shard routing, fan-out reporting, read-model views, audit, reports, demo guard."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from apigw_support import (
    MONDAY_10AM,
    ROLE_KEYS,
    Harness,
    err,
    hdr,
    make_gateway,
    new_key,
    position,
    rule_status,
    snapshot,
    with_keys,
)

from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.messages.enums import (
    Action,
    ClockTarget,
    ControlOrigin,
    EodPhase,
    Exchange,
    FirmAction,
    OrderStatus,
    PosDirection,
    ProcessState,
    ReconBreakKind,
    ReconBreakStatus,
    ReportType,
    RiskEventKind,
    Role,
    ServiceState,
    Severity,
)
from futrisk.messages.envelope import Reply, ok_reply
from futrisk.messages.eod import EodPhaseEvent, ReportReady, SettlementResult
from futrisk.messages.orders import CancelRequest, OrderAck, OrderRequest
from futrisk.messages.risk import (
    ExposureView,
    FirmControl,
    KillSwitchRequest,
    ReconBreakBatch,
    ReconBreakView,
    ResetRequest,
    RiskEvent,
    RiskEventBatch,
)
from futrisk.messages.system import Heartbeat, PsRequest, ServiceInfo, ServiceList

API = "/api/v1"


@pytest.fixture
async def h(tmp_path: Path, default_config: FullConfig) -> AsyncIterator[Harness]:
    cfg, env = with_keys(default_config)
    harness = await make_gateway(tmp_path, cfg, env=env)
    try:
        yield harness
    finally:
        await harness.close()


ORDER = {
    "client_order_id": "0192f000-0000-7000-8000-000000000001",
    "account_id": "ACC3",
    "instrument_id": "rb2701",
    "direction": "BUY",
    "offset": "OPEN",
    "order_type": "LIMIT",
    "price": 3100.0,
    "qty": 1,
}

# (method, path, body, permission); representative endpoints of each matrix row (02 §9.6)
MATRIX: list[tuple[str, str, Any, str]] = [
    ("GET", "/accounts", None, "READ"),
    ("GET", "/risk/compare", None, "READ"),
    ("POST", "/orders", ORDER, "TRADE"),
    ("DELETE", "/orders/X1?account=ACC3", None, "TRADE"),
    ("POST", "/orders/cancel-all", {"account_id": "ACC3"}, "TRADE"),
    ("POST", "/risk/accounts/ACC3/reset", {"reason": "checked"}, "RISK"),
    ("POST", "/risk/accounts/ACC3/halt", {"reason": "manual"}, "RISK"),
    ("POST", "/risk/firm/kill-switch", {"on": True, "reason": "drill"}, "RISK"),
    ("POST", "/risk/eod/20260928/force-unfreeze", {"reason": "audit ok"}, "RISK"),
    ("POST", "/settlement/20260928/prices/rb2701", {"price": "3100", "reason": "fix"}, "RISK"),
    ("POST", "/accounts/ACC3/cash", {"type": "DEPOSIT", "amount": "1000.00", "reason": "top up"}, "ADMIN"),
    ("POST", "/settlement/run", {"td": "20260928"}, "ADMIN"),
    ("POST", "/market/sources/feed_a", {"action": "EXCLUDE", "reason": "maint"}, "ADMIN"),
    ("POST", "/services/md-gateway/restart", None, "ADMIN"),
    ("POST", "/backtests", {"config_name": "baseline"}, "ADMIN"),
    ("POST", "/demo/faults", {"type": "FEED_GAP", "target": "feed_a", "params": {"drop_count": 3}}, "ADMIN"),
]
ALLOWED = {
    "READ": {Role.VIEWER, Role.TRADER, Role.RISK_MANAGER, Role.ADMIN},
    "TRADE": {Role.TRADER, Role.ADMIN},
    "RISK": {Role.RISK_MANAGER, Role.ADMIN},
    "ADMIN": {Role.ADMIN},
}


async def call(h: Harness, method: str, path: str, body: Any = None, headers: dict[str, str] | None = None) -> Any:
    async with h.client() as c:
        return await c.request(method, API + path, json=body, headers=headers or {})


class TestAuth:
    async def test_health_is_public(self, h: Harness) -> None:
        r = await call(h, "GET", "/health")
        assert r.status_code == 200 and r.json()["ok"] and r.json()["data"]["status"] == "ok"

    async def test_missing_and_bad_key(self, h: Harness) -> None:
        r = await call(h, "GET", "/accounts")
        assert r.status_code == 401 and r.json()["code"] == codes.UNAUTHORIZED
        r = await call(h, "GET", "/accounts", headers={"X-API-Key": "nope"})
        assert r.status_code == 401

    async def test_auth_me(self, h: Harness) -> None:
        r = await call(h, "GET", "/auth/me", headers=hdr(Role.RISK_MANAGER))
        assert r.json()["data"] == {"key_id": "risk_manager", "name": "risk_manager", "role": "RISK_MANAGER"}

    @pytest.mark.parametrize("role", list(ROLE_KEYS))
    async def test_permission_matrix(self, h: Harness, role: Role) -> None:
        for method, path, body, perm in MATRIX:
            headers = hdr(role, idem=new_key())
            r = await call(h, method, path, body, headers)
            if role in ALLOWED[perm]:
                assert r.status_code not in (401, 403), (role, method, path, r.json())
            else:
                assert r.status_code == 403 and r.json()["code"] == codes.FORBIDDEN, (role, method, path)


class TestIdempotency:
    async def test_replay_conflict_and_validation(self, h: Harness) -> None:
        key = new_key()
        body = {"reason": "false alarm"}
        r1 = await call(h, "POST", "/risk/accounts/ACC3/reset", body, hdr(Role.RISK_MANAGER, idem=key))
        assert r1.status_code == 200 and "duplicate" not in r1.json()
        assert len(h.bus.of("risk.reset")) == 1
        sent = h.bus.of("risk.reset")[0].payload
        assert isinstance(sent, ResetRequest) and sent.command_id == key
        r2 = await call(h, "POST", "/risk/accounts/ACC3/reset", body, hdr(Role.RISK_MANAGER, idem=key))
        assert r2.status_code == 200 and r2.json()["duplicate"] is True
        assert r2.json()["data"] == r1.json()["data"] and len(h.bus.of("risk.reset")) == 1
        r3 = await call(h, "POST", "/risk/accounts/ACC3/reset", {"reason": "other"}, hdr(Role.RISK_MANAGER, idem=key))
        assert r3.status_code == 409 and r3.json()["code"] == codes.CONFLICT
        r4 = await call(h, "POST", "/risk/accounts/ACC3/reset", body, hdr(Role.RISK_MANAGER))
        assert r4.status_code == 400 and "Idempotency-Key" in r4.json()["message"]
        r5 = await call(h, "POST", "/risk/accounts/ACC3/reset", body, hdr(Role.RISK_MANAGER, idem="not-a-uuid"))
        assert r5.status_code == 400
        # the record survives a restart of the gateway (processed_commands in api.db)
        entry = await h.gw.store.lookup_processed(key)
        assert entry is not None and entry.requester == "risk_manager"

    async def test_retryable_answers_are_not_recorded(self, h: Harness) -> None:
        h.bus.on("risk.halt", err(codes.TIMEOUT))
        key = new_key()
        r1 = await call(h, "POST", "/risk/accounts/ACC3/halt", {"reason": "x"}, hdr(Role.RISK_MANAGER, idem=key))
        assert r1.status_code == 504
        h.bus.on("risk.halt", ok_reply())
        r2 = await call(h, "POST", "/risk/accounts/ACC3/halt", {"reason": "x"}, hdr(Role.RISK_MANAGER, idem=key))
        assert r2.status_code == 200 and "duplicate" not in r2.json()
        assert len(h.bus.of("risk.halt")) == 2
        assert {c.payload.command_id for c in h.bus.of("risk.halt")} == {key}  # same command_id downstream

    async def test_concurrent_duplicates_run_once(self, h: Harness) -> None:
        async def slow(target: str, payload: Any) -> Reply:
            await asyncio.sleep(0.1)
            return ok_reply()

        h.bus.on("risk.reset", slow)
        key = new_key()
        rs = await asyncio.gather(
            *(call(h, "POST", "/risk/accounts/ACC3/reset", {"reason": "r"}, hdr(Role.RISK_MANAGER, idem=key))
              for _ in range(3))
        )
        assert [r.status_code for r in rs] == [200, 200, 200]
        assert len(h.bus.of("risk.reset")) == 1
        assert sum(1 for r in rs if r.json().get("duplicate")) == 2

    async def test_orders_use_client_order_id(self, h: Harness) -> None:
        h.bus.on("order.submit", lambda t, p: ok_reply(OrderAck(client_order_id=p.client_order_id, accepted=True,
                                                                 status=OrderStatus.ROUTING, trading_day="20260928")))
        r = await call(h, "POST", "/orders", ORDER, hdr(Role.TRADER))  # no Idempotency-Key needed
        assert r.status_code == 200 and r.json()["data"]["status"] == "ROUTING"
        sent = h.bus.of("order.submit")[0]
        assert sent.target == "risk-engine-1" and isinstance(sent.payload, OrderRequest)
        assert sent.payload.actor == "trader" and sent.payload.source == "MANUAL"


class TestErrorMapping:
    @pytest.mark.parametrize(
        ("code", "status"),
        [
            (codes.BAD_REQUEST, 400),
            (codes.FORBIDDEN, 403),
            (codes.NOT_FOUND, 404),
            (codes.CONFLICT, 409),
            (codes.BUSY, 429),
            (codes.RISK_BUSY, 429),
            (codes.UNAVAILABLE, 503),
            (codes.TIMEOUT, 504),
            (codes.NOT_OWNER, 409),
            (codes.PRECONDITION_FAILED, 409),
            (codes.VERSION_UNSUPPORTED, 502),
            (codes.INTERNAL, 500),
            (codes.REJECT_ACCOUNT_HALTED, 200),
            (codes.ROUTE_EXPIRED, 200),
        ],
    )
    async def test_code_to_status(self, h: Harness, code: str, status: int) -> None:
        h.bus.on("order.submit", Reply(ok=False, code=code, message="m", retry_after_ms=250))
        r = await call(h, "POST", "/orders", ORDER, hdr(Role.TRADER))
        body = r.json()
        assert r.status_code == status and body["ok"] is False and body["code"] == code
        assert set(body) >= {"ok", "code", "message", "data"}
        if status in (429, 503):
            assert r.headers["Retry-After"] == "1"

    def test_every_code_has_an_explicit_status(self) -> None:
        from futrisk.services.api_gateway.rest import _STATUS, BUSINESS_PREFIXES, http_status

        names = [n for n in dir(codes) if n.isupper() and isinstance(getattr(codes, n), str)]
        assert len(names) > 60
        for n in names:
            code = getattr(codes, n)
            assert code in _STATUS or code.startswith(BUSINESS_PREFIXES), code
            assert (http_status(code) == 500) == (code == codes.INTERNAL), code  # INTERNAL is the only 500

    async def test_precondition_failed_is_409(self, h: Harness) -> None:
        h.bus.on("settle.run", Reply(ok=False, code=codes.PRECONDITION_FAILED, message="D-1 not settled"))
        r = await call(h, "POST", "/settlement/run", {"td": "20260928"}, hdr(Role.ADMIN, idem=new_key()))
        assert r.status_code == 409 and r.json()["code"] == codes.PRECONDITION_FAILED

    async def test_reads_retry_a_starting_shard(self, h: Harness) -> None:
        from futrisk.messages.risk import PositionBatch

        answers = [err(codes.UNAVAILABLE, "peer not connected"), err(codes.UNAVAILABLE, "peer not connected")]
        h.bus.on("risk.query.positions", lambda t, p: answers.pop(0) if answers else ok_reply(PositionBatch(items=[])))
        r = await call(h, "GET", "/accounts/ACC3/positions", headers=hdr(Role.VIEWER))
        assert r.status_code == 200 and len(h.bus.of("risk.query.positions")) == 3
        # a shard known to be down is not waited for: one attempt, 503 with Retry-After
        h.peers["risk-engine-1"] = "DOWN"
        h.bus.on("risk.query.positions", err(codes.UNAVAILABLE, "peer is down"))
        r = await call(h, "GET", "/accounts/ACC3/positions", headers=hdr(Role.VIEWER))
        assert r.status_code == 503 and r.headers["Retry-After"] == "1" and len(h.bus.of("risk.query.positions")) == 4

    async def test_validation_error_is_400(self, h: Harness) -> None:
        r = await call(h, "POST", "/orders", {**ORDER, "qty": 0}, hdr(Role.TRADER))
        assert r.status_code == 400 and r.json()["code"] == codes.BAD_REQUEST
        r = await call(h, "POST", "/accounts/ACC3/cash", {"type": "DEPOSIT", "amount": "1e5", "reason": "x"},
                       hdr(Role.ADMIN, idem=new_key()))
        assert r.status_code == 400

    async def test_unknown_route_is_404_envelope(self, h: Harness) -> None:
        r = await call(h, "GET", "/nope", headers=hdr(Role.VIEWER))
        assert r.status_code == 404 and r.json()["code"] == codes.NOT_FOUND


class TestRouting:
    async def test_per_account_shard(self, h: Harness) -> None:
        for acct, shard in (("ACC1", "risk-engine-0"), ("ACC2", "risk-engine-0"), ("ACC3", "risk-engine-1"),
                            ("ACC5", "risk-engine-1")):
            r = await call(h, "POST", f"/risk/accounts/{acct}/reset", {"reason": "ok", "level": "halt"},
                           hdr(Role.RISK_MANAGER, idem=new_key()))
            assert r.status_code == 200
            c = h.bus.of("risk.reset")[-1]
            assert c.target == shard and c.payload.account_id == acct
            assert c.payload.actor == "risk_manager" and c.payload.role == Role.RISK_MANAGER  # re-check downstream
        r = await call(h, "POST", "/risk/accounts/ACC9/reset", {"reason": "x"}, hdr(Role.RISK_MANAGER, idem=new_key()))
        assert r.status_code == 404

    async def test_unknown_account_order_is_business_reject(self, h: Harness) -> None:
        r = await call(h, "POST", "/orders", {**ORDER, "account_id": "ACC9"}, hdr(Role.TRADER))
        assert r.status_code == 200 and r.json()["code"] == codes.REJECT_UNKNOWN_ACCOUNT
        assert r.json()["data"]["status"] == "REJECTED" and not h.bus.of("order.submit")

    async def test_cancel_finds_owner(self, h: Harness) -> None:
        from futrisk.messages.orders import OrderUpdate

        h.gw.readmodel.merge_orders("ACC1", [OrderUpdate(
            client_order_id="C1", account_id="ACC1", instrument_id="rb2701", exchange=Exchange.SHFE,
            direction="BUY", offset="OPEN", order_type="LIMIT", tif="GFD", qty=1, status=OrderStatus.ACCEPTED,
            source="BOT", trading_day="20260928", created_ns=1, updated_ns=1)])  # type: ignore[arg-type]
        r = await call(h, "DELETE", "/orders/C1", headers=hdr(Role.TRADER, idem=new_key()))
        assert r.status_code == 200
        c = h.bus.of("order.cancel")[-1]
        assert c.target == "risk-engine-0" and isinstance(c.payload, CancelRequest) and c.payload.account_id == "ACC1"

    async def test_kill_switch_partial_failure(self, h: Harness) -> None:
        def reply(target: str, payload: KillSwitchRequest) -> Reply:
            if target == "risk-engine-1":
                return err(codes.TIMEOUT, "no answer")
            return ok_reply(FirmControl(action=FirmAction.KILL_SWITCH_ON, origin=ControlOrigin.MANUAL, active=True,
                                        trading_day="20260928", actor=payload.actor, reason=payload.reason, ts_ns=1))

        h.bus.on("firm.kill_switch", reply)
        h.gw.killswitch.interval_s = 30.0  # keep the background retry out of the way
        key = new_key()
        r = await call(h, "POST", "/risk/firm/kill-switch", {"on": True, "reason": "drill"},
                       hdr(Role.RISK_MANAGER, idem=key))
        body = r.json()
        # a target's code never becomes the HTTP status: 200, ok=false, code = the worst target code (M1)
        assert r.status_code == 200 and body["ok"] is False and body["code"] == codes.TIMEOUT
        assert r.headers["X-Result-Code"] == codes.TIMEOUT
        targets = {t["target"]: t for t in body["data"]["targets"]}
        assert set(targets) == {"risk-engine-0", "risk-engine-1", "trade-gateway"}
        assert targets["risk-engine-0"]["ok"] and targets["trade-gateway"]["ok"]
        assert not targets["risk-engine-1"]["ok"] and targets["risk-engine-1"]["code"] == codes.TIMEOUT
        assert body["data"]["retrying"] == ["risk-engine-1"] and body["data"]["control"]["active"] is True
        assert body["data"]["failed"] == []
        assert {c.payload.command_id for c in h.bus.of("firm.kill_switch")} == {key}
        assert h.gw.killswitch.pending() == ["risk-engine-1"] and h.gw.killswitch.retrying
        # the answer is recorded even though a target timed out: a retry replays it (HTTP 200 kept)
        r2 = await call(h, "POST", "/risk/firm/kill-switch", {"on": True, "reason": "drill"},
                        hdr(Role.RISK_MANAGER, idem=key))
        assert r2.status_code == 200 and r2.json()["duplicate"] is True and r2.json()["code"] == codes.TIMEOUT
        assert len(h.bus.of("firm.kill_switch")) == 3

    async def test_kill_switch_all_ok_and_force_unfreeze(self, h: Harness) -> None:
        r = await call(h, "POST", "/risk/firm/kill-switch", {"on": False, "reason": "done"},
                       hdr(Role.ADMIN, idem=new_key()))
        assert r.status_code == 200 and r.json()["data"]["all_ok"] is True
        r = await call(h, "POST", "/risk/eod/20260928/force-unfreeze", {"reason": "audited"},
                       hdr(Role.RISK_MANAGER, idem=new_key()))
        assert r.status_code == 200
        assert sorted(c.target for c in h.bus.of("eod.force_unfreeze")) == ["risk-engine-0", "risk-engine-1"]
        # a shard refusing is reported per target, still HTTP 200 (an authorized caller never gets a 403)
        h.bus.on("eod.force_unfreeze", err(codes.FORBIDDEN, "role re-check failed"), target="risk-engine-1")
        r = await call(h, "POST", "/risk/eod/20260928/force-unfreeze", {"reason": "audited"},
                       hdr(Role.RISK_MANAGER, idem=new_key()))
        body = r.json()
        assert r.status_code == 200 and not body["ok"] and body["code"] == codes.FORBIDDEN
        assert [t["ok"] for t in body["data"]["targets"]] == [True, False]

    async def test_recon_breaks_fan_out_and_resolve(self, h: Harness) -> None:
        def breaks(target: str, payload: Any) -> Reply:
            if target == "risk-engine-1":
                return err(codes.UNAVAILABLE)
            return ok_reply(ReconBreakBatch(items=[ReconBreakView(
                break_id="B1", trading_day="20260928", account_id="ACC1", kind=ReconBreakKind.POSITION_MISMATCH,
                status=ReconBreakStatus.OPEN, blocking=True, created_ns=5)]))

        h.bus.on("risk.query.recon_breaks", breaks)
        r = await call(h, "GET", "/risk/recon-breaks", headers=hdr(Role.VIEWER))
        body = r.json()
        assert r.status_code == 200 and [b["break_id"] for b in body["data"]["items"]] == ["B1"]
        assert "risk-engine-1" in body["message"]
        r = await call(h, "POST", "/risk/recon-breaks/B1/resolve", {"resolution": "ACCEPTED", "note": "broker adj"},
                       hdr(Role.RISK_MANAGER, idem=new_key()))
        assert r.status_code == 200 and h.bus.of("risk.recon.resolve")[-1].target == "risk-engine-0"

    async def test_ack_routes_by_alert_key(self, h: Harness) -> None:
        key = "20260928|daily_loss|halt|acct:ACC3/prod:rb|1"
        r = await call(h, "POST", f"/risk/events/{key.replace('|', '%7C').replace('/', '%2F')}/ack", {},
                       hdr(Role.RISK_MANAGER, idem=new_key()))
        assert r.status_code == 200
        c = h.bus.of("risk.ack_alert")[-1]
        assert c.target == "risk-engine-1" and c.payload.alert_key == key


class TestViews:
    def seed(self, h: Harness) -> None:
        h.gw.readmodel.seed_snapshots([
            snapshot("ACC1", shard=0, daily_pnl_pct=-0.015,
                     positions=[position("ACC1", "rb2701", "rb", PosDirection.LONG, 5, 150_000.0)],
                     exposures=[ExposureView(product="rb", sector="black", exchange=Exchange.SHFE, long_lots=5,
                                             short_lots=0, net_lots=5, net_notional=150_000.0,
                                             gross_notional=150_000.0, net_pct_equity=0.015)],
                     rules=[rule_status("daily_loss", "account.daily_pnl_pct_init", -0.015, "acct:ACC1")]),
            snapshot("ACC3", shard=1, daily_pnl_pct=-0.0302,
                     positions=[position("ACC3", "rb2701", "rb", PosDirection.SHORT, 2, 60_000.0),
                                position("ACC3", "IF2612", "IF", PosDirection.LONG, 1, 1_300_000.0,
                                         Exchange.CFFEX)],
                     exposures=[ExposureView(product="rb", sector="black", exchange=Exchange.SHFE, long_lots=0,
                                             short_lots=2, net_lots=-2, net_notional=-60_000.0,
                                             gross_notional=60_000.0, net_pct_equity=-0.006)],
                     rules=[rule_status("daily_loss", "account.daily_pnl_pct_init", -0.0302)]),
        ])

    async def test_accounts_and_names(self, h: Harness) -> None:
        self.seed(h)
        r = await call(h, "GET", "/accounts", headers=hdr(Role.VIEWER))
        items = {i["account_id"]: i for i in r.json()["data"]}
        assert sorted(items) == sorted(h.cfg.accounts.accounts)  # every configured account, with or without data
        assert items["ACC1"]["name"] == h.cfg.accounts.accounts["ACC1"].name and items["ACC1"]["missing"] is False
        assert items["ACC2"] == {"account_id": "ACC2", "name": h.cfg.accounts.accounts["ACC2"].name,
                                 "shard": h.cfg.accounts.accounts["ACC2"].shard, "missing": True, "stale": False,
                                 "age_ms": None}
        r = await call(h, "GET", "/accounts/ACC3/positions", headers=hdr(Role.VIEWER))
        assert {p["instrument_id"] for p in r.json()["data"]} == {"rb2701", "IF2612"}
        r = await call(h, "GET", "/accounts/ACC9", headers=hdr(Role.VIEWER))
        assert r.status_code == 404

    async def test_positions_group_by(self, h: Harness) -> None:
        self.seed(h)
        r = await call(h, "GET", "/positions?group_by=product", headers=hdr(Role.VIEWER))
        data = r.json()["data"]
        groups = {g["group"]: g for g in data["groups"]}
        assert groups["rb"]["long_qty"] == 5 and groups["rb"]["short_qty"] == 2 and groups["rb"]["net_qty"] == 3
        assert groups["rb"]["net_notional"] == pytest.approx(90_000.0) and groups["rb"]["accounts"] == ["ACC1", "ACC3"]
        assert data["accounts_missing"] == ["ACC2", "ACC4", "ACC5"]
        r = await call(h, "GET", "/positions?sector=black&account=ACC3", headers=hdr(Role.VIEWER))
        assert [p["instrument_id"] for p in r.json()["data"]["items"]] == ["rb2701"]
        r = await call(h, "GET", "/positions?group_by=exchange", headers=hdr(Role.VIEWER))
        assert {g["group"] for g in r.json()["data"]["groups"]} == {"SHFE", "CFFEX"}
        r = await call(h, "GET", "/positions?group_by=colour", headers=hdr(Role.VIEWER))
        assert r.status_code == 400

    async def test_compare_view(self, h: Harness) -> None:
        self.seed(h)
        r = await call(h, "GET", "/risk/compare", headers=hdr(Role.VIEWER))
        view = r.json()["data"]
        assert set(view) == {"trading_day", "as_of_ns", "accounts", "rules", "matrix", "ranking", "exposure",
                             "liveness", "accounts_stale"}
        assert view["accounts_stale"] == [] and view["liveness"]["ACC3"] == {"stale": False, "age_ms": None}
        cell = next(c for c in view["matrix"] if c["account_id"] == "ACC3" and c["rule_id"] == "daily_loss")
        assert cell["limit"] == -0.03 and cell["utilization"] == pytest.approx(0.0302 / 0.03) and cell["band"] == "breach"
        # ranking = closeness to the *next* level: ACC1 at 75 % of warn beats ACC3 at 60 % of derisk
        ranking = [(x["account_id"], x["next_level"]) for x in view["ranking"] if x["rule_id"] == "daily_loss"]
        assert ranking == [("ACC1", "warn"), ("ACC3", "derisk")]
        assert view["ranking"][0]["utilization_next"] == pytest.approx(0.75)
        assert view["exposure"]["firm_net"]["rb"] == pytest.approx(90_000.0)

    async def test_compare_history_with_breakers(self, h: Harness) -> None:
        def settle(target: str, payload: Any) -> Reply:
            if payload.trading_day == "20260928":
                return ok_reply(SettlementResult(trading_day="20260928", version=2, accounts=[]))
            return err(codes.NOT_FOUND)

        def alerts(target: str, payload: Any) -> Reply:
            items = []
            if target == "risk-engine-1":
                items.append(RiskEvent(event_id="e1", kind=RiskEventKind.ALERT_RAISED, trading_day=payload.trading_day,
                                       ts_ns=1, account_id="ACC3", action=Action.CIRCUIT_BREAK,
                                       severity=Severity.CRITICAL))
            return ok_reply(RiskEventBatch(items=items))

        h.bus.on("settle.query", settle)
        h.bus.on("risk.query.alerts", alerts)
        r = await call(h, "GET", "/risk/compare/history?days=3", headers=hdr(Role.VIEWER))
        data = r.json()["data"]
        assert [d["trading_day"] for d in data["days"]] == ["20260928"]
        assert data["days"][0]["breakers"]["ACC3"] == 1 and data["days"][0]["breakers"]["ACC1"] == 0
        assert len(data["missing"]) == 2

    async def test_settlement_days(self, h: Harness) -> None:
        def status(target: str, payload: Any) -> Reply:
            if payload.trading_day == "20260924":
                return ok_reply(EodPhaseEvent(trading_day="20260924", version=1, phase=EodPhase.DONE, ts_ns=1))
            return err(codes.NOT_FOUND)

        h.bus.on("settle.status", status)
        r = await call(h, "GET", "/settlement/days?limit=5", headers=hdr(Role.VIEWER))
        assert r.json()["data"]["items"] == [{"trading_day": "20260924", "version": 1, "phase": "DONE",
                                              "warning_count": 0}]


class TestSystemAndAudit:
    async def test_system_services_merges_ps_and_heartbeats(self, h: Harness) -> None:
        h.bus.on("svc.ps", lambda t, p: ok_reply(ServiceList(items=[
            ServiceInfo(instance="md-gateway", service="md-gateway", state=ProcessState.RUNNING, pid=11, restarts=2)])))
        h.gw.readmodel.on_heartbeat(Heartbeat(instance="md-gateway", service="md-gateway", state=ServiceState.RUNNING,
                                              pid=11, epoch="e", app_version="0", started_ns=1, clock_version=3))
        r = await call(h, "GET", "/system/services", headers=hdr(Role.VIEWER))
        items = {i["instance"]: i for i in r.json()["data"]["items"]}
        md = items["md-gateway"]
        assert md["state"] == "RUNNING" and md["process_state"] == "RUNNING" and md["restarts"] == 2
        assert md["heartbeat_state"] == "RUNNING" and md["heartbeat"]["clock_version"] == 3
        assert "risk-engine-1" in items  # configured even without heartbeat
        assert isinstance(h.bus.of("svc.ps")[0].payload, PsRequest)
        r = await call(h, "GET", "/system/status", headers=hdr(Role.VIEWER))
        st = r.json()["data"]
        assert st["trading_day"] == "20260928" and st["clock"]["market_now_ns"] == MONDAY_10AM
        assert "service_events" in st and st["mode"] == "demo"

    async def test_audit_rows(self, h: Harness) -> None:
        key = new_key()
        await call(h, "POST", "/risk/accounts/ACC3/halt", {"reason": "x"}, hdr(Role.RISK_MANAGER, idem=key))
        await call(h, "GET", "/accounts", headers=hdr(Role.VIEWER))
        await call(h, "GET", "/accounts")
        await asyncio.to_thread(h.db.flush)
        rows = await h.gw.store.audit_rows()
        halt = next(r for r in rows if r["path"].endswith("/halt"))
        assert halt["key_id"] == "risk_manager" and halt["role"] == "RISK_MANAGER" and halt["method"] == "POST"
        assert halt["command"] == "risk.halt" and halt["command_id"] == key and halt["target"] == "ACC3"
        assert halt["http_status"] == 200 and halt["result_code"] == "OK" and halt["latency_us"] >= 0
        anon = [r for r in rows if r["key_id"] is None and r["path"].endswith("/accounts")]
        assert anon and anon[-1]["http_status"] == 401 and anon[-1]["result_code"] == codes.UNAUTHORIZED

    async def test_report_download(self, h: Harness) -> None:
        f = h.layout.reports_dir / "20260928" / "risk_daily.html"
        f.parent.mkdir(parents=True)
        f.write_text("<h1>日报</h1>", encoding="utf-8")

        def get(target: str, payload: Any) -> Reply:
            path = "reports/20260928/risk_daily.html" if payload.report_id == "R1" else "config/app.yaml"
            return ok_reply(ReportReady(report_id=payload.report_id, trading_day="20260928",
                                        type=ReportType.RISK_DAILY_HTML, path=path, sha256="ab", size_bytes=1,
                                        created_ns=1))

        h.bus.on("report.get", get)
        r = await call(h, "GET", "/reports/R1", headers=hdr(Role.VIEWER))
        assert r.status_code == 200 and "日报" in r.text and r.headers["X-Report-Sha256"] == "ab"
        assert r.headers["content-type"].startswith("text/html")
        r = await call(h, "GET", "/reports/R2", headers=hdr(Role.VIEWER))
        assert r.status_code == 403  # outside the reports volume

    async def test_demo_guard(self, tmp_path: Path, default_config: FullConfig) -> None:
        cfg, env = with_keys(default_config, mode="prod")
        prod = await make_gateway(tmp_path, cfg, env=env)
        try:
            r = await call(prod, "POST", "/demo/shock", {"pct": -0.04, "sector": "black"},
                           hdr(Role.ADMIN, idem=new_key()))
            assert r.status_code == 403 and "demo" in r.json()["message"] and not prod.sim.calls
        finally:
            await prod.close()

    async def test_demo_proxy(self, h: Harness) -> None:
        h.sim.responses[("POST", "/faults")] = {"fault_id": "F1"}
        r = await call(h, "POST", "/demo/faults", {"type": "FEED_DISCONNECT", "target": "feed_a",
                                                   "params": {"refuse_reconnect_s": 20}}, hdr(Role.ADMIN, idem=new_key()))
        assert r.status_code == 200 and r.json()["data"] == {"fault_id": "F1"}
        assert h.sim.calls[-1] == ("POST", "/faults", {"type": "FEED_DISCONNECT", "target": "feed_a",
                                                       "params": {"refuse_reconnect_s": 20}})
        r = await call(h, "DELETE", "/demo/faults/F1", headers=hdr(Role.ADMIN, idem=new_key()))
        assert r.status_code == 200 and h.sim.calls[-1] == ("DELETE", "/faults/F1", None)
        r = await call(h, "POST", "/demo/kill/risk-engine-1", None, hdr(Role.ADMIN, idem=new_key()))
        assert r.status_code == 200 and h.bus.of("svc.kill")[-1].payload.instance == "risk-engine-1"
        # 02 §8.8 through REST (details in test_apigw_clock)
        h.bus.on("svc.clock", lambda t, p: err(codes.FORBIDDEN, "not demo"))
        r = await call(h, "POST", "/demo/clock", {"target": ClockTarget.PRE_CLOSE.value},
                       hdr(Role.ADMIN, idem=new_key()))
        assert r.status_code == 403 and [c[1] for c in h.sim.calls[-2:]] == ["/pause", "/resume"]

    async def test_openapi_and_docs(self, h: Harness) -> None:
        async with h.client() as c:
            spec = (await c.get("/openapi.json")).json()
            assert "/api/v1/risk/firm/kill-switch" in spec["paths"] and "/api/v1/demo/clock" in spec["paths"]
            assert (await c.get("/docs")).status_code == 200
            assert (await c.get("/")).status_code == 200  # placeholder without a dashboard build


class TestQueries:
    async def test_positions_fallback_and_events_merge(self, h: Harness) -> None:
        from futrisk.messages.envelope import make_envelope
        from futrisk.messages.risk import PositionBatch

        h.bus.on("risk.query.positions", lambda t, p: ok_reply(PositionBatch(items=[
            position("ACC4", "IF2612", "IF", PosDirection.LONG, 1, 1_300_000.0, Exchange.CFFEX)])))
        r = await call(h, "GET", "/accounts/ACC4/positions", headers=hdr(Role.VIEWER))
        assert r.status_code == 200 and [p["instrument_id"] for p in r.json()["data"]] == ["IF2612"]
        assert h.bus.of("risk.query.positions")[-1].target == "risk-engine-1"

        def ev(event_id: str, ts: int) -> RiskEvent:
            return RiskEvent(event_id=event_id, kind=RiskEventKind.ALERT_RAISED, trading_day="20260928", ts_ns=ts,
                             account_id="ACC3", severity=Severity.HIGH, message=event_id)

        h.bus.on("risk.query.alerts", lambda t, p: ok_reply(RiskEventBatch(items=[ev("q1", 5)]))
                 if t == "risk-engine-1" else err(codes.UNAVAILABLE))
        h.gw.readmodel.apply(make_envelope("risk.event", ev("live1", 9), src="risk-engine-1", epoch="e", ts_ns=9, seq=1))
        r = await call(h, "GET", "/risk/events", headers=hdr(Role.VIEWER))
        data = r.json()["data"]
        assert [e["event_id"] for e in data["items"]] == ["q1", "live1"]
        assert data["errors"] == {"risk-engine-0": codes.UNAVAILABLE} and "partial" in r.json()["message"]
        r = await call(h, "GET", "/risk/events?account=ACC3&td=20260928", headers=hdr(Role.VIEWER))
        assert h.bus.of("risk.query.alerts")[-1].target == "risk-engine-1" and r.json()["data"]["errors"] == {}

    async def test_market_endpoints(self, h: Harness) -> None:
        from futrisk.messages.envelope import make_envelope
        from futrisk.messages.md import MdStatus, Tick

        t = Tick(instrument_id="rb2701", exchange=Exchange.SHFE, trading_day="20260928", action_day="20260928",
                 exch_ts_ns=MONDAY_10AM, last=3100.0, volume=10, turnover=310_000.0, open_interest=5,
                 upper_limit=3276.0, lower_limit=2964.0, pre_settle=3120.0, source="FEED_A",  # type: ignore[arg-type]
                 recv_ns=MONDAY_10AM)
        h.gw.readmodel.apply(make_envelope("md.tick.rb2701", t, src="md-gateway", epoch="e", ts_ns=1, seq=1))
        r = await call(h, "GET", "/market/quotes", headers=hdr(Role.VIEWER))
        assert [q["instrument_id"] for q in r.json()["data"]] == ["rb2701"]
        r = await call(h, "GET", "/market/quotes?instruments=cu2611", headers=hdr(Role.VIEWER))
        assert r.json()["data"] == []
        h.bus.on("md.sources", lambda tg, p: ok_reply(MdStatus(as_of_ns=1, trading_day="20260928", sources=[],
                                                               instruments=[])))
        r = await call(h, "GET", "/market/feeds", headers=hdr(Role.VIEWER))
        assert r.json()["data"]["trading_day"] == "20260928" and h.bus.of("md.sources")
        r = await call(h, "POST", "/market/sources/feed_x", {"action": "EXCLUDE", "reason": "x"},
                       hdr(Role.ADMIN, idem=new_key()))
        assert r.status_code == 400
        r = await call(h, "POST", "/market/sources/feed_b", {"action": "EXCLUDE", "reason": "maint"},
                       hdr(Role.ADMIN, idem=new_key()))
        sent = h.bus.of("md.source.set")[-1].payload
        assert r.status_code == 200 and sent.source == "FEED_B" and sent.actor == "admin"
