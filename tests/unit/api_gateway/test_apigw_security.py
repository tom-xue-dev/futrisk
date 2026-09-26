"""REST hardening (review M7, L1, L5, L7, L10, L14, L15 + CLI addendum): every route of the OpenAPI document
requires a key (permanent enumeration), roles per route class, body-size limit and key check before any body is
read, failed-auth throttle, scrubbed errors, 404/405 envelopes for unknown API paths, security headers,
statement shape, read deadlines, health identity, the demo proxy routes and their idempotency."""

from __future__ import annotations

import json
import os
import re
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from apigw_support import Harness, err, hdr, make_gateway, new_key, with_keys
from loguru import logger

from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.messages.enums import ReportType, Role
from futrisk.messages.envelope import Reply, ok_reply
from futrisk.messages.eod import ReportList, ReportReady, SettlementResult
from futrisk.services.api_gateway.app import DEFAULT_BODY_LIMIT
from futrisk.services.api_gateway.ports import SimControlError

API = "/api/v1"
PUBLIC = {("GET", f"{API}/health")}
SAMPLE = {"account_id": "ACC3", "td": "20260928", "instrument": "rb2701", "instance": "md-gateway",
          "source": "feed_a", "client_order_id": "0192f000-0000-7000-8000-000000000009", "fault_id": "F1",
          "report_id": "R1", "run_id": "B1", "plan_id": "P1", "break_id": "K1", "alert_key": "a|b|c|acct:ACC3|1",
          "account": "ACC3", "version": "1"}


@pytest.fixture
async def h(tmp_path: Path, default_config: FullConfig) -> AsyncIterator[Harness]:
    cfg, env = with_keys(default_config)
    harness = await make_gateway(tmp_path, cfg, env=env)
    try:
        yield harness
    finally:
        await harness.close()


def routes(h: Harness) -> list[tuple[str, str]]:
    spec = h.app.openapi()
    out = []
    for path, ops in spec["paths"].items():
        concrete = re.sub(r"\{(\w+)(?::\w+)?\}", lambda m: SAMPLE.get(m.group(1), "x"), path)
        for method in ops:
            out.append((method.upper(), concrete))
    return out


async def call(h: Harness, method: str, path: str, body: Any = None, headers: dict[str, str] | None = None,
               **kw: Any) -> Any:
    async with h.client() as c:
        return await c.request(method, path, json=body, headers=headers or {}, **kw)


class TestEveryRoute:
    async def test_every_route_requires_a_key_and_the_right_role(self, h: Harness) -> None:
        all_routes = routes(h)
        assert len(all_routes) >= 60  # the enumeration really covers the API (58 route x method pairs + new ones)
        for method, path in all_routes:
            h.gw.auth_throttle.reset()
            r = await call(h, method, path, {})
            if (method, path) in PUBLIC:
                assert r.status_code == 200, (method, path)
                continue
            assert r.status_code == 401 and r.json()["code"] == codes.UNAUTHORIZED, (method, path, r.status_code)
            viewer = await call(h, method, path, {}, hdr(Role.VIEWER, idem=new_key()))
            admin_only_read = path.startswith(f"{API}/demo/")
            if method != "GET" or admin_only_read:
                assert viewer.status_code == 403, (method, path, viewer.status_code)
            else:
                assert viewer.status_code not in (401, 403), (method, path, viewer.status_code)
            assert viewer.headers["X-Content-Type-Options"] == "nosniff"


class TestBodies:
    async def test_body_size_limit(self, h: Harness) -> None:
        big = {"reason": "x" * (DEFAULT_BODY_LIMIT + 10)}
        r = await call(h, "POST", f"{API}/risk/accounts/ACC3/halt", big, hdr(Role.RISK_MANAGER, idem=new_key()))
        assert r.status_code == 413 and r.json()["code"] == codes.BAD_REQUEST and not h.bus.of("risk.halt")
        # without Content-Length (chunked) the limit still holds
        payload = json.dumps(big).encode()

        async def chunks() -> Any:
            for i in range(0, len(payload), 65536):
                yield payload[i:i + 65536]

        async with h.client() as c:
            r = await c.post(f"{API}/risk/accounts/ACC3/halt", content=chunks(),
                             headers=hdr(Role.RISK_MANAGER, idem=new_key()) | {"Content-Type": "application/json"})
        assert r.status_code == 413
        # trade imports may be larger (32 MiB)
        trades = {"trading_day": "20260928", "trades": [], "source": "x" * (DEFAULT_BODY_LIMIT + 10)}
        r = await call(h, "POST", f"{API}/trades/import", trades, hdr(Role.ADMIN, idem=new_key()))
        assert r.status_code != 413

    async def test_no_key_no_body_read(self, h: Harness) -> None:
        received: list[str] = []
        sent: list[dict[str, Any]] = []

        async def receive() -> dict[str, Any]:
            received.append("body")
            return {"type": "http.request", "body": b"{}" * 1000, "more_body": True}

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        scope = {"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": "POST",
                 "scheme": "http", "path": f"{API}/risk/accounts/ACC3/halt", "raw_path": b"", "query_string": b"",
                 "root_path": "", "headers": [(b"host", b"x"), (b"content-type", b"application/json")],
                 "client": ("10.0.0.9", 5555), "server": ("127.0.0.1", 8000)}
        await h.app(scope, receive, send)
        assert sent[0]["status"] == 401 and received == []  # p07: 151 MB without a key were buffered

    async def test_client_order_id_must_be_a_uuid(self, h: Harness) -> None:
        order = {"client_order_id": "abc", "account_id": "ACC3", "instrument_id": "rb2701", "direction": "BUY",
                 "offset": "OPEN", "price": 3100.0, "qty": 1}
        r = await call(h, "POST", f"{API}/orders", order, hdr(Role.TRADER))
        assert r.status_code == 400 and "UUID" in r.json()["message"] and not h.bus.of("order.submit")


class TestErrors:
    async def test_unexpected_errors_are_scrubbed(self, h: Harness) -> None:
        def boom(target: str, payload: Any) -> Reply:
            raise RuntimeError("C:\\Users\\ops\\secret.db: SELECT password FROM x")

        h.bus.on("risk.query.positions", boom)
        lines: list[str] = []
        sink = logger.add(lambda m: lines.append(str(m) + json.dumps(m.record["extra"], default=str)), level="ERROR")
        try:
            r = await call(h, "GET", f"{API}/accounts/ACC3/positions", headers=hdr(Role.VIEWER))
        finally:
            logger.remove(sink)
        body = r.json()
        assert r.status_code == 500 and body["code"] == codes.INTERNAL
        assert "secret" not in r.text and "SELECT" not in r.text and "RuntimeError" not in r.text
        error_id = body["data"]["error_id"]
        assert error_id in body["message"] and any(error_id in line for line in lines)  # details only in the log

    async def test_unknown_api_paths(self, h: Harness) -> None:
        r = await call(h, "POST", f"{API}/nope", {}, hdr(Role.ADMIN, idem=new_key()))
        assert r.status_code == 404 and r.json()["code"] == codes.NOT_FOUND
        r = await call(h, "POST", f"{API}/nope", {})
        assert r.status_code == 401  # no key: nothing is said about the path
        r = await call(h, "DELETE", f"{API}/accounts", None, hdr(Role.ADMIN, idem=new_key()))
        assert r.status_code == 405 and r.json()["code"] == codes.BAD_REQUEST and "GET" in r.json()["message"]

    async def test_failed_auth_is_throttled(self, h: Harness) -> None:
        h.gw.auth_throttle.burst = 5
        for _ in range(5):
            assert (await call(h, "GET", f"{API}/accounts", headers={"X-API-Key": "guess-guess-guess"})).status_code == 401
        r = await call(h, "GET", f"{API}/accounts", headers=hdr(Role.VIEWER))
        assert r.status_code == 429 and r.json()["code"] == codes.BUSY and int(r.headers["Retry-After"]) >= 1
        r = await call(h, "POST", f"{API}/risk/accounts/ACC3/halt", {"reason": "x"},
                       {"X-API-Key": "guess-guess-guess", "Idempotency-Key": new_key()})
        assert r.status_code == 429
        h.gw.auth_throttle.reset()
        assert (await call(h, "GET", f"{API}/accounts", headers=hdr(Role.VIEWER))).status_code == 200


class TestContent:
    async def test_statement_shape_and_sandbox(self, h: Harness) -> None:
        f = h.layout.reports_dir / "20260928" / "ACC3.txt"
        f.parent.mkdir(parents=True)
        f.write_text("结算单 ACC3", encoding="utf-8")
        rep = ReportReady(report_id="S1", trading_day="20260928", type=ReportType.STATEMENT_TXT, account_id="ACC3",
                          path="reports/20260928/ACC3.txt", sha256="ab", size_bytes=1, created_ns=1)
        h.bus.on("report.list", lambda t, p: ok_reply(ReportList(items=[rep])))
        h.bus.on("report.get", lambda t, p: ok_reply(rep))
        h.bus.on("settle.query", lambda t, p: ok_reply(SettlementResult(trading_day="20260928", version=1,
                                                                         accounts=[])))
        r = await call(h, "GET", f"{API}/settlement/20260928/statements/ACC3", headers=hdr(Role.VIEWER))
        assert r.json()["data"] == {"format": "text", "text": "结算单 ACC3"}
        assert r.headers["Content-Security-Policy"] == "sandbox" and r.headers["X-Content-Type-Options"] == "nosniff"
        h.bus.on("report.list", lambda t, p: ok_reply(ReportList(items=[])))
        r = await call(h, "GET", f"{API}/settlement/20260928/statements/ACC3", headers=hdr(Role.VIEWER))
        data = r.json()["data"]
        assert data["format"] == "settlement" and data["settlement"]["version"] == 1 and "text" not in data
        # report files are served as sandboxed, never sniffed documents
        r = await call(h, "GET", f"{API}/reports/S1", headers=hdr(Role.VIEWER))
        assert r.status_code == 200 and r.headers["Content-Security-Policy"] == "sandbox"
        assert r.headers["X-Content-Type-Options"] == "nosniff"
        # ?kind= filter
        other = ReportReady(report_id="D1", trading_day="20260928", type=ReportType.RISK_DAILY_HTML, path="x",
                            sha256="cd", size_bytes=1, created_ns=2)
        h.bus.on("report.list", lambda t, p: ok_reply(ReportList(items=[rep, other])))
        r = await call(h, "GET", f"{API}/reports?kind=RISK_DAILY_HTML", headers=hdr(Role.VIEWER))
        assert [x["report_id"] for x in r.json()["data"]["items"]] == ["D1"]
        r = await call(h, "GET", f"{API}/reports?kind=PDF", headers=hdr(Role.VIEWER))
        assert r.status_code == 400

    async def test_read_deadline_and_settle_run_budget(self, h: Harness) -> None:
        await call(h, "GET", f"{API}/settlement/20260928", headers=hdr(Role.VIEWER))
        q = h.bus.of("settle.query")[-1]
        assert q.kw["retries"] == 0 and 0 < q.kw["timeout_ms"] <= 5000  # was 30 s x 4 attempts (L1)
        await call(h, "GET", f"{API}/risk/recon-breaks", headers=hdr(Role.VIEWER))
        assert all(c.kw["retries"] == 0 and c.kw["timeout_ms"] <= 5000 for c in h.bus.of("risk.query.recon_breaks"))
        await call(h, "POST", f"{API}/settlement/run", {"td": "20260928"}, hdr(Role.ADMIN, idem=new_key()))
        run = h.bus.of("settle.run")[-1]
        assert run.kw == {"timeout_ms": 10_000, "retries": 0}

    async def test_health_identity(self, h: Harness) -> None:
        for path in (f"{API}/health", "/health"):
            data = (await call(h, "GET", path)).json()["data"]
            assert data["home_id"] == h.layout.home_id() and data["pid"] == os.getpid()
            assert data["epoch"] == "test-epoch" and data["state"]
            assert not any("key" in k.lower() for k in data if k != "home_id")


class TestDemoProxy:
    async def test_read_and_cancel_routes(self, h: Harness) -> None:
        h.sim.responses[("GET", "/state")] = {"faults": [{"fault_id": "F1"}], "fault_history": [{"fault_id": "F0"}],
                                              "paused": False}
        h.sim.responses[("GET", "/scenarios")] = [{"name": "crash", "description": "x"}]
        h.sim.responses[("DELETE", "/scenario")] = {"run_id": "R1", "state": "CANCELLED"}
        r = await call(h, "GET", f"{API}/demo/state", headers=hdr(Role.ADMIN))
        assert r.status_code == 200 and r.json()["data"]["paused"] is False
        r = await call(h, "GET", f"{API}/demo/faults", headers=hdr(Role.ADMIN))
        assert r.json()["data"] == {"items": [{"fault_id": "F1"}], "history": [{"fault_id": "F0"}]}
        r = await call(h, "GET", f"{API}/demo/scenarios", headers=hdr(Role.ADMIN))
        assert r.json()["data"] == [{"name": "crash", "description": "x"}]
        key = new_key()
        r = await call(h, "DELETE", f"{API}/demo/scenario", None, hdr(Role.ADMIN, idem=key))
        assert r.status_code == 200 and r.json()["data"]["state"] == "CANCELLED"
        assert ("DELETE", "/scenario", None) in h.sim.calls
        r = await call(h, "GET", f"{API}/demo/state", headers=hdr(Role.VIEWER))
        assert r.status_code == 403  # same roles as the other /demo routes (ADMIN)
        await h.gw.store.db.run_write(lambda c: None)
        rows = await h.gw.store.audit_rows()
        assert any(x["path"].endswith("/demo/scenario") and x["command"] == "sim.scenario.cancel" for x in rows)

    async def test_timeouts_are_recorded(self, h: Harness) -> None:
        h.sim.fail[("POST", "/faults")] = SimControlError(codes.TIMEOUT, "sim did not answer")
        key = new_key()
        body = {"type": "FEED_GAP", "target": "feed_a", "params": {"drop_count": 3}}
        r1 = await call(h, "POST", f"{API}/demo/faults", body, hdr(Role.ADMIN, idem=key))
        r2 = await call(h, "POST", f"{API}/demo/faults", body, hdr(Role.ADMIN, idem=key))
        assert r1.status_code == 504 and r2.status_code == 504 and r2.json()["duplicate"] is True
        assert [c[1] for c in h.sim.calls].count("/faults") == 1  # never injected twice (L10)

    async def test_prod_mode_hides_demo_routes(self, tmp_path: Path, default_config: FullConfig) -> None:
        cfg, env = with_keys(default_config, mode="prod")
        prod = await make_gateway(tmp_path, cfg, env=env)
        try:
            for method, path in (("GET", "/demo/state"), ("GET", "/demo/scenarios"), ("DELETE", "/demo/scenario")):
                r = await call(prod, method, API + path, None, hdr(Role.ADMIN, idem=new_key()))
                assert r.status_code == 403 and "demo" in r.json()["message"]
            assert not prod.sim.calls
        finally:
            await prod.close()


def _unused() -> Reply:
    return err(codes.OK)
