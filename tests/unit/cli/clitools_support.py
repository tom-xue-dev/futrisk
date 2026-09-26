"""Test support for the tools CLI: a fake api-gateway + sim-exchange control API behind an
``httpx.MockTransport``, a home factory and a CliRunner harness.

Every route a test fakes is checked against the *real* routes (api-gateway's ``build_api_router``
and sim-exchange's ``create_control_app``), so the fakes cannot drift from the API.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from functools import cache
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, unquote

import httpx
import typer
import yaml
from typer.testing import CliRunner

from futrisk.cli.tools_cmd import register
from futrisk.common.config import ensure_home_config, snapshot_working_config, write_active
from futrisk.common.paths import HomeLayout

API_URL = "http://127.0.0.1:18080"
API_PORT = 18080
SIM_PORT = 7004  # sim.yaml control port of the bundled config
KEY = "frk_test-admin-key"
PREFIX = "/api/v1"
HEALTH = {"status": "ok", "instance": "api-gateway", "pid": 4242, "epoch": "e-1", "trading_day": "20260928",
          "ts_ns": 1_790_000_000_000_000_000}


def http_status(code: str) -> int:
    """The api-gateway's own code -> HTTP status mapping (02 §9.6; business outcomes are 200)."""
    from futrisk.services.api_gateway.rest import http_status as gateway_status

    return gateway_status(code)


def envelope(data: Any = None, *, code: str = "OK", ok: bool | None = None, message: str = "",
             duplicate: bool = False) -> dict[str, Any]:
    doc: dict[str, Any] = {"ok": code == "OK" if ok is None else ok, "code": code, "message": message, "data": data}
    if duplicate:
        doc["duplicate"] = True
    return doc


# ---------------------------------------------------------------------------- real routes
@cache
def _api_routes() -> list[Any]:
    from futrisk.services.api_gateway.routes import build_api_router

    return list(build_api_router(object()).routes)  # type: ignore[arg-type]


@cache
def _sim_routes() -> list[Any]:
    from futrisk.sim.exchange.control import create_control_app

    return list(create_control_app(object()).routes)  # type: ignore[arg-type]


def route_exists(routes: list[Any], method: str, path: str) -> bool:
    from starlette.routing import Match

    scope = {"type": "http", "path": unquote(path), "root_path": "", "method": method}
    return any(r.matches(scope)[0] == Match.FULL for r in routes)


# ---------------------------------------------------------------------------- fake API
@dataclass
class Req:
    """One recorded request."""

    method: str
    host: str
    port: int | None
    path: str  # raw (percent-encoded) path
    params: dict[str, str]
    headers: dict[str, str]
    body: Any

    @property
    def idem(self) -> str | None:
        return self.headers.get("idempotency-key")


Answer = tuple[int, bytes, dict[str, str]]
Handler = Callable[[Req], Any]


@dataclass
class FakeApi:
    """api-gateway ``/api/v1`` (auth, Idempotency-Key, demo guard) and sim-exchange ``/control``.

    ``GET /health`` answers with ``home_id`` unless a test fakes it; a route without a fake answers
    like api-gateway's catch-all (``404 unknown API path``), i.e. like an api-gateway without it.
    """

    demo: bool = True
    home_id: str = ""
    requests: list[Req] = field(default_factory=list)
    routes: dict[tuple[str, str], list[Handler]] = field(default_factory=dict)
    sim_routes: dict[tuple[str, str], Handler] = field(default_factory=dict)
    answered: set[tuple[str, str]] = field(default_factory=set)

    # ---------------------------------------------------------------- setup
    def on(self, method: str, path: str, data: Any = None, *, code: str = "OK", ok: bool | None = None,
           message: str = "", duplicate: bool = False, status: int | None = None, raw: bytes | None = None,
           headers: dict[str, str] | None = None, handler: Handler | None = None, times: int | None = None) -> None:
        """Answer ``method PREFIX+path`` (raw path) with an envelope, ``raw`` bytes or ``handler(req)``.

        Several ``on`` calls for the same route before it is called answer in turn (the last one
        repeats); an ``on`` after the route answered replaces what it answers."""
        full = PREFIX + path
        assert route_exists(_api_routes(), method, full), f"not an api-gateway route: {method} {full}"

        def answer(req: Req) -> Any:
            if handler is not None:
                return handler(req)
            body = raw if raw is not None else json.dumps(
                envelope(data, code=code, ok=ok, message=message, duplicate=duplicate)).encode()
            return status or http_status(code), body, headers or {}

        if (method, full) in self.answered:
            self.answered.discard((method, full))
            self.routes.pop((method, full), None)
        self.routes.setdefault((method, full), []).extend([answer] * (times or 1))

    def on_sim(self, method: str, path: str, doc: Any, *, status: int = 200) -> None:
        full = "/control" + path
        assert route_exists(_sim_routes(), method, full), f"not a sim-exchange control route: {method} {full}"
        self.sim_routes[(method, full)] = lambda req: (status, json.dumps(doc).encode(), {})

    # ------------------------------------------------------------- queries
    def calls(self, method: str | None = None, path: str | None = None) -> list[Req]:
        return [r for r in self.requests if (method is None or r.method == method)
                and (path is None or r.path == (path if path.startswith("/control") else PREFIX + path))]

    def last(self, method: str, path: str) -> Req:
        found = self.calls(method, path)
        assert found, f"no {method} {path}; got {[(r.method, r.path) for r in self.requests]}"
        return found[-1]

    # ------------------------------------------------------------- transport
    def __call__(self, request: httpx.Request) -> httpx.Response:
        raw_path = request.url.raw_path.decode("ascii").split("?", 1)[0]
        body: Any = None
        if request.content:
            body = json.loads(request.content)
        req = Req(
            method=request.method,
            host=request.url.host,
            port=request.url.port,
            path=raw_path,
            params=dict(parse_qsl(request.url.query.decode("ascii"), keep_blank_values=True)),
            headers={k.lower(): v for k, v in request.headers.items()},
            body=body,
        )
        self.requests.append(req)
        if req.port == SIM_PORT:
            handler = self.sim_routes.get((req.method, raw_path))
            status, content, headers = handler(req) if handler else (404, b'{"ok":false,"code":"NOT_FOUND"}', {})
            return httpx.Response(status, content=content, headers={"content-type": "application/json", **headers})
        assert req.port == API_PORT, f"unexpected host {request.url}"
        return self._api(req)

    def _api(self, req: Req) -> httpx.Response:
        def reply(code: str, message: str) -> httpx.Response:
            return httpx.Response(http_status(code), json=envelope(None, code=code, message=message))

        if req.path != PREFIX + "/health" and req.headers.get("x-api-key") != KEY:
            return reply("UNAUTHORIZED", "missing or invalid X-API-Key")
        if req.method in ("POST", "DELETE") and req.path != PREFIX + "/orders":
            key = req.idem or ""
            try:
                uuid.UUID(key)
            except ValueError:
                return reply("BAD_REQUEST", "the Idempotency-Key header (UUID) is required")
        if req.path.startswith(PREFIX + "/demo/") and not self.demo:
            return reply("FORBIDDEN", "demo endpoints are only available in demo mode (app.mode=demo)")
        answers = self.routes.get((req.method, req.path))
        if not answers and (req.method, req.path) == ("GET", PREFIX + "/health"):
            return httpx.Response(200, json=envelope(HEALTH | {"home_id": self.home_id}))
        if not answers:
            return reply("NOT_FOUND", f"unknown API path {req.path}")
        answer = answers.pop(0) if len(answers) > 1 else answers[0]
        self.answered.add((req.method, req.path))
        result = answer(req)
        if isinstance(result, httpx.Response):
            return result
        if isinstance(result, dict):  # an envelope
            return httpx.Response(http_status(str(result.get("code"))), json=result)
        status, content, headers = result
        return httpx.Response(status, content=content, headers={"content-type": "application/json", **headers})


# ---------------------------------------------------------------------------- home
def make_home(root: Path, *, api_url: str = API_URL, mode: str = "demo", key: str | None = KEY,
              services: dict[str, Any] | None = None, sim: dict[str, Any] | None = None) -> Path:
    """A home whose committed config points api-gateway at ``api_url`` (+ the bootstrap key file)."""
    layout = HomeLayout(root)
    ensure_home_config(layout)
    svc = yaml.safe_load((layout.config_dir / "services.yaml").read_text(encoding="utf-8"))
    svc["instances"]["api-gateway"]["http"] = api_url
    for name, update in (services or {}).items():
        svc["instances"].setdefault(name, {}).update(update)
    (layout.config_dir / "services.yaml").write_text(yaml.safe_dump(svc, sort_keys=False), encoding="utf-8")
    if sim:
        doc = yaml.safe_load((layout.config_dir / "sim.yaml").read_text(encoding="utf-8"))
        for section, update in sim.items():
            doc.setdefault(section, {}).update(update)
        (layout.config_dir / "sim.yaml").write_text(yaml.safe_dump(doc, sort_keys=False), encoding="utf-8")
    app = yaml.safe_load((layout.config_dir / "app.yaml").read_text(encoding="utf-8"))
    app["mode"] = mode
    (layout.config_dir / "app.yaml").write_text(yaml.safe_dump(app, sort_keys=False), encoding="utf-8")
    n, path = snapshot_working_config(layout)
    write_active(layout, n, path)
    if key is not None:
        layout.secrets_dir.mkdir(parents=True, exist_ok=True)
        (layout.secrets_dir / "bootstrap_admin.key").write_text(key + "\n", encoding="utf-8")
    return layout.home


@dataclass
class Cli:
    """The tools commands on a fresh Typer app, a home and the fake API."""

    app: typer.Typer
    home: Path
    api: FakeApi
    sleeps: list[float]

    def invoke(self, *args: str, home: bool = True) -> Any:
        argv = [*args, "--home", str(self.home)] if home else list(args)
        return CliRunner().invoke(self.app, argv, catch_exceptions=False)

    def json(self, *args: str) -> Any:
        r = self.invoke(*args, "--json")
        return r, json.loads(r.stdout)


def tools_app() -> typer.Typer:
    app = typer.Typer()

    @app.command("version")  # a second command keeps Typer in multi-command mode
    def _version() -> None:
        pass

    register(app)
    return app


def is_uuid(value: str | None) -> bool:
    try:
        uuid.UUID(value or "")
    except ValueError:
        return False
    return True


# ---------------------------------------------------------------------------- canned data
def position(account: str, instrument: str, direction: str = "LONG", **kw: Any) -> dict[str, Any]:
    doc = {"account_id": account, "instrument_id": instrument, "product": instrument.rstrip("0123456789"),
           "exchange": "SHFE", "direction": direction, "td_qty": 2, "yd_qty": 1, "closable_qty": 3,
           "closable_td_qty": 2, "closable_yd_qty": 1, "avg_open_price": 3120.5, "basis_avg_price": 3120.5,
           "last": 3100.0, "position_pnl": -615.0, "margin": 12345.6, "notional": 93000.0, "pct_equity": 0.0093,
           "sector": "black"}
    doc.update(kw)
    return doc


def snapshot(account: str, state: str = "NORMAL", **funds: Any) -> dict[str, Any]:
    f = {"initial_capital": 10000000.0, "equity": 10000000.0, "margin": 3500000.0, "available": 6500000.0,
         "risk_degree": 0.35}
    f.update(funds)
    return {"account_id": account, "name": f"{account} name", "shard": 1, "trading_day": "20260928", "version": 7,
            "state_version": 3, "as_of_ns": 1_790_000_000_000_000_000, "quality": "OK", "state": state,
            "halted_latched": state == "HALTED", "trading_mode": "OPEN", "restrictions": [], "funds": f,
            "metrics": {"daily_pnl": -302000.0, "daily_pnl_pct_init": -0.0302}, "positions": [], "open_orders": 2}


__all__ = [
    "API_URL",
    "KEY",
    "Cli",
    "FakeApi",
    "Req",
    "envelope",
    "is_uuid",
    "make_home",
    "position",
    "route_exists",
    "snapshot",
    "tools_app",
]
