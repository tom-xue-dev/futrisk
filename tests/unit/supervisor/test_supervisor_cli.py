"""Operations CLI against a fake supervisor (bus, identity ``cli``) and a fake api-gateway (REST):
``ps``, ``kill``, ``status``, ``down``, ``up``, ``logs``, ``demo`` (01 §7.1; 02 §4.1, §6.6, §9.6), and the
review-round guards: another home's supervisor / api-gateway is refused, the demo's identity checks, the
bots' mode switch and revert, seeding only into empty books, ``logs -f`` vs rotation."""

from __future__ import annotations

import http.server
import io
import json
import os
import re
import threading
import time
import uuid
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
import typer
import yaml
import zmq
from typer.testing import CliRunner

from futrisk.bus.rpc_common import DEFERRED
from futrisk.bus.rpc_server import RpcServer
from futrisk.bus.whitelist import Whitelist
from futrisk.cli import ops_cmd
from futrisk.common.clock import BJ, bj_to_ns
from futrisk.common.config import FullConfig, ensure_home_config, snapshot_working_config, write_active
from futrisk.common.paths import HomeLayout
from futrisk.messages import codes, encode_envelope, make_envelope
from futrisk.messages.common import Pong, SnapshotBatch, SnapshotItem
from futrisk.messages.enums import ClockMode, ProcessState, ServiceState
from futrisk.messages.envelope import err_reply, ok_reply
from futrisk.messages.system import ClockState, Heartbeat, ServiceInfo, ServiceList, SvcRequest
from futrisk.runtime.testing import config_with_free_ports

MARKET_NOW = bj_to_ns(datetime(2026, 9, 28, 8, 58, tzinfo=BJ))
INSTANCES = ["sim-exchange", "md-gateway", "trade-gateway", "clearing-worker", "risk-engine-0", "risk-engine-1", "api-gateway", "strategy-sim"]


# ---------------------------------------------------------------------------- fakes
class FakeSupervisor:
    """The supervisor's ROUTER (with the real whitelist) answering canned replies."""

    def __init__(
        self,
        cfg: FullConfig,
        *,
        home_id: str | None,
        states: dict[str, str] | None = None,
        instances: list[str] | None = None,
        heartbeat: dict[str, str | None] | None = None,
        env: dict[str, dict[str, str]] | None = None,
        ps_silent: bool = False,
        timeline: list[str] | None = None,
        clock_only: bool = False,
    ) -> None:
        self.cfg = cfg
        self.home_id = home_id  # what ctl.ping / svc.ps report (None: an old or foreign build)
        self.offset = MARKET_NOW - time.time_ns()
        self.states = {i: "RUNNING" for i in (INSTANCES if instances is None else instances)}
        self.states.update(states or {})
        self.heartbeat = dict(heartbeat or {})  # instance -> heartbeat_state override
        self.env: dict[str, dict[str, str]] = {k: dict(v) for k, v in (env or {}).items()}
        self.ps_silent = ps_silent  # svc.ps never answered (a timeout)
        self.clock_only = clock_only  # heartbeat detail.clock_only (futrisk up --clock-only)
        self.timeline = timeline if timeline is not None else []
        self.calls: list[tuple[str, str, Any]] = []
        self.ctx: zmq.Context[zmq.Socket[bytes]] = zmq.Context()
        self.server = RpcServer(
            self.ctx,
            str(cfg.services.instance("supervisor").router),
            instance="supervisor",
            epoch="fake",
            whitelist=Whitelist(internal=cfg.services.instances),
        )
        for command, handler in {
            "ctl.ping": self._ping,
            "ctl.status": self._status,
            "ctl.snapshot": self._snapshot,
            "svc.ps": self._ps,
            "svc.kill": self._kill,
            "svc.stop": self._stop,
            "svc.start": self._start,
        }.items():
            self.server.add_handler(command, handler)
        self._closing_at: float | None = None
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while self._running:
            if self.server.socket.poll(20):
                self.server.pump()
            if self._closing_at is not None and time.monotonic() >= self._closing_at:
                break
        self.server.close()

    def close(self) -> None:
        self._running = False
        self._thread.join(5)
        self.ctx.destroy(0)

    def _record(self, command: str, payload: Any, ctx: Any) -> None:
        self.calls.append((command, ctx.requester, payload))
        self.timeline.append(command)

    def pid(self, instance: str) -> int:
        return 1000 + list(self.states).index(instance)

    def _hb_state(self, instance: str, state: str) -> ServiceState | None:
        if instance in self.heartbeat:
            hb = self.heartbeat[instance]
            return ServiceState(hb) if hb else None
        return ServiceState.RUNNING if state == "RUNNING" and instance not in ("sim-exchange", "strategy-sim") else None

    def _infos(self) -> list[ServiceInfo]:
        now = time.time_ns() + self.offset
        return [
            ServiceInfo(
                instance=i,
                service=i.rsplit("-", 1)[0] if i.startswith("risk-engine") else i,
                state=ProcessState(s),
                pid=self.pid(i),
                started_ns=now - 65 * 10**9,
                restarts=1 if i == "md-gateway" else 0,
                heartbeat_state=self._hb_state(i, s),
                last_heartbeat_ns=now - 10**9,
                env_keys=sorted(self.env.get(i, {})) or None,
                last_error="port 7101 in use by pid 4 (System)" if s == "FAILED" else None,
            )
            for i, s in self.states.items()
        ]

    def _ping(self, command: str, payload: Any, ctx: Any) -> Any:
        self._record(command, payload, ctx)
        return ok_reply(
            Pong(instance="supervisor", state="RUNNING", ts_ns=time.time_ns() + self.offset, home_id=self.home_id, pid=4_194_300)
        )

    def _status(self, command: str, payload: Any, ctx: Any) -> Any:
        self._record(command, payload, ctx)
        return ok_reply(
            Heartbeat(
                instance="supervisor",
                service="supervisor",
                state=ServiceState.RUNNING,
                pid=4_194_300,  # not a live process: `down` does not wait for it
                epoch="fake",
                app_version="test",
                started_ns=time.time_ns() - 120 * 10**9,
                clock_version=3,
                detail={"clock_only": True} if self.clock_only else {},
            )
        )

    def _snapshot(self, command: str, payload: Any, ctx: Any) -> Any:
        self._record(command, payload, ctx)
        state = ClockState(mode=ClockMode.DEMO, offset_ns=self.offset, version=3, market_now_ns=time.time_ns() + self.offset)
        env = make_envelope("sys.clock", state, src="supervisor", epoch="fake", ts_ns=time.time_ns(), seq=7)
        item = SnapshotItem(topic="sys.clock", seq=7, epoch="fake", envelope=encode_envelope(env))
        return ok_reply(SnapshotBatch(items=[item], as_of_ns=time.time_ns()))

    def _ps(self, command: str, payload: Any, ctx: Any) -> Any:
        self._record(command, payload, ctx)
        if self.ps_silent:
            return DEFERRED  # never answered: the client times out
        return ok_reply(ServiceList(items=self._infos(), home_id=self.home_id))

    def _kill(self, command: str, payload: Any, ctx: Any) -> Any:
        self._record(command, payload, ctx)
        assert isinstance(payload, SvcRequest)
        if payload.instance not in self.states:
            return err_reply(codes.NOT_FOUND, f"unknown instance {payload.instance}")
        info = next(i for i in self._infos() if i.instance == payload.instance)
        import msgspec

        return ok_reply(msgspec.structs.replace(info, state=ProcessState.RESTARTING, last_exit_code=1, restarts=2))

    def _start(self, command: str, payload: Any, ctx: Any) -> Any:
        self._record(command, payload, ctx)
        assert isinstance(payload, SvcRequest)
        if payload.instance not in self.states:
            return err_reply(codes.NOT_FOUND, f"unknown instance {payload.instance}")
        info = next(i for i in self._infos() if i.instance == payload.instance)
        before = self.env.get(payload.instance, {})
        wanted = {**before, **(payload.env or {})}
        wanted = {k: v for k, v in wanted.items() if v is not None}  # null unsets
        changed = wanted != before
        self.env[payload.instance] = wanted
        import msgspec

        return ok_reply(msgspec.structs.replace(info, state=ProcessState.STOPPING if changed else info.state))

    def _stop(self, command: str, payload: Any, ctx: Any) -> Any:
        self._record(command, payload, ctx)
        self._closing_at = time.monotonic() + 0.3  # the supervisor exits after answering
        return ok_reply(ServiceList(items=self._infos(), home_id=self.home_id))


def acc3_snapshot(ratio: float) -> dict[str, Any]:
    """``GET /accounts/ACC3`` data: long rb/hc/i worth ``ratio`` x equity, plus positions that must not count."""
    equity = 9_800_000.0
    book = ratio * equity

    def pos(product: str, direction: str, notional: float) -> dict[str, Any]:
        return {"account_id": "ACC3", "instrument_id": f"{product}2701", "product": product, "exchange": "SHFE",
                "direction": direction, "td_qty": 1, "yd_qty": 0, "notional": notional, "pct_equity": notional / equity}

    return {
        "account_id": "ACC3",
        "name": "日内逆势加仓",
        "funds": {"equity": equity, "initial_capital": 10_000_000.0},
        "positions": [
            pos("rb", "LONG", 0.5 * book),
            pos("hc", "LONG", 0.3 * book),
            pos("i", "LONG", 0.2 * book),
            pos("rb", "SHORT", 3_000_000.0),  # a short leg
            pos("cu", "LONG", 5_000_000.0),  # another sector
        ],
    }


class FakeApi:
    """api-gateway REST endpoints used by ``futrisk demo`` / ``status``."""

    def __init__(
        self,
        *,
        seed_status: int = 200,
        settlement_exists: bool = False,
        books: list[float | None] | None = None,
        health: dict[str, Any] | None = None,
        snapshots: list[str] | None = None,
        accounts_status: int = 200,
        settled_days: list[str] | None = None,
        timeline: list[str] | None = None,
    ) -> None:
        self.requests: list[tuple[str, str, dict[str, str], Any]] = []
        self.seed_status = seed_status
        self.settlement_exists = settlement_exists
        self.books = list(books if books is not None else [0.78])  # ACC3 ratios served in turn (None: 404)
        self.health: dict[str, Any] = dict(health or {})  # /health data: home_id, pid, epoch (set by the test)
        self.snapshots = list(snapshots or [])  # accounts that already have an AccountSnapshot
        self.accounts_status = accounts_status
        self.settled_days = list(settled_days or [])
        self.timeline = timeline if timeline is not None else []
        api = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def _answer(self, status: int, doc: dict[str, Any]) -> None:
                body = json.dumps(doc).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _record(self, body: Any) -> None:
                api.requests.append((self.command, self.path, {k.lower(): v for k, v in self.headers.items()}, body))
                api.timeline.append(f"{self.command} {self.path}")

            def do_GET(self) -> None:  # noqa: N802 - http.server API
                self._record(None)
                if self.path == "/api/v1/health":
                    self._answer(200, {"ok": True, "code": "OK", "data": {"status": "ok", "instance": "api-gateway", **api.health}})
                elif self.path == "/api/v1/accounts":
                    if api.accounts_status != 200:
                        self._answer(api.accounts_status, {"ok": False, "code": "UNAVAILABLE", "message": "risk shard down"})
                        return
                    items = [{"account_id": a, "missing": a not in api.snapshots} for a in ("ACC1", "ACC2", "ACC3", "ACC4", "ACC5")]
                    self._answer(200, {"ok": True, "code": "OK", "data": items})
                elif self.path == "/api/v1/settlement/days":
                    days = [{"trading_day": d, "version": 1, "phase": "DONE"} for d in api.settled_days]
                    self._answer(200, {"ok": True, "code": "OK", "data": {"items": days, "errors": {}}})
                elif self.path == "/api/v1/accounts/ACC3":
                    ratio = api.books.pop(0) if len(api.books) > 1 else api.books[0]
                    if ratio is None:
                        self._answer(404, {"ok": False, "code": "NOT_FOUND", "message": "no snapshot for ACC3 yet"})
                    else:
                        self._answer(200, {"ok": True, "code": "OK", "data": acc3_snapshot(ratio)})
                elif self.path.startswith("/api/v1/settlement/") and api.settlement_exists:
                    self._answer(200, {"ok": True, "code": "OK", "data": {"accounts": [{"account_id": "ACC1"}]}})
                else:
                    self._answer(404, {"ok": False, "code": "NOT_FOUND", "message": "no settlement"})

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"null")
                self._record(body)
                if self.path == "/api/v1/settlement/seed-sod":
                    ok = api.seed_status == 200
                    self._answer(api.seed_status, {"ok": ok, "code": "OK" if ok else "FORBIDDEN", "message": "" if ok else "role", "data": {"version": 0} if ok else None})
                elif self.path == "/api/v1/demo/scenario":
                    self._answer(200, {"ok": True, "code": "OK", "data": {"run_id": "run-1", "name": body["name"]}})
                else:
                    self._answer(404, {"ok": False, "code": "NOT_FOUND"})

            def log_message(self, *args: object) -> None:
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def calls(self, method: str, path: str) -> list[tuple[str, str, dict[str, str], Any]]:
        return [r for r in self.requests if r[0] == method and r[1] == path]


# ---------------------------------------------------------------------------- fixtures
def make_home(root: Path, cfg: FullConfig, *, api_url: str | None = None, mode: str | None = None) -> Path:
    """A home whose committed config uses the test's bus ports (and the fake api URL)."""
    layout = HomeLayout(root)
    ensure_home_config(layout)
    services = cfg.services
    if api_url is not None:
        api = services.instances["api-gateway"].model_copy(update={"http": api_url})
        services = services.model_copy(update={"instances": {**services.instances, "api-gateway": api}})
    (layout.config_dir / "services.yaml").write_text(yaml.safe_dump(services.model_dump(mode="json"), sort_keys=False), encoding="utf-8")
    if mode is not None:
        app_yaml = layout.config_dir / "app.yaml"
        doc = yaml.safe_load(app_yaml.read_text(encoding="utf-8"))
        doc["mode"] = mode
        app_yaml.write_text(yaml.safe_dump(doc, sort_keys=False, allow_unicode=True), encoding="utf-8")
    n, path = snapshot_working_config(layout)
    write_active(layout, n, path)
    return layout.home


@pytest.fixture
def cli_app(monkeypatch: pytest.MonkeyPatch) -> typer.Typer:
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.delenv(ops_cmd.API_KEY_ENV, raising=False)
    app = typer.Typer()
    ops_cmd.register(app)
    return app


@pytest.fixture
def cfg(default_config: FullConfig) -> FullConfig:
    return config_with_free_ports(default_config, heartbeat_ms=100)


@pytest.fixture
def api() -> Iterator[FakeApi]:
    fake = FakeApi()
    yield fake
    fake.close()


def own_supervisor(cfg: FullConfig, home: Path | str, **kw: Any) -> FakeSupervisor:
    """A fake supervisor of ``home`` (reports its home id)."""
    return FakeSupervisor(cfg, home_id=HomeLayout(Path(home)).home_id(), **kw)


def serve_health(api: FakeApi, home: Path | str, sup: FakeSupervisor | None = None) -> None:
    """``/health`` of this home's api-gateway (same pid as svc.ps reports)."""
    api.health = {"home_id": HomeLayout(Path(home)).home_id(), "pid": sup.pid("api-gateway") if sup else 1006, "epoch": "e"}


def keyed(api: FakeApi) -> list[str]:
    """Requests that carried the API key."""
    return [f"{m} {p}" for m, p, h, _ in api.requests if "x-api-key" in h]


def invoke(app: typer.Typer, *args: str) -> Any:
    return CliRunner().invoke(app, list(args), catch_exceptions=False)


# ---------------------------------------------------------------------------- tests
def test_ps_kill_status_and_up_refusal(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer, api: FakeApi) -> None:
    home = str(make_home(tmp_path / "home", cfg, api_url=api.url))
    sup = own_supervisor(cfg, home)
    serve_health(api, home, sup)
    try:
        r = invoke(cli_app, "ps", "--home", home)
        assert r.exit_code == 0, r.output
        for inst in INSTANCES:
            assert inst in r.output
        assert "RUNNING" in r.output and "1:0" in r.output and "clock v3" in r.output
        assert re.search(r"RUNNING \d+\.\ds", r.output)  # heartbeat state and age

        r = invoke(cli_app, "ps", "--home", home, "--json")
        assert [i["instance"] for i in json.loads(r.output)["items"]] == INSTANCES

        r = invoke(cli_app, "kill", "md-gateway", "--home", home)
        assert r.exit_code == 0 and "RESTARTING" in r.output
        command, requester, payload = [c for c in sup.calls if c[0] == "svc.kill"][0]
        assert requester == "cli" and payload.instance == "md-gateway" and payload.actor.startswith("cli")
        uuid.UUID(payload.command_id)
        r = invoke(cli_app, "kill", "nope", "--home", home)
        assert r.exit_code == 1 and "NOT_FOUND" in r.output

        r = invoke(cli_app, "status", "--home", home)
        assert r.exit_code == 0, r.output
        assert "8/8 running" in r.output and "2026-09-28 08:5" in r.output and "DEMO" in r.output and "ok" in r.output

        r = invoke(cli_app, "up", "--home", home)
        assert r.exit_code == 1 and "already running" in r.output
    finally:
        sup.close()


def test_status_unhealthy_and_down(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer, api: FakeApi) -> None:
    home = str(make_home(tmp_path / "home", cfg, api_url=api.url))
    sup = own_supervisor(cfg, home, states={"risk-engine-1": "RESTARTING"})
    serve_health(api, home, sup)
    try:
        r = invoke(cli_app, "status", "--home", home)
        assert r.exit_code == 1 and "7/8 running" in r.output and "risk-engine-1=RESTARTING" in r.output

        r = invoke(cli_app, "down", "--home", home, "--timeout", "20")
        assert r.exit_code == 0, r.output
        assert "all services stopped" in r.output
        stop = [c for c in sup.calls if c[0] == "svc.stop"]
        assert len(stop) == 1 and stop[0][1] == "cli" and stop[0][2].instance == "*"
    finally:
        sup.close()
    r = invoke(cli_app, "down", "--home", home)  # nothing running any more
    assert r.exit_code == 0 and "not running" in r.output
    r = invoke(cli_app, "ps", "--home", home)
    assert r.exit_code == 1 and "not running" in r.output


def test_logs_pretty_raw_and_errors(tmp_path: Path, cli_app: typer.Typer) -> None:
    logs = tmp_path / "home" / "logs"
    logs.mkdir(parents=True)
    old = logs / "md-gateway_20260924.log"
    old.write_text('{"event": "old.day"}\n', encoding="utf-8")
    lines = [
        {"ts": "2026-09-25T10:00:00.000001+08:00", "level": "INFO", "instance": "md-gateway", "event": "first"},
        {"ts": "2026-09-25T10:00:01.123456+08:00", "level": "WARNING", "instance": "md-gateway", "event": "feed.gap",
         "detail": {"source": "feed_a", "missing": [5, 6]}, "trading_day": "20260928"},
        {"ts": "2026-09-25T10:00:02.500000+08:00", "level": "ERROR", "instance": "md-gateway", "event": "boom",
         "corr_id": "c-1", "exception": "ValueError: bad"},
    ]
    new = logs / "md-gateway_20260925.log"
    new.write_text("\n".join(json.dumps(x) for x in lines) + "\nnot json at all\n", encoding="utf-8")
    old_time = time.time() - 3600
    import os

    os.utime(old, (old_time, old_time))
    (logs / "md-gateway.console.log").write_text("captured stdout line\n", encoding="utf-8")
    home = str(tmp_path / "home")

    r = invoke(cli_app, "logs", "md-gateway", "--home", home, "-n", "3")
    assert r.exit_code == 0, r.output
    out = r.output.splitlines()
    assert "first" not in r.output and "old.day" not in r.output
    assert out[0].startswith("10:00:01.123 WARNING") and "feed.gap" in out[0] and 'missing=[5,6]' in out[0] and "trading_day=20260928" in out[0]
    assert "boom" in r.output and "corr_id=c-1" in r.output and "ValueError: bad" in r.output
    assert out[-1] == "not json at all"

    r = invoke(cli_app, "logs", "md-gateway", "--home", home, "-n", "1", "--raw")
    assert r.output.strip() == "not json at all"
    r = invoke(cli_app, "logs", "md-gateway", "--home", home, "--console")
    assert "captured stdout line" in r.output
    r = invoke(cli_app, "logs", "risk-engine-0", "--home", home)
    assert r.exit_code == 1 and "no log file" in r.output
    r = CliRunner().invoke(cli_app, ["logs", "../etc", "--home", home])
    assert r.exit_code == 2


def test_demo_headless_seeds_once_and_starts_the_scenario(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer) -> None:
    timeline: list[str] = []
    api = FakeApi(timeline=timeline)
    root = make_home(tmp_path / "home", cfg, api_url=api.url)
    layout = HomeLayout(root)
    layout.secrets_dir.mkdir(parents=True, exist_ok=True)
    (layout.secrets_dir / "bootstrap_admin.key").write_text("frk_test_key\n", encoding="utf-8")
    sup = own_supervisor(cfg, root, timeline=timeline)
    serve_health(api, root, sup)
    try:
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--scenario", "breaker", "--timeout", "20")
        assert r.exit_code == 0, r.output
        assert "already running" in r.output and "seeded the SOD of 20260928" in r.output and "breaker" in r.output

        # the books are checked first: account snapshots, settled days, today's / the previous day's settlement
        checks = [q[1] for q in api.requests if q[0] == "GET" and q[1] != "/api/v1/health" and "ACC3" not in q[1]]
        assert checks == [
            "/api/v1/accounts", "/api/v1/settlement/days", "/api/v1/settlement/20260928", "/api/v1/settlement/20260924",
        ]
        [(_, _, headers, body)] = api.calls("POST", "/api/v1/settlement/seed-sod")
        assert headers["x-api-key"] == "frk_test_key"
        assert headers["idempotency-key"] == ops_cmd.seed_idempotency_key(layout, "20260928")
        uuid.UUID(headers["idempotency-key"])
        assert body["trading_day"] == "20260928" and body["reason"]
        accounts = {a["account_id"]: a for a in body["accounts"]}
        assert sorted(accounts) == ["ACC1", "ACC2", "ACC3", "ACC4", "ACC5"]
        assert accounts["ACC2"] == {
            "account_id": "ACC2", "pre_balance": "20000000", "initial_capital": "20000000",
            "cum_net_deposit": "0", "hwm_equity": "20000000", "lots": [],
        }
        assert body["pre_settle"]["rb2701"] == "3120" and body["pre_settle"]["IF2612"] == "4414.8"
        [(_, _, sh, sbody)] = api.calls("POST", "/api/v1/demo/scenario")
        assert sbody == {"name": "breaker"} and sh["x-api-key"] == "frk_test_key" and sh["idempotency-key"]
        marker = json.loads((layout.db_dir / ops_cmd.SEED_MARKER).read_text(encoding="utf-8"))
        assert marker["trading_day"] == "20260928" and marker["source"] == "seed-sod"
        # strategy-sim is switched to scenario mode (svc.start + env, identity cli) just before the scenario:
        # after the seed, before ACC3's book is awaited
        starts = [(c[1], c[2].instance, c[2].env) for c in sup.calls if c[0] == "svc.start"]
        assert starts == [("cli", "strategy-sim", {"FUTRISK_BOTS_MODE": "scenario"})]
        assert "strategy-sim restarting with FUTRISK_BOTS_MODE=scenario" in r.output
        seed, switch = timeline.index("POST /api/v1/settlement/seed-sod"), timeline.index("svc.start")
        book, scenario = timeline.index("GET /api/v1/accounts/ACC3"), timeline.index("POST /api/v1/demo/scenario")
        assert seed < switch < book < scenario
        assert "ACC3's rb/hc/i long book at 0.78x equity" in r.output

        # second run without a scenario: the books exist, no second seed, the bots are left alone,
        # but the operator is told how to switch them back
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "20")
        assert r.exit_code == 0, r.output
        assert len(api.calls("POST", "/api/v1/settlement/seed-sod")) == 1 and "already seeded" in r.output
        assert len([c for c in sup.calls if c[0] == "svc.start"]) == 1
        assert "futrisk demo --bots normal" in r.output

        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "20", "--bots", "normal")
        assert r.exit_code == 0, r.output
        assert [c[2].env for c in sup.calls if c[0] == "svc.start"][-1] == {"FUTRISK_BOTS_MODE": None}
        assert sup.env["strategy-sim"] == {} and "FUTRISK_BOTS_MODE unset" in r.output
        assert CliRunner().invoke(cli_app, ["demo", "--headless", "--home", str(root), "--bots", "wild"]).exit_code == 2
    finally:
        sup.close()
        api.close()


def test_demo_headless_fails_on_step_errors(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer) -> None:
    api = FakeApi(seed_status=403)
    root = make_home(tmp_path / "home", cfg, api_url=api.url)
    layout = HomeLayout(root)
    sup = own_supervisor(cfg, root)
    serve_health(api, root, sup)
    try:
        # no bootstrap key file: the key step fails quickly
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "1")
        assert r.exit_code == 1 and "bootstrap_admin.key" in r.output
        layout.secrets_dir.mkdir(parents=True, exist_ok=True)
        (layout.secrets_dir / "bootstrap_admin.key").write_text("k", encoding="utf-8")
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "5")
        assert r.exit_code == 1 and "seed-sod failed: HTTP 403 FORBIDDEN" in r.output
        assert not (layout.db_dir / ops_cmd.SEED_MARKER).exists()
    finally:
        sup.close()
        api.close()


def test_demo_skips_seeding_when_settlements_exist(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer) -> None:
    api = FakeApi(settlement_exists=True)
    root = make_home(tmp_path / "home", cfg, api_url=api.url)
    layout = HomeLayout(root)
    layout.secrets_dir.mkdir(parents=True, exist_ok=True)
    (layout.secrets_dir / "bootstrap_admin.key").write_text("k", encoding="utf-8")
    sup = own_supervisor(cfg, root)
    serve_health(api, root, sup)
    try:
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "10")
        assert r.exit_code == 0 and "books already exist" in r.output
        assert api.calls("POST", "/api/v1/settlement/seed-sod") == []
    finally:
        sup.close()
        api.close()


def test_up_argv_for_the_demo_child(tmp_path: Path) -> None:
    layout = HomeLayout(tmp_path)
    env = {"strategy-sim": {"FUTRISK_BOTS_MODE": "scenario"}}
    argv = ops_cmd.up_argv(layout, start_at="2026-09-28T09:30:00", parent_pid=42, svc_env=env)
    assert argv[1:] == [
        "-m", "futrisk.main", "up", "--home", str(tmp_path), "--headless", "--start-at", "2026-09-28T09:30:00",
        "--parent-pid", "42", "--svc-env", "strategy-sim:FUTRISK_BOTS_MODE=scenario",
    ]
    assert ops_cmd.up_argv(layout, start_at=None, parent_pid=None)[-1] == "--headless"  # plain demo: normal bots


def test_svc_env_parsing_and_up_passes_it_to_the_supervisor(
    tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer, monkeypatch: pytest.MonkeyPatch
) -> None:
    env = {"strategy-sim": {"FUTRISK_BOTS_MODE": "scenario", "FUTRISK_BOTS_SEED": "7"}, "md-gateway": {"X": "a=b"}}
    items = ["strategy-sim:FUTRISK_BOTS_MODE=scenario", "strategy-sim:FUTRISK_BOTS_SEED=7", "md-gateway:X=a=b"]
    assert ops_cmd.parse_svc_env(items) == env and ops_cmd.parse_svc_env(None) == {}
    assert ops_cmd.parse_svc_env(ops_cmd.svc_env_args(env)[1::2]) == env
    for bad in ("strategy-sim", "strategy-sim:=x", ":A=b", "strategy-sim:1A=b"):
        with pytest.raises(typer.BadParameter):
            ops_cmd.parse_svc_env([bad])

    captured: dict[str, Any] = {}

    def fake_run(**kw: Any) -> int:
        captured.update(kw)
        return 0

    monkeypatch.setattr("futrisk.services.supervisor.main.run", fake_run)
    home = str(make_home(tmp_path / "home", cfg))
    r = invoke(cli_app, "up", "--home", home, "--headless", "--svc-env", "strategy-sim:FUTRISK_BOTS_MODE=scenario")
    assert r.exit_code == 0, r.output
    assert captured["instance_env"] == {"strategy-sim": {"FUTRISK_BOTS_MODE": "scenario"}}
    r = invoke(cli_app, "up", "--home", home, "--headless")
    assert r.exit_code == 0 and captured["instance_env"] is None
    assert CliRunner().invoke(cli_app, ["up", "--home", home, "--svc-env", "nonsense"]).exit_code == 2
    # the supervisor's per-instance allow-list applies to --svc-env as well
    for bad, message in (
        ("md-gateway:FUTRISK_BOTS_MODE=scenario", "md-gateway accepts no environment overrides"),
        ("strategy-sim:PYTHONPATH=x", "accepts only FUTRISK_BOTS_ACCOUNTS, FUTRISK_BOTS_MODE, FUTRISK_BOTS_SEED"),
    ):
        captured.clear()
        r = CliRunner().invoke(cli_app, ["up", "--home", home, "--svc-env", bad])
        assert r.exit_code == 2 and message in " ".join(r.output.split()) and captured == {}


def test_supervisor_run_passes_the_instance_env_on(monkeypatch: pytest.MonkeyPatch) -> None:
    from futrisk.services.supervisor import main

    got: dict[str, Any] = {}

    async def fake_supervise(**kw: Any) -> int:
        got.update(kw)
        return 0

    monkeypatch.setattr(main, "supervise", fake_supervise)
    env = {"strategy-sim": {"FUTRISK_BOTS_MODE": "scenario"}}
    assert main.run(home="H", parent_pid=5, instance_env=env) == 0
    assert got["instance_env"] == env and got["parent_pid"] == 5 and got["home"] == "H"


def test_book_ratio_counts_long_notional_of_the_products() -> None:
    snap = acc3_snapshot(0.78)
    black = ops_cmd.SCENARIO_BOOKS["breaker"].products
    assert ops_cmd.book_ratio(snap, black) == pytest.approx(0.78)  # the short rb leg and cu do not count
    assert ops_cmd.book_ratio(snap, ("cu",)) == pytest.approx(5_000_000 / 9_800_000)
    no_funds = {k: v for k, v in snap.items() if k != "funds"}
    assert ops_cmd.book_ratio(no_funds, black) == pytest.approx(0.78)  # pct_equity fallback
    assert ops_cmd.book_ratio({"account_id": "ACC3", "funds": {"equity": 1e7}, "positions": []}, black) == 0.0
    assert ops_cmd.book_ratio(None, black) is None and ops_cmd.book_ratio({"account_id": "ACC3"}, black) is None


def test_bots_mode_and_book_mapping_match_the_config(default_config: FullConfig) -> None:
    from futrisk.sim.bots.config import ENV_MODE, env_aggressive

    assert ops_cmd.BOTS_MODE_ENV == ENV_MODE
    assert env_aggressive({ENV_MODE: ops_cmd.SCENARIO_BOTS_MODE}) is True and env_aggressive({}) is None
    req = ops_cmd.SCENARIO_BOOKS["breaker"]
    assert set(req.products) <= set(default_config.accounts.accounts[req.account_id].products)
    assert {default_config.products.products[p].sector for p in req.products} == {"black"}
    assert set(ops_cmd.SCENARIO_BOOKS) <= {p.stem for p in (default_config.root / "scenarios").glob("*.yaml")}


def _demo_home(tmp_path: Path, cfg: FullConfig, api: FakeApi) -> Path:
    root = make_home(tmp_path / "home", cfg, api_url=api.url)
    layout = HomeLayout(root)
    layout.secrets_dir.mkdir(parents=True, exist_ok=True)
    (layout.secrets_dir / "bootstrap_admin.key").write_text("k", encoding="utf-8")
    return root


def test_demo_breaker_waits_for_acc3s_book(
    tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ops_cmd, "BOOK_POLL_S", 0.05)
    monkeypatch.setattr(ops_cmd, "BOOK_NOTE_S", 0.0)
    api = FakeApi(books=[None, 0.30, 0.55, 0.78])  # no snapshot yet, then the book builds
    root = _demo_home(tmp_path, cfg, api)
    sup = own_supervisor(cfg, root)
    serve_health(api, root, sup)
    try:
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--scenario", "breaker", "--timeout", "20")
        assert r.exit_code == 0, r.output
        gets = [i for i, q in enumerate(api.requests) if q[1] == "/api/v1/accounts/ACC3"]
        [post] = [i for i, q in enumerate(api.requests) if q[1] == "/api/v1/demo/scenario"]
        assert len(gets) == 4 and gets[-1] < post
        assert all(api.requests[i][2]["x-api-key"] == "k" for i in gets)
        assert "waiting for ACC3's rb/hc/i long book" in r.output and "0.30x equity" in r.output
        assert "ACC3's rb/hc/i long book at 0.78x equity" in r.output and "scenario breaker started" in r.output
    finally:
        sup.close()
        api.close()


def test_demo_breaker_is_not_started_without_the_book(
    tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ops_cmd, "BOOK_POLL_S", 0.05)
    api = FakeApi(books=[0.30])  # a partial book that never grows
    root = _demo_home(tmp_path, cfg, api)
    sup = own_supervisor(cfg, root)
    serve_health(api, root, sup)
    try:
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--scenario", "breaker", "--book-timeout", "0.5")
        assert r.exit_code == 1
        assert "reached 0.30x equity" in r.output and "need 0.70x" in r.output and "breaker not started" in r.output
        assert api.calls("POST", "/api/v1/demo/scenario") == []
        # the bots were switched for the scenario only: switched back when it did not start
        envs = [c[2].env for c in sup.calls if c[0] == "svc.start"]
        assert envs == [{"FUTRISK_BOTS_MODE": "scenario"}, {"FUTRISK_BOTS_MODE": None}]
        assert sup.env["strategy-sim"] == {} and "strategy-sim switched back" in r.output
    finally:
        sup.close()
        api.close()


def test_demo_keeps_bots_that_were_already_in_scenario_mode(
    tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ops_cmd, "BOOK_POLL_S", 0.05)
    api = FakeApi(books=[0.30])
    root = _demo_home(tmp_path, cfg, api)
    sup = own_supervisor(cfg, root, env={"strategy-sim": {"FUTRISK_BOTS_MODE": "scenario"}})
    serve_health(api, root, sup)
    try:
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--scenario", "breaker", "--book-timeout", "0.3")
        assert r.exit_code == 1 and "breaker not started" in r.output
        # nothing changed by this run (the reply is RUNNING): nothing to switch back
        assert [c[2].env for c in sup.calls if c[0] == "svc.start"] == [{"FUTRISK_BOTS_MODE": "scenario"}]
        assert sup.env["strategy-sim"] == {"FUTRISK_BOTS_MODE": "scenario"}
    finally:
        sup.close()
        api.close()


def test_demo_ignores_a_foreign_api_gateway(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer) -> None:
    """Port clash: the dashboard port answers, but this supervisor's own api-gateway is not running."""
    api = FakeApi()
    root = _demo_home(tmp_path, cfg, api)
    sup = own_supervisor(cfg, root, states={"api-gateway": "RESTARTING"})
    api.health = {"status": "ok"}  # an older / foreign api-gateway: no home_id, no pid
    try:
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "1.5")
        assert r.exit_code == 1
        assert "this supervisor's api-gateway is RESTARTING" in r.output
        assert keyed(api) == []  # the key never left this machine
    finally:
        sup.close()
        api.close()


def test_demo_refuses_another_homes_api_gateway_before_reading_the_key(
    tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer
) -> None:
    api = FakeApi(health={"home_id": "0" * 32, "pid": 1006, "epoch": "x"})
    root = _demo_home(tmp_path, cfg, api)
    sup = own_supervisor(cfg, root)
    try:
        t0 = time.monotonic()
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "20")
        assert r.exit_code == 1 and time.monotonic() - t0 < 10  # refused at once, no waiting
        assert f"is served by another home's api-gateway ({'0' * 32}); use that home or change ports" in r.output
        assert keyed(api) == [] and "api key" not in r.output
    finally:
        sup.close()
        api.close()


@pytest.mark.parametrize(
    ("heartbeat", "health", "expected"),
    [
        # our api-gateway heartbeats but its bind failed; another process answers /health
        ("STARTING", {"status": "ok"}, "this supervisor's api-gateway is STARTING"),
        # RUNNING here, but /health answers from another pid and without a home id
        ("RUNNING", {"status": "ok", "pid": 999}, "answers from pid 999 without a home id"),
    ],
)
def test_demo_guard_needs_a_verified_running_api_gateway(
    tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer, heartbeat: str, health: dict[str, Any], expected: str
) -> None:
    api = FakeApi(health=health)
    root = _demo_home(tmp_path, cfg, api)
    sup = own_supervisor(cfg, root, heartbeat={"api-gateway": heartbeat})
    try:
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "1.5")
        assert r.exit_code == 1 and expected in r.output, r.output
        assert keyed(api) == []
    finally:
        sup.close()
        api.close()


def test_demo_guard_accepts_an_older_gateway_by_pid_on_two_polls(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer) -> None:
    api = FakeApi()
    root = _demo_home(tmp_path, cfg, api)
    sup = own_supervisor(cfg, root)
    api.health = {"status": "ok", "pid": sup.pid("api-gateway")}  # no home_id yet: pid equality with svc.ps
    try:
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "20")
        assert r.exit_code == 0, r.output
        assert f"identity by pid {sup.pid('api-gateway')}" in r.output
        assert len([c for c in sup.calls if c[0] == "svc.ps"]) >= 3  # two polls with the same pid (+ identity)
    finally:
        sup.close()
        api.close()


def test_demo_does_not_trust_an_svc_ps_timeout(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer) -> None:
    api = FakeApi()
    root = _demo_home(tmp_path, cfg, api)
    sup = own_supervisor(cfg, root, ps_silent=True)
    serve_health(api, root, sup)
    try:
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "2")
        assert r.exit_code == 1 and "the supervisor did not answer svc.ps" in r.output
        assert keyed(api) == []
    finally:
        sup.close()
        api.close()


@pytest.mark.parametrize(
    ("fake", "expected"),
    [
        ({"snapshots": ["ACC2"]}, "books already exist: account snapshots exist (ACC2)"),
        ({"settled_days": ["20260925"]}, "books already exist: settlements exist (20260925)"),
    ],
)
def test_demo_never_seeds_books_that_exist(
    tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer, fake: dict[str, Any], expected: str
) -> None:
    """A-LOW-6: without the marker file, account snapshots or settled days still mean 'books exist'."""
    api = FakeApi(**fake)
    root = _demo_home(tmp_path, cfg, api)
    sup = own_supervisor(cfg, root)
    serve_health(api, root, sup)
    try:
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "20")
        assert r.exit_code == 0 and expected in r.output, r.output
        assert api.calls("POST", "/api/v1/settlement/seed-sod") == []
        assert (HomeLayout(root).db_dir / ops_cmd.SEED_MARKER).exists()
    finally:
        sup.close()
        api.close()


def test_demo_does_not_seed_when_the_books_cannot_be_checked(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer) -> None:
    api = FakeApi(accounts_status=500)
    root = _demo_home(tmp_path, cfg, api)
    sup = own_supervisor(cfg, root)
    serve_health(api, root, sup)
    try:
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "3")
        assert r.exit_code == 1 and "cannot verify that this home has no books yet (GET /accounts: HTTP 500)" in r.output
        assert api.calls("POST", "/api/v1/settlement/seed-sod") == []
        assert not (HomeLayout(root).db_dir / ops_cmd.SEED_MARKER).exists()
    finally:
        sup.close()
        api.close()


def test_prod_mode_demo_scenario_changes_nothing(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer) -> None:
    """A-LOW-1: the mode is checked first - no bots switch, no key, no REST call, no supervisor started."""
    api = FakeApi()
    root = make_home(tmp_path / "home", cfg, api_url=api.url, mode="prod")
    sup = own_supervisor(cfg, root)
    serve_health(api, root, sup)
    try:
        for args in (["--scenario", "breaker"], ["--bots", "scenario"]):
            r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--timeout", "5", *args)
            assert r.exit_code == 1 and "only available in demo mode (app.mode=prod)" in r.output, r.output
        assert [c for c in sup.calls if c[0].startswith("svc.")] == [] and api.requests == []
    finally:
        sup.close()
        api.close()


def test_cli_refuses_another_homes_supervisor(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer, api: FakeApi) -> None:
    """A-HIGH-1: a supervisor that owns the configured ports but belongs to another home (or says
    nothing about its home) is never stopped, killed, reported as ours or used by the demo."""
    root = _demo_home(tmp_path, cfg, api)
    home = str(root)
    serve_health(api, root)
    other = "f" * 32
    for home_id, shown in ((other, other), (None, "it reports no home id")):
        sup = FakeSupervisor(cfg, home_id=home_id)
        try:
            message = f"the supervisor on these ports belongs to another home ({shown}); use that home or change ports"
            for args in (["ps"], ["ps", "--json"], ["status"], ["kill", "md-gateway"], ["down", "--timeout", "5"],
                         ["demo", "--headless", "--timeout", "5"], ["up", "--headless"]):
                r = invoke(cli_app, *args, "--home", home)
                assert r.exit_code == 1, (args, r.output)
                assert message in " ".join(r.output.split()), (args, r.output)
            assert [c[0] for c in sup.calls if c[0].startswith("svc.") and c[0] != "svc.ps"] == []
            assert keyed(api) == []
        finally:
            sup.close()


def test_ps_shows_env_overrides_and_failures(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer, api: FakeApi) -> None:
    home = str(make_home(tmp_path / "home", cfg, api_url=api.url))
    sup = own_supervisor(cfg, home, states={"md-gateway": "FAILED"}, env={"strategy-sim": {"FUTRISK_BOTS_MODE": "scenario"}})
    try:
        r = invoke(cli_app, "ps", "--home", home)
        assert r.exit_code == 0, r.output
        assert "DETAIL" in r.output and "env FUTRISK_BOTS_MODE" in r.output and "port 7101 in use by pid 4 (System)" in r.output
        assert "scenario" not in r.output  # names only
        doc = json.loads(invoke(cli_app, "ps", "--home", home, "--json").output)
        assert doc["home_id"] == HomeLayout(Path(home)).home_id()
    finally:
        sup.close()


def test_other_scenarios_start_at_once(tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer) -> None:
    api = FakeApi(books=[0.0])
    root = _demo_home(tmp_path, cfg, api)
    sup = own_supervisor(cfg, root, instances=[i for i in INSTANCES if i != "strategy-sim"])  # bots not managed here
    serve_health(api, root, sup)
    try:
        r = invoke(cli_app, "demo", "--headless", "--home", str(root), "--scenario", "feed_outage", "--timeout", "20")
        assert r.exit_code == 0, r.output
        assert api.calls("GET", "/api/v1/accounts/ACC3") == []
        [(_, _, _, body)] = api.calls("POST", "/api/v1/demo/scenario")
        assert body == {"name": "feed_outage"}
        assert "strategy-sim is not managed by this supervisor" in r.output
        assert [c[2].env for c in sup.calls if c[0] == "svc.start"] == [{"FUTRISK_BOTS_MODE": "scenario"}]
    finally:
        sup.close()
        api.close()


def test_open_shared_lets_the_writer_rename_and_delete(tmp_path: Path) -> None:
    path = tmp_path / "x_20260928.log"
    path.write_bytes(b"one\n")
    with ops_cmd.open_shared(path) as f:
        os.replace(path, tmp_path / "x_20260928.1.log")  # loguru's rename on rotation
        (tmp_path / "x_20260928.1.log").unlink()  # ... and the removal after compression
        assert f.read() == b"one\n"
    with pytest.raises(FileNotFoundError):
        ops_cmd.open_shared(path)


def test_logs_follow_does_not_block_loguru_rotation(tmp_path: Path, capfd: pytest.CaptureFixture[str]) -> None:
    """A-MED-3: ``futrisk logs -f`` while loguru rotates (rename) and compresses (delete) the file."""
    from loguru import logger
    from rich.console import Console

    logs_dir = tmp_path / "logs"
    logs_dir.mkdir()
    sink = logger.add(
        str(logs_dir / "svc_{time:YYYYMMDD}.log"),
        format="{message}",
        rotation=2_000,
        compression="zip",
        filter=lambda r: r["extra"].get("follow_test") is True,
        enqueue=False,
    )
    log = logger.bind(follow_test=True)
    out = io.StringIO()
    stop = threading.Event()
    try:
        log.info("line 0000 first")
        path = ops_cmd.find_log_file(logs_dir, "svc")
        assert path is not None
        follower = threading.Thread(
            target=ops_cmd._follow,
            args=(Console(file=out, width=200), path, logs_dir, "svc"),
            kwargs={"raw": True, "console_log": False, "offset": 0, "poll_s": 0.005, "stop": stop.is_set},
            daemon=True,
        )
        follower.start()
        for n in range(1, 400):
            log.info(f"line {n:04d} " + "x" * 40)
            if n % 20 == 0:
                time.sleep(0.02)
        log.info("line 9999 last")
        deadline = time.monotonic() + 10
        while "line 9999 last" not in out.getvalue():
            assert time.monotonic() < deadline, out.getvalue()[-500:]
            time.sleep(0.02)
    finally:
        stop.set()
        logger.remove(sink)
    err = capfd.readouterr().err
    assert "Logging error in Loguru Handler" not in err, err  # rename and delete never failed
    assert len(list(logs_dir.glob("svc_*.zip"))) >= 5  # rotated and compressed repeatedly
    printed = out.getvalue()
    assert "line 0000 first" in printed and "rotated" in printed
    got = [int(m) for m in re.findall(r"line (\d{4})", printed)]
    assert got == [*range(400), 9999]  # every line once, in order, across the rotations


def test_up_clock_only_and_how_ps_status_show_it(
    tmp_path: Path, cfg: FullConfig, cli_app: typer.Typer, api: FakeApi, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(**kw: Any) -> int:
        captured.update(kw)
        return 0

    monkeypatch.setattr("futrisk.services.supervisor.main.run", fake_run)
    home = str(make_home(tmp_path / "home", cfg, api_url=api.url))
    r = invoke(cli_app, "up", "--home", home, "--clock-only")
    assert r.exit_code == 0 and captured["clock_only"] is True and "market clock only" in r.output
    r = invoke(cli_app, "up", "--home", home, "--headless")
    assert r.exit_code == 0 and captured["clock_only"] is False
    captured.clear()
    r = CliRunner().invoke(cli_app, ["up", "--home", home, "--clock-only", "--svc-env", "strategy-sim:FUTRISK_BOTS_MODE=scenario"])
    assert r.exit_code == 2 and captured == {}

    serve_health(api, home)
    sup = own_supervisor(cfg, home, instances=[], clock_only=True)
    try:
        r = invoke(cli_app, "ps", "--home", home)
        assert r.exit_code == 0 and "clock-only supervisor" in r.output, r.output
        r = invoke(cli_app, "status", "--home", home)
        assert r.exit_code == 0, r.output  # no managed services is healthy for a clock-only supervisor
        assert "clock only" in r.output and "none managed here" in r.output
    finally:
        sup.close()
    sup = own_supervisor(cfg, home, instances=[])  # a normal supervisor without services is not healthy
    try:
        r = invoke(cli_app, "status", "--home", home)
        assert r.exit_code == 1 and "0/0 running" in r.output
    finally:
        sup.close()

