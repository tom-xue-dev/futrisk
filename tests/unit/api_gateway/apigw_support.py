"""Test support for api-gateway: fake bus / sim / sender, a Gateway built without a runtime, builders."""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from futrisk.common.clock import bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.common.config.app import ApiKeyConfig
from futrisk.common.paths import HomeLayout
from futrisk.db import Database
from futrisk.messages import codes
from futrisk.messages.base import Msg
from futrisk.messages.enums import (
    AccountState,
    Exchange,
    MdQuality,
    PosDirection,
    Role,
    RuleSmState,
    TradingMode,
)
from futrisk.messages.envelope import Reply, ok_reply
from futrisk.messages.risk import (
    AccountFunds,
    AccountMetricsView,
    AccountSnapshot,
    ExposureView,
    PositionView,
    RuleStatus,
)
from futrisk.services.api_gateway.app import create_app
from futrisk.services.api_gateway.auth import KeyManager
from futrisk.services.api_gateway.clockjump import ClockJumpOrchestrator
from futrisk.services.api_gateway.config2pc import ConfigCoordinator
from futrisk.services.api_gateway.gateway import Gateway, ws_audit_writer
from futrisk.services.api_gateway.notifier import Notifier
from futrisk.services.api_gateway.ports import SimControlError
from futrisk.services.api_gateway.readmodel import ReadModel
from futrisk.services.api_gateway.repair import ReadModelRepair
from futrisk.services.api_gateway.router import CommandRouter
from futrisk.services.api_gateway.schema import METADATA, MIGRATIONS
from futrisk.services.api_gateway.store import ApiStore
from futrisk.services.api_gateway.wshub import WsHub

MONDAY_10AM = bj_to_ns(datetime(2026, 9, 28, 10, 0, 0))
ROLE_KEYS = {
    Role.VIEWER: "viewer-secret-key",
    Role.TRADER: "trader-secret-key",
    Role.RISK_MANAGER: "risk-mgr-secret-key",
    Role.ADMIN: "admin-secret-key",
}


def new_key() -> str:
    return str(uuid.uuid4())


class FixedClock:
    """Market clock with a settable ``now_ns`` and a real monotonic clock (timeouts still work)."""

    def __init__(self, now_ns: int = MONDAY_10AM) -> None:
        self._now = now_ns
        self.mode = "DEMO"
        self.offset_ns = 0
        self.version = 0

    def now_ns(self) -> int:
        return self._now

    def monotonic_ns(self) -> int:
        return time.perf_counter_ns()

    def set(self, now_ns: int) -> None:
        self._now = now_ns


@dataclass
class Call:
    target: str
    command: str
    payload: Any
    kw: dict[str, Any]


Handler = Callable[[str, Any], Reply | Awaitable[Reply]]


class FakeBus:
    """BusFacade fake: records calls; handlers per (target, command) or per command."""

    def __init__(self, default: Handler | None = None) -> None:
        self.calls: list[Call] = []
        self.handlers: dict[tuple[str | None, str], Handler] = {}
        self.default: Handler = default or (lambda t, p: ok_reply())

    def on(self, command: str, handler: Handler | Reply, target: str | None = None) -> None:
        if isinstance(handler, Reply):
            reply = handler
            self.handlers[(target, command)] = lambda t, p: reply
        else:
            self.handlers[(target, command)] = handler

    async def request(self, target: str, command: str, payload: Msg, **kw: Any) -> Reply:
        self.calls.append(Call(target, command, payload, kw))
        fn = self.handlers.get((target, command)) or self.handlers.get((None, command)) or self.default
        res = fn(target, payload)
        if inspect.isawaitable(res):
            res = await res
        assert isinstance(res, Reply)
        return res

    def of(self, command: str) -> list[Call]:
        return [c for c in self.calls if c.command == command]


class FakeSim:
    """SimControl fake recording ``(method, path, body)``."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Any]] = []
        self.responses: dict[tuple[str, str], Any] = {}
        self.fail: dict[tuple[str, str], SimControlError] = {}

    async def call(self, method: str, path: str, body: Any = None) -> Any:
        self.calls.append((method, path, body))
        err = self.fail.get((method, path))
        if err is not None:
            raise err
        return self.responses.get((method, path), {"ok": True})


class FakeSender:
    """Notification sender fake; ``fail_times`` failures before succeeding (-1: always fail)."""

    def __init__(self, fail_times: int = 0) -> None:
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self.attempts = 0
        self.fail_times = fail_times

    async def send(self, channel: str, payload: Mapping[str, Any]) -> None:
        self.attempts += 1
        if self.fail_times < 0 or self.attempts <= self.fail_times:
            raise RuntimeError("webhook down")
        self.sent.append((channel, dict(payload)))


def with_keys(cfg: FullConfig, *, mode: str = "demo") -> tuple[FullConfig, dict[str, str]]:
    """Config with one named key per role (plaintext in the returned env mapping)."""
    specs = [ApiKeyConfig(name=r.value.lower(), role=r.value, key_env=f"FRK_TEST_{r.value}") for r in ROLE_KEYS]  # type: ignore[arg-type]
    app = cfg.app.model_copy(update={"api_keys": specs, "mode": mode})
    env = {f"FRK_TEST_{r.value}": k for r, k in ROLE_KEYS.items()}
    return dataclasses.replace(cfg, app=app), env


@dataclass
class Harness:
    gw: Gateway
    app: Any
    db: Database
    bus: FakeBus
    sim: FakeSim
    sender: FakeSender
    layout: HomeLayout
    clock: FixedClock
    cfg_ref: dict[str, FullConfig] = field(default_factory=dict)
    resyncs: list[str] = field(default_factory=list)
    peers: dict[str, str] = field(default_factory=dict)  # liveness overrides (default "UP")

    def client(self) -> httpx.AsyncClient:
        transport = httpx.ASGITransport(app=self.app, raise_app_exceptions=False)
        return httpx.AsyncClient(transport=transport, base_url="http://testserver")

    @property
    def cfg(self) -> FullConfig:
        return self.cfg_ref["cfg"]

    def set_peer(self, instance: str, state: str) -> None:
        """Fake a liveness change (``UP`` / ``DOWN``) and tell the gateway, as the runtime would."""
        self.peers[instance] = state
        self.gw.on_peer_change(instance, state)

    async def close(self) -> None:
        await self.gw.hub.stop()
        await self.gw.coordinator.close()
        await self.gw.aclose()
        if self.gw.notifier is not None:
            await self.gw.notifier.close()
        await asyncio.to_thread(self.db.close)


async def open_db(layout: HomeLayout) -> Database:
    db = Database(layout.db_path("api"), METADATA, name="api", synchronous="FULL", migrations=MIGRATIONS)
    await asyncio.to_thread(db.open)
    return db


async def make_gateway(
    tmp_path: Path,
    cfg: FullConfig,
    *,
    env: Mapping[str, str] | None = None,
    bus: FakeBus | None = None,
    sim: FakeSim | None = None,
    sender: FakeSender | None = None,
    clock: FixedClock | None = None,
    heartbeat_config: Callable[[str], tuple[int, str] | None] | None = None,
    clock_versions: Callable[[], dict[str, int]] | None = None,
    verify_timeout_s: float = 1.0,
    prepare_timeout_ms: int = 5000,
    hub_kw: dict[str, Any] | None = None,
    notifier_kw: dict[str, Any] | None = None,
    clockjump_kw: dict[str, Any] | None = None,
    gateway_kw: dict[str, Any] | None = None,
) -> Harness:
    layout = HomeLayout(tmp_path / "home")
    layout.ensure()
    clock = clock or FixedClock()
    bus = bus or FakeBus()
    sim = sim or FakeSim()
    sender = sender or FakeSender()
    db = await open_db(layout)
    store = ApiStore(db)
    keys = KeyManager(store, layout, env=env or {}, restrict_permissions=False)
    await keys.sync(cfg.app)
    ref = {"cfg": cfg}

    def config() -> FullConfig:
        return ref["cfg"]

    readmodel = ReadModel(clock)
    router = CommandRouter(bus, config)
    gw_ref: dict[str, Gateway] = {}
    peers: dict[str, str] = {}
    resyncs: list[str] = []

    def liveness(instance: str) -> str:
        return peers.get(instance, "UP")

    hub = WsHub(
        readmodel,
        keys=keys,
        router=router,
        config=config,
        clock=clock,
        trading_day=lambda: gw_ref["gw"].trading_day(),
        clock_state=lambda: gw_ref["gw"].clock_state(),
        liveness=liveness,
        audit=ws_audit_writer(store, clock),
        **(hub_kw or {}),
    )
    coordinator = ConfigCoordinator(
        store=store,
        bus=bus,
        layout=layout,
        config=config,
        clock=clock,
        instances=lambda: sorted(config().services.instances),
        heartbeat_config=heartbeat_config or (lambda i: None),
        verify_timeout_s=verify_timeout_s,
        prepare_timeout_ms=prepare_timeout_ms,
        resend_s=0.2,
        poll_s=0.02,
    )
    clockjump = ClockJumpOrchestrator(
        bus=bus, sim=sim, clock_versions=clock_versions or (lambda: {}), clock=clock, wait_timeout_s=0.3, poll_s=0.01,
        **({"confirm_s": 0.2, "sim_backoff_s": 0.01} | (clockjump_kw or {})),
    )
    notifier = Notifier(
        db=db, store=store, config=config, clock=clock, sender=sender, channel=lambda: "log",
        **({"backoff_s": 0.01, "digest_delay_s": 0.05} | (notifier_kw or {})),
    )
    repair = ReadModelRepair(readmodel=readmodel, router=router, hub=hub, clock=clock,
                             trading_day=lambda: gw_ref["gw"].trading_day(), resync=resyncs.append, retry_s=0.01)
    gw = Gateway(
        instance="api-gateway",
        layout=layout,
        config=config,
        clock=clock,
        store=store,
        keys=keys,
        readmodel=readmodel,
        router=router,
        hub=hub,
        coordinator=coordinator,
        clockjump=clockjump,
        sim=sim,
        notifier=notifier,
        liveness=liveness,
        epoch="test-epoch",
        repair=repair,
        **({"killswitch_interval_s": 0.05, "killswitch_attempts": 5} | (gateway_kw or {})),
    )
    gw_ref["gw"] = gw
    app = create_app(gw, static=tmp_path / "no-dashboard")
    return Harness(gw=gw, app=app, db=db, bus=bus, sim=sim, sender=sender, layout=layout, clock=clock, cfg_ref=ref,
                   resyncs=resyncs, peers=peers)


def hdr(role: Role | None = Role.ADMIN, *, idem: str | None = None, key: str | None = None) -> dict[str, str]:
    h: dict[str, str] = {}
    if key is not None:
        h["X-API-Key"] = key
    elif role is not None:
        h["X-API-Key"] = ROLE_KEYS[role]
    if idem is not None:
        h["Idempotency-Key"] = idem
    return h


# ------------------------------------------------------------------- payloads
def snapshot(
    account_id: str = "ACC3",
    *,
    shard: int = 1,
    equity: float = 10_000_000.0,
    daily_pnl_pct: float = -0.01,
    state: AccountState = AccountState.NORMAL,
    state_version: int = 1,
    as_of_ns: int = MONDAY_10AM,
    positions: list[PositionView] | None = None,
    exposures: list[ExposureView] | None = None,
    rules: list[RuleStatus] | None = None,
) -> AccountSnapshot:
    funds = AccountFunds(
        initial_capital=10_000_000.0,
        cum_net_deposit=0.0,
        pre_balance=10_000_000.0,
        net_deposit=0.0,
        static_equity=10_000_000.0,
        close_pnl=0.0,
        position_pnl=equity - 10_000_000.0,
        fee=0.0,
        equity=equity,
        margin=1_000_000.0,
        frozen_margin=0.0,
        frozen_fee=0.0,
        available=equity - 1_000_000.0,
        risk_degree=1_000_000.0 / equity,
    )
    metrics = AccountMetricsView(
        daily_pnl=equity - 10_000_000.0,
        daily_pnl_pct_init=daily_pnl_pct,
        daily_pnl_pct_sod=daily_pnl_pct,
        floating_pnl_pct_init=daily_pnl_pct,
        perf_equity=equity,
        nav=equity / 10_000_000.0,
        intraday_peak_equity=10_000_000.0,
        intraday_dd=max(0.0, -daily_pnl_pct),
        hwm_equity=10_000_000.0,
        hwm_dd=max(0.0, -daily_pnl_pct),
        gross_notional=5_000_000.0,
        net_notional=5_000_000.0,
        leverage=0.5,
        effective_n=3.0,
        limit_stress=0.02,
        orders_today=3,
        cancels_today=0,
    )
    return AccountSnapshot(
        account_id=account_id,
        shard=shard,
        trading_day="20260928",
        version=1,
        state_version=state_version,
        as_of_ns=as_of_ns,
        quality=MdQuality.OK,
        state=state,
        halted_latched=False,
        trading_mode=TradingMode.OPEN,
        funds=funds,
        metrics=metrics,
        exposures=exposures or [],
        positions=positions or [],
        rules=rules or [],
    )


def position(account_id: str, instrument: str, product: str, direction: PosDirection, qty: int,
             notional: float, exchange: Exchange = Exchange.SHFE) -> PositionView:
    return PositionView(
        account_id=account_id,
        instrument_id=instrument,
        product=product,
        exchange=exchange,
        direction=direction,
        td_qty=qty,
        yd_qty=0,
        closable_qty=qty,
        closable_td_qty=qty,
        closable_yd_qty=0,
        avg_open_price=3100.0,
        basis_avg_price=3100.0,
        last=3000.0,
        position_pnl=-1000.0,
        float_pnl_fifo=-1000.0,
        margin=notional * 0.08,
        notional=notional,
        pct_equity=notional / 10_000_000.0,
    )


def rule_status(rule_id: str, metric: str, value: float, scope_key: str = "acct:ACC3",
                utilization: float | None = None) -> RuleStatus:
    return RuleStatus(rule_id=rule_id, scope_key=scope_key, metric=metric, value=value,
                      sm_state=RuleSmState.OK, utilization=utilization)


def err(code: str, message: str = "") -> Reply:
    return Reply(ok=False, code=code, message=message or code)


RETRYABLE = sorted(codes.RETRYABLE)
