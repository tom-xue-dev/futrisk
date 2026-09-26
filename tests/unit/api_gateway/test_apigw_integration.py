"""api-gateway on a real AsyncServiceRuntime next to a stub risk-engine-1 runtime (tcp loopback):
Clone read model (snapshot + live), account seeding, REST over HTTP routed to the shard with actor/role,
WS snapshot, config.decision over the bus, heartbeat liveness, graceful stop."""

from __future__ import annotations

import asyncio
import inspect
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
from apigw_support import ROLE_KEYS, FakeSender, FakeSim, hdr, new_key, snapshot, with_keys
from websockets.asyncio.client import connect

from futrisk.bus.rpc_common import RequestContext
from futrisk.common.clock import MarketClock
from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.messages.base import Msg
from futrisk.messages.enums import AccountState, ConfigStatus, Role, ServiceState
from futrisk.messages.envelope import Reply, ok_reply
from futrisk.messages.risk import AccountSnapshotBatch, ResetAck, ResetRequest
from futrisk.messages.system import ConfigDecision, DecisionQuery
from futrisk.runtime import AsyncServiceRuntime
from futrisk.runtime.testing import config_with_free_ports
from futrisk.services.api_gateway.service import ApiGatewayService
from futrisk.services.api_gateway.store import ConfigVersionRow


async def eventually(pred: Callable[[], Any], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        res = pred()
        if inspect.isawaitable(res):
            res = await res
        if res:
            return
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


async def test_gateway_with_stub_risk_engine(tmp_path: Path, default_config: FullConfig) -> None:
    keyed, env = with_keys(default_config)
    cfg = config_with_free_ports(keyed, heartbeat_ms=100)

    # ---------------------------------------------------------------- stub shard
    risk = AsyncServiceRuntime("risk-engine-1", config=cfg, home=tmp_path / "re1", market_clock=MarketClock(),
                               setup_logs=False)
    resets: list[tuple[str, ResetRequest]] = []

    async def on_reset(cmd: str, req: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(req, ResetRequest)
        resets.append((ctx.requester, req))
        return ok_reply(ResetAck(account_state=AccountState.WARNING, rearm_threshold=-0.041))

    def on_accounts(cmd: str, req: Msg, ctx: RequestContext) -> Reply:
        return ok_reply(AccountSnapshotBatch(items=[snapshot("ACC4", equity=10_100_000.0)]))

    risk.serve({"risk.reset": on_reset, "risk.query.accounts": on_accounts})
    await risk.start()
    risk.set_state(ServiceState.RUNNING)
    risk.publish("risk.snapshot.ACC3", snapshot("ACC3", equity=9_800_000.0))

    # ---------------------------------------------------------------- gateway
    api_rt = AsyncServiceRuntime("api-gateway", config=cfg, home=tmp_path / "api", market_clock=MarketClock(),
                                 setup_logs=False)
    svc = ApiGatewayService(api_rt, sim=FakeSim(), sender=FakeSender(), env=env, restrict_permissions=False,
                            http_port=0, ready_grace_s=60)
    svc.register_handlers()
    await api_rt.start()
    try:
        gw = await svc.start()
        assert svc.http is not None
        base = f"http://127.0.0.1:{svc.http.port}"

        # Clone snapshot of risk.snapshot.* and seeding through risk.query.accounts
        await eventually(lambda: "ACC3" in gw.readmodel.snapshots and "ACC4" in gw.readmodel.snapshots)
        assert svc.synced()["risk-engine-1"] is True
        # live LV update
        risk.publish("risk.snapshot.ACC3", snapshot("ACC3", equity=9_500_000.0, as_of_ns=snapshot().as_of_ns + 1))
        await eventually(lambda: gw.readmodel.snapshots["ACC3"].funds.equity == 9_500_000.0)
        await eventually(lambda: api_rt.peers.state("risk-engine-1") == "UP")
        assert api_rt.state == ServiceState.STARTING or api_rt.state == ServiceState.DEGRADED  # other peers down

        async with httpx.AsyncClient(base_url=base, trust_env=False) as c:
            r = await c.get("/api/v1/accounts/ACC3", headers=hdr(Role.VIEWER))
            assert r.status_code == 200 and r.json()["data"]["funds"]["equity"] == 9_500_000.0
            key = new_key()
            r = await c.post("/api/v1/risk/accounts/ACC3/reset", json={"reason": "checked", "level": "halt"},
                             headers=hdr(Role.RISK_MANAGER, idem=key))
            assert r.status_code == 200 and r.json()["data"] == {"account_state": "WARNING", "rearm_threshold": -0.041}
            r2 = await c.post("/api/v1/risk/accounts/ACC3/reset", json={"reason": "checked", "level": "halt"},
                              headers=hdr(Role.RISK_MANAGER, idem=key))
            assert r2.json()["duplicate"] is True
            # ACC1 lives on risk-engine-0, which is not running: fast UNAVAILABLE -> 503
            r3 = await c.post("/api/v1/risk/accounts/ACC1/halt", json={"reason": "x"},
                              headers=hdr(Role.RISK_MANAGER, idem=new_key()))
            assert r3.status_code == 503 and r3.json()["code"] == codes.UNAVAILABLE
        assert len(resets) == 1
        requester, req = resets[0]
        assert requester == "api-gateway" and req.command_id == key and req.account_id == "ACC3"
        assert req.actor == "risk_manager" and req.role == Role.RISK_MANAGER and req.level == "halt"

        # WebSocket through the real server
        async with connect(base.replace("http", "ws") + "/ws") as ws:
            await ws.send(json.dumps({"op": "auth", "key": ROLE_KEYS[Role.VIEWER]}))
            await ws.send(json.dumps({"op": "sub", "topics": ["accounts"]}))
            msg = json.loads(await asyncio.wait_for(ws.recv(), 3))
            assert msg["type"] == "snapshot" and {a["account_id"] for a in msg["data"]} == {"ACC3", "ACC4"}

        # config.decision answered from api.db, over the bus, to a participant identity
        await gw.store.insert_config_version(ConfigVersionRow(
            config_version=9, config_hash="h9", path="config/versions/9", files={}, effective={},
            status=ConfigStatus.COMMITTING, requested_by="admin", requested_ns=1))
        reply = await risk.request("api-gateway", "config.decision", DecisionQuery(config_version=9), retries=0)
        assert reply.ok and reply.open_as(ConfigDecision).status == ConfigStatus.COMMITTING
        missing = await risk.request("api-gateway", "config.decision", DecisionQuery(config_version=99), retries=0)
        assert missing.code == codes.NOT_FOUND

        # the gateway's own heartbeat carries its service fields
        await eventually(lambda: gw.readmodel.heartbeat_of("api-gateway") is not None)
        hb = gw.readmodel.heartbeat_of("api-gateway")
        assert hb is not None and hb.trading_day and "ws.clients" in hb.queues
    finally:
        await api_rt.stop()
        await risk.stop()
    assert svc.http is None  # stopped through the runtime's stop hook


async def test_ctl_shutdown_with_an_open_websocket(tmp_path: Path, default_config: FullConfig) -> None:
    from websockets.exceptions import ConnectionClosed

    from futrisk.messages.common import ShutdownRequest

    keyed, env = with_keys(default_config)
    cfg = config_with_free_ports(keyed, heartbeat_ms=100)
    sup = AsyncServiceRuntime("supervisor", config=cfg, home=tmp_path / "sup", market_clock=MarketClock(),
                              setup_logs=False)
    await sup.start()
    api_rt = AsyncServiceRuntime("api-gateway", config=cfg, home=tmp_path / "api", market_clock=MarketClock(),
                                 setup_logs=False)
    svc = ApiGatewayService(api_rt, sim=FakeSim(), sender=FakeSender(), env=env, restrict_permissions=False,
                            http_port=0, ready_grace_s=60)
    svc.register_handlers()
    await api_rt.start()
    try:
        await svc.start()
        assert svc.http is not None
        base = f"127.0.0.1:{svc.http.port}"
        async with httpx.AsyncClient(base_url=f"http://{base}", trust_env=False) as c:
            health = (await c.get("/api/v1/health")).json()["data"]
        assert api_rt.layout is not None and health["home_id"] == api_rt.layout.home_id()
        assert health["epoch"] == api_rt.epoch and isinstance(health["pid"], int)
        running = asyncio.ensure_future(api_rt.run_until_stopped())
        async with connect(f"ws://{base}/ws") as ws:
            await ws.send(json.dumps({"op": "auth", "key": ROLE_KEYS[Role.VIEWER]}))
            await ws.send(json.dumps({"op": "sub", "topics": ["accounts"]}))
            assert json.loads(await asyncio.wait_for(ws.recv(), 3))["type"] == "snapshot"
            started = time.monotonic()
            reply = await sup.request("api-gateway", "ctl.shutdown", ShutdownRequest(reason="test", deadline_ms=5000),
                                      retries=0)
            assert reply.ok
            with pytest.raises(ConnectionClosed):
                while True:
                    await asyncio.wait_for(ws.recv(), 10)
            assert ws.close_code in (1001, 1012)  # going away / service restart
        assert await asyncio.wait_for(running, 15) == 0
        assert time.monotonic() - started < 10 and svc.http is None
    finally:
        await api_rt.stop()
        await sup.stop()


async def test_busy_http_port_exits_before_looking_alive(tmp_path: Path) -> None:
    import shutil
    import socket

    from loguru import logger

    from futrisk.common.config import ensure_home_config
    from futrisk.common.config.loader import bundled_config_dir
    from futrisk.common.paths import HomeLayout
    from futrisk.services.api_gateway.auth import bootstrap_key_path
    from futrisk.services.api_gateway.main import main

    busy = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    busy.bind(("127.0.0.1", 0))
    busy.listen()
    port = busy.getsockname()[1]
    layout = HomeLayout(tmp_path / "home")
    layout.ensure()
    shutil.copytree(bundled_config_dir(), layout.config_dir, dirs_exist_ok=True)
    services = layout.config_dir / "services.yaml"
    text = services.read_text(encoding="utf-8")
    assert 'http: "http://127.0.0.1:8000"' in text
    services.write_text(text.replace('http: "http://127.0.0.1:8000"', f'http: "http://127.0.0.1:{port}"'),
                        encoding="utf-8")
    ensure_home_config(layout)
    lines: list[str] = []
    sink = logger.add(lambda m: lines.append(str(m) + json.dumps(m.record["extra"], default=str)), level="INFO")
    try:
        code = await asyncio.wait_for(main(home=str(layout.home)), 20)
    finally:
        logger.remove(sink)
        busy.close()
    assert code == 1
    assert any("api_gateway.http_port_in_use" in line and str(port) in line for line in lines)
    # it never became a live service: no bootstrap key written, api.db never opened, no runtime started
    assert not bootstrap_key_path(layout).exists() and not layout.db_path("api").exists()
    assert not any("runtime.started" in line for line in lines)
