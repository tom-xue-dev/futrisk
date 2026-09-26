"""Dashboard contract additions: liquidation plans, the WS ``backtests`` topic, every configured account in
``/accounts``, service timestamps, stale market feeds and ``/system/status.kill_switch_retry``."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from apigw_support import MONDAY_10AM, ROLE_KEYS, Harness, err, hdr, make_gateway, new_key, with_keys
from websockets.asyncio.client import connect

from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.messages.enums import (
    BacktestState,
    Direction,
    LiquidationStatus,
    Offset,
    ProcessState,
    Role,
    ServiceChangeReason,
    ServiceState,
)
from futrisk.messages.envelope import Reply, make_envelope, ok_reply
from futrisk.messages.eod import BacktestList, BacktestStatus
from futrisk.messages.md import MdStatus
from futrisk.messages.risk import LiquidationPlan, LiquidationPlanBatch, LiquidationStep
from futrisk.messages.system import Heartbeat, ServiceInfo, ServiceList, ServiceStateChanged
from futrisk.services.api_gateway.service import HttpServer

API = "/api/v1"


@pytest.fixture
async def h(tmp_path: Path, default_config: FullConfig) -> AsyncIterator[Harness]:
    cfg, env = with_keys(default_config)
    harness = await make_gateway(tmp_path, cfg, env=env, hub_kw={"tick_s": 0.02})
    try:
        yield harness
    finally:
        await harness.close()


async def get(h: Harness, path: str) -> Any:
    async with h.client() as c:
        return await c.get(API + path, headers=hdr(Role.VIEWER))


def plan(plan_id: str, account: str, created: int,
         status: LiquidationStatus = LiquidationStatus.PENDING_CONFIRM) -> LiquidationPlan:
    return LiquidationPlan(plan_id=plan_id, account_id=account, trading_day="20260928",
                           trigger_alert_key=f"20260928|daily_loss|liquidate|acct:{account}|1", status=status,
                           steps=[LiquidationStep(instrument_id="rb2701", direction=Direction.SELL,
                                                  offset=Offset.CLOSE, qty=2, price_rule="LIMIT_BEST")],
                           created_ns=created)


def backtest(run_id: str, state: BacktestState, progress: float, submitted: int) -> BacktestStatus:
    return BacktestStatus(run_id=run_id, name="baseline", state=state, progress=progress, submitted_ns=submitted)


class TestLiquidationPlans:
    async def test_fan_out_filters_and_partial_answers(self, h: Harness) -> None:
        def plans(target: str, req: Any) -> Reply:
            if target == "risk-engine-0":
                return err(codes.UNAVAILABLE)
            return ok_reply(LiquidationPlanBatch(items=[plan("P2", "ACC3", 2), plan("P1", "ACC5", 1)]))

        h.bus.on("risk.query.liquidation_plans", plans)
        r = await get(h, "/risk/liquidation-plans")
        body = r.json()
        assert r.status_code == 200 and body["message"].startswith("partial")
        assert [p["plan_id"] for p in body["data"]["items"]] == ["P1", "P2"]  # by creation time
        assert body["data"]["errors"] == {"risk-engine-0": codes.UNAVAILABLE}
        assert h.gw.readmodel.plan_account["P2"] == "ACC3"  # confirm is routed by plan id
        r = await get(h, "/risk/liquidation-plans?account=ACC3&status=PENDING_CONFIRM&td=20260928")
        q = h.bus.of("risk.query.liquidation_plans")[-1]
        assert q.target == "risk-engine-1" and q.payload.account_id == "ACC3"
        assert q.payload.status == LiquidationStatus.PENDING_CONFIRM and q.payload.trading_day == "20260928"
        assert (await get(h, "/risk/liquidation-plans?status=DONE")).status_code == 400
        assert (await get(h, "/risk/liquidation-plans?account=ACC9")).status_code == 404


class TestAccountsAndServices:
    async def test_services_carry_state_and_heartbeat_times(self, h: Harness) -> None:
        h.bus.on("svc.ps", lambda t, p: ok_reply(ServiceList(items=[
            ServiceInfo(instance="md-gateway", service="md-gateway", state=ProcessState.RUNNING, pid=11,
                        started_ns=5)])))
        h.gw.readmodel.apply(make_envelope("sys.service", ServiceStateChanged(
            instance="risk-engine-1", service="risk-engine", from_state=ProcessState.RUNNING,
            to_state=ProcessState.EXITED, reason=ServiceChangeReason.EXITED, ts_ns=77), src="supervisor",
            epoch="e", ts_ns=77, seq=1, stream="s"))
        h.gw.readmodel.on_heartbeat(Heartbeat(instance="md-gateway", service="md-gateway",
                                              state=ServiceState.RUNNING, pid=11, epoch="e", app_version="0",
                                              started_ns=5))
        items = {i["instance"]: i for i in (await get(h, "/system/services")).json()["data"]["items"]}
        assert items["md-gateway"]["since_ns"] == 5 and items["md-gateway"]["heartbeat_ns"] == MONDAY_10AM
        assert items["risk-engine-1"]["since_ns"] == 77 and items["risk-engine-1"]["heartbeat_ns"] is None
        assert items["clearing-worker"]["since_ns"] is None

    async def test_feeds_are_stale_while_md_gateway_is_down(self, h: Harness) -> None:
        h.gw.readmodel.apply(make_envelope("md.status", MdStatus(as_of_ns=1, trading_day="20260928", sources=[],
                                                                  instruments=[]),
                                           src="md-gateway", epoch="e", ts_ns=1, seq=1))
        data = (await get(h, "/market/feeds")).json()["data"]
        assert data["stale"] is False and data["trading_day"] == "20260928"
        h.set_peer("md-gateway", "DOWN")
        data = (await get(h, "/market/feeds")).json()["data"]
        assert data["stale"] is True and "age_ms" in data

    async def test_kill_switch_retry_in_system_status(self, h: Harness) -> None:
        st = (await get(h, "/system/status")).json()["data"]
        assert st["kill_switch_retry"] == {"pending": [], "failed": [], "desired": None, "updated_ns": None,
                                           "retrying": False}
        h.bus.on("firm.kill_switch", err(codes.TIMEOUT), target="trade-gateway")
        h.gw.killswitch.interval_s = 30.0
        key = new_key()
        async with h.client() as c:
            await c.post(f"{API}/risk/firm/kill-switch", json={"on": True, "reason": "drill"},
                         headers=hdr(Role.RISK_MANAGER, idem=key))
        ks = (await get(h, "/system/status")).json()["data"]["kill_switch_retry"]
        assert ks["pending"] == ["trade-gateway"] and ks["failed"] == [] and ks["retrying"] is True
        assert ks["desired"] == {"on": True, "reason": "drill", "actor": "risk_manager", "command_id": key}
        assert isinstance(ks["updated_ns"], int)


class TestBacktestsTopic:
    async def test_backtests_snapshot_and_upserts(self, h: Harness) -> None:
        h.bus.on("backtest.list", lambda t, p: ok_reply(BacktestList(items=[
            backtest("B1", BacktestState.DONE, 1.0, 1), backtest("B2", BacktestState.RUNNING, 0.2, 2)])))
        h.gw.hub.start()
        server = HttpServer(h.app, "127.0.0.1", 0)
        await server.start()
        try:
            async with connect(f"ws://127.0.0.1:{server.port}/ws?token={ROLE_KEYS[Role.VIEWER]}") as ws:
                await ws.send(json.dumps({"op": "sub", "topics": ["backtests"]}))
                snap = json.loads(await asyncio.wait_for(ws.recv(), 3))
                assert snap["type"] == "snapshot" and [b["run_id"] for b in snap["data"]] == ["B2", "B1"]
                h.gw.readmodel.apply(make_envelope("backtest.status", backtest("B2", BacktestState.RUNNING, 0.6, 2),
                                                   src="clearing-worker", epoch="e", ts_ns=1, seq=1))
                while True:
                    up = json.loads(await asyncio.wait_for(ws.recv(), 3))
                    if up["type"] != "heartbeat":
                        break
                assert up["type"] == "upsert" and up["topic"] == "backtests" and up["data"]["progress"] == 0.6
        finally:
            await server.stop()


class TestFirmControlLayers:
    async def test_manual_layer_comes_from_the_kill_switch_manager(self, h: Harness) -> None:
        from futrisk.messages.enums import ControlOrigin, FirmAction
        from futrisk.messages.risk import FirmControl, FirmSnapshot

        async with h.client() as c:
            assert (await c.get(f"{API}/system/status", headers=hdr(Role.VIEWER))).json()["data"]["firm_control"] is None
            key = new_key()
            await c.post(f"{API}/risk/firm/kill-switch", json={"on": True, "reason": "drill"},
                         headers=hdr(Role.RISK_MANAGER, idem=key))
            st = (await c.get(f"{API}/system/status", headers=hdr(Role.VIEWER))).json()["data"]["firm_control"]
        manual = st["manual"]
        assert st["kill_switch"] is True and st["rule"] is None
        assert manual["active"] is True and manual["actor"] == "risk_manager" and manual["reason"] == "drill"
        assert manual["command_id"] == key and set(manual["targets"]) == {"risk-engine-0", "risk-engine-1",
                                                                          "trade-gateway"}
        assert manual["confirmed"] == ["risk-engine-0", "risk-engine-1", "trade-gateway"] and manual["pending"] == []
        # the rule layer (RULE_STATE) sits next to it; the WS firm snapshot carries the same state
        h.gw.readmodel.apply(make_envelope("risk.control.firm", FirmControl(
            action=FirmAction.RULE_STATE, origin=ControlOrigin.RULE, active=False, trading_day="20260928",
            ts_ns=5), src="risk-engine-0", epoch="e", ts_ns=1, seq=1, stream="s"))
        h.gw.readmodel.apply(make_envelope("risk.firm", FirmSnapshot(
            trading_day="20260928", as_of_ns=1, equity=1.0, daily_pnl=0.0, daily_pnl_pct_init=0.0, margin=0.0,
            risk_degree=0.0, gross_notional=0.0, net_notional=0.0, kill_switch=True), src="risk-engine-0",
            epoch="e", ts_ns=1, seq=1))
        control = h.gw.hub.snapshot_data_sync("firm")["control"]
        assert control["manual"]["active"] is True and control["rule"]["active"] is False
        assert control["kill_switch"] is True
        async with h.client() as c:
            await c.post(f"{API}/risk/firm/kill-switch", json={"on": False, "reason": "done"},
                         headers=hdr(Role.RISK_MANAGER, idem=new_key()))
        control = h.gw.hub.snapshot_data_sync("firm")["control"]
        assert control["manual"]["active"] is False and control["kill_switch"] is False


class TestWorstUtilization:
    def test_core_hard_utilization_wins(self) -> None:
        from types import SimpleNamespace

        from futrisk.services.api_gateway.readmodel import worst_hard_utilization

        old = [SimpleNamespace(rule_id="nav", utilization=0.9, next_level="warn"),
               SimpleNamespace(rule_id="daily_loss", utilization=0.5, next_level="halt")]
        assert worst_hard_utilization(old) == (0.9, "nav", "warn")  # older shards: next-level utilization
        new = [SimpleNamespace(rule_id="nav", utilization=0.9, next_level="warn", hard_utilization=0.3,
                               hard_level="halt"),
               SimpleNamespace(rule_id="daily_loss", utilization=0.5, next_level="halt", hard_utilization=0.62,
                               hard_level="halt")]
        assert worst_hard_utilization(new) == (0.62, "daily_loss", "halt")  # the core's hard values, max over rules

    def test_compare_cell_uses_the_core_value(self, default_config: FullConfig) -> None:
        from types import SimpleNamespace

        from futrisk.services.api_gateway.views import _cell_util

        rule = next(r for r in default_config.risk_rules.rules if r.id == "daily_loss")
        rs = SimpleNamespace(rule_id="daily_loss", value=-0.01, metric=rule.metric, utilization=0.9,
                             next_threshold=None, hard_utilization=0.4, hard_level="halt")
        u, limit = _cell_util(rs, rule, "ACC3")  # type: ignore[arg-type]
        halt = next(lv for lv in rule.levels if lv.name == "halt")
        assert u == 0.4 and limit == float(halt.threshold)
