"""Trading day and day-scoped views (dashboard review M5/M4): the gateway's current trading day follows the
shards (heartbeats, ``sys.dayroll``), a roll rotates the day-scoped read models and re-snapshots the WS topics,
and the ``events`` views carry the state-change history (``type`` discriminator, the alert that caused it)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from apigw_support import MONDAY_10AM, Harness, hdr, make_gateway, snapshot, with_keys

from futrisk.common.config import FullConfig
from futrisk.messages.base import Msg
from futrisk.messages.enums import (
    AccountState,
    Action,
    DayRollMode,
    Exchange,
    OrderStatus,
    RiskEventKind,
    Role,
    ServiceState,
    Severity,
)
from futrisk.messages.envelope import make_envelope
from futrisk.messages.eod import DayRoll
from futrisk.messages.orders import OrderUpdate
from futrisk.messages.risk import AccountStateChanged, RiskEvent
from futrisk.messages.system import Heartbeat
from futrisk.services.api_gateway.auth import Principal
from futrisk.services.api_gateway.wshub import WsClient

API = "/api/v1"
ALERT = "20260928|daily_loss|halt|acct:ACC3|1"


@pytest.fixture
async def h(tmp_path: Path, default_config: FullConfig) -> AsyncIterator[Harness]:
    cfg, env = with_keys(default_config)
    harness = await make_gateway(tmp_path, cfg, env=env)
    try:
        yield harness
    finally:
        await harness.close()


seq = {"n": 0}


def feed(h: Harness, topic: str, body: Msg, src: str = "risk-engine-1", stream: str | None = "s") -> None:
    seq["n"] += 1
    h.gw.readmodel.apply(make_envelope(topic, body, src=src, epoch="e", ts_ns=1, seq=seq["n"], stream=stream))


def heartbeat(instance: str, td: str) -> Heartbeat:
    return Heartbeat(instance=instance, service="risk-engine", state=ServiceState.RUNNING, pid=1, epoch="e",
                     app_version="0", started_ns=1, trading_day=td)


def raised(event_id: str, td: str = "20260928", ts: int = 1) -> RiskEvent:
    return RiskEvent(event_id=event_id, kind=RiskEventKind.ALERT_RAISED, trading_day=td, ts_ns=MONDAY_10AM + ts,
                     account_id="ACC3", alert_key=ALERT, rule_id="daily_loss", level="halt",
                     action=Action.CIRCUIT_BREAK, severity=Severity.CRITICAL, metric="account.daily_pnl_pct_init",
                     value=-0.0302, threshold=-0.03, message="ACC3 当日亏损 -3.02%")


def changed(version: int, td: str = "20260928", ts: int = 2, cause: str | None = ALERT) -> AccountStateChanged:
    return AccountStateChanged(account_id="ACC3", trading_day=td, from_state=AccountState.NORMAL,
                               to_state=AccountState.HALTED, halted_latched=True, state_version=version,
                               restrictions=[], cause="ALERT", cause_alert_key=cause, ts_ns=MONDAY_10AM + ts)


def roll(to_td: str, accounts: list[str], shard: int = 1) -> DayRoll:
    return DayRoll(shard=shard, mode=DayRollMode.ROLL, from_trading_day="20260928", to_trading_day=to_td,
                   sod_version=1, accounts=accounts, ts_ns=MONDAY_10AM + 100)


def subscriber(h: Harness, *topics: str) -> WsClient:
    c = WsClient(ws=None, principal=Principal("v", "v", Role.VIEWER), hub=h.gw.hub)  # type: ignore[arg-type]
    for t in topics:
        c.topics.add(t)
        h.gw.hub.subs.setdefault(t, set()).add(c)
    return c


class TestTradingDay:
    async def test_follows_the_shards_not_the_calendar(self, h: Harness) -> None:
        assert h.gw.trading_day() == "20260928"  # calendar (Monday 10:00), nothing known about the shards
        h.gw.readmodel.on_heartbeat(heartbeat("risk-engine-1", "20260929"))  # the shard already rolled
        assert h.gw.trading_day() == "20260929" and h.gw.hub.trading_day() == "20260929"
        async with h.client() as c:
            r = await c.get(f"{API}/risk/events", headers=hdr(Role.VIEWER))
        assert r.json()["data"]["trading_day"] == "20260929"
        assert h.bus.of("risk.query.alerts")[-1].payload.trading_day == "20260929"  # REST default td
        # no live shard: the newer of the last day a shard reported and the calendar
        h.peers["risk-engine-1"] = "DOWN"
        assert h.gw.trading_day() == "20260929"
        h.gw.readmodel.heartbeats.clear()
        assert h.gw.trading_day() == "20260928"

    async def test_dayroll_moves_the_day_before_the_next_heartbeat(self, h: Harness) -> None:
        h.gw.readmodel.on_heartbeat(heartbeat("risk-engine-1", "20260928"))
        feed(h, "sys.dayroll", roll("20260929", ["ACC3", "ACC4", "ACC5"]))
        assert h.gw.trading_day() == "20260929"


class TestRoll:
    async def test_roll_rotates_and_resnapshots(self, h: Harness) -> None:
        for i, td in enumerate(("20260925", "20260928")):
            feed(h, "risk.event", raised(f"old{i}", td), stream=None)
            feed(h, "risk.state.ACC3", changed(i + 1, td, ts=i + 2))
        order = OrderUpdate(client_order_id="O1", account_id="ACC3", instrument_id="rb2701", exchange=Exchange.SHFE,
                            direction="BUY", offset="OPEN", order_type="LIMIT", tif="GFD", qty=1,  # type: ignore[arg-type]
                            status=OrderStatus.ACCEPTED, source="BOT", trading_day="20260928",  # type: ignore[arg-type]
                            created_ns=1, updated_ns=1)
        h.gw.readmodel.merge_orders("ACC3", [order])
        h.gw.readmodel.seed_snapshots([snapshot("ACC3")])
        ws = subscriber(h, "events", "orders:ACC3", "equity:ACC3")
        h.gw.readmodel.on_heartbeat(heartbeat("risk-engine-1", "20260928"))
        feed(h, "sys.dayroll", roll("20260929", ["ACC3", "ACC4", "ACC5"]))
        rm = h.gw.readmodel
        # the new day and the day before are kept, older days are dropped
        assert {e.trading_day for e in rm.risk_events.values()} == {"20260928"}
        assert {s.trading_day for s in rm.state_changes} == {"20260928"}
        assert not rm.orders.get("ACC3")  # the previous day's orders of the rolled accounts
        # every day-scoped WS topic starts over: the events snapshot is the new (empty) day
        snaps = {f.topic: f for f in ws.events if f.type == "snapshot"}
        assert set(snaps) == {"events", "orders:ACC3", "equity:ACC3"}
        assert snaps["events"].data == b"[]" and snaps["orders:ACC3"].data == b"[]"
        assert "ACC3" not in h.gw.hub._orders_seeded


class TestStateChanges:
    async def test_state_changes_in_rest_and_ws(self, h: Harness) -> None:
        feed(h, "risk.event", raised("r1"), stream=None)
        feed(h, "risk.state.ACC3", changed(7))
        feed(h, "risk.state.ACC3", changed(7))  # the same change again (snapshot / resync): kept once
        async with h.client() as c:
            data = (await c.get(f"{API}/risk/events?account=ACC3", headers=hdr(Role.VIEWER))).json()["data"]
        assert [(i["type"], i["event_id"]) for i in data["items"]] == [("risk_event", "r1")]
        [sc] = data["state_changes"]
        assert sc["type"] == "state_change" and sc["to_state"] == "HALTED" and sc["state_version"] == 7
        assert sc["cause_alert_key"] == ALERT and sc["rule_id"] == "daily_loss" and sc["level"] == "halt"
        assert sc["value"] == -0.0302 and sc["threshold"] == -0.03 and sc["metric"] == "account.daily_pnl_pct_init"
        # WS: one list, both kinds, in time order; live frames are tagged the same way
        snap = h.gw.hub.snapshot_data_sync("events")
        assert [(x["type"], x.get("event_id") or x.get("state_version")) for x in snap] == [
            ("risk_event", "r1"), ("state_change", 7)]
        ws = subscriber(h, "events")
        feed(h, "risk.state.ACC3", changed(8, ts=5, cause=None))
        import json

        frame = json.loads(ws.events[-1].data)
        assert frame["type"] == "state_change" and frame["state_version"] == 8 and frame["rule_id"] is None

    def test_rule_and_level_come_from_the_alert_key_alone(self, h: Harness) -> None:
        item = h.gw.readmodel.state_change_item(changed(3, cause="20260928|net_exposure|restrict|acct:ACC3|2"))
        assert (item["rule_id"], item["level"], item["value"]) == ("net_exposure", "restrict", None)


def _unused(x: Any) -> None:
    del x
