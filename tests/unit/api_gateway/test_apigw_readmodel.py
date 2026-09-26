"""Read model (review M3/M4/L2): FACT gap repair (orders, events, state resync, stream change), LV silence
watchdog, staleness of accounts whose shard is down (REST + compare + positions), bounded memory
(order cap, other-day queries, plan owners)."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from apigw_support import MONDAY_10AM, Harness, hdr, make_gateway, snapshot, with_keys

from futrisk.common.config import FullConfig
from futrisk.messages.base import Msg
from futrisk.messages.enums import Exchange, OrderStatus, RiskEventKind, Role, ServiceState, Severity
from futrisk.messages.envelope import make_envelope, ok_reply
from futrisk.messages.md import MdStatus
from futrisk.messages.orders import OrderUpdate
from futrisk.messages.risk import CashMovement, LiquidationPlanBatch, OrderBatch, RiskEvent, RiskEventBatch
from futrisk.messages.system import Heartbeat
from futrisk.services.api_gateway import readmodel as rm
from futrisk.services.api_gateway.auth import Principal
from futrisk.services.api_gateway.repair import ReadModelRepair
from futrisk.services.api_gateway.wshub import WsClient

API = "/api/v1"


@pytest.fixture
async def h(tmp_path: Path, default_config: FullConfig) -> AsyncIterator[Harness]:
    cfg, env = with_keys(default_config)
    harness = await make_gateway(tmp_path, cfg, env=env)
    try:
        yield harness
    finally:
        await harness.close()


def order(coid: str, *, td: str = "20260928", status: OrderStatus = OrderStatus.ACCEPTED, updated: int = 1,
          account: str = "ACC3") -> OrderUpdate:
    return OrderUpdate(client_order_id=coid, account_id=account, instrument_id="rb2701", exchange=Exchange.SHFE,
                       direction="BUY", offset="OPEN", order_type="LIMIT", tif="GFD", qty=1,  # type: ignore[arg-type]
                       status=status, source="BOT", trading_day=td, created_ns=updated,  # type: ignore[arg-type]
                       updated_ns=updated)


def event(event_id: str, ts: int = 0, kind: RiskEventKind = RiskEventKind.ALERT_RAISED, **kw: Any) -> RiskEvent:
    return RiskEvent(event_id=event_id, kind=kind, trading_day="20260928", ts_ns=MONDAY_10AM + ts,
                     account_id="ACC3", severity=Severity.HIGH, **kw)


class Feed:
    """What the service's Clone handler does with every envelope: repair.observe, then readmodel.apply."""

    def __init__(self, h: Harness, src: str = "risk-engine-1") -> None:
        self.h, self.src = h, src

    def send(self, topic: str, body: Msg, seq: int, stream: str | None = "stream-1") -> None:
        env = make_envelope(topic, body, src=self.src, epoch="e1", ts_ns=MONDAY_10AM, seq=seq, stream=stream)
        assert self.h.gw.repair is not None
        self.h.gw.repair.observe(env)
        self.h.gw.readmodel.apply(env)


def subscriber(h: Harness, topic: str) -> WsClient:
    c = WsClient(ws=None, principal=Principal("v", "v", Role.VIEWER), hub=h.gw.hub)  # type: ignore[arg-type]
    c.topics.add(topic)
    h.gw.hub.subs.setdefault(topic, set()).add(c)
    return c


async def settle(h: Harness) -> None:
    import asyncio

    for _ in range(50):
        await asyncio.sleep(0.01)
        if not h.gw.repair._running:  # type: ignore[union-attr]
            return


class TestFactGaps:
    async def test_order_gap_is_repaired_and_resnapshot(self, h: Harness) -> None:
        h.bus.on("risk.query.orders", lambda t, p: ok_reply(OrderBatch(items=[order(f"O{i}", updated=i)
                                                                              for i in range(1, 6)])))
        ws = subscriber(h, "orders:ACC3")
        feed = Feed(h)
        for seq in (1, 2, 5):  # p10b: seqs 3 and 4 were lost
            feed.send("oms.order.ACC3", order(f"O{seq}", updated=seq), seq)
        await settle(h)
        q = h.bus.of("risk.query.orders")
        assert len(q) == 1 and q[0].target == "risk-engine-1" and q[0].payload.trading_day == "20260928"
        assert sorted(h.gw.readmodel.orders["ACC3"]) == ["O1", "O2", "O3", "O4", "O5"]
        snap = [f for f in ws.events if f.type == "snapshot"]
        assert len(snap) == 1 and b'"O3"' in snap[0].data and h.gw.repair.gaps == 1  # type: ignore[union-attr]
        # contiguous seqs: no further repair
        feed.send("oms.order.ACC3", order("O6", updated=6), 6)
        await settle(h)
        assert len(h.bus.of("risk.query.orders")) == 1

    async def test_event_gap_requeries_the_shard(self, h: Harness) -> None:
        h.bus.on("risk.query.alerts", lambda t, p: ok_reply(RiskEventBatch(items=[event(f"e{i}", i)
                                                                                  for i in range(1, 4)])))
        ws = subscriber(h, "events")
        feed = Feed(h)
        feed.send("risk.event", event("e1", 1), 1)
        feed.send("risk.event", event("e3", 3), 3)
        await settle(h)
        assert [c.target for c in h.bus.of("risk.query.alerts")] == ["risk-engine-1"]
        assert set(h.gw.readmodel.risk_events) == {"e1", "e2", "e3"}
        assert [f.type for f in ws.events][-1] == "snapshot"

    async def test_state_gap_and_stream_change_resync(self, h: Harness) -> None:
        from futrisk.messages.enums import AccountState
        from futrisk.messages.risk import AccountStateChanged

        def st(v: int) -> AccountStateChanged:
            return AccountStateChanged(account_id="ACC3", trading_day="20260928", from_state=AccountState.NORMAL,
                                       to_state=AccountState.WARNING, state_version=v, halted_latched=False,
                                       restrictions=[], cause="ALERT", ts_ns=MONDAY_10AM + v)

        feed = Feed(h)
        feed.send("risk.state.ACC3", st(1), 1)
        feed.send("risk.state.ACC3", st(4), 4)
        assert h.resyncs == ["risk-engine-1"]  # ctl.snapshot of the publisher
        # the publisher's database was replaced (new stream): repaired as well, resyncs are rate limited
        feed.send("oms.order.ACC3", order("A"), 1)
        h.bus.on("risk.query.orders", lambda t, p: ok_reply(OrderBatch(items=[])))
        feed.send("oms.order.ACC3", order("B"), 1, stream="stream-2")
        await settle(h)
        assert len(h.bus.of("risk.query.orders")) == 1
        # cash movements are not shown: counted only
        cash = CashMovement(movement_id="m1", account_id="ACC3", type="DEPOSIT", amount="1.00",  # type: ignore[arg-type]
                            reason="x", actor="a", trading_day="20260928", ts_ns=1)

        feed.send("risk.cash.ACC3", cash, 1)
        feed.send("risk.cash.ACC3", cash, 3)
        assert h.resyncs == ["risk-engine-1"] and h.gw.repair.gaps == 3  # type: ignore[union-attr]

    async def test_failed_repair_retries_then_invalidates(self, h: Harness) -> None:
        from apigw_support import err

        from futrisk.messages import codes

        h.bus.on("risk.query.orders", err(codes.UNAVAILABLE))
        h.gw.hub._orders_seeded["ACC3"] = "20260928"
        feed = Feed(h)
        feed.send("oms.order.ACC3", order("O1"), 1)
        feed.send("oms.order.ACC3", order("O3"), 3)
        await settle(h)
        assert len(h.bus.of("risk.query.orders")) == 3  # REPAIR_ATTEMPTS
        assert "ACC3" not in h.gw.hub._orders_seeded  # the next WS subscription asks the shard again
        assert h.gw.repair.repair_failures == 1  # type: ignore[union-attr]


class TestSilence:
    def test_silent_periodic_topic_triggers_a_resync(self) -> None:
        class Clock:
            mono = 0

            def now_ns(self) -> int:
                return MONDAY_10AM

            def monotonic_ns(self) -> int:
                return self.mono

        clock = Clock()
        resyncs: list[str] = []
        model = rm.ReadModel(clock)  # type: ignore[arg-type]
        repair = ReadModelRepair(readmodel=model, router=None, hub=None, clock=clock,  # type: ignore[arg-type]
                                 trading_day=lambda: "20260928", resync=resyncs.append, silence_s=30.0,
                                 resync_min_interval_s=5.0)
        repair.watch_periodic("md-gateway", "md.status")
        alive = {"md-gateway": True}
        status = MdStatus(as_of_ns=1, trading_day="20260928", sources=[], instruments=[])
        repair.observe(make_envelope("md.status", status, src="md-gateway", epoch="e", ts_ns=1, seq=1))
        clock.mono = 29 * 10**9
        assert repair.check_silence(lambda p: alive[p]) == []
        clock.mono = 31 * 10**9
        assert repair.check_silence(lambda p: alive[p]) == ["md-gateway"] and resyncs == ["md-gateway"]
        clock.mono = 40 * 10**9  # counted again from the resync
        assert repair.check_silence(lambda p: alive[p]) == []
        alive["md-gateway"] = False  # a publisher that is down is not "silent"
        clock.mono = 100 * 10**9
        assert repair.check_silence(lambda p: alive[p]) == [] and resyncs == ["md-gateway"]


class TestStaleness:
    async def test_down_shard_accounts_are_stale(self, h: Harness) -> None:
        h.gw.readmodel.seed_snapshots([snapshot("ACC1", shard=0), snapshot("ACC3", shard=1)])
        h.gw.readmodel.on_heartbeat(Heartbeat(instance="risk-engine-1", service="risk-engine",
                                              state=ServiceState.RUNNING, pid=1, epoch="e", app_version="0",
                                              started_ns=1))
        h.set_peer("risk-engine-1", "DOWN")  # p10a: shard down, its last snapshot still in the model
        async with h.client() as c:
            accounts = {a["account_id"]: a for a in (await c.get(f"{API}/accounts", headers=hdr(Role.VIEWER))).json()["data"]}
            one = (await c.get(f"{API}/accounts/ACC3", headers=hdr(Role.VIEWER))).json()["data"]
            positions = (await c.get(f"{API}/positions", headers=hdr(Role.VIEWER))).json()["data"]
            compare = (await c.get(f"{API}/risk/compare", headers=hdr(Role.VIEWER))).json()["data"]
            state = (await c.get(f"{API}/risk/state?account=ACC3", headers=hdr(Role.VIEWER))).json()["data"]
            status = (await c.get(f"{API}/system/status", headers=hdr(Role.VIEWER))).json()["data"]
        assert accounts["ACC3"]["stale"] is True and isinstance(accounts["ACC3"]["age_ms"], int)
        assert accounts["ACC1"]["stale"] is False and accounts["ACC1"]["age_ms"] is None  # never heard: unknown age
        assert one["stale"] is True and positions["accounts_stale"] == ["ACC3"]
        assert compare["accounts_stale"] == ["ACC3"] and compare["liveness"]["ACC1"]["stale"] is False
        assert [s["account_id"] for s in state] == ["ACC3"] and state[0]["stale"] is True
        assert status["readmodel"]["accounts_stale"] == ["ACC3"]


class TestBounds:
    def test_merge_orders_cap_and_day(self) -> None:
        model = rm.ReadModel(None)  # type: ignore[arg-type]
        n = model.merge_orders("ACC3", [order(f"T{i}", status=OrderStatus.FILLED, updated=i) for i in range(6000)],
                               trading_day="20260928")
        assert n == 6000 and len(model.orders["ACC3"]) == rm.MAX_ORDERS_PER_ACCOUNT
        assert "T0" not in model.orders["ACC3"] and "T5999" in model.orders["ACC3"]  # oldest terminal went first
        assert len(model.order_account) == rm.MAX_ORDERS_PER_ACCOUNT
        # a history query of another day never enters the live model (p13)
        assert model.merge_orders("ACC3", [order("OLD", td="20260925")], trading_day="20260928") == 0
        assert "OLD" not in model.orders["ACC3"]

    async def test_history_orders_query_is_not_merged(self, h: Harness) -> None:
        h.gw.readmodel.seed_snapshots([snapshot("ACC3")])
        h.bus.on("risk.query.orders", lambda t, p: ok_reply(OrderBatch(items=[order(f"H{i}", td=p.trading_day)
                                                                              for i in range(3)])))
        async with h.client() as c:
            r = await c.get(f"{API}/accounts/ACC3/orders?td=20260925", headers=hdr(Role.VIEWER))
            assert r.status_code == 200 and len(r.json()["data"]["items"]) == 3
            assert not h.gw.readmodel.orders.get("ACC3")
            await c.get(f"{API}/accounts/ACC3/orders", headers=hdr(Role.VIEWER))
        assert sorted(h.gw.readmodel.orders["ACC3"]) == ["H0", "H1", "H2"]

    def test_plan_owners_are_bounded(self) -> None:
        model = rm.ReadModel(None)  # type: ignore[arg-type]
        for i in range(rm.MAX_PLANS + 50):
            model._on_risk_event(event(f"p{i}", i, RiskEventKind.LIQUIDATION_PLAN, detail={"plan": {"plan_id": f"P{i}"}}))
        assert len(model.plan_account) == rm.MAX_PLANS and "P0" not in model.plan_account
        assert model.plan_account[f"P{rm.MAX_PLANS + 49}"] == "ACC3"

    def test_merge_risk_events_keeps_the_newest(self) -> None:
        model = rm.ReadModel(None)  # type: ignore[arg-type]
        added = model.merge_risk_events([event(f"e{i}", i) for i in range(rm.MAX_RISK_EVENTS + 10)])
        assert added == rm.MAX_RISK_EVENTS + 10 and len(model.risk_events) == rm.MAX_RISK_EVENTS
        assert "e0" not in model.risk_events and f"e{rm.MAX_RISK_EVENTS + 9}" in model.risk_events



class TestSeeding:
    async def test_shard_is_seeded_once_running_and_stale_before(self, h: Harness) -> None:
        from types import SimpleNamespace

        from futrisk.messages.risk import AccountSnapshotBatch
        from futrisk.services.api_gateway.service import ApiGatewayService

        svc = ApiGatewayService(SimpleNamespace(instance="api-gateway"))  # type: ignore[arg-type]
        svc.gateway = h.gw
        h.gw.shard_ready = lambda shard: shard in svc._seeded
        h.bus.on("risk.query.accounts", lambda t, p: ok_reply(AccountSnapshotBatch(items=[snapshot("ACC3")])))

        def hb(state: ServiceState, epoch: str = "e1") -> Heartbeat:
            return Heartbeat(instance="risk-engine-1", service="risk-engine", state=state, pid=1, epoch=epoch,
                             app_version="0", started_ns=1)

        async def beat(state: ServiceState, epoch: str = "e1") -> None:
            svc._on_shard_heartbeat("risk-engine-1", hb(state, epoch))
            await asyncio.sleep(0.05)

        await beat(ServiceState.STARTING)  # still in SOD / CATCHUP: an answer now could be empty or partial
        assert not h.bus.of("risk.query.accounts") and h.gw.account_liveness("ACC3")["stale"] is True
        await beat(ServiceState.RUNNING)
        assert len(h.bus.of("risk.query.accounts")) == 1 and "ACC3" in h.gw.readmodel.snapshots
        assert h.gw.account_liveness("ACC3")["stale"] is False
        await beat(ServiceState.RUNNING)
        await beat(ServiceState.DEGRADED)
        assert len(h.bus.of("risk.query.accounts")) == 1  # steady: no re-seed
        await beat(ServiceState.RUNNING)  # back from DEGRADED: seeded again
        assert len(h.bus.of("risk.query.accounts")) == 2
        await beat(ServiceState.STARTING, "e2")  # the shard restarted: stale until its new incarnation runs
        assert h.gw.account_liveness("ACC3")["stale"] is True
        await beat(ServiceState.RUNNING, "e2")
        assert len(h.bus.of("risk.query.accounts")) == 3 and h.gw.account_liveness("ACC3")["stale"] is False


class TestEodRuns:
    def phase(self, phase: str, ts: int, **detail: Any) -> Any:
        from futrisk.messages.enums import EodPhase
        from futrisk.messages.eod import EodPhaseEvent

        return EodPhaseEvent(trading_day="20260928", version=1, phase=EodPhase(phase), ts_ns=ts, detail=detail)

    def feed(self, h: Harness, events: list[Any], first_seq: int = 1) -> None:
        for i, e in enumerate(events):
            h.gw.readmodel.apply(make_envelope("sys.eod", e, src="clearing-worker", epoch="e", ts_ns=1,
                                               seq=first_seq + i, stream="s"))

    def test_rewind_repeats_and_warnings(self, h: Harness) -> None:
        model = h.gw.readmodel
        ws = subscriber(h, "eod")
        seen: list[tuple[str, str]] = []
        model.add_listener(lambda ch: seen.append((ch.kind, str(getattr(ch.obj, "phase", "")))))
        mismatch = {"warnings": [{"kind": "PRE_SETTLE_MISMATCH", "message": "rb2701 SOD 3100 / md 3105"}]}
        first = [self.phase("BEGIN", 1), self.phase("FROZEN", 2), self.phase("TRADES_RECONCILED", 3),
                 self.phase("PRICES_FIXED", 4, **mismatch)]
        self.feed(h, first)
        # late facts found at step 5: the run goes back to FROZEN for the same version
        self.feed(h, [self.phase("FROZEN", 5)], 5)
        assert str(model.eod_phase["20260928"].phase) == "FROZEN"
        rest = ["TRADES_RECONCILED", "PRICES_FIXED", "SETTLED", "ROLLED", "REPORTED", "DONE"]
        self.feed(h, [self.phase(name, 6 + i) for i, name in enumerate(rest)], 6)
        warn = self.phase("DONE", 20, warning="迟到成交 T9", kind="LATE_FACT", warn_id="w1", account_id="ACC3")
        self.feed(h, [warn], 12)
        self.feed(h, [first[1]], 2)  # an exact re-delivery (snapshot item) is dropped
        assert str(model.eod_phase["20260928"].phase) == "DONE"  # the WARN is no phase change
        phases = [p for k, p in seen if k == rm.EOD]
        assert phases.count("FROZEN") == 2 and phases.count("PRICES_FIXED") == 2 and phases.count("DONE") == 1
        assert [k for k, _ in seen].count(rm.EOD_WARNING) == 1
        warnings = model.eod_warnings_of("20260928", 1)
        assert [w["kind"] for w in warnings] == ["PRE_SETTLE_MISMATCH", "LATE_FACT"]
        assert warnings[1]["warn_id"] == "w1" and warnings[1]["context"] == {"account_id": "ACC3"}
        assert sum(1 for f in ws.events if b"LATE_FACT" in f.data) == 1  # pushed on the WS eod topic

    async def test_done_triggers_maintenance_once(self, h: Harness) -> None:
        from types import SimpleNamespace

        from futrisk.services.api_gateway.service import ApiGatewayService

        svc = ApiGatewayService(SimpleNamespace(instance="api-gateway", config=h.cfg))  # type: ignore[arg-type]
        svc.gateway = h.gw
        runs: list[str] = []

        async def maintenance(td: str) -> None:
            runs.append(td)

        svc._maintenance = maintenance  # type: ignore[method-assign]
        h.gw.readmodel.add_listener(svc._on_change)
        self.feed(h, [self.phase("DONE", 1),
                      self.phase("DONE", 2, warning="w", kind="PUSH_INCOMPLETE", warn_id="w9"),
                      self.phase("DONE", 3)])
        await asyncio.sleep(0.05)
        assert runs == ["20260928"]

    async def test_settlement_status_passes_warnings_through(self, h: Harness) -> None:
        from apigw_support import err

        from futrisk.messages import codes

        detail = {"warning_count": 2, "warnings": [{"kind": "LATE_FACT", "message": "a"},
                                                  {"kind": "OFFICIAL_PRICE_DIFF", "message": "b"}]}
        h.bus.on("settle.status", lambda t, p: ok_reply(self.phase("DONE", 9, **detail)))
        async with h.client() as c:
            data = (await c.get(f"{API}/settlement/20260928/status", headers=hdr(Role.VIEWER))).json()["data"]
            assert data["detail"]["warning_count"] == 2 and len(data["detail"]["warnings"]) == 2
            days = (await c.get(f"{API}/settlement/days?limit=3", headers=hdr(Role.VIEWER))).json()["data"]
            assert days["items"][0]["warning_count"] == 2
            # clearing-worker down: the latest sys.eod phase and the WARNs seen live
            h.bus.on("settle.status", err(codes.UNAVAILABLE))
            self.feed(h, [self.phase("SETTLED", 4),
                          self.phase("SETTLED", 5, warning="late", kind="LATE_FACT", warn_id="w1")])
            r = await c.get(f"{API}/settlement/20260928/status", headers=hdr(Role.VIEWER))
        body = r.json()
        assert r.status_code == 200 and body["message"].startswith("clearing-worker UNAVAILABLE")
        assert body["data"]["phase"] == "SETTLED" and body["data"]["detail"]["warning_count"] == 1
        assert body["data"]["detail"]["warnings"][0]["warn_id"] == "w1"


class TestLiquidationPlans:
    def created(self, plan_id: str = "LP1", ts: int = 10) -> RiskEvent:
        plan = {"plan_id": plan_id, "account_id": "ACC3", "trading_day": "20260928",
                "trigger_alert_key": "20260928|daily_loss|liquidate|acct:ACC3|1", "status": "PENDING_CONFIRM",
                "steps": [], "created_ns": ts}
        return event(f"c-{plan_id}", ts, RiskEventKind.LIQUIDATION_PLAN, detail={"plan_id": plan_id, "plan": plan})

    def decided(self, status: str, plan_id: str = "LP1", ts: int = 20) -> RiskEvent:
        return event(f"d-{plan_id}-{status}", ts, RiskEventKind.LIQUIDATION_PLAN,
                     detail={"plan_id": plan_id, "status": status, "actor": "risk_manager"})

    def test_events_keep_plans_current(self) -> None:
        model = rm.ReadModel(None)  # type: ignore[arg-type]
        model.apply(make_envelope("risk.event", self.created(), src="risk-engine-1", epoch="e", ts_ns=1, seq=1))
        assert model.liquidation_plans["LP1"]["status"] == "PENDING_CONFIRM" and model.plan_account["LP1"] == "ACC3"
        model.apply(make_envelope("risk.event", self.decided("CONFIRMED"), src="risk-engine-1", epoch="e", ts_ns=1,
                                  seq=2))
        plan = model.liquidation_plans["LP1"]
        assert plan["status"] == "CONFIRMED" and plan["confirmed_by"] == "risk_manager"
        # the creation replayed later (snapshot / backfill) never reverts the decision
        model.merge_risk_events([self.created()])
        model._on_risk_event(event("c-LP1-again", 10, RiskEventKind.LIQUIDATION_PLAN,
                                   detail=self.created().detail))
        assert model.liquidation_plans["LP1"]["status"] == "CONFIRMED"
        assert [p["plan_id"] for p in model.plans_of(status="CONFIRMED")] == ["LP1"]

    async def test_plans_of_a_down_shard_come_from_the_read_model(self, h: Harness) -> None:
        from apigw_support import err

        from futrisk.messages import codes

        h.gw.readmodel.apply(make_envelope("risk.event", self.created(ts=MONDAY_10AM), src="risk-engine-1",
                                           epoch="e", ts_ns=1, seq=1))
        h.bus.on("risk.query.liquidation_plans",
                 lambda t, p: err(codes.UNAVAILABLE) if t == "risk-engine-1" else ok_reply(LiquidationPlanBatch(items=[])))
        async with h.client() as c:
            body = (await c.get(f"{API}/risk/liquidation-plans", headers=hdr(Role.VIEWER))).json()
        assert body["data"]["errors"] == {"risk-engine-1": codes.UNAVAILABLE}
        [plan] = body["data"]["items"]
        assert plan["plan_id"] == "LP1" and plan["stale"] is True and plan["status"] == "PENDING_CONFIRM"


class TestFirmControls:
    def control(self, action: str, origin: str, ts: int, active: bool = False,
                restrictions: list[Any] | None = None) -> Any:
        from futrisk.messages.enums import ControlOrigin, FirmAction
        from futrisk.messages.risk import FirmControl

        return FirmControl(action=FirmAction(action), origin=ControlOrigin(origin), active=active,
                           restrictions=restrictions or [], trading_day="20260928", ts_ns=ts)

    def restriction(self, value: float) -> Any:
        from futrisk.messages.enums import RestrictionKind
        from futrisk.messages.risk import Restriction

        return Restriction(kind=RestrictionKind.ORDER_SIZE_FACTOR, scope_key="firm", value=value, source="firm_dd")

    def feed(self, h: Harness, *controls: Any) -> None:
        for i, c in enumerate(controls):
            h.gw.readmodel.apply(make_envelope("risk.control.firm", c, src="risk-engine-0", epoch="e", ts_ns=1,
                                               seq=i + 1, stream="s"))

    async def test_rule_state_manual_and_legacy(self, h: Harness) -> None:
        model = h.gw.readmodel
        assert model.firm_control_state() is None
        # RULE_STATE: the complete rule layer in one fact
        self.feed(h, self.control("RULE_STATE", "RULE", 10, active=True, restrictions=[self.restriction(0.5)]))
        st = model.firm_control_state()
        assert st is not None and st["kill_switch"] is True and st["manual"] is None
        assert st["rule"]["active"] is True and st["rule"]["restrictions"][0]["value"] == 0.5
        # the rule layer clears; a manual kill switch keeps the effective switch on (MANUAL or RULE)
        h.gw.readmodel.firm_controls.clear()
        self.feed(h, self.control("RULE_STATE", "RULE", 20, active=False),
                  self.control("KILL_SWITCH_ON", "MANUAL", 21, active=True))
        st = model.firm_control_state()
        assert st is not None and st["rule"]["active"] is False and st["rule"]["restrictions"] == []
        assert st["manual"]["active"] is True and st["kill_switch"] is True
        h.gw.readmodel.firm_controls.clear()
        self.feed(h, self.control("KILL_SWITCH_OFF", "MANUAL", 30),
                  self.control("RULE_STATE", "RULE", 15, active=True))  # an older rule fact changes nothing
        st = model.firm_control_state()
        assert st is not None and st["kill_switch"] is False
        # legacy RULE actions are still applied
        h.gw.readmodel.firm_controls.clear()
        self.feed(h, self.control("KILL_SWITCH_ON", "RULE", 40, active=True),
                  self.control("SET_RESTRICTIONS", "RULE", 41, restrictions=[self.restriction(0.3)]))
        st = model.firm_control_state()
        assert st is not None and st["kill_switch"] is True and st["rule"]["restrictions"][0]["value"] == 0.3
        async with h.client() as c:
            status = (await c.get(f"{API}/system/status", headers=hdr(Role.VIEWER))).json()["data"]
        assert status["kill_switch"] is True and status["firm_control"]["rule"]["active"] is True


class TestWatermarkResetTick:
    def test_flagged_tick_replaces_a_newer_one(self) -> None:
        """md-gateway's FLAG_WM_RESET: the first LIVE tick after a watermark roll-back may go backwards."""
        import msgspec

        from futrisk.client.testing import make_tick
        from futrisk.messages.md import FLAG_WM_RESET

        model = rm.ReadModel(None)  # type: ignore[arg-type]
        t0 = 1_790_559_300_000_000_000
        model._on_tick(make_tick("rb2701", 3120.0, ts_ns=t0 + 5_000_000_000))
        model._on_tick(make_tick("rb2701", 3110.0, ts_ns=t0))  # older key: ignored
        assert model.ticks["rb2701"].last == 3120.0
        reset = msgspec.structs.replace(make_tick("rb2701", 3111.0, ts_ns=t0 + 1_000_000_000), flags=FLAG_WM_RESET)
        model._on_tick(reset)
        assert model.ticks["rb2701"].last == 3111.0
