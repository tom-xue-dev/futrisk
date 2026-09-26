"""Kill switch (02 §6.3, §9.6; review H1/M1): one persisted desired state, a new command supersedes pending
retries, only retryable codes are retried (always with the current desired state), give-up is persisted,
notified CRITICAL and published, the REST answer is HTTP 200 with per-target results, start-up reconcile."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
from apigw_support import Harness, err, hdr, make_gateway, new_key, with_keys

from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.messages.enums import ControlOrigin, FirmAction, Role
from futrisk.messages.envelope import Reply, ok_reply
from futrisk.messages.risk import FirmControl, KillSwitchRequest
from futrisk.services.api_gateway.killswitch import KillSwitchManager
from futrisk.services.api_gateway.schema import NotificationStatus
from futrisk.services.api_gateway.store import KillSwitchRow, KillSwitchTarget, TargetStatus

API = "/api/v1"
TARGETS = ["risk-engine-0", "risk-engine-1", "trade-gateway"]


class Targets:
    """firm.kill_switch receivers: per-target answer codes, every call recorded as (target, on, command_id)."""

    def __init__(self, h: Harness) -> None:
        self.calls: list[tuple[str, bool, str]] = []
        self.code: dict[str, str] = {}
        self.script: dict[str, list[str]] = {}  # per target: codes answered in turn, then self.code
        h.bus.on("firm.kill_switch", self.handle)

    def handle(self, target: str, req: KillSwitchRequest) -> Reply:
        self.calls.append((target, req.on, req.command_id))
        script = self.script.get(target)
        code = script.pop(0) if script else self.code.get(target, codes.OK)
        if code != codes.OK:
            return err(code, f"{target} said {code}")
        action = FirmAction.KILL_SWITCH_ON if req.on else FirmAction.KILL_SWITCH_OFF
        return ok_reply(FirmControl(action=action, origin=ControlOrigin.MANUAL, active=req.on,
                                    trading_day="20260928", actor=req.actor, reason=req.reason, ts_ns=1))

    def to(self, target: str) -> list[tuple[bool, str]]:
        return [(on, cid) for t, on, cid in self.calls if t == target]


@pytest.fixture
async def h(tmp_path: Path, default_config: FullConfig) -> AsyncIterator[Harness]:
    cfg, env = with_keys(default_config)
    harness = await make_gateway(tmp_path, cfg, env=env)
    try:
        yield harness
    finally:
        await harness.close()


async def switch(h: Harness, on: bool, key: str | None = None) -> Any:
    async with h.client() as c:
        return await c.post(f"{API}/risk/firm/kill-switch", json={"on": on, "reason": "drill"},
                            headers=hdr(Role.RISK_MANAGER, idem=key or new_key()))


async def eventually(pred: Any, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while True:
        res = pred()
        if asyncio.iscoroutine(res):
            res = await res
        if res:
            return
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.02)


class TestKillSwitch:
    async def test_new_command_supersedes_pending_retries(self, h: Harness) -> None:
        t = Targets(h)
        t.code["trade-gateway"] = codes.UNAVAILABLE  # trade-gateway is down while ON is decided
        r = await switch(h, True)
        assert r.status_code == 200 and r.json()["code"] == codes.UNAVAILABLE
        await eventually(lambda: len(t.to("trade-gateway")) >= 3)  # the real retrier resends ON
        t.code["trade-gateway"] = codes.OK
        off_key = new_key()
        r = await switch(h, False, off_key)
        assert r.status_code == 200 and r.json()["ok"] is True
        n_after_off = len(t.calls)
        await asyncio.sleep(0.4)  # many retry intervals later (p01): no stale ON ever reaches a target
        assert all(not on for _, on, _ in t.calls[n_after_off - 3:])
        assert t.to("trade-gateway")[-1] == (False, off_key)
        assert len(t.calls) == n_after_off and not h.gw.killswitch.retrying
        row = await h.gw.store.load_kill_switch()
        assert row is not None and row.desired_on is False and row.command_id == off_key
        assert row.with_status(TargetStatus.CONFIRMED) == TARGETS

    async def test_retry_sends_the_current_state_and_confirms(self, h: Harness) -> None:
        t = Targets(h)
        t.script["trade-gateway"] = [codes.TIMEOUT, codes.BUSY]
        key = new_key()
        r = await switch(h, True, key)
        body = r.json()
        assert body["data"]["retrying"] == ["trade-gateway"] and body["data"]["all_ok"] is False
        await eventually(lambda: not h.gw.killswitch.retrying)
        assert t.to("trade-gateway") == [(True, key)] * 3  # same command_id: the target deduplicates
        row = await h.gw.store.load_kill_switch()
        assert row is not None and row.targets["trade-gateway"].status == TargetStatus.CONFIRMED
        assert row.targets["trade-gateway"].attempts == 3

    async def test_non_retryable_is_not_retried_and_is_surfaced(self, h: Harness) -> None:
        t = Targets(h)
        t.code["trade-gateway"] = codes.FORBIDDEN
        events: list[dict[str, Any]] = []
        h.gw.hub.system_event = events.append  # type: ignore[method-assign]
        key = new_key()
        r = await switch(h, True, key)
        body = r.json()
        # an authorized RISK_MANAGER never gets the target's 403 as HTTP status (M1)
        assert r.status_code == 200 and not body["ok"] and body["code"] == codes.FORBIDDEN
        assert body["data"]["failed"] == ["trade-gateway"] and body["data"]["retrying"] == []
        await asyncio.sleep(0.3)
        assert len(t.to("trade-gateway")) == 1 and not h.gw.killswitch.retrying  # FORBIDDEN is not retried
        assert events and events[-1]["kind"] == "kill_switch" and events[-1]["failed"] == ["trade-gateway"]
        await self._assert_critical(h, key)
        async with h.client() as c:
            st = (await c.get(f"{API}/system/status", headers=hdr(Role.VIEWER))).json()["data"]
        ctl = st["kill_switch_control"]
        assert ctl["desired_on"] is True and ctl["failed"] == ["trade-gateway"] and ctl["pending"] == []
        assert ctl["targets"]["trade-gateway"]["code"] == codes.FORBIDDEN

    async def test_exhausted_retries_are_surfaced(self, h: Harness) -> None:
        t = Targets(h)
        t.code["trade-gateway"] = codes.TIMEOUT
        key = new_key()
        await switch(h, True, key)
        await eventually(lambda: not h.gw.killswitch.retrying)
        assert len(t.to("trade-gateway")) == h.gw.killswitch.max_attempts == 5
        row = await h.gw.store.load_kill_switch()
        assert row is not None and row.targets["trade-gateway"].status == TargetStatus.FAILED
        assert row.targets["trade-gateway"].code == codes.TIMEOUT
        await self._assert_critical(h, key)

    async def _assert_critical(self, h: Harness, key: str) -> None:
        assert h.gw.notifier is not None
        await h.gw.notifier.drain()
        n = await h.gw.store.get_notification(f"KILL_SWITCH|{key}|trade-gateway", "log")
        assert n is not None and n.severity.value == "CRITICAL" and n.status == NotificationStatus.SENT
        assert n.payload["target"] == "trade-gateway" and n.kind == "KILL_SWITCH_FAILED"
        assert any(p["notify_key"] == f"KILL_SWITCH|{key}|trade-gateway" for _, p in h.sender.sent)

    async def test_startup_reconcile_pushes_the_persisted_state(self, h: Harness) -> None:
        t = Targets(h)
        await h.gw.store.save_kill_switch(KillSwitchRow(
            desired_on=True, command_id="cmd-before-restart", reason="drill", actor="risk_manager",
            role=Role.RISK_MANAGER, requested_ns=1, updated_ns=1,
            targets={"risk-engine-0": KillSwitchTarget(TargetStatus.CONFIRMED),
                     "risk-engine-1": KillSwitchTarget(TargetStatus.CONFIRMED),
                     "trade-gateway": KillSwitchTarget(TargetStatus.PENDING, codes.UNAVAILABLE, attempts=4)}))
        # a fresh manager = api-gateway after a restart
        ks = KillSwitchManager(store=h.gw.store, router=h.gw.router, clock=h.clock, targets=lambda: TARGETS,
                               interval_s=0.05, max_attempts=5)
        try:
            await ks.load()
            assert await ks.reconcile(first_delay_s=0) == ["trade-gateway"]
            await eventually(lambda: not ks.retrying)
            assert t.calls == [("trade-gateway", True, "cmd-before-restart")]  # only the unconfirmed target
            row = await h.gw.store.load_kill_switch()
            assert row is not None and row.with_status(TargetStatus.CONFIRMED) == TARGETS
        finally:
            await ks.close()
