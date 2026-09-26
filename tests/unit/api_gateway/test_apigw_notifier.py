"""Notifier (02 §9.8): notify_key rules, dedupe in api.db across restarts, cursor in the same transaction,
retries with backoff, stale backlog suppression (never CRITICAL) with a digest, min_severity from the config,
alert lifecycle notifications, per-channel rate limit, internal notifications, PENDING resend, adapters."""

from __future__ import annotations

import asyncio
import dataclasses
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from apigw_support import MONDAY_10AM, FakeSender, FixedClock, open_db

from futrisk.bus.fact import CursorUpdate
from futrisk.common.config import FullConfig
from futrisk.common.paths import HomeLayout
from futrisk.db import Database, FactCursors
from futrisk.messages.base import Msg
from futrisk.messages.enums import (
    Action,
    EodPhase,
    FactStartPolicy,
    MdEventKind,
    ProcessState,
    RiskEventKind,
    ServiceChangeReason,
    Severity,
    SourceId,
)
from futrisk.messages.envelope import Envelope, make_envelope
from futrisk.messages.eod import EodPhaseEvent
from futrisk.messages.md import MdEvent
from futrisk.messages.risk import RiskEvent
from futrisk.messages.system import ServiceStateChanged
from futrisk.services.api_gateway.notifier import (
    Notifier,
    WebhookSender,
    build_notification,
    render_channel_body,
)
from futrisk.services.api_gateway.schema import NotificationStatus
from futrisk.services.api_gateway.store import ApiStore, NotificationRow

ALERT_KEY = "20260928|daily_loss|halt|acct:ACC3|1"


def risk_event(event_id: str = "ev1", *, kind: RiskEventKind = RiskEventKind.ALERT_RAISED,
               severity: Severity = Severity.CRITICAL, alert_key: str | None = ALERT_KEY,
               ts_ns: int = MONDAY_10AM, action: Action | None = Action.CIRCUIT_BREAK) -> RiskEvent:
    return RiskEvent(event_id=event_id, kind=kind, trading_day="20260928", ts_ns=ts_ns, account_id="ACC3",
                     scope_key="acct:ACC3", alert_key=alert_key, rule_id="daily_loss", level="halt", episode=1,
                     action=action, severity=severity, latched=True, value=-0.0302, threshold=-0.03,
                     message="ACC3 当日亏损 -3.02%，已熔断禁开")


class Feed:
    """FACT envelopes of one topic with increasing seq (what the FactTracker hands to ``apply``)."""

    def __init__(self, src: str = "risk-engine-1", topic: str = "risk.event", stream: str = "stream-1") -> None:
        self.src, self.topic, self.stream, self.seq = src, topic, stream, 0

    def next(self, body: Msg) -> tuple[Envelope, CursorUpdate]:
        self.seq += 1
        env = make_envelope(self.topic, body, src=self.src, epoch="e1", ts_ns=MONDAY_10AM, seq=self.seq,
                            stream=self.stream)
        return env, CursorUpdate(self.src, self.topic, self.stream, self.seq, FactStartPolicy.FROM_NOW)


def notifier(db: Database, cfg: FullConfig, sender: FakeSender, clock: FixedClock | None = None,
             **kw: Any) -> Notifier:
    kw = {"backoff_s": 0.001, "digest_delay_s": 0.01, "rate_per_min": 6000.0, "burst": 100.0} | kw
    return Notifier(db=db, store=ApiStore(db), config=lambda: cfg, clock=clock or FixedClock(), sender=sender,
                    channel=lambda: "wecom", **kw)


def with_min_severity(cfg: FullConfig, level: str) -> FullConfig:
    notify = cfg.app.notify.model_copy(update={"min_severity": level})
    return dataclasses.replace(cfg, app=cfg.app.model_copy(update={"notify": notify}))


def sent_keys(sender: FakeSender) -> list[str]:
    return [p["notify_key"] for _, p in sender.sent]


class TestMapping:
    def test_risk_events(self) -> None:
        n = build_notification(risk_event())
        assert n is not None and n.notify_key == f"{ALERT_KEY}|ALERT_RAISED" and n.kind == "ALERT_RAISED"
        p = n.payload
        assert p["severity"] == "CRITICAL" and p["account_id"] == "ACC3" and p["action"] == "CIRCUIT_BREAK"
        assert p["value"] == -0.0302 and p["threshold"] == -0.03 and p["ts"].endswith("+08:00")
        n2 = build_notification(risk_event("ev2", kind=RiskEventKind.KILL_SWITCH_ON, alert_key=None))
        assert n2 is not None and n2.notify_key == "KILL_SWITCH_ON|ev2"
        assert build_notification(risk_event(severity=Severity.WARNING)) is None  # below HIGH
        assert build_notification(risk_event(severity=Severity.WARNING), min_severity=Severity.WARNING) is not None

    def test_md_eod_service(self) -> None:
        md = MdEvent(event_id="m1", kind=MdEventKind.MD_DOWN, ts_ns=1, trading_day="20260928", source=SourceId.FEED_A)
        n = build_notification(md)
        assert n is not None and n.notify_key == "MD|m1" and n.severity == Severity.HIGH
        gap = MdEvent(event_id="m2", kind=MdEventKind.GAP_UNRECOVERABLE, ts_ns=1, trading_day="20260928")
        assert build_notification(gap).severity == Severity.WARNING  # type: ignore[union-attr]
        assert build_notification(MdEvent(event_id="m3", kind=MdEventKind.GAP_FILLED, ts_ns=1,
                                          trading_day="20260928")) is None
        eod = EodPhaseEvent(trading_day="20260928", version=2, phase=EodPhase.FAILED, error="prices", ts_ns=1)
        n = build_notification(eod)
        assert n is not None and n.notify_key == "EOD|20260928|2" and n.kind == "EOD_FAILED"
        assert build_notification(EodPhaseEvent(trading_day="20260928", version=2, phase=EodPhase.DONE,
                                                ts_ns=1)) is None
        svc = ServiceStateChanged(instance="risk-engine-1", service="risk-engine", from_state=ProcessState.RESTARTING,
                                  to_state=ProcessState.FAILED, reason=ServiceChangeReason.EXITED, ts_ns=77)
        n = build_notification(svc)
        assert n is not None and n.notify_key == "SVC|risk-engine-1|77" and n.kind == "SERVICE_FAILED"

    def test_channel_bodies(self) -> None:
        p = build_notification(risk_event()).payload  # type: ignore[union-attr]
        assert render_channel_body("wecom", p)["msgtype"] == "markdown"
        assert "ACC3" in render_channel_body("dingtalk", p)["markdown"]["text"]
        assert render_channel_body("feishu", p)["msg_type"] == "text"


class TestDelivery:
    async def test_dedupe_cursor_and_restart(self, tmp_path: Path, default_config: FullConfig) -> None:
        layout = HomeLayout(tmp_path)
        db = await open_db(layout)
        sender = FakeSender()
        n = notifier(db, default_config, sender)
        feed = Feed()
        n.apply(*feed.next(risk_event("ev1")))
        n.apply(*feed.next(risk_event("ev-info", severity=Severity.INFO, alert_key="x|y|z|acct:ACC3|1")))
        await n.drain()
        assert [s[1]["notify_key"] for s in sender.sent] == [f"{ALERT_KEY}|ALERT_RAISED"]
        row = await ApiStore(db).get_notification(f"{ALERT_KEY}|ALERT_RAISED", "wecom")
        assert row is not None and row.status == NotificationStatus.SENT and row.attempts == 1 and row.sent_ns
        cur = FactCursors(db).load_all()[("risk-engine-1", "risk.event")]
        assert cur.last_seq == 2 and cur.stream == "stream-1"  # advanced even for the INFO event
        # the same alert again (e.g. re-emitted after a publisher restore): suppressed, not resent
        n.apply(*feed.next(risk_event("ev1-again")))
        await n.drain()
        assert len(sender.sent) == 1 and n.stats.duplicates == 1
        await n.close()
        await asyncio.to_thread(db.close)

        # restart: new database handle and notifier, dedupe still holds
        db2 = await open_db(layout)
        sender2 = FakeSender()
        n2 = notifier(db2, default_config, sender2)
        feed2 = Feed(stream="stream-2")  # publisher database restored: new stream, events replayed
        n2.apply(*feed2.next(risk_event("ev1")))
        n2.apply(*feed2.next(risk_event("ev9", kind=RiskEventKind.RESET)))
        await n2.drain()
        assert [s[1]["notify_key"] for s in sender2.sent] == [f"{ALERT_KEY}|RESET"]
        assert n2.stats.duplicates == 1
        rows = await ApiStore(db2).list_notifications()
        assert sorted(r.notify_key for r in rows) == [f"{ALERT_KEY}|ALERT_RAISED", f"{ALERT_KEY}|RESET"]
        await n2.close()
        await asyncio.to_thread(db2.close)

    @pytest.mark.parametrize(("fail_times", "status", "attempts"), [(2, NotificationStatus.SENT, 3),
                                                                     (-1, NotificationStatus.FAILED, 4)])
    async def test_retries(self, tmp_path: Path, default_config: FullConfig, fail_times: int,
                           status: NotificationStatus, attempts: int) -> None:
        db = await open_db(HomeLayout(tmp_path))
        sender = FakeSender(fail_times=fail_times)
        n = notifier(db, default_config, sender)
        n.apply(*Feed().next(risk_event()))
        await n.drain()
        row = await ApiStore(db).get_notification(f"{ALERT_KEY}|ALERT_RAISED", "wecom")
        assert row is not None and row.status == status and row.attempts == attempts and sender.attempts == attempts
        if status == NotificationStatus.FAILED:
            assert "webhook down" in (row.last_error or "") and n.stats.failed == 1
        await n.close()
        await asyncio.to_thread(db.close)

    async def test_stale_backlog_is_suppressed_but_never_critical(self, tmp_path: Path,
                                                                  default_config: FullConfig) -> None:
        db = await open_db(HomeLayout(tmp_path))
        sender = FakeSender()
        n = notifier(db, default_config, sender, clock=FixedClock(MONDAY_10AM + 2 * 3600 * 10**9))
        feed = Feed()
        stale_high = risk_event("h1", severity=Severity.HIGH, action=Action.RESTRICT, alert_key="k|r|warn|acct:ACC3|1")
        n.apply(*feed.next(stale_high))
        n.apply(*feed.next(risk_event("c1")))  # CRITICAL circuit break, 2 h late: still sent (p08)
        eod = EodPhaseEvent(trading_day="20260928", version=1, phase=EodPhase.FAILED, error="prices", ts_ns=MONDAY_10AM)
        n.apply(*Feed(src="clearing-worker", topic="sys.eod").next(eod))
        await n.drain()
        store = ApiStore(db)
        high = await store.get_notification("k|r|warn|acct:ACC3|1|ALERT_RAISED", "wecom")
        assert high is not None and high.status == NotificationStatus.SUPPRESSED
        assert (await store.get_notification(f"{ALERT_KEY}|ALERT_RAISED", "wecom")).status == NotificationStatus.SENT  # type: ignore[union-attr]
        assert (await store.get_notification("EOD|20260928|1", "wecom")).status == NotificationStatus.SENT  # type: ignore[union-attr]
        # one digest sums up what was not sent individually
        digests = [p for _, p in sender.sent if p["kind"] == "SUPPRESSED_DIGEST"]
        assert len(digests) == 1 and digests[0]["count"] == 1 and digests[0]["kinds"] == {"ALERT_RAISED": 1}
        assert set(sent_keys(sender)) >= {f"{ALERT_KEY}|ALERT_RAISED", "EOD|20260928|1"}
        counts = await store.count_notifications_by_status()
        assert counts["SUPPRESSED"] == 1 and counts["SENT"] == 3
        await n.close()
        await asyncio.to_thread(db.close)

    async def test_min_severity_comes_from_the_config(self, tmp_path: Path, default_config: FullConfig) -> None:
        db = await open_db(HomeLayout(tmp_path))
        sender = FakeSender()
        cfg = {"c": with_min_severity(default_config, "WARNING")}
        n = Notifier(db=db, store=ApiStore(db), config=lambda: cfg["c"], clock=FixedClock(), sender=sender,
                     channel=lambda: "wecom", backoff_s=0.001)
        feed = Feed()
        n.apply(*feed.next(risk_event("w1", severity=Severity.WARNING, action=Action.ALERT, alert_key="a|b|c|d|1")))
        cfg["c"] = with_min_severity(default_config, "CRITICAL")  # read on every event (hot reload)
        n.apply(*feed.next(risk_event("w2", severity=Severity.HIGH, action=Action.ALERT, alert_key="a|b|c|d|2")))
        await n.drain()
        assert sent_keys(sender) == ["a|b|c|d|1|ALERT_RAISED"]
        await n.close()
        await asyncio.to_thread(db.close)

    async def test_lifecycle_of_a_notified_alert(self, tmp_path: Path, default_config: FullConfig) -> None:
        db = await open_db(HomeLayout(tmp_path))
        sender = FakeSender()
        n = notifier(db, default_config, sender)
        feed = Feed()
        n.apply(*feed.next(risk_event("r1")))  # CRITICAL raise: notified
        for i, kind in enumerate((RiskEventKind.ALERT_ACKED, RiskEventKind.ALERT_CLEARED, RiskEventKind.RESET)):
            n.apply(*feed.next(risk_event(f"l{i}", kind=kind, severity=Severity.INFO, action=None)))
        # the lifecycle of an alert whose raise was below the threshold stays silent
        other = "20260928|daily_loss|warn|acct:ACC3|1"
        n.apply(*feed.next(risk_event("x1", severity=Severity.WARNING, action=Action.ALERT, alert_key=other)))
        n.apply(*feed.next(risk_event("x2", kind=RiskEventKind.ALERT_ACKED, severity=Severity.INFO, action=None,
                                      alert_key=other)))
        await n.drain()
        assert sent_keys(sender) == [f"{ALERT_KEY}|ALERT_RAISED", f"{ALERT_KEY}|ALERT_ACKED",
                                     f"{ALERT_KEY}|ALERT_CLEARED", f"{ALERT_KEY}|RESET"]
        assert n.stats.lifecycle_skipped == 1
        await n.close()
        await asyncio.to_thread(db.close)

    async def test_rate_limit_queues_then_suppresses_non_critical(self, tmp_path: Path,
                                                                  default_config: FullConfig) -> None:
        db = await open_db(HomeLayout(tmp_path))
        sender = FakeSender()
        # 2 tokens, refilled slowly; at most 1 delivery may wait for a token
        n = notifier(db, default_config, sender, rate_per_min=60.0, burst=2.0, max_waiting=1, digest_delay_s=1.3)
        feed = Feed()
        for i in range(5):
            n.apply(*feed.next(risk_event(f"h{i}", severity=Severity.HIGH, action=Action.ALERT,
                                          alert_key=f"k|r|l|acct:ACC3|{i}")))
        n.apply(*feed.next(risk_event("crit")))  # CRITICAL: waits for its token, never suppressed
        await asyncio.wait_for(n.drain(), 10)
        store = ApiStore(db)
        counts = await store.count_notifications_by_status()
        assert counts["SUPPRESSED"] >= 2 and n.stats.suppressed_rate == counts["SUPPRESSED"]
        assert (await store.get_notification(f"{ALERT_KEY}|ALERT_RAISED", "wecom")).status == NotificationStatus.SENT  # type: ignore[union-attr]
        digest = [p for _, p in sender.sent if p["kind"] == "SUPPRESSED_DIGEST"]
        assert len(digest) == 1 and digest[0]["count"] == counts["SUPPRESSED"]
        await n.close()
        await asyncio.to_thread(db.close)

    async def test_internal_notifications_dedupe(self, tmp_path: Path, default_config: FullConfig) -> None:
        db = await open_db(HomeLayout(tmp_path))
        sender = FakeSender()
        n = notifier(db, default_config, sender)
        assert await n.notify(kind="KILL_SWITCH_FAILED", severity=Severity.CRITICAL,
                              notify_key="KILL_SWITCH|c1|trade-gateway", message="not confirmed", target="trade-gateway")
        assert not await n.notify(kind="KILL_SWITCH_FAILED", severity=Severity.CRITICAL,
                                  notify_key="KILL_SWITCH|c1|trade-gateway", message="again")
        await n.drain()
        assert sent_keys(sender) == ["KILL_SWITCH|c1|trade-gateway"] and sender.sent[0][1]["target"] == "trade-gateway"
        await n.close()
        await asyncio.to_thread(db.close)

    async def test_resume_pending(self, tmp_path: Path, default_config: FullConfig) -> None:
        db = await open_db(HomeLayout(tmp_path))
        row = NotificationRow(notify_key="SVC|md-gateway|1", channel="wecom", kind="SERVICE_FAILED",
                              severity=Severity.CRITICAL, status=NotificationStatus.PENDING,
                              payload={"notify_key": "SVC|md-gateway|1"}, first_seen_ns=1)
        await db.run_write(lambda c: ApiStore.insert_notification(c, row))
        sender = FakeSender()
        n = notifier(db, default_config, sender)
        assert await n.resume_pending() == 1
        await n.drain()
        assert sender.sent[0][1]["notify_key"] == "SVC|md-gateway|1"
        got = await ApiStore(db).get_notification("SVC|md-gateway|1", "wecom")
        assert got is not None and got.status == NotificationStatus.SENT
        await n.close()
        await asyncio.to_thread(db.close)


class TestWebhook:
    async def test_posts_channel_body_and_checks_errcode(self) -> None:
        seen: list[dict[str, Any]] = []
        answer = {"errcode": 0, "errmsg": "ok"}

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(json.loads(request.content))
            return httpx.Response(200, json=answer)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        sender = WebhookSender("https://qyapi.example/webhook?key=secret", client=client)
        payload = build_notification(risk_event()).payload  # type: ignore[union-attr]
        await sender.send("wecom", payload)
        assert seen[0]["msgtype"] == "markdown" and ALERT_KEY in seen[0]["markdown"]["content"]
        answer["errcode"] = 93000
        with pytest.raises(RuntimeError, match="93000"):
            await sender.send("wecom", payload)
        await sender.send("log", payload)  # log channel never posts
        assert len(seen) == 2
        await client.aclose()

    async def test_url_is_never_in_errors(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused", request=request)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        sender = WebhookSender("https://qyapi.example/webhook?key=topsecret", client=client)
        with pytest.raises(RuntimeError) as info:
            await sender.send("wecom", {"notify_key": "k", "kind": "X", "severity": "HIGH"})
        assert "topsecret" not in str(info.value) and "qyapi" not in str(info.value)
        sender.set_url(None)  # re-resolved after a config commit: no URL -> log channel behaviour
        await sender.send("wecom", {"notify_key": "k"})
        await client.aclose()


class TestEodWarnings:
    def warn(self, warn_id: str = "w1", phase: EodPhase = EodPhase.SETTLED, **context: Any) -> EodPhaseEvent:
        return EodPhaseEvent(trading_day="20260928", version=1, phase=phase, ts_ns=MONDAY_10AM,
                             detail={"warning": "迟到成交 T9 未计入结算", "kind": "LATE_FACT", "warn_id": warn_id,
                                     **context})

    def prices_fixed(self) -> EodPhaseEvent:
        return EodPhaseEvent(trading_day="20260928", version=1, phase=EodPhase.PRICES_FIXED, ts_ns=MONDAY_10AM,
                             detail={"warnings": [{"kind": "PRE_SETTLE_MISMATCH", "message": "md differs"}],
                                     "pre_settle_mismatch": {"rb2701": {"sod": "3100", "md": "3105"},
                                                             "cu2611": {"sod": "75000", "md": "75010"}}})

    def test_mapping(self) -> None:
        from futrisk.services.api_gateway.notifier import build_notifications

        [n] = build_notifications(self.warn(account_id="ACC3"))
        assert n.notify_key == "EODWARN|20260928|1|w1" and n.kind == "EOD_WARNING" and n.severity == Severity.WARNING
        assert n.payload["warn_kind"] == "LATE_FACT" and n.payload["account_id"] == "ACC3"
        assert n.payload["phase"] == "SETTLED" and "迟到成交" in n.payload["message"]
        keys = [x.notify_key for x in build_notifications(self.prices_fixed())]
        assert keys == ["EODWARN|20260928|1|PRE_SETTLE_MISMATCH|cu2611",
                        "EODWARN|20260928|1|PRE_SETTLE_MISMATCH|rb2701"]
        # a plain phase event carries nothing; FAILED stays EOD_FAILED (CRITICAL)
        assert build_notifications(EodPhaseEvent(trading_day="20260928", version=1, phase=EodPhase.FROZEN,
                                                 ts_ns=1)) == []
        [failed] = build_notifications(EodPhaseEvent(trading_day="20260928", version=1, phase=EodPhase.FAILED,
                                                     error="prices", ts_ns=1))
        assert failed.notify_key == "EOD|20260928|1" and failed.severity == Severity.CRITICAL

    async def test_each_warning_is_notified_once(self, tmp_path: Path, default_config: FullConfig) -> None:
        db = await open_db(HomeLayout(tmp_path))
        sender = FakeSender()
        n = notifier(db, default_config, sender)
        feed = Feed(src="clearing-worker", topic="sys.eod")
        n.apply(*feed.next(self.warn()))
        n.apply(*feed.next(self.warn()))  # the same WARN again (resync, stream change): deduped by warn_id
        n.apply(*feed.next(self.warn("w2", phase=EodPhase.DONE)))  # a WARN on a finished run repeats DONE
        n.apply(*feed.next(self.prices_fixed()))
        await n.drain()
        assert sorted(sent_keys(sender)) == ["EODWARN|20260928|1|PRE_SETTLE_MISMATCH|cu2611",
                                             "EODWARN|20260928|1|PRE_SETTLE_MISMATCH|rb2701",
                                             "EODWARN|20260928|1|w1", "EODWARN|20260928|1|w2"]
        assert n.stats.duplicates == 1
        assert all(p["severity"] == "WARNING" and p["kind"] == "EOD_WARNING" for _, p in sender.sent)
        await n.close()
        await asyncio.to_thread(db.close)


class TestLiquidationPlanEvents:
    async def test_plan_decisions_follow_a_notified_creation(self, tmp_path: Path,
                                                             default_config: FullConfig) -> None:
        db = await open_db(HomeLayout(tmp_path))
        sender = FakeSender()
        n = notifier(db, default_config, sender)
        feed = Feed()
        ak = "20260928|daily_loss|liquidate|acct:ACC3|1"
        created = RiskEvent(event_id="p1", kind=RiskEventKind.LIQUIDATION_PLAN, trading_day="20260928",
                            ts_ns=MONDAY_10AM, account_id="ACC3", alert_key=ak, action=Action.LIQUIDATE,
                            severity=Severity.CRITICAL, message="plan", detail={"plan_id": "LP1", "plan": {}})
        confirmed = RiskEvent(event_id="p2", kind=RiskEventKind.LIQUIDATION_PLAN, trading_day="20260928",
                              ts_ns=MONDAY_10AM + 1, account_id="ACC3", alert_key=ak, action=Action.LIQUIDATE,
                              severity=Severity.WARNING, message="confirmed", actor="risk_manager",
                              detail={"plan_id": "LP1", "status": "CONFIRMED", "actor": "risk_manager"})
        other = RiskEvent(event_id="p3", kind=RiskEventKind.LIQUIDATION_PLAN, trading_day="20260928",
                          ts_ns=MONDAY_10AM + 2, account_id="ACC5", alert_key="x|y|liquidate|acct:ACC5|1",
                          action=Action.LIQUIDATE, severity=Severity.INFO, message="expired",
                          detail={"plan_id": "LP9", "status": "EXPIRED", "actor": None})
        for ev in (created, confirmed, other):
            n.apply(*feed.next(ev))
        await n.drain()
        # the decision shares the alert key of the creation: its own key, sent although WARNING < HIGH
        assert sent_keys(sender) == [f"{ak}|LIQUIDATION_PLAN", f"{ak}|LIQUIDATION_PLAN|CONFIRMED"]
        assert sender.sent[1][1]["plan_status"] == "CONFIRMED" and sender.sent[1][1]["plan_id"] == "LP1"
        assert n.stats.lifecycle_skipped == 1  # LP9's creation was never notified here
        await n.close()
        await asyncio.to_thread(db.close)


def test_md_integrity_severity_follows_the_arbitration_action() -> None:
    from futrisk.messages.enums import MdEventKind
    from futrisk.messages.md import MdEvent
    from futrisk.services.api_gateway.notifier import build_notification

    def ev(action: str | None) -> MdEvent:
        return MdEvent(event_id=f"e-{action}", kind=MdEventKind.MD_INTEGRITY, ts_ns=1, trading_day="20260928",
                       instrument_id="rb2701", detail={"action": action} if action else {})

    sev = {a: (n.severity if (n := build_notification(ev(a))) else None)
           for a in (None, "VOTE", "WM_ROLLBACK", "HOLD", "RELEASE", "EVICT", "NO_VOTE")}
    assert sev == {None: "HIGH", "VOTE": "HIGH", "WM_ROLLBACK": "HIGH", "HOLD": "WARNING",
                   "RELEASE": None, "EVICT": None, "NO_VOTE": None}
