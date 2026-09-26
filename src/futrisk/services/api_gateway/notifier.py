"""Notifier: webhook alerts with dedupe, retries and rate limiting (02 §9.8, 01 §5.3.5; 03 §4.5 ``notifications``).

Sources (consumed as FACTs with persisted cursors, start policy ``FROM_NOW``
so that a first start does not resend history, 02 §4.5):

* ``risk.event`` of every risk shard, severity >= ``notify.min_severity``
  (default ``HIGH``); the lifecycle events of an alert (``ALERT_CLEARED``,
  ``ALERT_ACKED``, ``RESET``) are notified whatever their own severity when the
  raise of the same alert was notified (02 §9.8: raise, clear, ack and reset
  are each notified once); likewise the decision on a liquidation plan
  (``LIQUIDATION_PLAN`` with ``detail.status`` CONFIRMED / CANCELED / EXPIRED)
  when the plan's creation was notified;
* ``md.event`` kinds ``MD_DOWN`` (HIGH), ``MD_INTEGRITY`` by ``detail.action`` (VOTE and
  WM_ROLLBACK HIGH, HOLD WARNING; RELEASE / EVICT / NO_VOTE not notified),
  ``GAP_UNRECOVERABLE`` (WARNING);
* ``sys.eod`` phase ``FAILED`` -> kind ``EOD_FAILED`` (CRITICAL); ``sys.eod``
  WARN events (``detail.warning``, ``detail.kind`` in ``LATE_FACT`` /
  ``OFFICIAL_PRICE_DIFF`` / ``PUSH_INCOMPLETE``, ``detail.warn_id``) -> kind
  ``EOD_WARNING`` (WARNING), one per warning; ``PRE_SETTLE_MISMATCH`` items of
  ``detail.warnings`` (on ``PRICES_FIXED``) -> one ``EOD_WARNING`` per
  instrument;
* ``sys.service`` ``to_state=FAILED`` -> kind ``SERVICE_FAILED`` (CRITICAL);
* api-gateway itself (:meth:`Notifier.notify`): ``KILL_SWITCH_FAILED`` and
  ``CONFIG_FAILED`` (CRITICAL), and the ``SUPPRESSED_DIGEST`` summary.

``notify_key`` (02 §9.8): ``{alert_key}|{kind}`` for alert-related risk events
(``{alert_key}|LIQUIDATION_PLAN|{status}`` for a plan decision, which shares
the alert key of the plan's creation),
``{kind}|{event_id}`` for other risk events, ``MD|{event_id}``,
``EOD|{trading_day}|{version}``, ``EODWARN|{trading_day}|{version}|{warn_id}``,
``EODWARN|{trading_day}|{version}|PRE_SETTLE_MISMATCH|{instrument}``, ``SVC|{instance}|{ts_ns}``,
``KILL_SWITCH|{command_id}|{target}``, ``CONFIG|{version}|FAILED``,
``DIGEST|{ts_ns}``.

Exactly-once bookkeeping (03 §7): for every consumed FACT one transaction
inserts the notification row (``PENDING``) with ``ON CONFLICT DO NOTHING`` on
``(notify_key, channel)`` **and** advances the FACT cursor. A conflict means
the notification already exists (same alert re-emitted, replay after a
publisher stream change, ...): it is not sent again (counted and logged).
After the commit the new rows are delivered by the channel adapter (``wecom``
/ ``dingtalk`` / ``feishu`` webhooks, URL from the environment variable named
by ``notify.webhook_url_env``, or ``log``; re-resolved on every config commit)
with up to three retries and exponential backoff; the row ends ``SENT`` or
``FAILED``. ``PENDING`` rows left by a crash are resent at start-up.

Flood control -- **CRITICAL notifications are never suppressed** (severity
CRITICAL, ``action=CIRCUIT_BREAK``, EOD/service/kill-switch/config failures):

* backlog events older than ``stale_after_s`` (market time) when first seen
  (a long outage) are stored ``SUPPRESSED`` instead of being sent;
* every webhook channel is rate limited (token bucket, ``rate_per_min``,
  default 20/min like WeCom robots); deliveries queue for a token, and when more
  than ``max_waiting`` are queued further non-critical rows are ``SUPPRESSED``;
* suppressed rows are summed up in one ``SUPPRESSED_DIGEST`` notification
  ("N alerts were not sent individually") sent ``digest_delay_s`` after the
  first suppression of a burst.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from collections.abc import Awaitable, Callable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

import httpx
from loguru import logger
from sqlalchemy import Connection

from futrisk.bus.fact import CursorUpdate
from futrisk.common.clock import BJ, Clock
from futrisk.common.config import FullConfig
from futrisk.db import Database, FactCursors
from futrisk.messages.base import Msg
from futrisk.messages.enums import Action, EodPhase, MdEventKind, ProcessState, RiskEventKind, Severity
from futrisk.messages.envelope import Envelope, open_payload
from futrisk.messages.eod import EodPhaseEvent
from futrisk.messages.md import MdEvent
from futrisk.messages.risk import RiskEvent
from futrisk.messages.system import ServiceStateChanged
from futrisk.services.api_gateway.schema import NotificationStatus
from futrisk.services.api_gateway.store import ApiStore, NotificationRow

SEVERITY_RANK = {Severity.INFO: 0, Severity.WARNING: 1, Severity.HIGH: 2, Severity.CRITICAL: 3}
MD_NOTIFY: Mapping[MdEventKind, Severity] = {
    MdEventKind.MD_DOWN: Severity.HIGH,
    MdEventKind.MD_INTEGRITY: Severity.HIGH,
    MdEventKind.GAP_UNRECOVERABLE: Severity.WARNING,
}
#: MD_INTEGRITY notification severity by md-gateway's ``detail.action`` (absent = VOTE)
MD_INTEGRITY_ACTIONS: Mapping[str, Severity] = {
    "VOTE": Severity.HIGH,
    "WM_ROLLBACK": Severity.HIGH,
    "HOLD": Severity.WARNING,
}
# lifecycle events of an alert: notified when the raise of the same alert was (02 §9.8)
LIFECYCLE_KINDS = frozenset({RiskEventKind.ALERT_CLEARED, RiskEventKind.ALERT_ACKED, RiskEventKind.RESET})
RAISE_KIND = RiskEventKind.ALERT_RAISED.value
DIGEST_KIND = "SUPPRESSED_DIGEST"
CRITICAL_KINDS = frozenset({"EOD_FAILED", "SERVICE_FAILED", "KILL_SWITCH_FAILED", "CONFIG_FAILED", DIGEST_KIND})
CHANNELS = ("wecom", "dingtalk", "feishu", "log")
MAX_ATTEMPTS = 4  # first try + 3 retries (02 §9.8)
RATE_PER_MIN = 20.0  # WeCom group robots accept 20 messages per minute
MAX_WAITING = 100


@dataclass(frozen=True)
class Notification:
    """One notification in the generic 02 §9.8 format."""

    notify_key: str
    kind: str
    severity: Severity
    payload: dict[str, Any]
    ts_ns: int
    requires: str | None = None  # notify only when this notify_key was notified (alert lifecycle)

    @property
    def critical(self) -> bool:
        return is_critical(self.kind, self.severity, self.payload)


def is_critical(kind: str, severity: Severity, payload: Mapping[str, Any]) -> bool:
    """Never suppressed (stale backlog or rate limit): CRITICAL, circuit breaks, failures, digests."""
    return (
        severity == Severity.CRITICAL
        or kind in CRITICAL_KINDS
        or payload.get("action") == Action.CIRCUIT_BREAK.value
    )


def _iso(ts_ns: int) -> str:
    dt = datetime.fromtimestamp(ts_ns / 1e9, tz=BJ)
    return dt.isoformat(timespec="milliseconds")


def _base(notify_key: str, kind: str, severity: Severity, ts_ns: int, message: str, **extra: Any) -> dict[str, Any]:
    d: dict[str, Any] = {"notify_key": notify_key, "kind": kind, "severity": severity.value}
    d.update({k: v for k, v in extra.items() if v is not None})
    d["message"] = message
    d["ts"] = _iso(ts_ns)
    return d


EOD_WARNING = "EOD_WARNING"
PRE_SETTLE_MISMATCH = "PRE_SETTLE_MISMATCH"


def _eod_warnings(ev: EodPhaseEvent) -> list[Notification]:
    """``EOD_WARNING`` notifications carried by one ``sys.eod`` event (WARN event or ``detail.warnings``)."""
    detail = ev.detail if isinstance(ev.detail, dict) else {}
    td, version = ev.trading_day, ev.version
    out: list[Notification] = []
    if isinstance(detail.get("warning"), str):
        warn_kind = str(detail.get("kind") or EOD_WARNING)
        warn_id = str(detail.get("warn_id") or ev.ts_ns)
        key = f"EODWARN|{td}|{version}|{warn_id}"
        context = {k: v for k, v in detail.items() if k not in ("warning", "kind", "warn_id")
                   and isinstance(v, (str, int, float, bool))}
        payload = _base(key, EOD_WARNING, Severity.WARNING, ev.ts_ns,
                        f"{td} 盘后结算警告（版本 {version}，{warn_kind}）：{detail['warning']}",
                        trading_day=td, version=version, phase=str(ev.phase), warn_kind=warn_kind, warn_id=warn_id,
                        **context)
        out.append(Notification(key, EOD_WARNING, Severity.WARNING, payload, ev.ts_ns))
    items = detail.get("warnings")
    raw_mismatch = detail.get("pre_settle_mismatch")
    mismatch: dict[str, Any] = raw_mismatch if isinstance(raw_mismatch, dict) else {}
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict) or item.get("kind") != PRE_SETTLE_MISMATCH:
            continue
        instruments = [str(item["instrument"])] if item.get("instrument") else sorted(mismatch) or ["*"]
        for inst in instruments:
            key = f"EODWARN|{td}|{version}|{PRE_SETTLE_MISMATCH}|{inst}"
            raw_prices = mismatch.get(inst)
            prices: dict[str, Any] = raw_prices if isinstance(raw_prices, dict) else {}
            if prices:
                message = (f"{td} 昨结算价不一致（版本 {version}）{inst}：SOD {prices.get('sod')} / "
                           f"md-gateway {prices.get('md')}，按 SOD 结算")
            else:
                message = f"{td} 昨结算价不一致（版本 {version}）：{item.get('message') or inst}"
            payload = _base(key, EOD_WARNING, Severity.WARNING, ev.ts_ns, message, trading_day=td, version=version,
                            phase=str(ev.phase), warn_kind=PRE_SETTLE_MISMATCH,
                            instrument_id=inst if inst != "*" else None, sod=prices.get("sod"), md=prices.get("md"))
            out.append(Notification(key, EOD_WARNING, Severity.WARNING, payload, ev.ts_ns))
    return out


def build_notifications(body: Msg, *, min_severity: Severity = Severity.HIGH) -> list[Notification]:
    """Every notification a consumed FACT payload carries (02 §9.8); empty when none."""
    out: list[Notification] = []
    first = build_notification(body, min_severity=min_severity)
    if first is not None:
        out.append(first)
    if isinstance(body, EodPhaseEvent):
        out.extend(_eod_warnings(body))
    return out


def build_notification(body: Msg, *, min_severity: Severity = Severity.HIGH) -> Notification | None:
    """Map a consumed FACT payload to its main notification, or None (02 §9.8).

    ``sys.eod`` warnings are added by :func:`build_notifications`.
    """
    if isinstance(body, RiskEvent):
        sev = Severity(str(body.severity))
        kind = str(body.kind)
        detail = body.detail if isinstance(body.detail, dict) else {}
        plan_status = detail.get("status") if kind == RiskEventKind.LIQUIDATION_PLAN.value else None
        requires: str | None = None
        if SEVERITY_RANK[sev] < SEVERITY_RANK[min_severity]:
            if not body.alert_key:
                return None
            if RiskEventKind(kind) in LIFECYCLE_KINDS:
                requires = f"{body.alert_key}|{RAISE_KIND}"  # below the threshold: only after a notified raise
            elif isinstance(plan_status, str):
                requires = f"{body.alert_key}|{kind}"  # a plan decision: only after its notified creation
            else:
                return None
        key = f"{body.alert_key}|{kind}" if body.alert_key else f"{kind}|{body.event_id}"
        if isinstance(plan_status, str):
            key = f"{key}|{plan_status}"  # the decision shares the alert key of the creation
        payload = _base(
            key,
            kind,
            sev,
            body.ts_ns,
            body.message or kind,
            account_id=body.account_id,
            rule_id=body.rule_id,
            level=body.level,
            action=str(body.action) if body.action is not None else None,
            value=body.value,
            threshold=body.threshold,
            alert_key=body.alert_key,
            actor=body.actor,
            trading_day=body.trading_day,
            plan_id=detail.get("plan_id") if kind == RiskEventKind.LIQUIDATION_PLAN.value else None,
            plan_status=plan_status if isinstance(plan_status, str) else None,
        )
        return Notification(key, kind, sev, payload, body.ts_ns, requires)
    if isinstance(body, MdEvent):
        md_kind = MdEventKind(str(body.kind))
        md_sev = MD_NOTIFY.get(md_kind)
        if md_kind == MdEventKind.MD_INTEGRITY:
            # by the arbitration step (md-gateway detail.action): a source proven wrong or a watermark
            # roll-back is HIGH, a held instrument WARNING; releases, evictions and no-vote outcomes
            # are routine bookkeeping and not notified
            action = str((body.detail or {}).get("action") or "VOTE")
            md_sev = MD_INTEGRITY_ACTIONS.get(action)
        if md_sev is None:
            return None
        key = f"MD|{body.event_id}"
        what = body.instrument_id or (str(body.source) if body.source else "all sources")
        payload = _base(
            key,
            md_kind.value,
            md_sev,
            body.ts_ns,
            f"行情事件 {md_kind.value}: {what}",
            source=str(body.source) if body.source else None,
            instrument_id=body.instrument_id,
            trading_day=body.trading_day,
        )
        return Notification(key, md_kind.value, md_sev, payload, body.ts_ns)
    if isinstance(body, EodPhaseEvent):
        if EodPhase(str(body.phase)) != EodPhase.FAILED:
            return None
        key = f"EOD|{body.trading_day}|{body.version}"
        payload = _base(
            key,
            "EOD_FAILED",
            Severity.CRITICAL,
            body.ts_ns,
            f"{body.trading_day} 盘后结算失败（版本 {body.version}）：{body.error or '未知原因'}",
            trading_day=body.trading_day,
            version=body.version,
            error=body.error,
        )
        return Notification(key, "EOD_FAILED", Severity.CRITICAL, payload, body.ts_ns)
    if isinstance(body, ServiceStateChanged):
        if ProcessState(str(body.to_state)) != ProcessState.FAILED:
            return None
        key = f"SVC|{body.instance}|{body.ts_ns}"
        payload = _base(
            key,
            "SERVICE_FAILED",
            Severity.CRITICAL,
            body.ts_ns,
            f"服务 {body.instance} 失败（{body.reason}，退出码 {body.exit_code}，当日重启 {body.restarts} 次）",
            instance=body.instance,
            exit_code=body.exit_code,
            restarts=body.restarts,
            reason=str(body.reason),
        )
        return Notification(key, "SERVICE_FAILED", Severity.CRITICAL, payload, body.ts_ns)
    return None


# ------------------------------------------------------------------ adapters
def render_channel_body(channel: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Channel-specific webhook body for the generic payload."""
    title = f"[futrisk] {payload.get('severity')} {payload.get('kind')}"
    lines = [f"**{title}**", str(payload.get("message", ""))]
    for k in ("account_id", "rule_id", "level", "action", "value", "threshold", "instrument_id", "trading_day",
              "target", "config_version", "count"):
        if payload.get(k) is not None:
            lines.append(f"{k}: {payload[k]}")
    lines.append(f"ts: {payload.get('ts')}")
    lines.append(f"key: {payload.get('notify_key')}")
    text = "\n".join(lines)
    if channel == "wecom":
        return {"msgtype": "markdown", "markdown": {"content": text}}
    if channel == "dingtalk":
        return {"msgtype": "markdown", "markdown": {"title": title, "text": text}}
    if channel == "feishu":
        return {"msg_type": "text", "content": {"text": text.replace("**", "")}}
    return dict(payload)


class Sender(Protocol):
    """Delivers one notification on one channel; raises on failure."""

    async def send(self, channel: str, payload: Mapping[str, Any]) -> None: ...


class WebhookSender:
    """Posts to the configured webhook; ``log`` just writes the payload to the service log.

    The URL (a secret: it carries the robot key) is never logged.
    """

    def __init__(self, url: str | None, *, timeout_s: float = 5.0, client: httpx.AsyncClient | None = None) -> None:
        self._url = url
        self._client = client
        self._timeout = timeout_s

    def set_url(self, url: str | None) -> None:
        """New webhook URL after a config commit (``notify.webhook_url_env``)."""
        self._url = url

    async def send(self, channel: str, payload: Mapping[str, Any]) -> None:
        if channel == "log" or not self._url:
            logger.bind(corr_id=payload.get("notify_key")).warning("notify.alert", **{"payload": dict(payload)})
            return
        client = self._client or httpx.AsyncClient(timeout=self._timeout, trust_env=False)
        try:
            resp = await client.post(self._url, json=render_channel_body(channel, payload))
        except httpx.HTTPError as exc:
            raise RuntimeError(f"webhook unreachable: {type(exc).__name__}") from None  # no URL in the message
        finally:
            if self._client is None:
                await client.aclose()
        if resp.status_code >= 300:
            raise RuntimeError(f"webhook HTTP {resp.status_code}")
        try:
            data = resp.json()
        except ValueError:
            data = None
        if isinstance(data, dict):
            code = data.get("errcode", data.get("code", data.get("StatusCode", 0)))
            if code not in (0, "0", None):
                raise RuntimeError(f"webhook error {code}: {data.get('errmsg') or data.get('msg')}")


def resolve_channel(cfg: FullConfig, env: Mapping[str, str] | None = None) -> tuple[str, str | None]:
    """(channel, webhook url) from ``app.notify``; falls back to ``log`` without a URL."""
    notify = cfg.app.notify
    envmap = env if env is not None else os.environ
    url = envmap.get(notify.webhook_url_env) if notify.webhook_url_env else None
    channel = str(notify.channel)
    if channel != "log" and not url:
        if notify.webhook_url_env:
            logger.warning("notify.webhook_url_missing", channel=channel, env=notify.webhook_url_env)
        return "log", None
    return channel, url


def min_severity_of(cfg: FullConfig) -> Severity:
    """``notify.min_severity`` (02 §9.8; a validated literal, default HIGH)."""
    return Severity(cfg.app.notify.min_severity)


class RateLimiter:
    """Token bucket per channel: ``rate_per_min`` messages a minute, bursts up to ``burst``."""

    def __init__(self, rate_per_min: float, burst: float, clock: Callable[[], float]) -> None:
        self.per_s = max(rate_per_min, 1e-9) / 60.0
        self.burst = max(1.0, burst)
        self._clock = clock
        self._state: dict[str, tuple[float, float]] = {}

    def take(self, channel: str) -> float:
        """Take a token: 0 when one was available, else the seconds until the next one."""
        now = self._clock()
        tokens, last = self._state.get(channel, (self.burst, now))
        tokens = min(self.burst, tokens + (now - last) * self.per_s)
        if tokens >= 1.0:
            self._state[channel] = (tokens - 1.0, now)
            return 0.0
        self._state[channel] = (tokens, now)
        return (1.0 - tokens) / self.per_s


@dataclass
class NotifierStats:
    consumed: int = 0
    created: int = 0
    duplicates: int = 0
    lifecycle_skipped: int = 0
    suppressed_stale: int = 0
    suppressed_rate: int = 0
    digests: int = 0
    sent: int = 0
    failed: int = 0
    retries: int = 0
    last_error: str | None = None
    recent: list[str] = field(default_factory=list)


class Notifier:
    """Consumes FACTs, persists notifications with the cursor, delivers them with retries."""

    def __init__(
        self,
        *,
        db: Database,
        store: ApiStore,
        config: Callable[[], FullConfig],
        clock: Clock,
        sender: Sender,
        channel: Callable[[], str],
        submit: Callable[[Callable[[Connection], Any]], Future[Any]] | None = None,
        backoff_s: float = 1.0,
        max_attempts: int = MAX_ATTEMPTS,
        stale_after_s: float = 3600.0,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
        rate_per_min: float = RATE_PER_MIN,
        burst: float = RATE_PER_MIN,
        max_waiting: int = MAX_WAITING,
        digest_delay_s: float = 30.0,
        monotonic: Callable[[], float] | None = None,
    ) -> None:
        self._db = db
        self._store = store
        self._config = config
        self._clock = clock
        self._sender = sender
        self._channel = channel
        self._submit = submit or (lambda fn: db.submit(fn))
        self._cursors = FactCursors(db)
        self._backoff_s = backoff_s
        self._max_attempts = max(1, max_attempts)
        self._stale_ns = int(stale_after_s * 1e9)
        self._sleep = sleep
        self._limiter = RateLimiter(rate_per_min, burst, monotonic or (lambda: clock.monotonic_ns() / 1e9))
        self._max_waiting = max_waiting
        self._digest_delay_s = digest_delay_s
        self._digest_n = 0
        self._digest_kinds: dict[str, int] = {}
        self._digest_task: asyncio.Task[Any] | None = None
        self._waiting = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._closed = False
        self.stats = NotifierStats()

    @property
    def cursors(self) -> FactCursors:
        return self._cursors

    @property
    def channel(self) -> str:
        return self._channel()

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def status(self) -> dict[str, Any]:
        return {"channel": self._channel(), "waiting": self._waiting, "stats": vars(self.stats).copy()}

    # --------------------------------------------------------------- consume
    def apply(self, env: Envelope, cursor: CursorUpdate) -> Future[list[str]]:
        """FACT ``apply`` callback: one transaction = the notification row + the cursor (03 §7)."""
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        self.stats.consumed += 1
        try:
            body = open_payload(env)
        except Exception:
            logger.warning("notify.decode_failed", topic=env.topic, type=env.type)
            body = None
        notes = build_notifications(body, min_severity=min_severity_of(self._config())) if body is not None else []
        channel = self._channel()
        now = self._clock.now_ns()
        rows: list[tuple[NotificationRow, str | None]] = []
        for note in notes:
            stale = self._stale_ns > 0 and now - note.ts_ns > self._stale_ns and not note.critical
            rows.append((NotificationRow(
                notify_key=note.notify_key,
                channel=channel,
                kind=note.kind,
                severity=note.severity,
                status=NotificationStatus.SUPPRESSED if stale else NotificationStatus.PENDING,
                payload=note.payload,
                first_seen_ns=now,
                last_error="stale backlog event: counted in a digest, not sent" if stale else None,
            ), note.requires))

        def tx(conn: Connection) -> list[str]:
            outcomes: list[str] = []
            for row, requires in rows:
                if requires is not None and not ApiStore.was_notified(conn, requires, channel):
                    outcomes.append("skipped")  # lifecycle event of an alert whose raise was not notified
                elif ApiStore.insert_notification(conn, row):
                    outcomes.append("created")
                else:
                    outcomes.append("duplicate")
            cursor.save(conn, self._cursors, updated_ns=now)
            return outcomes

        fut = self._submit(tx)
        if rows:
            fut.add_done_callback(lambda f: self._after_commit(f, [r for r, _ in rows]))
        return fut

    def _after_commit(self, fut: Future[list[str]], rows: list[NotificationRow]) -> None:
        """Writer thread: hop to the loop with the outcome of the transaction."""
        if fut.cancelled() or fut.exception() is not None:
            return
        outcomes = fut.result()
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        for row, outcome in zip(rows, outcomes, strict=True):
            with contextlib.suppress(RuntimeError):  # loop closed meanwhile
                loop.call_soon_threadsafe(self._on_committed, row, outcome)

    def _on_committed(self, row: NotificationRow, outcome: str) -> None:
        if outcome == "duplicate":
            self.stats.duplicates += 1
            logger.info("notify.duplicate_suppressed", notify_key=row.notify_key, channel=row.channel)
            return
        if outcome == "skipped":
            self.stats.lifecycle_skipped += 1
            return
        if outcome != "created":
            return
        self.stats.created += 1
        if row.status == NotificationStatus.SUPPRESSED:
            self.stats.suppressed_stale += 1
            logger.info("notify.stale_suppressed", notify_key=row.notify_key)
            self._note_digest(row.kind)
            return
        self._dispatch(row)

    # -------------------------------------------------------------- internal
    async def notify(
        self,
        *,
        kind: str,
        severity: Severity,
        notify_key: str,
        message: str,
        ts_ns: int | None = None,
        **extra: Any,
    ) -> bool:
        """Notification raised by api-gateway itself (kill switch / config failures, digests).

        Returns False when ``notify_key`` was already notified on the channel.
        """
        now = self._clock.now_ns()
        row = NotificationRow(
            notify_key=notify_key,
            channel=self._channel(),
            kind=kind,
            severity=severity,
            status=NotificationStatus.PENDING,
            payload=_base(notify_key, kind, severity, ts_ns if ts_ns is not None else now, message, **extra),
            first_seen_ns=now,
        )
        try:
            created = await self._db.run_write(lambda c: ApiStore.insert_notification(c, row))
        except RuntimeError:  # database closed during shutdown
            logger.error("notify.not_recorded", notify_key=notify_key, kind=kind)
            return False
        if not created:
            self.stats.duplicates += 1
            return False
        self.stats.created += 1
        self._dispatch(row)
        return True

    # --------------------------------------------------------------- deliver
    def _spawn(self, coro: Awaitable[Any]) -> None:
        if self._closed:
            if asyncio.iscoroutine(coro):
                coro.close()
            return
        task = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _dispatch(self, row: NotificationRow) -> None:
        """Deliver a PENDING row, or suppress it into the digest while the channel backlog is full."""
        limited = row.channel != "log"
        if limited and self._waiting >= self._max_waiting and not is_critical(row.kind, row.severity, row.payload):
            self._spawn(self._suppress(row, "rate limited: channel queue full, counted in a digest"))
            return
        if limited:
            self._waiting += 1  # queued until its first send gets a rate-limit token
        self._spawn(self.deliver(row, queued=limited))

    async def _suppress(self, row: NotificationRow, reason: str) -> None:
        self.stats.suppressed_rate += 1
        logger.warning("notify.rate_suppressed", notify_key=row.notify_key, channel=row.channel)
        await self._store.update_notification(row.notify_key, row.channel, status=NotificationStatus.SUPPRESSED,
                                              attempts=row.attempts, last_error=reason, sent_ns=None)
        self._note_digest(row.kind)

    async def _acquire(self, channel: str) -> None:
        if channel == "log":
            return
        while True:
            wait = self._limiter.take(channel)
            if wait <= 0:
                return
            await self._sleep(wait)

    async def deliver(self, row: NotificationRow, *, queued: bool = False) -> NotificationStatus:
        """Send with retries (idempotent per ``notify_key``); persists SENT or FAILED.

        ``queued``: the row was counted in the channel backlog by :meth:`_dispatch`.
        """
        try:
            await self._acquire(row.channel)
        finally:
            if queued:
                self._waiting -= 1
        return await self._send_attempts(row)

    async def _send_attempts(self, row: NotificationRow) -> NotificationStatus:
        attempts = row.attempts
        error: str | None = row.last_error
        first = True
        while attempts < self._max_attempts:
            attempts += 1
            if not first:
                await self._acquire(row.channel)
            first = False
            try:
                await self._sender.send(row.channel, row.payload)
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                self.stats.last_error = error
                logger.warning("notify.send_failed", notify_key=row.notify_key, attempt=attempts, error=error)
                if attempts < self._max_attempts:
                    self.stats.retries += 1
                    await self._store.update_notification(
                        row.notify_key, row.channel, status=NotificationStatus.PENDING, attempts=attempts,
                        last_error=error, sent_ns=None,
                    )
                    await self._sleep(self._backoff_s * (2 ** (attempts - 1)))
                continue
            sent = self._clock.now_ns()
            await self._store.update_notification(
                row.notify_key, row.channel, status=NotificationStatus.SENT, attempts=attempts, last_error=None,
                sent_ns=sent,
            )
            self.stats.sent += 1
            self.stats.recent = ([row.notify_key] + self.stats.recent)[:20]
            return NotificationStatus.SENT
        await self._store.update_notification(
            row.notify_key, row.channel, status=NotificationStatus.FAILED, attempts=attempts, last_error=error,
            sent_ns=None,
        )
        self.stats.failed += 1
        logger.error("notify.failed", notify_key=row.notify_key, attempts=attempts, error=error)
        return NotificationStatus.FAILED

    # ---------------------------------------------------------------- digest
    def _note_digest(self, kind: str) -> None:
        self._digest_n += 1
        self._digest_kinds[kind] = self._digest_kinds.get(kind, 0) + 1
        if not self._closed and (self._digest_task is None or self._digest_task.done()):
            self._digest_task = asyncio.ensure_future(self._flush_digest())
            self._tasks.add(self._digest_task)
            self._digest_task.add_done_callback(self._tasks.discard)

    async def _flush_digest(self) -> None:
        await self._sleep(self._digest_delay_s)
        n, kinds = self._digest_n, dict(sorted(self._digest_kinds.items()))
        self._digest_n, self._digest_kinds = 0, {}
        if n == 0:
            return
        now = self._clock.now_ns()
        what = ", ".join(f"{k}×{v}" for k, v in kinds.items())
        self.stats.digests += 1
        await self.notify(kind=DIGEST_KIND, severity=Severity.WARNING, notify_key=f"DIGEST|{now}",
                          message=f"{n} 条告警未单独发送（积压过期或通道限流）：{what}", count=n, kinds=kinds)

    # ----------------------------------------------------------------- misc
    async def resume_pending(self) -> int:
        """Start-up: resend PENDING rows left by a crash between commit and delivery."""
        rows = await self._store.notifications_with_status([NotificationStatus.PENDING])
        for row in rows:
            self._dispatch(row)
        if rows:
            logger.info("notify.resume_pending", count=len(rows))
        return len(rows)

    async def drain(self) -> None:
        """Wait for pending deliveries and digests (tests, shutdown)."""
        await asyncio.to_thread(self._db.flush)
        await asyncio.sleep(0)
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
            await asyncio.sleep(0)

    async def close(self) -> None:
        self._closed = True
        for t in list(self._tasks):
            t.cancel()
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)


__all__ = [
    "CHANNELS",
    "CRITICAL_KINDS",
    "DIGEST_KIND",
    "EOD_WARNING",
    "LIFECYCLE_KINDS",
    "MD_INTEGRITY_ACTIONS",
    "MD_NOTIFY",
    "Notification",
    "Notifier",
    "NotifierStats",
    "RateLimiter",
    "Sender",
    "WebhookSender",
    "build_notification",
    "build_notifications",
    "is_critical",
    "min_severity_of",
    "render_channel_body",
    "resolve_channel",
]
