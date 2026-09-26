"""WsHub: the ``/ws`` push protocol (02 §9.7; 01 §5.3.5).

Protocol
--------
* Connect ``/ws?token={api_key}`` or send ``{"op":"auth","key":…}`` as the first
  message; without a valid key within 5 s the server sends an ``error`` frame and
  closes (code 4401). Repeated failures from one address are throttled.
* Client ops: ``sub`` / ``unsub`` / ``resync`` with ``topics: [...]`` (at most
  100 per op, 500 subscriptions per session) and ``ping``. A client message is
  at most 64 KiB (larger: close 1009).
* Topic frames: ``{"v":1,"type","topic","seq","ts","mts","td","data"}`` with
  ``type`` in ``snapshot | upsert | event | resync``; ``seq`` counts per topic
  and connection from 1 (a gap means a lost frame: send ``resync``).
* Session frames: ``{"v":1,"type","ts","mts","data"}`` without ``topic`` /
  ``seq`` / ``td``: ``heartbeat`` (every 5 s, ``data`` null), ``pong``
  (``data.ts`` echoes the ping's ``ts``), ``error`` (``data.code``,
  ``data.message``; unknown topics are listed in ``data.topics``).
* ``ts`` is the server's **wall clock** in epoch ms (use it for staleness);
  ``mts`` is the **market clock** in epoch ms (differs from ``ts`` in demo mode
  after a clock jump, 02 §8.8); ``td`` is the current trading day.
* ``sub`` and ``resync`` answer with a ``snapshot`` of the topic; a ``resync``
  for a topic whose snapshot is still queued is ignored.

Topics and pacing (02 §9.7)
---------------------------
==========================  =====================  ==================  =========
topic                       source                 frames              pacing
==========================  =====================  ==================  =========
``quotes``                  ``md.tick.*``          upsert per contract 2 Hz
``feeds``                   ``md.status``/``md.event``  snapshot / event  1 Hz / at once
``accounts``                ``risk.snapshot.*``    upsert per account  2 Hz
``positions:{acct}``        ``risk.snapshot.{acct}``  snapshot         2 Hz
``orders:{acct}``           ``oms.order.{acct}``   upsert per order    at once
``equity:{acct}``           5 s samples (+ ``risk.query.metrics`` history)  upsert  5 s
``events``                  ``risk.event``, ``risk.state.*`` (+ ``risk.query.alerts`` history)  event  at once
``compare``                 all snapshots          snapshot            1 Hz
``firm``                    ``risk.firm`` (+ ``control``: MANUAL / RULE layers) / ``risk.control.firm``  snapshot / event  1 Hz / at once
``system``                  heartbeats / ``sys.service`` / kill switch / config  snapshot / event  1 Hz / at once
``eod``                     ``sys.eod`` (phases and WARN events), ``clearing.settled``, ``report.ready``, ``sys.dayroll``  event  at once
``backtests``               ``backtest.status`` (+ ``backtest.list`` history)  upsert per run  1 Hz
==========================  =====================  ==================  =========

``events`` items (snapshot list and event frames) carry ``type``:
``"risk_event"`` (a ``RiskEvent``) or ``"state_change"`` (an
``AccountStateChanged`` plus ``rule_id`` / ``level`` / ``metric`` / ``value`` /
``threshold`` of the alert that caused it); the snapshot holds the current
trading day only and is sent again when the shards roll to a new day (as are
the ``orders:`` / ``equity:`` snapshots of the rolled accounts).

``accounts`` items carry ``stale`` / ``age_ms`` (liveness of the owning shard).
``system`` snapshot items carry ``kind``: ``"service"`` (one per instance) or
``"clock"`` (``instance="sys.clock"``, the market clock); ``system`` events
carry ``kind`` ``"service"`` (``ServiceStateChanged``), ``"kill_switch"`` or
``"config"``.

Slow clients (bounded memory)
-----------------------------
* state-like frames keep only the latest value per key; a topic snapshot
  supersedes everything queued for that topic (upserts, events, a pending
  ``resync``);
* event topics are never dropped silently: at most 1000 queued frames per
  topic, beyond that the topic's frames are discarded and a ``resync`` frame
  tells the client to reload it;
* session frames are coalesced (one queued ``pong`` / ``heartbeat``, one
  ``resync`` per topic, at most 16 errors); more than 64 queued session frames
  closes the session with 1008 (policy violation);
* every session has a byte budget (8 MiB queued) and a send timeout (10 s):
  past either the session is closed with 1013 (try again later);
* sessions are re-checked after every key sync and every 5 s: a revoked,
  disabled or expired key closes the session with 4401.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import msgspec
from loguru import logger
from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState

from futrisk.common.clock import Clock
from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.messages.base import to_builtins
from futrisk.messages.envelope import Reply
from futrisk.messages.eod import BacktestList, BacktestListRequest
from futrisk.messages.risk import (
    MetricSeries,
    OrderBatch,
    QueryAlerts,
    QueryMetrics,
    QueryOrders,
    RiskEventBatch,
)
from futrisk.services.api_gateway import readmodel as rm
from futrisk.services.api_gateway.auth import AuthThrottle, KeyManager, Perm, Principal, hash_key
from futrisk.services.api_gateway.router import CLEARING, CommandRouter, UnknownAccount
from futrisk.services.api_gateway.views import build_compare_view

STATIC_TOPICS = ("quotes", "feeds", "accounts", "events", "compare", "firm", "system", "eod", "backtests")
KEYED_PREFIXES = ("positions:", "orders:", "equity:")
EVENT_TOPICS = frozenset({"events", "eod"})
INTERVAL_S: dict[str, float] = {
    "quotes": 0.5,
    "accounts": 0.5,
    "positions": 0.5,
    "feeds": 1.0,
    "compare": 1.0,
    "firm": 1.0,
    "system": 1.0,
    "backtests": 1.0,
}
AUTH_TIMEOUT_S = 5.0
HEARTBEAT_S = 5.0
REVALIDATE_S = 5.0
QUEUE_LIMIT = 1000  # queued event frames per topic before a resync
CONTROL_LIMIT = 64  # queued session frames before the session is closed (1008)
ERROR_LIMIT = 16  # queued error frames; further errors are dropped
MAX_TOPICS_PER_OP = 100
MAX_TOPICS_PER_CLIENT = 500
MAX_MESSAGE_BYTES = 64 * 1024
BYTE_BUDGET = 8 * 1024 * 1024
SEND_TIMEOUT_S = 10.0
SNAPSHOT_EVENTS = 200
FRAME_OVERHEAD = 96  # bytes counted per queued frame besides its data
CLOSE_UNAUTHORIZED = 4401
CLOSE_POLICY = 1008
CLOSE_TOO_BIG = 1009
CLOSE_TRY_AGAIN = 1013
_enc = msgspec.json.Encoder()


def is_event_topic(topic: str) -> bool:
    return topic in EVENT_TOPICS or topic.startswith(("orders:", "equity:"))


def topic_family(topic: str) -> str:
    return topic.split(":", 1)[0]


@dataclass
class _Frame:
    type: str
    topic: str | None
    data: bytes  # pre-encoded JSON of ``data``

    @property
    def size(self) -> int:
        return len(self.data) + FRAME_OVERHEAD


@dataclass(eq=False)  # identity semantics: clients live in sets
class WsClient:
    """One authenticated WebSocket session and its bounded send queues."""

    ws: WebSocket
    principal: Principal
    hub: WsHub
    key_hash: str = ""
    host: str = ""
    topics: set[str] = field(default_factory=set)
    seq: dict[str, int] = field(default_factory=dict)
    events: deque[_Frame] = field(default_factory=deque)
    event_counts: dict[str, int] = field(default_factory=dict)
    state: OrderedDict[tuple[str, str], _Frame] = field(default_factory=OrderedDict)
    control: deque[_Frame] = field(default_factory=deque)
    pending_snapshots: set[str] = field(default_factory=set)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task[None] | None = None
    queued_bytes: int = 0
    closed: bool = False
    terminating: bool = False
    close_code: int | None = None
    close_reason: str = ""
    sent: int = 0
    overflows: int = 0
    dropped_errors: int = 0

    # ------------------------------------------------------- byte accounting
    def _add(self, f: _Frame) -> None:
        self.queued_bytes += f.size
        if self.queued_bytes > self.hub.byte_budget:
            self.evict(CLOSE_TRY_AGAIN, f"slow consumer: more than {self.hub.byte_budget} bytes queued")

    def _sub(self, f: _Frame) -> None:
        self.queued_bytes = max(0, self.queued_bytes - f.size)

    def _purge_events(self, topic: str) -> None:
        keep: deque[_Frame] = deque()
        for f in self.events:
            if f.topic == topic:
                self._sub(f)
            else:
                keep.append(f)
        self.events = keep
        self.event_counts.pop(topic, None)

    def _purge_control(self, topic: str) -> None:
        for f in [f for f in self.control if f.topic == topic]:
            self.control.remove(f)
            self._sub(f)

    # ----------------------------------------------------------- queueing
    def put_event(self, frame: _Frame) -> None:
        if self.closed:
            return
        topic = frame.topic or ""
        n = self.event_counts.get(topic, 0)
        if n >= QUEUE_LIMIT:
            # slow consumer: drop the topic's backlog and ask the client to reload it (02 §9.7)
            self._purge_events(topic)
            self.overflows += 1
            logger.warning("ws.overflow", client=self.principal.key_id, topic=topic)
            self.put_control(_Frame("resync", topic, b"null"))
        else:
            self.events.append(frame)
            self.event_counts[topic] = n + 1
            self._add(frame)
        self.wake.set()

    def put_state(self, key: str, frame: _Frame) -> None:
        if self.closed:
            return
        topic = frame.topic or ""
        if frame.type == "snapshot":
            for k in [k for k in self.state if k[0] == topic]:
                self._sub(self.state.pop(k))
        k2 = (topic, key)
        old = self.state.pop(k2, None)
        if old is not None:
            self._sub(old)
        self.state[k2] = frame
        self._add(frame)
        self.wake.set()

    def put_snapshot(self, topic: str, frame: _Frame) -> None:
        """A fresh snapshot supersedes everything queued for ``topic`` (events, upserts, a pending resync)."""
        if self.closed:
            return
        self._purge_control(topic)
        self.pending_snapshots.add(topic)
        if is_event_topic(topic):
            self._purge_events(topic)
            self.events.append(frame)
            self.event_counts[topic] = 1
            self._add(frame)
            self.wake.set()
        else:
            self.put_state("*", frame)

    def put_control(self, frame: _Frame) -> None:
        if self.closed:
            return
        kind = frame.type
        if kind in ("pong", "heartbeat"):
            for old in [f for f in self.control if f.type == kind]:  # only the latest one is useful
                self.control.remove(old)
                self._sub(old)
        elif kind == "resync":
            if any(f.type == "resync" and f.topic == frame.topic for f in self.control):
                return
        elif kind == "error" and sum(1 for f in self.control if f.type == "error") >= ERROR_LIMIT:
            self.dropped_errors += 1
            if self.dropped_errors > CONTROL_LIMIT * 16:
                self.evict(CLOSE_POLICY, "too many invalid requests")
            return
        if len(self.control) >= CONTROL_LIMIT:
            self.evict(CLOSE_POLICY, "session frame queue overflow (client flood)")
            return
        self.control.append(frame)
        self._add(frame)
        self.wake.set()

    def purge_topic(self, topic: str) -> None:
        """Unsubscribe: nothing queued for ``topic`` may be sent after a later re-subscribe (L3)."""
        self._purge_events(topic)
        self._purge_control(topic)
        for k in [k for k in self.state if k[0] == topic]:
            self._sub(self.state.pop(k))
        self.pending_snapshots.discard(topic)
        self.seq.pop(topic, None)

    def _next(self) -> _Frame | None:
        f: _Frame | None = None
        if self.control:
            f = self.control.popleft()
        elif self.events:
            f = self.events.popleft()
            t = f.topic or ""
            self.event_counts[t] = max(0, self.event_counts.get(t, 1) - 1)
        elif self.state:
            _, f = self.state.popitem(last=False)
        if f is not None:
            self._sub(f)
        return f

    def _render(self, f: _Frame) -> str:
        ts = int(time.time() * 1000)
        mts = self.hub.market_ms()
        if f.topic is None:
            return f'{{"v":1,"type":"{f.type}","ts":{ts},"mts":{mts},"data":{f.data.decode()}}}'
        seq = self.seq.get(f.topic, 0) + 1
        self.seq[f.topic] = seq
        td = self.hub.trading_day()
        head = _enc.encode({"v": 1, "type": f.type, "topic": f.topic, "seq": seq, "ts": ts, "mts": mts, "td": td})
        return head[:-1].decode() + ',"data":' + f.data.decode() + "}"

    # ------------------------------------------------------------- closing
    def evict(self, code: int, reason: str) -> None:
        """Stop queueing and close the session with ``code`` (slow consumer, flood, revoked key)."""
        if self.closed:
            return
        self.closed = True
        self.close_code = code
        self.close_reason = reason
        self.events.clear()
        self.state.clear()
        self.control.clear()
        self.event_counts.clear()
        self.queued_bytes = 0
        self.wake.set()
        logger.warning("ws.evicted", client=self.principal.key_id, code=code, reason=reason)
        self.hub.evicted(self)

    async def sender(self) -> None:
        try:
            while not self.closed:
                await self.wake.wait()
                self.wake.clear()
                while not self.closed:
                    f = self._next()
                    if f is None:
                        break
                    if f.topic is not None and f.topic not in self.topics and f.type not in ("error", "resync"):
                        continue  # unsubscribed meanwhile
                    if f.type == "snapshot" and f.topic is not None:
                        self.pending_snapshots.discard(f.topic)
                    try:
                        await asyncio.wait_for(self.ws.send_text(self._render(f)), self.hub.send_timeout_s)
                    except TimeoutError:
                        self.evict(CLOSE_TRY_AGAIN, f"send blocked for {self.hub.send_timeout_s:g}s (slow consumer)")
                        return
                    self.sent += 1
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # disconnects surface as various exception types
            logger.debug("ws.send_stopped", client=self.principal.key_id, error=type(exc).__name__)
        finally:
            self.closed = True


def frame(type_: str, topic: str | None, data: Any) -> _Frame:
    return _Frame(type_, topic, _enc.encode(data))


def _open(reply: Reply, cls: type[Any]) -> Any:
    """``reply.open_as(cls)`` for an ok reply; None when it is not ok or carries something else."""
    if not reply.ok:
        return None
    try:
        return reply.open_as(cls)
    except (msgspec.DecodeError, msgspec.ValidationError, KeyError):
        logger.warning("ws.unexpected_reply", expected=cls.__name__, data_type=reply.data_type)
        return None


class WsHub:
    """Fan-out of read-model changes to WebSocket clients with per-topic pacing."""

    def __init__(
        self,
        readmodel: rm.ReadModel,
        *,
        keys: KeyManager,
        router: CommandRouter,
        config: Callable[[], FullConfig],
        clock: Clock,
        trading_day: Callable[[], str],
        clock_state: Callable[[], dict[str, Any]] | None = None,
        liveness: Callable[[str], str] | None = None,
        audit: Callable[..., None] | None = None,
        throttle: AuthThrottle | None = None,
        tick_s: float = 0.1,
        heartbeat_s: float = HEARTBEAT_S,
        auth_timeout_s: float = AUTH_TIMEOUT_S,
        equity_sample_s: float = rm.EQUITY_SAMPLE_S,
        revalidate_s: float = REVALIDATE_S,
        byte_budget: int = BYTE_BUDGET,
        send_timeout_s: float = SEND_TIMEOUT_S,
        max_message_bytes: int = MAX_MESSAGE_BYTES,
    ) -> None:
        self.rm = readmodel
        self._keys = keys
        self._router = router
        self._config = config
        self._clock = clock
        self.trading_day = trading_day
        self._clock_state = clock_state
        self._liveness = liveness
        self._audit = audit
        self.throttle = throttle
        self.firm_control: Callable[[], dict[str, Any] | None] | None = None  # set by the gateway
        self._tick_s = tick_s
        self._heartbeat_s = heartbeat_s
        self._auth_timeout_s = auth_timeout_s
        self._equity_s = equity_sample_s
        self._revalidate_s = revalidate_s
        self.byte_budget = byte_budget
        self.send_timeout_s = send_timeout_s
        self.max_message_bytes = max_message_bytes
        self.clients: set[WsClient] = set()
        self.subs: dict[str, set[WsClient]] = {}
        self._dirty_keys: dict[str, set[str]] = {}
        self._dirty_snap: set[str] = set()
        self._last_flush: dict[str, float] = {}
        self._orders_seeded: dict[str, str] = {}
        self._equity_seeded: dict[str, str] = {}
        self._events_seeded: str | None = None
        self._backtests_seeded = False
        self._task: asyncio.Task[None] | None = None
        self._closing: set[asyncio.Task[None]] = set()
        self.evictions = 0
        self.revoked = 0
        readmodel.add_listener(self.on_change)

    def market_ms(self) -> int:
        return self._clock.now_ns() // 1_000_000

    # ------------------------------------------------------------ lifecycle
    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.ensure_future(self._run())
            self._task.set_name("ws-hub")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
            self._task = None
        for c in list(self.clients):
            c.closed = True
            c.wake.set()
            with contextlib.suppress(Exception):
                await asyncio.wait_for(c.ws.close(code=1001), 2.0)
        for t in list(self._closing):
            t.cancel()
        if self._closing:
            await asyncio.gather(*self._closing, return_exceptions=True)
        self.clients.clear()
        self.subs.clear()

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        next_hb = loop.time() + self._heartbeat_s
        next_equity = loop.time() + self._equity_s
        next_check = loop.time() + self._revalidate_s
        while True:
            await asyncio.sleep(self._tick_s)
            now = loop.time()
            try:
                self.flush(now)
                if now >= next_equity:
                    next_equity = now + self._equity_s
                    for acct, point in self.rm.sample_equity():
                        self._event(f"equity:{acct}", point, "upsert")
                if now >= next_hb:
                    next_hb = now + self._heartbeat_s
                    self.broadcast_heartbeat()
                if now >= next_check:
                    next_check = now + self._revalidate_s
                    self.revalidate()  # key expiry (M8)
            except Exception:
                logger.exception("ws.hub_tick_failed")

    # ------------------------------------------------------ change routing
    def on_change(self, ch: rm.Change) -> None:
        k = ch.kind
        if k == rm.TICK:
            self._mark_key("quotes", ch.key)
        elif k == rm.SNAPSHOT:
            self._mark_key("accounts", ch.key)
            self._mark_snap(f"positions:{ch.key}")
            self._mark_snap("compare")
        elif k == rm.STATE:
            self._mark_key("accounts", ch.key)
            self._event("events", self.rm.state_change_item(ch.obj))
        elif k == rm.RISK_EVENT:
            self._event("events", self.rm.risk_event_item(ch.obj))
        elif k == rm.MD_STATUS:
            self._mark_snap("feeds")
        elif k == rm.MD_EVENT:
            self._event("feeds", to_builtins(ch.obj))
        elif k == rm.FIRM:
            self._mark_snap("firm")
        elif k == rm.FIRM_CONTROL:
            self._event("firm", to_builtins(ch.obj))
            self._mark_snap("firm")  # the snapshot carries the resulting control state
        elif k == rm.ORDER:
            self._event(f"orders:{ch.key}", to_builtins(ch.obj), "upsert")
        elif k == rm.HEARTBEAT:
            self._mark_snap("system")
        elif k == rm.SERVICE:
            self._event("system", {"kind": "service", **to_builtins(ch.obj)})
            self._mark_snap("system")
        elif k in (rm.EOD, rm.EOD_WARNING, rm.SETTLED, rm.REPORT, rm.DAYROLL):
            self._event("eod", to_builtins(ch.obj))
            if k == rm.DAYROLL:
                self._on_roll(ch.obj)
        elif k == rm.BACKTEST:
            self._mark_key("backtests", ch.key)

    def _on_roll(self, roll: Any) -> None:
        """A shard rolled to a new trading day: day-scoped topics start over for its accounts."""
        self.resnapshot("events")
        for acct in getattr(roll, "accounts", []) or []:
            self.invalidate_orders(acct)
            self._equity_seeded.pop(acct, None)
            self.resnapshot(f"orders:{acct}")
            self.resnapshot(f"equity:{acct}")

    def mark_snapshot(self, topic: str) -> None:
        """Push a fresh snapshot of ``topic`` at its next pacing slot."""
        self._mark_snap(topic)

    def mark_accounts(self, accounts: list[str]) -> None:
        """Re-push account items (owner liveness changed: ``stale`` / ``age_ms``)."""
        for a in accounts:
            self._mark_key("accounts", a)
        self._mark_snap("compare")

    def system_event(self, data: dict[str, Any]) -> None:
        """A ``system`` event frame (kill switch, config 2PC alerts)."""
        self._event("system", data)
        self._mark_snap("system")

    def _mark_key(self, topic: str, key: str | None) -> None:
        if key is None or not self.subs.get(topic):
            return
        self._dirty_keys.setdefault(topic, set()).add(key)

    def _mark_snap(self, topic: str) -> None:
        if self.subs.get(topic):
            self._dirty_snap.add(topic)

    def _event(self, topic: str, data: Any, type_: str = "event") -> None:
        clients = self.subs.get(topic)
        if not clients:
            return
        f = frame(type_, topic, data)
        for c in list(clients):
            c.put_event(f)

    def resnapshot(self, topic: str) -> None:
        """Push a fresh snapshot of ``topic`` to its subscribers (after a read-model repair)."""
        clients = self.subs.get(topic)
        if not clients:
            return
        f = frame("snapshot", topic, self.snapshot_data_sync(topic))
        for c in list(clients):
            c.put_snapshot(topic, f)

    def invalidate_orders(self, account_id: str) -> None:
        """The next ``orders:{acct}`` subscription queries the shard again."""
        self._orders_seeded.pop(account_id, None)

    # -------------------------------------------------------------- pacing
    def _due(self, topic: str, now: float) -> bool:
        interval = INTERVAL_S.get(topic_family(topic), 1.0)
        return now - self._last_flush.get(topic, -1e9) >= interval

    def flush(self, now: float | None = None) -> None:
        """Push throttled state/snapshot topics whose interval elapsed."""
        now = asyncio.get_running_loop().time() if now is None else now
        for topic in list(self._dirty_keys):
            if not self._due(topic, now):
                continue
            keys = self._dirty_keys.pop(topic)
            self._last_flush[topic] = now
            clients = self.subs.get(topic)
            if not clients:
                continue
            for key in sorted(keys):
                item = self._item(topic, key)
                if item is None:
                    continue
                f = frame("upsert", topic, item)
                for c in list(clients):
                    c.put_state(key, f)
        for topic in list(self._dirty_snap):
            if not self._due(topic, now):
                continue
            self._dirty_snap.discard(topic)
            self._last_flush[topic] = now
            clients = self.subs.get(topic)
            if not clients:
                continue
            f = frame("snapshot", topic, self.snapshot_data_sync(topic))
            for c in list(clients):
                c.put_state("*", f)

    def _item(self, topic: str, key: str) -> Any:
        if topic == "quotes":
            t = self.rm.ticks.get(key)
            return self.rm.quote_item(t) if t is not None else None
        if topic == "accounts":
            return self._named(self.rm.account_summary(key))
        if topic == "backtests":
            bt = self.rm.backtests.get(key)
            return to_builtins(bt) if bt is not None else None
        return None

    def _named(self, item: dict[str, Any] | None) -> dict[str, Any] | None:
        """Add the display name of accounts.yaml to an account item."""
        if item is not None:
            spec = self._config().accounts.accounts.get(str(item.get("account_id")))
            if spec is not None:
                item["name"] = spec.name
        return item

    def broadcast_heartbeat(self) -> None:
        f = _Frame("heartbeat", None, b"null")
        for c in list(self.clients):
            c.put_control(f)

    # ------------------------------------------------------------ snapshots
    def valid_topic(self, topic: str) -> bool:
        if topic in STATIC_TOPICS:
            return True
        for p in KEYED_PREFIXES:
            if topic.startswith(p):
                return self._router.has_account(topic[len(p):])
        return False

    def system_items(self) -> list[dict[str, Any]]:
        """``system`` snapshot: one ``kind="service"`` item per instance + the ``kind="clock"`` item."""
        items: list[dict[str, Any]] = [{"kind": "service", **x} for x in self.rm.system_items(self._liveness)]
        if self._clock_state is not None:
            items.append({"kind": "clock", "instance": "sys.clock", **self._clock_state()})
        return items

    def snapshot_data_sync(self, topic: str) -> Any:
        """Snapshot of a topic from the read model only (no bus requests)."""
        fam = topic_family(topic)
        if topic == "quotes":
            return self.rm.quote_items()
        if topic == "feeds":
            return to_builtins(self.rm.md_status) if self.rm.md_status is not None else None
        if topic == "accounts":
            return [self._named(x) for x in self.rm.account_summaries()]
        if fam == "positions":
            snap = self.rm.snapshots.get(topic.split(":", 1)[1])
            return to_builtins(snap.positions) if snap is not None else []
        if fam == "orders":
            acct = topic.split(":", 1)[1]
            return to_builtins(self.rm.orders_of(acct, self._account_td(acct)))
        if fam == "equity":
            return self.rm.equity_curve(topic.split(":", 1)[1])
        if topic == "events":
            return self.rm.event_items(trading_day=self.trading_day(), limit=SNAPSHOT_EVENTS)
        if topic == "compare":
            snaps = [self.rm.snapshots[a] for a in sorted(self.rm.snapshots)]
            return build_compare_view(snaps, self._config().risk_rules.rules, self.rm.liveness_of)
        if topic == "firm":
            if self.rm.firm is None:
                return None
            control = self.firm_control() if self.firm_control is not None else self.rm.firm_control_state()
            return {**to_builtins(self.rm.firm), "control": control}
        if topic == "system":
            return self.system_items()
        if topic == "eod":
            return to_builtins(self.rm.eod_recent())
        if topic == "backtests":
            return to_builtins(sorted(self.rm.backtests.values(), key=lambda b: b.submitted_ns, reverse=True))
        return None

    def _account_td(self, account_id: str) -> str:
        snap = self.rm.snapshots.get(account_id)
        return snap.trading_day if snap is not None else self.trading_day()

    async def snapshot_data(self, topic: str) -> Any:
        """Snapshot including one-time history backfills (orders, equity, events) over the bus."""
        fam = topic_family(topic)
        if fam == "orders":
            await self._seed_orders(topic.split(":", 1)[1])
        elif fam == "equity":
            await self._seed_equity(topic.split(":", 1)[1])
        elif topic == "events":
            await self._seed_events()
        elif topic == "backtests":
            await self._seed_backtests()
        return self.snapshot_data_sync(topic)

    async def _seed_orders(self, acct: str) -> None:
        td = self._account_td(acct)
        if self._orders_seeded.get(acct) == td:
            return
        try:
            reply = await self._router.to_account(
                acct, "risk.query.orders", QueryOrders(account_id=acct, trading_day=td), timeout_ms=2000, retries=0
            )
        except UnknownAccount:
            return
        if reply.ok:
            batch = _open(reply, OrderBatch)
            if batch is not None:
                self.rm.merge_orders(acct, batch.items, trading_day=td)
            self._orders_seeded[acct] = td
        else:
            logger.debug("ws.seed_orders_failed", account=acct, code=reply.code)

    async def _seed_equity(self, acct: str) -> None:
        td = self._account_td(acct)
        if self._equity_seeded.get(acct) == td:
            return
        try:
            reply = await self._router.to_account(
                acct,
                "risk.query.metrics",
                QueryMetrics(account_id=acct, trading_day=td, step_s=int(self._equity_s)),
                timeout_ms=2000,
                retries=0,
            )
        except UnknownAccount:
            return
        if reply.ok:
            series = _open(reply, MetricSeries)
            if series is not None:
                self.rm.merge_equity(acct, td, series.points)
            self._equity_seeded[acct] = td
        else:
            logger.debug("ws.seed_equity_failed", account=acct, code=reply.code)

    async def _seed_events(self) -> None:
        """Backfill the day's risk events from every shard once (L9: after a restart the FACT
        snapshot holds only the last ``risk.event`` per shard)."""
        td = self.trading_day()
        if self._events_seeded == td:
            return
        results = await self._router.fan_out(self._router.risk_instances(), "risk.query.alerts",
                                             QueryAlerts(trading_day=td), timeout_ms=2000, retries=0)
        complete = True
        for res in results:
            batch = _open(res.reply, RiskEventBatch)
            if batch is None:
                complete = False
                continue
            self.rm.merge_risk_events(batch.items)
        if complete:
            self._events_seeded = td

    async def _seed_backtests(self) -> None:
        """Backfill the job list from clearing-worker once (``backtest.status`` is LV: only live progress)."""
        if self._backtests_seeded:
            return
        reply = await self._router.request(CLEARING, "backtest.list", BacktestListRequest(), timeout_ms=2000,
                                           retries=0)
        lst = _open(reply, BacktestList)
        if lst is None:
            return
        for bt in lst.items:
            self.rm.backtests.setdefault(bt.run_id, bt)  # live updates already in the model are newer
        self._backtests_seeded = True

    async def send_snapshot(self, client: WsClient, topic: str) -> None:
        client.pending_snapshots.add(topic)  # a resync while this one is being built is ignored
        data = await self.snapshot_data(topic)
        if client.closed or topic not in client.topics:
            client.pending_snapshots.discard(topic)
            return
        client.put_snapshot(topic, frame("snapshot", topic, data))

    # ------------------------------------------------------------- sessions
    async def serve(self, ws: WebSocket) -> None:
        """Run one WebSocket session (the ``/ws`` endpoint)."""
        await ws.accept()
        host = ws.client.host if ws.client is not None else ""
        started = time.perf_counter()
        auth = await self._authenticate(ws, host)
        latency_us = int((time.perf_counter() - started) * 1e6)
        if auth is None:
            self._audit_ws(None, CLOSE_UNAUTHORIZED, codes.UNAUTHORIZED, latency_us, host)
            return
        principal, key_hash = auth
        self._audit_ws(principal, 101, codes.OK, latency_us, host)
        client = WsClient(ws=ws, principal=principal, hub=self, key_hash=key_hash, host=host)
        self.clients.add(client)
        client.task = asyncio.ensure_future(client.sender())
        try:
            while not client.closed:
                text = await self._receive(ws)
                if text is None:
                    break
                if len(text) > self.max_message_bytes:
                    client.evict(CLOSE_TOO_BIG, f"message larger than {self.max_message_bytes} bytes")
                    break
                await self._on_op(client, text)
        finally:
            client.closed = True
            client.wake.set()
            await self._terminate(client)

    def evicted(self, client: WsClient) -> None:
        """Called by :meth:`WsClient.evict`: drop the client now, close its socket in the background."""
        self.evictions += 1
        self._drop(client)
        task = asyncio.ensure_future(self._terminate(client))
        self._closing.add(task)
        task.add_done_callback(self._closing.discard)

    async def _terminate(self, client: WsClient) -> None:
        if client.terminating:
            return
        client.terminating = True
        self._drop(client)
        task = client.task
        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        ws = client.ws
        if ws.application_state == WebSocketState.DISCONNECTED or ws.client_state == WebSocketState.DISCONNECTED:
            return
        code = client.close_code or 1000
        with contextlib.suppress(Exception):
            if code == CLOSE_UNAUTHORIZED:
                await asyncio.wait_for(ws.send_text(self._session_frame(
                    "error", {"code": codes.UNAUTHORIZED, "message": client.close_reason})), 2.0)
            await asyncio.wait_for(ws.close(code=code, reason=client.close_reason[:100]), self.send_timeout_s)

    def _session_frame(self, type_: str, data: Any) -> str:
        return _enc.encode({"v": 1, "type": type_, "ts": int(time.time() * 1000), "mts": self.market_ms(),
                            "data": data}).decode()

    @staticmethod
    async def _receive(ws: WebSocket) -> str | None:
        """Next text (or UTF-8 binary) message; None once the client is gone."""
        try:
            msg = await ws.receive()
        except (WebSocketDisconnect, RuntimeError):
            return None
        if msg.get("type") == "websocket.disconnect":
            return None
        text = msg.get("text")
        if text is None:
            data = msg.get("bytes") or b""
            text = data.decode("utf-8", errors="replace")
        return str(text)

    async def _authenticate(self, ws: WebSocket, host: str) -> tuple[Principal, str] | None:
        if self.throttle is not None:
            wait = self.throttle.blocked(host)
            if wait > 0:
                await self._reject(ws, f"too many failed authentications, retry in {wait:.0f}s", codes.BUSY)
                return None
        presented = ws.query_params.get("token")
        if not presented:
            try:
                text = await asyncio.wait_for(self._receive(ws), self._auth_timeout_s)
            except TimeoutError:
                await self._reject(ws, f"no auth within {self._auth_timeout_s:g}s")
                return None
            if text is None:
                return None
            try:
                msg = msgspec.json.decode(text) if len(text) <= self.max_message_bytes else None
            except msgspec.DecodeError:
                msg = None
            if not isinstance(msg, dict) or msg.get("op") != "auth" or not isinstance(msg.get("key"), str):
                await self._reject(ws, 'the first message must be {"op":"auth","key":...}')
                return None
            presented = msg["key"]
        principal = self._keys.authenticate(presented)
        if principal is None or not principal.can(Perm.READ):
            if self.throttle is not None:
                self.throttle.failure(host)
            await self._reject(ws, "invalid API key")
            return None
        return principal, hash_key(presented)

    async def _reject(self, ws: WebSocket, message: str, code: str = codes.UNAUTHORIZED) -> None:
        with contextlib.suppress(Exception):
            await ws.send_text(self._session_frame("error", {"code": code, "message": message}))
            await ws.close(code=CLOSE_UNAUTHORIZED, reason="unauthorized")

    def _audit_ws(self, principal: Principal | None, status: int, code: str, latency_us: int, host: str) -> None:
        if self._audit is not None:
            try:
                self._audit(principal, status, code, latency_us, host)
            except Exception:
                logger.exception("ws.audit_failed")

    def _drop(self, client: WsClient) -> None:
        self.clients.discard(client)
        for t in list(client.topics):
            subs = self.subs.get(t)
            if subs is not None:
                subs.discard(client)
                if not subs:
                    del self.subs[t]

    def revalidate(self) -> int:
        """Close the sessions whose key was revoked, disabled or expired (4401); returns how many (M8)."""
        n = 0
        for c in list(self.clients):
            if c.closed:
                continue
            p = self._keys.authenticate_hash(c.key_hash)
            if p is None or not p.can(Perm.READ):
                n += 1
                self.revoked += 1
                c.evict(CLOSE_UNAUTHORIZED, "API key revoked, disabled or expired")
            elif p != c.principal:
                c.principal = p
        return n

    def _error(self, client: WsClient, code: str, message: str, **extra: Any) -> None:
        client.put_control(frame("error", None, {"code": code, "message": message, **extra}))

    async def _on_op(self, client: WsClient, text: str) -> None:
        try:
            msg = msgspec.json.decode(text)
        except msgspec.DecodeError:
            self._error(client, codes.BAD_REQUEST, "invalid JSON")
            return
        if not isinstance(msg, dict):
            self._error(client, codes.BAD_REQUEST, "expected a JSON object")
            return
        op = msg.get("op")
        topics = msg.get("topics")
        if op in ("sub", "unsub", "resync"):
            if not isinstance(topics, list) or not all(isinstance(t, str) for t in topics):
                self._error(client, codes.BAD_REQUEST, "topics must be a list of strings")
                return
            if len(topics) > MAX_TOPICS_PER_OP:
                self._error(client, codes.BAD_REQUEST, f"at most {MAX_TOPICS_PER_OP} topics per op")
                return
        if op == "sub":
            await self._sub(client, topics)  # type: ignore[arg-type]
        elif op == "unsub":
            for t in topics:  # type: ignore[union-attr]
                client.topics.discard(t)
                client.purge_topic(t)
                subs = self.subs.get(t)
                if subs is not None:
                    subs.discard(client)
                    if not subs:
                        del self.subs[t]
        elif op == "resync":
            for t in topics:  # type: ignore[union-attr]
                if t in client.topics and t not in client.pending_snapshots:
                    await self.send_snapshot(client, t)
        elif op == "ping":
            ts = msg.get("ts")
            client.put_control(frame("pong", None, {"ts": ts if isinstance(ts, (int, float, str)) else None}))
        elif op == "auth":
            pass  # already authenticated
        else:
            self._error(client, codes.BAD_REQUEST, "unknown op")

    async def _sub(self, client: WsClient, topics: list[str]) -> None:
        unknown: list[str] = []
        for t in dict.fromkeys(topics):
            if not self.valid_topic(t):
                unknown.append(t[:64])
                continue
            if t in client.topics:
                continue
            if len(client.topics) >= MAX_TOPICS_PER_CLIENT:
                self._error(client, codes.BAD_REQUEST, f"at most {MAX_TOPICS_PER_CLIENT} subscriptions per session")
                break
            client.topics.add(t)
            self.subs.setdefault(t, set()).add(client)
            await self.send_snapshot(client, t)
            if client.closed:
                return
        if unknown:
            self._error(client, codes.BAD_REQUEST, "unknown topics", topics=unknown[:20])

    # --------------------------------------------------------------- status
    def stats(self) -> dict[str, Any]:
        return {
            "clients": len(self.clients),
            "subscriptions": {t: len(c) for t, c in sorted(self.subs.items())},
            "queued": sum(len(c.events) + len(c.state) + len(c.control) for c in self.clients),
            "queued_bytes": sum(c.queued_bytes for c in self.clients),
            "overflows": sum(c.overflows for c in self.clients),
            "evictions": self.evictions,
            "revoked": self.revoked,
        }


__all__ = [
    "AUTH_TIMEOUT_S",
    "BYTE_BUDGET",
    "CLOSE_POLICY",
    "CLOSE_TOO_BIG",
    "CLOSE_TRY_AGAIN",
    "CLOSE_UNAUTHORIZED",
    "CONTROL_LIMIT",
    "HEARTBEAT_S",
    "INTERVAL_S",
    "MAX_MESSAGE_BYTES",
    "MAX_TOPICS_PER_OP",
    "QUEUE_LIMIT",
    "STATIC_TOPICS",
    "WsClient",
    "WsHub",
    "frame",
    "is_event_topic",
]
