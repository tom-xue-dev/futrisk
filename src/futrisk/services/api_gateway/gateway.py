"""Gateway: the dependency container of the HTTP/WS layer plus REST idempotency (02 §9.6, 03 §4.0).

``Idempotency-Key`` (02 §9.6): every POST/DELETE except ``POST /orders`` must
carry a UUID ``Idempotency-Key``; it is forwarded as the command's
``command_id`` (``movement_id`` for cash) and deduplicated in
``processed_commands``:

* same key, same request (method, path, caller, body) -> the first answer is
  replayed with ``"duplicate": true`` (also while the first one is in flight);
* same key, different request -> ``409 CONFLICT``;
* answers with a retryable code (``TIMEOUT``/``UNAVAILABLE``/``BUSY``/
  ``RISK_BUSY``) or ``INTERNAL`` are not recorded, so the client may retry with
  the same key (the downstream service deduplicates by the same ``command_id``)
  -- except where the downstream side cannot deduplicate or a re-run could
  override a newer decision: the ``/demo/*`` sim proxies and the kill switch
  record every outcome (``record_all``), so a client retry gets the recorded
  answer instead of acting twice.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any

from loguru import logger
from starlette.requests import Request
from starlette.responses import Response

from futrisk.common.clock import Clock, bj_to_ns, ns_to_bj, parse_td, td_str
from futrisk.common.config import FullConfig
from futrisk.common.paths import HomeLayout
from futrisk.db import request_hash
from futrisk.messages import codes
from futrisk.messages.enums import Severity
from futrisk.messages.envelope import Reply
from futrisk.services.api_gateway.auth import AuthThrottle, KeyManager, Principal, SyncReport
from futrisk.services.api_gateway.clockjump import ClockJumpOrchestrator
from futrisk.services.api_gateway.config2pc import ConfigCoordinator
from futrisk.services.api_gateway.killswitch import MAX_ATTEMPTS, RETRY_INTERVAL_S, KillSwitchManager
from futrisk.services.api_gateway.notifier import Notifier
from futrisk.services.api_gateway.ports import SimControl
from futrisk.services.api_gateway.readmodel import ReadModel
from futrisk.services.api_gateway.repair import ReadModelRepair
from futrisk.services.api_gateway.rest import ApiError, ApiResult, decode_json, encode_json
from futrisk.services.api_gateway.router import TRADE_GATEWAY, CommandRouter, TargetResult, UnknownAccount
from futrisk.services.api_gateway.store import ApiStore, AuditRecord, KillSwitchRow
from futrisk.services.api_gateway.wshub import WsHub

JSON_TYPE = "application/json"
_STATUS_PARAM = ";http_status="
_NOT_RECORDED = codes.RETRYABLE | {codes.INTERNAL}


def result_to_reply(res: ApiResult) -> Reply:
    """Store an ApiResult as a Reply in ``processed_commands`` (data as JSON bytes, HTTP status override kept)."""
    data_type = JSON_TYPE if res.status_override is None else f"{JSON_TYPE}{_STATUS_PARAM}{res.status_override}"
    return Reply(
        ok=res.ok,
        code=res.code,
        message=res.message,
        data_type=data_type,
        data=encode_json(res.data),
        retry_after_ms=res.retry_after_ms,
    )


def reply_to_result(reply: Reply, *, duplicate: bool) -> ApiResult:
    data = decode_json(reply.data) if reply.data else None
    status: int | None = None
    if reply.data_type and _STATUS_PARAM in reply.data_type:
        try:
            status = int(reply.data_type.split(_STATUS_PARAM, 1)[1])
        except ValueError:
            status = None
    return ApiResult(
        ok=reply.ok,
        code=reply.code,
        message=reply.message or "",
        data=data,
        retry_after_ms=reply.retry_after_ms,
        duplicate=duplicate,
        status_override=status,
    )


def valid_idempotency_key(key: str) -> bool:
    try:
        uuid.UUID(key)
    except (ValueError, AttributeError, TypeError):
        return False
    return True


class Idempotency:
    """REST ``Idempotency-Key`` dedupe on ``processed_commands``."""

    def __init__(self, store: ApiStore, trading_day: Callable[[], str], clock: Clock) -> None:
        self._store = store
        self._td = trading_day
        self._clock = clock
        self._inflight: dict[str, asyncio.Future[tuple[str, ApiResult]]] = {}
        self.replays = 0
        self.conflicts = 0

    async def execute(
        self,
        *,
        key: str,
        requester: str,
        command: str,
        fingerprint: dict[str, Any],
        run: Callable[[], Awaitable[ApiResult]],
        record_all: bool = False,
    ) -> ApiResult:
        h = request_hash(fingerprint)
        while True:
            pending = self._inflight.get(key)
            if pending is None:
                break
            # a request with this key is being processed: wait for it (no await between check and claim below)
            try:
                prev_hash, prev = await asyncio.shield(pending)
            except Exception:
                continue  # the first attempt crashed without recording: claim the key ourselves
            return self._replay(key, h, prev_hash, prev)
        # claim the key first, then look at the database: concurrent duplicates wait on ``fut``,
        # later ones find the committed record (it is written before the claim is released)
        fut: asyncio.Future[tuple[str, ApiResult]] = asyncio.get_running_loop().create_future()
        self._inflight[key] = fut
        try:
            entry = await self._store.lookup_processed(key)
            if entry is not None:
                prev = reply_to_result(entry.reply(), duplicate=False)
                fut.set_result((entry.request_hash, prev))
                return self._replay(key, h, entry.request_hash, prev)
            try:
                res = await run()
            except ApiError as exc:
                res = exc.result
            if record_all or res.code not in _NOT_RECORDED:
                recorded = await self._store.record_processed(
                    command_id=key,
                    command=command,
                    requester=requester,
                    request_hash=h,
                    reply=result_to_reply(res),
                    trading_day=self._td(),
                    received_ns=self._clock.now_ns(),
                )
                if not recorded:
                    logger.warning("idempotency.race", key=key)
            fut.set_result((h, res))
            return res
        except BaseException as exc:
            if not fut.done():
                fut.set_exception(exc)
                fut.exception()  # mark retrieved
            raise
        finally:
            self._inflight.pop(key, None)

    def _replay(self, key: str, h: str, prev_hash: str, prev: ApiResult) -> ApiResult:
        if prev_hash != h:
            self.conflicts += 1
            return ApiResult.failure(codes.CONFLICT, f"Idempotency-Key {key} was used for a different request")
        self.replays += 1
        return ApiResult(
            ok=prev.ok,
            code=prev.code,
            message=prev.message,
            data=prev.data,
            retry_after_ms=prev.retry_after_ms,
            duplicate=True,
            status_override=prev.status_override,
        )


def ws_audit_writer(store: ApiStore, clock: Clock) -> Callable[..., None]:
    """``api_audit`` row per WebSocket session (method ``WS``, path ``/ws`` without the query; 03 §4.5)."""

    def write(principal: Principal | None, status: int, code: str, latency_us: int, client_ip: str = "") -> None:
        rec = AuditRecord(
            ts_ns=clock.now_ns(),
            key_id=principal.key_id if principal else None,
            role=principal.role.value if principal else None,
            method="WS",
            path="/ws",
            http_status=status,
            result_code=code,
            latency_us=latency_us,
            client_ip=client_ip,
        )
        try:
            store.submit_audit(rec)
        except RuntimeError:  # database closed during shutdown
            logger.debug("audit.ws_skipped")

    return write


def parse_market_time(at: str | int | None) -> int | None:
    """``/demo/clock`` ``at``: epoch ns (int or digits) or ISO 8601 (naive = Beijing market time)."""
    if at is None or at == "":
        return None
    if isinstance(at, int):
        return at
    s = at.strip()
    if s.isdigit():
        return int(s)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ApiError(codes.BAD_REQUEST, f"bad market time {at!r}: use ISO 8601 or epoch ns") from exc
    return bj_to_ns(dt)


@dataclass
class Gateway:
    """Everything the REST/WS handlers need; built by the service (or by tests with fakes)."""

    instance: str
    layout: HomeLayout
    config: Callable[[], FullConfig]
    clock: Clock
    store: ApiStore
    keys: KeyManager
    readmodel: ReadModel
    router: CommandRouter
    hub: WsHub
    coordinator: ConfigCoordinator
    clockjump: ClockJumpOrchestrator
    sim: SimControl
    notifier: Notifier | None = None
    state: Callable[[], str] = lambda: "RUNNING"
    liveness: Callable[[str], str] | None = None
    epoch: str = ""
    synced: Callable[[], dict[str, bool]] | None = None
    repair: ReadModelRepair | None = None
    auth_throttle: AuthThrottle = field(default_factory=AuthThrottle)
    home_id: str = ""
    shard_ready: Callable[[str], bool] | None = None  # the shard's current incarnation was seeded while RUNNING
    killswitch_interval_s: float = RETRY_INTERVAL_S
    killswitch_attempts: int = MAX_ATTEMPTS
    idempotency: Idempotency = field(init=False)
    killswitch: KillSwitchManager = field(init=False)

    def __post_init__(self) -> None:
        if not self.home_id:
            self.home_id = self.layout.home_id()  # /health identity: the CLI never sends a key to another home
        self.idempotency = Idempotency(self.store, self.trading_day, self.clock)
        self.killswitch = KillSwitchManager(
            store=self.store,
            router=self.router,
            clock=self.clock,
            targets=self.killswitch_targets,
            interval_s=self.killswitch_interval_s,
            max_attempts=self.killswitch_attempts,
        )
        self.killswitch.on_event = self.system_event
        self.killswitch.on_failed = self._kill_switch_failed
        self.readmodel.liveness_of = self.account_liveness
        self.hub.throttle = self.auth_throttle
        self.hub.firm_control = self.firm_control_state
        if self.router.peer_state is None:
            self.router.peer_state = self.liveness
        if self.coordinator.alert is None:
            self.coordinator.alert = self.config_alert

    async def aclose(self) -> None:
        """Stop the background work owned by the gateway (kill-switch retries, repairs)."""
        await self.killswitch.close()
        if self.repair is not None:
            await self.repair.close()

    # ----------------------------------------------------------------- time
    def trading_day(self) -> str:
        """The current trading day: the shards' (they roll it, ``sys.dayroll``), else the calendar's.

        A live risk shard's heartbeat is authoritative (after the roll to D+1 every
        default of the API and the WS ``td`` follow at once, even before the
        calendar day changes). Without a live shard, the newer of the last day a
        shard reported and the calendar day (market time; night -> next day).
        """
        liveness = self.liveness
        shard, live = self.readmodel.shard_trading_day(
            (lambda i: liveness(i) == "UP") if liveness is not None else None)
        if shard is not None and live:
            return shard
        cal = self.calendar_trading_day()
        return max(shard, cal) if shard is not None else cal

    def calendar_trading_day(self) -> str:
        """Trading day of the market clock by the exchange calendar (night session -> next day)."""
        now = self.clock.now_ns()
        try:
            return td_str(self.config().calendar.current_or_next_trading_day(now))
        except LookupError:
            return td_str(ns_to_bj(now).date())

    def recent_trading_days(self, n: int) -> list[str]:
        """The last ``n`` trading days up to the current one (newest first)."""
        cal = self.config().calendar
        today = parse_td(self.trading_day())
        out: list[str] = []
        d: date | None = today
        if not cal.is_trading_day(today):
            try:
                d = cal.prev_trading_day(today)
            except LookupError:
                d = None
        while d is not None and len(out) < n:
            out.append(td_str(d))
            try:
                d = cal.prev_trading_day(d)
            except LookupError:
                d = None
        return out

    def account_td(self, account_id: str) -> str:
        snap = self.readmodel.snapshots.get(account_id)
        return snap.trading_day if snap is not None else self.trading_day()

    def clock_state(self) -> dict[str, Any]:
        """The market clock as a ``ClockState``-like dict (02 §7.9)."""
        return {
            "mode": str(getattr(self.clock, "mode", "REAL")),
            "offset_ns": int(getattr(self.clock, "offset_ns", 0)),
            "version": int(getattr(self.clock, "version", 0)),
            "market_now_ns": self.clock.now_ns(),
        }

    def demo_mode(self) -> bool:
        return self.config().app.mode == "demo"

    # ------------------------------------------------------------- liveness
    def instance_liveness(self, instance: str) -> dict[str, Any]:
        """``{"stale", "age_ms"}`` of an instance: not UP (heartbeats), ms since it was last heard (None: never)."""
        state = self.liveness(instance) if self.liveness is not None else "UP"
        seen = self.readmodel.heartbeats.get(instance)
        age = (self.clock.monotonic_ns() - seen.recv_mono_ns) // 1_000_000 if seen is not None else None
        return {"stale": state != "UP", "age_ms": age}

    def account_liveness(self, account_id: str) -> dict[str, Any]:
        """``{"stale", "age_ms"}``: is the account's owning shard up (and, after a restart, seeded again once
        it reported RUNNING), and since when was it last heard (M4)."""
        try:
            owner = self.router.owner_of(account_id)
        except UnknownAccount:
            return {"stale": False, "age_ms": None}
        live = self.instance_liveness(owner)
        if not live["stale"] and self.shard_ready is not None and not self.shard_ready(owner):
            live["stale"] = True  # a restarted shard still starting up: its accounts are not live yet
        return live

    def kill_switch_retry(self) -> dict[str, Any]:
        """``/system/status.kill_switch_retry``: the desired state and the targets still pending / failed."""
        row = self.killswitch.row
        if row is None:
            return {"pending": [], "failed": [], "desired": None, "updated_ns": None, "retrying": False}
        return {
            "pending": self.killswitch.pending(),
            "failed": self.killswitch.failed(),
            "desired": {"on": row.desired_on, "reason": row.reason, "actor": row.actor, "command_id": row.command_id},
            "updated_ns": row.updated_ns,
            "retrying": self.killswitch.retrying,
        }

    def on_peer_change(self, peer: str, state: str) -> None:
        """A peer went UP/DOWN: re-push the account items of a shard (``stale`` changed)."""
        if peer in self.router.risk_instances():
            self.hub.mark_accounts(self.router.accounts_of(peer))

    # ------------------------------------------------------ system events
    def system_event(self, data: dict[str, Any]) -> None:
        """WS ``system`` event (kill switch progress, config 2PC alerts)."""
        self.hub.system_event(data)
        if data.get("kind") == "kill_switch":
            self.hub.mark_snapshot("firm")  # the firm snapshot's ``control.manual`` changed

    def firm_control_state(self) -> dict[str, Any] | None:
        """The firm kill switch by layer: ``{kill_switch, manual, rule}`` (None before anything is known).

        * ``manual`` -- the operator's desired state kept by the kill-switch manager (a
          manual kill switch is fanned out by api-gateway and never travels on
          ``risk.control.firm``): ``{active, actor, reason, ts_ns, command_id, updated_ns,
          targets, pending, failed, confirmed, retrying}``; before the first command, a
          legacy MANUAL fact of ``risk.control.firm`` if one was seen;
        * ``rule`` -- the coordinator's rule layer (``RULE_STATE``): ``{active, restrictions, ...}``;
        * ``kill_switch`` -- MANUAL or RULE (the desired manual state; ``/system/status.kill_switch``
          is the coordinator's view of what is in force).
        """
        seen = self.readmodel.firm_control_state()
        row = self.killswitch.row
        manual: dict[str, Any] | None
        if row is not None:
            st = row.to_dict()
            manual = {"active": row.desired_on, "actor": row.actor, "reason": row.reason, "ts_ns": row.requested_ns,
                      "command_id": row.command_id, "updated_ns": row.updated_ns, "targets": st["targets"],
                      "pending": st["pending"], "failed": st["failed"], "confirmed": st["confirmed"],
                      "retrying": self.killswitch.retrying}
        else:
            manual = seen["manual"] if seen is not None else None
        rule = seen["rule"] if seen is not None else None
        if manual is None and rule is None:
            return None
        active = bool(manual and manual.get("active")) or bool(rule and rule.get("active"))
        return {"kill_switch": active, "manual": manual, "rule": rule}

    def killswitch_targets(self) -> list[str]:
        """``firm.kill_switch`` goes to every risk shard and trade-gateway (02 §6.3)."""
        return [*self.router.risk_instances(), TRADE_GATEWAY]

    async def _kill_switch_failed(self, row: KillSwitchRow, target: str) -> None:
        if self.notifier is None:
            return
        t = row.targets[target]
        state = "ON" if row.desired_on else "OFF"
        await self.notifier.notify(
            kind="KILL_SWITCH_FAILED",
            severity=Severity.CRITICAL,
            notify_key=f"KILL_SWITCH|{row.command_id}|{target}",
            message=f"kill switch {state} 未被 {target} 确认（{t.code}，{t.attempts} 次尝试）：请人工处理",
            target=target,
            command_id=row.command_id,
            desired_on=row.desired_on,
            code=t.code,
            attempts=t.attempts,
            actor=row.actor,
        )

    async def config_alert(self, kind: str, detail: dict[str, Any]) -> None:
        """2PC alert hook: WS ``system`` event; ``CONFIG_FAILED`` is also notified CRITICAL."""
        self.system_event({"kind": "config", "event": kind, **detail})
        if kind == "CONFIG_FAILED" and self.notifier is not None:
            version = detail.get("config_version")
            lagging = [str(x) for x in detail.get("lagging") or []]
            await self.notifier.notify(
                kind="CONFIG_FAILED",
                severity=Severity.CRITICAL,
                notify_key=f"CONFIG|{version}|FAILED",
                message=f"配置版本 {version} 提交后未在 30s 内收敛：{', '.join(lagging) or '?'}",
                config_version=version,
                lagging=lagging,
            )

    # ----------------------------------------------------------------- keys
    async def resync_keys(self, app: Any) -> SyncReport:
        """``app.yaml`` keys -> api.db, then close WebSocket sessions whose key is no longer valid (M8)."""
        report = await self.keys.sync(app)
        self.hub.revalidate()
        return report

    def check_key(self, presented: str | None, client: str) -> tuple[Principal | None, ApiResult | None]:
        """Authenticate with the failed-attempt throttle: (principal, None) or (None, error result)."""
        wait = self.auth_throttle.blocked(client)
        if wait > 0:
            return None, ApiResult.failure(codes.BUSY, "too many failed authentications from this address",
                                           retry_after_ms=int(wait * 1000) + 1)
        principal = self.keys.authenticate(presented)
        if principal is None:
            self.auth_throttle.failure(client)
            return None, ApiResult.failure(codes.UNAUTHORIZED, "missing or invalid X-API-Key")
        return principal, None

    # --------------------------------------------------------------- audits
    @staticmethod
    def set_audit(request: Request, *, command: str | None, command_id: str | None, target: str | None) -> None:
        request.state.audit = {"command": command, "command_id": command_id, "target": target}

    # ------------------------------------------------------------ mutations
    async def mutate(
        self,
        request: Request,
        principal: Principal,
        *,
        command: str,
        target: str | None,
        body: Any,
        run: Callable[[str], Awaitable[ApiResult]],
        record_all: bool = False,
    ) -> Response:
        """Idempotent mutation: validate the Idempotency-Key, dedupe, run, answer.

        ``record_all`` records every outcome, retryable ones included (see the module docstring).
        """
        key = request.headers.get("Idempotency-Key", "").strip()
        self.set_audit(request, command=command, command_id=key or None, target=target)
        if not key:
            res = ApiResult.failure(codes.BAD_REQUEST, "the Idempotency-Key header (UUID) is required")
        elif not valid_idempotency_key(key):
            res = ApiResult.failure(codes.BAD_REQUEST, "Idempotency-Key must be a UUID")
        else:
            fingerprint = {
                "method": request.method,
                "path": request.url.path,
                "requester": principal.key_id,
                "body": body,
            }
            res = await self.idempotency.execute(
                key=key, requester=principal.key_id, command=command, fingerprint=fingerprint,
                run=lambda: run(key), record_all=record_all,
            )
        request.state.result_code = res.code
        return res.response()

    @staticmethod
    def fanout_result(results: list[TargetResult], data_extra: dict[str, Any] | None = None) -> ApiResult:
        """Per-target report of a fan-out, always HTTP 200 (a target's code is never the HTTP status).

        ``ok`` only when every target confirmed; ``code`` = the worst target code.
        """
        ok, code, message = CommandRouter.summarize(results)
        data: dict[str, Any] = {"targets": [r.to_dict() for r in results], "all_ok": ok}
        if data_extra:
            data.update(data_extra)
        return ApiResult(ok=ok, code=code, message=message, data=data, status_override=200)


__all__ = [
    "Gateway",
    "Idempotency",
    "parse_market_time",
    "reply_to_result",
    "result_to_reply",
    "valid_idempotency_key",
    "ws_audit_writer",
]
