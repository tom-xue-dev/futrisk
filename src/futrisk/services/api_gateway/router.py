"""CommandRouter: per-account routing to the owning risk shard and fan-out (01 §5.3.5, 02 §3, §4.1, §6.3).

* The shard of an account comes from ``accounts.yaml`` (``shard``); the
  instance of shard k is ``risk-engine-k`` (02 §3). A command for an account
  that is not configured is rejected locally; a command reaching the wrong
  shard would get ``NOT_OWNER``.
* ``firm.kill_switch`` is fanned out to every risk shard **and** trade-gateway,
  ``eod.force_unfreeze`` to every shard; every target answers separately and
  the per-target results are reported (02 §6.3, §9.6).
* Commands whose owner is only known by a shard (a break id, a plan id, an
  order unknown to the read model) are broadcast and the first answer that is
  not ``NOT_FOUND``/``NOT_OWNER`` wins (idempotent: the same key everywhere).
* Read-only REST requests run under a **read deadline** (set per GET request
  by the app, :func:`read_deadline`): every bus call made while it is active
  uses at most ``READ_TIMEOUT_MS`` per attempt and never outlives the deadline
  (a call that would start after it answers ``TIMEOUT`` at once). An
  ``UNAVAILABLE`` / ``BUSY`` answer of a peer that is not known to be down
  (e.g. a shard in its first milliseconds, "peer not connected") is retried a
  few times with a short backoff inside the deadline; a ``TIMEOUT`` is not.
  Commands (POST/DELETE) keep the bus retry policy of 02 §4.7 (same payload,
  same idempotency key).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable, Iterable, Iterator, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any

from futrisk.bus.timeouts import default_timeout_ms
from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.messages.base import Msg
from futrisk.messages.envelope import Reply
from futrisk.services.api_gateway.ports import BusFacade
from futrisk.services.api_gateway.rest import reply_data

TRADE_GATEWAY = "trade-gateway"
CLEARING = "clearing-worker"
MD_GATEWAY = "md-gateway"
SUPERVISOR = "supervisor"
_MISS = frozenset({codes.NOT_FOUND, codes.NOT_OWNER})

READ_TIMEOUT_MS = 5_000  # per bus call of a read-only REST request (review L1)
READ_DEADLINE_S = 12.0  # whole read-only REST request
READ_RETRIES = 3  # UNAVAILABLE / BUSY answers of reads: 100, 200, 400 ms backoff inside the deadline
READ_BACKOFF_S = 0.1
_READ_RETRYABLE = frozenset({codes.UNAVAILABLE, codes.BUSY, codes.RISK_BUSY})
_read_deadline: ContextVar[float | None] = ContextVar("apigw_read_deadline", default=None)

# how bad a target's code is when several targets answered (worst wins); retryable codes rank lowest
_RANK = {
    codes.INTERNAL: 100,
    codes.VERSION_UNSUPPORTED: 95,
    codes.UNAUTHORIZED: 90,
    codes.FORBIDDEN: 90,
    codes.BAD_REQUEST: 80,
    codes.PRECONDITION_FAILED: 75,
    codes.CONFLICT: 70,
    codes.NOT_OWNER: 65,
    codes.NOT_FOUND: 60,
    codes.UNAVAILABLE: 40,
    codes.TIMEOUT: 35,
    codes.RISK_BUSY: 30,
    codes.BUSY: 30,
    codes.OK: 0,
}


def _rank(code: str) -> int:
    return _RANK.get(code, 55 if code.startswith("REJECT_") else 50)


def worst_code(results: Iterable[str]) -> str:
    """The worst of several target codes (``OK`` when all are ``OK``); non-retryable failures rank first."""
    worst = codes.OK
    for c in results:
        if _rank(c) > _rank(worst):
            worst = c
    return worst


@contextlib.contextmanager
def read_deadline(seconds: float = READ_DEADLINE_S) -> Iterator[None]:
    """Bound every bus call made inside (same task / tasks it creates) by a read deadline (L1)."""
    token = _read_deadline.set(time.monotonic() + seconds)
    try:
        yield
    finally:
        _read_deadline.reset(token)


def _bounded(command: str, kw: dict[str, Any]) -> dict[str, Any] | None:
    """Apply the active read deadline to one call's kwargs; None when the deadline has passed."""
    deadline = _read_deadline.get()
    if deadline is None:
        return kw
    remaining_ms = int((deadline - time.monotonic()) * 1000)
    if remaining_ms <= 0:
        return None
    timeout = kw.get("timeout_ms") or default_timeout_ms(command)
    return {**kw, "timeout_ms": max(1, min(int(timeout), READ_TIMEOUT_MS, remaining_ms)), "retries": 0}


class UnknownAccount(KeyError):
    """The account is not configured in accounts.yaml."""


@dataclass
class TargetResult:
    """Answer of one fan-out target."""

    target: str
    reply: Reply

    @property
    def ok(self) -> bool:
        return self.reply.ok

    def to_dict(self) -> dict[str, Any]:
        return {
            "target": self.target,
            "ok": self.reply.ok,
            "code": self.reply.code,
            "message": self.reply.message or "",
            "duplicate": self.reply.duplicate,
            "data": reply_data(self.reply),
        }


class CommandRouter:
    """Routes commands to the instances owning them."""

    def __init__(self, bus: BusFacade, config: Callable[[], FullConfig]) -> None:
        self._bus = bus
        self._config = config
        self.peer_state: Callable[[str], str] | None = None  # liveness (UP / DOWN / UNKNOWN), set by the gateway

    @property
    def bus(self) -> BusFacade:
        return self._bus

    # ------------------------------------------------------------- lookup
    def risk_instances(self) -> list[str]:
        """Every configured risk-engine shard instance, e.g. ``["risk-engine-0", "risk-engine-1"]``."""
        return self._config().services.risk_instances()

    def coordinator(self) -> str:
        return self._config().services.coordinator()

    def accounts(self) -> list[str]:
        return sorted(self._config().accounts.accounts)

    def has_account(self, account_id: str) -> bool:
        return account_id in self._config().accounts.accounts

    def owner_of(self, account_id: str) -> str:
        """Instance owning ``account_id`` (raises :class:`UnknownAccount`)."""
        cfg = self._config()
        try:
            shard = cfg.accounts.shard_of(account_id)
        except KeyError:
            raise UnknownAccount(account_id) from None
        return cfg.services.shard_instance(shard)

    def accounts_of(self, instance: str) -> list[str]:
        cfg = self._config()
        return [a for a in sorted(cfg.accounts.accounts) if cfg.services.shard_instance(cfg.accounts.shard_of(a)) == instance]

    # ------------------------------------------------------------ sending
    async def request(self, target: str, command: str, payload: Msg, **kw: Any) -> Reply:
        attempt = 0
        while True:
            bounded = _bounded(command, kw)
            if bounded is None:
                return Reply(ok=False, code=codes.TIMEOUT, message=f"{target} {command}: request deadline passed")
            reply = await self._bus.request(target, command, payload, **bounded)
            deadline = _read_deadline.get()
            if deadline is None or reply.code not in _READ_RETRYABLE or attempt >= READ_RETRIES:
                return reply
            if self.peer_state is not None and self.peer_state(target) == "DOWN":
                return reply  # known dead: no point waiting
            delay = max(READ_BACKOFF_S * (2 ** attempt), (reply.retry_after_ms or 0) / 1000)
            if time.monotonic() + delay >= deadline:
                return reply
            attempt += 1
            await asyncio.sleep(delay)

    async def to_account(self, account_id: str, command: str, payload: Msg, **kw: Any) -> Reply:
        """Send to the shard owning ``account_id``."""
        return await self.request(self.owner_of(account_id), command, payload, **kw)

    async def fan_out(self, targets: Sequence[str], command: str, payload: Msg, **kw: Any) -> list[TargetResult]:
        """Send the same payload (same idempotency key) to every target concurrently."""
        replies = await asyncio.gather(*(self.request(t, command, payload, **kw) for t in targets))
        return [TargetResult(t, r) for t, r in zip(targets, replies, strict=True)]

    async def first_owner(self, targets: Sequence[str], command: str, payload: Msg, **kw: Any) -> TargetResult:
        """Broadcast; the first target not answering NOT_FOUND/NOT_OWNER wins (else the first miss)."""
        results = await self.fan_out(targets, command, payload, **kw)
        for res in results:
            if res.reply.code not in _MISS:
                return res
        return results[0] if results else TargetResult("", Reply(ok=False, code=codes.UNAVAILABLE, message="no targets"))

    @staticmethod
    def summarize(results: Sequence[TargetResult]) -> tuple[bool, str, str]:
        """(all ok, code, message) of a fan-out: the worst target code when any target failed."""
        failed = [r for r in results if not r.ok]
        if not failed:
            return True, codes.OK, f"{len(results)}/{len(results)} targets confirmed"
        names = ", ".join(f"{r.target}={r.reply.code}" for r in failed)
        code = worst_code(r.reply.code for r in failed)
        return False, code, f"{len(results) - len(failed)}/{len(results)} targets confirmed; failed: {names}"


__all__ = [
    "CLEARING",
    "MD_GATEWAY",
    "READ_DEADLINE_S",
    "READ_TIMEOUT_MS",
    "SUPERVISOR",
    "TRADE_GATEWAY",
    "CommandRouter",
    "TargetResult",
    "UnknownAccount",
    "read_deadline",
    "worst_code",
]
