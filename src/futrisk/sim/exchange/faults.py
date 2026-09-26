"""Fault injection registry of the virtual exchange (02 §9.5 fault table).

A fault is created by ``POST /control/faults {type, target?, params, duration_s?}``
and lives until ``duration_s`` elapses (monotonic clock), until it completes by itself
(one-shot faults) or until ``DELETE /control/faults/{fault_id}``. The feeds and the
broker *query* this registry on their hot paths; faults with side effects (disconnects,
restarts, force close, engine-level freeze) are applied through the
:class:`FaultHooks` implemented by :class:`~futrisk.sim.exchange.app.SimExchange`.

Targets use the lower-case source names ``feed_a`` / ``feed_b`` (02 §7.3 ``SourceId``).
Ratios are decimals (README §3.4): ``ratio: 0.2`` duplicates 20% of the messages,
``offset_pct: 0.05`` moves a bad tick by +5%.
"""

from __future__ import annotations

import math
import random
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Protocol

from futrisk.common.ids import uuid7
from futrisk.sim.exchange.errors import bad_request

FEED_A = "feed_a"
FEED_B = "feed_b"
FEEDS = (FEED_A, FEED_B)

FAULT_TYPES = (
    "FEED_DISCONNECT",
    "FEED_GAP",
    "FEED_DUPLICATE",
    "FEED_REORDER",
    "FEED_LATENCY",
    "FEED_FREEZE",
    "BAD_TICK",
    "MALFORMED",
    "SIM_RESTART",
    "BROKER_FORCE_CLOSE",
    "BROKER_DISCONNECT",
)

ACTIVE = "ACTIVE"
DONE = "DONE"
EXPIRED = "EXPIRED"
DELETED = "DELETED"


class FaultHooks(Protocol):
    """Side effects of faults, implemented by the exchange application."""

    def known_instrument(self, instrument_id: str) -> bool: ...

    async def fault_started(self, fault: Fault) -> None: ...

    async def fault_ended(self, fault: Fault) -> None: ...


@dataclass
class Fault:
    fault_id: str
    type: str
    target: str | None
    params: dict[str, Any]
    duration_s: float | None
    created_ns: int
    started_mono: float
    expires_mono: float | None
    state: str = ACTIVE
    detail: dict[str, Any] = field(default_factory=dict)
    rng: random.Random = field(default_factory=random.Random, repr=False)
    pending: set[str] = field(default_factory=set)  # BAD_TICK: feeds that still have to emit it
    ended_mono: float | None = None

    @property
    def active(self) -> bool:
        return self.state == ACTIVE

    def remaining_s(self, now: float | None = None) -> float | None:
        if self.expires_mono is None or not self.active:
            return None
        return max(0.0, self.expires_mono - (time.monotonic() if now is None else now))

    def to_dict(self) -> dict[str, Any]:
        return {
            "fault_id": self.fault_id,
            "type": self.type,
            "target": self.target,
            "params": self.params,
            "duration_s": self.duration_s,
            "state": self.state,
            "created_ns": self.created_ns,
            "remaining_s": self.remaining_s(),
            "detail": self.detail,
        }


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _num(p: dict[str, Any], key: str, default: float | None = None, *, lo: float | None = None,
         hi: float | None = None, lo_open: bool = False) -> float:
    if key not in p or p[key] is None:
        if default is None:
            raise bad_request(f"params.{key} is required")
        return default
    v = p[key]
    if isinstance(v, bool) or not isinstance(v, int | float) or not math.isfinite(float(v)):
        raise bad_request(f"params.{key} must be a number")
    f = float(v)
    if lo is not None and (f <= lo if lo_open else f < lo):
        raise bad_request(f"params.{key} must be {'>' if lo_open else '>='} {lo}")
    if hi is not None and f > hi:
        raise bad_request(f"params.{key} must be <= {hi}")
    return f


def _int(p: dict[str, Any], key: str, default: int | None = None, *, lo: int = 0) -> int:
    if key not in p or p[key] is None:
        if default is None:
            raise bad_request(f"params.{key} is required")
        return default
    v = p[key]
    if isinstance(v, bool) or not isinstance(v, int | float) or float(v) != int(v):
        raise bad_request(f"params.{key} must be an integer")
    if int(v) < lo:
        raise bad_request(f"params.{key} must be >= {lo}")
    return int(v)


def _choice(p: dict[str, Any], key: str, default: str, choices: tuple[str, ...]) -> str:
    v = p.get(key, default)
    if not isinstance(v, str) or v.lower() not in choices:
        raise bad_request(f"params.{key} must be one of {choices}")
    return v.lower()


def _str(p: dict[str, Any], key: str, *, required: bool = True) -> str | None:
    v = p.get(key)
    if v is None or v == "":
        if required:
            raise bad_request(f"params.{key} is required")
        return None
    if not isinstance(v, str):
        raise bad_request(f"params.{key} must be a string")
    return v


def _target(target: str | None, allowed: tuple[str | None, ...], default: str | None) -> str | None:
    t = target.lower() if isinstance(target, str) and target else None
    if t is None:
        t = default
    if t not in allowed:
        shown = [a if a is not None else "(none)" for a in allowed]
        raise bad_request(f"target must be one of {shown}")
    return t


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class FaultManager:
    """Active faults plus a short history; see the module docstring."""

    def __init__(self, hooks: FaultHooks, *, seed: int = 0, history: int = 50) -> None:
        self._hooks = hooks
        self._seed = seed
        self._counter = 0
        self._active: dict[str, Fault] = {}
        self._done: deque[Fault] = deque(maxlen=history)

    # ------------------------------------------------------------------ queries
    def get(self, fault_id: str) -> Fault | None:
        f = self._active.get(fault_id)
        if f is not None:
            return f
        return next((x for x in self._done if x.fault_id == fault_id), None)

    def active(self, type_: str | None = None, target: str | None = None) -> list[Fault]:
        return [
            f for f in self._active.values()
            if (type_ is None or f.type == type_) and (target is None or f.target == target)
        ]

    def first(self, type_: str, target: str | None = None) -> Fault | None:
        for f in self._active.values():
            if f.type == type_ and (target is None or f.target == target):
                return f
        return None

    def latency_ms(self, target: str) -> float:
        return max((f.params["ms"] for f in self.active("FEED_LATENCY", target)), default=0.0)

    def feed_frozen(self, target: str, instrument_id: str) -> bool:
        """``FEED_FREEZE`` aimed at one feed (engine-level freezes are applied by hooks)."""
        return any(
            f.params["instrument"] == instrument_id for f in self.active("FEED_FREEZE", target)
        )

    def engine_frozen(self) -> set[str]:
        return {f.params["instrument"] for f in self.active("FEED_FREEZE") if f.target is None}

    def take_bad_tick(self, target: str, instrument_id: str) -> Fault | None:
        """Consume a pending ``BAD_TICK`` for ``(target, instrument)`` (one-shot per feed)."""
        for f in list(self._active.values()):
            if f.type == "BAD_TICK" and f.params["instrument"] == instrument_id and target in f.pending:
                f.pending.discard(target)
                f.detail.setdefault("emitted", []).append(target)
                if not f.pending:
                    self.finish(f, DONE)
                return f
        return None

    def list(self, include_done: bool = False) -> list[dict[str, Any]]:
        out = [f.to_dict() for f in self._active.values()]
        if include_done:
            out.extend(f.to_dict() for f in self._done)
        return out

    # ------------------------------------------------------------------ lifecycle
    async def add(
        self,
        type_: str,
        target: str | None = None,
        params: dict[str, Any] | None = None,
        duration_s: float | None = None,
        *,
        created_ns: int = 0,
    ) -> Fault:
        """Validate, register and start a fault. Raises :class:`SimError` (400/404/409)."""
        if not isinstance(type_, str) or type_.upper() not in FAULT_TYPES:
            raise bad_request(f"unknown fault type {type_!r}; expected one of {list(FAULT_TYPES)}")
        t = type_.upper()
        if duration_s is not None and (not math.isfinite(duration_s) or duration_s < 0):
            raise bad_request("duration_s must be >= 0")
        tgt, clean, lifetime = self._validate(t, target, dict(params or {}), duration_s)
        self._counter += 1
        now = time.monotonic()
        f = Fault(
            fault_id=uuid7(),
            type=t,
            target=tgt,
            params=clean,
            duration_s=duration_s,
            created_ns=created_ns,
            started_mono=now,
            expires_mono=None if lifetime is None else now + lifetime,
            rng=random.Random(self._seed * 1_000_003 + self._counter),
        )
        if t == "BAD_TICK":
            f.pending = set(FEEDS) if tgt is None else {tgt}
        self._active[f.fault_id] = f
        try:
            await self._hooks.fault_started(f)
        except Exception:  # SimError (409 etc.) or a bug: the fault never became active
            self._active.pop(f.fault_id, None)
            raise
        if f.active and lifetime == 0.0:
            await self.complete(f, DONE)
        return f

    async def remove(self, fault_id: str) -> bool:
        f = self._active.get(fault_id)
        if f is None:
            return False
        await self.complete(f, DELETED)
        return True

    async def clear(self) -> None:
        for f in list(self._active.values()):
            await self.complete(f, DELETED)

    async def complete(self, f: Fault, state: str = DONE) -> None:
        """End a fault and run its end hook (idempotent)."""
        if not self.finish(f, state):
            return
        await self._hooks.fault_ended(f)

    def finish(self, f: Fault, state: str = DONE) -> bool:
        """End a fault without running hooks (hot-path completions). False if already ended."""
        if self._active.pop(f.fault_id, None) is None:
            return False
        f.state = state
        f.ended_mono = time.monotonic()
        self._done.append(f)
        return True

    async def reap(self) -> None:
        """Expire faults whose lifetime elapsed (called periodically)."""
        now = time.monotonic()
        for f in list(self._active.values()):
            if f.expires_mono is not None and now >= f.expires_mono:
                await self.complete(f, EXPIRED)

    # ------------------------------------------------------------------ validation
    def _validate(
        self, t: str, target: str | None, p: dict[str, Any], duration_s: float | None
    ) -> tuple[str | None, dict[str, Any], float | None]:
        """Return ``(target, clean params, lifetime_s)``; ``lifetime`` None = until deleted/done."""
        if t == "FEED_DISCONNECT":
            tgt = _target(target, FEEDS, None)
            refuse = _num(p, "refuse_reconnect_s", 0.0, lo=0.0)
            mode = _choice(p, "mode", "close", ("close", "silent"))
            if mode == "silent" and tgt != FEED_A:
                raise bad_request("mode=silent only applies to feed_a")
            if duration_s is not None:
                lifetime: float | None = duration_s
            elif refuse > 0:
                lifetime = refuse
            else:
                # feed_a/close without refusal: disconnect once; otherwise until deleted
                lifetime = 0.0 if (tgt == FEED_A and mode == "close") else None
            return tgt, {"refuse_reconnect_s": refuse, "mode": mode}, lifetime
        if t == "FEED_GAP":
            tgt = _target(target, (FEED_A,), FEED_A)
            return tgt, {"drop_count": _int(p, "drop_count", lo=1)}, duration_s
        if t == "FEED_DUPLICATE":
            tgt = _target(target, FEEDS, FEED_A)
            return tgt, {"ratio": _num(p, "ratio", lo=0.0, hi=1.0)}, duration_s
        if t == "FEED_REORDER":
            tgt = _target(target, FEEDS, FEED_A)
            return tgt, {"window": _int(p, "window", lo=2)}, duration_s
        if t == "FEED_LATENCY":
            tgt = _target(target, FEEDS, FEED_A)
            return tgt, {"ms": _num(p, "ms", lo=0.0)}, duration_s
        if t == "FEED_FREEZE":
            tgt = _target(target, (None, *FEEDS), None)
            inst = _str(p, "instrument")
            assert inst is not None
            if not self._hooks.known_instrument(inst):
                raise bad_request(f"unknown instrument {inst}")
            return tgt, {"instrument": inst}, duration_s
        if t == "BAD_TICK":
            tgt = _target(target, (None, *FEEDS), None)
            inst = _str(p, "instrument")
            assert inst is not None
            if not self._hooks.known_instrument(inst):
                raise bad_request(f"unknown instrument {inst}")
            off = _num(p, "offset_pct", lo=-0.99, hi=10.0)
            if off == 0:
                raise bad_request("params.offset_pct must be non-zero (ratio, e.g. 0.05 for +5%)")
            wv = p.get("with_volume", False)
            if not isinstance(wv, bool):
                raise bad_request("params.with_volume must be a boolean")
            return tgt, {"instrument": inst, "offset_pct": off, "with_volume": wv}, duration_s
        if t == "MALFORMED":
            tgt = _target(target, (FEED_B,), FEED_B)
            ratio = _num(p, "ratio", lo=0.0, hi=1.0)
            mode = _choice(p, "mode", "body", ("body", "record"))
            return tgt, {"ratio": ratio, "mode": mode}, duration_s
        if t == "SIM_RESTART":
            _target(target, (None,), None)
            return None, {}, 0.0
        if t == "BROKER_FORCE_CLOSE":
            _target(target, (None,), None)
            acct = _str(p, "account_id")
            inst = _str(p, "instrument_id")
            assert acct is not None and inst is not None
            if not self._hooks.known_instrument(inst):
                raise bad_request(f"unknown instrument {inst}")
            qty = _int(p, "qty", 0, lo=0)
            pd = p.get("posi_direction")
            if pd is not None and (not isinstance(pd, str) or pd.upper() not in ("LONG", "SHORT")):
                raise bad_request("params.posi_direction must be LONG or SHORT")
            clean: dict[str, Any] = {"account_id": acct, "instrument_id": inst, "qty": qty or None,
                                     "posi_direction": pd.upper() if isinstance(pd, str) else None}
            return None, clean, 0.0
        if t == "BROKER_DISCONNECT":
            _target(target, (None,), None)
            seconds = _num(p, "seconds", lo=0.0, lo_open=True)
            return None, {"seconds": seconds}, duration_s if duration_s is not None else seconds
        raise bad_request(f"unsupported fault type {t}")  # pragma: no cover
