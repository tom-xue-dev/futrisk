"""Identifier helpers: time-ordered UUIDv7 strings and deterministic generators.

Services use :func:`uuid7` for idempotency keys and event ids (02 §4.7). The
pure core never calls it directly; it receives an :class:`IdGen` so that
backtests and unit tests can produce reproducible ids.
"""

from __future__ import annotations

import os
import threading
import time
import uuid
from typing import Protocol

_lock = threading.Lock()
_last_ms = 0
_counter = 0


def uuid7() -> str:
    """Return a UUIDv7 string (RFC 9562), monotonic within the process."""
    global _last_ms, _counter
    with _lock:
        ms = time.time_ns() // 1_000_000
        if ms <= _last_ms:
            ms = _last_ms
            _counter += 1
            if _counter > 0xFFF:
                ms += 1
                _counter = 0
        else:
            _counter = int.from_bytes(os.urandom(2), "big") & 0x3FF
        _last_ms = ms
        rand_b = int.from_bytes(os.urandom(8), "big") & ((1 << 62) - 1)
        value = (
            (ms & ((1 << 48) - 1)) << 80
            | 0x7 << 76
            | (_counter & 0xFFF) << 64
            | 0b10 << 62
            | rand_b
        )
    return str(uuid.UUID(int=value))


def new_epoch() -> str:
    """Process incarnation id, regenerated on every start (Envelope.epoch)."""
    return uuid7()


class IdGen(Protocol):
    """Id factory injected into the pure core."""

    def next_id(self) -> str: ...


class Uuid7Gen:
    """Production id generator."""

    def next_id(self) -> str:
        return uuid7()


class SeqIdGen:
    """Deterministic id generator for backtests and tests: ``{prefix}-{n:08d}``."""

    def __init__(self, prefix: str = "id") -> None:
        self._prefix = prefix
        self._n = 0

    def next_id(self) -> str:
        self._n += 1
        return f"{self._prefix}-{self._n:08d}"
