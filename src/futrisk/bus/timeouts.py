"""Request timeouts and retry policy (02 §4.7).

=====================================  =========
command                                timeout
=====================================  =========
``order.*``                            500ms
``route.*``                            1s
``ctl.snapshot``, ``ctl.replay``       5s
``eod.*``, ``settle.*``                30s
``ctl.config.prepare``                 5s
others                                 2s
=====================================  =========

Only ``TIMEOUT``, ``UNAVAILABLE``, ``BUSY`` and ``RISK_BUSY`` are retried, at
most 3 times, with a delay of ``100ms * 2**n`` plus jitter (or the server's
``retry_after_ms`` when larger). Retries resend the same payload bytes, hence
the same idempotency key.
"""

from __future__ import annotations

import random

from futrisk.messages import codes

RETRYABLE: frozenset[str] = codes.RETRYABLE
MAX_RETRIES = 3
BACKOFF_BASE_MS = 100
JITTER_MS = 50
_RNG = random.Random()


def default_timeout_ms(command: str) -> int:
    """Default per-attempt timeout of ``command``."""
    if command.startswith("order."):
        return 500
    if command.startswith("route."):
        return 1_000
    if command in ("ctl.snapshot", "ctl.replay", "ctl.config.prepare"):
        return 5_000
    if command.startswith(("eod.", "settle.")):
        return 30_000
    return 2_000


def is_retryable(code: str) -> bool:
    return code in RETRYABLE


def retry_delay_ms(attempt: int, retry_after_ms: int | None = None, rng: random.Random | None = None) -> float:
    """Delay before retry number ``attempt`` (0-based): ``100ms * 2**attempt`` + jitter."""
    r = rng if rng is not None else _RNG
    delay: float = BACKOFF_BASE_MS * (1 << max(0, attempt)) + r.uniform(0, JITTER_MS)
    if retry_after_ms is not None and retry_after_ms > delay:
        delay = float(retry_after_ms)
    return delay


__all__ = [
    "BACKOFF_BASE_MS",
    "MAX_RETRIES",
    "RETRYABLE",
    "default_timeout_ms",
    "is_retryable",
    "retry_delay_ms",
]
