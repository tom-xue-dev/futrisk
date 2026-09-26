"""ZeroMQ socket factories with the 02 §4.1/§4.9 options.

========  =========================================================
socket    options
========  =========================================================
PUB       SNDHWM (md-gateway 100k, others 10k); LINGER 1000ms for FACT
          publishers, 0 otherwise
SUB       RCVHWM (default 10k); LINGER 0
ROUTER    ROUTER_HANDOVER=1; RCVHWM 1k; LINGER 0
DEALER    IDENTITY = own instance id; IMMEDIATE=1; LINGER 0
========  =========================================================

Sockets are never shared across threads (02 §4.1): each factory is called by
the thread (or event loop) that will use the socket. The functions accept
both ``zmq.Context`` and ``zmq.asyncio.Context``.
"""

from __future__ import annotations

from typing import Any

import zmq

DEFAULT_SUB_HWM = 10_000
DEFAULT_ROUTER_HWM = 1_000
FACT_LINGER_MS = 1_000
RECONNECT_IVL_MS = 100
RECONNECT_IVL_MAX_MS = 1_000


def make_pub(ctx: zmq.Context[Any], address: str, *, sndhwm: int, linger_ms: int) -> zmq.Socket[Any]:
    sock: zmq.Socket[Any] = ctx.socket(zmq.PUB)
    sock.setsockopt(zmq.SNDHWM, sndhwm)
    sock.setsockopt(zmq.LINGER, linger_ms)
    try:
        sock.bind(address)
    except zmq.ZMQError:
        sock.close(0)
        raise
    return sock


def make_sub(ctx: zmq.Context[Any], *, rcvhwm: int = DEFAULT_SUB_HWM) -> zmq.Socket[Any]:
    sock: zmq.Socket[Any] = ctx.socket(zmq.SUB)
    sock.setsockopt(zmq.RCVHWM, rcvhwm)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.RECONNECT_IVL, RECONNECT_IVL_MS)
    sock.setsockopt(zmq.RECONNECT_IVL_MAX, RECONNECT_IVL_MAX_MS)
    return sock


def make_router(ctx: zmq.Context[Any], address: str, *, rcvhwm: int = DEFAULT_ROUTER_HWM) -> zmq.Socket[Any]:
    sock: zmq.Socket[Any] = ctx.socket(zmq.ROUTER)
    sock.setsockopt(zmq.ROUTER_HANDOVER, 1)
    sock.setsockopt(zmq.RCVHWM, rcvhwm)
    sock.setsockopt(zmq.LINGER, 0)
    try:
        sock.bind(address)
    except zmq.ZMQError:
        sock.close(0)
        raise
    return sock


def make_dealer(ctx: zmq.Context[Any], address: str, *, identity: str) -> zmq.Socket[Any]:
    sock: zmq.Socket[Any] = ctx.socket(zmq.DEALER)
    sock.setsockopt(zmq.IDENTITY, identity.encode("utf-8"))
    sock.setsockopt(zmq.IMMEDIATE, 1)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.RECONNECT_IVL, RECONNECT_IVL_MS)
    sock.setsockopt(zmq.RECONNECT_IVL_MAX, RECONNECT_IVL_MAX_MS)
    sock.connect(address)
    return sock


__all__ = [
    "DEFAULT_ROUTER_HWM",
    "DEFAULT_SUB_HWM",
    "FACT_LINGER_MS",
    "make_dealer",
    "make_pub",
    "make_router",
    "make_sub",
]
