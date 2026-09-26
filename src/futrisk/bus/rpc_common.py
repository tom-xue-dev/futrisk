"""Request/reply building blocks shared by servers and clients (02 §4.2, §4.7).

* Requests: DEALER sends one frame ``[envelope]`` with ``topic`` = command,
  ``type`` = payload type, ``seq`` = 0 and ``corr_id`` = request id (UUIDv7).
* Replies: ROUTER answers ``[identity, envelope]`` with ``topic`` =
  ``"<command>.reply"``, ``type`` = ``"Reply"`` and the same ``corr_id``.

A command handler has the signature ``handler(command, payload, ctx)`` and
returns a :class:`~futrisk.messages.envelope.Reply` (``None`` means an empty
OK), :data:`DEFERRED` to answer later with ``server.reply(ctx, reply)``, or an
awaitable of those (asyncio servers only).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Final

from futrisk.messages import codes
from futrisk.messages.base import Msg, encode, type_name, type_version
from futrisk.messages.envelope import PROTO, Envelope, Reply, encode_envelope


class _Deferred:
    """Sentinel type: the handler will reply later via ``server.reply(ctx, reply)``."""

    _instance: _Deferred | None = None

    def __new__(cls) -> _Deferred:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "DEFERRED"


DEFERRED: Final = _Deferred()


@dataclass(frozen=True, slots=True)
class RequestContext:
    """Everything needed to answer a request, now or later.

    Keep the context to reply later (e.g. ``order.submit`` after persist and
    route, ``eod.rollover`` after the ``settle.sod_snapshot`` sub-request).
    """

    command: str
    requester: str
    identity: bytes
    corr_id: str | None
    received_ns: int
    received_mono_ns: int
    envelope: Envelope

    @property
    def td(self) -> str | None:
        return self.envelope.td


type HandlerResult = Reply | _Deferred | None
type Handler = Callable[[str, Msg, RequestContext], HandlerResult | Awaitable[HandlerResult]]


@dataclass(frozen=True)
class HandlerSpec:
    handler: Handler
    payload_type: type[Msg] | None = None


def reply_topic(command: str) -> str:
    return f"{command}.reply"


def request_envelope(
    command: str,
    payload: Msg,
    *,
    src: str,
    epoch: str,
    ts_ns: int,
    corr_id: str,
    td: str | None = None,
) -> bytes:
    name = type_name(payload)
    return encode_envelope(
        Envelope(
            proto=PROTO,
            topic=command,
            type=name,
            v=type_version(name),
            src=src,
            epoch=epoch,
            seq=0,
            ts_ns=ts_ns,
            td=td,
            corr_id=corr_id,
            payload=encode(payload),
        )
    )


def reply_envelope(command: str, reply: Reply, *, src: str, epoch: str, ts_ns: int, corr_id: str | None) -> bytes:
    return encode_envelope(
        Envelope(
            proto=PROTO,
            topic=reply_topic(command),
            type="Reply",
            v=type_version("Reply"),
            src=src,
            epoch=epoch,
            seq=0,
            ts_ns=ts_ns,
            corr_id=corr_id,
            payload=encode(reply),
        )
    )


def unavailable(target: str, detail: str = "peer not connected") -> Reply:
    return Reply(ok=False, code=codes.UNAVAILABLE, message=f"{target}: {detail}")


def timeout(target: str, command: str, timeout_ms: int) -> Reply:
    return Reply(ok=False, code=codes.TIMEOUT, message=f"{target} {command}: no reply within {timeout_ms}ms")


def is_awaitable(obj: Any) -> bool:
    return hasattr(obj, "__await__")


__all__ = [
    "DEFERRED",
    "Handler",
    "HandlerResult",
    "HandlerSpec",
    "RequestContext",
    "reply_envelope",
    "reply_topic",
    "request_envelope",
    "timeout",
    "unavailable",
]
