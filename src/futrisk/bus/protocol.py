"""The in-process ``Bus`` interface of 02 §10.

Implemented by :class:`futrisk.bus.inproc.InprocBus` (backtests, unit tests:
synchronous, deterministic delivery). Networked services use the runtimes in
:mod:`futrisk.runtime` (publish via the publisher / outbox, subscribe via SUB
sockets, request via DEALER clients); their request API is asynchronous or
non-blocking, so they do not implement this synchronous protocol directly.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Protocol

from futrisk.bus.rpc_common import Handler
from futrisk.messages.base import Msg
from futrisk.messages.envelope import Envelope, Reply

EnvelopeHandler = Callable[[Envelope], None]


class Bus(Protocol):
    def publish(
        self,
        topic: str,
        payload: Msg,
        *,
        corr_id: str | None = None,
        fact: bool = False,
        td: str | None = None,
    ) -> int:
        """Publish ``payload`` on ``topic``; returns the sequence number."""
        ...

    def subscribe(self, publisher: str, prefixes: Sequence[str], handler: EnvelopeHandler) -> None:
        """Deliver every envelope of ``publisher`` matching ``prefixes`` (exact-topic semantics)."""
        ...

    def request(self, target: str, command: str, payload: Msg, *, timeout_ms: int | None = None) -> Reply:
        """Send ``command`` to ``target`` and return its reply."""
        ...

    def serve(self, handlers: Mapping[str, Handler]) -> None:
        """Register command handlers of this instance."""
        ...


__all__ = ["Bus", "EnvelopeHandler"]
