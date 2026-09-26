"""Helpers shared by the bus tests."""

from __future__ import annotations

import time
from collections.abc import Callable

from futrisk.bus.subscriber import Subscriber
from futrisk.db.outbox import OutboxRow
from futrisk.messages import encode
from futrisk.messages.common import Ping
from futrisk.messages.envelope import Envelope
from futrisk.runtime.testing import free_tcp_port


def addr() -> str:
    return f"tcp://127.0.0.1:{free_tcp_port()}"


def wait_until(pred: Callable[[], bool], timeout: float = 3.0, step: float = 0.005) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(step)
    return pred()


def warm_up(send: Callable[[], None], sub: Subscriber, timeout: float = 3.0) -> list[Envelope]:
    """Publish until the SUB socket receives something (ZeroMQ slow-joiner)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        send()
        got = sub.recv(20)
        if got is not None:
            return [got, *sub.drain()]
    raise TimeoutError("subscription not established")


def outbox_row(topic: str, seq: int, *, stream: str = "stream-1", outbox_id: int | None = None) -> OutboxRow:
    return OutboxRow(
        outbox_id=outbox_id if outbox_id is not None else seq,
        topic=topic,
        topic_seq=seq,
        type="Ping",
        v=1,
        epoch="pub-epoch",
        td="20260928",
        corr_id=None,
        payload=encode(Ping()),
        created_ns=1_000 + seq,
        stream=stream,
    )
