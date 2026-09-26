"""Standard control commands implemented by every internal service (02 §6.1).

================  ==================  ==========================================
command           request             reply data
================  ==================  ==========================================
``ctl.ping``      ``Ping``            ``Pong{instance, state, ts_ns, home_id, pid}``
``ctl.status``    ``StatusRequest``   ``Heartbeat`` (instant value)
``ctl.snapshot``  ``SnapshotRequest`` ``SnapshotBatch`` from the publisher LV cache
``ctl.replay``    ``ReplayRequest``   ``ReplayBatch`` from the outbox; ``NOT_FOUND``
                                      once the start of the range has been purged
                                      (the reply's ``ReplayBatch.next_seq`` is the
                                      first seq still stored)
``ctl.shutdown``  ``ShutdownRequest`` empty OK, then the service enters STOPPING
================  ==================  ==========================================

``ctl.config.*`` is handled by the runtime's config participant
(:mod:`futrisk.runtime.config2pc`). All handlers here are synchronous and fast;
``ctl.replay`` performs one small read on a ``query_only`` connection.

``Pong.home_id`` (the answering process' ``HomeLayout.home_id()``, ``None``
without a home) and ``Pong.pid`` let the CLI and the supervisor check that the
process owning a port belongs to their home (cross-home guard).
"""

from __future__ import annotations

import os
from collections.abc import Callable

from futrisk.bus.lvcache import LastValueCache
from futrisk.bus.rpc_common import Handler, RequestContext
from futrisk.common.clock import Clock
from futrisk.db.outbox import MAX_REPLAY_LIMIT, Outbox, ReplayNotFound
from futrisk.messages import codes
from futrisk.messages.base import Msg
from futrisk.messages.common import (
    Ping,
    Pong,
    ReplayBatch,
    ReplayRequest,
    ShutdownRequest,
    SnapshotRequest,
    StatusRequest,
)
from futrisk.messages.enums import ServiceState
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.system import Heartbeat
from futrisk.messages.topics import is_fact

CTL_PING = "ctl.ping"
CTL_STATUS = "ctl.status"
CTL_SNAPSHOT = "ctl.snapshot"
CTL_REPLAY = "ctl.replay"
CTL_SHUTDOWN = "ctl.shutdown"
CTL_CONFIG_PREPARE = "ctl.config.prepare"
CTL_CONFIG_COMMIT = "ctl.config.commit"
CTL_CONFIG_ABORT = "ctl.config.abort"


class ControlHandlers:
    """``ctl.ping``/``status``/``snapshot``/``replay``/``shutdown`` bound to runtime callbacks."""

    def __init__(
        self,
        *,
        instance: str,
        clock: Clock,
        state: Callable[[], ServiceState],
        status: Callable[[], Heartbeat],
        lv_cache: Callable[[], LastValueCache | None],
        outbox: Callable[[], Outbox | None],
        on_shutdown: Callable[[ShutdownRequest], None],
        home_id: str | None = None,
        pid: int | None = None,
    ) -> None:
        self._instance = instance
        self._clock = clock
        self._state = state
        self._status = status
        self._lv_cache = lv_cache
        self._outbox = outbox
        self._on_shutdown = on_shutdown
        self._home_id = home_id
        self._pid = os.getpid() if pid is None else pid

    def handlers(self) -> dict[str, tuple[Handler, type[Msg]]]:
        """``{command: (handler, payload_type)}`` for registration on a server."""
        return {
            CTL_PING: (self.ping, Ping),
            CTL_STATUS: (self.status, StatusRequest),
            CTL_SNAPSHOT: (self.snapshot, SnapshotRequest),
            CTL_REPLAY: (self.replay, ReplayRequest),
            CTL_SHUTDOWN: (self.shutdown, ShutdownRequest),
        }

    def ping(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        return ok_reply(
            Pong(
                instance=self._instance,
                state=str(self._state()),
                ts_ns=self._clock.now_ns(),
                home_id=self._home_id,
                pid=self._pid,
            )
        )

    def status(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        return ok_reply(self._status())

    def snapshot(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, SnapshotRequest)
        if not payload.prefixes or any(not p for p in payload.prefixes):
            return err_reply(codes.BAD_REQUEST, "prefixes must be a non-empty list of non-empty strings")
        cache = self._lv_cache()
        if cache is None:
            return err_reply(codes.NOT_FOUND, f"{self._instance} publishes nothing")
        batch = cache.snapshot(
            payload.prefixes,
            as_of_ns=self._clock.now_ns(),
            include_fact=payload.include_fact,
            cursor=payload.next_cursor,
            limit=payload.limit,
        )
        return ok_reply(batch)

    def replay(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, ReplayRequest)
        if not is_fact(payload.topic):
            return err_reply(codes.BAD_REQUEST, f"{payload.topic} is not a FACT topic")
        if payload.from_seq < 1 or (payload.to_seq is not None and payload.to_seq < payload.from_seq):
            return err_reply(codes.BAD_REQUEST, "invalid seq range")
        outbox = self._outbox()
        if outbox is None:
            return err_reply(codes.NOT_FOUND, f"{self._instance} has no outbox")
        limit = max(1, min(payload.limit, MAX_REPLAY_LIMIT))
        try:
            page = outbox.replay(payload.topic, payload.from_seq, payload.to_seq, limit)
        except ReplayNotFound as exc:
            # next_seq tells the subscriber where replay can resume (first seq still stored);
            # to_seq + 1 when nothing of the requested range is left.
            resume = exc.first_available if exc.first_available is not None else exc.to_seq + 1
            data = ReplayBatch(topic=payload.topic, envelopes=[], next_seq=resume, complete=False)
            return err_reply(codes.NOT_FOUND, str(exc), data=data)
        return ok_reply(page.to_batch())

    def shutdown(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, ShutdownRequest)
        self._on_shutdown(payload)
        return ok_reply(message=f"{self._instance} stopping")


__all__ = [
    "CTL_CONFIG_ABORT",
    "CTL_CONFIG_COMMIT",
    "CTL_CONFIG_PREPARE",
    "CTL_PING",
    "CTL_REPLAY",
    "CTL_SHUTDOWN",
    "CTL_SNAPSHOT",
    "CTL_STATUS",
    "ControlHandlers",
]
