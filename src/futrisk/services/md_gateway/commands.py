"""md-gateway commands (02 §6.2): md.settle_inputs, md.events, md.sources, md.source.set.

Heavy work (raw shard flush, canonical build, Parquet reads, settlement computation) runs
on the recorder thread so the event loop keeps serving ticks and heartbeats.
``md.source.set`` is idempotent by ``command_id`` (``rt.idempotent``: check, the
``SOURCE_STATE`` event with its actor and the reply are one writer transaction); that
event is also the persisted manual pin restored at start-up.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec
from sqlalchemy import Connection

from futrisk.db.processed import request_hash
from futrisk.messages import codes, topics
from futrisk.messages.base import Msg
from futrisk.messages.enums import MdEventKind
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.md import (
    MdEventBatch,
    MdEventsQuery,
    SettleInput,
    SettleInputsBatch,
    SettleInputsRequest,
    SourceControl,
    SourcesRequest,
)
from futrisk.services.md_gateway import store
from futrisk.services.md_gateway.recording import build_canonical, read_canonical
from futrisk.services.md_gateway.settle import compute_settle_input

if TYPE_CHECKING:
    from futrisk.bus.rpc_common import RequestContext
    from futrisk.services.md_gateway.service import MdGatewayService


def register_commands(svc: MdGatewayService) -> None:
    """Register the 02 §6.2 handlers on the runtime's ROUTER."""
    rt = svc.rt
    cmds = _Commands(svc)
    rt.add_handler("md.settle_inputs", cmds.settle_inputs, payload_type=SettleInputsRequest)
    rt.add_handler("md.events", cmds.events, payload_type=MdEventsQuery)
    rt.add_handler("md.sources", cmds.sources, payload_type=SourcesRequest)
    rt.add_handler("md.source.set", cmds.source_set, payload_type=SourceControl)


class _Commands:
    def __init__(self, svc: MdGatewayService) -> None:
        self.svc = svc

    async def settle_inputs(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        """Flush raw shards, build the canonical day file, compute and persist versioned inputs (03 §4.1)."""
        assert isinstance(payload, SettleInputsRequest)
        svc = self.svc
        td = payload.trading_day
        if len(td) != 8 or not td.isdigit():
            return err_reply(codes.BAD_REQUEST, f"bad trading_day {td!r}")
        await svc.flush_recording()
        layout = svc.layout
        sources = list(svc.priorities)
        prio = {str(s): p for s, p in svc.priorities.items()}
        nxt = svc.sessions.next_trading_day(td)
        now = svc.clock.now_ns()
        wanted = list(payload.instruments) if payload.instruments else None
        day_close = svc.day_close_ns(td)
        spill = list(svc.spill_dirs)

        def compute() -> list[SettleInput]:
            path = build_canonical(layout.ticks_raw_dir, layout.ticks_canonical_dir, td, sources, prio, next_day=nxt, spill_dirs=spill)
            df = read_canonical(path)
            if not len(df):
                return []
            df["instrument_id"] = df["instrument_id"].astype(str)
            ids = wanted if wanted is not None else sorted(df["instrument_id"].unique())
            out: list[SettleInput] = []
            for iid in ids:
                sub = df[df["instrument_id"] == iid]
                if not len(sub) or not svc.registry.has(iid):
                    continue
                templates = svc.sessions_of(iid)
                close = svc.sessions.day_close_ns(templates, td) if templates else day_close
                item = compute_settle_input(
                    sub, instrument_id=iid, trading_day=td, registry=svc.registry, computed_ns=now, day_close_ns=close
                )
                if item is not None:
                    out.append(item)
            return out

        items = await svc.recorder.run(compute)

        def persist(conn: Connection) -> list[SettleInput]:
            result: list[SettleInput] = []
            for item in items:
                prev = store.latest_settle_input(conn, td, item.instrument_id)
                if prev is not None and store.same_settle_values(prev, item):
                    result.append(prev)
                    continue
                new = msgspec.structs.replace(item, version=(prev.version + 1) if prev is not None else 1)
                store.insert_settle_input(conn, new)
                result.append(new)
            return result

        assert svc.db is not None
        saved = await svc.db.run_write(persist)
        return ok_reply(SettleInputsBatch(items=saved))

    async def events(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, MdEventsQuery)
        svc = self.svc
        assert svc.db is not None
        await svc.db.run_write(lambda c: None)  # barrier: events emitted so far are committed
        items = await svc.db.run_read(lambda c: store.query_events(c, payload.trading_day, payload.kinds, payload.instrument_id))
        return ok_reply(MdEventBatch(items=items))

    def sources(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        return ok_reply(self.svc.status())

    async def source_set(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        """Manual EXCLUDE / INCLUDE, idempotent by ``command_id`` (02 §6.2, 03 §7 人工控制); the pin is persisted."""
        assert isinstance(payload, SourceControl)
        svc = self.svc
        if payload.action not in ("EXCLUDE", "INCLUDE"):
            return err_reply(codes.BAD_REQUEST, f"action must be EXCLUDE or INCLUDE, got {payload.action!r}")
        if payload.source not in svc.trust.sources:
            return err_reply(codes.BAD_REQUEST, f"source {payload.source} is not enabled")
        if not payload.reason.strip() or not payload.actor.strip():
            return err_reply(codes.BAD_REQUEST, "reason and actor are required (audit)")
        processed = svc.processed
        db = svc.db
        assert processed is not None and db is not None
        h = request_hash(payload)
        # one command at a time: the dedupe check, the in-memory change and the persisted record stay consistent
        async with svc.command_lock:
            chk = await db.run_write(lambda c: processed.check(c, payload.command_id, h))
            if chk.reply is not None:
                return chk.reply  # DUPLICATE (duplicate=True) or CONFLICT: nothing is applied twice
            change = svc.trust.manual(payload.source, payload.action, payload.actor, payload.reason)
            reply = ok_reply(svc.status())
            ev = svc.events.build(
                MdEventKind.SOURCE_STATE,
                source=payload.source,
                from_state=str(change.from_state),
                to_state=str(change.to_state),
                detail={
                    **change.detail,
                    "manual": True,
                    "action": payload.action,
                    "reason": payload.reason,
                    "actor": payload.actor,
                    "command_id": payload.command_id,
                    "changed": change.from_state != change.to_state,
                },
            )
            td = ev.trading_day
            rt = svc.rt

            def tx(conn: Connection) -> Reply:
                store.insert_event(conn, ev, actor=payload.actor)
                rt.append_fact(conn, topics.MD_EVENT, ev, td=td, corr_id=ev.event_id)
                return reply

            result = await rt.idempotent(
                db, command, ctx.requester, payload.command_id, payload, tx, trading_day=td, received_ns=ctx.received_ns
            )
            svc.events.counts[str(MdEventKind.SOURCE_STATE)] = svc.events.counts.get(str(MdEventKind.SOURCE_STATE), 0) + 1
            svc.update_state()
            return result


__all__ = ["register_commands"]
