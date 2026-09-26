"""md-gateway commands (02 §6.2): md.settle_inputs, md.events, md.ticks, md.bars, md.sources, md.source.set.

Heavy work (raw shard flush, canonical build, Parquet reads, settlement computation)
runs on the recorder thread so the event loop keeps serving ticks and heartbeats.
``md.ticks`` keeps the day's raw shards in a cache that only reads new shards and
flushes the recording at most every ``ticks_flush_interval_s`` (paging through a range
never writes a tiny shard per page). ``md.source.set`` is idempotent by ``command_id``
(``rt.idempotent``: check, the ``SOURCE_STATE`` event with its actor and the reply are one
writer transaction); that event is also the persisted manual pin restored at start-up.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import msgspec
import pyarrow as pa
import pyarrow.compute as pc
from sqlalchemy import Connection

from futrisk.db.processed import request_hash
from futrisk.messages import codes, topics
from futrisk.messages.base import Msg
from futrisk.messages.enums import MdEventKind
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.md import (
    BarsBatch,
    BarsRequest,
    MdEventBatch,
    MdEventsQuery,
    SettleInput,
    SettleInputsBatch,
    SettleInputsRequest,
    SourceControl,
    SourcesRequest,
    TickBatch,
    TicksQuery,
)
from futrisk.services.md_gateway import store
from futrisk.services.md_gateway.recording import (
    ShardCache,
    build_canonical,
    day_shards,
    read_canonical,
    row_to_tick,
)
from futrisk.services.md_gateway.settle import compute_settle_input

if TYPE_CHECKING:
    from futrisk.bus.rpc_common import RequestContext
    from futrisk.services.md_gateway.service import MdGatewayService

TICKS_PAGE = 1500


def register_commands(svc: MdGatewayService) -> None:
    """Register the 02 §6.2 handlers on the runtime's ROUTER."""
    rt = svc.rt
    cmds = _Commands(svc)
    rt.add_handler("md.settle_inputs", cmds.settle_inputs, payload_type=SettleInputsRequest)
    rt.add_handler("md.events", cmds.events, payload_type=MdEventsQuery)
    rt.add_handler("md.ticks", cmds.ticks, payload_type=TicksQuery)
    rt.add_handler("md.bars", cmds.bars, payload_type=BarsRequest)
    rt.add_handler("md.sources", cmds.sources, payload_type=SourcesRequest)
    rt.add_handler("md.source.set", cmds.source_set, payload_type=SourceControl)


class _Commands:
    def __init__(self, svc: MdGatewayService) -> None:
        self.svc = svc
        self._shards = ShardCache()

    # ------------------------------------------------------------ settle inputs
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
        day_close = svc._day_close_ns(td)
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
                templates = svc._sessions_of(iid)
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
                new = _with_version(item, (prev.version + 1) if prev is not None else 1)
                store.insert_settle_input(conn, new)
                result.append(new)
            return result

        assert svc.db is not None
        saved = await svc.db.run_write(persist)
        return ok_reply(SettleInputsBatch(items=saved))

    # ------------------------------------------------------------------ events
    async def events(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, MdEventsQuery)
        svc = self.svc
        assert svc.db is not None
        await svc.db.run_write(lambda c: None)  # barrier: events emitted so far are committed
        items = await svc.db.run_read(lambda c: store.query_events(c, payload.trading_day, payload.kinds, payload.instrument_id))
        return ok_reply(MdEventBatch(items=items))

    # ------------------------------------------------------------------- ticks
    async def ticks(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        """Historical ticks from the raw tick files, paged by ``exch_ts_ns`` (RETRO uses ``modes=[BACKFILL]``)."""
        assert isinstance(payload, TicksQuery)
        svc = self.svc
        if payload.to_ns <= payload.from_ns:
            return ok_reply(TickBatch(ticks=[], next_from_ns=None))
        mono = svc.clock.monotonic_ns()
        if mono - svc.last_query_flush_mono >= int(svc.params.ticks_flush_interval_s * 1e9):
            svc.last_query_flush_mono = mono
            await svc.flush_recording()
        layout = svc.layout
        sources = list(svc.priorities)
        nxt = svc.sessions.next_trading_day(payload.trading_day)
        modes = [str(m) for m in payload.modes] if payload.modes else None
        spill = list(svc.spill_dirs)
        cache = self._shards

        def read() -> TickBatch:
            shards = day_shards(layout.ticks_raw_dir, payload.trading_day, sources, nxt, spill)
            return _page(cache.table(payload.trading_day, shards), payload, modes)

        batch = await svc.recorder.run(read)
        return ok_reply(batch)

    # -------------------------------------------------------------------- bars
    async def bars(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, BarsRequest)
        svc = self.svc
        assert svc.db is not None
        await svc.db.run_write(lambda c: None)
        bars = await svc.db.run_read(
            lambda c: store.load_bars(c, payload.trading_day, payload.instrument_id, payload.from_ns, payload.to_ns)
        )
        return ok_reply(BarsBatch(bars=bars))

    # ----------------------------------------------------------------- sources
    def sources(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        return ok_reply(self.svc.status())

    async def source_set(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        """Manual EXCLUDE / INCLUDE, idempotent by ``command_id`` (02 §6.2, 03 §7 人工控制); the pin is persisted (M4)."""
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
            svc._update_state()
            return result


def _with_version(item: SettleInput, version: int) -> SettleInput:
    return msgspec.structs.replace(item, version=version)


def _page(table: pa.Table, q: TicksQuery, modes: list[str] | None) -> TickBatch:
    if table.num_rows == 0:
        return TickBatch(ticks=[], next_from_ns=None)
    iid = pc.cast(table["instrument_id"], pa.string())
    td = pc.cast(table["trading_day"], pa.string())
    ts = table["exch_ts_ns"]
    mask = pc.and_(pc.equal(iid, q.instrument_id), pc.equal(td, q.trading_day))
    mask = pc.and_(mask, pc.and_(pc.greater_equal(ts, q.from_ns), pc.less(ts, q.to_ns)))
    if modes is not None:
        mask = pc.and_(mask, pc.is_in(pc.cast(table["mode"], pa.string()), value_set=pa.array(modes)))
    sub = table.filter(pc.fill_null(mask, False))
    if sub.num_rows == 0:
        return TickBatch(ticks=[], next_from_ns=None)
    sub = sub.sort_by([("exch_ts_ns", "ascending"), ("volume", "ascending"), ("recv_ns", "ascending")])
    rows = sub.slice(0, TICKS_PAGE + 1).to_pylist()
    next_from: int | None = None
    if len(rows) > TICKS_PAGE:
        next_from = int(rows[TICKS_PAGE]["exch_ts_ns"])
        keep = [r for r in rows[:TICKS_PAGE] if int(r["exch_ts_ns"]) < next_from]
        if not keep:  # a single timestamp group larger than a page: return the whole group
            group = sub.filter(pc.equal(sub["exch_ts_ns"], next_from)).to_pylist()
            keep = group
            later = sub.filter(pc.greater(sub["exch_ts_ns"], next_from))
            next_from = int(later["exch_ts_ns"][0].as_py()) if later.num_rows else None
        rows = keep
    ticks = [t for t in (row_to_tick(r) for r in rows) if t is not None]
    return TickBatch(ticks=ticks, next_from_ns=next_from)


__all__ = ["TICKS_PAGE", "register_commands"]
