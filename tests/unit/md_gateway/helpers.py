"""Builders shared by the md-gateway unit tests (ticks, incoming states, a settable clock)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import msgspec

from futrisk.common.clock import BJ, NS_PER_MS, NS_PER_S, bj_to_ns
from futrisk.marketdata.fingerprint import tick_fingerprint
from futrisk.messages.enums import Exchange, MdEventKind, SourceId, TickMode
from futrisk.messages.md import Tick
from futrisk.services.md_gateway.model import Incoming

#: Monday 2026-09-28 09:35 Beijing: every product is in a continuous session
T0 = bj_to_ns(datetime(2026, 9, 28, 9, 35, tzinfo=BJ))
TD = "20260928"
MS = NS_PER_MS
S = NS_PER_S


class Clock:
    """Settable market + monotonic clock (``now_ns == monotonic_ns``)."""

    def __init__(self, start: int = T0) -> None:
        self.t = start

    def now_ns(self) -> int:
        return self.t

    def monotonic_ns(self) -> int:
        return self.t

    def advance(self, ns: int) -> None:
        self.t += ns


def tick(
    *,
    iid: str = "rb2701",
    ts: int = T0,
    last: float = 3120.0,
    volume: int = 100,
    turnover: float | None = None,
    oi: int = 1_800_000,
    bid1: float | None = None,
    ask1: float | None = None,
    bid_vol1: int = 5,
    ask_vol1: int = 7,
    source: SourceId = SourceId.FEED_A,
    seq: int | None = None,
    recv: int | None = None,
    upper: float = 3276.0,
    lower: float = 2964.0,
    pre_settle: float = 3120.0,
    exchange: Exchange = Exchange.SHFE,
    td: str = TD,
    high: float | None = None,
    low: float | None = None,
    flags: int = 0,
) -> Tick:
    t = Tick(
        instrument_id=iid,
        exchange=exchange,
        trading_day=td,
        action_day=td,
        exch_ts_ns=ts,
        last=last,
        volume=volume,
        turnover=turnover if turnover is not None else last * volume * 10,
        open_interest=oi,
        bid1=bid1 if bid1 is not None else last - 1,
        ask1=ask1 if ask1 is not None else last,
        bid_vol1=bid_vol1,
        ask_vol1=ask_vol1,
        open=None,
        high=high,
        low=low,
        upper_limit=upper,
        lower_limit=lower,
        pre_settle=pre_settle,
        source=source,
        source_seq=seq,
        recv_ns=recv if recv is not None else ts + 5 * MS,
        flags=flags,
    )
    return msgspec.structs.replace(t, fingerprint=tick_fingerprint(t))


def inc(t: Tick, *, replay: bool = False, snapshot: bool = False, recv: int | None = None) -> Incoming:
    return Incoming(
        source=t.source,
        recv_ns=recv if recv is not None else t.recv_ns,
        recv_mono_ns=recv if recv is not None else t.recv_ns,
        tick=t,
        seq=t.source_seq,
        replay=replay,
        snapshot=snapshot,
        epoch="E1" if t.source is SourceId.FEED_A else None,
    )


def as_source(t: Tick, source: SourceId, *, recv: int | None = None, seq: int | None = None) -> Tick:
    """The same market state seen by another source (identical fingerprint)."""
    return msgspec.structs.replace(t, source=source, source_seq=seq, recv_ns=recv if recv is not None else t.recv_ns)


@dataclass
class FakeEffects:
    """Collects what the pipeline publishes, records and emits."""

    published: list[Tick] = field(default_factory=list)
    recorded: list[tuple[Incoming, TickMode, str | None]] = field(default_factory=list)
    events: list[tuple[MdEventKind, dict[str, Any]]] = field(default_factory=list)

    def publish_tick(self, tick: Tick, recv_mono_ns: int) -> None:
        self.published.append(tick)

    def record(self, inc: Incoming, mode: TickMode, reason: str | None) -> None:
        self.recorded.append((inc, mode, reason))

    def emit(self, kind: MdEventKind, **kw: Any) -> None:
        self.events.append((kind, kw))

    def kinds(self) -> list[MdEventKind]:
        return [k for k, _ in self.events]

    def modes(self) -> list[TickMode]:
        return [m for _, m, _ in self.recorded]
