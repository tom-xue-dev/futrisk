"""In-process data model of the md-gateway pipeline (01 §5.3.2, 02 §10).

A source adapter turns every vendor record it receives into an :class:`Incoming`:
either a normalized :class:`~futrisk.messages.md.Tick` or a normalization failure
with its 02 §11.3 reason. The pipeline (validator → arbiter) then decides the
:class:`~futrisk.messages.enums.TickMode` recorded for it (03 §5.1).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from futrisk.messages.enums import SourceId, TickMode
from futrisk.messages.md import Tick

OrderKey = tuple[str, int, int]
"""``(trading_day, volume, exch_ts_ns)``: cross-source ordering key (01 §8.13)."""


def order_key(tick: Tick) -> OrderKey:
    return (tick.trading_day, tick.volume, tick.exch_ts_ns)


@dataclass(slots=True)
class Incoming:
    """One received state (or a record that could not be normalized).

    ``recv_ns`` is market time (runtime market clock, 01 §8.2); ``recv_mono_ns`` is the
    ``perf_counter`` time of receipt used for latency statistics. ``replay`` marks Feed A
    replays (in-connection or resume), ``snapshot`` states from a subscribe snapshot;
    neither can ever be LIVE (01 §8.13).
    """

    source: SourceId
    recv_ns: int
    recv_mono_ns: int
    tick: Tick | None = None
    reject_reason: str | None = None
    detail: str = ""
    seq: int | None = None
    replay: bool = False
    snapshot: bool = False
    epoch: str | None = None
    instrument_id: str = ""
    exchange: str | None = None

    @property
    def key(self) -> OrderKey | None:
        return order_key(self.tick) if self.tick is not None else None

    @property
    def live_eligible(self) -> bool:
        return not self.replay and not self.snapshot


@dataclass(slots=True)
class Outcome:
    """Final classification of one received state (what goes to the recorder)."""

    incoming: Incoming
    mode: TickMode
    reason: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)


__all__ = ["Incoming", "OrderKey", "Outcome", "order_key"]
