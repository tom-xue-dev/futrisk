"""Source adapters without network: Feed A frame handling, Feed B polls (mock transport), Sina adapter (02 §9)."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

from futrisk.common.clock import BJ, NS_PER_MS, bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.marketdata.poller import SinaPoll
from futrisk.messages.codes import MD_REJECT_MALFORMED
from futrisk.messages.enums import SourceId
from futrisk.messages.md import FLAG_COARSE_TS
from futrisk.services.md_gateway.model import Incoming
from futrisk.services.md_gateway.normalizer import Normalizer
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.seqtracker import Hole
from futrisk.services.md_gateway.sources import FeedAWsSource, FeedBRestSource, SinaSourceAdapter

from .helpers import Clock

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "sina"


class Sink:
    def __init__(self) -> None:
        self.incoming: list[Incoming] = []
        self.polls: list[tuple[SourceId, bool, float | None, list[str]]] = []
        self.connected: list[tuple[SourceId, str | None]] = []
        self.frames: list[float | None] = []
        self.gaps: list[tuple[str, str]] = []

    def on_incoming(self, inc: Incoming) -> None:
        self.incoming.append(inc)

    def on_connected(self, source: SourceId, *, epoch: str | None = None) -> None:
        self.connected.append((source, epoch))

    def on_disconnected(self, source: SourceId, reason: str) -> None:
        pass

    def on_poll(self, source: SourceId, ok: bool, *, error: str | None = None, lag_ms: float | None = None, instruments: Iterable[str] = ()) -> None:
        self.polls.append((source, ok, lag_ms, list(instruments)))

    def on_frame(self, source: SourceId, *, lag_ms: float | None = None) -> None:
        self.frames.append(lag_ms)

    def on_gap(self, source: SourceId, hole: Hole, event: str) -> None:
        self.gaps.append((event, hole.kind))

    def on_epoch_change(self, source: SourceId, old: str | None, new: str) -> None:
        pass

    def on_resume_finished(self, source: SourceId) -> None:
        pass


def feed_a_tick(vol: int, ms: int) -> dict[str, Any]:
    return {
        "InstrumentID": "rb2701", "ExchangeID": "SHFE", "TradingDay": "20260928", "ActionDay": "20260928",
        "UpdateTime": "09:35:01", "UpdateMillisec": ms, "LastPrice": 3118.0, "Volume": vol, "Turnover": 3118.0 * vol * 10,
        "OpenInterest": 1615888.0, "BidPrice1": 3117.0, "BidVolume1": 12, "AskPrice1": 3118.0, "AskVolume1": 5,
        "OpenPrice": 3115.0, "HighestPrice": 3120.0, "LowestPrice": 3100.0, "UpperLimitPrice": 3276.0,
        "LowerLimitPrice": 2964.0, "PreSettlementPrice": 3120.0, "PreClosePrice": 3118.0, "PreOpenInterest": 1600000.0,
        "AveragePrice": 3114.3,
    }


def test_feed_a_frames_drive_the_tracker(default_config: FullConfig, normalizer: Normalizer) -> None:
    clock = Clock(bj_to_ns(datetime(2026, 9, 28, 9, 35, 1, 900_000, tzinfo=BJ)))
    sink = Sink()
    src = FeedAWsSource(default_config.feeds.feed_a, MdParams(), normalizer, sink, clock)  # type: ignore[arg-type]
    src.tracker.on_subscribe_sent(1, None)
    src._on_frame(json.dumps({"op": "welcome", "req": 1, "epoch": "E1", "head_seq": 10, "ring_low": 1, "hb_ms": 200, "trading_day": "20260928"}))
    assert sink.connected == [(SourceId.FEED_A, "E1")] and src.connected
    src._on_frame(json.dumps({"op": "snapshot", "as_of_seq": 10, "ticks": [feed_a_tick(100, 0)]}))
    src._on_frame(json.dumps({"op": "tick", "seq": 11, "replay": False, "d": feed_a_tick(101, 500)}))
    src._on_frame(json.dumps({"op": "tick", "seq": 11, "replay": False, "d": feed_a_tick(101, 500)}))  # duplicate
    src._on_frame(json.dumps({"op": "tick", "seq": 12, "replay": False, "d": {"InstrumentID": "rb2701"}}))  # malformed
    src._on_frame(json.dumps({"op": "heartbeat", "ts": clock.t // NS_PER_MS - 3, "head_seq": 12}))
    src._on_frame(b"not json")
    snap, live, bad, junk = sink.incoming
    assert snap.snapshot and snap.tick is not None and snap.seq is None
    assert live.seq == 11 and not live.replay and live.tick is not None and live.tick.volume == 101 and live.epoch == "E1"
    assert bad.seq == 12 and bad.reject_reason == MD_REJECT_MALFORMED and bad.instrument_id == "rb2701"
    assert junk.reject_reason == MD_REJECT_MALFORMED
    assert sink.frames and abs((sink.frames[-1] or 0) - 3.0) < 1e-6
    assert src.tracker.contiguous == 12
    # typed fast path for well-formed frames; only the malformed tick and the junk took the dict path (M6)
    assert src.slow_frames == 2


async def test_feed_b_poll_success_failure_and_bad_record(default_config: FullConfig, normalizer: Normalizer) -> None:
    now = bj_to_ns(datetime(2026, 9, 28, 9, 35, 2, tzinfo=BJ))
    clock = Clock(now)
    t_ms = now // NS_PER_MS - 500
    rec = {"sym": "RB2701", "ex": "SHFE", "px": "3118", "bp": "3117", "ap": "3118", "bv": 12, "av": 5, "vol": 570630,
           "amt": "17771239560", "oi": 1615888, "up": "3276", "dn": "2964", "ps": "3120", "pc": "3118", "o": "3115",
           "h": "3120", "l": "3100", "t": t_ms}
    bodies = [
        (200, json.dumps({"ts": now // NS_PER_MS - 10, "td": "20260928", "data": [rec, dict(rec, sym="IF2612", px="N/A")]}).encode()),
        (503, b'{"status":"unavailable"}'),
        (200, b'{"ts": 1, "td": "2026'),  # truncated body (MALFORMED fault)
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        status, body = bodies.pop(0)
        return httpx.Response(status, content=body)

    sink = Sink()
    src = FeedBRestSource(default_config.feeds.feed_b, normalizer, sink, clock)  # type: ignore[arg-type]
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await src.poll_once(client)
        await src.poll_once(client)
        await src.poll_once(client)
    ok, fail503, fail_body = sink.polls
    # coverage counts only records the pipeline accepts (H2): the poll itself reports no instrument list
    assert ok[1] is True and ok[2] == 10.0 and ok[3] == []
    assert fail503[1] is False and fail_body[1] is False
    good, bad = sink.incoming
    assert good.tick is not None and good.tick.instrument_id == "rb2701" and good.source is SourceId.FEED_B
    assert bad.reject_reason == MD_REJECT_MALFORMED and bad.instrument_id == "IF2612"
    t = src.timeout()  # 02 §9.2 / L8: connect 1 s, read 1.5 s
    assert t.connect == 1.0 and t.read == 1.5


def test_sina_adapter_turns_results_into_coarse_ticks(default_config: FullConfig) -> None:
    body = (FIXTURES / "all_configured.txt").read_bytes()
    recv = bj_to_ns(datetime(2026, 9, 24, 15, 20, 0, tzinfo=BJ))  # after every closing broadcast of the fixture
    clock = Clock(recv)
    sink = Sink()
    feeds = default_config.feeds
    sina = SinaSourceAdapter(feeds.sina, default_config.products, default_config.calendar, sink, clock)  # type: ignore[arg-type]
    codes = tuple(sina._source.poller.codes)
    poll = SinaPoll(seq=1, codes=codes, sent_ns=recv - 50 * NS_PER_MS, recv_ns=recv, elapsed_ms=50.0, status=200, body=body)
    sina._on_batch(sina._source.batch_for(poll))
    assert sink.polls[0][1] is True
    ticks = [i.tick for i in sink.incoming if i.reject_reason is None and i.tick is not None]
    assert len(ticks) == len(feeds.sina.symbols)
    assert all(t.flags & FLAG_COARSE_TS and t.source is SourceId.SINA for t in ticks)
    failed = SinaPoll(seq=2, codes=codes, sent_ns=recv, recv_ns=recv, elapsed_ms=1.0, status=None, body=None, error="timeout")
    sina._on_batch(sina._source.batch_for(failed))
    assert sink.polls[-1][1] is False
