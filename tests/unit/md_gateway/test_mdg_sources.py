"""Source adapters without network: Feed A frames, the poll transport (mock HTTP), Feed B and Sina decoders (02 §9, §10)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Iterable
from datetime import datetime
from typing import Any

import httpx

from futrisk.common.clock import BJ, NS_PER_MS, bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.messages.codes import MD_REJECT_MALFORMED
from futrisk.messages.enums import SourceId
from futrisk.messages.md import FLAG_COARSE_TS
from futrisk.services.md_gateway.model import Incoming
from futrisk.services.md_gateway.normalizer import Normalizer
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.seqtracker import Hole
from futrisk.services.md_gateway.sina import SinaDecoder
from futrisk.services.md_gateway.sources import (
    FeedAWsSource,
    PollResult,
    PollSpec,
    PollTransport,
    feed_b_source,
    sina_source,
)

from .helpers import Clock

A, B, SINA = SourceId.FEED_A, SourceId.FEED_B, SourceId.SINA


class Sink:
    def __init__(self) -> None:
        self.incoming: list[Incoming] = []
        self.polls: list[tuple[SourceId, bool, float | None, str | None]] = []
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
        self.polls.append((source, ok, lag_ms, error))

    def on_frame(self, source: SourceId, *, lag_ms: float | None = None) -> None:
        self.frames.append(lag_ms)

    def on_gap(self, source: SourceId, hole: Hole, event: str) -> None:
        self.gaps.append((event, hole.kind))

    def on_epoch_change(self, source: SourceId, old: str | None, new: str) -> None:
        pass

    def on_resume_finished(self, source: SourceId) -> None:
        pass


def client_for(handler: Callable[[httpx.Request], Any]) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def feed_a_tick(vol: int, ms: int) -> dict[str, Any]:
    return {
        "InstrumentID": "rb2701", "ExchangeID": "SHFE", "TradingDay": "20260928", "ActionDay": "20260928", "UpdateTime": "09:35:01",
        "UpdateMillisec": ms, "LastPrice": 3118.0, "Volume": vol, "Turnover": 3118.0 * vol * 10, "OpenInterest": 1615888.0,
        "BidPrice1": 3117.0, "BidVolume1": 12, "AskPrice1": 3118.0, "AskVolume1": 5, "OpenPrice": 3115.0, "HighestPrice": 3120.0,
        "LowestPrice": 3100.0, "UpperLimitPrice": 3276.0, "LowerLimitPrice": 2964.0, "PreSettlementPrice": 3120.0,
        "PreClosePrice": 3118.0, "PreOpenInterest": 1600000.0, "AveragePrice": 3114.3,
    }


def test_feed_a_frames_drive_the_tracker(default_config: FullConfig, normalizer: Normalizer) -> None:
    clock = Clock(bj_to_ns(datetime(2026, 9, 28, 9, 35, 1, 900_000, tzinfo=BJ)))
    sink = Sink()
    src = FeedAWsSource(default_config.feeds.feed_a, MdParams(), normalizer, sink, clock)  # type: ignore[arg-type]
    src.tracker.on_subscribe_sent(1, None)
    src._on_frame(json.dumps({"op": "welcome", "req": 1, "epoch": "E1", "head_seq": 10, "ring_low": 1, "hb_ms": 200, "trading_day": "20260928"}))
    assert sink.connected == [(A, "E1")] and src.connected
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
    assert src.slow_frames == 2  # typed fast path for well-formed frames; only the malformed tick and the junk took the dict path


# ---------------------------------------------------------------- the poll transport
async def test_feed_b_polls_success_failure_and_bad_record(default_config: FullConfig, normalizer: Normalizer) -> None:
    now = bj_to_ns(datetime(2026, 9, 28, 9, 35, 2, tzinfo=BJ))
    clock = Clock(now)
    rec = {"sym": "RB2701", "ex": "SHFE", "px": "3118", "bp": "3117", "ap": "3118", "bv": 12, "av": 5, "vol": 570630, "amt": "17771239560",
           "oi": 1615888, "up": "3276", "dn": "2964", "ps": "3120", "pc": "3118", "o": "3115", "h": "3120", "l": "3100", "t": now // NS_PER_MS - 500}
    bodies = [
        (200, json.dumps({"ts": now // NS_PER_MS - 10, "td": "20260928", "data": [rec, dict(rec, sym="IF2612", px="N/A")]}).encode()),
        (503, b'{"status":"unavailable"}'),
        (200, b'{"ts": 1, "td": "2026'),  # truncated body (MALFORMED fault)
    ]
    sink = Sink()
    cfg = default_config.feeds.feed_b
    def handler(request: httpx.Request) -> httpx.Response:
        status, body = bodies.pop(0)
        return httpx.Response(status, content=body)

    async with client_for(handler) as client:
        src = feed_b_source(cfg, normalizer, sink, clock, client=client)
        assert [await src.poll_once() for _ in range(3)] == [True, False, False]
    assert src.timeout.connect == 1.0 and src.timeout.read == cfg.timeout_ms / 1000  # 02 §9.2
    ok, fail503, fail_body = sink.polls
    assert ok[1] is True and ok[2] == 10.0 and fail503[1:] == (False, None, "HTTP 503") and fail_body[1] is False and "bad JSON" in (fail_body[3] or "")
    good, bad = sink.incoming
    assert good.tick is not None and good.tick.instrument_id == "rb2701" and good.source is B
    assert bad.reject_reason == MD_REJECT_MALFORMED and bad.instrument_id == "IF2612"
    assert (src.stats.polls, src.stats.ok, src.stats.failed, src.stats.consecutive_failures) == (3, 1, 2, 2)
    assert src.current_interval_s() == src.poll_s  # Feed B never backs off: the trust manager excludes it after 3 failures


async def test_sina_source_decodes_the_captured_response(sina_decoder: SinaDecoder, default_config: FullConfig, sina_bytes: dict[str, bytes]) -> None:
    recv = bj_to_ns(datetime(2026, 9, 24, 15, 20, 0, tzinfo=BJ))  # after every closing broadcast of the fixture
    clock = Clock(recv)
    sink = Sink()
    results: list[PollResult] = []
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, content=sina_bytes["all_configured.txt"]) if len(seen) == 1 else httpx.Response(403, content=b"Forbidden")

    async with client_for(handler) as client:
        src = sina_source(default_config.feeds.sina, sina_decoder, sink, clock, client=client, on_result=results.append)
        assert await src.poll_once() and not await src.poll_once()
    assert str(seen[0].url).startswith("https://hq.sinajs.cn/list=nf_RB2701,") and seen[0].headers["Referer"] == "https://finance.sina.com.cn"
    assert src.capabilities.coarse_ts and src.source_id is SINA and src.poll_s == 1.5
    ticks = [i.tick for i in sink.incoming if i.reject_reason is None and i.tick is not None]
    assert len(ticks) == len(default_config.feeds.sina.symbols) and all(t.flags & FLAG_COARSE_TS and t.source is SINA for t in ticks)
    assert sink.polls[0][1] is True and sink.polls[1][1:] == (False, None, "HTTP 403")
    assert [r.status for r in results] == [200, 403] and results[1].body == b"Forbidden"
    assert src.current_interval_s() == 3.0  # one 403: the interval doubles


def _spec(**kw: Any) -> PollSpec:
    base: dict[str, Any] = {"source_id": SINA, "url": "https://x/list=a", "poll_ms": 1500, "timeout_ms": 1200, "min_poll_ms": 1000,
                            "ban_backoff_cap_s": 60.0, "network_backoff_cap_s": 5.0}
    base.update(kw)
    return PollSpec(**base)


class Noop:
    def decode(self, body: bytes, recv_ns: int, mono: int) -> tuple[list[Incoming], float | None]:
        return [], None


def test_interval_clamp_and_backoff_policy() -> None:
    t = PollTransport(_spec(poll_ms=200), Noop(), Sink(), Clock())  # below the 1s minimum (02 §9.3)
    assert t.poll_s == 1.0
    t = PollTransport(_spec(), Noop(), Sink(), Clock())

    def intervals(status: int | None) -> list[float]:
        out = []
        for n in range(8):
            t.stats.consecutive_failures, t.stats.last_status = n, status
            out.append(t.current_interval_s())
        return out

    assert intervals(None) == [1.5, 1.5, 3.0, 5.0, 5.0, 5.0, 5.0, 5.0]  # network errors / timeouts / 5xx: capped at 5 s
    assert intervals(503) == intervals(None)
    assert intervals(403) == [1.5, 3.0, 6.0, 12.0, 24.0, 48.0, 60.0, 60.0] == intervals(429)  # blocked / rate limited
    slow = PollTransport(_spec(poll_ms=10_000), Noop(), Sink(), Clock())
    slow.stats.consecutive_failures = 4
    assert slow.current_interval_s() == 10.0  # a slow configured rate is never shortened


async def test_transport_errors_and_timeouts_are_reported_not_raised() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    async def hang(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(5)
        return httpx.Response(200)

    sink = Sink()
    async with client_for(refuse) as client:
        res = await PollTransport(_spec(min_poll_ms=0), Noop(), sink, Clock(), client=client).fetch()
    assert res.status is None and res.body is None and (res.error or "").startswith("ConnectError")
    async with client_for(hang) as client:
        t = PollTransport(_spec(timeout_ms=30, connect_timeout_s=0.03, min_poll_ms=0), Noop(), sink, Clock(), client=client)
        assert not await t.poll_once()
    assert (t.stats.last_error or "").startswith("TimeoutError") and sink.polls[-1][1] is False


async def test_polls_never_overlap_overrun_slots_are_skipped_and_the_rate_recovers(sina_bytes: dict[str, bytes]) -> None:
    active = max_active = calls = 0
    statuses = [200, 403, 403, 200, 200]

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, max_active, calls
        calls += 1
        active += 1
        max_active = max(max_active, active)
        try:
            if calls == 1:
                await asyncio.sleep(0.1)  # ten 10 ms slots elapse while this request hangs
        finally:
            active -= 1
        status = statuses[min(calls, len(statuses)) - 1]
        return httpx.Response(status, content=sina_bytes["all_configured.txt"] if status == 200 else b"x")

    chosen: list[float | None] = []
    sink = Sink()
    async with client_for(handler) as client:
        t = PollTransport(_spec(poll_ms=10, min_poll_ms=0, timeout_ms=2000, connect_timeout_s=2.0), Noop(), sink, Clock(), client=client)
        await t.start()
        assert t.running
        while len(sink.polls) < 5:
            chosen.append(t.stats.last_interval_s)
            await asyncio.sleep(0.005)
        await t.stop()
    assert not t.running and max_active == 1 and t.stats.skipped >= 5  # overrun slots are dropped, not replayed in a burst
    assert [ok for _, ok, _, _ in sink.polls[:5]] == [True, False, False, True, True]
    assert t.stats.last_interval_s == 0.01  # the first success restores the configured rate at once


async def test_keep_running_ends_the_loop_and_handler_bugs_do_not(sina_bytes: dict[str, bytes]) -> None:
    class Broken:
        def decode(self, body: bytes, recv_ns: int, mono: int) -> tuple[list[Incoming], float | None]:
            raise RuntimeError("bug")

    sink = Sink()
    async with client_for(lambda r: httpx.Response(200, content=sina_bytes["all_configured.txt"])) as client:
        t = PollTransport(_spec(poll_ms=10, min_poll_ms=0), Broken(), sink, Clock(), client=client)
        await asyncio.wait_for(t.run(keep_running=lambda: t.stats.polls < 3), 5)
    assert t.stats.polls >= 3 and all(not ok for _, ok, _, _ in sink.polls)  # a decoder bug is a failed poll, the loop goes on
