"""Async poller and SinaSource tests with a mocked transport (02 §9.3, §10); no network."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import httpx
import pytest

from futrisk.marketdata.poller import (
    SINA_CAPABILITIES,
    SinaBatch,
    SinaPoll,
    SinaPoller,
    SinaSource,
    SourceCapabilities,
)
from futrisk.marketdata.sina import SinaNormalizer

CODES = ("nf_RB2701", "CFF_RE_IF2612")


async def wait_until(cond: Callable[[], bool], timeout_s: float = 5.0) -> None:
    async def spin() -> None:
        while not cond():
            await asyncio.sleep(0.005)

    await asyncio.wait_for(spin(), timeout_s)


def client_for(handler: Callable[[httpx.Request], object]) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))  # type: ignore[arg-type]


def fast_poller(codes: tuple[str, ...], client: httpx.AsyncClient, **kw: object) -> SinaPoller:
    opts: dict[str, object] = {
        "poll_ms": 10,
        "min_poll_ms": 0,
        "timeout_ms": 2000,
        "connect_timeout_ms": 2000,
    }
    opts.update(kw)
    return SinaPoller(codes, client=client, **opts)  # type: ignore[arg-type]


class TestFetch:
    async def test_request_carries_codes_and_referer(self, sina_bytes: dict[str, bytes]) -> None:
        seen: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, content=sina_bytes["all_configured.txt"])

        async with client_for(handler) as client:
            poll = await fast_poller(CODES, client).fetch()
        assert str(seen[0].url) == "https://hq.sinajs.cn/list=nf_RB2701,CFF_RE_IF2612"
        assert seen[0].headers["Referer"] == "https://finance.sina.com.cn"
        assert poll.ok and poll.status == 200 and poll.error is None
        assert poll.codes == CODES and poll.seq == 1
        assert poll.recv_ns >= poll.sent_ns and poll.elapsed_ms >= 0
        assert "螺纹钢2701" in poll.text()

    async def test_http_error_is_reported_not_raised(self, sina_bytes: dict[str, bytes]) -> None:
        async with client_for(
            lambda r: httpx.Response(403, content=sina_bytes["forbidden_no_referer.txt"])
        ) as client:
            poller = fast_poller(CODES, client)
            poll = await poller.fetch()
        assert not poll.ok and poll.status == 403 and poll.error == "HTTP 403" and poll.body == b"Forbidden"
        assert (poller.stats.polls, poller.stats.failed, poller.stats.consecutive_failures) == (1, 1, 1)

    async def test_transport_error_is_reported(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("connection refused", request=request)

        async with client_for(handler) as client:
            poll = await fast_poller(CODES, client).fetch()
        assert poll.status is None and poll.body is None and poll.error is not None
        assert poll.error.startswith("ConnectError")

    async def test_timeout_is_reported(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(5)
            return httpx.Response(200)

        async with client_for(handler) as client:
            poll = await fast_poller(CODES, client, timeout_ms=30, connect_timeout_ms=30).fetch()
        assert not poll.ok and poll.error is not None and poll.error.startswith("TimeoutError")

    def test_interval_clamp_and_backoff_policy(self) -> None:
        poller = SinaPoller(CODES, poll_ms=200)  # below the 1s minimum (02 §9.3)
        assert poller.poll_s == 1.0
        poller = SinaPoller(CODES, poll_ms=1500)

        def intervals(status: int | None) -> list[float]:
            out = []
            for n in range(8):
                poller.stats.consecutive_failures = n
                poller.stats.last_status = status
                out.append(poller.current_interval_s())
            return out

        # network errors / timeouts / 5xx: short backoff capped at 5 s
        assert intervals(None) == [1.5, 1.5, 3.0, 5.0, 5.0, 5.0, 5.0, 5.0]
        assert intervals(503) == [1.5, 1.5, 3.0, 5.0, 5.0, 5.0, 5.0, 5.0]
        # blocked / rate limited: long backoff up to 60 s
        assert intervals(403) == [1.5, 3.0, 6.0, 12.0, 24.0, 48.0, 60.0, 60.0]
        assert intervals(429) == intervals(403)
        # a slow configured rate is never shortened by the network cap
        slow = SinaPoller(CODES, poll_ms=10_000)
        slow.stats.consecutive_failures = 4
        assert slow.current_interval_s() == 10.0


class TestRun:
    async def test_polls_never_overlap_and_overrun_ticks_are_skipped(
        self, sina_bytes: dict[str, bytes]
    ) -> None:
        active = 0
        max_active = 0
        calls = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal active, max_active, calls
            calls += 1
            active += 1
            max_active = max(max_active, active)
            try:
                if calls == 1:
                    await asyncio.sleep(0.1)  # ten 10 ms ticks elapse while this request hangs
            finally:
                active -= 1
            return httpx.Response(200, content=sina_bytes["all_configured.txt"])

        polls: list[SinaPoll] = []
        async with client_for(handler) as client:
            poller = fast_poller(CODES, client)
            stop = asyncio.Event()
            task = asyncio.create_task(poller.run(polls.append, stop=stop))
            await wait_until(lambda: len(polls) >= 4)
            stop.set()
            await task
        assert max_active == 1
        assert poller.stats.skipped >= 5  # the overrun ticks were dropped, not replayed in a burst
        assert [p.seq for p in polls] == list(range(1, len(polls) + 1))  # delivered in order, none lost

    async def test_first_success_restores_the_rate(self, sina_bytes: dict[str, bytes]) -> None:
        statuses = [403, 403, 403, 200, 200]
        started: list[float] = []
        loop = asyncio.get_running_loop()

        def handler(request: httpx.Request) -> httpx.Response:
            started.append(loop.time())
            status = statuses[min(len(started), len(statuses)) - 1]
            return httpx.Response(status, content=sina_bytes["all_configured.txt"] if status == 200 else b"x")

        chosen: list[float | None] = []
        async with client_for(handler) as client:
            poller = fast_poller(CODES, client, poll_ms=20)
            # each handler call sees the interval the scheduler chose after the previous cycle
            await asyncio.wait_for(
                poller.run(
                    lambda p: chosen.append(poller.stats.last_interval_s),
                    keep_running=lambda: len(started) < 5,
                ),
                10,
            )
        chosen.append(poller.stats.last_interval_s)
        # 403 backs off 40/80/160 ms; the first success restores the 20 ms rate immediately
        assert chosen[1:5] == [0.04, 0.08, 0.16, 0.02]
        gaps = [b - a for a, b in zip(started, started[1:], strict=False)]
        assert gaps[2] >= 0.15  # still backing off before the success ...
        assert gaps[3] < gaps[2] / 2  # ... and the normal rate right after it

    async def test_network_errors_back_off_at_most_5s(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("unreachable", request=request)

        async with client_for(handler) as client:
            poller = fast_poller(CODES, client, poll_ms=1000, min_poll_ms=0)
            for _ in range(6):
                await poller.fetch()
        assert poller.stats.consecutive_failures == 6 and poller.stats.last_status is None
        assert poller.current_interval_s() == 5.0

    async def test_handler_errors_do_not_stop_polling(self, sina_bytes: dict[str, bytes]) -> None:
        calls = 0

        async def handler(poll: SinaPoll) -> None:
            nonlocal calls
            calls += 1
            raise RuntimeError("disk full")

        async with client_for(
            lambda r: httpx.Response(200, content=sina_bytes["all_configured.txt"])
        ) as client:
            poller = fast_poller(CODES, client)
            stop = asyncio.Event()
            task = asyncio.create_task(poller.run(handler, stop=stop))
            await wait_until(lambda: calls >= 3)
            stop.set()
            await task

    async def test_keep_running_ends_the_loop(self, sina_bytes: dict[str, bytes]) -> None:
        polls: list[SinaPoll] = []
        async with client_for(
            lambda r: httpx.Response(200, content=sina_bytes["all_configured.txt"])
        ) as client:
            poller = fast_poller(CODES, client)
            await asyncio.wait_for(poller.run(polls.append, keep_running=lambda: len(polls) < 2), 5)
        assert len(polls) >= 2


class TestSinaSource:
    async def test_start_stop_delivers_normalized_batches(
        self, normalizer: SinaNormalizer, sina_bytes: dict[str, bytes]
    ) -> None:
        batches: list[SinaBatch] = []
        async with client_for(
            lambda r: httpx.Response(200, content=sina_bytes["all_configured.txt"])
        ) as client:
            source = SinaSource(fast_poller(tuple(normalizer.codes), client), normalizer)
            await source.start(batches.append)
            assert source.running
            with pytest.raises(RuntimeError):
                await source.start(batches.append)
            await wait_until(lambda: len(batches) >= 2)
            await source.stop()
        assert not source.running
        first = batches[0]
        assert first.poll.ok and len(first.results) == 11 and all(r.ok for r in first.results)
        assert (
            source.capabilities
            == SINA_CAPABILITIES
            == SourceCapabilities(has_seq=False, can_replay=False, coarse_ts=True)
        )
        assert str(source.source_id) == "SINA"

    async def test_failed_polls_reach_the_sink_without_results(self, normalizer: SinaNormalizer) -> None:
        batches: list[SinaBatch] = []
        async with client_for(lambda r: httpx.Response(503, content=b"busy")) as client:
            source = SinaSource(fast_poller(tuple(normalizer.codes), client), normalizer)

            async def sink(batch: SinaBatch) -> None:
                batches.append(batch)

            await source.start(sink)
            await wait_until(lambda: len(batches) >= 1)
            await source.stop()
        assert batches[0].results == () and batches[0].poll.status == 503
