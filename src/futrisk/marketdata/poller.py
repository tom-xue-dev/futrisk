"""Async Sina poller and the ``SinaSource`` market-data source (02 §9.3, §10; 01 §8.13).

:class:`SinaPoller` issues one merged ``GET`` per cycle on a single shared
``httpx.AsyncClient`` (keep-alive, per-request timeout, ``trust_env``
configurable for proxy environments):

- **fixed rate, no overlap**: cycles (request + handler) run one after
  another; the next one starts ``interval`` after the previous one *started*,
  and ticks that elapse while a slow cycle is still running are *skipped*
  (counted in :attr:`PollerStats.skipped`), never made up in a burst, like
  Feed B (02 §9.2);
- **minimum interval**: ``poll_ms`` is clamped to >= 1000ms, because more
  frequent polling risks an IP ban (02 §9.3);
- **backoff, decided after each cycle completes** (so it reflects that cycle's
  outcome):

  - success: back to ``poll_ms`` immediately;
  - HTTP 403 / 429 (blocked or rate limited): ``poll_ms * 2**n`` up to
    ``max_backoff_s`` (60 s), so a ban is not hammered;
  - anything else (network error, timeout, 5xx, ...): ``poll_ms * 2**(n-1)``
    capped at ``network_backoff_cap_s`` (5 s), so a flaky network loses little data;

- every cycle yields a :class:`SinaPoll` (raw bytes + timing), including failed
  ones, so consumers can count failures (md-gateway excludes a source after 3
  failed polls) and the recorder can archive the wire data.

:class:`SinaSource` wraps poller + :class:`~futrisk.marketdata.sina.SinaNormalizer`
behind the ``MarketDataSource`` shape of 02 §10: ``capabilities`` (no seq, no
replay, coarse timestamps), ``start(sink)`` and ``stop()``.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import math
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from typing import Final

import httpx
from loguru import logger

from futrisk.common.clock import Clock, SystemClock
from futrisk.marketdata.sina import (
    SINA_REFERER,
    SINA_URL,
    SinaNormalizer,
    SinaResult,
    build_url,
    decode_body,
    request_headers,
)
from futrisk.messages.enums import SourceId

MIN_POLL_MS: Final = 1000
NETWORK_BACKOFF_CAP_S: Final = 5.0
BAN_BACKOFF_CAP_S: Final = 60.0
BAN_STATUSES: Final = frozenset({403, 429})
_MAX_ERROR_LEN = 300
_MAX_SLEEP_CHUNK_S = 1.0  # re-check keep_running() at least this often while waiting


def make_sina_client(trust_env: bool = False, timeout: httpx.Timeout | None = None) -> httpx.AsyncClient:
    """HTTP client suited to Sina polling: keep-alive, at most 2 connections, no redirects.

    ``trust_env`` honours ``HTTP(S)_PROXY`` (02 §9.3, ``SinaConfig.trust_env``).
    """
    return httpx.AsyncClient(
        timeout=timeout or httpx.Timeout(5.0),
        trust_env=trust_env,
        follow_redirects=False,
        limits=httpx.Limits(max_connections=2, max_keepalive_connections=1),
    )


@dataclass(frozen=True, slots=True)
class SourceCapabilities:
    """``MarketDataSource.capabilities`` (02 §10)."""

    has_seq: bool
    can_replay: bool
    coarse_ts: bool


SINA_CAPABILITIES: Final = SourceCapabilities(has_seq=False, can_replay=False, coarse_ts=True)


@dataclass(frozen=True, slots=True)
class SinaPoll:
    """One HTTP cycle: what was asked, what came back, and when.

    ``sent_ns`` / ``recv_ns`` are wall-clock UTC ns from the injected clock
    (``SystemClock`` advances with ``perf_counter_ns``, 01 §8.2); ``recv_ns`` is
    taken after the whole body has been read. ``body`` holds the raw
    (gzip-decoded) GB18030 bytes; ``status`` is ``None`` on transport errors.
    """

    seq: int
    codes: tuple[str, ...]
    sent_ns: int
    recv_ns: int
    elapsed_ms: float
    status: int | None
    body: bytes | None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.status == 200 and self.body is not None

    def text(self) -> str:
        """Body decoded with GB18030 (empty string when there is no body)."""
        return decode_body(self.body or b"")


@dataclass
class PollerStats:
    polls: int = 0
    ok: int = 0
    failed: int = 0
    skipped: int = 0
    consecutive_failures: int = 0
    last_status: int | None = None
    last_error: str | None = None
    last_elapsed_ms: float | None = None
    last_interval_s: float | None = None


PollHandler = Callable[[SinaPoll], Awaitable[None] | None]


class SinaPoller:
    """Fixed-rate, non-overlapping poller for ``hq.sinajs.cn``."""

    def __init__(
        self,
        codes: Sequence[str],
        *,
        url: str = SINA_URL,
        referer: str = SINA_REFERER,
        poll_ms: int = 1500,
        timeout_ms: int = 1200,
        connect_timeout_ms: int | None = None,
        trust_env: bool = False,
        clock: Clock | None = None,
        client: httpx.AsyncClient | None = None,
        min_poll_ms: int = MIN_POLL_MS,
        max_backoff_s: float = BAN_BACKOFF_CAP_S,
        network_backoff_cap_s: float = NETWORK_BACKOFF_CAP_S,
    ) -> None:
        self._codes = tuple(codes)
        self._url = build_url(self._codes, url)
        self._headers = request_headers(referer)
        if poll_ms < min_poll_ms:
            logger.warning(f"sina.poll_ms {poll_ms} below minimum {min_poll_ms}; clamped")
            poll_ms = min_poll_ms
        self._poll_s = poll_ms / 1000.0
        read_s = timeout_ms / 1000.0
        connect_s = (connect_timeout_ms / 1000.0) if connect_timeout_ms is not None else max(read_s, 3.0)
        self._timeout = httpx.Timeout(read_s, connect=connect_s)
        self._total_timeout_s = connect_s + read_s
        self._trust_env = trust_env
        self._clock: Clock = clock or SystemClock()
        self._client = client
        self._owns_client = client is None
        self._max_backoff_s = max_backoff_s
        self._network_backoff_cap_s = network_backoff_cap_s
        self._seq = 0
        self.stats = PollerStats()

    # ------------------------------------------------------------ properties
    @property
    def codes(self) -> tuple[str, ...]:
        return self._codes

    @property
    def url(self) -> str:
        return self._url

    @property
    def poll_s(self) -> float:
        return self._poll_s

    def current_interval_s(self) -> float:
        """Interval before the next poll, given the outcome of the last completed one."""
        st = self.stats
        n = st.consecutive_failures
        if n == 0:
            return self._poll_s
        if st.last_status in BAN_STATUSES:
            return max(self._poll_s, min(self._max_backoff_s, self._poll_s * 2.0**n))
        return max(self._poll_s, min(self._network_backoff_cap_s, self._poll_s * 2.0 ** (n - 1)))

    # --------------------------------------------------------------- client
    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = make_sina_client(self._trust_env, self._timeout)
            self._owns_client = True
        return self._client

    async def aclose(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> SinaPoller:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # ---------------------------------------------------------------- fetch
    async def fetch(self) -> SinaPoll:
        """One request; never raises for network/HTTP problems (they are reported in the poll)."""
        client = self._ensure_client()
        self._seq += 1
        seq = self._seq
        sent_ns = self._clock.now_ns()
        t0 = self._clock.monotonic_ns()
        status: int | None = None
        body: bytes | None = None
        error: str | None = None
        try:
            async with asyncio.timeout(self._total_timeout_s):
                resp = await client.get(self._url, headers=self._headers, timeout=self._timeout)
            status = resp.status_code
            body = resp.content
            if status != 200:
                error = f"HTTP {status}"
        except (httpx.HTTPError, TimeoutError, OSError) as exc:
            error = f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_LEN]
        recv_ns = self._clock.now_ns()
        elapsed_ms = (self._clock.monotonic_ns() - t0) / 1e6
        poll = SinaPoll(seq, self._codes, sent_ns, recv_ns, elapsed_ms, status, body, error)
        self._account(poll)
        return poll

    def _account(self, poll: SinaPoll) -> None:
        st = self.stats
        st.polls += 1
        st.last_elapsed_ms = poll.elapsed_ms
        st.last_status = poll.status
        if poll.ok:
            st.ok += 1
            if st.consecutive_failures:
                logger.info(
                    f"sina.recovered after {st.consecutive_failures} failed polls; back to {self._poll_s:.1f}s"
                )
            st.consecutive_failures = 0
        else:
            st.failed += 1
            st.consecutive_failures += 1
            st.last_error = poll.error
            if st.consecutive_failures in (1, 3) or st.consecutive_failures % 20 == 0:
                logger.warning(
                    f"sina.poll_failed seq={poll.seq} error={poll.error} consecutive={st.consecutive_failures} "
                    f"next_interval_s={self.current_interval_s():.1f}"
                )

    # ------------------------------------------------------------------ run
    async def _cycle(self, handler: PollHandler) -> None:
        poll = await self.fetch()
        try:
            res = handler(poll)
            if inspect.isawaitable(res):
                await res
        except Exception:
            logger.opt(exception=True).error(f"sina.handler_failed seq={poll.seq}")

    async def run(
        self,
        handler: PollHandler,
        *,
        stop: asyncio.Event | None = None,
        keep_running: Callable[[], bool] | None = None,
    ) -> None:
        """Poll until ``stop`` is set or ``keep_running()`` turns False.

        Each cycle is ``fetch()`` followed by ``handler(poll)`` (sync or async).
        Cycles run one after another, so they never overlap and polls reach the
        handler in order. The next start is chosen *after* a cycle completes,
        as its start time plus :meth:`current_interval_s` (which already
        reflects that cycle's outcome); ticks that elapsed while a slow cycle
        was running are skipped. ``stop`` and ``keep_running`` are checked
        between cycles (at least every second while waiting), so an in-flight
        cycle always finishes and its data is not lost.
        """
        loop = asyncio.get_running_loop()
        next_at = loop.time()
        while True:
            if stop is not None and stop.is_set():
                return
            if keep_running is not None and not keep_running():
                return
            now = loop.time()
            if next_at > now:
                await _sleep_or_stop(min(next_at - now, _MAX_SLEEP_CHUNK_S), stop)
                continue
            started = loop.time()
            await self._cycle(handler)
            interval = self.current_interval_s()
            self.stats.last_interval_s = interval
            next_at = started + interval
            now = loop.time()
            if next_at < now:  # the cycle overran its slot(s): skip them instead of bursting
                missed = math.ceil((now - next_at) / interval)
                self.stats.skipped += missed
                next_at += missed * interval


async def _sleep_or_stop(delay_s: float, stop: asyncio.Event | None) -> None:
    if delay_s <= 0:
        await asyncio.sleep(0)
        return
    if stop is None:
        await asyncio.sleep(delay_s)
        return
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=delay_s)


# ---------------------------------------------------------------------------
# MarketDataSource adapter (02 §10)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SinaBatch:
    """What a sink receives per poll: the raw poll plus one result per requested code.

    ``results`` is empty when the poll failed (``poll.ok`` is False).
    """

    poll: SinaPoll
    results: tuple[SinaResult, ...] = field(default_factory=tuple)


BatchSink = Callable[[SinaBatch], Awaitable[None] | None]


class SinaSource:
    """Sina as a ``MarketDataSource`` (02 §10): ``capabilities``, ``start(sink)``, ``stop()``.

    No ``request_replay``: Sina has neither sequence numbers nor history, so
    gaps are only filled by other sources (01 §8.13).
    """

    source_id: Final = SourceId.SINA
    capabilities: Final = SINA_CAPABILITIES

    def __init__(self, poller: SinaPoller, normalizer: SinaNormalizer) -> None:
        self._poller = poller
        self._normalizer = normalizer
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def poller(self) -> SinaPoller:
        return self._poller

    @property
    def running(self) -> bool:
        return self._task is not None and not self._task.done()

    def batch_for(self, poll: SinaPoll) -> SinaBatch:
        """Normalize one poll (also usable without ``start``, e.g. for replays of archived polls)."""
        if not poll.ok:
            return SinaBatch(poll)
        return SinaBatch(poll, tuple(self._normalizer.normalize_text(poll.text(), poll.recv_ns, poll.codes)))

    async def start(self, sink: BatchSink) -> None:
        """Start polling in a background task and return immediately."""
        if self.running:
            raise RuntimeError("SinaSource already started")
        self._stop.clear()

        async def handler(poll: SinaPoll) -> None:
            res = sink(self.batch_for(poll))
            if inspect.isawaitable(res):
                await res

        self._task = asyncio.create_task(self._poller.run(handler, stop=self._stop), name="sina-source")

    async def stop(self) -> None:
        """Stop polling (the in-flight poll is delivered first) and close the HTTP client."""
        self._stop.set()
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        await self._poller.aclose()
