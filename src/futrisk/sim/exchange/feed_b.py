"""Feed B: vendor-style polled quotes over HTTP (02 §9.2).

- ``GET /v1/quotes?symbols=RB2701,IF2612``: the virtual exchange's *latest* state of each
  contract, no resampling, so it normalises to the same fingerprint as the matching
  Feed A tick. ``symbols`` is case-insensitive and optional (all contracts);
- ``GET /v1/health``: ``{"status": "ok", "ts": …}``; it reports process health and stays
  200 during feed faults (supervisor uses it to decide whether to restart sim-exchange).

The top-level ``td`` is the trading day of the current market time; a contract that has
not traded yet in that trading day (e.g. CFFEX during the night session) keeps its last
state, whose own ``t`` places it in the previous trading day.

Faults (02 §9.5, target ``feed_b``): ``FEED_DISCONNECT`` -> HTTP 503; ``FEED_LATENCY``
delays the answer; ``FEED_DUPLICATE`` resends the previous body; ``FEED_REORDER`` serves
an older snapshot; ``MALFORMED`` truncates the body (``mode=body``) or corrupts one record
(``mode=record``); ``FEED_FREEZE`` pins a contract's record; ``BAD_TICK`` corrupts one
record once.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Sequence
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response

from futrisk.common.calendar import TradingCalendar
from futrisk.common.clock import NS_PER_MS, MarketClock, td_str
from futrisk.sim.exchange.engine import InstrumentState
from futrisk.sim.exchange.faults import FEED_B, FaultManager
from futrisk.sim.exchange.formats import corrupted_state, dumps_bytes, feed_b_record
from futrisk.sim.exchange.servers import UvicornRunner

HISTORY = 64


class FeedBService:
    """State and HTTP app of Feed B."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        clock: MarketClock,
        calendar: TradingCalendar,
        faults: FaultManager,
        health_extra: Any = None,
    ) -> None:
        self._clock = clock
        self._calendar = calendar
        self._faults = faults
        self._views: dict[str, dict[str, Any]] = {}
        self._states: dict[str, InstrumentState] = {}
        self._history: deque[dict[str, dict[str, Any]]] = deque(maxlen=HISTORY)
        self._last_body: bytes | None = None
        self._health_extra = health_extra
        self.requests = 0
        self.app = self._make_app()
        self._runner = UvicornRunner(self.app, host, port, name="feed-b")

    @property
    def host(self) -> str:
        return self._runner.host

    @property
    def port(self) -> int:
        return self._runner.port

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}/v1/quotes"

    async def start(self) -> None:
        await self._runner.start()

    async def stop(self) -> None:
        await self._runner.stop()

    # ------------------------------------------------------------------ state
    def init_views(self, states: Sequence[InstrumentState]) -> None:
        for st in states:
            self._views[st.instrument_id] = feed_b_record(st)
            self._states[st.instrument_id] = st.copy()

    def publish(self, states: Sequence[InstrumentState]) -> None:
        if not states:
            return
        for st in states:
            if self._faults.feed_frozen(FEED_B, st.instrument_id):
                continue
            self._views[st.instrument_id] = feed_b_record(st)
            self._states[st.instrument_id] = st.copy()
        self._history.append(dict(self._views))

    def _trading_day(self) -> str:
        try:
            return td_str(self._calendar.current_or_next_trading_day(self._clock.now_ns()))
        except LookupError:
            return ""

    def build_body(self, symbols: str | None) -> tuple[int, bytes]:
        """``(status, body)`` of one ``/v1/quotes`` request, faults applied."""
        self.requests += 1
        if self._faults.first("FEED_DISCONNECT", FEED_B) is not None:
            return 503, dumps_bytes({"status": "unavailable", "reason": "FEED_DISCONNECT"})
        dup = self._faults.first("FEED_DUPLICATE", FEED_B)
        if dup is not None and self._last_body is not None and dup.rng.random() < dup.params["ratio"]:
            dup.detail["duplicated"] = dup.detail.get("duplicated", 0) + 1
            return 200, self._last_body
        view = self._views
        reorder = self._faults.first("FEED_REORDER", FEED_B)
        if reorder is not None and len(self._history) > 1:
            k = reorder.rng.randint(1, min(reorder.params["window"], len(self._history)))
            view = self._history[-k]
            reorder.detail["served_older"] = reorder.detail.get("served_older", 0) + (k > 1)
        wanted: set[str] | None = None
        if symbols:
            wanted = {s.strip().upper() for s in symbols.split(",") if s.strip()}
        records: list[dict[str, Any]] = []
        for iid, rec in view.items():
            if wanted is not None and rec["sym"] not in wanted:
                continue
            bad = self._faults.take_bad_tick(FEED_B, iid)
            if bad is not None and iid in self._states:
                st = self._states[iid]
                rec = feed_b_record(corrupted_state(
                    st, bad.params["offset_pct"], bad.params["with_volume"], st.ts_ns + NS_PER_MS))
            records.append(rec)
        malformed = self._faults.first("MALFORMED", FEED_B)
        corrupt = malformed is not None and malformed.rng.random() < malformed.params["ratio"]
        if corrupt and malformed is not None and malformed.params["mode"] == "record" and records:
            records = [dict(records[0], px="N/A", vol=-1), *records[1:]]
        body = dumps_bytes({"ts": self._clock.now_ns() // NS_PER_MS, "td": self._trading_day(), "data": records})
        if corrupt and malformed is not None:
            malformed.detail["corrupted"] = malformed.detail.get("corrupted", 0) + 1
            if malformed.params["mode"] == "body":
                body = body[: max(1, len(body) // 2)]
        self._last_body = body
        return 200, body

    # ------------------------------------------------------------------ app
    def _make_app(self) -> FastAPI:
        app = FastAPI(title="sim-exchange Feed B", docs_url=None, redoc_url=None, openapi_url=None)
        service = self

        @app.get("/v1/quotes")
        async def quotes(symbols: str | None = None) -> Response:
            status, body = service.build_body(symbols)
            latency = service._faults.latency_ms(FEED_B)
            if latency > 0:
                await asyncio.sleep(latency / 1000.0)
            return Response(content=body, status_code=status, media_type="application/json")

        @app.get("/v1/health")
        async def health() -> JSONResponse:
            doc: dict[str, Any] = {"status": "ok", "ts": service._clock.now_ns() // NS_PER_MS}
            if service._health_extra is not None:
                doc.update(service._health_extra())
            return JSONResponse(doc)

        return app

    def state(self) -> dict[str, Any]:
        return {"url": self.url, "requests": self.requests, "instruments": len(self._views)}
