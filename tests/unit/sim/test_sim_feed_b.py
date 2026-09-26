"""Feed B vendor-style REST quotes (02 §9.2): JSON shape, symbol filter, consistency with Feed A."""

from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any

import httpx
import pytest

from futrisk.sim.exchange import SimExchange

from .helpers import FeedAClient, wait_for

pytestmark = pytest.mark.timeout(30)

RECORD_KEYS = {"sym", "ex", "px", "bp", "ap", "bv", "av", "vol", "amt", "oi", "up", "dn", "ps", "pc", "o", "h", "l", "t"}


def _client() -> httpx.AsyncClient:
    return httpx.AsyncClient(trust_env=False, timeout=5)


class TestFeedB:
    async def test_quotes_shape(self, exchange: SimExchange) -> None:
        async with _client() as c:
            r = await c.get(exchange.feed_b_url)
        assert r.status_code == 200 and r.headers["content-type"].startswith("application/json")
        doc = r.json()
        assert set(doc) == {"ts", "td", "data"}
        assert doc["td"] == "20260928" and isinstance(doc["ts"], int)
        assert len(doc["data"]) == len(exchange.engine.metas)
        for rec in doc["data"]:
            assert set(rec) == RECORD_KEYS
            assert rec["sym"] == rec["sym"].upper()
            meta = exchange.engine.meta(next(i for i in exchange.engine.instrument_ids if i.upper() == rec["sym"]))
            assert meta is not None and rec["ex"] == meta.exchange
            for key in ("px", "up", "dn", "ps", "pc", "amt"):
                assert isinstance(rec[key], str)
                Decimal(rec[key])  # parses as a plain decimal
            for key in ("px", "up", "dn"):
                assert Decimal(rec[key]) % meta.tick == 0
            for key in ("bp", "ap", "o", "h", "l"):
                assert rec[key] is None or isinstance(rec[key], str)
            for key in ("bv", "av", "vol", "oi", "t"):
                assert isinstance(rec[key], int)
            assert Decimal(rec["dn"]) <= Decimal(rec["px"]) <= Decimal(rec["up"])

    async def test_symbols_filter_is_case_insensitive(self, exchange: SimExchange) -> None:
        async with _client() as c:
            r = await c.get(exchange.feed_b_url, params={"symbols": "rb2701,IF2612,XX0000"})
        assert [d["sym"] for d in r.json()["data"]] == ["RB2701", "IF2612"]

    async def test_same_state_as_feed_a(self, exchange: SimExchange) -> None:
        await exchange.pause()  # no new states: both feeds must show the same snapshot
        async with FeedAClient(exchange.feed_a_url) as ws:
            await ws.subscribe()
            snap = {d["InstrumentID"]: d for d in (await ws.recv())["ticks"]}
        async with _client() as c:
            data = (await c.get(exchange.feed_b_url)).json()["data"]
        for rec in data:
            iid = next(i for i in snap if i.upper() == rec["sym"])
            a = snap[iid]
            assert Decimal(rec["px"]) == Decimal(str(a["LastPrice"]))
            assert rec["vol"] == a["Volume"] and rec["oi"] == int(a["OpenInterest"])
            assert Decimal(rec["amt"]) == Decimal(str(a["Turnover"]))
            if rec["bp"] is not None:
                assert Decimal(rec["bp"]) == Decimal(str(a["BidPrice1"])) and rec["bv"] == a["BidVolume1"]

    async def test_health(self, exchange: SimExchange) -> None:
        async with _client() as c:
            r = await c.get(exchange.feed_b_url.replace("/quotes", "/health"))
        doc = r.json()
        assert r.status_code == 200 and doc["status"] == "ok" and isinstance(doc["ts"], int)
        assert doc["epoch"] == exchange.epoch



def _record(doc: dict[str, Any], sym: str) -> dict[str, Any]:
    return next(d for d in doc["data"] if d["sym"] == sym)


class TestFeedBFaults:
    async def test_duplicate_serves_the_previous_body(self, exchange: SimExchange) -> None:
        await exchange.faults.add("FEED_DUPLICATE", "feed_b", {"ratio": 1.0})
        async with _client() as c:
            first = await c.get(exchange.feed_b_url)
            await asyncio.sleep(0.15)  # new market states in between
            second = await c.get(exchange.feed_b_url)
        assert second.content == first.content

    async def test_reorder_serves_older_snapshots(self, exchange: SimExchange) -> None:
        await wait_for(lambda: exchange.engine.rounds >= 25)
        await exchange.faults.add("FEED_REORDER", "feed_b", {"window": 20})
        seen = []
        async with _client() as c:
            for _ in range(15):
                seen.append(_record((await c.get(exchange.feed_b_url)).json(), "RB2701")["t"])
                await asyncio.sleep(0.02)
        assert any(b < a for a, b in zip(seen, seen[1:], strict=False))  # older data after newer

    async def test_bad_tick_corrupts_one_answer(self, exchange: SimExchange) -> None:
        await exchange.pause()  # a stable record
        async with _client() as c:
            good = _record((await c.get(exchange.feed_b_url)).json(), "RB2701")
            await exchange.faults.add("BAD_TICK", "feed_b", {"instrument": "rb2701", "offset_pct": 0.05})
            bad = _record((await c.get(exchange.feed_b_url)).json(), "RB2701")
            again = _record((await c.get(exchange.feed_b_url)).json(), "RB2701")
        assert abs(Decimal(bad["px"]) - Decimal(good["px"]) * Decimal("1.05")) <= 1
        assert bad["vol"] == good["vol"]  # with_volume=false
        assert again == good

    async def test_malformed_record_keeps_valid_json(self, exchange: SimExchange) -> None:
        await exchange.faults.add("MALFORMED", "feed_b", {"ratio": 1.0, "mode": "record"})
        async with _client() as c:
            doc = json.loads((await c.get(exchange.feed_b_url)).content)
        assert doc["data"][0]["px"] == "N/A" and doc["data"][0]["vol"] == -1
        assert all(d["px"] != "N/A" for d in doc["data"][1:])

    async def test_freeze_pins_one_record(self, exchange: SimExchange) -> None:
        await exchange.faults.add("FEED_FREEZE", "feed_b", {"instrument": "m2701"})
        async with _client() as c:
            first = (await c.get(exchange.feed_b_url)).json()
            rounds = exchange.engine.rounds
            await wait_for(lambda: exchange.engine.rounds >= rounds + 5)
            second = (await c.get(exchange.feed_b_url)).json()
        assert _record(second, "M2701") == _record(first, "M2701")
        assert _record(second, "RB2701")["t"] > _record(first, "RB2701")["t"]
