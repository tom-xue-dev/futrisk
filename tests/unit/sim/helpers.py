"""Test clients and helpers for the sim-exchange tests (Feed A, SimBroker, config)."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

import zmq
from websockets.asyncio.client import ClientConnection, connect

from futrisk.common.clock import BJ, bj_to_ns
from futrisk.sim.exchange.testing import MARKET_OPEN, offset_for
from futrisk.sim.exchange.testing import make_test_config as make_cfg

__all__ = ["MARKET_OPEN", "BrokerClient", "FeedAClient", "make_cfg", "offset_for", "tick_ns", "wait_for"]


def tick_ns(d: dict[str, Any]) -> int:
    """Exchange time (UTC ns) of a FeedATick: ActionDay + UpdateTime + UpdateMillisec, Beijing time."""
    dt = datetime.strptime(d["ActionDay"] + d["UpdateTime"], "%Y%m%d%H:%M:%S").replace(tzinfo=BJ)
    return bj_to_ns(dt) + int(d["UpdateMillisec"]) * 1_000_000


async def wait_for(pred: Callable[[], bool], timeout: float = 5.0, interval: float = 0.01) -> None:
    end = time.monotonic() + timeout
    while not pred():
        if time.monotonic() > end:
            raise TimeoutError("condition not met in time")
        await asyncio.sleep(interval)


class FeedAClient:
    """Minimal Feed A client over ``websockets``."""

    def __init__(self, url: str) -> None:
        self.url = url
        self.ws: ClientConnection | None = None

    async def __aenter__(self) -> FeedAClient:
        # close_timeout: a test that stops reading leaves the server's close frame queued
        # behind live ticks (client-side backpressure); do not wait the default 10s for it
        self.ws = await connect(
            self.url, proxy=None, compression=None, open_timeout=5, ping_interval=None, close_timeout=0.5
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        assert self.ws is not None
        await self.ws.close()

    async def send(self, obj: dict[str, Any]) -> None:
        assert self.ws is not None
        await self.ws.send(json.dumps(obj))

    async def recv(self, timeout: float = 5.0) -> dict[str, Any]:
        assert self.ws is not None
        raw = await asyncio.wait_for(self.ws.recv(), timeout)
        return json.loads(raw)

    async def recv_until(self, pred: Callable[[dict[str, Any]], bool], timeout: float = 5.0) -> list[dict[str, Any]]:
        """Messages up to and including the first one matching ``pred``."""
        out: list[dict[str, Any]] = []
        end = time.monotonic() + timeout
        while True:
            left = end - time.monotonic()
            if left <= 0:
                raise TimeoutError(f"no matching message; got ops {[m['op'] for m in out][-20:]}")
            msg = await self.recv(left)
            out.append(msg)
            if pred(msg):
                return out

    async def subscribe(self, resume: dict[str, Any] | None = None, req: int = 1, instruments: Any = "*") -> dict[str, Any]:
        await self.send({"op": "subscribe", "req": req, "instruments": instruments, "resume": resume})
        welcome = await self.recv()
        assert welcome["op"] == "welcome", welcome
        return welcome

    async def ticks(self, n: int, timeout: float = 5.0) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        end = time.monotonic() + timeout
        while len(out) < n:
            msg = await self.recv(max(0.01, end - time.monotonic()))
            if msg["op"] == "tick":
                out.append(msg)
        return out


class BrokerClient:
    """DEALER client speaking the SimBroker JSON protocol (polls without zmq.asyncio)."""

    def __init__(self, endpoint: str, identity: bytes = b"trade-gateway") -> None:
        self.ctx = zmq.Context()
        self.sock = self.ctx.socket(zmq.DEALER)
        self.sock.setsockopt(zmq.IDENTITY, identity)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.connect(endpoint)
        self.log: list[dict[str, Any]] = []
        self._buffer: list[dict[str, Any]] = []
        self._ref = 0
        self._req = 0

    def close(self) -> None:
        self.sock.close(0)
        self.ctx.term()

    def __enter__(self) -> BrokerClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def send(self, obj: dict[str, Any]) -> None:
        self.sock.send(json.dumps(obj).encode("utf-8"))

    def _drain(self) -> None:
        while self.sock.poll(0):
            msg = json.loads(self.sock.recv())
            self.log.append(msg)
            if msg.get("msg") != "Heartbeat":
                self._buffer.append(msg)

    async def wait(self, pred: Callable[[dict[str, Any]], bool], timeout: float = 5.0) -> dict[str, Any]:
        """First buffered/incoming non-heartbeat message matching ``pred`` (others stay buffered)."""
        end = time.monotonic() + timeout
        while True:
            self._drain()
            for i, msg in enumerate(self._buffer):
                if pred(msg):
                    return self._buffer.pop(i)
            if time.monotonic() > end:
                raise TimeoutError(f"no matching broker message; buffered: {self._buffer[-10:]}")
            await asyncio.sleep(0.005)

    async def quiet(self, seconds: float) -> list[dict[str, Any]]:
        """Collect the non-heartbeat messages arriving within ``seconds``."""
        await asyncio.sleep(seconds)
        self._drain()
        out, self._buffer = self._buffer, []
        return out

    def next_req(self) -> int:
        self._req += 1
        return self._req

    async def login(self) -> dict[str, Any]:
        req = self.next_req()
        self.send({"msg": "ReqUserLogin", "req_id": req, "broker_id": "9999", "user_id": "tg", "app_id": "futrisk"})
        return await self.wait(lambda m: m.get("msg") == "RspUserLogin" and m.get("req_id") == req)

    def insert(
        self,
        instrument: str,
        exchange: str,
        direction: str,
        offset: str,
        price: float | None,
        qty: int,
        *,
        investor: str = "ACC3",
        tc: str = "GFD",
        vc: str = "AV",
        price_type: str = "LIMIT",
        client_order_id: str | None = None,
        order_ref: str | None = None,
    ) -> str:
        if order_ref is None:
            self._ref += 1
            order_ref = str(self._ref)
        self.send({
            "msg": "ReqOrderInsert", "req_id": self.next_req(), "order_ref": order_ref,
            "client_order_id": client_order_id or f"coid-{order_ref}-{time.perf_counter_ns()}",
            "investor_id": investor, "instrument_id": instrument, "exchange_id": exchange,
            "direction": direction, "comb_offset_flag": offset, "order_price_type": price_type,
            "limit_price": price if price is not None else 0.0, "volume_total_original": qty,
            "time_condition": tc, "volume_condition": vc,
        })
        return order_ref

    async def order_msg(self, order_ref: str, status: str | None = None, timeout: float = 5.0) -> dict[str, Any]:
        return await self.wait(
            lambda m: m.get("msg") == "RtnOrder" and m.get("order_ref") == order_ref
            and (status is None or m.get("order_status") == status),
            timeout,
        )

    async def trade_msg(self, order_ref: str, timeout: float = 5.0) -> dict[str, Any]:
        return await self.wait(lambda m: m.get("msg") == "RtnTrade" and m.get("order_ref") == order_ref, timeout)

    async def positions(self, investor: str | None = None, trading_day: str | None = None) -> list[dict[str, Any]]:
        req = self.next_req()
        body: dict[str, Any] = {"msg": "ReqQryInvestorPosition", "req_id": req, "trading_day": trading_day or ""}
        if investor:
            body["investor_id"] = investor
        self.send(body)
        rsp = await self.wait(lambda m: m.get("msg") == "RspQryInvestorPosition" and m.get("req_id") == req)
        return list(rsp["items"])
