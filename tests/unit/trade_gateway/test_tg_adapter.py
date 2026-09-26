"""SimBrokerAdapter against a scripted ROUTER (02 §9.4): login, order_ref, reports, query pages, RspError,
heartbeat liveness, reconnect with a fresh session, login failure backoff, no synchronous callbacks."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest
import zmq

from futrisk.messages.enums import BrokerConnState, Direction, Exchange, Offset, OrderType, TimeInForce
from futrisk.messages.orders import RouteOrder
from futrisk.services.trade_gateway.broker import (
    ActionError,
    BrokerSession,
    BrokerUnavailable,
    InsertError,
    InvalidReport,
    OrderReport,
    QueryResult,
    SimBrokerAdapter,
    TradeReport,
)

pytestmark = pytest.mark.timeout(30)


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, Any]] = []

    def on_status(self, state: BrokerConnState, session: BrokerSession | None) -> None:
        self.events.append(("status", (state, session)))

    def on_order(self, report: OrderReport) -> None:
        self.events.append(("order", report))

    def on_trade(self, report: TradeReport) -> None:
        self.events.append(("trade", report))

    def on_invalid(self, report: InvalidReport) -> None:
        self.events.append(("invalid", report))

    def on_insert_error(self, error: InsertError) -> None:
        self.events.append(("insert_error", error))

    def on_action_error(self, error: ActionError) -> None:
        self.events.append(("action_error", error))

    def on_heartbeat(self, trading_day: str, broker_seq_head: int) -> None:
        self.events.append(("heartbeat", (trading_day, broker_seq_head)))

    def of(self, kind: str) -> list[Any]:
        return [e[1] for e in self.events if e[0] == kind]

    def states(self) -> list[BrokerConnState]:
        return [s for s, _ in self.of("status")]


class Router:
    """The test side of the SimBroker link (same thread as the adapter)."""

    def __init__(self, ctx: zmq.Context[Any]) -> None:
        self.sock: zmq.Socket[Any] = ctx.socket(zmq.ROUTER)
        self.sock.setsockopt(zmq.LINGER, 0)
        self.sock.setsockopt(zmq.ROUTER_HANDOVER, 1)
        self.sock.bind("tcp://127.0.0.1:*")
        self.endpoint = self.sock.getsockopt_string(zmq.LAST_ENDPOINT)
        self.identity: bytes | None = None
        self.received: list[dict[str, Any]] = []

    def recv_all(self) -> list[dict[str, Any]]:
        out = []
        while self.sock.poll(0):
            ident, data = self.sock.recv_multipart()
            self.identity = ident
            msg = json.loads(data)
            self.received.append(msg)
            out.append(msg)
        return out

    def send(self, msg: dict[str, Any]) -> None:
        assert self.identity is not None
        self.sock.send_multipart([self.identity, json.dumps(msg).encode()])


@pytest.fixture
def zctx() -> Iterator[zmq.Context[Any]]:
    ctx: zmq.Context[Any] = zmq.Context()
    yield ctx
    ctx.destroy(linger=0)


def make_adapter(endpoint: str, **kw: Any) -> tuple[SimBrokerAdapter, Recorder]:
    rec = Recorder()
    ad = SimBrokerAdapter(endpoint, heartbeat_timeout_ms=400, login_timeout_ms=600, backoff_initial_ms=50,
                          backoff_max_ms=200, query_timeout_ms=1_000, **kw)
    ad.bind(rec)
    ad.start()
    return ad, rec


def spin(ad: SimBrokerAdapter, router: Router, until: Callable[[], bool], *, serve: Callable[[dict[str, Any]], None] | None = None,
         timeout: float = 5.0) -> None:
    end = time.monotonic() + timeout
    while not until():
        if time.monotonic() > end:
            raise AssertionError("condition not met in time")
        ad.poll_timers(time.perf_counter_ns())
        ad.pump()
        for msg in router.recv_all():
            if serve is not None:
                serve(msg)
        time.sleep(0.002)


def login_reply(router: Router, msg: dict[str, Any], *, session_id: int = 7, max_ref: str = "12", head: int = 41,
                error: dict[str, str] | None = None) -> None:
    router.send({"msg": "RspUserLogin", "req_id": msg["req_id"], "trading_day": "20260928", "front_id": 1,
                 "session_id": session_id, "max_order_ref": max_ref, "broker_seq_head": head, "error": error})


def order(**kw: Any) -> RouteOrder:
    base: dict[str, Any] = dict(client_order_id="c1", account_id="ACC3", instrument_id="rb2701", exchange=Exchange.SHFE,
                                direction=Direction.BUY, offset=Offset.OPEN, order_type=OrderType.LIMIT,
                                tif=TimeInForce.GFD, price=3120.0, qty=2, trading_day="20260928", risk_passed_ns=1,
                                expire_ns=2, src_instance="risk-engine-1")
    base.update(kw)
    return RouteOrder(**base)


def test_login_orders_reports_and_queries(zctx: zmq.Context[Any]) -> None:
    router = Router(zctx)
    ad, rec = make_adapter(router.endpoint)
    try:
        with pytest.raises(BrokerUnavailable):
            ad.insert_order(order())
        spin(ad, router, lambda: ad.state is BrokerConnState.LOGGED_IN,
             serve=lambda m: login_reply(router, m) if m["msg"] == "ReqUserLogin" else None)
        assert router.identity == b"trade-gateway"
        assert rec.states() == [BrokerConnState.CONNECTED, BrokerConnState.LOGGED_IN]
        sess = rec.of("status")[-1][1]
        assert (sess.trading_day, sess.front_id, sess.session_id, sess.broker_seq_head) == ("20260928", 1, 7, 41)

        ref = ad.insert_order(order())
        assert ref == "1:7:13"  # max_order_ref + 1
        spin(ad, router, lambda: any(m["msg"] == "ReqOrderInsert" for m in router.received))
        ins = next(m for m in router.received if m["msg"] == "ReqOrderInsert")
        assert (ins["order_ref"], ins["client_order_id"], ins["comb_offset_flag"], ins["volume_total_original"]) == ("13", "c1", "0", 2)

        rtn = {"msg": "RtnOrder", "broker_seq": 42, "trading_day": "20260928", "front_id": 1, "session_id": 7,
               "order_ref": "13", "order_sys_id": "31", "client_order_id": "c1", "investor_id": "ACC3",
               "instrument_id": "rb2701", "exchange_id": "SHFE", "direction": "0", "comb_offset_flag": "0",
               "order_price_type": "LIMIT", "time_condition": "GFD", "volume_condition": "AV", "limit_price": 3120.0,
               "volume_total_original": 2, "volume_traded": 0, "volume_total": 2, "order_status": "ACCEPTED",
               "status_msg": "ACCEPTED", "update_ns": 5}
        router.send(rtn)
        router.send({**rtn, "msg": "RtnOrder", "broker_seq": 43, "direction": "?"})
        router.send({"msg": "Heartbeat", "ts": 1, "broker_seq_head": 43, "trading_day": "20260928"})
        router.send({"msg": "ErrRtnOrderInsert", "req_id": 99, "order_ref": "14", "client_order_id": "c2",
                     "order_sys_id": None, "error_id": "BROKER_BAD_REQUEST", "error_msg": "bad"})
        spin(ad, router, lambda: bool(rec.of("insert_error")))
        assert [r.broker_seq for r in rec.of("order")] == [42]
        assert [r.broker_seq for r in rec.of("invalid")] == [43] and ad.stats.invalid_reports == 1
        assert rec.of("heartbeat") == [("20260928", 43)]
        assert rec.of("insert_error")[0].client_order_id == "c2"

        ad.cancel("c1", broker_order_ref=ref, order_sys_id="31")
        spin(ad, router, lambda: any(m["msg"] == "ReqOrderAction" for m in router.received))
        act = next(m for m in router.received if m["msg"] == "ReqOrderAction")
        router.send({"msg": "ErrRtnOrderAction", "req_id": act["req_id"], "error_id": "BROKER_ORDER_FINISHED",
                     "error_msg": "done"})
        spin(ad, router, lambda: bool(rec.of("action_error")))
        assert rec.of("action_error")[0].client_order_id == "c1"

        results: list[QueryResult[Any]] = []
        ad.query_trades("20260928", 40, results.append)
        assert results == []  # never synchronous
        spin(ad, router, lambda: any(m["msg"] == "ReqQryTrade" for m in router.received))
        q = next(m for m in router.received if m["msg"] == "ReqQryTrade")
        assert (q["trading_day"], q["from_broker_seq"]) == ("20260928", 40)
        trade = {"broker_seq": 44, "trading_day": "20260928", "trade_id": "19", "front_id": 1, "session_id": 7,
                 "order_ref": "13", "order_sys_id": "31", "client_order_id": "c1", "investor_id": "ACC3",
                 "instrument_id": "rb2701", "exchange_id": "SHFE", "direction": "0", "offset_flag": "0",
                 "price": 3119.0, "volume": 2, "trade_ts_ns": 7}
        router.send({"msg": "RspQryTrade", "req_id": q["req_id"], "items": [trade], "is_last": False})
        router.send({"msg": "RspQryTrade", "req_id": q["req_id"], "items": [{**trade, "broker_seq": 45, "trade_id": "20"}],
                     "is_last": True})
        spin(ad, router, lambda: bool(results))
        assert results[0].ok and [t.broker_seq for t in results[0].items] == [44, 45]

        ad.query_orders("20260928", 1, results.append)
        spin(ad, router, lambda: any(m["msg"] == "ReqQryOrder" for m in router.received))
        qo = next(m for m in router.received if m["msg"] == "ReqQryOrder")
        router.send({"msg": "RspError", "req_id": qo["req_id"], "error_id": "BROKER_BAD_REQUEST", "error_msg": "x"})
        spin(ad, router, lambda: len(results) == 2)
        assert results[1].error == "BROKER_BAD_REQUEST"
    finally:
        ad.stop()


def test_heartbeat_timeout_relogs_in_with_a_fresh_session(zctx: zmq.Context[Any]) -> None:
    router = Router(zctx)
    ad, rec = make_adapter(router.endpoint)
    sessions = iter([7, 8])
    pending: list[QueryResult[Any]] = []

    def serve(m: dict[str, Any]) -> None:
        if m["msg"] == "ReqUserLogin":
            login_reply(router, m, session_id=next(sessions))

    try:
        spin(ad, router, lambda: ad.state is BrokerConnState.LOGGED_IN, serve=serve)
        ad.query_orders("20260928", 1, pending.append)  # never answered: failed by the link loss
        spin(ad, router, lambda: BrokerConnState.DISCONNECTED in rec.states() and bool(pending), serve=None)
        assert pending[0].error == "BROKER_DISCONNECTED" and ad.stats.disconnects == 1
        spin(ad, router, lambda: rec.states().count(BrokerConnState.LOGGED_IN) == 2, serve=serve)
        assert ad.session is not None and ad.session.session_id == 8
        assert rec.states() == [BrokerConnState.CONNECTED, BrokerConnState.LOGGED_IN, BrokerConnState.DISCONNECTED,
                                BrokerConnState.CONNECTED, BrokerConnState.LOGGED_IN]
    finally:
        ad.stop()


def test_login_failure_backs_off_and_retries(zctx: zmq.Context[Any]) -> None:
    router = Router(zctx)
    ad, rec = make_adapter(router.endpoint)
    attempts: list[int] = []

    def serve(m: dict[str, Any]) -> None:
        if m["msg"] == "ReqUserLogin":
            attempts.append(m["req_id"])
            err = {"error_id": "BROKER_LOGIN_FAILED", "error_msg": "no"} if len(attempts) < 3 else None
            login_reply(router, m, error=err)

    try:
        spin(ad, router, lambda: ad.state is BrokerConnState.LOGGED_IN, serve=serve)
        assert len(attempts) == 3 and ad.stats.login_failures == 2
    finally:
        ad.stop()


def test_no_broker_means_disconnected_and_unavailable(zctx: zmq.Context[Any]) -> None:
    ad, rec = make_adapter("tcp://127.0.0.1:1")  # nothing listens there
    results: list[QueryResult[Any]] = []
    try:
        ad.query_positions("20260928", results.append)
        assert results == []
        end = time.monotonic() + 1.0
        while time.monotonic() < end and not results:
            ad.poll_timers(time.perf_counter_ns())
            time.sleep(0.01)
        assert results and results[0].error == "BROKER_NOT_LOGGED_IN"
        assert ad.state is BrokerConnState.DISCONNECTED
        with pytest.raises(BrokerUnavailable):
            ad.cancel("c1", order_sys_id="1")
    finally:
        ad.stop()
