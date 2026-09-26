"""Client order book (02 §7.4, §8.3): submit outcomes, idempotent/monotonic updates, fill deltas."""

from __future__ import annotations

from typing import Any

import msgspec

from futrisk.client.orders import ClientState, OrderBook
from futrisk.messages import codes
from futrisk.messages.enums import (
    Direction,
    Exchange,
    Offset,
    OrderStatus,
    OrderType,
    PosDirection,
    TimeInForce,
)
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.orders import FillRef, OrderAck, OrderRequest, OrderUpdate


def req(cid: str = "o-1", *, offset: Offset = Offset.OPEN, direction: Direction = Direction.BUY, qty: int = 10,
        instrument: str = "rb2701") -> OrderRequest:
    return OrderRequest(client_order_id=cid, account_id="ACC3", instrument_id=instrument, direction=direction,
                        offset=offset, price=3120.0, qty=qty, sent_ns=1_000)


def upd(r: OrderRequest, status: OrderStatus, *, filled: int = 0, ts: int = 2_000, **kw: Any) -> OrderUpdate:
    return OrderUpdate(client_order_id=r.client_order_id, account_id=r.account_id, instrument_id=r.instrument_id,
                       exchange=Exchange.SHFE, direction=r.direction, offset=r.offset, order_type=OrderType.LIMIT,
                       tif=TimeInForce.GFD, price=r.price, qty=r.qty, status=status, filled_qty=filled,
                       source=r.source, trading_day="20260928", created_ns=1_000, updated_ns=ts, **kw)


def ack(r: OrderRequest, accepted: bool = True, code: str | None = None) -> OrderAck:
    return OrderAck(client_order_id=r.client_order_id, accepted=accepted,
                    status=OrderStatus.ROUTING if accepted else OrderStatus.REJECTED, reject_code=code,
                    trading_day="20260928")


def test_submit_outcomes() -> None:
    book = OrderBook()
    a, b, c, d = req("a"), req("b"), req("c"), req("d")
    for r in (a, b, c, d):
        assert book.add_request(r).state == ClientState.PENDING
    book.on_submit_reply("a", ok_reply(ack(a)), ack(a))
    assert (book.get("a").state, book.get("a").status, book.get("a").working) == (ClientState.ACKED, OrderStatus.ROUTING, True)
    rej = ack(b, False, codes.REJECT_ACCOUNT_HALTED)
    book.on_submit_reply("b", err_reply(codes.REJECT_ACCOUNT_HALTED, data=rej), rej)
    ob = book.get("b")
    assert ob is not None and ob.terminal and ob.reject_code == codes.REJECT_ACCOUNT_HALTED
    # no answer after the retries: maybe accepted -> in doubt, still working
    book.on_submit_reply("c", Reply(ok=False, code=codes.TIMEOUT), None)
    assert book.get("c").state == ClientState.IN_DOUBT and book.get("c").working
    book.apply_update(upd(c, OrderStatus.ACCEPTED))
    assert book.get("c").state == ClientState.ACKED
    # non-retryable error without OrderAck
    book.on_submit_reply("d", err_reply(codes.FORBIDDEN), None)
    assert book.get("d").terminal and book.get("d").reject_code == codes.FORBIDDEN
    assert {o.client_order_id for o in book.working("ACC3")} == {"a", "c"}
    assert book.mark_lost("c") is not None and not book.get("c").working


def test_fill_deltas_idempotent_and_monotonic() -> None:
    book = OrderBook()
    r = req("x", qty=10)
    book.add_request(r)
    _, f = book.apply_update(upd(r, OrderStatus.ACCEPTED, ts=2_000))
    assert f is None
    _, f = book.apply_update(upd(r, OrderStatus.PARTIALLY_FILLED, filled=4, ts=3_000, avg_fill_price=3119.0,
                                 last_fill=FillRef(trade_id="t1", price=3119.0, qty=4, trade_ts_ns=3_000)))
    assert f is not None and (f.qty, f.price) == (4, 3119.0)
    # duplicate delivery: no new fill
    _, f = book.apply_update(upd(r, OrderStatus.PARTIALLY_FILLED, filled=4, ts=3_000, avg_fill_price=3119.0))
    assert f is None
    # second fill without last_fill: price derived from the average
    o, f = book.apply_update(upd(r, OrderStatus.FILLED, filled=10, ts=4_000, avg_fill_price=3120.2))
    assert f is not None and f.qty == 6 and abs(f.price - (3120.2 * 10 - 3119.0 * 4) / 6) < 1e-9
    assert o.terminal and o.state == ClientState.DONE and o.remaining == 0
    # an older update replayed late cannot regress the order
    o, f = book.apply_update(upd(r, OrderStatus.ACCEPTED, filled=0, ts=2_500))
    assert f is None and o.status == OrderStatus.FILLED and o.filled_qty == 10


def test_terminal_not_regressed_but_late_fill_applies() -> None:
    book = OrderBook()
    r = req("y", qty=5)
    book.add_request(r)
    book.apply_update(upd(r, OrderStatus.CANCELED, ts=5_000))
    o, f = book.apply_update(upd(r, OrderStatus.CANCEL_PENDING, ts=6_000))
    assert f is None and o.status == OrderStatus.CANCELED
    # 02 §8.3: a fill after a terminal state is a fact and must be applied
    o, f = book.apply_update(upd(r, OrderStatus.FILLED, filled=5, ts=7_000,
                                 last_fill=FillRef(trade_id="t", price=3121.0, qty=5, trade_ts_ns=7_000)))
    assert f is not None and f.qty == 5 and o.status == OrderStatus.FILLED


def test_foreign_orders_and_baseline() -> None:
    book = OrderBook()
    manual = req("manual", qty=3)
    o, f = book.apply_update(upd(manual, OrderStatus.FILLED, filled=3, last_fill=FillRef(
        trade_id="t", price=3100.0, qty=3, trade_ts_ns=1)))
    assert not o.own and f is not None and f.qty == 3
    old = req("old", qty=4)
    o, f = book.apply_update(upd(old, OrderStatus.PARTIALLY_FILLED, filled=2), baseline=True)
    assert f is None and o.baseline_filled == 2  # already part of a position snapshot
    o, f = book.apply_update(upd(old, OrderStatus.FILLED, filled=4, ts=9_000))
    assert f is not None and f.qty == 2


def test_pending_closes_by_bucket() -> None:
    book = OrderBook()
    ct = req("ct", offset=Offset.CLOSE_TODAY, direction=Direction.SELL, qty=3)
    cy = req("cy", offset=Offset.CLOSE_YESTERDAY, direction=Direction.SELL, qty=5)
    cs = req("cs", offset=Offset.CLOSE_TODAY, direction=Direction.BUY, qty=7)  # closes shorts
    for r in (ct, cy, cs):
        book.add_request(r)
        book.on_submit_reply(r.client_order_id, ok_reply(ack(r)), ack(r))
    book.apply_update(upd(cy, OrderStatus.PARTIALLY_FILLED, filled=1))
    p = book.pending_closes("ACC3", "rb2701", PosDirection.LONG)
    assert (p.td, p.yd, p.any, p.total) == (3, 4, 0, 7)
    assert book.pending_closes("ACC3", "rb2701", PosDirection.SHORT).td == 7
    assert msgspec.structs.asdict(ack(ct))["accepted"] is True


def test_lost_order_revived_by_a_non_terminal_update() -> None:
    book = OrderBook()
    r = req("z", offset=Offset.CLOSE_TODAY, direction=Direction.SELL, qty=3)
    book.add_request(r)
    book.on_submit_reply("z", Reply(ok=False, code=codes.TIMEOUT), None)
    book.mark_lost("z")
    assert not book.get("z").working and book.pending_closes("ACC3", "rb2701", PosDirection.LONG).td == 0
    o, _ = book.apply_update(upd(r, OrderStatus.ACCEPTED, ts=5_000))
    assert o.state == ClientState.ACKED and o.working  # alive after all: managed (TTL) and counted again
    assert book.pending_closes("ACC3", "rb2701", PosDirection.LONG).td == 3
    book.mark_lost("z")
    o, f = book.apply_update(upd(r, OrderStatus.FILLED, filled=3, ts=6_000))
    assert o.state == ClientState.DONE and f is not None and f.qty == 3


def test_client_clock_never_compared_with_engine_times() -> None:
    """probe_b5: a demo market clock three days behind the wall clock must not drop engine updates."""
    wall = 1_790_559_300_000_000_000
    offset = -3 * 86_400 * 10**9
    book = OrderBook()
    r = msgspec.structs.replace(req("w"), sent_ns=wall)  # a wall-clock sent_ns
    book.add_request(r)
    assert book.get("w").updated_ns == 0
    book.on_submit_reply("w", ok_reply(ack(r)), ack(r))
    o, _ = book.apply_update(upd(r, OrderStatus.ACCEPTED, ts=wall + offset + 10**9))
    assert o.status == OrderStatus.ACCEPTED
    o, _ = book.apply_update(upd(r, OrderStatus.CANCELED, ts=wall + offset + 2 * 10**9))
    assert o.status == OrderStatus.CANCELED and o.terminal
