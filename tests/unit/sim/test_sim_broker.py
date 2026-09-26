"""SimBroker over ZeroMQ (02 §9.4): order flows, SHFE close checks, cancels, FAK/FOK, queries, persistence."""

from __future__ import annotations

import time
from datetime import datetime
from decimal import Decimal

import pytest

from futrisk.common.clock import BJ
from futrisk.common.config import FullConfig
from futrisk.common.decimals import limit_prices
from futrisk.common.paths import HomeLayout
from futrisk.sim.exchange import SimExchange
from futrisk.sim.exchange import broker as broker_module

from .helpers import MARKET_OPEN, BrokerClient, make_cfg, offset_for, wait_for

pytestmark = pytest.mark.timeout(30)

BUY, SELL = "0", "1"
OPEN, CLOSE, CLOSE_TODAY, CLOSE_YD = "0", "1", "3", "4"


def limits(ex: SimExchange, iid: str) -> tuple[float, float]:
    """(upper, lower) limit prices: marketable buy / sell prices that always cross."""
    st = ex.engine.states[iid]
    return float(st.meta.price(st.upper)), float(st.meta.price(st.lower))


def pos(items: list[dict[str, object]], investor: str, iid: str, pdir: str) -> tuple[int, int]:
    for p in items:
        if (p["investor_id"], p["instrument_id"], p["posi_direction"]) == (investor, iid, pdir):
            return int(p["today_position"]), int(p["yd_position"])  # type: ignore[call-overload]
    return 0, 0


async def buy_open(b: BrokerClient, ex: SimExchange, iid: str = "rb2701", exch: str = "SHFE", qty: int = 1,
                   investor: str = "ACC3") -> dict[str, object]:
    up, _ = limits(ex, iid)
    ref = b.insert(iid, exch, BUY, OPEN, up, qty, investor=investor)
    return await b.order_msg(ref, "ALL_TRADED")


class TestOrderFlow:
    async def test_login(self, exchange: SimExchange) -> None:
        with BrokerClient(exchange.broker_endpoint) as b:
            rsp = await b.login()
            assert rsp["error"] is None and rsp["trading_day"] == "20260928"
            assert rsp["front_id"] == 1 and rsp["session_id"] >= 1
            assert rsp["broker_seq_head"] == 0 and rsp["max_order_ref"] == "0"
            hb = await b.quiet(1.2)
            assert hb == []
            assert any(m["msg"] == "Heartbeat" and "broker_seq_head" in m for m in b.log)

    async def test_open_then_close_today(self, exchange: SimExchange) -> None:
        up, lo = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            ref = b.insert("rb2701", "SHFE", BUY, OPEN, up, 2, client_order_id="c-open-1")
            acc = await b.order_msg(ref, "ACCEPTED")
            trade = await b.trade_msg(ref)
            done = await b.order_msg(ref, "ALL_TRADED")
            assert acc["broker_seq"] == 1 and trade["broker_seq"] == 2 and done["broker_seq"] == 3
            assert acc["order_sys_id"] == trade["order_sys_id"] == done["order_sys_id"]
            assert trade["client_order_id"] == "c-open-1" and trade["offset_flag"] == OPEN
            assert trade["direction"] == BUY and trade["volume"] == 2 and trade["price"] <= up
            assert trade["trading_day"] == "20260928" and trade["trade_date"] == "20260928"
            assert done["volume_traded"] == 2 and done["volume_total"] == 0
            assert pos(await b.positions(), "ACC3", "rb2701", "LONG") == (2, 0)
            ref2 = b.insert("rb2701", "SHFE", SELL, CLOSE_TODAY, lo, 2)
            t2 = await b.trade_msg(ref2)
            assert t2["offset_flag"] == CLOSE_TODAY and t2["volume"] == 2
            await b.order_msg(ref2, "ALL_TRADED")
            assert pos(await b.positions(), "ACC3", "rb2701", "LONG") == (0, 0)

    async def test_shfe_close_today_and_yesterday_checks(self, exchange: SimExchange) -> None:
        _, lo = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            ref = b.insert("rb2701", "SHFE", SELL, CLOSE_TODAY, lo, 1)
            err = await b.wait(lambda m: m["msg"] == "ErrRtnOrderInsert" and m["order_ref"] == ref)
            assert err["error_id"] == "BROKER_REJECT_CLOSE_TODAY"
            rej = await b.order_msg(ref, "REJECTED")
            assert rej["status_msg"] == "BROKER_REJECT_CLOSE_TODAY" and rej["broker_seq"] >= 1
            await buy_open(b, exchange, qty=1)
            for flag in (CLOSE_YD, CLOSE):  # on SHFE plain CLOSE means close-yesterday (CTP)
                ref = b.insert("rb2701", "SHFE", SELL, flag, lo, 1)
                err = await b.wait(lambda m, r=ref: m["msg"] == "ErrRtnOrderInsert" and m["order_ref"] == r)
                assert err["error_id"] == "BROKER_REJECT_CLOSE_YD"
            # a pending close-today freezes the position: a second one is rejected
            ref_a = b.insert("rb2701", "SHFE", SELL, CLOSE_TODAY, lo, 1)
            ref_b = b.insert("rb2701", "SHFE", SELL, CLOSE_TODAY, lo, 1)
            err = await b.wait(lambda m: m["msg"] == "ErrRtnOrderInsert" and m["order_ref"] == ref_b)
            assert err["error_id"] == "BROKER_REJECT_CLOSE_TODAY"
            await b.order_msg(ref_a, "ALL_TRADED")

    async def test_other_exchanges_close_the_whole_position(self, exchange: SimExchange) -> None:
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            for iid, exch in (("i2701", "DCE"), ("IF2612", "CFFEX")):
                await buy_open(b, exchange, iid, exch, qty=1)
                _, lo = limits(exchange, iid)
                ref = b.insert(iid, exch, SELL, CLOSE, lo, 1)
                await b.order_msg(ref, "ALL_TRADED")
                ref = b.insert(iid, exch, SELL, CLOSE, lo, 1)
                err = await b.wait(lambda m, r=ref: m["msg"] == "ErrRtnOrderInsert" and m["order_ref"] == r)
                assert err["error_id"] == "BROKER_REJECT_POSITION"

    async def test_invalid_orders_are_rejected(self, exchange: SimExchange) -> None:
        up, _ = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            cases = [
                (("rb2701", "SHFE", BUY, OPEN, up + 1000, 1), "BROKER_REJECT_PRICE"),  # outside the limits
                (("rb2701", "SHFE", BUY, OPEN, up - 0.5, 1), "BROKER_REJECT_PRICE"),  # off the tick grid
                (("rb2701", "DCE", BUY, OPEN, up, 1), "BROKER_REJECT_INVALID"),  # wrong exchange
                (("zz2701", "SHFE", BUY, OPEN, 1.0, 1), "BROKER_REJECT_INVALID"),  # not simulated
                (("rb2701", "SHFE", BUY, "2", up, 1), "BROKER_REJECT_INVALID"),  # force close is broker-only
                (("rb2701", "SHFE", BUY, OPEN, up, 0), "BROKER_REJECT_INVALID"),
            ]
            for args, code in cases:
                ref = b.insert(*args)  # type: ignore[arg-type]
                err = await b.wait(lambda m, r=ref: m["msg"] == "ErrRtnOrderInsert" and m["order_ref"] == r)
                assert err["error_id"] == code, (args, err)

    async def test_not_logged_in(self, exchange: SimExchange) -> None:
        up, _ = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            ref = b.insert("rb2701", "SHFE", BUY, OPEN, up, 1)
            err = await b.wait(lambda m: m["msg"] == "ErrRtnOrderInsert")
            assert err["order_ref"] == ref and err["error_id"] == "BROKER_NOT_LOGGED_IN"
            b.send({"msg": "ReqQryTrade", "req_id": 99, "trading_day": "20260928", "from_broker_seq": 1})
            assert (await b.wait(lambda m: m["msg"] == "RspError"))["error_id"] == "BROKER_NOT_LOGGED_IN"

    async def test_duplicate_client_order_id_is_idempotent(self, exchange: SimExchange) -> None:
        up, _ = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            ref = b.insert("rb2701", "SHFE", BUY, OPEN, up, 1, client_order_id="same-id")
            done = await b.order_msg(ref, "ALL_TRADED")
            await b.quiet(0.1)  # drop the earlier reports of this order
            b.insert("rb2701", "SHFE", BUY, OPEN, up, 1, client_order_id="same-id")
            again = await b.wait(lambda m: m["msg"] == "RtnOrder" and m["client_order_id"] == "same-id")
            assert again["order_sys_id"] == done["order_sys_id"] and again["broker_seq"] == done["broker_seq"]
            assert pos(await b.positions(), "ACC3", "rb2701", "LONG") == (1, 0)


class TestCancelAndTimeInForce:
    async def test_cancel_resting_order(self, exchange: SimExchange) -> None:
        _, lo = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            login = await b.login()
            ref = b.insert("rb2701", "SHFE", BUY, OPEN, lo, 1)  # far below the market: rests
            acc = await b.order_msg(ref, "ACCEPTED")
            assert await b.quiet(0.3) == []  # no fill
            b.send({"msg": "ReqOrderAction", "req_id": 50, "front_id": login["front_id"],
                    "session_id": login["session_id"], "order_ref": ref, "action": "DELETE"})
            canceled = await b.order_msg(ref, "CANCELED")
            assert canceled["status_msg"] == "USER_CANCELED" and canceled["broker_seq"] > acc["broker_seq"]
            b.send({"msg": "ReqOrderAction", "req_id": 51, "order_sys_id": acc["order_sys_id"], "action": "DELETE"})
            err = await b.wait(lambda m: m["msg"] == "ErrRtnOrderAction")
            assert err["req_id"] == 51 and err["error_id"] == "BROKER_ORDER_FINISHED"
            b.send({"msg": "ReqOrderAction", "req_id": 52, "front_id": 1, "session_id": 999, "order_ref": "77",
                    "action": "DELETE"})
            err = await b.wait(lambda m: m["msg"] == "ErrRtnOrderAction")
            assert err["req_id"] == 52 and err["error_id"] == "BROKER_ORDER_NOT_FOUND"

    async def test_fok_and_fak(self, exchange: SimExchange) -> None:
        up, _ = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            # 400 lots exceed any level-1 size of rb (mean ~80): FOK cannot fill completely
            ref = b.insert("rb2701", "SHFE", BUY, OPEN, up, 400, tc="IOC", vc="CV")
            fok = await b.order_msg(ref, "CANCELED")
            assert fok["volume_traded"] == 0 and fok["status_msg"] == "FOK_CANCELED"
            ref = b.insert("rb2701", "SHFE", BUY, OPEN, up, 400, tc="IOC", vc="AV")
            trade = await b.trade_msg(ref)
            fak = await b.order_msg(ref, "CANCELED")
            assert 0 < trade["volume"] < 400 and fak["volume_traded"] == trade["volume"]
            assert fak["status_msg"] == "FAK_REMAINDER_CANCELED"
            assert all(not (m["msg"] == "RtnTrade" and m["order_ref"] == "1") for m in b.log)

    async def test_market_order_fills_at_the_opposite_price(self, exchange: SimExchange) -> None:
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            ref = b.insert("m2701", "DCE", BUY, OPEN, None, 1, price_type="ANY")
            trade = await b.trade_msg(ref)
            done = await b.order_msg(ref, "ALL_TRADED")
            assert trade["volume"] == 1 and done["limit_price"] == 0.0

    async def test_resting_order_fills_when_the_market_crosses(self, exchange: SimExchange) -> None:
        st = exchange.engine.states["rb2701"]
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            bid = float(st.meta.price(st.last - 3))
            ref = b.insert("rb2701", "SHFE", BUY, OPEN, bid, 1)
            await b.order_msg(ref, "ACCEPTED")
            exchange.add_shock(instruments=["rb2701"], pct=-0.01)  # the market falls through the bid
            trade = await b.trade_msg(ref)
            assert trade["price"] == bid  # a resting order fills at its own price

    async def test_locked_side_has_no_liquidity(self, exchange: SimExchange) -> None:
        exchange.add_shock(instruments=["rb2701"], pct=-0.2)
        st = exchange.engine.states["rb2701"]
        await wait_for(lambda: st.lock.value == "DOWN")
        up, lo = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            await buy_open(b, exchange, qty=1)  # offers exist at the lower limit
            ref = b.insert("rb2701", "SHFE", SELL, CLOSE_TODAY, lo, 1, tc="IOC")
            canceled = await b.order_msg(ref, "CANCELED")
            assert canceled["volume_traded"] == 0  # no bids at limit-down


class TestQueriesAndPersistence:
    async def test_query_orders_and_trades_from_seq(self, exchange: SimExchange) -> None:
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            first = await buy_open(b, exchange, qty=1)
            await buy_open(b, exchange, qty=2)
            b.send({"msg": "ReqQryTrade", "req_id": 5, "trading_day": "20260928", "from_broker_seq": 1})
            rsp = await b.wait(lambda m: m["msg"] == "RspQryTrade")
            assert rsp["is_last"] and [t["volume"] for t in rsp["items"]] == [1, 2]
            second_seq = rsp["items"][1]["broker_seq"]
            b.send({"msg": "ReqQryTrade", "req_id": 6, "trading_day": "20260928", "from_broker_seq": second_seq})
            rsp = await b.wait(lambda m: m["msg"] == "RspQryTrade")
            assert [t["broker_seq"] for t in rsp["items"]] == [second_seq]
            b.send({"msg": "ReqQryOrder", "req_id": 7, "trading_day": "20260928",
                    "from_broker_seq": int(first["broker_seq"]) + 1})  # type: ignore[call-overload]
            rsp = await b.wait(lambda m: m["msg"] == "RspQryOrder")
            assert [o["order_status"] for o in rsp["items"]] == ["ALL_TRADED"]
            assert rsp["items"][0]["volume_total_original"] == 2

    async def test_state_survives_a_restart(self, sim_cfg: FullConfig, sim_home: HomeLayout) -> None:
        async with SimExchange(sim_cfg, sim_home, offset_for(MARKET_OPEN)) as ex1:
            await wait_for(lambda: ex1.engine.rounds >= 2)
            with BrokerClient(ex1.broker_endpoint) as b:
                login1 = await b.login()
                done = await buy_open(b, ex1, qty=3)
                trade1 = next(m for m in b.log if m["msg"] == "RtnTrade")
        assert done["broker_seq"] == 3
        async with SimExchange(sim_cfg, sim_home, offset_for(MARKET_OPEN)) as ex2:
            await wait_for(lambda: ex2.engine.rounds >= 2)
            with BrokerClient(ex2.broker_endpoint) as b:
                login2 = await b.login()
                assert login2["broker_seq_head"] == 3
                assert login2["session_id"] > login1["session_id"]
                assert login2["max_order_ref"] == "1"
                assert pos(await b.positions(), "ACC3", "rb2701", "LONG") == (3, 0)
                up, _ = limits(ex2, "rb2701")
                ref = b.insert("rb2701", "SHFE", BUY, OPEN, up, 1, order_ref="2")
                acc = await b.order_msg(ref, "ACCEPTED")
                trade2 = await b.trade_msg(ref)
                assert acc["broker_seq"] == 4 and trade2["broker_seq"] == 5
                assert int(trade2["trade_id"]) == int(trade1["trade_id"]) + 1
                assert int(acc["order_sys_id"]) > int(done["order_sys_id"])  # type: ignore[call-overload]
                b.send({"msg": "ReqQryTrade", "req_id": 3, "trading_day": "20260928", "from_broker_seq": 0})
                rsp = await b.wait(lambda m: m["msg"] == "RspQryTrade")
                assert [t["trade_id"] for t in rsp["items"]] == [trade1["trade_id"], trade2["trade_id"]]


class TestSessionsAndDays:
    async def test_session_close_cancels_gfd_orders(self, exchange: SimExchange) -> None:
        _, lo = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            ref = b.insert("rb2701", "SHFE", BUY, OPEN, lo, 1)
            await b.order_msg(ref, "ACCEPTED")
            await exchange.pause()
            exchange.set_clock(offset_for(datetime(2026, 9, 28, 15, 1, tzinfo=BJ)), 1)
            exchange.resume()
            canceled = await b.order_msg(ref, "CANCELED")
            assert canceled["status_msg"] == "SESSION_CLOSE"
            ref2 = b.insert("rb2701", "SHFE", BUY, OPEN, lo, 1)
            err = await b.wait(lambda m: m["msg"] == "ErrRtnOrderInsert" and m["order_ref"] == ref2)
            assert err["error_id"] == "BROKER_REJECT_SESSION_CLOSED"

    async def test_next_trading_day_rolls_positions_and_broker_seq(self, exchange: SimExchange) -> None:
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            await buy_open(b, exchange, qty=2)
            # ask for the next trading day before the roll: today's lots already count as yesterday's
            assert pos(await b.positions(trading_day="20260929"), "ACC3", "rb2701", "LONG") == (0, 2)
            await exchange.pause()
            exchange.set_clock(offset_for(datetime(2026, 9, 28, 21, 5, tzinfo=BJ)), 1)  # night of 09-29
            exchange.resume()
            await wait_for(lambda: exchange.broker.trading_day == "20260929")
            assert pos(await b.positions(), "ACC3", "rb2701", "LONG") == (0, 2)
            login = await b.login()
            assert login["trading_day"] == "20260929" and login["broker_seq_head"] == 0
            await wait_for(lambda: exchange.engine.states["rb2701"].trading_day == "20260929")
            _, lo = limits(exchange, "rb2701")
            ref = b.insert("rb2701", "SHFE", SELL, CLOSE_YD, lo, 1)
            acc = await b.order_msg(ref, "ACCEPTED")
            assert acc["broker_seq"] == 1 and acc["trading_day"] == "20260929"
            await b.order_msg(ref, "ALL_TRADED")
            assert pos(await b.positions(), "ACC3", "rb2701", "LONG") == (0, 1)


class TestPendingOrdersAndDelays:
    async def test_restart_keeps_pending_orders_aggressive(self, sim_cfg: FullConfig, sim_home: HomeLayout) -> None:
        async with SimExchange(sim_cfg, sim_home, offset_for(MARKET_OPEN)) as ex1:
            await wait_for(lambda: ex1.engine.rounds >= 2)
            await ex1.pause()  # matching held: the orders wait for their first (aggressive) match
            up, _ = limits(ex1, "rb2701")
            with BrokerClient(ex1.broker_endpoint) as b:
                await b.login()
                fak = b.insert("rb2701", "SHFE", BUY, OPEN, up, 1, tc="IOC")
                mkt = b.insert("rb2701", "SHFE", BUY, OPEN, None, 1, price_type="ANY")
                gfd = b.insert("rb2701", "SHFE", BUY, OPEN, up, 1)
                for ref in (fak, mkt, gfd):
                    await b.order_msg(ref, "ACCEPTED")
                assert [m for m in await b.quiet(0.2) if m["msg"] == "RtnTrade"] == []
        async with SimExchange(sim_cfg, sim_home, offset_for(MARKET_OPEN)) as ex2:
            await wait_for(lambda: ex2.broker.state()["active_orders"] == 0, timeout=5)
            with BrokerClient(ex2.broker_endpoint) as b:
                await b.login()
                b.send({"msg": "ReqQryTrade", "req_id": 1, "trading_day": "20260928", "from_broker_seq": 0})
                trades = (await b.wait(lambda m: m["msg"] == "RspQryTrade"))["items"]
                b.send({"msg": "ReqQryOrder", "req_id": 2, "trading_day": "20260928", "from_broker_seq": 0})
                orders = (await b.wait(lambda m: m["msg"] == "RspQryOrder"))["items"]
        assert sorted(t["order_ref"] for t in trades) == sorted([fak, mkt, gfd])
        # matched at the opposite best price like any new order, not at their own limit
        assert all(t["price"] < up for t in trades)
        assert {o["order_ref"]: o["order_status"] for o in orders} == {fak: "ALL_TRADED", mkt: "ALL_TRADED",
                                                                     gfd: "ALL_TRADED"}

    async def test_orders_while_paused_wait_for_resume(self, exchange: SimExchange) -> None:
        up, _ = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            await exchange.pause()
            ref = b.insert("rb2701", "SHFE", BUY, OPEN, up, 1)
            await b.order_msg(ref, "ACCEPTED")
            assert [m for m in await b.quiet(0.3) if m["msg"] == "RtnTrade"] == []
            exchange.resume()
            trade = await b.trade_msg(ref)
            assert trade["price"] < up

    async def test_auction_orders_use_the_new_trading_day_band(self, exchange: SimExchange) -> None:
        rb = exchange.engine.states["rb2701"]
        exchange.add_shock(instruments=["rb2701"], pct=0.03)  # most of the day trades 3% higher
        await wait_for(lambda: exchange.engine.rounds >= 25)
        await exchange.pause()
        old_upper = rb.upper
        settle = exchange.engine.settlement_price(rb)
        up, dn = limit_prices(settle, Decimal("0.05"), Decimal("1"))
        assert int(up) > old_upper  # the next day's band really differs
        # 20:57 = opening auction of the 2026-09-29 night session (engine still paused)
        exchange.set_clock(offset_for(datetime(2026, 9, 28, 20, 57, tzinfo=BJ)), 1)
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            gfd = b.insert("rb2701", "SHFE", BUY, OPEN, float(up), 1)  # the NEW upper limit
            await b.order_msg(gfd, "ACCEPTED")
            assert rb.trading_day == "20260929" and rb.upper == int(up) and rb.lower == int(dn)
            fak = b.insert("rb2701", "SHFE", BUY, OPEN, float(up), 1, tc="IOC")
            await b.order_msg(fak, "ACCEPTED")
            exchange.resume()  # still the auction: the FAK cannot wait, the GFD does
            canceled = await b.order_msg(fak, "CANCELED")
            assert canceled["status_msg"] == "NOT_IN_CONTINUOUS_SESSION" and canceled["volume_traded"] == 0
            assert [m for m in await b.quiet(0.3) if m["msg"] == "RtnTrade"] == []
            await exchange.pause()
            exchange.set_clock(offset_for(datetime(2026, 9, 28, 20, 59, 59, 700_000, tzinfo=BJ)), 2)
            exchange.resume()
            trade = await b.trade_msg(gfd)  # matched on the first continuous state at 21:00
            assert trade["trading_day"] == "20260929" and trade["price"] < float(up)

    async def test_ack_delay_does_not_block_or_reorder(self, default_config: FullConfig, sim_home: HomeLayout) -> None:
        cfg = make_cfg(default_config, ack_ms=250)
        async with SimExchange(cfg, sim_home, offset_for(MARKET_OPEN)) as ex:
            await wait_for(lambda: ex.engine.rounds >= 2)
            _, lo = limits(ex, "rb2701")
            with BrokerClient(ex.broker_endpoint) as b:
                login = await b.login()
                t0 = time.monotonic()
                refs = [b.insert("rb2701", "SHFE", BUY, OPEN, lo, 1) for _ in range(4)]
                # a cancel right behind its insert is processed after it (FIFO delay line)
                b.send({"msg": "ReqOrderAction", "req_id": 9, "front_id": login["front_id"],
                        "session_id": login["session_id"], "order_ref": refs[-1], "action": "DELETE"})
                for ref in refs:
                    await b.order_msg(ref, "ACCEPTED")
                elapsed = time.monotonic() - t0
                canceled = await b.order_msg(refs[-1], "CANCELED")
        assert 0.2 <= elapsed < 0.7  # one 250 ms delay for the batch, not four
        assert canceled["status_msg"] == "USER_CANCELED"

    async def test_force_close_cancels_pending_closes(self, exchange: SimExchange) -> None:
        up, _ = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            await buy_open(b, exchange, qty=2)
            close_ref = b.insert("rb2701", "SHFE", SELL, CLOSE_TODAY, up, 2)  # rests at the upper limit
            await b.order_msg(close_ref, "ACCEPTED")
            result = await exchange.broker.force_close("ACC3", "rb2701", 2)
            canceled = await b.order_msg(close_ref, "CANCELED")
            assert canceled["status_msg"] == "FORCE_CLOSE_CANCELED"
            assert result["canceled_orders"] == [canceled["order_sys_id"]] and result["qty"] == 2
            trade = await b.wait(lambda m: m["msg"] == "RtnTrade" and m["offset_flag"] == "2")
            assert trade["volume"] == 2
            assert pos(await b.positions(), "ACC3", "rb2701", "LONG") == (0, 0)


class TestQueryDetails:
    async def test_query_pages(self, exchange: SimExchange, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(broker_module, "QUERY_PAGE", 3)
        _, lo = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            refs = [b.insert("rb2701", "SHFE", BUY, OPEN, lo, 1) for _ in range(7)]
            for ref in refs:
                await b.order_msg(ref, "ACCEPTED")
            b.send({"msg": "ReqQryOrder", "req_id": 70, "trading_day": "20260928", "from_broker_seq": 0})
            pages = [await b.wait(lambda m: m["msg"] == "RspQryOrder" and m["req_id"] == 70) for _ in range(3)]
        assert [len(p["items"]) for p in pages] == [3, 3, 1]
        assert [p["is_last"] for p in pages] == [False, False, True]
        assert sorted(o["order_ref"] for p in pages for o in p["items"]) == sorted(refs)

    async def test_past_trading_days_are_queried_from_sim_db(self, exchange: SimExchange) -> None:
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            done = await buy_open(b, exchange, qty=1)
            await exchange.pause()
            exchange.set_clock(offset_for(datetime(2026, 9, 28, 21, 5, tzinfo=BJ)), 1)
            exchange.resume()
            await wait_for(lambda: exchange.broker.trading_day == "20260929")
            b.send({"msg": "ReqQryTrade", "req_id": 80, "trading_day": "20260928", "from_broker_seq": 0})
            old_trades = (await b.wait(lambda m: m["msg"] == "RspQryTrade" and m["req_id"] == 80))["items"]
            b.send({"msg": "ReqQryOrder", "req_id": 81, "trading_day": "20260928", "from_broker_seq": 0})
            old_orders = (await b.wait(lambda m: m["msg"] == "RspQryOrder" and m["req_id"] == 81))["items"]
            b.send({"msg": "ReqQryTrade", "req_id": 82, "trading_day": "20260929", "from_broker_seq": 0})
            new_trades = (await b.wait(lambda m: m["msg"] == "RspQryTrade" and m["req_id"] == 82))["items"]
        assert [t["order_sys_id"] for t in old_trades] == [done["order_sys_id"]]
        assert old_trades[0]["trading_day"] == "20260928"
        assert [(o["order_sys_id"], o["order_status"]) for o in old_orders] == [(done["order_sys_id"], "ALL_TRADED")]
        assert new_trades == []

    async def test_duplicate_order_ref(self, exchange: SimExchange) -> None:
        up, _ = limits(exchange, "rb2701")
        with BrokerClient(exchange.broker_endpoint) as b:
            await b.login()
            ref = b.insert("rb2701", "SHFE", BUY, OPEN, up, 1, order_ref="42")
            await b.order_msg(ref, "ALL_TRADED")
            b.insert("rb2701", "SHFE", BUY, OPEN, up, 1, order_ref="42", client_order_id="another-order")
            err = await b.wait(lambda m: m["msg"] == "ErrRtnOrderInsert" and m["order_ref"] == "42")
        assert err["error_id"] == "BROKER_DUPLICATE_ORDER_REF"
