"""FutriskClient over loopback sockets against a scripted risk-engine and md-gateway (02 §4.4, §4.5, §4.7, §6.3).

Covers shard routing and the DEALER identity, rejects with ``OrderAck``, retries with the
same ``client_order_id``, in-doubt orders, cancels (own orders only; ``cancel_all`` needs a
scope), exact topic filtering, LV tick handling, fills -> positions incl. SHFE today/yesterday
closes, FACT gap repair, ``build_position``, the ``risk.state`` / ``risk.snapshot`` view with
its start-up Clone, the position snapshot rule (no double count after a restart or for late
foreign fills), LOST revival and a demo market clock behind the wall clock.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import msgspec
import pytest

from futrisk.client import ClientState, FutriskClient, OrderEvent, RiskUpdate, coid_marker, has_marker
from futrisk.client.testing import NO_REPLY, FakeBus, account_snapshot, busy, make_tick, reject
from futrisk.common.config import FullConfig
from futrisk.common.ids import uuid7
from futrisk.messages import codes
from futrisk.messages.enums import (
    AccountState,
    Direction,
    Exchange,
    Offset,
    OrderSource,
    OrderStatus,
    PosDirection,
    RestrictionKind,
)
from futrisk.messages.md import Tick
from futrisk.messages.orders import FillRef, OrderRequest
from futrisk.messages.risk import PositionView, Restriction
from futrisk.runtime.testing import config_with_free_ports

T0 = 1_790_559_300_000_000_000  # 2026-09-28 09:35 Beijing (continuous session)
S = 1_000_000_000
BUY, SELL = Direction.BUY, Direction.SELL


async def until(pred: Callable[[], bool], timeout: float = 3.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not pred():
        if loop.time() > deadline:
            raise AssertionError("condition not reached")
        await asyncio.sleep(0.005)


async def ticks_ready(client: FutriskClient, bus: FakeBus, *instruments: str) -> None:
    client.subscribe_ticks(list(instruments))
    for i in instruments:
        await bus.md.wait_subscribed(f"md.tick.{i}")
        await bus.md.tick(make_tick(i, 3120.0 if i.startswith("rb") else 3260.0, ts_ns=T0))
    assert await client.wait_ticks(instruments, timeout_s=3)


async def orders_ready(client: FutriskClient, bus: FakeBus, account: str, instance: str,
                       cb: Callable[[OrderEvent], None] | None = None) -> None:
    client.subscribe_orders([account], cb)
    await bus.shard(instance).pub.wait_subscribed(f"oms.order.{account}")


class TestCommands:
    async def test_routes_to_owning_shard_with_strategy_identity(self, client: FutriskClient, fake_bus: FakeBus) -> None:
        r1 = await client.place_limit("ACC1", "rb2701", BUY, Offset.OPEN, 2, 3120.0, strategy_tag="t")
        r3 = await client.place_limit("ACC3", "hc2701", SELL, Offset.OPEN, 1, 3260.0)
        assert r1.accepted and r3.accepted and r1.ack is not None and r1.ack.trading_day == "20260928"
        s0, s1 = fake_bus.shard("risk-engine-0"), fake_bus.shard("risk-engine-1")
        assert [r.account_id for r in s0.submits()] == ["ACC1"]
        assert [r.account_id for r in s1.submits()] == ["ACC3"]
        assert s0.requesters() == {"strategy-sim"} == s1.requesters()  # 02 §4.11 whitelist identity
        sent = s0.submits()[0]
        assert sent.client_order_id == r1.client_order_id and len(sent.client_order_id) == 36
        assert sent.sent_ns > 0 and sent.strategy_tag == "t"
        # unknown account: answered locally, nothing sent
        bad = await client.place_limit("ACC9", "rb2701", BUY, Offset.OPEN, 1, 3120.0)
        assert bad.reply.code == codes.REJECT_UNKNOWN_ACCOUNT and len(s0.submits()) + len(s1.submits()) == 2

    async def test_reject_carries_ack_and_is_terminal(self, client: FutriskClient, fake_bus: FakeBus) -> None:
        fake_bus.shard("risk-engine-1").script.append(reject(codes.REJECT_ACCOUNT_HALTED, {"halted_latched": True}))
        res = await client.place_limit("ACC3", "rb2701", BUY, Offset.OPEN, 5, 3120.0)
        assert not res.accepted and res.code == codes.REJECT_ACCOUNT_HALTED
        assert res.ack is not None and res.ack.reject_detail == {"halted_latched": True}
        o = client.orders.get(res.client_order_id)
        assert o is not None and o.terminal and o.status == OrderStatus.REJECTED

    async def test_retries_keep_client_order_id(self, client: FutriskClient, fake_bus: FakeBus) -> None:
        shard = fake_bus.shard("risk-engine-1")
        shard.script.extend([busy(codes.RISK_BUSY), busy(codes.BUSY)])
        res = await client.place_limit("ACC4", "IF2612", BUY, Offset.OPEN, 1, 4414.8)
        assert res.accepted
        seen = [(ctx.envelope.payload, ctx.corr_id) for c, _, ctx in shard.requests if c == "order.submit"]
        assert len(seen) == 3
        assert len({p for p, _ in seen}) == 1  # identical payload bytes: same client_order_id (02 §4.7)
        assert len({c for _, c in seen}) == 1  # same request id
        assert {r.client_order_id for r in shard.submits()} == {res.client_order_id}

    async def test_timeout_leaves_order_in_doubt_and_resubmit_resolves(self, bus_cfg: FullConfig,
                                                                       fake_bus: FakeBus) -> None:
        shard = fake_bus.shard("risk-engine-1")
        shard.script.append(NO_REPLY)
        async with FutriskClient(bus_cfg, order_timeout_ms=100, max_retries=0) as cli:
            res = await cli.place_limit("ACC5", "rb2705", SELL, Offset.OPEN, 3, 3150.0)
            assert res.reply.code == codes.TIMEOUT
            o = cli.orders.get(res.client_order_id)
            assert o is not None and o.state == ClientState.IN_DOUBT and o.working
            reply, ack = await cli.resubmit(res.client_order_id)
            assert reply.ok and ack is not None and ack.client_order_id == res.client_order_id
            assert cli.orders.get(res.client_order_id).state == ClientState.ACKED
            assert [r.client_order_id for r in shard.submits()] == [res.client_order_id] * 2

    async def test_unavailable_shard(self, default_config: FullConfig) -> None:
        cfg = config_with_free_ports(default_config, ["md-gateway", "risk-engine-0", "risk-engine-1"])
        async with FutriskClient(cfg, max_retries=0) as cli:
            res = await cli.place_limit("ACC1", "rb2701", BUY, Offset.OPEN, 1, 3120.0)
            assert res.reply.code == codes.UNAVAILABLE
            assert cli.orders.get(res.client_order_id).state == ClientState.IN_DOUBT
            pongs = await cli.ping_shards(timeout_ms=200)
            assert pongs == {"risk-engine-0": None, "risk-engine-1": None}

    async def test_cancel_and_cancel_all(self, client: FutriskClient, fake_bus: FakeBus) -> None:
        await orders_ready(client, fake_bus, "ACC3", "risk-engine-1")
        a = await client.place_limit("ACC3", "rb2701", BUY, Offset.OPEN, 2, 3100.0)
        b = await client.place_limit("ACC3", "hc2701", BUY, Offset.OPEN, 2, 3200.0)
        reply, upd = await client.cancel("ACC3", a.client_order_id, reason="test")
        assert reply.ok and upd is not None and upd.status == OrderStatus.CANCELED
        assert client.orders.get(a.client_order_id).terminal
        reply, _ = await client.cancel("ACC3", a.client_order_id)
        assert reply.code == codes.CONFLICT
        with pytest.raises(TypeError):
            await client.cancel_all("ACC3", reason="flat")  # type: ignore[call-arg]  # no implicit scope
        reply, cnt = await client.cancel_all("ACC3", scope="ALL", reason="flat")  # operator tool
        assert reply.ok and cnt is not None and cnt.count == 1
        await until(lambda: client.orders.get(b.client_order_id).terminal)
        pongs = await client.ping_shards(["ACC3"])
        assert pongs["risk-engine-1"] is not None and pongs["risk-engine-1"].state == "RUNNING"


class TestSubscriptions:
    async def test_order_topics_are_filtered_exactly(self, client: FutriskClient, fake_bus: FakeBus) -> None:
        got: list[str] = []
        await orders_ready(client, fake_bus, "ACC1", "risk-engine-0", lambda e: got.append(e.account_id))
        shard = fake_bus.shard("risk-engine-0")
        r10 = OrderRequest(client_order_id="x10", account_id="ACC10", instrument_id="rb2701", direction=BUY,
                           offset=Offset.OPEN, price=3120.0, qty=1)
        r1 = msgspec.structs.replace(r10, client_order_id="x1", account_id="ACC1")
        await shard.publish_update(shard.update_for(r10, OrderStatus.ROUTING))  # passes the ZMQ prefix filter
        await shard.publish_update(shard.update_for(r1, OrderStatus.ROUTING))
        await until(lambda: got == ["ACC1"])
        await asyncio.sleep(0.05)
        assert got == ["ACC1"] and client.orders.get("x10") is None  # 02 §4.4: ACC1 never sees ACC10

    async def test_watermark_reset_tick_goes_backwards(self, client: FutriskClient, fake_bus: FakeBus) -> None:
        """md-gateway's FLAG_WM_RESET: the first LIVE tick after a watermark roll-back is applied anyway."""
        from futrisk.messages.md import FLAG_WM_RESET

        got: list[float] = []
        client.subscribe_ticks(["rb2701"], lambda t: got.append(t.last))
        await fake_bus.md.wait_subscribed("md.tick.rb2701")
        await fake_bus.md.tick(make_tick("rb2701", 3120.0, ts_ns=T0 + 5 * S), seq=1)
        await fake_bus.md.tick(make_tick("rb2701", 3110.0, ts_ns=T0), seq=2)  # older key: dropped
        reset = msgspec.structs.replace(make_tick("rb2701", 3111.0, ts_ns=T0 + S), flags=FLAG_WM_RESET)
        await fake_bus.md.tick(reset, seq=3)  # older key but flagged: applied
        await fake_bus.md.tick(make_tick("rb2701", 3112.0, ts_ns=T0 + 2 * S), seq=4)  # ordering continues
        await until(lambda: len(got) == 3)
        assert got == [3120.0, 3111.0, 3112.0]

    async def test_tick_topics_lv_and_order_key(self, client: FutriskClient, fake_bus: FakeBus) -> None:
        exact: list[Tick] = []
        everything: list[str] = []
        client.subscribe_ticks(["rb2701"], exact.append)
        await fake_bus.md.wait_subscribed("md.tick.rb2701")
        await fake_bus.md.tick(make_tick("rb27010", 1.0, ts_ns=T0))  # prefix match only
        await fake_bus.md.tick(make_tick("rb2701", 3120.0, ts_ns=T0), seq=1)
        await fake_bus.md.tick(make_tick("rb2701", 3999.0, ts_ns=T0 + S), seq=1)  # duplicate seq (LV)
        await fake_bus.md.tick(make_tick("rb2701", 3121.0, ts_ns=T0 - S), seq=2)  # older order_key
        await fake_bus.md.tick(make_tick("rb2701", 3122.0, ts_ns=T0 + 2 * S), seq=3)
        await until(lambda: len(exact) == 2)
        assert [t.last for t in exact] == [3120.0, 3122.0]
        assert client.last_tick("rb2701").last == 3122.0 and client.last_tick("rb27010") is None
        assert client.duplicates_dropped >= 2 and client.seconds_since_last_tick() is not None
        client.subscribe_ticks(None, lambda t: everything.append(t.instrument_id))
        await fake_bus.md.wait_subscribed("md.tick.")
        await fake_bus.md.tick(make_tick("hc2701", 3260.0, ts_ns=T0 + 3 * S))
        await until(lambda: everything == ["hc2701"])
        assert len(exact) == 2

    async def test_fills_drive_positions_and_shfe_close_offsets(self, bus_cfg: FullConfig) -> None:
        async with FakeBus(bus_cfg, fill_mode="full") as bus, FutriskClient(bus_cfg) as cli:
            events: list[OrderEvent] = []
            await orders_ready(cli, bus, "ACC3", "risk-engine-1", events.append)
            await ticks_ready(cli, bus, "rb2701")
            res = await cli.market_like_limit("ACC3", "rb2701", BUY, 5, ticks=2)
            assert res.accepted and res.request.price == 3123.0  # ask 3121 + 2 ticks
            o = await cli.wait_order(res.client_order_id, timeout_s=3)
            assert o is not None and o.status == OrderStatus.FILLED
            assert cli.positions.leg("ACC3", "rb2701", PosDirection.LONG).td == 5
            assert any(e.fill is not None and e.fill.qty == 5 for e in events)
            # next trading day (night session belongs to it): today's lots become yesterday's
            await bus.md.tick(make_tick("rb2701", 3110.0, ts_ns=T0 + 60 * S, trading_day="20260929"))
            await until(lambda: cli.positions.trading_day == "20260929")
            leg = cli.positions.leg("ACC3", "rb2701", PosDirection.LONG)
            assert (leg.td, leg.yd) == (0, 5)
            opened = await cli.market_like_limit("ACC3", "rb2701", BUY, 3)
            await cli.wait_order(opened.client_order_id, timeout_s=3)
            closes = await cli.close_position("ACC3", "rb2701", PosDirection.LONG)
            assert [(r.request.offset, r.request.qty, r.request.direction) for r in closes] == [
                (Offset.CLOSE_YESTERDAY, 5, SELL), (Offset.CLOSE_TODAY, 3, SELL)]
            for r in closes:
                await cli.wait_order(r.client_order_id, timeout_s=3)
            assert cli.positions.net("ACC3", "rb2701") == 0
            sent = bus.shard("risk-engine-1").submits()
            assert [r.offset for r in sent] == [Offset.OPEN, Offset.OPEN, Offset.CLOSE_YESTERDAY, Offset.CLOSE_TODAY]

    async def test_fact_gap_repaired_with_replay(self, client: FutriskClient, fake_bus: FakeBus) -> None:
        shard = fake_bus.shard("risk-engine-1")
        await orders_ready(client, fake_bus, "ACC3", "risk-engine-1")
        a = OrderRequest(client_order_id="a", account_id="ACC3", instrument_id="i2701", direction=BUY,
                         offset=Offset.OPEN, price=780.0, qty=4)
        b = msgspec.structs.replace(a, client_order_id="b")
        shard.drop_seqs = {2}  # lost on the wire, still in the outbox
        await shard.publish_update(shard.update_for(a, OrderStatus.ROUTING))  # seq 1
        await shard.fill(b)  # seq 2: a fill we never receive live
        await shard.publish_update(shard.update_for(a, OrderStatus.ACCEPTED))  # seq 3 -> gap [2, 2]
        await until(lambda: client.positions.net("ACC3", "i2701") == 4)
        assert client.gaps_detected == 1
        assert any(c == "ctl.replay" for c, _, _ in shard.requests)
        assert client.orders.get("b").status == OrderStatus.FILLED
        # a duplicate FACT is dropped
        await shard.publish_update(shard.update_for(a, OrderStatus.ACCEPTED), seq=3)
        await until(lambda: client.duplicates_dropped >= 1)
        assert client.positions.net("ACC3", "i2701") == 4

    async def test_build_position(self, bus_cfg: FullConfig) -> None:
        async with FakeBus(bus_cfg, fill_mode="full") as bus, FutriskClient(bus_cfg) as cli:
            cli.subscribe_ticks(["rb2701", "hc2701", "i2701"])
            for i, px in (("rb2701", 3120.0), ("hc2701", 3260.0), ("i2701", 780.0)):
                await bus.md.wait_subscribed(f"md.tick.{i}")
                await bus.md.tick(make_tick(i, px, ts_ns=T0))
            cli.subscribe_orders(["ACC3"])
            await bus.shard("risk-engine-1").pub.wait_subscribed("oms.order.ACC3")
            res = await cli.build_position("ACC3", [("rb2701", "BUY", 12), ("hc2701", BUY, 5), ("i2701", BUY, 3)],
                                           timeout_s=10)
            assert res.complete, res
            assert [cli.positions.net("ACC3", i) for i in ("rb2701", "hc2701", "i2701")] == [12, 5, 3]

    async def test_build_position_partial_fills_retry_rounds(self, bus_cfg: FullConfig) -> None:
        async with FakeBus(bus_cfg, fill_mode="partial") as bus, FutriskClient(bus_cfg) as cli:
            await orders_ready(cli, bus, "ACC5", "risk-engine-1")
            await ticks_ready(cli, bus, "rb2705")
            res = await cli.build_position("ACC5", [("rb2705", SELL, 8)], fill_wait_s=0.2, max_rounds=5, timeout_s=10)
            leg = res.legs[0]
            assert not res.complete and leg.filled == 7 and len(leg.orders) == 5  # 4 + 2 + 1 + 0 + 0
            assert cli.positions.net("ACC5", "rb2705") == -7
            assert sum(1 for c, _, _ in bus.shard("risk-engine-1").requests if c == "order.cancel") == 5

    async def test_sync_positions_and_account(self, client: FutriskClient, fake_bus: FakeBus) -> None:
        fake_bus.shard("risk-engine-1").positions = [view("hc2701", yd=6)]
        assert await client.sync_positions("ACC3")  # risk.query.positions: no snapshot time
        assert client.positions.snapshot("ACC3") == {"hc2701": {"LONG": (0, 6)}}
        snap = await client.sync_account("ACC3")  # risk.query.accounts: positions + as_of_ns + equity
        assert snap is not None and snap.funds.equity == 10_000_000.0
        assert client.positions.snapshot("ACC3") == {"hc2701": {"LONG": (0, 6)}}
        assert await client.refresh_orders("ACC3", "20260928") == 0


def view(instrument: str, *, td: int = 0, yd: int = 0, side: PosDirection = PosDirection.LONG,
         account: str = "ACC3") -> PositionView:
    return PositionView(account_id=account, instrument_id=instrument, product=instrument.rstrip("0123456789"),
                        exchange=Exchange.SHFE, direction=side, td_qty=td, yd_qty=yd, closable_qty=td + yd,
                        closable_td_qty=td, closable_yd_qty=yd, avg_open_price=3100.0, basis_avg_price=3100.0)


def resting(shard, coid: str, *, source: OrderSource, offset: Offset = Offset.OPEN, direction: Direction = BUY,
            qty: int = 2, instrument: str = "rb2701", status: OrderStatus = OrderStatus.ACCEPTED, filled: int = 0):  # type: ignore[no-untyped-def]
    """A working order already on the fake shard (as if placed earlier by someone)."""
    req = OrderRequest(client_order_id=coid, account_id="ACC3", instrument_id=instrument, direction=direction,
                       offset=offset, price=3100.0, qty=qty, source=source)
    shard.orders[coid] = shard.update_for(req, status, filled_qty=filled)
    return req


class TestOwnOrders:
    async def test_marker_and_cancel_own_orders_only(self, bus_cfg: FullConfig, fake_bus: FakeBus) -> None:
        shard = fake_bus.shard("risk-engine-1")
        async with FutriskClient(bus_cfg, coid_tag="strategy-sim/bots") as cli:
            marker = coid_marker("strategy-sim/bots")
            req = cli.new_order("ACC3", "rb2701", BUY, Offset.OPEN, 1, 3100.0)
            assert has_marker(req.client_order_id, marker) and cli.owns_coid(req.client_order_id)
            assert req.client_order_id[14] == "7"  # still a UUIDv7
            # a previous incarnation's own orders (same tag) + other people's orders on the account
            prev = FutriskClient(bus_cfg, coid_tag="strategy-sim/bots").new_client_order_id()
            resting(shard, prev, source=OrderSource.BOT)
            resting(shard, "manual-close", source=OrderSource.MANUAL, offset=Offset.CLOSE_TODAY, direction=SELL)
            resting(shard, "liquidation", source=OrderSource.LIQUIDATION, offset=Offset.CLOSE_TODAY, direction=SELL)
            resting(shard, uuid7(), source=OrderSource.BOT)  # a BOT order of another client (no marker)
            resting(shard, "api-order", source=OrderSource.API)
            done = await cli.cancel_own_orders("ACC3", "20260928", reason="strategy-sim start")
            assert done == [prev]
            cancelled = {getattr(payload, "client_order_id", None) for cmd, payload, _ in shard.requests
                         if cmd == "order.cancel"}
            assert cancelled == {prev}
            still = {k for k, o in shard.orders.items() if o.status == OrderStatus.ACCEPTED}
            assert {"manual-close", "liquidation", "api-order"} <= still and prev not in still
            assert not any(c == "order.cancel_all" for c, _, _ in shard.requests)


class TestRiskView:
    async def test_clone_at_start_then_live_state_and_snapshots(self, bus_cfg: FullConfig, fake_bus: FakeBus) -> None:
        shard = fake_bus.shard("risk-engine-1")
        halted = await shard.publish_state("ACC3", AccountState.HALTED, latched=True, publish=False)  # before us
        updates: list[RiskUpdate] = []
        async with FutriskClient(bus_cfg) as cli:
            cli.subscribe_risk(["ACC3"], updates.append)
            await until(lambda: "ACC3" in cli.clones_done)
            v = cli.risk_view("ACC3")
            assert v is not None and v.halted and v.halted_latched and v.state_version == halted.state_version
            assert updates[-1].source == "clone" and updates[-1].state_changed
            await shard.pub.wait_subscribed("risk.state.ACC3")
            await shard.pub.wait_subscribed("risk.snapshot.ACC3")
            lim = [Restriction(kind=RestrictionKind.ORDER_SIZE_FACTOR, scope_key="acct:ACC3", value=0.5,
                               source="daily_loss/restrict"),
                   Restriction(kind=RestrictionKind.NO_ADD_TO_LOSERS, scope_key="acct:ACC3", source="daily_loss/restrict")]
            await shard.publish_state("ACC3", AccountState.RESTRICTED, restrictions=lim,
                                      from_state=AccountState.HALTED)  # e.g. after risk.reset
            await until(lambda: v.state == AccountState.RESTRICTED)
            assert v.restrictions == lim and updates[-1].source == "state"
            # a snapshot with an older state_version keeps the state, but brings equity and positions
            snap = account_snapshot("ACC3", as_of_ns=shard.now_ns(), equity=9_700_000.0, state=AccountState.HALTED,
                                    state_version=halted.state_version, positions=[view("rb2701", td=10)])
            await shard.publish_snapshot(snap)
            await until(lambda: v.equity == 9_700_000.0)
            assert v.state == AccountState.RESTRICTED and updates[-1].snapshot is not None
            # risk.state.ACC30 passes the ZeroMQ prefix filter of risk.state.ACC3 but never reaches it (02 §4.4)
            n = len(updates)
            await shard.publish_state("ACC30", AccountState.HALTED)
            await shard.publish_state("ACC3", AccountState.WARNING, from_state=AccountState.RESTRICTED)
            await until(lambda: v.state == AccountState.WARNING)
            assert all(u.account_id == "ACC3" for u in updates[n:]) and cli.risk_view("ACC30") is None


class TestSnapshotRule:
    async def test_restart_refresh_does_not_double_count(self, bus_cfg: FullConfig, fake_bus: FakeBus) -> None:
        shard = fake_bus.shard("risk-engine-1")
        resting(shard, "prev-1", source=OrderSource.BOT, qty=100, filled=100, status=OrderStatus.FILLED)
        shard.positions = [view("rb2701", td=100)]
        async with FutriskClient(bus_cfg) as cli:
            cli.subscribe_orders(["ACC3"])
            await shard.pub.wait_subscribed("oms.order.ACC3")
            assert await cli.sync_account("ACC3") is not None
            assert cli.positions.net("ACC3", "rb2701") == 100
            assert await cli.refresh_orders("ACC3", "20260928") == 1  # a predecessor's filled order
            assert cli.positions.net("ACC3", "rb2701") == 100 and "ACC3" in cli.resync_wanted

    async def test_foreign_order_seen_late_counts_only_new_fills(self, bus_cfg: FullConfig, fake_bus: FakeBus) -> None:
        shard = fake_bus.shard("risk-engine-1")
        shard.positions = [view("rb2701", td=105)]  # includes 5 lots of a resting manual order
        async with FutriskClient(bus_cfg) as cli:
            cli.subscribe_orders(["ACC3"])
            await shard.pub.wait_subscribed("oms.order.ACC3")
            snap = await cli.sync_account("ACC3")
            assert snap is not None and cli.positions.net("ACC3", "rb2701") == 105
            req = OrderRequest(client_order_id="manual-2", account_id="ACC3", instrument_id="rb2701", direction=BUY,
                               offset=Offset.OPEN, price=3121.0, qty=10, source=OrderSource.MANUAL)
            late = shard.update_for(req, OrderStatus.PARTIALLY_FILLED, filled_qty=6, avg_fill_price=3121.0,
                                    last_fill=FillRef(trade_id="T6", price=3121.0, qty=1, trade_ts_ns=shard.now_ns()))
            await shard.publish_update(late)
            await until(lambda: cli.orders.get("manual-2") is not None)
            assert cli.positions.net("ACC3", "rb2701") == 106  # only the 6th lot is newer than the snapshot
            # status-only update of another order whose fills all predate the snapshot: nothing new
            req2 = OrderRequest(client_order_id="manual-3", account_id="ACC3", instrument_id="rb2701", direction=BUY,
                                offset=Offset.OPEN, price=3121.0, qty=4, source=OrderSource.MANUAL)
            old = shard.update_for(req2, OrderStatus.CANCELED, filled_qty=3, avg_fill_price=3121.0,
                                   last_fill=FillRef(trade_id="T7", price=3121.0, qty=3, trade_ts_ns=snap.as_of_ns - S))
            await shard.publish_update(old)
            await until(lambda: cli.orders.get("manual-3") is not None)
            assert cli.positions.net("ACC3", "rb2701") == 106

    async def test_seed_replays_fills_newer_than_the_snapshot(self, bus_cfg: FullConfig) -> None:
        async with FutriskClient(bus_cfg) as cli:
            cli.seed_positions("ACC3", [view("rb2701", td=10)], as_of_ns=T0)
            own = cli.new_order("ACC3", "rb2701", BUY, Offset.OPEN, 4, 3120.0)
            cli.orders.add_request(own)
            await cli._apply_update(FakeShardUpdate.of(own, 2, 2, T0 + 5 * S))  # applied after the snapshot
            assert cli.positions.net("ACC3", "rb2701") == 12
            # a later seed taken before that fill (the RPC reply overtook the order stream): replayed
            cli.seed_positions("ACC3", [view("rb2701", td=10)], as_of_ns=T0 + 2 * S)
            assert cli.positions.net("ACC3", "rb2701") == 12
            # a seed that already contains it: not counted twice
            cli.seed_positions("ACC3", [view("rb2701", td=12)], as_of_ns=T0 + 6 * S)
            assert cli.positions.net("ACC3", "rb2701") == 12
            # a fill that happened before the seed but arrives afterwards: part of the seed
            await cli._apply_update(FakeShardUpdate.of(own, 3, 1, T0 + 6 * S - 1))
            assert cli.positions.net("ACC3", "rb2701") == 12
            await cli._apply_update(FakeShardUpdate.of(own, 4, 1, T0 + 7 * S))
            assert cli.positions.net("ACC3", "rb2701") == 13


class FakeShardUpdate:
    """``OrderUpdate`` of an own request with one new fill at engine time ``ts``."""

    @staticmethod
    def of(req: OrderRequest, filled: int, last_qty: int, ts: int):  # type: ignore[no-untyped-def]
        from futrisk.messages.enums import OrderType, TimeInForce
        from futrisk.messages.orders import OrderUpdate

        return OrderUpdate(client_order_id=req.client_order_id, account_id=req.account_id,
                           instrument_id=req.instrument_id, exchange=Exchange.SHFE, direction=req.direction,
                           offset=req.offset, order_type=OrderType.LIMIT, tif=TimeInForce.GFD, price=req.price,
                           qty=req.qty, status=OrderStatus.FILLED if filled >= req.qty else OrderStatus.PARTIALLY_FILLED,
                           filled_qty=filled, avg_fill_price=req.price,
                           last_fill=FillRef(trade_id=f"T{filled}", price=req.price or 0.0, qty=last_qty, trade_ts_ns=ts),
                           source=req.source, trading_day="20260928", created_ns=ts, updated_ns=ts)


class TestClockAndRevival:
    async def test_lost_order_is_revived_by_a_live_update(self, bus_cfg: FullConfig, fake_bus: FakeBus) -> None:
        shard = fake_bus.shard("risk-engine-1")
        shard.script.append(NO_REPLY)
        async with FutriskClient(bus_cfg, order_timeout_ms=100, max_retries=0) as cli:
            cli.subscribe_orders(["ACC3"])
            await shard.pub.wait_subscribed("oms.order.ACC3")
            res = await cli.place_limit("ACC3", "rb2701", SELL, Offset.CLOSE_TODAY, 3, 3120.0)
            assert res.reply.code == codes.TIMEOUT
            cli.orders.mark_lost(res.client_order_id)
            o = cli.orders.get(res.client_order_id)
            assert o is not None and not o.working
            await shard.publish_update(shard.update_for(res.request, OrderStatus.ACCEPTED))
            await until(lambda: cli.orders.get(res.client_order_id).working)
            assert o.state == ClientState.ACKED and o.status == OrderStatus.ACCEPTED
            assert cli.orders.pending_closes("ACC3", "rb2701", PosDirection.LONG).td == 3

    async def test_demo_clock_behind_the_wall_clock(self, bus_cfg: FullConfig) -> None:
        offset = -3 * 86_400 * S  # e.g. `futrisk demo --start-at` three days in the past
        async with FakeBus(bus_cfg, fill_mode="none", clock_offset_ns=offset) as bus, FutriskClient(bus_cfg) as cli:
            shard = bus.shard("risk-engine-1")
            cli.subscribe_orders(["ACC3"])
            await shard.pub.wait_subscribed("oms.order.ACC3")
            cli.subscribe_ticks(["rb2701"])
            await bus.md.wait_subscribed("md.tick.rb2701")
            await bus.md.tick(make_tick("rb2701", 3120.0, ts_ns=shard.now_ns()))
            assert await cli.wait_ticks(["rb2701"], timeout_s=3)
            res = await cli.place_limit("ACC3", "rb2701", BUY, Offset.OPEN, 2, 3121.0)
            assert abs(res.request.sent_ns - shard.now_ns()) < 5 * S  # sent_ns follows the market clock
            await shard.publish_update(shard.update_for(res.request, OrderStatus.ACCEPTED))
            await until(lambda: cli.orders.get(res.client_order_id).status == OrderStatus.ACCEPTED)
            await shard.publish_update(shard.update_for(res.request, OrderStatus.CANCELED))
            await until(lambda: cli.orders.get(res.client_order_id).terminal)
            assert cli.orders.get(res.client_order_id).status == OrderStatus.CANCELED


async def test_start_close_idempotent(bus_cfg: FullConfig) -> None:
    cli = FutriskClient(bus_cfg)
    await cli.start()
    await cli.start()
    await cli.close()
    await cli.close()
    with pytest.raises(ValueError):
        FutriskClient()


async def test_bad_messages_and_failing_callbacks_never_stop_the_readers(client: FutriskClient,
                                                                        fake_bus: FakeBus) -> None:
    from futrisk.messages.base import encode
    from futrisk.messages.common import Pong
    from futrisk.messages.envelope import Envelope, encode_envelope

    got: list[float] = []

    def boom(t: Tick) -> None:
        raise RuntimeError("strategy bug")

    client.subscribe_ticks(["rb2701"], boom)
    client.subscribe_ticks(["rb2701"], lambda t: got.append(t.last))
    await fake_bus.md.wait_subscribed("md.tick.rb2701")
    sock = fake_bus.md.sock
    assert sock is not None
    garbage = Envelope(topic="md.tick.rb2701", type="Tick", src="md-gateway", epoch="md-epoch", seq=1, ts_ns=1,
                       payload=encode(Pong(instance="x", state="y", ts_ns=1)))
    await sock.send_multipart([b"md.tick.rb2701", encode_envelope(garbage)])  # wrong payload for the type
    await sock.send_multipart([b"md.tick.rb2701", b"not msgpack"])  # undecodable envelope
    await sock.send_multipart([b"md.tick.hc2701", encode_envelope(garbage)])  # frame/topic mismatch
    await fake_bus.md.tick(make_tick("rb2701", 3120.0, ts_ns=T0), seq=2)
    await until(lambda: got == [3120.0])
    events: list[str] = []
    await orders_ready(client, fake_bus, "ACC3", "risk-engine-1", lambda e: events.append(e.update.client_order_id))
    shard = fake_bus.shard("risk-engine-1")
    assert shard.pub.sock is not None
    bad = Envelope(topic="oms.order.ACC3", type="OrderUpdate", src="risk-engine-1", epoch="e", stream="s", seq=1,
                   ts_ns=1, payload=b"\x93\x01\x02\x03")
    await shard.pub.sock.send_multipart([b"oms.order.ACC3", encode_envelope(bad)])
    ok = OrderRequest(client_order_id="ok", account_id="ACC3", instrument_id="rb2701", direction=BUY,
                      offset=Offset.OPEN, price=3120.0, qty=1)
    await shard.publish_update(shard.update_for(ok, OrderStatus.ROUTING))
    await until(lambda: events == ["ok"])
