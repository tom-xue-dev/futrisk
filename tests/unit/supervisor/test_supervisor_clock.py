"""ClockKeeper: market clock targets with the real calendar, persistence, forward-only jumps
(01 §8.2; 02 §6.6 ``svc.clock``, §8.8; 03 §4.6 ``clock_state``, §7)."""

from __future__ import annotations

from datetime import datetime
from functools import partial
from pathlib import Path

import pytest

from futrisk.common.clock import BJ, MarketClock, bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.common.config.app import AppConfig
from futrisk.db import Database, ProcessedCommands
from futrisk.messages import codes
from futrisk.messages.enums import ClockMode, ClockTarget
from futrisk.messages.system import ClockJumpRequest, ClockState
from futrisk.services.supervisor.clock_keeper import (
    ClockJumpError,
    ClockKeeper,
    initial_market_ns,
    jump_target_ns,
    read_clock_state,
    session_templates,
)
from futrisk.services.supervisor.schema import METADATA

HOUR = 3_600 * 1_000_000_000


def bj(text: str) -> int:
    return bj_to_ns(datetime.fromisoformat(text).replace(tzinfo=BJ))


class FakeWall:
    def __init__(self, ns: int) -> None:
        self.ns = ns

    def now_ns(self) -> int:
        return self.ns

    def monotonic_ns(self) -> int:
        return self.ns


@pytest.fixture
def target(default_config: FullConfig) -> partial[int]:
    return partial(
        jump_target_ns,
        calendar=default_config.calendar,
        demo=default_config.app.demo,
        templates=session_templates(default_config.products),
    )


def test_session_templates_are_the_union_used_by_products(default_config: FullConfig) -> None:
    assert session_templates(default_config.products) == sorted(
        {"night_2100_2300", "night_2100_0100", "night_2100_0230", "day_commodity", "day_index", "day_treasury"}
    )


def test_initial_market_time(default_config: FullConfig) -> None:
    cal = default_config.calendar
    # Friday 2026-09-25 is a holiday: the next trading day's 08:58 is Monday's
    assert initial_market_ns(cal, None, bj("2026-09-25T13:00:00")) == bj("2026-09-28T08:58:00")
    assert initial_market_ns(cal, None, bj("2026-09-28T07:00:00")) == bj("2026-09-28T08:58:00")
    assert initial_market_ns(cal, None, bj("2026-09-28T09:00:00")) == bj("2026-09-29T08:58:00")
    assert initial_market_ns(cal, None, bj("2026-09-30T16:00:00")) == bj("2026-10-08T08:58:00")  # National Day
    assert initial_market_ns(cal, "2026-09-28T21:05:00", 0) == bj("2026-09-28T21:05:00")  # naive = Beijing
    assert initial_market_ns(cal, "2026-09-28T13:05:00+00:00", 0) == bj("2026-09-28T21:05:00")


def test_pre_close(target: partial[int]) -> None:
    assert target(ClockTarget.PRE_CLOSE, market_now_ns=bj("2026-09-28T10:00:00")) == bj("2026-09-28T14:58:00")
    # a night session belongs to the next trading day, so does its pre-close
    assert target(ClockTarget.PRE_CLOSE, market_now_ns=bj("2026-09-28T21:30:00")) == bj("2026-09-29T14:58:00")
    # Saturday -> Monday
    assert target(ClockTarget.PRE_CLOSE, market_now_ns=bj("2026-10-10T10:00:00")) == bj("2026-10-12T14:58:00")
    with pytest.raises(ClockJumpError) as err:  # after the close: backwards
        target(ClockTarget.PRE_CLOSE, market_now_ns=bj("2026-09-28T15:10:00"))
    assert err.value.code == codes.CONFLICT


def test_next_open_across_the_national_day_holiday(target: partial[int]) -> None:
    # no night session on 09-30 (a weekday holiday follows): 10-08 08:55 auction minus 120 s
    assert target(ClockTarget.NEXT_OPEN, market_now_ns=bj("2026-09-30T15:30:00")) == bj("2026-10-08T08:53:00")
    # an ordinary evening: night auction 20:55 minus 120 s
    assert target(ClockTarget.NEXT_OPEN, market_now_ns=bj("2026-09-28T15:30:00")) == bj("2026-09-28T20:53:00")
    # Friday evening: Monday's night session opens on Friday
    assert target(ClockTarget.NEXT_OPEN, market_now_ns=bj("2026-10-09T15:30:00")) == bj("2026-10-09T20:53:00")
    # inside the commodity session the next window start is the index/treasury auction at 09:25
    assert target(ClockTarget.NEXT_OPEN, market_now_ns=bj("2026-10-08T09:10:00")) == bj("2026-10-08T09:23:00")
    with pytest.raises(ClockJumpError) as err:  # already inside lead_s of the next window
        target(ClockTarget.NEXT_OPEN, market_now_ns=bj("2026-09-28T20:54:00"))
    assert err.value.code == codes.CONFLICT


def test_at(target: partial[int]) -> None:
    now = bj("2026-09-28T10:00:00")
    assert target(ClockTarget.AT, market_now_ns=now, at_ns=now + 1) == now + 1
    with pytest.raises(ClockJumpError) as err:
        target(ClockTarget.AT, market_now_ns=now, at_ns=now)
    assert err.value.code == codes.CONFLICT
    with pytest.raises(ClockJumpError) as err:
        target(ClockTarget.AT, market_now_ns=now)
    assert err.value.code == codes.BAD_REQUEST


async def test_clock_state_is_persisted_reused_and_only_moves_forward(tmp_path: Path, default_config: FullConfig) -> None:
    db = Database(tmp_path / "supervisor.db", METADATA, synchronous="FULL")
    db.open()
    wall = FakeWall(bj("2026-09-25T13:00:00"))
    published: list[ClockState] = []
    cal, tpl = default_config.calendar, session_templates(default_config.products)
    demo_app: AppConfig = default_config.app.model_copy(
        update={"mode": "demo", "demo": default_config.app.demo.model_copy(update={"start_at": None})}
    )

    def keeper(app: AppConfig, override: str | None = None, market: MarketClock | None = None) -> ClockKeeper:
        return ClockKeeper(
            db=db,
            app=app,
            calendar=cal,
            templates=tpl,
            market_clock=market or MarketClock(wall),
            wall=wall,
            publish=published.append,
            start_at_override=override,
        )

    async def jump(k: ClockKeeper, cid: str, tgt: ClockTarget, at_ns: int | None = None, requester: str = "api-gateway"):
        return await k.jump(ClockJumpRequest(command_id=cid, target=tgt, at_ns=at_ns, actor="ops"), requester=requester, received_ns=1)

    try:
        market = MarketClock(wall)
        k = keeper(demo_app, market=market)
        rec = await k.start()
        assert rec.mode == ClockMode.DEMO and rec.version == 1
        assert market.mode == "DEMO" and market.version == 1 and market.now_ns() == bj("2026-09-28T08:58:00")
        assert published[-1].version == 1 and published[-1].market_now_ns == bj("2026-09-28T08:58:00")
        assert db.read(read_clock_state) == rec

        # PRE_CLOSE: persisted before it is published, version + 1
        r = await jump(k, "c1", ClockTarget.PRE_CLOSE)
        assert r.ok and not r.duplicate
        st = r.open_as(ClockState)
        assert (st.version, st.market_now_ns, st.actor) == (2, bj("2026-09-28T14:58:00"), "ops")
        assert market.version == 2 and market.now_ns() == bj("2026-09-28T14:58:00") and published[-1].version == 2
        assert db.read(read_clock_state).version == 2

        dup = await jump(k, "c1", ClockTarget.PRE_CLOSE)  # retry: same answer, nothing moves
        assert dup.duplicate and dup.open_as(ClockState).version == 2 and k.version == 2
        clash = await jump(k, "c1", ClockTarget.NEXT_OPEN)
        assert clash.code == codes.CONFLICT
        back = await jump(k, "c2", ClockTarget.AT, at_ns=bj("2026-09-28T10:00:00"))
        assert back.code == codes.CONFLICT and k.version == 2 and market.version == 2

        # restart two (wall) hours later: the persisted offset is reused, market time continued
        wall.ns += 2 * HOUR
        k2 = keeper(demo_app)
        rec2 = await k2.start()
        assert (rec2.offset_ns, rec2.version) == (k.offset_ns, 2)
        assert k2.market_now_ns() == bj("2026-09-28T16:58:00")
        # NEXT_OPEN from 16:58: the night auction 20:55 minus 2 min
        r3 = await jump(k2, "c3", ClockTarget.NEXT_OPEN)
        assert r3.ok and r3.open_as(ClockState).market_now_ns == bj("2026-09-28T20:53:00") and k2.version == 3

        # --start-at never moves the clock backwards; a later one moves it forward
        assert (await keeper(demo_app, override="2026-09-28T09:00:00").start()).version == 3
        k4 = keeper(demo_app, override="2026-09-29T08:58:00")
        assert (await k4.start()).version == 4 and k4.market_now_ns() == bj("2026-09-29T08:58:00")

        # prod mode: REAL clock, offset 0; svc.clock refused
        k5 = keeper(default_config.app.model_copy(update={"mode": "prod"}))
        rec5 = await k5.start()
        assert (rec5.mode, rec5.offset_ns, rec5.version) == (ClockMode.REAL, 0, 5)
        refused = await jump(k5, "c4", ClockTarget.NEXT_OPEN)
        assert refused.code == codes.PRECONDITION_FAILED
        processed = ProcessedCommands(db)
        stored = db.read(lambda c: [processed.lookup(c, cid) for cid in ("c1", "c2", "c3", "c4")])
        assert all(e is not None and e.command == "svc.clock" and e.requester == "api-gateway" for e in stored)
    finally:
        db.close()
