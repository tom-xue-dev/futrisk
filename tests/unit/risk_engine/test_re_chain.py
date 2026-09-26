"""Slow chain test: real sim-exchange -> real md-gateway -> the risk-engine shell (fake trade-gateway / clearing).

The asyncio services (sim-exchange, md-gateway) run on a selector event loop in their own thread; the shell and the
fakes run on their sync runtimes. Checks that the shell warms up on md-gateway's LIVE ticks after its bootstrap,
evaluates on them, and fails safe (STALE, opens refused) when md-gateway goes away.
"""

from __future__ import annotations

import asyncio
import dataclasses
import threading
import time
from collections.abc import Coroutine
from pathlib import Path
from typing import Any, TypeVar

import pytest

from futrisk.common.clock import MarketClock
from futrisk.common.config import FullConfig, RiskRulesConfig
from futrisk.common.config.feeds import ArbitrationConfig
from futrisk.common.paths import HomeLayout
from futrisk.messages import codes
from futrisk.messages.enums import Direction, MdQuality, Offset, OrderStatus, OrderType, SourceId
from futrisk.messages.orders import OrderRequest
from futrisk.runtime import AsyncServiceRuntime
from futrisk.runtime.testing import config_with_free_ports
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.service import MdGatewayService
from futrisk.services.risk_engine.service import Phase
from futrisk.sim.exchange import SimExchange
from futrisk.sim.exchange.testing import MARKET_OPEN, make_test_config, offset_for

from .re_support import (
    PEER_TIMEOUT_MISSED,
    FakeClearing,
    FakeTradeGateway,
    Shell,
    client,
    daily_loss_rule,
    request,
    sod,
    uuid7,
    wait_for,
    wait_ready,
)

pytestmark = [pytest.mark.slow, pytest.mark.timeout(180)]
T = TypeVar("T")


class LoopThread:
    """A selector event loop on its own thread (zmq.asyncio needs add_reader on Windows)."""

    def __init__(self) -> None:
        self.loop = asyncio.SelectorEventLoop()
        self._t = threading.Thread(target=self.loop.run_forever, name="async-services", daemon=True)
        self._t.start()

    def call(self, coro: Coroutine[Any, Any, T], timeout: float = 30.0) -> T:
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._t.join(10)
        self.loop.close()


def chain_config(default_config: FullConfig) -> FullConfig:
    """Sim on ephemeral ports + bus on free ports + fast Feed B (as the md-gateway tests) + the daily_loss rule."""
    cfg = config_with_free_ports(make_test_config(default_config), heartbeat_ms=100,
                                 peer_timeout_missed=PEER_TIMEOUT_MISSED)
    feeds = cfg.feeds.model_copy(update={
        "feed_a": cfg.feeds.feed_a.model_copy(update={"reconnect_backoff_ms": [50, 100, 200]}),
        "feed_b": cfg.feeds.feed_b.model_copy(update={"poll_ms": 200, "timeout_ms": 500}),
    })
    rr = RiskRulesConfig(rules=[daily_loss_rule()], pretrade=cfg.risk_rules.pretrade, system=cfg.risk_rules.system)
    return dataclasses.replace(cfg, feeds=feeds, risk_rules=rr)


def md_params() -> MdParams:
    arb = ArbitrationConfig(probation_healthy_s=1.0, min_dwell_s=0.5, stale_s=0.6, unknown_s=2.0, open_grace_s=1.0)
    return MdParams(arb=arb, startup_grace_s=5.0, raw_roll_s=1.0, record_handoff_s=0.1, bar_close_grace_s=1.0)


async def start_md(cfg: FullConfig, home: Path, offset: int, ex: SimExchange) -> tuple[AsyncServiceRuntime, MdGatewayService]:
    rt = AsyncServiceRuntime("md-gateway", config=cfg, home=home, market_clock=MarketClock(mode="DEMO", offset_ns=offset),
                             setup_logs=False)
    await rt.start()
    svc = MdGatewayService(rt, params=md_params(),
                           source_urls={SourceId.FEED_A: ex.feed_a_url, SourceId.FEED_B: ex.feed_b_url})
    await svc.start()
    return rt, svc


def test_real_md_gateway_ticks_drive_the_shell_and_its_loss_fails_safe(tmp_path: Path, default_config: FullConfig) -> None:
    cfg = chain_config(default_config)
    offset = offset_for(MARKET_OPEN)
    pre = {k: float(v.pre_settle) for k, v in cfg.sim.price_engine.instruments.items() if k in ("rb2701", "IF2612")}
    aio = LoopThread()
    ex = SimExchange(cfg, HomeLayout(tmp_path / "sim"), offset)
    aio.call(ex.start())
    md_rt: AsyncServiceRuntime | None = None
    clock = MarketClock(mode="DEMO", offset_ns=offset)
    tg = FakeTradeGateway(cfg, tmp_path / "tg", clock).start()
    cw = FakeClearing(cfg, tmp_path / "cw", clock, sods=[sod({"ACC3": ("10000000", []), "ACC4": ("10000000", []),
                                                               "ACC5": ("5000000", [])}, pre=pre)]).start()
    sh = Shell(cfg, tmp_path / "re1", clock, 1)
    bot = client(cfg, "strategy-sim")
    try:
        md_rt, _svc = aio.call(start_md(cfg, tmp_path / "md", offset, ex))
        sh.start(Phase.RUNNING, timeout=30)
        wait_ready([bot], "risk-engine-1")
        wait_for(lambda: sh.call(lambda: sh.core.quality_of("rb2701")) == MdQuality.OK, 30, 0.1,
                 "LIVE rb2701 ticks from md-gateway (after its bootstrap)")
        first = sh.call(lambda: sh.core.mark_of("rb2701"))
        n0 = sh.svc.rates.totals.get("marks", 0)
        wait_for(lambda: sh.svc.rates.totals.get("marks", 0) > n0 + 20, step=0.1, what="marks flowing")
        mark = sh.call(lambda: sh.core.mark_of("rb2701"))
        assert first is not None and mark is not None and abs(mark - pre["rb2701"]) / pre["rb2701"] < 0.05
        coid = uuid7()
        req = OrderRequest(client_order_id=coid, account_id="ACC3", instrument_id="rb2701", direction=Direction.BUY,
                           offset=Offset.OPEN, order_type=OrderType.LIMIT, price=float(round(mark)), qty=2,
                           sent_ns=time.time_ns())
        r = request(bot, "risk-engine-1", "order.submit", req)
        assert r.ok, r
        wait_for(lambda: sh.call(lambda: (lambda o: o is not None and o.status == OrderStatus.ACCEPTED)(
            sh.core.orders.get(coid))), what="accepted")
        tg.fill(coid)
        wait_for(lambda: sh.call(lambda: sh.core.account("ACC3").ledger.book.positions("ACC3") != []), what="position")
        snap = sh.call(lambda: sh.core.snapshot("ACC3"))
        assert snap.quality == MdQuality.OK and snap.positions and snap.positions[0].last is not None

        aio.call(md_rt.stop())  # md-gateway gone: heartbeats stop
        md_rt = None
        wait_for(lambda: sh.call(lambda: sh.core.quality_of("rb2701")) == MdQuality.STALE, step=0.05,
                 what="STALE after md down")
        req2 = OrderRequest(client_order_id=uuid7(), account_id="ACC3", instrument_id="rb2701", direction=Direction.BUY,
                            offset=Offset.OPEN, order_type=OrderType.LIMIT, price=float(round(mark)), qty=1,
                            sent_ns=time.time_ns())
        assert request(bot, "risk-engine-1", "order.submit", req2).code == codes.REJECT_MD_UNAVAILABLE
    finally:
        bot.close()
        sh.stop()
        tg.stop()
        cw.stop()
        if md_rt is not None:
            aio.call(md_rt.stop())
        aio.call(ex.stop(), timeout=30)
        aio.close()
