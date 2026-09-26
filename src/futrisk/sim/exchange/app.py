"""sim-exchange: the virtual exchange process (01 §5.1 / §8.13, 02 §9.1-§9.5, 03 §4.7).

One asyncio process hosting:

- :class:`~futrisk.sim.exchange.engine.PriceEngine`: a new market state every
  ``tick_interval_ms`` (aligned to market-time boundaries) for the contracts whose
  product is in a continuous session at that market time;
- Feed A (WebSocket push, 02 §9.1) and Feed B (HTTP poll, 02 §9.2), both fed from the
  same states;
- the SimBroker (ZeroMQ ROUTER + JSON, 02 §9.4) persisted in ``<home>/db/sim.db``;
- the control API (02 §9.5, ``app.mode=demo`` only) with faults, shocks, scenarios and
  the market-clock pause / clock / resume steps of 02 §8.8.

The market clock is ``wall + offset`` (01 §8.2). The offset comes from the command line
(``--clock-offset-ns``, passed by the supervisor at start-up) and is changed only through
``POST /control/clock`` while paused. Each round is persisted before it is published, so
a restarted process continues the day's cumulative volume.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from loguru import logger

from futrisk.common.clock import Clock, MarketClock, SystemClock, td_str
from futrisk.common.config import FullConfig
from futrisk.common.ids import uuid7
from futrisk.common.paths import HomeLayout, bundled_config_dir
from futrisk.sim.exchange.broker import SimBroker
from futrisk.sim.exchange.control import ControlServer
from futrisk.sim.exchange.engine import PriceEngine, Shock
from futrisk.sim.exchange.errors import bad_request, conflict
from futrisk.sim.exchange.faults import FEED_A, Fault, FaultManager
from futrisk.sim.exchange.feed_a import FeedAServer
from futrisk.sim.exchange.feed_b import FeedBService
from futrisk.sim.exchange.scenario import ScenarioRunner
from futrisk.sim.exchange.store import SimStore
from futrisk.sim.exchange.tasks import spawn


class SimExchange:
    """The whole virtual exchange; ``async with SimExchange(...)`` starts and stops it."""

    paused: bool
    clock: MarketClock
    faults: FaultManager
    store: SimStore
    feed_a: FeedAServer
    feed_b: FeedBService
    broker: SimBroker
    control: ControlServer | None
    scenarios: ScenarioRunner

    def __init__(
        self,
        cfg: FullConfig,
        layout: HomeLayout,
        clock_offset_ns: int = 0,
        *,
        clock_version: int = 0,
        wall: Clock | None = None,
    ) -> None:
        self.cfg = cfg
        self.layout = layout
        self.paused = False
        self.demo = cfg.app.mode == "demo"
        if not self.demo and clock_offset_ns:
            raise ValueError("a market-clock offset is only allowed in demo mode (02 §8.8)")
        self.clock = MarketClock(
            wall or SystemClock(),
            mode="DEMO" if self.demo else "REAL",
            offset_ns=clock_offset_ns,
            version=clock_version,
        )
        s = cfg.sim
        self.faults = FaultManager(self, seed=s.price_engine.seed)
        self.store = SimStore(layout.db_path("sim"))
        self._engine: PriceEngine | None = None
        self.feed_a = FeedAServer(
            host=s.feed_a.host,
            port=s.feed_a.port,
            ring_size=s.feed_a.ring_size,
            hb_ms=s.feed_a.heartbeat_ms,
            clock=self.clock,
            calendar=cfg.calendar,
            faults=self.faults,
        )
        self.feed_b = FeedBService(
            host=s.feed_b.host,
            port=s.feed_b.port,
            clock=self.clock,
            calendar=cfg.calendar,
            faults=self.faults,
            health_extra=lambda: {"epoch": self.feed_a.epoch, "paused": self.paused},
        )
        self.broker = SimBroker(cfg, clock=self.clock, store=self.store)
        self.control = ControlServer(self, s.control.host, s.control.port) if self.demo else None
        # the active config version first; the bundled defaults cover homes created before
        # scenario files existed
        scenario_dirs = [cfg.scenarios_dir]
        bundled = bundled_config_dir() / "scenarios"
        if bundled.resolve() != cfg.scenarios_dir.resolve():
            scenario_dirs.append(bundled)
        self.scenarios = ScenarioRunner(self, scenario_dirs)
        self._running = asyncio.Event()
        # held for a whole engine round (step -> persist -> publish): pause() and stop() take
        # it, so they never return in the middle of a round
        self._round_lock = asyncio.Lock()
        self._tasks: list[asyncio.Task[None]] = []
        self._state = "NEW"  # NEW | RUNNING | STOPPED
        self.started_ns = 0
        self.last_round_ns = 0

    # ------------------------------------------------------------------ properties
    @property
    def engine(self) -> PriceEngine:
        if self._engine is None:
            raise RuntimeError("sim-exchange is not started")
        return self._engine

    @property
    def epoch(self) -> str:
        return self.feed_a.epoch

    @property
    def feed_a_url(self) -> str:
        return self.feed_a.url

    @property
    def feed_b_url(self) -> str:
        return self.feed_b.url

    @property
    def broker_endpoint(self) -> str:
        return self.broker.endpoint

    @property
    def control_url(self) -> str | None:
        return self.control.url if self.control is not None else None

    def trading_day(self) -> str:
        try:
            return td_str(self.cfg.calendar.current_or_next_trading_day(self.clock.now_ns()))
        except LookupError:
            return ""

    # ------------------------------------------------------------------ lifecycle
    async def __aenter__(self) -> SimExchange:
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def start(self) -> None:
        if self._state != "NEW":
            raise RuntimeError("SimExchange can only be started once")
        self._state = "RUNNING"
        try:
            await self.store.open()
            meta = await self.store.call(self.store.load_meta)
            md = await self.store.call(self.store.load_md_states)
            self._engine = PriceEngine(
                self.cfg,
                self.clock.now_ns(),
                restored=md,
                rng_td=meta.get("md_rng_td"),
                rng_step=int(meta.get("md_step") or 0),
            )
            await self.store.call(self.store.set_md_epoch, self.feed_a.epoch, self.clock.now_ns())
            states = list(self._engine.states.values())
            self.feed_a.init_views(states)
            self.feed_b.init_views(states)
            await self.broker.start(self._engine)
            await self.feed_a.start()
            await self.feed_b.start()
            if self.control is not None:
                await self.control.start()
        except BaseException:
            await self.stop()
            raise
        self._running.set()
        self._tasks = [spawn("sim-engine", self._engine_loop), spawn("sim-fault-reaper", self._reaper)]
        self.started_ns = self.clock.now_ns()
        logger.info(
            "sim-exchange started: mode={} market_now_ns={} offset_ns={} epoch={} feed_a={} feed_b={} "
            "broker={} control={} instruments={}",
            self.cfg.app.mode, self.started_ns, self.clock.offset_ns, self.epoch, self.feed_a_url,
            self.feed_b_url, self.broker_endpoint, self.control_url, len(self.engine.metas),
        )

    async def stop(self) -> None:
        if self._state == "STOPPED":
            return
        self._state = "STOPPED"
        with contextlib.suppress(Exception):
            await self.scenarios.cancel()
        self._running.clear()
        async with self._round_lock:  # an in-progress round completes (persist + publish) first
            for t in self._tasks:
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        # network front ends first (concurrently), then the broker, then the database
        fronts = [self.feed_b.stop(), self.feed_a.stop()]
        if self.control is not None:
            fronts.append(self.control.stop())
        for res in await asyncio.gather(*fronts, return_exceptions=True):
            if isinstance(res, Exception):
                logger.opt(exception=res).error("sim-exchange: error while stopping a server")
        for step in (self.broker.stop, self.store.close):
            try:
                await step()
            except Exception:
                logger.exception("sim-exchange: error while stopping")
        logger.info("sim-exchange stopped")

    # ------------------------------------------------------------------ engine loop
    async def _engine_loop(self) -> None:
        interval = self.engine.interval_ns
        while True:
            await self._running.wait()
            now = self.clock.now_ns()
            boundary = (now // interval + 1) * interval
            # asyncio timers can fire ~15.6 ms early on Windows: never stamp a state in the future
            while now < boundary:
                await asyncio.sleep((boundary - now) / 1e9)
                now = self.clock.now_ns()
            async with self._round_lock:
                if self.paused or not self._running.is_set():
                    continue
                try:
                    await self._round(boundary)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("sim engine round failed")

    async def _round(self, t_ns: int) -> None:
        eng = self.engine
        changed = eng.step(t_ns)
        self.last_round_ns = t_ns
        if not changed:
            return
        ids = [st.instrument_id for st in changed]
        # persist before publishing: a restart never re-issues a lower cumulative volume;
        # shielded so a cancellation cannot drop the write once it is queued
        await asyncio.shield(
            self.store.call(self.store.save_md, eng.dump_states(ids), eng.rng_td, eng.rng_step, t_ns))
        self.feed_a.publish(changed)
        self.feed_b.publish(changed)
        self.broker.on_market([eng.quote(i) for i in ids])
        eng.prune_shocks()

    async def _reaper(self) -> None:
        while True:
            await asyncio.sleep(0.1)
            try:
                await self.faults.reap()
            except Exception:
                logger.exception("sim fault reaper failed")

    # ------------------------------------------------------------------ control operations
    async def pause(self) -> None:
        """02 §8.8 step 1: stop pushing market data and pause matching.

        Returns once a round that was already running has been published, so ``head_seq``
        and every price are frozen when the call returns.
        """
        if not self.paused:
            self.paused = True
            self._running.clear()
            self.broker.pause()
        async with self._round_lock:
            pass
        logger.info("sim-exchange paused at market time {} (head_seq={})", self.clock.now_ns(), self.feed_a.head_seq)

    def resume(self) -> None:
        """02 §8.8 step 5: continue at the current market time (only heartbeats out of session)."""
        if self.paused:
            self.paused = False
            self._running.set()
            self.broker.resume()
            logger.info("sim-exchange resumed at market time {}", self.clock.now_ns())

    def set_clock(self, offset_ns: int, version: int) -> int:
        """02 §8.8 step 3: adopt the supervisor's new offset (only while paused)."""
        if not self.demo:
            raise conflict("the market clock can only be moved in demo mode")
        if not self.paused:
            raise conflict("POST /control/pause first: the clock can only change while paused (02 §8.8)")
        if version < self.clock.version:
            raise conflict(f"stale clock version {version} < {self.clock.version}")
        self.clock.apply("DEMO", offset_ns, version)
        now = self.clock.now_ns()
        logger.info("sim-exchange market clock set: offset_ns={} version={} market_now_ns={}", offset_ns, version, now)
        return now

    def add_shock(
        self,
        *,
        pct: Any,
        instruments: list[str] | None = None,
        sector: str | None = None,
        duration_s: float = 0.0,
        shape: str = "STEP",
    ) -> Shock:
        if isinstance(pct, bool) or not isinstance(pct, int | float):
            raise bad_request("pct must be a number (ratio: -0.04 means -4%)")
        try:
            shock = self.engine.add_shock(
                uuid7(),
                pct=float(pct),
                instruments=instruments,
                sector=sector,
                duration_s=float(duration_s),
                shape=str(shape),
                created_ns=self.clock.now_ns(),
            )
        except ValueError as exc:
            raise bad_request(str(exc)) from exc
        logger.info("sim shock {}: {} {:+.2%} over {}s ({} targets)", shock.shock_id, shock.shape, shock.pct,
                    shock.duration_s, len(shock.targets))
        return shock

    # ------------------------------------------------------------------ fault hooks
    def known_instrument(self, instrument_id: str) -> bool:
        return self._engine is not None and self._engine.meta(instrument_id) is not None

    async def fault_started(self, fault: Fault) -> None:
        t = fault.type
        if t == "FEED_DISCONNECT" and fault.target == FEED_A:
            fault.detail["connections"] = self.feed_a.disconnect_all(fault.params["mode"])
        elif t == "FEED_FREEZE":
            self._refresh_frozen()
        elif t == "SIM_RESTART":
            epoch = self.feed_a.restart_epoch()
            await self.store.call(self.store.set_md_epoch, epoch, self.clock.now_ns())
            await self.broker.restart()
            fault.detail["epoch"] = epoch
        elif t == "BROKER_FORCE_CLOSE":
            fault.detail["result"] = await self.broker.force_close(**fault.params)
        elif t == "BROKER_DISCONNECT":
            await self.broker.disconnect()
        logger.warning("fault {} {} started (target={}, params={})", fault.fault_id, t, fault.target, fault.params)

    async def fault_ended(self, fault: Fault) -> None:
        t = fault.type
        if t == "FEED_DISCONNECT" and fault.target == FEED_A and fault.params["mode"] == "silent":
            self.feed_a.close_muted()
        elif t == "FEED_FREEZE":
            self._refresh_frozen()
        elif t == "FEED_REORDER" and fault.target == FEED_A:
            self.feed_a.flush_reorder()
        elif t == "BROKER_DISCONNECT":
            self.broker.reconnect()
        logger.info("fault {} {} ended ({})", fault.fault_id, t, fault.state)

    def _refresh_frozen(self) -> None:
        if self._engine is not None:
            self._engine.frozen = self.faults.engine_frozen()

    # ------------------------------------------------------------------ state
    def state(self) -> dict[str, Any]:
        eng = self.engine
        return {
            "epoch": self.epoch,
            "trading_day": self.trading_day(),
            "faults": self.faults.list(),
            "fault_history": [f for f in self.faults.list(include_done=True) if f["state"] != "ACTIVE"],
            "scenario": self.scenarios.state(),
            "prices": eng.prices(),
            "paused": self.paused,
            "market_now_ns": self.clock.now_ns(),
            "clock": {"mode": self.clock.mode, "offset_ns": self.clock.offset_ns, "version": self.clock.version},
            "shocks": [s.to_dict() for s in eng.shocks.values()],
            "instruments": [
                {
                    "instrument_id": st.instrument_id,
                    "trading_day": st.trading_day,
                    "last": float(st.meta.price(st.last)),
                    "volume": st.volume,
                    "open_interest": st.open_interest,
                    "limit_lock": st.lock.value,
                    "frozen": st.instrument_id in eng.frozen,
                    "ts_ns": st.ts_ns,
                }
                for st in eng.states.values()
            ],
            "engine": {
                "seed": eng.seed,
                "tick_interval_ms": eng.interval_ms,
                "rounds": eng.rounds,
                "rng_td": eng.rng_td,
                "rng_step": eng.rng_step,
                "last_round_ns": self.last_round_ns,
            },
            "feed_a": self.feed_a.state(),
            "feed_b": self.feed_b.state(),
            "broker": self.broker.state(),
        }


async def run_sim_exchange(
    cfg: FullConfig,
    layout: HomeLayout,
    clock_offset_ns: int = 0,
    stop: asyncio.Event | None = None,
    *,
    clock_version: int = 0,
) -> None:
    """Run sim-exchange until ``stop`` is set (or the task is cancelled).

    ``clock_offset_ns`` is the market-clock offset (market time − wall time) handed over
    by the supervisor at start-up (02 §9.5 交易时段); it must be 0 unless
    ``app.mode=demo``.
    """
    ex = SimExchange(cfg, layout, clock_offset_ns, clock_version=clock_version)
    await ex.start()
    try:
        if stop is None:
            await asyncio.Event().wait()
        else:
            await stop.wait()
    finally:
        await ex.stop()
