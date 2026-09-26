"""``futrisk svc sim-exchange``: the virtual exchange as a supervised process (01 §7.1; 02 §9.5).

Wraps :func:`futrisk.sim.exchange.run_sim_exchange` with the ``svc_cmd`` conventions:

* the committed config version (``<home>/config/active.json``; a fresh home gets the
  bundled defaults first), like every other service;
* logging for instance ``sim-exchange`` (``<home>/logs/sim-exchange_YYYYMMDD.log``, JSON
  lines, plus the console, which the supervisor captures in ``sim-exchange.console.log``);
* the market clock (demo mode):

  - spawned by the supervisor: ``--clock-offset-ns`` (market - wall) and its version from
    ``FUTRISK_CLOCK_VERSION``;
  - started on its own (one container per service, a manual start) while a ``supervisor``
    instance is configured: the supervisor's current ``sys.clock`` (``ctl.snapshot`` of its
    LV cache), waiting up to ``FUTRISK_SIM_CLOCK_WAIT_S`` (60 s) for it to answer; exit
    code 1 when it does not, so a restart policy tries again instead of running on a clock
    the services do not share. Later jumps reach sim-exchange through api-gateway
    (``POST /control/clock``, 02 §8.8);
  - no supervisor configured: the wall clock (offset 0);

* graceful stop on Ctrl-C / CTRL_BREAK / SIGTERM, and when ``--parent-pid`` dies.

Exit codes: 0 after a graceful stop, 1 when the exchange failed (or got no market clock), 2 on
a config error.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from loguru import logger

from futrisk.common.config import ConfigError, ensure_home_config, load_active_config
from futrisk.common.logging import setup_logging
from futrisk.common.paths import HomeLayout, resolve_home
from futrisk.services.supervisor.parent import ParentWatcher
from futrisk.services.supervisor.process import CLOCK_VERSION_ENV, SIM_EXCHANGE, SUPERVISOR

if TYPE_CHECKING:
    from futrisk.common.config import FullConfig
    from futrisk.messages.system import ClockState

CLOCK_WAIT_ENV = "FUTRISK_SIM_CLOCK_WAIT_S"
DEFAULT_CLOCK_WAIT_S = 60.0
CLOCK_POLL_S = 0.5
CLOCK_NOTE_S = 5.0


def _env_int(name: str, default: int = 0) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


@dataclass(frozen=True)
class StartClock:
    """The market clock sim-exchange starts with, and where it came from."""

    offset_ns: int
    version: int
    source: str


def fetch_supervisor_clock(cfg: FullConfig, *, timeout_s: float, poll_s: float = CLOCK_POLL_S) -> ClockState | None:
    """The supervisor's current ``sys.clock`` (``ctl.snapshot`` of its LV cache, identity
    ``sim-exchange``); ``None`` when it does not answer with one within ``timeout_s``."""
    from futrisk.bus.endpoints import Endpoints
    from futrisk.bus.rpc_client import BlockingRpcClient
    from futrisk.messages import decode, decode_envelope
    from futrisk.messages.common import Ping, Pong, SnapshotBatch, SnapshotRequest
    from futrisk.messages.system import ClockState
    from futrisk.messages.topics import SYS_CLOCK

    start = time.monotonic()
    deadline = start + max(0.0, timeout_s)
    next_note = start + CLOCK_NOTE_S
    client = BlockingRpcClient(Endpoints(cfg.services), identity=SIM_EXCHANGE)
    try:
        while True:
            reply = client.request(SUPERVISOR, "ctl.snapshot", SnapshotRequest(prefixes=[SYS_CLOCK]), timeout_ms=1_000, retries=0)
            batch = reply.open_as(SnapshotBatch) if reply.ok else None
            for item in batch.items if batch is not None else []:
                env = decode_envelope(item.envelope)
                if item.topic == SYS_CLOCK and env.type == "ClockState":
                    state = decode(env.payload, ClockState)
                    pong = client.request(SUPERVISOR, "ctl.ping", Ping(), timeout_ms=1_000, retries=0)
                    who = pong.open_as(Pong) if pong.ok else None
                    logger.info(
                        "sim-exchange.clock_from_supervisor",
                        offset_ns=state.offset_ns,
                        version=state.version,
                        mode=str(state.mode),
                        supervisor_pid=who.pid if who is not None else None,
                        supervisor_home_id=who.home_id if who is not None else None,
                        waited_s=round(time.monotonic() - start, 1),
                    )
                    return state
            now = time.monotonic()
            if now + poll_s > deadline:
                return None
            if now >= next_note:
                next_note = now + CLOCK_NOTE_S
                logger.info(
                    "sim-exchange.waiting_for_clock",
                    supervisor=str(cfg.services.instance(SUPERVISOR).router),
                    last=reply.code,
                    left_s=round(deadline - now),
                )
            time.sleep(poll_s)
    finally:
        client.close()


def start_clock(cfg: FullConfig, clock_offset_ns: int | None, *, wait_s: float | None = None) -> StartClock | None:
    """The clock to start with; ``None`` when a configured supervisor gave none (see the module docstring)."""
    if clock_offset_ns is not None:
        return StartClock(int(clock_offset_ns), _env_int(CLOCK_VERSION_ENV, 0), "--clock-offset-ns")
    if cfg.app.mode != "demo":
        return StartClock(0, _env_int(CLOCK_VERSION_ENV, 0), "prod: the wall clock")
    sup = cfg.services.instances.get(SUPERVISOR)
    if sup is None or not sup.router:
        return StartClock(0, 0, "no supervisor configured: the wall clock")
    timeout_s = _env_float(CLOCK_WAIT_ENV, DEFAULT_CLOCK_WAIT_S) if wait_s is None else wait_s
    state = fetch_supervisor_clock(cfg, timeout_s=timeout_s)
    if state is None:
        return None
    return StartClock(int(state.offset_ns), int(state.version), "supervisor sys.clock")


async def _serve(cfg: Any, layout: HomeLayout, offset_ns: int, clock_version: int, parent_pid: int | None) -> int:
    from futrisk.sim.exchange.app import run_sim_exchange

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_stop(signum: int, _frame: object) -> None:
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(stop.set)
        logger.info("sim-exchange.signal", signal=signum)

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            with contextlib.suppress(ValueError, OSError):  # ValueError: not the main thread
                signal.signal(sig, request_stop)

    watcher: asyncio.Task[None] | None = None
    if parent_pid:
        parent = ParentWatcher(parent_pid)

        async def watch() -> None:
            await parent.wait_gone()
            logger.warning("sim-exchange.parent_gone", parent_pid=parent_pid)
            stop.set()

        watcher = asyncio.create_task(watch(), name="sim-exchange-parent-watch")
    try:
        await run_sim_exchange(cfg, layout, offset_ns, stop, clock_version=clock_version)
    except Exception:
        logger.exception("sim-exchange.failed")
        return 1
    finally:
        if watcher is not None:
            watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watcher
    logger.info("sim-exchange.exit", code=0)
    return 0


def run(
    home: str | None = None,
    instance: str | None = None,
    shard: int | None = None,
    parent_pid: int | None = None,
    clock_offset_ns: int | None = None,
    **_: Any,
) -> int:
    """Run sim-exchange in the foreground until stopped; returns the exit code."""
    layout = HomeLayout(resolve_home(home)).ensure()
    try:
        ensure_home_config(layout)
        cfg = load_active_config(layout)
    except ConfigError as exc:
        print(f"sim-exchange: configuration error: {exc}", file=sys.stderr)
        return 2
    log = cfg.app.logging
    setup_logging(
        SIM_EXCHANGE,
        layout.logs_dir,
        level=log.level,
        retention_days=log.retention_days,
        rotation_mb=log.rotation_mb,
    )
    try:
        clock = start_clock(cfg, clock_offset_ns)
    except KeyboardInterrupt:
        return 0
    if clock is None:
        wait = _env_float(CLOCK_WAIT_ENV, DEFAULT_CLOCK_WAIT_S)
        logger.error(
            "sim-exchange.no_market_clock",
            supervisor=str(cfg.services.instance(SUPERVISOR).router),
            detail=f"the supervisor gave no sys.clock within {wait:.0f}s ({CLOCK_WAIT_ENV}); start it first "
            "(e.g. futrisk up --clock-only) or pass --clock-offset-ns",
        )
        return 1
    offset = clock.offset_ns
    if offset and cfg.app.mode != "demo":
        logger.warning("sim-exchange.clock_offset_ignored", offset_ns=offset, reason="only allowed in demo mode")
        offset = 0
    logger.info(
        "sim-exchange.launch",
        home=str(layout.home),
        config_version=cfg.config_version,
        clock_offset_ns=offset,
        clock_version=clock.version,
        clock_source=clock.source,
        parent_pid=parent_pid,
    )
    try:
        return asyncio.run(_serve(cfg, layout, offset, clock.version, parent_pid))
    except KeyboardInterrupt:
        return 0


__all__ = ["CLOCK_WAIT_ENV", "StartClock", "fetch_supervisor_clock", "run", "start_clock"]
