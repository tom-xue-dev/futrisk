"""``python -m futrisk.sim.exchange``: run the virtual exchange by hand.

Examples::

    python -m futrisk.sim.exchange --home var
    python -m futrisk.sim.exchange --home var --clock-offset-ns 123456789000
    python -m futrisk.sim.exchange --home var --market-time 2026-09-28T09:30:00

Configuration: ``--config DIR`` loads a config directory directly; otherwise the
committed version of ``<home>/config/active.json`` is used, and on a fresh home the
bundled defaults are copied first (``ensure_home_config``). ``--market-time`` is a demo
convenience that computes the offset from a Beijing time.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import sys
from datetime import datetime

from futrisk.common.clock import BJ, SystemClock, bj_to_ns
from futrisk.common.config import (
    ConfigError,
    FullConfig,
    ensure_home_config,
    load_active_config,
    load_config_dir,
)
from futrisk.common.logging import setup_logging
from futrisk.common.paths import HomeLayout, resolve_home
from futrisk.sim.exchange.app import run_sim_exchange


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m futrisk.sim.exchange", description="futrisk virtual exchange")
    p.add_argument("--home", default=None, help="runtime home directory (default: FUTRISK_HOME or <repo>/var)")
    p.add_argument("--config", default=None, help="load this config directory instead of <home>/config/active.json")
    p.add_argument("--clock-offset-ns", type=int, default=0, help="market time minus wall time, ns (demo mode)")
    p.add_argument("--clock-version", type=int, default=0, help="version of that clock state (sys.clock)")
    p.add_argument("--market-time", default=None,
                   help="demo shortcut: start with market time = this Beijing time (ISO, e.g. 2026-09-28T09:30:00)")
    p.add_argument("--log-level", default=None, help="log level (default: app.yaml logging.level)")
    p.add_argument("--no-file-log", action="store_true", help="log to the console only")
    return p.parse_args(argv)


def _load_config(args: argparse.Namespace, layout: HomeLayout) -> FullConfig:
    if args.config:
        return load_config_dir(args.config)
    if not layout.active_json.exists():
        ensure_home_config(layout)
    return load_active_config(layout)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    layout = HomeLayout(resolve_home(args.home)).ensure()
    try:
        cfg = _load_config(args, layout)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    offset = args.clock_offset_ns
    if args.market_time:
        target = datetime.fromisoformat(args.market_time)
        if target.tzinfo is None:
            target = target.replace(tzinfo=BJ)
        offset = bj_to_ns(target) - SystemClock().now_ns()
    setup_logging(
        "sim-exchange",
        None if args.no_file_log else layout.logs_dir,
        level=args.log_level or cfg.app.logging.level,
        retention_days=cfg.app.logging.retention_days,
        rotation_mb=cfg.app.logging.rotation_mb,
    )

    async def runner() -> None:
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()

        def request_stop(*_: object) -> None:
            loop.call_soon_threadsafe(stop.set)

        for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
            sig = getattr(signal, name, None)
            if sig is not None:
                with contextlib.suppress(ValueError, OSError):
                    signal.signal(sig, request_stop)
        await run_sim_exchange(cfg, layout, offset, stop, clock_version=args.clock_version)

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(runner())
    return 0


if __name__ == "__main__":
    sys.exit(main())
