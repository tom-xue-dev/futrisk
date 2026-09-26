"""supervisor process entry point: ``futrisk up`` / ``futrisk svc supervisor`` (01 §7.1, §6.8).

:func:`run` is the ``futrisk.cli.svc_cmd`` convention (``run(home, instance, shard,
parent_pid, **extras) -> exit code``). It runs :func:`supervise` on a selector event loop:

1. start the ``supervisor`` runtime (config, logs, PUB 7601 / ROUTER 7602, heartbeat);
   a second supervisor on the same home fails here (the endpoints are taken);
2. open ``supervisor.db`` (``synchronous=FULL``), start the ClockKeeper (``sys.clock``)
   and the process manager, which spawns every instance in ``startup_order``;
3. wait for a stop request: Ctrl-C / CTRL_BREAK / SIGTERM, ``ctl.shutdown``,
   ``svc.stop *`` (``futrisk down``) or the death of ``--parent-pid``;
4. stop the instances in reverse order, then the supervisor itself (exit code 0).

Clock-only mode (``clock_only``; ``futrisk up --clock-only``, or ``futrisk svc supervisor`` with
``FUTRISK_SUPERVISOR_CLOCK_ONLY=1``): step 2 spawns nothing - the supervisor only keeps and
publishes the market clock (``sys.clock``, ``svc.clock``) for services that run elsewhere, e.g.
one container per service under docker compose. See :mod:`.supervisor`.
"""

from __future__ import annotations

import contextlib
import os
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from loguru import logger

from futrisk.common.config import FullConfig
from futrisk.messages.enums import ServiceChangeReason
from futrisk.runtime import SUPERVISOR, AsyncServiceRuntime, run_async
from futrisk.services.supervisor.process import ArgvFactory
from futrisk.services.supervisor.schema import DB_NAME, METADATA, MIGRATIONS
from futrisk.services.supervisor.supervisor import Supervisor

CLOCK_ONLY_ENV = "FUTRISK_SUPERVISOR_CLOCK_ONLY"


def clock_only_from_env() -> bool:
    """``FUTRISK_SUPERVISOR_CLOCK_ONLY`` is set to 1 / true / yes / on."""
    return os.environ.get(CLOCK_ONLY_ENV, "").strip().lower() in ("1", "true", "yes", "on")


async def supervise(
    *,
    home: str | Path | None = None,
    config: FullConfig | None = None,
    parent_pid: int | None = None,
    log_console: bool = True,
    setup_logs: bool = True,
    start_at: str | None = None,
    argv_factory: ArgvFactory | None = None,
    extra_env: Mapping[str, str] | None = None,
    instance_env: Mapping[str, Mapping[str, str]] | None = None,
    install_signals: bool = True,
    clock_only: bool = False,
) -> int:
    """Run the supervisor until it is asked to stop; returns the process exit code.

    ``instance_env`` holds per-instance environment overrides (``futrisk up --svc-env``),
    e.g. ``{"strategy-sim": {"FUTRISK_BOTS_MODE": "scenario"}}``; they apply to every
    (re)spawn of that instance. ``clock_only``: keep the market clock, manage no process.
    """
    rt = AsyncServiceRuntime(
        SUPERVISOR,
        home=home,
        config=config,
        setup_logs=setup_logs,
        log_console=log_console,
        parent_pid=parent_pid,  # futrisk demo: stop everything when the demo CLI goes away
    )
    try:
        await rt.start()
    except Exception as exc:
        logger.opt(exception=exc).critical(
            "supervisor.start_failed", detail="is another supervisor running on the same endpoints?"
        )
        return 1
    sup: Supervisor | None = None
    try:
        if install_signals:
            with contextlib.suppress(ValueError):  # not the main thread
                rt.install_signal_handlers()
        db = await rt.open_database(DB_NAME, METADATA, migrations=MIGRATIONS, synchronous="FULL")
        sup = Supervisor(
            rt,
            db,
            argv_factory=argv_factory,
            extra_env=extra_env,
            instance_env=instance_env,
            start_at_override=start_at,
            clock_only=clock_only,
        )
        await sup.start()
        await rt.wait_stopped()
        logger.info("supervisor.stopping", reason=rt.stop_reason)
        await sup.shutdown(ServiceChangeReason.SHUTDOWN, sup.shutdown_actor)
    except Exception as exc:
        rt.fatal(exc, "supervisor")
        if sup is not None and not sup.shutting_down:
            with contextlib.suppress(Exception):
                await sup.shutdown(ServiceChangeReason.SHUTDOWN, None)
    finally:
        await rt.stop()
    return rt.exit_code


def run(
    home: str | None = None,
    instance: str | None = None,
    shard: int | None = None,
    parent_pid: int | None = None,
    *,
    log_console: bool = True,
    start_at: str | None = None,
    instance_env: Mapping[str, Mapping[str, str]] | None = None,
    clock_only: bool | None = None,
    **_: Any,
) -> int:
    """``futrisk svc supervisor`` / ``futrisk up``: run the supervisor in the foreground.

    ``clock_only`` None: from ``FUTRISK_SUPERVISOR_CLOCK_ONLY`` (``futrisk svc supervisor`` has
    no flag for it).
    """
    if instance not in (None, SUPERVISOR):
        print(f"supervisor: ignoring --instance {instance!r}", file=sys.stderr)
    try:
        return run_async(
            supervise(
                home=home,
                parent_pid=parent_pid,
                log_console=log_console,
                start_at=start_at,
                instance_env=instance_env,
                clock_only=clock_only_from_env() if clock_only is None else clock_only,
            )
        )
    except KeyboardInterrupt:  # before the signal handlers were installed
        return 130


__all__ = ["CLOCK_ONLY_ENV", "clock_only_from_env", "run", "supervise"]
