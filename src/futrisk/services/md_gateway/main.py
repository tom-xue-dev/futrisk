"""``futrisk svc md-gateway`` entry point (01 §7.1, §5.3.6; ``futrisk.cli.svc_cmd``).

``run(home=None, instance=None, shard=None, parent_pid=None, **_) -> int`` builds the
runtime (config from ``<home>/config/active.json``, market clock following ``sys.clock``),
starts :class:`MdGatewayService`, blocks until ``ctl.shutdown`` / Ctrl-C / CTRL_BREAK and
exits when the supervisor (``parent_pid``) dies, so no orphan keeps ports or files busy.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from loguru import logger

from futrisk.runtime import AsyncServiceRuntime, run_async
from futrisk.runtime.parent import ParentWatch
from futrisk.services.md_gateway.service import MdGatewayService

PARENT_POLL_S = 2.0


async def watch_parent(rt: AsyncServiceRuntime, parent_pid: int, interval_s: float = PARENT_POLL_S) -> None:
    """Stop the service once the parent process has exited (polled every ~2 s)."""
    watch = ParentWatch(parent_pid)
    while True:
        if not watch.alive():
            logger.warning("md.parent_exited", parent_pid=parent_pid)
            rt.request_stop(f"parent {parent_pid} exited")
            return
        await asyncio.sleep(interval_s)


async def main_async(
    *,
    home: str | None = None,
    instance: str | None = None,
    parent_pid: int | None = None,
    runtime_kwargs: dict[str, Any] | None = None,
    service_kwargs: dict[str, Any] | None = None,
    signals: bool = True,
    parent_poll_s: float = PARENT_POLL_S,
) -> int:
    """Run md-gateway until stopped; returns the process exit code."""
    rt = AsyncServiceRuntime(instance or "md-gateway", home=home, **(runtime_kwargs or {}))
    await rt.start()
    watcher: asyncio.Task[None] | None = None
    try:
        if signals:
            with contextlib.suppress(ValueError):  # signal handlers only on the main thread
                rt.install_signal_handlers()
        if parent_pid:
            # before the (possibly long) start-up: an orphan must not wait for sys.clock forever
            watcher = asyncio.create_task(watch_parent(rt, parent_pid, parent_poll_s), name="md-parent-watch")
        service = MdGatewayService(rt, **(service_kwargs or {}))
        await service.start()
        return await rt.run_until_stopped()
    except BaseException as exc:
        if not isinstance(exc, asyncio.CancelledError):
            logger.opt(exception=exc).critical("md.start_failed")
            rt.exit_code = 1
        await rt.stop()
        return rt.exit_code or 1
    finally:
        if watcher is not None:
            watcher.cancel()


def run(
    home: str | None = None,
    instance: str | None = None,
    shard: int | None = None,
    parent_pid: int | None = None,
    **_: Any,
) -> int:
    """Blocking entry point used by ``futrisk svc md-gateway``."""
    return run_async(main_async(home=home, instance=instance, parent_pid=parent_pid))


__all__ = ["main_async", "run", "watch_parent"]
