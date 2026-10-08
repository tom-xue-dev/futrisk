"""``futrisk svc md-gateway`` entry point (01 §7.1, §5.3.6; ``futrisk.cli.svc_cmd``).

``run(home, instance, shard, parent_pid)`` builds the runtime (config from
``<home>/config/active.json``, market clock following ``sys.clock``), starts
:class:`MdGatewayService` and blocks until ``ctl.shutdown`` / Ctrl-C / CTRL_BREAK; with
``parent_pid`` the runtime exits when the supervisor dies (polled every ~2 s, also while
waiting for ``sys.clock``), so no orphan keeps ports or files busy.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from loguru import logger

from futrisk.runtime import AsyncServiceRuntime, run_async
from futrisk.services.md_gateway.service import MdGatewayService

PARENT_POLL_S = 2.0


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
    rt = AsyncServiceRuntime(
        instance or "md-gateway", home=home, parent_pid=parent_pid, parent_poll_s=parent_poll_s, **(runtime_kwargs or {})
    )
    await rt.start()
    try:
        if signals:
            with contextlib.suppress(ValueError):  # signal handlers only on the main thread
                rt.install_signal_handlers()
        service = MdGatewayService(rt, **(service_kwargs or {}))
        await service.start()
        return await rt.run_until_stopped()
    except BaseException as exc:
        if not isinstance(exc, asyncio.CancelledError):
            logger.opt(exception=exc).critical("md.start_failed")
            rt.exit_code = 1
        await rt.stop()
        return rt.exit_code or 1


def run(home: str | None = None, instance: str | None = None, shard: int | None = None, parent_pid: int | None = None, **_: Any) -> int:
    """Blocking entry point used by ``futrisk svc md-gateway``."""
    return run_async(main_async(home=home, instance=instance, parent_pid=parent_pid))


__all__ = ["main_async", "run"]
