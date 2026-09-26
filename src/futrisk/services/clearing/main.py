"""``futrisk svc clearing-worker`` entry point (01 §7.1, §5.3.6; ``futrisk.cli.svc_cmd``).

``run(home, instance, shard, parent_pid)`` starts the asyncio runtime on a
selector loop (``zmq.asyncio`` on Windows), the :class:`ClearingService`, and
returns the exit code when the service stops (``ctl.shutdown``, a signal, a
fatal error -> 1). With ``parent_pid`` the runtime exits when the supervisor
dies (it polls the parent process every ~2 s).

The process pool uses the ``spawn`` start method; frozen executables must call
``multiprocessing.freeze_support()`` in their entry point.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

from loguru import logger

from futrisk.common.clock import MarketClock
from futrisk.common.config import FullConfig
from futrisk.runtime import AsyncServiceRuntime, run_async
from futrisk.services.clearing.service import ClearingService
from futrisk.services.clearing.settings import ClearingSettings

INSTANCE = "clearing-worker"


async def serve(
    *,
    home: str | Path | None = None,
    instance: str = INSTANCE,
    parent_pid: int | None = None,
    config: FullConfig | None = None,
    market_clock: MarketClock | None = None,
    setup_logs: bool = True,
    settings: ClearingSettings | None = None,
) -> int:
    """Run the clearing-worker until it is asked to stop; returns the exit code."""
    rt = AsyncServiceRuntime(
        instance,
        home=home,
        config=config,
        market_clock=market_clock,
        setup_logs=setup_logs,
        parent_pid=parent_pid,
    )
    await rt.start()
    try:
        if threading.current_thread() is threading.main_thread():
            rt.install_signal_handlers()
        svc = ClearingService(rt, settings)
        await svc.start()
        return await rt.run_until_stopped()
    except BaseException:
        logger.exception("clearing.failed")
        rt.exit_code = rt.exit_code or 1
        raise
    finally:
        await rt.stop()


def run(
    home: str | None = None,
    instance: str | None = None,
    shard: int | None = None,
    parent_pid: int | None = None,
    **_: Any,
) -> int:
    """Service entry used by ``futrisk svc clearing-worker`` (``shard`` is ignored: one instance)."""
    try:
        return run_async(serve(home=home, instance=instance or INSTANCE, parent_pid=parent_pid))
    except KeyboardInterrupt:
        return 0
    except Exception:
        return 1


__all__ = ["INSTANCE", "run", "serve"]
