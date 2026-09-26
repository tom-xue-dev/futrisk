"""Entry point of ``futrisk svc trade-gateway`` (01 §7.1, ``futrisk.cli.svc_cmd``).

``run()`` builds the sync runtime of the instance (default ``trade-gateway``), starts the
service loop on the calling (main) thread and returns the process exit code: 0 after
a graceful stop (``ctl.shutdown``, Ctrl-C / CTRL_BREAK, parent exit), 1 after a fatal
error (the supervisor then restarts the service, 01 §8.6). ``parent_pid`` is handed to
the runtime, which stops the service when that process disappears.
"""

from __future__ import annotations

import threading
from typing import Any

from futrisk.runtime import SyncServiceRuntime
from futrisk.services.trade_gateway.service import TradeGatewayService

INSTANCE = "trade-gateway"


def run(
    home: str | None = None,
    instance: str | None = None,
    shard: int | None = None,
    parent_pid: int | None = None,
    **_: Any,
) -> int:
    """Run trade-gateway in the foreground until it stops; returns the exit code.

    Args:
        home: runtime home directory (default: ``resolve_home()``).
        instance: instance id (default ``trade-gateway``).
        shard: unused (trade-gateway is not sharded).
        parent_pid: exit when this process dies (watched by the runtime).
    """
    rt = SyncServiceRuntime(instance or INSTANCE, home=home, parent_pid=parent_pid)
    svc = TradeGatewayService(rt)
    return svc.run(install_signals=threading.current_thread() is threading.main_thread())


__all__ = ["INSTANCE", "run"]
