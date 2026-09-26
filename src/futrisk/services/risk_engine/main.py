"""Entry point of ``futrisk svc risk-engine --shard N`` (01 §7.1, ``futrisk.cli.svc_cmd``).

The supervisor spawns ``[python, -m, futrisk.main, svc, risk-engine, --home, H, --parent-pid, P,
--shard, k]`` (no ``--instance``): the instance id is ``risk-engine-{shard}`` and the database
``<home>/db/risk-{shard}.db``. ``--instance`` alone also works (the shard is then taken from
``services.yaml`` or the name suffix). ``run()`` returns the process exit code: 0 after a
graceful stop (``ctl.shutdown``, Ctrl-C / CTRL_BREAK, parent exit), 1 after a fatal error (the
supervisor restarts the service, 01 §8.6).
"""

from __future__ import annotations

import threading
from typing import Any

from futrisk.runtime import SyncServiceRuntime
from futrisk.services.risk_engine.service import RiskEngineService


def resolve_instance(instance: str | None, shard: int | None) -> tuple[str, int]:
    """``(instance, shard)`` from the command line options (``--shard`` wins for the shard number)."""
    if instance is None and shard is None:
        raise SystemExit("risk-engine needs --shard N (or --instance risk-engine-N)")
    if instance is None:
        assert shard is not None
        return f"risk-engine-{shard}", int(shard)
    if shard is None:
        suffix = instance.rsplit("-", 1)[-1]
        if not suffix.isdigit():
            raise SystemExit(f"cannot derive the shard of {instance!r}; pass --shard")
        return instance, int(suffix)
    return instance, int(shard)


def run(
    home: str | None = None,
    instance: str | None = None,
    shard: int | None = None,
    parent_pid: int | None = None,
    **_: Any,
) -> int:
    """Run risk-engine-{shard} in the foreground until it stops; returns the exit code.

    Args:
        home: runtime home directory (default: ``resolve_home()``).
        instance: instance id (default ``risk-engine-{shard}``).
        shard: shard number (``accounts.yaml`` ``shard``); required unless ``instance`` names it.
        parent_pid: exit when this process dies (polled every ~2 s by the runtime).
    """
    inst, shard_no = resolve_instance(instance, shard)
    rt = SyncServiceRuntime(inst, home=home, parent_pid=parent_pid)
    svc = RiskEngineService(rt, shard=shard_no)
    return svc.run(install_signals=threading.current_thread() is threading.main_thread())


__all__ = ["resolve_instance", "run"]
