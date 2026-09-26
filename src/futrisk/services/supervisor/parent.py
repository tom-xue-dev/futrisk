"""Parent-process watch for ``--parent-pid`` (01 §5.3.6: children exit when the supervisor dies).

Services built on the runtimes get this from ``RuntimeBase(parent_pid=...)``; this
import-light variant serves processes that are not (``launch_sim``: sim-exchange).
``psutil.Process.is_running()`` compares the creation time, so a recycled pid is not
mistaken for the parent.
"""

from __future__ import annotations

import asyncio

import psutil

DEFAULT_INTERVAL_S = 1.0


class ParentWatcher:
    """Tells whether a given parent process is still alive."""

    def __init__(self, pid: int) -> None:
        self.pid = int(pid)
        try:
            proc: psutil.Process | None = psutil.Process(self.pid)
        except psutil.Error:
            proc = None
        self._proc = proc

    def alive(self) -> bool:
        proc = self._proc
        if proc is None:
            return False
        try:
            return bool(proc.is_running()) and proc.status() != psutil.STATUS_ZOMBIE
        except psutil.Error:
            return False

    async def wait_gone(self, interval_s: float = DEFAULT_INTERVAL_S) -> None:
        """Return once the parent process has exited."""
        while self.alive():
            await asyncio.sleep(interval_s)


__all__ = ["ParentWatcher"]
