"""Parent-process watchdog (01 §5.3.6).

Children spawned by the supervisor receive ``--parent-pid``; the runtime
(``parent_pid=...``) polls the parent every ~2 s and stops the service when it
is gone, so no orphan keeps ports and database files busy.

The ``psutil.Process`` of the parent is captured once at start-up and polled
with ``is_running()``: psutil compares the process identity it captured then
(pid + start time, taken from a clock that wall-clock steps do not move: ticks
since boot on Linux, the creation FILETIME on Windows), so a recycled pid is
not mistaken for the parent and a stepped wall clock (Docker Desktop, NTP) is
not mistaken for a dead parent. Comparing ``create_time()`` values ourselves
was wrong on Linux, where that value is derived from the boot time and moves
when the clock is stepped.
"""

from __future__ import annotations

import psutil


class ParentWatch:
    """``alive()`` is False once the process ``pid`` (as seen at construction) has exited."""

    def __init__(self, pid: int) -> None:
        self.pid = pid
        try:
            self._proc: psutil.Process | None = psutil.Process(pid)
        except psutil.Error:  # already gone
            self._proc = None

    def alive(self) -> bool:
        proc = self._proc
        if proc is None:
            return False
        try:
            if not proc.is_running():  # exited, or its pid now belongs to another process
                return False
            return bool(proc.status() != psutil.STATUS_ZOMBIE)
        except psutil.NoSuchProcess:
            return False
        except psutil.AccessDenied:
            return True  # is_running() said so; the status is just not inspectable
        except psutil.Error:
            return False


__all__ = ["ParentWatch"]
