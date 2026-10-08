"""Process-lifecycle guards for long-running recorders (Windows first; 01 §7.1, 03 §5.1).

A recording run spans three trading days, so the process must survive what a
desktop does to long-running console programs:

- :func:`keep_awake` asks Windows not to idle-sleep
  (``SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED)``) and clears
  the request on exit. It cannot stop a lid close, the power button, battery
  saver or a forced update restart -- those stay the user's responsibility;
- :class:`InstanceLock` makes a second recorder on the same home exit with a
  clear message instead of writing interleaved shards. The OS releases the
  lock when the process dies, so a crash never leaves a stale lock;
- :class:`ConsoleCloseGuard` flushes buffered rows when the console window is
  closed or the user logs off / shuts down (``SetConsoleCtrlHandler``;
  Windows kills the process about 5 s after ``CTRL_CLOSE_EVENT``);
- :func:`install_stop_signals` turns the first Ctrl-C / Ctrl-Break
  (``SIGINT`` / ``SIGBREAK``; ``SIGTERM`` on POSIX) into a graceful stop
  request; a second one raises ``KeyboardInterrupt``.

A hard kill (``taskkill /F``, power loss) runs no handler at all; the
recorder's 5 s shard roll bounds that loss.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import os
import signal
import socket
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from types import FrameType
from typing import Any, Final

from loguru import logger

from futrisk.common.clock import SystemClock, ns_to_bj

ES_CONTINUOUS: Final = 0x80000000
ES_SYSTEM_REQUIRED: Final = 0x00000001

CTRL_C_EVENT: Final = 0
CTRL_BREAK_EVENT: Final = 1
CTRL_CLOSE_EVENT: Final = 2
CTRL_LOGOFF_EVENT: Final = 5
CTRL_SHUTDOWN_EVENT: Final = 6
FLUSH_EVENTS: Final = frozenset({CTRL_CLOSE_EVENT, CTRL_LOGOFF_EVENT, CTRL_SHUTDOWN_EVENT})

IS_WINDOWS: Final = sys.platform == "win32"


def _kernel32() -> Any:
    if sys.platform != "win32":
        raise OSError("kernel32 is only available on Windows")
    return ctypes.WinDLL("kernel32", use_last_error=True)


# ---------------------------------------------------------------------------
# Keep awake
# ---------------------------------------------------------------------------


@contextlib.contextmanager
def keep_awake(enabled: bool = True) -> Iterator[bool]:
    """Prevent idle sleep while the block runs; yields whether the request is active.

    Must be entered on the thread that stays alive for the whole run (the main
    thread): the execution state belongs to the calling thread.
    """
    active = False
    kernel32: Any = None
    if enabled and IS_WINDOWS:
        try:
            kernel32 = _kernel32()
            kernel32.SetThreadExecutionState.argtypes = [ctypes.c_uint32]
            kernel32.SetThreadExecutionState.restype = ctypes.c_uint32
            active = kernel32.SetThreadExecutionState(ES_CONTINUOUS | ES_SYSTEM_REQUIRED) != 0
        except (OSError, AttributeError) as exc:
            logger.warning(f"lifecycle.keep_awake unavailable: {exc!r}")
        if active:
            logger.warning(
                "lifecycle.keep_awake on: Windows will not idle-sleep during this run. Closing the lid, the power "
                "button, battery saver and update restarts can still suspend or reboot the machine: stay on AC "
                "power, set 'When I close the lid' to 'Do nothing' and pause Windows Update."
            )
    elif enabled:
        logger.info(
            "lifecycle.keep_awake is only implemented on Windows; make sure the machine does not sleep"
        )
    try:
        yield active
    finally:
        if active and kernel32 is not None:
            kernel32.SetThreadExecutionState(ES_CONTINUOUS)
            logger.info("lifecycle.keep_awake off")


# ---------------------------------------------------------------------------
# Single-instance lock
# ---------------------------------------------------------------------------


class LockHeldError(RuntimeError):
    """Another process holds the instance lock."""


def _try_lock(fd: int) -> None:
    if sys.platform == "win32":
        import msvcrt

        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)  # raises OSError when another handle holds byte 0
    else:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)


def _unlock(fd: int) -> None:
    if sys.platform == "win32":
        import msvcrt

        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_UN)


class InstanceLock:
    """Exclusive, crash-safe lock file (``msvcrt.locking`` / ``fcntl.flock``).

    The lock lives on byte 0 of ``path``; the owner's pid, host and start time
    go to a readable sidecar ``<path>.owner`` so that a refused second instance
    can say who holds the lock.
    """

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.owner_path = self.path.with_name(self.path.name + ".owner")
        self._fd: int | None = None

    @property
    def held(self) -> bool:
        return self._fd is not None

    def owner_info(self) -> str:
        try:
            return self.owner_path.read_text(encoding="utf-8").strip() or "unknown owner"
        except OSError:
            return "unknown owner"

    def acquire(self, what: str = "recorder") -> InstanceLock:
        if self._fd is not None:
            return self
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            _try_lock(fd)
        except OSError as exc:
            os.close(fd)
            raise LockHeldError(
                f"another {what} is already running on this home ({self.owner_info()}); lock file: {self.path}"
            ) from exc
        self._fd = fd
        started = ns_to_bj(SystemClock().now_ns()).isoformat(timespec="seconds")
        with contextlib.suppress(OSError):
            self.owner_path.write_text(
                f"pid={os.getpid()} host={socket.gethostname()} started={started}\n", encoding="utf-8"
            )
        return self

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        with contextlib.suppress(OSError):
            self.owner_path.unlink()
        with contextlib.suppress(OSError):
            _unlock(fd)
        os.close(fd)

    def __enter__(self) -> InstanceLock:
        return self.acquire()

    def __exit__(self, *exc: object) -> None:
        self.release()


# ---------------------------------------------------------------------------
# Console close / logoff / shutdown
# ---------------------------------------------------------------------------


class ConsoleCloseGuard:
    """Runs ``flush()`` when the console is closed or the session ends (Windows).

    The system calls the handler on a new thread; for ``CTRL_CLOSE_EVENT`` the
    process is terminated when the handler returns or after about 5 s, so
    ``flush`` must be quick and thread-safe. Ctrl-C and Ctrl-Break are passed
    on (``False``) to Python's own signal handlers.
    """

    def __init__(self, flush: Callable[[], object]) -> None:
        self._flush = flush
        self._callback: Any = None
        self.calls: list[int] = []

    def handle(self, event: int) -> bool:
        """The handler logic (callable directly in tests)."""
        if event not in FLUSH_EVENTS:
            return False
        self.calls.append(event)
        logger.warning(
            f"lifecycle.console_event {event}: flushing buffered rows before Windows ends the process"
        )
        try:
            self._flush()
        except Exception:
            logger.opt(exception=True).critical("lifecycle.console_flush_failed")
        return True

    def install(self) -> bool:
        if sys.platform != "win32" or self._callback is not None:
            return False
        from ctypes import wintypes

        handler_type = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.DWORD)

        def _c_handler(event: int) -> bool:
            return self.handle(int(event))

        callback = handler_type(_c_handler)
        kernel32 = _kernel32()
        kernel32.SetConsoleCtrlHandler.argtypes = [handler_type, wintypes.BOOL]
        kernel32.SetConsoleCtrlHandler.restype = wintypes.BOOL
        if not kernel32.SetConsoleCtrlHandler(callback, True):
            logger.warning(f"lifecycle.console_guard not installed: error {ctypes.get_last_error()}")
            return False
        self._callback = callback  # keep a reference: the OS holds only a raw pointer
        return True

    def uninstall(self) -> None:
        if self._callback is None:
            return
        with contextlib.suppress(OSError, AttributeError):
            _kernel32().SetConsoleCtrlHandler(self._callback, False)
        self._callback = None


# ---------------------------------------------------------------------------
# Graceful stop on Ctrl-C / Ctrl-Break / SIGTERM
# ---------------------------------------------------------------------------


def stop_signals() -> list[signal.Signals]:
    """Signals that request a graceful stop on this platform."""
    sigs = [signal.SIGINT]
    if sys.platform == "win32":
        sigs.append(signal.SIGBREAK)
    else:
        sigs.append(signal.SIGTERM)
    return sigs


def install_stop_signals(
    loop: asyncio.AbstractEventLoop, on_stop: Callable[[], object]
) -> Callable[[], None]:
    """First signal -> ``on_stop()`` on ``loop``; a second one raises ``KeyboardInterrupt``.

    Must be called from the main thread. Returns a function restoring the previous handlers.
    """
    previous: dict[signal.Signals, Any] = {}
    fired: list[int] = []

    def handler(signum: int, frame: FrameType | None) -> None:
        if fired:
            raise KeyboardInterrupt
        fired.append(signum)
        logger.warning(f"lifecycle.stop_requested signal={signal.Signals(signum).name}; flushing and exiting")
        loop.call_soon_threadsafe(on_stop)

    for sig in stop_signals():
        with contextlib.suppress(ValueError, OSError):
            previous[sig] = signal.signal(sig, handler)

    def restore() -> None:
        for sig, old in previous.items():
            with contextlib.suppress(ValueError, OSError, TypeError):
                signal.signal(sig, old)

    return restore
