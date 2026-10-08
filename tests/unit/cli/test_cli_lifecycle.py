"""Process-lifecycle guards: keep-awake, single-instance lock, console-close flush, stop signals."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from pathlib import Path

import pytest

from futrisk.cli.lifecycle import (
    CTRL_BREAK_EVENT,
    CTRL_C_EVENT,
    CTRL_CLOSE_EVENT,
    CTRL_LOGOFF_EVENT,
    CTRL_SHUTDOWN_EVENT,
    ConsoleCloseGuard,
    InstanceLock,
    LockHeldError,
    install_stop_signals,
    keep_awake,
    stop_signals,
)

windows_only = pytest.mark.skipif(sys.platform != "win32", reason="Windows-only API")


class TestKeepAwake:
    @windows_only
    def test_request_is_set_and_cleared(self) -> None:
        with keep_awake(True) as active:
            assert active is True

    def test_disabled_is_a_no_op(self) -> None:
        with keep_awake(False) as active:
            assert active is False


class TestInstanceLock:
    def test_second_instance_is_refused_with_owner_info(self, tmp_path: Path) -> None:
        path = tmp_path / "ticks" / ".recorder.lock"
        first = InstanceLock(path).acquire()
        try:
            assert first.held and first.owner_path.exists()
            with pytest.raises(LockHeldError) as err:
                InstanceLock(path).acquire()
            assert f"pid={os.getpid()}" in str(err.value) and str(path) in str(err.value)
        finally:
            first.release()
        assert not first.held and not first.owner_path.exists()
        with InstanceLock(path) as again:  # released: the next recorder may start
            assert again.held

    def test_release_is_idempotent(self, tmp_path: Path) -> None:
        lock = InstanceLock(tmp_path / "x.lock").acquire()
        lock.release()
        lock.release()
        assert lock.acquire().held
        lock.release()


class TestConsoleCloseGuard:
    @pytest.mark.parametrize("event", [CTRL_CLOSE_EVENT, CTRL_LOGOFF_EVENT, CTRL_SHUTDOWN_EVENT])
    def test_close_events_flush(self, event: int) -> None:
        flushed: list[bool] = []
        guard = ConsoleCloseGuard(lambda: flushed.append(True))
        assert guard.handle(event) is True and flushed == [True] and guard.calls == [event]

    @pytest.mark.parametrize("event", [CTRL_C_EVENT, CTRL_BREAK_EVENT])
    def test_ctrl_c_and_break_are_left_to_python(self, event: int) -> None:
        flushed: list[bool] = []
        assert ConsoleCloseGuard(lambda: flushed.append(True)).handle(event) is False and flushed == []

    def test_flush_errors_are_contained(self) -> None:
        def broken() -> None:
            raise OSError("disk gone")

        assert ConsoleCloseGuard(broken).handle(CTRL_CLOSE_EVENT) is True

    @windows_only
    def test_install_and_uninstall(self) -> None:
        guard = ConsoleCloseGuard(lambda: None)
        try:
            assert guard.install() is True
            assert guard.install() is False  # already installed
        finally:
            guard.uninstall()
        guard.uninstall()


class TestStopSignals:
    async def test_first_signal_requests_stop_second_interrupts(self) -> None:
        sig = stop_signals()[-1]  # SIGBREAK on Windows, SIGTERM elsewhere
        before = signal.getsignal(sig)
        stop = asyncio.Event()
        restore = install_stop_signals(asyncio.get_running_loop(), stop.set)
        try:
            signal.raise_signal(sig)
            await asyncio.wait_for(stop.wait(), 5)
            with pytest.raises(KeyboardInterrupt):
                signal.raise_signal(sig)
        finally:
            restore()
        assert signal.getsignal(sig) == before
