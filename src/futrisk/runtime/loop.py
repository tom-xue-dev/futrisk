"""Event-loop helpers for asyncio services (01 §8.12 Windows notes).

``zmq.asyncio`` needs ``add_reader`` support, which the Windows default
``ProactorEventLoop`` lacks, so asyncio services must run on a
``SelectorEventLoop``::

    from futrisk.runtime import AsyncServiceRuntime, run_async

    async def main() -> int:
        async with AsyncServiceRuntime("clearing-worker") as rt:
            rt.install_signal_handlers()
            ...
            return await rt.run_until_stopped()

    raise SystemExit(run_async(main()))

Note: a selector loop on Windows cannot spawn asyncio subprocesses; the
supervisor manages children with ``subprocess.Popen`` + polling instead.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Callable, Coroutine
from typing import Any, TypeVar

T = TypeVar("T")


def selector_loop_factory() -> Callable[[], asyncio.AbstractEventLoop]:
    """Loop factory giving a selector event loop on every platform."""
    return asyncio.SelectorEventLoop


def run_async(main: Coroutine[Any, Any, T], *, debug: bool | None = None) -> T:
    """``asyncio.run(main)`` on a selector event loop (required by ``zmq.asyncio`` on Windows)."""
    return asyncio.run(main, debug=debug, loop_factory=selector_loop_factory())


def ensure_selector_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Raise a clear error when running on a Proactor loop on Windows."""
    proactor = getattr(asyncio, "ProactorEventLoop", None)
    if sys.platform == "win32" and proactor is not None and isinstance(loop, proactor):
        raise RuntimeError(
            "zmq.asyncio needs a selector event loop on Windows: start the service with "
            "futrisk.runtime.run_async(main()) or asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)"
        )


__all__ = ["ensure_selector_loop", "run_async", "selector_loop_factory"]
