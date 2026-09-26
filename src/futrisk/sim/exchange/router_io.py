"""ZeroMQ ROUTER socket of the SimBroker, owned by a dedicated I/O thread (02 §9.4).

The socket lives in its own thread with a blocking ``zmq.Poller`` (like the socket
threads of the internal services, 02 §8.1), so the broker works under any asyncio event
loop - including the Windows Proactor loop, which ``zmq.asyncio`` cannot use without
tornado. Inbound frames are handed to the loop with ``call_soon_threadsafe``; outbound
frames and control commands go through a thread-safe queue plus an inproc wake-up pair.

Socket options follow 02 §4.1 / §4.9: ``ROUTER_HANDOVER=1`` so a restarted
trade-gateway with the same identity takes the connection over, ``LINGER=0``.
"""

from __future__ import annotations

import asyncio
import contextlib
import queue
import threading
import time
from collections.abc import Callable
from typing import Any

import zmq
from loguru import logger

_HWM = 10_000


class RouterIO:
    """Thread-owned ROUTER socket. All public methods are called from the event-loop thread."""

    def __init__(self, endpoint: str, on_frame: Callable[[bytes, bytes], None], loop: asyncio.AbstractEventLoop) -> None:
        self._endpoint_cfg = endpoint
        self.endpoint = endpoint
        self._on_frame = on_frame
        self._loop = loop
        self._ctx = zmq.Context()
        self._cmds: queue.SimpleQueue[tuple[Any, ...]] = queue.SimpleQueue()
        self._wake_addr = f"inproc://sim-broker-wake-{id(self)}"
        self._router: zmq.Socket[bytes] | None = None
        self._wake_rx: zmq.Socket[bytes] | None = None
        self._wake_tx: zmq.Socket[bytes] | None = None
        self._thread: threading.Thread | None = None
        self._bound = False

    # ------------------------------------------------------------------ loop-thread API
    def start(self) -> str:
        """Bind (errors surface here) and start the I/O thread; returns the actual endpoint."""
        self._router = self._new_router(self._endpoint_cfg)
        self.endpoint = self._router.getsockopt_string(zmq.LAST_ENDPOINT)
        self._bound = True
        self._wake_rx = self._ctx.socket(zmq.PAIR)
        self._wake_rx.bind(self._wake_addr)
        self._wake_tx = self._ctx.socket(zmq.PAIR)
        self._wake_tx.setsockopt(zmq.LINGER, 0)
        self._wake_tx.connect(self._wake_addr)
        self._thread = threading.Thread(target=self._run, name="sim-broker-io", daemon=True)
        self._thread.start()
        return self.endpoint

    @property
    def bound(self) -> bool:
        return self._bound

    def send(self, identity: bytes, data: bytes) -> None:
        self._command("send", identity, data)

    def unbind(self) -> None:
        """Simulate a broker front outage: close the ROUTER (clients see a dead peer)."""
        self._bound = False
        self._command("unbind")

    def rebind(self) -> None:
        self._bound = True
        self._command("rebind")

    def stop(self) -> None:
        if self._thread is None:
            self._ctx.term()
            return
        self._command("stop")
        self._thread.join(timeout=5)
        if self._wake_tx is not None:
            self._wake_tx.close(0)
            self._wake_tx = None
        if self._thread.is_alive():  # pragma: no cover - defensive: never block shutdown on term()
            logger.warning("sim broker I/O thread did not stop in time")
        else:
            self._ctx.term()
        self._thread = None

    def _command(self, *cmd: Any) -> None:
        self._cmds.put(cmd)
        if self._wake_tx is not None:
            with contextlib.suppress(zmq.ZMQError):  # a wake-up is already pending
                self._wake_tx.send(b"", zmq.NOBLOCK)

    # ------------------------------------------------------------------ I/O thread
    def _new_router(self, endpoint: str) -> zmq.Socket[bytes]:
        sock = self._ctx.socket(zmq.ROUTER)
        sock.setsockopt(zmq.ROUTER_HANDOVER, 1)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RCVHWM, _HWM)
        sock.setsockopt(zmq.SNDHWM, _HWM)
        try:
            sock.bind(endpoint)
        except zmq.ZMQError:
            sock.close(0)
            raise
        return sock

    def _rebind_with_retry(self) -> zmq.Socket[bytes] | None:
        deadline = time.monotonic() + 5.0
        while True:
            try:
                return self._new_router(self.endpoint)
            except zmq.ZMQError as exc:
                if time.monotonic() > deadline:
                    logger.error("sim broker could not re-bind {}: {}", self.endpoint, exc)
                    return None
                time.sleep(0.1)

    def _deliver(self, identity: bytes, data: bytes) -> bool:
        try:
            self._loop.call_soon_threadsafe(self._on_frame, identity, data)
            return True
        except RuntimeError:  # event loop closed
            return False

    def _run(self) -> None:
        router = self._router
        wake = self._wake_rx
        assert wake is not None
        poller = zmq.Poller()
        poller.register(wake, zmq.POLLIN)
        if router is not None:
            poller.register(router, zmq.POLLIN)
        try:
            while True:
                events = dict(poller.poll(500))
                if router is not None and router in events:
                    while True:
                        try:
                            frames = router.recv_multipart(zmq.NOBLOCK)
                        except zmq.Again:
                            break
                        if len(frames) >= 2 and not self._deliver(frames[0], frames[-1]):
                            return
                if wake in events:
                    while True:
                        try:
                            wake.recv(zmq.NOBLOCK)
                        except zmq.Again:
                            break
                while True:
                    try:
                        cmd = self._cmds.get_nowait()
                    except queue.Empty:
                        break
                    op = cmd[0]
                    if op == "send":
                        if router is not None:
                            # peer gone or HWM reached: drop like a real front would
                            with contextlib.suppress(zmq.ZMQError):
                                router.send_multipart([cmd[1], cmd[2]], zmq.NOBLOCK)
                    elif op == "unbind":
                        if router is not None:
                            poller.unregister(router)
                            router.close(0)
                            router = None
                    elif op == "rebind":
                        if router is None:
                            router = self._rebind_with_retry()
                            if router is not None:
                                poller.register(router, zmq.POLLIN)
                    elif op == "stop":
                        return
        finally:
            if router is not None:
                router.close(0)
            wake.close(0)
