"""Run several uvicorn servers inside one event loop without touching process signals.

``uvicorn.Server.serve()`` installs SIGINT/SIGTERM handlers and re-raises captured
signals on exit, which does not compose when one process hosts two HTTP servers plus a
WebSocket server. :class:`UvicornRunner` drives the public ``startup`` / ``main_loop`` /
``shutdown`` steps directly; the owner decides when to stop. Port 0 binds an ephemeral
port (used by the tests); :attr:`port` reports the actual one.
"""

from __future__ import annotations

import asyncio
from typing import Any

import uvicorn


class UvicornRunner:
    def __init__(self, app: Any, host: str, port: int, *, name: str) -> None:
        self.name = name
        self.host = host
        self.port = port
        self.config = uvicorn.Config(
            app,
            host=host,
            port=port,
            log_config=None,  # never reconfigure the process-wide logging
            log_level="warning",
            access_log=False,
            lifespan="off",
            ws="none",
            timeout_graceful_shutdown=2,
        )
        self.server = uvicorn.Server(self.config)
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        config = self.config
        if not config.loaded:
            config.load()
        self.server.lifespan = config.lifespan_class(config)
        try:
            await self.server.startup()
        except SystemExit as exc:  # uvicorn exits when the port cannot be bound
            raise OSError(f"{self.name}: cannot listen on {self.host}:{self.port}") from exc
        sockets = self.server.servers[0].sockets if self.server.servers else []
        if sockets:
            self.port = sockets[0].getsockname()[1]
        self._task = asyncio.create_task(self.server.main_loop(), name=f"{self.name}-http")

    async def stop(self) -> None:
        if self._task is None:
            return
        self.server.should_exit = True
        self._task.cancel()  # main_loop only refreshes the Date header; no need to wait for its 0.1s tick
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        await self.server.shutdown()
