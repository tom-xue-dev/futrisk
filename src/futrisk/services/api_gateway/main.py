"""Entry point of ``futrisk svc api-gateway`` (01 §7.1; ``futrisk.cli.svc_cmd``).

``run(home=None, instance=None, shard=None, parent_pid=None, **_) -> int`` runs
the service in the foreground on a selector event loop (``zmq.asyncio`` on
Windows) and returns the exit code: 0 after a graceful stop (``ctl.shutdown``,
Ctrl-C / Ctrl-Break, parent exit), 1 on a start-up failure or a fatal error
(the supervisor then restarts it, 01 §8.6).

Order of start-up (cross-home identity guard): the HTTP port of
``services.yaml`` is bound **before** the runtime starts (no heartbeat yet, so
nothing looks alive) and before the bootstrap key is written. A busy port --
e.g. the api-gateway of another home -- ends the process with exit code 1 and
``HTTP port N in use``; it never reports ``RUNNING`` and never writes a key.

The uvicorn server runs inside the runtime's event loop and never installs
signal handlers; the runtime owns Ctrl-C / Ctrl-Break and watches the parent
process (``parent_pid``, polled every 2 s) so the service exits when the
supervisor is gone.
"""

from __future__ import annotations

import socket
from typing import Any

from loguru import logger

from futrisk.common.config import ensure_home_config, load_active_config
from futrisk.common.paths import HomeLayout, resolve_home
from futrisk.runtime import AsyncServiceRuntime, run_async
from futrisk.services.api_gateway.service import (
    ApiGatewayService,
    HttpPortInUse,
    bind_http_socket,
    http_endpoint,
)

INSTANCE = "api-gateway"
PARENT_POLL_S = 2.0
CLOCK_WAIT_S = 5.0


def bind_configured_port(home: str | None, instance: str) -> socket.socket:
    """Bind the HTTP socket of ``services.yaml`` (active config of ``home``); raises when it is taken."""
    layout = HomeLayout(resolve_home(home))
    ensure_home_config(layout)
    host, port = http_endpoint(load_active_config(layout), instance)
    return bind_http_socket(host, port)


async def main(*, home: str | None = None, instance: str = INSTANCE, parent_pid: int | None = None) -> int:
    try:
        sock = bind_configured_port(home, instance)
    except HttpPortInUse as exc:
        logger.error("api_gateway.http_port_in_use", instance=instance, error=str(exc))
        return 1
    except Exception:
        logger.exception("api_gateway.config_failed", instance=instance)
        return 1
    rt = AsyncServiceRuntime(instance, home=home, parent_pid=parent_pid, parent_poll_s=PARENT_POLL_S)
    service = ApiGatewayService(rt, http_socket=sock)
    service.register_handlers()
    try:
        await rt.start()
    except Exception:
        sock.close()
        logger.exception("api_gateway.runtime_start_failed", instance=instance)
        return 1
    try:
        try:
            rt.install_signal_handlers()
        except (ValueError, RuntimeError):  # not the main thread (embedded use)
            logger.debug("api_gateway.no_signal_handlers")
        if not await rt.wait_clock_ready(timeout=CLOCK_WAIT_S):
            if rt.is_stopping:
                sock.close()
                return await rt.run_until_stopped()
            logger.warning("api_gateway.clock_not_synced", note="market time follows sys.clock once it arrives")
        await service.start()
    except Exception:
        logger.exception("api_gateway.start_failed", instance=instance)
        rt.exit_code = 1
        await rt.stop()
        return 1
    return await rt.run_until_stopped()


def run(
    home: str | None = None,
    instance: str | None = None,
    shard: int | None = None,
    parent_pid: int | None = None,
    **_: Any,
) -> int:
    """``futrisk svc api-gateway`` (``shard`` is ignored: api-gateway is a single instance)."""
    return run_async(main(home=home, instance=instance or INSTANCE, parent_pid=parent_pid))


__all__ = ["bind_configured_port", "main", "run"]
