"""Test helpers for services built on the runtimes (random loopback ports, fast heartbeats).

Example::

    cfg = config_with_free_ports(load_default_config(), heartbeat_ms=100)
    rt = SyncServiceRuntime("trade-gateway", config=cfg, home=tmp_path,
                            market_clock=MarketClock(), setup_logs=False)
"""

from __future__ import annotations

import dataclasses
import socket
import types
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from unittest import mock

from futrisk.common import ids
from futrisk.common.clock import SystemClock
from futrisk.common.config import FullConfig


@contextmanager
def ids_clock_stepped_back(seconds: float) -> Iterator[None]:
    """UUIDv7 ids generated inside (process epochs, database stream ids, ...) are stamped ``seconds`` early.

    Simulates a process started right after Windows time sync stepped the wall
    clock back: its epoch (and a database it creates, its stream id) sorts
    before the ids of its predecessor. Only :mod:`futrisk.common.ids` sees the
    shifted clock; its in-process monotonic guard is reset on entry (a new
    process has none) and restored on exit. Ids created by other threads
    meanwhile are shifted too.
    """
    shift_ns = int(seconds * 1_000_000_000)
    wall = SystemClock()
    fake_time = types.SimpleNamespace(time_ns=lambda: wall.now_ns() - shift_ns)
    with ids._lock:
        saved = ids._last_ms
        ids._last_ms = 0
    try:
        with mock.patch.object(ids, "time", fake_time):
            yield
    finally:
        with ids._lock:
            ids._last_ms = max(saved, ids._last_ms)


def free_tcp_port() -> int:
    """A currently free TCP port on 127.0.0.1."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def config_with_free_ports(
    config: FullConfig,
    instances: Iterable[str] | None = None,
    *,
    heartbeat_ms: int | None = None,
    peer_timeout_missed: int | None = None,
) -> FullConfig:
    """Copy of ``config`` whose PUB/ROUTER endpoints use fresh loopback ports.

    ``instances`` limits the rewrite (default: every instance with endpoints).
    ``heartbeat_ms`` / ``peer_timeout_missed`` shorten the bus timings for tests.
    """
    services = config.services
    wanted = set(instances) if instances is not None else set(services.instances)
    new_instances = {}
    for name, inst in services.instances.items():
        if name in wanted:
            update: dict[str, str] = {}
            if inst.pub is not None or instances is not None:
                update["pub"] = f"tcp://127.0.0.1:{free_tcp_port()}"
            if inst.router is not None or instances is not None:
                update["router"] = f"tcp://127.0.0.1:{free_tcp_port()}"
            new_instances[name] = inst.model_copy(update=update)
        else:
            new_instances[name] = inst
    bus_update: dict[str, int] = {}
    if heartbeat_ms is not None:
        bus_update["heartbeat_ms"] = heartbeat_ms
    if peer_timeout_missed is not None:
        bus_update["peer_timeout_missed"] = peer_timeout_missed
    bus = services.bus.model_copy(update=bus_update) if bus_update else services.bus
    new_services = services.model_copy(update={"instances": new_instances, "bus": bus})
    return dataclasses.replace(config, services=new_services)


__all__ = ["config_with_free_ports", "free_tcp_port", "ids_clock_stepped_back"]
