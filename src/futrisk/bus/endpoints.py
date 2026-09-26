"""Bus endpoints from ``services.yaml`` (02 §3, §4.9).

Every internal service binds one PUB and one ROUTER socket; subscribers and
callers connect to the addresses configured for the target instance.
"""

from __future__ import annotations

import re

from futrisk.common.config.services import InstanceConfig, ServicesConfig

MD_GATEWAY = "md-gateway"
_SHARD_SUFFIX = re.compile(r"^(?P<service>.+?)-(?P<shard>\d+)$")


def service_of(instance: str) -> str:
    """Service name of an instance: ``risk-engine-1`` -> ``risk-engine``; others unchanged."""
    m = _SHARD_SUFFIX.match(instance)
    return m.group("service") if m else instance


class Endpoints:
    """Instance -> PUB/ROUTER address lookup plus bus parameters."""

    def __init__(self, services: ServicesConfig) -> None:
        self._services = services

    @property
    def services(self) -> ServicesConfig:
        return self._services

    def instances(self) -> list[str]:
        """All configured internal instances (the ``config.decision`` whitelist, 02 §4.11)."""
        return sorted(self._services.instances)

    def has(self, instance: str) -> bool:
        return instance in self._services.instances

    def instance(self, instance: str) -> InstanceConfig:
        return self._services.instance(instance)

    def pub(self, instance: str) -> str:
        """PUB address of ``instance`` (raises ``KeyError`` when not configured)."""
        addr = self._services.instance(instance).pub
        if not addr:
            raise KeyError(f"instance {instance!r} has no pub endpoint")
        return addr

    def router(self, instance: str) -> str:
        """ROUTER address of ``instance`` (raises ``KeyError`` when not configured)."""
        addr = self._services.instance(instance).router
        if not addr:
            raise KeyError(f"instance {instance!r} has no router endpoint")
        return addr

    def publishers(self) -> list[str]:
        return sorted(n for n, c in self._services.instances.items() if c.pub)

    # ------------------------------------------------------- bus parameters
    @property
    def heartbeat_ms(self) -> int:
        return self._services.bus.heartbeat_ms

    @property
    def peer_timeout_missed(self) -> int:
        return self._services.bus.peer_timeout_missed

    @property
    def router_hwm(self) -> int:
        return self._services.bus.router_hwm

    def pub_hwm(self, instance: str) -> int:
        """SNDHWM of the instance's PUB socket: md-gateway 100k, others 10k (02 §4.9)."""
        bus = self._services.bus
        return bus.md_pub_hwm if service_of(instance) == MD_GATEWAY else bus.pub_hwm


__all__ = ["Endpoints", "service_of"]
