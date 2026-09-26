"""Fixtures for the strategy SDK tests: selector event loops (zmq.asyncio on Windows), a fake bus."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Mapping

import pytest

from futrisk.client import FutriskClient
from futrisk.client.testing import FakeBus
from futrisk.common.config import FullConfig
from futrisk.runtime.testing import config_with_free_ports


def pytest_asyncio_loop_factories(config: pytest.Config, item: pytest.Item) -> Mapping[str, Callable[[], asyncio.AbstractEventLoop]]:
    """zmq.asyncio needs add_reader: run async tests of this package on a selector loop."""
    return {"selector": asyncio.SelectorEventLoop}


@pytest.fixture
def bus_cfg(default_config: FullConfig) -> FullConfig:
    """Default config whose md-gateway and risk-engine endpoints use fresh loopback ports."""
    return config_with_free_ports(default_config, ["md-gateway", "risk-engine-0", "risk-engine-1"])


@pytest.fixture
async def fake_bus(bus_cfg: FullConfig) -> AsyncIterator[FakeBus]:
    async with FakeBus(bus_cfg, fill_mode="none") as bus:
        yield bus


@pytest.fixture
async def client(bus_cfg: FullConfig, fake_bus: FakeBus) -> AsyncIterator[FutriskClient]:
    async with FutriskClient(bus_cfg) as cli:
        yield cli
