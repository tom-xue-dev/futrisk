"""Fixtures for the runtime tests: selector event loops (zmq.asyncio on Windows), free-port config."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping

import pytest

from futrisk.common.config import FullConfig
from futrisk.runtime.testing import config_with_free_ports


def pytest_asyncio_loop_factories(config: pytest.Config, item: pytest.Item) -> Mapping[str, Callable[[], asyncio.AbstractEventLoop]]:
    """zmq.asyncio needs add_reader: run async tests of this package on a selector loop."""
    return {"selector": asyncio.SelectorEventLoop}


@pytest.fixture
def bus_config(default_config: FullConfig) -> FullConfig:
    """Default config with fresh loopback ports and 100 ms heartbeats."""
    return config_with_free_ports(default_config, heartbeat_ms=100)
