"""Fixtures for the strategy-bot tests: selector event loops (zmq.asyncio on Windows), contracts, contexts."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping

import pytest

from futrisk.client.positions import PositionTracker
from futrisk.client.pricing import ContractTable
from futrisk.common.config import FullConfig
from futrisk.runtime.testing import config_with_free_ports


def pytest_asyncio_loop_factories(config: pytest.Config, item: pytest.Item) -> Mapping[str, Callable[[], asyncio.AbstractEventLoop]]:
    """zmq.asyncio needs add_reader: run async tests of this package on a selector loop."""
    return {"selector": asyncio.SelectorEventLoop}


@pytest.fixture
def contracts(default_config: FullConfig) -> ContractTable:
    return ContractTable(default_config.products)


@pytest.fixture
def tracker(contracts: ContractTable) -> PositionTracker:
    return PositionTracker(contracts, trading_day="20260928")


@pytest.fixture
def bus_cfg(default_config: FullConfig) -> FullConfig:
    return config_with_free_ports(default_config, ["md-gateway", "risk-engine-0", "risk-engine-1"])
