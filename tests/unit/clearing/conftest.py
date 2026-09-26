"""Fixtures for the clearing-worker tests: selector loops (zmq.asyncio on Windows), free ports, a spawn pool."""

from __future__ import annotations

import asyncio
import multiprocessing
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ProcessPoolExecutor

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


@pytest.fixture(scope="session")
def pool() -> Iterator[ProcessPoolExecutor]:
    """One ``spawn`` process pool shared by the tests (the service does not shut down an injected pool)."""
    ex = ProcessPoolExecutor(max_workers=2, mp_context=multiprocessing.get_context("spawn"))
    try:
        yield ex
    finally:
        ex.shutdown(wait=True, cancel_futures=True)
