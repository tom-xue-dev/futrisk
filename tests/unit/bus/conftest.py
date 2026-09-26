"""Fixtures for the bus tests: selector event loops (zmq.asyncio on Windows), zmq context, ports."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator, Mapping
from typing import Any

import pytest
import zmq


def pytest_asyncio_loop_factories(config: pytest.Config, item: pytest.Item) -> Mapping[str, Callable[[], asyncio.AbstractEventLoop]]:
    """zmq.asyncio needs add_reader: run async tests of this package on a selector loop."""
    return {"selector": asyncio.SelectorEventLoop}


@pytest.fixture
def zctx() -> Iterator[zmq.Context[Any]]:
    ctx: zmq.Context[Any] = zmq.Context()
    yield ctx
    ctx.destroy(linger=0)
