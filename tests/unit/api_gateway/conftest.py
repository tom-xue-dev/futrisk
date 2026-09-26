"""Fixtures for the api-gateway tests: selector event loops (zmq.asyncio on Windows)."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping

import pytest


def pytest_asyncio_loop_factories(config: pytest.Config, item: pytest.Item) -> Mapping[str, Callable[[], asyncio.AbstractEventLoop]]:
    """zmq.asyncio needs add_reader: run async tests of this package on a selector loop."""
    return {"selector": asyncio.SelectorEventLoop}
