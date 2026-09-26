"""Fixtures of the md-gateway tests: selector event loops (zmq.asyncio on Windows), session index, params."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping

import pytest

from futrisk.common.config import FullConfig
from futrisk.services.md_gateway.normalizer import Normalizer
from futrisk.services.md_gateway.sessions import SessionIndex


def pytest_asyncio_loop_factories(config: pytest.Config, item: pytest.Item) -> Mapping[str, Callable[[], asyncio.AbstractEventLoop]]:
    """zmq.asyncio needs add_reader: run the async tests of this package on a selector loop."""
    return {"selector": asyncio.SelectorEventLoop}


@pytest.fixture(scope="session")
def sessions(default_config: FullConfig) -> SessionIndex:
    return SessionIndex(default_config.calendar)


@pytest.fixture
def normalizer(default_config: FullConfig, sessions: SessionIndex) -> Normalizer:
    return Normalizer(default_config.products, sessions)
