"""Fixtures of the sim-exchange tests: a running exchange on ephemeral ports in a temp home."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from futrisk.common.config import FullConfig
from futrisk.common.paths import HomeLayout
from futrisk.sim.exchange import SimExchange

from .helpers import MARKET_OPEN, make_cfg, offset_for, wait_for


@pytest.fixture
def sim_home(tmp_path: Path) -> HomeLayout:
    return HomeLayout(tmp_path / "home")


@pytest.fixture
def sim_cfg(default_config: FullConfig) -> FullConfig:
    return make_cfg(default_config)


@pytest.fixture
async def exchange(sim_cfg: FullConfig, sim_home: HomeLayout) -> AsyncIterator[SimExchange]:
    """A running exchange whose market time is 2026-09-28 09:35 Beijing (all sessions open)."""
    async with SimExchange(sim_cfg, sim_home, offset_for(MARKET_OPEN)) as ex:
        await wait_for(lambda: ex.engine.rounds >= 2)
        yield ex
