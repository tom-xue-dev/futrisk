"""Shared pytest fixtures."""

from __future__ import annotations

import pytest

from futrisk.common.config import FullConfig, load_default_config


@pytest.fixture(scope="session")
def default_config() -> FullConfig:
    return load_default_config()
