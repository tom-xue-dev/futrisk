"""Fixtures of the risk-engine shell tests.

The environment itself (fakes, clients, shells) lives in :mod:`.re_support`; test modules import from there with
package-relative imports and never from ``conftest`` (another test package's conftest could answer that name).
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from futrisk.common.config import FullConfig

from .re_support import Env, build_env


@pytest.fixture
def env(tmp_path: Path, default_config: FullConfig) -> Iterator[Env]:
    e = build_env(tmp_path, default_config)
    try:
        yield e
    finally:
        e.close()


@pytest.fixture
def env_factory(tmp_path: Path, default_config: FullConfig) -> Iterator[Callable[..., Env]]:
    made: list[Env] = []

    def make(**kw: Any) -> Env:
        e = build_env(tmp_path, default_config, **kw)
        made.append(e)
        return e

    try:
        yield make
    finally:
        for e in made:
            e.close()
