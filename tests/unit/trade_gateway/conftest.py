"""Fixtures of the trade-gateway tests: a core over a temporary td.db with a fake broker."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from tgtest_support import Harness, make_harness

from futrisk.common.config import FullConfig


@pytest.fixture
def harness(tmp_path: Path, default_config: FullConfig) -> Iterator[Harness]:
    h = make_harness(tmp_path / "td.db", default_config)
    yield h
    h.db.close()
