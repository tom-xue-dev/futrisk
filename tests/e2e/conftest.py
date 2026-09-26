"""End-to-end tests start the whole system (8 processes) and take several minutes.

They run only when selected explicitly: ``pytest -m e2e tests/e2e`` or ``FUTRISK_E2E=1``.
"""

from __future__ import annotations

import os

import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    selected = "e2e" in (config.getoption("-m") or "") or os.environ.get("FUTRISK_E2E") == "1"
    if selected:
        return
    skip = pytest.mark.skip(reason="end-to-end test: run with `pytest -m e2e tests/e2e` or FUTRISK_E2E=1")
    for item in items:
        if "e2e" in item.keywords:
            item.add_marker(skip)
