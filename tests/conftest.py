"""Shared pytest fixtures: the default config and the captured Sina responses.

The files under ``tests/fixtures/sina`` are raw response bodies captured from ``hq.sinajs.cn``
on 2026-09-25 (GB18030 bytes, see ``manifest.json``); tests never touch the network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from futrisk.common.config import FullConfig, load_default_config
from futrisk.services.md_gateway.sina import decode_body, split_response

SINA_FIXTURES = Path(__file__).resolve().parent / "fixtures" / "sina"


@pytest.fixture(scope="session")
def default_config() -> FullConfig:
    return load_default_config()


@pytest.fixture(scope="session")
def sina_bytes() -> dict[str, bytes]:
    """File name -> raw captured bytes."""
    return {p.name: p.read_bytes() for p in SINA_FIXTURES.glob("*.txt")}


@pytest.fixture(scope="session")
def payloads(sina_bytes: dict[str, bytes]) -> dict[str, str]:
    """Sina code -> payload, merged from every captured response."""
    out: dict[str, str] = {}
    for name in ("commodity.txt", "cffex.txt", "invalid.txt", "all_configured.txt"):
        out.update(split_response(decode_body(sina_bytes[name])).payloads)
    return out
