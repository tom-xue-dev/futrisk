"""Fixtures for the market-data tests: captured Sina responses and a normalizer over the default config.

The files under ``tests/fixtures/sina`` are raw response bodies captured from
``hq.sinajs.cn`` on 2026-09-25 (GB18030 bytes, see ``manifest.json``); tests
never touch the network.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from futrisk.common.config import FullConfig
from futrisk.marketdata.sina import SinaNormalizer, decode_body, resolve_sina_instruments, split_response

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "sina"


@pytest.fixture(scope="session")
def sina_bytes() -> dict[str, bytes]:
    """File name -> raw captured bytes."""
    return {p.name: p.read_bytes() for p in FIXTURES.glob("*.txt")}


@pytest.fixture(scope="session")
def payloads(sina_bytes: dict[str, bytes]) -> dict[str, str]:
    """Sina code -> payload, merged from every captured response."""
    out: dict[str, str] = {}
    for name in ("commodity.txt", "cffex.txt", "invalid.txt", "all_configured.txt"):
        out.update(split_response(decode_body(sina_bytes[name])).payloads)
    return out


@pytest.fixture
def normalizer(default_config: FullConfig) -> SinaNormalizer:
    """Normalizer over every Sina symbol of the default ``feeds.yaml``."""
    metas = resolve_sina_instruments(default_config.products, default_config.feeds.sina.symbols)
    return SinaNormalizer(metas, default_config.calendar)
