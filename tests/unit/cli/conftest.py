"""Fixtures of the tools CLI tests: a home, the fake API behind ``httpx.MockTransport``, the CLI."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
from clitools_support import Cli, FakeApi, make_home, tools_app

from futrisk.cli import apiclient, ops_cmd, toolkit
from futrisk.cli.ops_cmd import OpsContext
from futrisk.common.paths import HomeLayout


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("COLUMNS", "220")
    monkeypatch.delenv("FUTRISK_API_KEY", raising=False)
    monkeypatch.delenv("FUTRISK_HOME", raising=False)


@pytest.fixture(autouse=True)
def _config_once_per_home(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each home's committed config is really loaded (``ops_cmd.load_context``) once per test, then reused:
    PyYAML parsing dominates an invocation and a test invokes the CLI several times on the same home."""
    real = ops_cmd.load_context  # the function toolkit.session() calls
    cache: dict[str | None, OpsContext] = {}

    def load_once(home: str | None) -> OpsContext:
        if home not in cache:
            cache[home] = real(home)
        return cache[home]

    monkeypatch.setattr(toolkit, "load_context", load_once)


def _install(monkeypatch: pytest.MonkeyPatch, api: FakeApi) -> list[float]:
    sleeps: list[float] = []
    monkeypatch.setattr(apiclient, "TRANSPORT", httpx.MockTransport(api))
    monkeypatch.setattr(apiclient, "SLEEP", sleeps.append)
    return sleeps


def _harness(home: Path, api: FakeApi, monkeypatch: pytest.MonkeyPatch) -> Cli:
    api.home_id = HomeLayout(home).home_id()  # the fake gateway serves this home
    return Cli(tools_app(), home, api, _install(monkeypatch, api))


@pytest.fixture
def cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Cli]:
    """Demo-mode home + fake api-gateway / sim control."""
    yield _harness(make_home(tmp_path / "home"), FakeApi(), monkeypatch)


@pytest.fixture
def prod_cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Cli]:
    """``app.mode=prod`` home; the fake api-gateway refuses ``/demo/*`` with 403."""
    yield _harness(make_home(tmp_path / "home", mode="prod"), FakeApi(demo=False), monkeypatch)
