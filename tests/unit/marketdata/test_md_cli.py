"""``futrisk data`` CLI tests with a mocked HTTP client (no network)."""

from __future__ import annotations

import asyncio
import dataclasses
import signal
import sys
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
from loguru import logger
from typer.testing import CliRunner

import futrisk.cli.data_cmd as data_cmd
from futrisk.common.clock import BJ, MarketClock, SystemClock, bj_to_ns
from futrisk.common.config import FullConfig, load_default_config
from futrisk.common.paths import HomeLayout
from futrisk.marketdata.lifecycle import InstanceLock
from futrisk.marketdata.recorder import list_shards, list_trading_days, read_raw_ticks, read_wire

runner = CliRunner()
ENV = {"COLUMNS": "250"}


def bj_ns(*args: int) -> int:
    return bj_to_ns(datetime(*args, tzinfo=BJ))


def market_clock_at(*bj: int) -> MarketClock:
    """Real-time clock shifted so that it starts at the given Beijing time."""
    wall = SystemClock()
    return MarketClock(wall=wall, mode="DEMO", offset_ns=bj_ns(*bj) - wall.now_ns())


class ListPrinter:
    def __init__(self) -> None:
        self.lines: list[str] = []

    def print(self, *objects: Any, **kwargs: Any) -> None:
        self.lines.extend(o for o in objects if isinstance(o, str))

    @property
    def text(self) -> str:
        return "\n".join(self.lines)


@pytest.fixture(autouse=True)
def _release_loguru() -> Iterator[None]:
    yield
    logger.remove()  # the commands configure file sinks under tmp_path; release them
    logger.add(sys.stderr, level="WARNING")


@pytest.fixture
def requests_seen(sina_bytes: dict[str, bytes]) -> list[httpx.Request]:
    return []


@pytest.fixture
def mock_client(requests_seen: list[httpx.Request], sina_bytes: dict[str, bytes]) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return httpx.Response(200, content=sina_bytes["all_configured.txt"])

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.fixture
def mock_sina(
    monkeypatch: pytest.MonkeyPatch, requests_seen: list[httpx.Request], sina_bytes: dict[str, bytes]
) -> list[bool]:
    """Route the CLI's HTTP client to a mock serving the captured response; records trust_env values."""
    trust: list[bool] = []

    def make(trust_env: bool) -> httpx.AsyncClient:
        trust.append(trust_env)

        def handler(request: httpx.Request) -> httpx.Response:
            requests_seen.append(request)
            return httpx.Response(200, content=sina_bytes["all_configured.txt"])

        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    monkeypatch.setattr(data_cmd, "_make_client", make)
    return trust


def record_args(home: Path, *extra: str) -> list[str]:
    return [
        "record",
        "--home",
        str(home),
        "--poll-ms",
        "1000",
        "--status-s",
        "100",
        "--no-keep-awake",
        *extra,
    ]


# ---------------------------------------------------------------------------
# sina-probe
# ---------------------------------------------------------------------------


class TestSinaProbe:
    def test_prints_every_symbol_and_saves_raw_bytes(
        self,
        mock_sina: list[bool],
        requests_seen: list[httpx.Request],
        tmp_path: Path,
        sina_bytes: dict[str, bytes],
    ) -> None:
        out = tmp_path / "probe.txt"
        result = runner.invoke(data_cmd.app, ["sina-probe", "--save", str(out)], env=ENV)
        assert result.exit_code == 0, result.output
        for iid in (
            "rb2701",
            "hc2701",
            "cu2611",
            "au2612",
            "i2701",
            "j2701",
            "m2701",
            "IF2612",
            "IC2612",
            "T2612",
        ):
            assert iid in result.output
        assert out.read_bytes() == sina_bytes["all_configured.txt"]
        assert requests_seen[0].headers["Referer"] == "https://finance.sina.com.cn"

    def test_symbol_selection_and_bad_symbol(
        self, mock_sina: list[bool], requests_seen: list[httpx.Request]
    ) -> None:
        result = runner.invoke(data_cmd.app, ["sina-probe", "--symbols", "RB2701,if2612"], env=ENV)
        assert result.exit_code == 0, result.output
        assert str(requests_seen[-1].url).endswith("list=nf_RB2701,CFF_RE_IF2612")
        result = runner.invoke(data_cmd.app, ["sina-probe", "--symbols", "zz2701"], env=ENV)
        assert result.exit_code == 2


# ---------------------------------------------------------------------------
# record (through the Typer command)
# ---------------------------------------------------------------------------


class TestRecordCommand:
    def test_short_run_writes_readable_shards(
        self, mock_sina: list[bool], requests_seen: list[httpx.Request], tmp_path: Path
    ) -> None:
        home = tmp_path / "home"
        result = runner.invoke(
            data_cmd.app, record_args(home, "--duration-s", "1.2", "--ignore-sessions"), env=ENV
        )
        assert result.exit_code == 0, result.output
        assert "all buffered rows were written" in result.output
        raw = home / "ticks" / "raw"
        [day] = list_trading_days(raw)
        df = read_raw_ticks(raw, day)
        polls = len(requests_seen)
        assert polls >= 1 and len(df) == 11 * polls
        assert set(df["instrument_id"].astype(str)) >= {"rb2701", "IF2612", "T2612"}
        assert len(read_wire(home / "ticks" / "wire", day)) == polls
        assert list((home / "logs").glob("data-recorder_*.log"))
        assert not (home / "ticks" / ".recorder.lock.owner").exists()  # lock released on exit

        result = runner.invoke(data_cmd.app, ["summary", "--home", str(home)], env=ENV)
        assert result.exit_code == 0, result.output
        assert "rb2701" in result.output

        result = runner.invoke(data_cmd.app, ["rebuild", "--home", str(home), "--td", day], env=ENV)
        assert result.exit_code == 0, result.output
        assert len(read_raw_ticks(raw, day)) == 11 * polls
        assert len(list_shards(raw, day)) == 1
        assert list((home / "ticks" / "rebuild-backup" / day).iterdir())

    def test_second_recorder_exits_with_a_clear_message(self, mock_sina: list[bool], tmp_path: Path) -> None:
        home = tmp_path / "home"
        with InstanceLock(data_cmd.lock_path(HomeLayout(home))):
            result = runner.invoke(
                data_cmd.app, record_args(home, "--duration-s", "1", "--ignore-sessions"), env=ENV
            )
            assert result.exit_code == data_cmd.EXIT_LOCKED
            assert "another recorder is already running" in result.output
            result = runner.invoke(
                data_cmd.app, ["rebuild", "--home", str(home), "--td", "20260928"], env=ENV
            )
            assert result.exit_code == data_cmd.EXIT_LOCKED

    def test_until_in_the_past_stops_at_once(
        self, mock_sina: list[bool], requests_seen: list[httpx.Request], tmp_path: Path
    ) -> None:
        home = tmp_path / "home"
        result = runner.invoke(data_cmd.app, record_args(home, "--until", "2026-01-01 00:00"), env=ENV)
        assert result.exit_code == 0, result.output
        assert "stopped (until)" in result.output and requests_seen == []
        bad = runner.invoke(data_cmd.app, record_args(home, "--until", "tomorrow"), env=ENV)
        assert bad.exit_code == 2

    def test_trust_env_follows_config_unless_overridden(
        self, mock_sina: list[bool], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        args = record_args(tmp_path / "home", "--until", "2026-01-01 00:00")
        for extra, expected in (([], False), (["--trust-env"], True), (["--no-trust-env"], False)):
            assert runner.invoke(data_cmd.app, [*args, *extra], env=ENV).exit_code == 0
            assert mock_sina[-1] is expected
        cfg = load_default_config()
        sina = cfg.feeds.sina.model_copy(update={"trust_env": True})
        proxied: FullConfig = dataclasses.replace(cfg, feeds=cfg.feeds.model_copy(update={"sina": sina}))
        monkeypatch.setattr(data_cmd, "load_default_config", lambda: proxied)
        for extra, expected in (([], True), (["--no-trust-env"], False)):
            assert runner.invoke(data_cmd.app, [*args, *extra], env=ENV).exit_code == 0
            assert mock_sina[-1] is expected


# ---------------------------------------------------------------------------
# run_record (session gate, signals, cancellation)
# ---------------------------------------------------------------------------


class TestRunRecord:
    async def test_gate_on_inside_a_session_polls(
        self, mock_client: httpx.AsyncClient, requests_seen: list[httpx.Request], tmp_path: Path
    ) -> None:
        home = tmp_path / "home"
        opts = data_cmd.RecordOptions(home=str(home), duration_s=1.3, status_s=100, poll_ms=1000)
        printer = ListPrinter()
        outcome = await data_cmd.run_record(
            opts, printer, mock_client, clock=market_clock_at(2026, 9, 28, 10, 30, 0), install_handlers=False
        )
        await mock_client.aclose()
        assert outcome.reason == "duration" and outcome.flushed is True
        assert "polling (session window open)" in printer.text and len(requests_seen) >= 1
        df = read_raw_ticks(home / "ticks" / "raw", "20260928")
        assert len(df) == 11 * len(requests_seen)
        # the captured 09-24 snapshot, received on 09-28, is a stale state of another trading day
        assert set(df["reject_reason"].astype(str)) == {"MD_REJECT_TRADING_DAY"}

    async def test_gate_on_outside_a_session_idles(
        self, mock_client: httpx.AsyncClient, requests_seen: list[httpx.Request], tmp_path: Path
    ) -> None:
        home = tmp_path / "home"
        opts = data_cmd.RecordOptions(home=str(home), duration_s=1.0, status_s=100, poll_ms=1000)
        printer = ListPrinter()
        # 12:00 Beijing: commodities and index futures are both in their lunch break
        outcome = await data_cmd.run_record(
            opts, printer, mock_client, clock=market_clock_at(2026, 9, 28, 12, 0, 0), install_handlers=False
        )
        await mock_client.aclose()
        assert outcome.reason == "duration" and outcome.flushed is True
        assert requests_seen == []
        assert "next polling window starts 2026-09-28 12:57:00" in printer.text
        assert list_trading_days(home / "ticks" / "raw") == []

    @pytest.mark.skipif(sys.platform != "win32", reason="SIGBREAK is Windows-only")
    async def test_ctrl_break_stops_gracefully_and_flushes(
        self, mock_client: httpx.AsyncClient, requests_seen: list[httpx.Request], tmp_path: Path
    ) -> None:
        home = tmp_path / "home"
        opts = data_cmd.RecordOptions(
            home=str(home), ignore_sessions=True, status_s=100, poll_ms=1000, roll_s=600
        )
        outcome = data_cmd.RecordOutcome()
        task = asyncio.create_task(
            data_cmd.run_record(opts, ListPrinter(), mock_client, install_handlers=True, outcome=outcome)
        )
        while len(requests_seen) < 2:
            await asyncio.sleep(0.02)
        signal.raise_signal(signal.SIGBREAK)  # what Ctrl-Break delivers
        await asyncio.wait_for(task, 10)
        await mock_client.aclose()
        assert outcome.reason == "signal" and outcome.flushed is True
        # nothing was rolled before the stop (roll_s=600): every row came from the final flush
        polls = outcome.stats.polls
        assert polls >= 2 and len(read_raw_ticks(home / "ticks" / "raw", "20260928")) == 11 * polls
        assert signal.getsignal(signal.SIGBREAK) is not None  # handlers restored

    async def test_cancellation_still_flushes(
        self, mock_client: httpx.AsyncClient, requests_seen: list[httpx.Request], tmp_path: Path
    ) -> None:
        home = tmp_path / "home"
        opts = data_cmd.RecordOptions(
            home=str(home), ignore_sessions=True, status_s=100, poll_ms=1000, roll_s=600
        )
        outcome = data_cmd.RecordOutcome()
        task = asyncio.create_task(
            data_cmd.run_record(opts, ListPrinter(), mock_client, install_handlers=False, outcome=outcome)
        )
        while len(requests_seen) < 2:
            await asyncio.sleep(0.02)
        task.cancel()  # what a second Ctrl-C does through asyncio.run
        with pytest.raises(asyncio.CancelledError):
            await task
        await mock_client.aclose()
        assert outcome.reason == "cancelled" and outcome.flushed is True
        rows = len(read_raw_ticks(home / "ticks" / "raw", "20260928"))
        assert rows == 11 * outcome.stats.polls and rows >= 11
