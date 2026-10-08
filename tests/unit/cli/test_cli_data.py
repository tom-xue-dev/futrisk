"""``futrisk data`` CLI tests with a mocked HTTP client (no network): the recorder, the probe, the session gate."""

from __future__ import annotations

import asyncio
import dataclasses
import re
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
from futrisk.cli.data_cmd import PollGate
from futrisk.cli.lifecycle import InstanceLock
from futrisk.common.clock import BJ, MarketClock, SystemClock, bj_to_ns
from futrisk.common.config import FullConfig, load_default_config
from futrisk.common.paths import HomeLayout
from futrisk.services.md_gateway.shards import list_trading_days, read_raw_ticks, read_wire
from futrisk.services.md_gateway.sina import decode_body

runner = CliRunner()
ENV = {"COLUMNS": "250"}
_LINE = re.compile(r'^(var\s+hq_str_)([A-Za-z0-9_]+)(\s*=\s*")([^"]*)(".*)$')


def bj_ns(*args: int) -> int:
    return bj_to_ns(datetime(*args, tzinfo=BJ))


def market_clock_at(*bj: int) -> MarketClock:
    """Real-time clock shifted so that it starts at the given Beijing time."""
    wall = SystemClock()
    return MarketClock(wall=wall, mode="DEMO", offset_ns=bj_ns(*bj) - wall.now_ns())


def body_at(sina_bytes: dict[str, bytes], dt: datetime) -> bytes:
    """The captured response re-stamped to ``dt`` (both layouts), so the quotes belong to that trading day."""
    out = []
    for line in decode_body(sina_bytes["all_configured.txt"]).splitlines():
        m = _LINE.match(line)
        if m is None:
            continue
        f = m.group(4).split(",")
        if m.group(2).startswith("CFF_RE_"):
            f[36], f[37] = dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")
        else:
            f[1], f[17] = dt.strftime("%H%M%S"), dt.strftime("%Y-%m-%d")
        out.append(f"{m.group(1)}{m.group(2)}{m.group(3)}{','.join(f)}{m.group(5)}")
    return "\n".join(out).encode("gb18030")


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
def requests_seen() -> list[httpx.Request]:
    return []


def _client(requests_seen: list[httpx.Request], body: bytes) -> httpx.AsyncClient:
    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        return httpx.Response(200, content=body)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.fixture
def session_client(requests_seen: list[httpx.Request], sina_bytes: dict[str, bytes]) -> httpx.AsyncClient:
    """Serves the snapshot re-stamped to 2026-09-28 10:30:00 (rb / IF continuous sessions)."""
    return _client(requests_seen, body_at(sina_bytes, datetime(2026, 9, 28, 10, 30, 0)))


@pytest.fixture
def mock_sina(monkeypatch: pytest.MonkeyPatch, requests_seen: list[httpx.Request], sina_bytes: dict[str, bytes]) -> list[bool]:
    """Route the CLI's HTTP client to a mock serving the captured response; records trust_env values."""
    trust: list[bool] = []

    def make(trust_env: bool) -> httpx.AsyncClient:
        trust.append(trust_env)
        return _client(requests_seen, sina_bytes["all_configured.txt"])

    monkeypatch.setattr(data_cmd, "_make_client", make)
    return trust


def record_args(home: Path, *extra: str) -> list[str]:
    return ["record", "--home", str(home), "--poll-ms", "1000", "--status-s", "100", "--no-keep-awake", *extra]


class TestSinaProbe:
    def test_prints_every_symbol_and_saves_raw_bytes(self, mock_sina: list[bool], requests_seen: list[httpx.Request], tmp_path: Path, sina_bytes: dict[str, bytes]) -> None:
        out = tmp_path / "probe.txt"
        result = runner.invoke(data_cmd.app, ["sina-probe", "--save", str(out)], env=ENV)
        assert result.exit_code == 0, result.output
        for iid in ("rb2701", "hc2701", "cu2611", "au2612", "i2701", "j2701", "m2701", "IF2612", "IC2612", "T2612"):
            assert iid in result.output
        assert out.read_bytes() == sina_bytes["all_configured.txt"]
        assert requests_seen[0].headers["Referer"] == "https://finance.sina.com.cn"

    def test_symbol_selection_and_bad_symbol(self, mock_sina: list[bool], requests_seen: list[httpx.Request]) -> None:
        result = runner.invoke(data_cmd.app, ["sina-probe", "--symbols", "RB2701,if2612"], env=ENV)
        assert result.exit_code == 0, result.output
        assert str(requests_seen[-1].url).endswith("list=nf_RB2701,CFF_RE_IF2612")
        assert runner.invoke(data_cmd.app, ["sina-probe", "--symbols", "zz2701"], env=ENV).exit_code == 2


class TestRecordCommand:
    def test_short_run_archives_polls_and_skips_stale_snapshots(self, mock_sina: list[bool], requests_seen: list[httpx.Request], tmp_path: Path) -> None:
        home = tmp_path / "home"
        result = runner.invoke(data_cmd.app, record_args(home, "--duration-s", "1.2", "--ignore-sessions"), env=ENV)
        assert result.exit_code == 0, result.output
        assert "all buffered rows were written" in result.output
        polls = len(requests_seen)
        assert polls >= 1
        wire = home / "ticks" / "wire"
        [day] = list_trading_days(wire, "sina", "wire")
        assert len(read_wire(wire, day)) == polls
        # the captured 09-24 snapshot is stale for every later day: skipped by the validator, never recorded (02 §9.2)
        assert list_trading_days(home / "ticks" / "raw") == []
        assert list((home / "logs").glob("data-recorder_*.log"))
        assert not (home / "ticks" / ".recorder.lock.owner").exists()  # lock released on exit
        result = runner.invoke(data_cmd.app, ["summary", "--home", str(home)], env=ENV)
        assert result.exit_code == 1 and "no recorded Sina ticks" in result.output

    def test_second_recorder_exits_with_a_clear_message(self, mock_sina: list[bool], tmp_path: Path) -> None:
        home = tmp_path / "home"
        with InstanceLock(data_cmd.lock_path(HomeLayout(home))):
            result = runner.invoke(data_cmd.app, record_args(home, "--duration-s", "1", "--ignore-sessions"), env=ENV)
            assert result.exit_code == data_cmd.EXIT_LOCKED and "another recorder is already running" in result.output

    def test_until_in_the_past_stops_at_once(self, mock_sina: list[bool], requests_seen: list[httpx.Request], tmp_path: Path) -> None:
        home = tmp_path / "home"
        result = runner.invoke(data_cmd.app, record_args(home, "--until", "2026-01-01 00:00"), env=ENV)
        assert result.exit_code == 0, result.output
        assert "stopped (until)" in result.output and requests_seen == []
        assert runner.invoke(data_cmd.app, record_args(home, "--until", "tomorrow"), env=ENV).exit_code == 2

    def test_trust_env_follows_config_unless_overridden(self, mock_sina: list[bool], tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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


class TestRunRecord:
    async def test_inside_a_session_every_state_is_recorded_through_the_pipeline(self, session_client: httpx.AsyncClient, requests_seen: list[httpx.Request], tmp_path: Path) -> None:
        home = tmp_path / "home"
        opts = data_cmd.RecordOptions(home=str(home), duration_s=1.3, status_s=100, poll_ms=1000)
        printer = ListPrinter()
        outcome = await data_cmd.run_record(opts, printer, session_client, clock=market_clock_at(2026, 9, 28, 10, 30, 0), install_handlers=False)
        await session_client.aclose()
        assert outcome.reason == "duration" and outcome.flushed is True and outcome.stats.polls == len(requests_seen) >= 1
        assert "polling (session window open)" in printer.text
        df = read_raw_ticks(home / "ticks" / "raw", "20260928")
        assert len(df) == 11 * len(requests_seen)  # 11 configured symbols per poll
        modes = df["mode"].astype(str).value_counts().to_dict()
        assert modes["LIVE"] == 11 and modes.get("DUPLICATE", 0) == 11 * (len(requests_seen) - 1)  # the same snapshot again: duplicates
        assert set(df["source"].astype(str)) == {"SINA"} and df["reject_reason"].isna().all()
        assert len(read_wire(home / "ticks" / "wire", "20260928")) == len(requests_seen)
        rb = outcome.stats.symbols["nf_RB2701"]
        assert rb.count("LIVE") == 1 and rb.last_tick is not None and rb.last_tick.volume == 570630

    async def test_outside_a_session_the_recorder_idles(self, session_client: httpx.AsyncClient, requests_seen: list[httpx.Request], tmp_path: Path) -> None:
        home = tmp_path / "home"
        opts = data_cmd.RecordOptions(home=str(home), duration_s=1.0, status_s=100, poll_ms=1000)
        printer = ListPrinter()
        # 12:00 Beijing: commodities and index futures are both in their lunch break
        outcome = await data_cmd.run_record(opts, printer, session_client, clock=market_clock_at(2026, 9, 28, 12, 0, 0), install_handlers=False)
        await session_client.aclose()
        assert outcome.reason == "duration" and outcome.flushed is True and requests_seen == []
        assert "next polling window starts 2026-09-28 12:57:00" in printer.text
        assert list_trading_days(home / "ticks" / "raw") == []

    @pytest.mark.skipif(sys.platform != "win32", reason="SIGBREAK is Windows-only")
    async def test_ctrl_break_stops_gracefully_and_flushes(self, session_client: httpx.AsyncClient, requests_seen: list[httpx.Request], tmp_path: Path) -> None:
        home = tmp_path / "home"
        opts = data_cmd.RecordOptions(home=str(home), ignore_sessions=True, status_s=100, poll_ms=1000, roll_s=600)
        outcome = data_cmd.RecordOutcome()
        task = asyncio.create_task(data_cmd.run_record(opts, ListPrinter(), session_client, clock=market_clock_at(2026, 9, 28, 10, 30, 0), install_handlers=True, outcome=outcome))
        while len(requests_seen) < 2:
            await asyncio.sleep(0.02)
        signal.raise_signal(signal.SIGBREAK)  # what Ctrl-Break delivers
        await asyncio.wait_for(task, 10)
        await session_client.aclose()
        assert outcome.reason == "signal" and outcome.flushed is True
        polls = outcome.stats.polls  # nothing was rolled before the stop (roll_s=600): every row came from the final flush
        assert polls >= 2 and len(read_raw_ticks(home / "ticks" / "raw", "20260928")) == 11 * polls
        assert signal.getsignal(signal.SIGBREAK) is not None  # handlers restored

    async def test_cancellation_still_flushes(self, session_client: httpx.AsyncClient, requests_seen: list[httpx.Request], tmp_path: Path) -> None:
        home = tmp_path / "home"
        opts = data_cmd.RecordOptions(home=str(home), ignore_sessions=True, status_s=100, poll_ms=1000, roll_s=600)
        outcome = data_cmd.RecordOutcome()
        task = asyncio.create_task(data_cmd.run_record(opts, ListPrinter(), session_client, clock=market_clock_at(2026, 9, 28, 10, 30, 0), install_handlers=False, outcome=outcome))
        while len(requests_seen) < 2:
            await asyncio.sleep(0.02)
        task.cancel()  # what a second Ctrl-C does through asyncio.run
        with pytest.raises(asyncio.CancelledError):
            await task
        await session_client.aclose()
        assert outcome.reason == "cancelled" and outcome.flushed is True
        rows = len(read_raw_ticks(home / "ticks" / "raw", "20260928"))
        assert rows == 11 * outcome.stats.polls and rows >= 11


# ---------------------------------------------------------------- the session gate (01 §8.2)
RB = ["night_2100_2300", "day_commodity"]
AU = ["night_2100_0230", "day_commodity"]
IF = ["day_index"]
CU = ["night_2100_0100", "day_commodity"]


@pytest.fixture
def rb_gate(default_config: FullConfig) -> PollGate:
    return PollGate(default_config.calendar, RB, pre_margin_s=180, post_margin_s=300)


class TestPollGate:
    @pytest.mark.parametrize(
        ("ts", "active"),
        [((2026, 9, 28, 8, 40), False),  # before the 08:55 auction (no night session after the holiday)
         ((2026, 9, 28, 8, 53), True),  # 3 min pre-margin
         ((2026, 9, 28, 10, 19), True),  # 10:15 close + 5 min post-margin
         ((2026, 9, 28, 10, 22), False),  # morning break
         ((2026, 9, 28, 10, 28), True),  # 3 min before 10:30
         ((2026, 9, 28, 15, 4), True), ((2026, 9, 28, 15, 6), False),
         ((2026, 9, 28, 20, 53), True),  # night auction 20:55 of trading day 09-29
         ((2026, 9, 28, 23, 4), True), ((2026, 9, 28, 23, 6), False),
         ((2026, 9, 30, 20, 53), False),  # no night session before the National Day holiday
         ((2026, 9, 26, 10, 0), False)],  # Saturday
    )
    def test_commodity_windows(self, rb_gate: PollGate, ts: tuple[int, ...], active: bool) -> None:
        assert rb_gate.is_active(bj_ns(*ts)) is active

    def test_other_templates_and_their_union(self, default_config: FullConfig) -> None:
        au = PollGate(default_config.calendar, AU)
        assert au.is_active(bj_ns(2026, 9, 29, 2, 33)) and not au.is_active(bj_ns(2026, 9, 29, 2, 36))  # gold trades until 02:30
        index = PollGate(default_config.calendar, IF)
        assert not index.is_active(bj_ns(2026, 9, 28, 9, 21)) and index.is_active(bj_ns(2026, 9, 28, 9, 23))
        assert index.is_active(bj_ns(2026, 9, 28, 11, 34)) and not index.is_active(bj_ns(2026, 9, 28, 11, 36)) and index.is_active(bj_ns(2026, 9, 28, 12, 58))
        assert PollGate(default_config.calendar, RB + IF).is_active(bj_ns(2026, 9, 28, 10, 22))  # commodity break, but IF trades
        cu = PollGate(default_config.calendar, CU)
        assert cu.is_active(bj_ns(2026, 9, 18, 23, 30)) and cu.is_active(bj_ns(2026, 9, 19, 0, 30)) and cu.is_active(bj_ns(2026, 9, 19, 1, 4))
        assert not cu.is_active(bj_ns(2026, 9, 19, 1, 6)) and cu.next_active_ns(bj_ns(2026, 9, 19, 1, 6)) == bj_ns(2026, 9, 21, 8, 57)
        with pytest.raises(ValueError):
            PollGate(default_config.calendar, [])

    def test_next_active_skips_holidays(self, rb_gate: PollGate) -> None:
        assert rb_gate.next_active_ns(bj_ns(2026, 9, 30, 15, 30)) == bj_ns(2026, 10, 8, 8, 52)
        assert rb_gate.next_active_ns(bj_ns(2026, 9, 28, 15, 30)) == bj_ns(2026, 9, 28, 20, 52)
        now = bj_ns(2026, 9, 28, 10, 0)
        assert rb_gate.next_active_ns(now) == now
        assert rb_gate.next_active_ns(bj_ns(2026, 9, 25, 12, 0)) == bj_ns(2026, 9, 28, 8, 52)  # Friday holiday -> Monday's auction
