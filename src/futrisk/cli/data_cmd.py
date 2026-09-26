"""``futrisk data ...``: real market data from Sina (02 §9.3, 03 §5.1, 01 §8.13).

Commands:

- ``record``: poll ``hq.sinajs.cn`` during trading sessions and record every
  received quote into ``<home>/ticks/raw/{trading_day}/sina/part-*.parquet``
  (plus the raw responses under ``<home>/ticks/wire``); runs until Ctrl-C,
  ``--until`` or ``--duration-s``; sleeps outside the session windows of the
  selected products. Built for a multi-day unattended run: 5 s shards, keep-awake,
  flush on Ctrl-C / Ctrl-Break / console close, single-instance lock, spill
  directories when the disk misbehaves. Wrap it in ``scripts/record_forever.ps1``
  to restart it after a crash;
- ``sina-probe``: fetch once and print the parsed quotes (quick connectivity and
  format check; ``--save`` keeps the raw response as a test fixture);
- ``summary``: per-instrument row counts by mode for recorded days;
- ``rebuild``: regenerate a day's tick shards from the archived raw responses
  (after a parser fix).

All times shown are Beijing time, whatever the machine's time zone.

Exit codes of ``record``: 0 finished (``--until`` / ``--duration-s``), 130 stopped
by Ctrl-C / Ctrl-Break, 3 another recorder holds the lock, 4 the final flush
failed (rows lost, see the log), 1 crash.
"""

from __future__ import annotations

import asyncio
import contextlib
import queue
import shutil
import sys
import tempfile
import threading
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Final, Protocol

import httpx
import typer
from loguru import logger
from rich.console import Console
from rich.table import Table

from futrisk.common.clock import BJ, NS_PER_MS, NS_PER_S, Clock, SystemClock, bj_to_ns, ns_to_bj
from futrisk.common.config import FullConfig, load_default_config
from futrisk.common.logging import setup_logging
from futrisk.common.paths import HomeLayout, resolve_home
from futrisk.marketdata.lifecycle import (
    ConsoleCloseGuard,
    InstanceLock,
    LockHeldError,
    install_stop_signals,
    keep_awake,
)
from futrisk.marketdata.poller import SinaPoll, SinaPoller, make_sina_client
from futrisk.marketdata.recorder import (
    DEFAULT_ROLL_S,
    SINA_DIR,
    TICK_PREFIX,
    WIRE_PREFIX,
    RecorderStats,
    SinaRecorder,
    SymbolStats,
    TickClassifier,
    bucket_trading_day,
    list_shards,
    list_trading_days,
    read_raw_ticks,
    rebuild_ticks_from_wire,
)
from futrisk.marketdata.sessions import SessionGate
from futrisk.marketdata.sina import DEFAULT_FUTURE_TOLERANCE_S, SinaNormalizer, resolve_sina_instruments
from futrisk.messages.enums import TickMode
from futrisk.messages.md import FLAG_AUCTION, FLAG_LIMIT_DOWN_LOCKED, FLAG_LIMIT_UP_LOCKED

app = typer.Typer(help="Real market data: record Sina ticks, probe, inspect", no_args_is_help=True)

INSTANCE = "data-recorder"
IDLE_STATUS_EVERY = 10  # while idle, print only every N-th status interval
EXIT_USER_STOP: Final = 130
EXIT_LOCKED: Final = 3
EXIT_FLUSH_FAILED: Final = 4
RECORD_TIP = (
    "Run it in Windows Terminal: in the legacy console, selecting text pauses the program's output. "
    "For an unattended multi-day run use scripts/record_forever.ps1 (restarts after a crash)."
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def lock_path(layout: HomeLayout) -> Path:
    """Single-instance lock of the Sina recorder (``<home>/ticks/.recorder.lock``)."""
    return layout.home / "ticks" / ".recorder.lock"


def spill_dirs(layout: HomeLayout) -> list[Path]:
    """Fallback roots used when the primary tick/wire directories cannot be written."""
    return [layout.home / "ticks" / "spill", Path(tempfile.gettempdir()) / "futrisk-spill"]


def _make_client(trust_env: bool) -> httpx.AsyncClient:
    """HTTP client for the poller; ``trust_env`` is already resolved (flag, else ``SinaConfig``). Tests patch this."""
    return make_sina_client(trust_env)


def effective_trust_env(config: FullConfig, flag: bool | None) -> bool:
    """``--trust-env/--no-trust-env`` when given, else ``feeds.yaml`` ``sina.trust_env``."""
    return config.feeds.sina.trust_env if flag is None else flag


def _parse_symbols(symbols: str | None) -> list[str] | None:
    if not symbols:
        return None
    out = [s.strip() for s in symbols.split(",") if s.strip()]
    return out or None


def _parse_until(value: str | None) -> int | None:
    """``YYYY-MM-DD HH:MM[:SS]`` (Beijing time) -> UTC ns."""
    if not value:
        return None
    text = value.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return bj_to_ns(datetime.strptime(text, fmt).replace(tzinfo=BJ))
        except ValueError:
            continue
    raise typer.BadParameter(
        f"expected 'YYYY-MM-DD HH:MM' (Beijing time), got {value!r}", param_hint="--until"
    )


def _resolve(
    config: FullConfig, symbols: str | None, future_tolerance_s: float = DEFAULT_FUTURE_TOLERANCE_S
) -> SinaNormalizer:
    try:
        metas = resolve_sina_instruments(config.products, config.feeds.sina.symbols, _parse_symbols(symbols))
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--symbols") from exc
    if not metas:
        raise typer.BadParameter(
            "no symbols selected (feeds.yaml sina.symbols is empty)", param_hint="--symbols"
        )
    return SinaNormalizer(metas, config.calendar, future_tolerance_s=future_tolerance_s)


def _fmt_ns(ns: int | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return "-" if ns is None else ns_to_bj(ns).strftime(fmt)


def _fmt_px(x: float | None) -> str:
    if x is None:
        return "-"
    return f"{x:.3f}".rstrip("0").rstrip(".")


def _flags_str(flags: int) -> str:
    parts = ["C"]  # every Sina tick is coarse (FLAG_COARSE_TS)
    if flags & FLAG_AUCTION:
        parts.append("A")
    if flags & FLAG_LIMIT_UP_LOCKED:
        parts.append("U")
    if flags & FLAG_LIMIT_DOWN_LOCKED:
        parts.append("D")
    return "".join(parts)


def _setup_logs(layout: HomeLayout | None) -> None:
    """JSON logs under ``<home>/logs`` (when a layout is given) plus warnings on stderr (non-blocking)."""
    setup_logging(INSTANCE, layout.logs_dir if layout is not None else None, console=False)
    logger.add(sys.stderr, level="WARNING", format="<level>{level: <7}</level> {message}", enqueue=True)


# ---------------------------------------------------------------------------
# console output off the event loop
# ---------------------------------------------------------------------------


class Printer(Protocol):
    def print(self, *objects: Any, **kwargs: Any) -> None: ...


class ConsolePrinter:
    """Prints rich renderables from a daemon thread fed by a bounded queue.

    A console that blocks (e.g. text selected in the legacy Windows console)
    then only stalls this thread, never the polling event loop; when the queue
    is full, status messages are dropped (and counted) instead of blocking.
    """

    def __init__(self, console: Console, maxsize: int = 256) -> None:
        self.console = console
        self.dropped = 0
        self._queue: queue.Queue[tuple[tuple[Any, ...], dict[str, Any]] | None] = queue.Queue(maxsize)
        self._thread = threading.Thread(target=self._run, name="console-printer", daemon=True)
        self._thread.start()

    def print(self, *objects: Any, **kwargs: Any) -> None:
        try:
            self._queue.put_nowait((objects, kwargs))
        except queue.Full:
            self.dropped += 1

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                return
            objects, kwargs = item
            with contextlib.suppress(Exception):  # a broken console must not kill the recorder
                self.console.print(*objects, **kwargs)

    def close(self, timeout_s: float = 5.0) -> None:
        """Drain the queue (bounded by ``timeout_s``) and stop the thread."""
        try:
            self._queue.put(None, timeout=timeout_s)
        except queue.Full:
            return
        self._thread.join(timeout_s)


# ---------------------------------------------------------------------------
# record
# ---------------------------------------------------------------------------


@dataclass
class RecordOptions:
    home: str | None = None
    symbols: str | None = None
    poll_ms: int | None = None
    timeout_ms: int | None = None
    duration_s: float | None = None
    until: str | None = None
    ignore_sessions: bool = False
    roll_s: float = DEFAULT_ROLL_S
    roll_rows: int = 10_000
    status_s: float = 60.0
    pre_margin_s: float = 180.0
    post_margin_s: float = 300.0
    close_grace_s: float = 300.0
    future_tolerance_s: float = DEFAULT_FUTURE_TOLERANCE_S
    wire: bool = True
    keep_awake: bool = True
    trust_env: bool | None = None


@dataclass
class RecordOutcome:
    """How a ``record`` run ended; filled in even when the run is cancelled."""

    reason: str = "running"  # "duration" | "until" | "signal" | "cancelled" | "error"
    flushed: bool | None = None  # final flush result; None if it never ran
    stats: RecorderStats = field(default_factory=RecorderStats)
    pending_rows: int = 0


class _Status:
    def __init__(
        self, printer: Printer, recorder: SinaRecorder, poller: SinaPoller, clock: Clock, status_s: float
    ) -> None:
        self.printer = printer
        self.recorder = recorder
        self.poller = poller
        self.clock = clock
        self.status_s = max(1.0, status_s)
        self.state = "starting"
        self.next_active_ns: int | None = None

    async def loop(self) -> None:
        n = 0
        while True:
            await asyncio.sleep(self.status_s)
            n += 1
            if self.state == "idle" and n % IDLE_STATUS_EVERY:
                continue
            self.emit(table=self.state == "polling")

    def headline(self) -> str:
        st, ps = self.recorder.stats, self.poller.stats
        totals = {m: sum(s.count(m) for s in st.symbols.values()) for m in TickMode}
        state = (
            self.state
            if self.state != "idle"
            else f"idle until {_fmt_ns(self.next_active_ns, '%m-%d %H:%M')}"
        )
        alarms = ""
        if st.write_errors:
            alarms += f" | [red]write errors {st.write_errors}[/]"
        if st.spilled_shards:
            alarms += f" | [red]spilled shards {st.spilled_shards} (last {st.last_spill})[/]"
        if st.dropped_rows:
            alarms += f" | [red bold]DROPPED ROWS {st.dropped_rows}[/]"
        return (
            f"[bold]{_fmt_ns(self.clock.now_ns(), '%H:%M:%S')}[/] TD {st.bucket_td or '-'} {state} | "
            f"polls {ps.polls} ok {ps.ok} fail {ps.failed} skip {ps.skipped} | "
            f"rows {st.rows} shards {st.shards} pending {self.recorder.pending_rows} | "
            f"live {totals[TickMode.LIVE]} bf {totals[TickMode.BACKFILL]} dup {totals[TickMode.DUPLICATE]} "
            f"rej {totals[TickMode.REJECTED]}{alarms}"
        )

    def emit(self, table: bool) -> None:
        self.printer.print(self.headline(), highlight=False)
        if table:
            self.printer.print(symbol_table(self.recorder.stats.symbols.values()))
        st, ps = self.recorder.stats, self.poller.stats
        logger.bind(trading_day=st.bucket_td).info(
            "recorder.status",
            state=self.state,
            polls=ps.polls,
            failed=ps.failed,
            skipped=ps.skipped,
            rows=st.rows,
            shards=st.shards,
            write_errors=st.write_errors,
            spilled_shards=st.spilled_shards,
            dropped_rows=st.dropped_rows,
            counts={s.instrument_id: s.counts for s in st.symbols.values()},
        )


def symbol_table(symbols: Iterable[SymbolStats]) -> Table:
    """Per-symbol status: last price, volume, lag and row counts by mode."""
    table = Table(box=None, pad_edge=False, show_edge=False, header_style="bold")
    for col in (
        "instrument",
        "last",
        "volume",
        "bid / ask",
        "exch time",
        "lag s",
        "LIVE",
        "BF",
        "DUP",
        "REJ",
        "last reject",
    ):
        table.add_column(
            col, justify="left" if col in ("instrument", "bid / ask", "last reject") else "right"
        )
    for s in symbols:
        t = s.last_tick
        reject = s.last_reason or ""
        if s.last_detail and reject:
            reject += f" ({s.last_detail.split(':')[0]})"
        table.add_row(
            s.instrument_id or s.code,
            _fmt_px(t.last) if t else "-",
            str(t.volume) if t else "-",
            f"{_fmt_px(t.bid1)} / {_fmt_px(t.ask1)}" if t else "-",
            _fmt_ns(t.exch_ts_ns, "%H:%M:%S") if t else "-",
            f"{s.last_lag_ms / 1000:.1f}" if s.last_lag_ms is not None else "-",
            str(s.count(TickMode.LIVE)),
            str(s.count(TickMode.BACKFILL)),
            str(s.count(TickMode.DUPLICATE)),
            str(s.count(TickMode.REJECTED)),
            reject,
        )
    return table


ROLL_TICK_S: Final = 0.5


async def _roll_ticker(recorder: SinaRecorder) -> None:
    """Close due shards every 0.5 s, not only after a poll, so a kill loses at most ~roll_s of rows."""
    while True:
        await asyncio.sleep(ROLL_TICK_S)
        recorder.roll_if_due()


async def _wait(stop: asyncio.Event, timeout_s: float) -> None:
    if timeout_s <= 0:
        return
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout_s)


async def run_record(
    opts: RecordOptions,
    printer: Printer,
    client: httpx.AsyncClient | None = None,
    clock: Clock | None = None,
    *,
    install_handlers: bool = True,
    outcome: RecordOutcome | None = None,
    config: FullConfig | None = None,
) -> RecordOutcome:
    """The ``record`` main loop (separate from the Typer command for tests).

    ``clock`` drives sessions, receive times and the deadline (tests inject a
    shifted clock to place the run inside or outside a session).
    ``install_handlers`` installs the Ctrl-C / Ctrl-Break stop handlers and the
    console-close flush guard (main thread only).
    """
    outcome = outcome or RecordOutcome()
    clock = clock or SystemClock()
    config = config or load_default_config()
    layout = HomeLayout(resolve_home(opts.home))
    sina_cfg = config.feeds.sina
    normalizer = _resolve(config, opts.symbols, opts.future_tolerance_s)
    templates = [t for meta in normalizer.instruments.values() for t in meta.sessions]
    gate = SessionGate(
        config.calendar, templates, pre_margin_s=opts.pre_margin_s, post_margin_s=opts.post_margin_s
    )
    recorder = SinaRecorder(
        layout.ticks_raw_dir,
        normalizer,
        wire_dir=layout.ticks_wire_dir if opts.wire else None,
        spill_dirs=spill_dirs(layout),
        clock=clock,
        roll_s=opts.roll_s,
        roll_rows=opts.roll_rows,
        close_grace_s=opts.close_grace_s,
    )
    poller = SinaPoller(
        normalizer.codes,
        url=sina_cfg.url,
        referer=sina_cfg.referer,
        poll_ms=opts.poll_ms or sina_cfg.poll_ms,
        timeout_ms=opts.timeout_ms or sina_cfg.timeout_ms,
        trust_env=effective_trust_env(config, opts.trust_env),
        clock=clock,
        client=client,
    )
    status = _Status(printer, recorder, poller, clock, opts.status_s)
    deadline_mono = clock.monotonic_ns() + int(opts.duration_s * NS_PER_S) if opts.duration_s else None
    until_ns = _parse_until(opts.until)
    stop = asyncio.Event()

    def finished() -> str | None:
        if stop.is_set():
            return "signal"
        if deadline_mono is not None and clock.monotonic_ns() >= deadline_mono:
            return "duration"
        if until_ns is not None and clock.now_ns() >= until_ns:
            return "until"
        return None

    def in_window() -> bool:
        return opts.ignore_sessions or gate.is_active(clock.now_ns())

    def handler(poll: SinaPoll) -> None:
        recorder.record_poll(poll)

    restore_signals = None
    guard = ConsoleCloseGuard(recorder.emergency_flush)
    tasks: list[asyncio.Task[None]] = []
    outcome.reason = "cancelled"
    try:
        if install_handlers:
            restore_signals = install_stop_signals(asyncio.get_running_loop(), stop.set)
            guard.install()
        printer.print(
            f"[bold]futrisk data record[/] home={layout.home}\n"
            f"  symbols={','.join(m.instrument_id for m in normalizer.instruments.values())}\n"
            f"  poll every {poller.poll_s:.1f}s; a shard every {opts.roll_s:g}s or {opts.roll_rows} rows -> "
            f"{layout.ticks_raw_dir}"
            + (f" (+ raw responses in {layout.ticks_wire_dir})" if opts.wire else "")
            + "\n"
            f"  now {_fmt_ns(clock.now_ns())} Beijing"
            + (f"; stops at {_fmt_ns(until_ns)} Beijing" if until_ns else "")
            + f"; logs in {layout.logs_dir}; Ctrl-C to stop",
            highlight=False,
        )
        logger.info(
            "recorder.start", home=str(layout.home), codes=normalizer.codes, epoch_ms=recorder.epoch_ms
        )
        tasks = [asyncio.create_task(status.loop()), asyncio.create_task(_roll_ticker(recorder))]
        while (why := finished()) is None:
            if in_window():
                status.state = "polling"
                printer.print(f"{_fmt_ns(clock.now_ns())} polling (session window open)", highlight=False)
                await poller.run(handler, stop=stop, keep_running=lambda: finished() is None and in_window())
                recorder.flush()
                continue
            status.state = "idle"
            status.next_active_ns = gate.next_active_ns(clock.now_ns())
            printer.print(
                f"{_fmt_ns(clock.now_ns())} idle: no session open; next polling window starts "
                f"{_fmt_ns(status.next_active_ns)} (Beijing)",
                highlight=False,
            )
            while finished() is None and not in_window():
                wait_s = 30.0
                if status.next_active_ns is not None:
                    wait_s = min(wait_s, max(0.2, (status.next_active_ns - clock.now_ns()) / NS_PER_S))
                if deadline_mono is not None:
                    wait_s = min(wait_s, max(0.0, (deadline_mono - clock.monotonic_ns()) / NS_PER_S))
                if until_ns is not None:
                    wait_s = min(wait_s, max(0.0, (until_ns - clock.now_ns()) / NS_PER_S))
                await _wait(stop, wait_s)
                recorder.roll_if_due()
        outcome.reason = why
    finally:
        outcome.flushed = recorder.close()  # first: nothing may stand between the buffers and the disk
        outcome.pending_rows = recorder.pending_rows + recorder.pending_wire_rows
        outcome.stats = recorder.stats
        if restore_signals is not None:
            restore_signals()
        guard.uninstall()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await poller.aclose()
        status.state = "stopped"
        status.emit(table=True)
        st = recorder.stats
        logger.info(
            "recorder.stop",
            reason=outcome.reason,
            flushed=outcome.flushed,
            rows=st.rows,
            shards=st.shards,
            write_errors=st.write_errors,
            spilled_shards=st.spilled_shards,
            dropped_rows=st.dropped_rows,
        )
    return outcome


@app.command(help=f"Record real Sina ticks into <home>/ticks/raw/<trading_day>/sina. {RECORD_TIP}")
def record(
    home: str | None = typer.Option(
        None, "--home", help="Runtime home (default: FUTRISK_HOME or <repo>/var)"
    ),
    symbols: str | None = typer.Option(
        None,
        "--symbols",
        help="Comma-separated contracts, e.g. rb2701,IF2612 (default: every Sina symbol in feeds.yaml)",
    ),
    poll_ms: int | None = typer.Option(
        None, "--poll-ms", help="Polling interval in ms, >= 1000 (default: feeds.yaml, 1500)"
    ),
    timeout_ms: int | None = typer.Option(
        None, "--timeout-ms", help="HTTP read timeout in ms (default: feeds.yaml)"
    ),
    duration_s: float | None = typer.Option(None, "--duration-s", help="Stop after N seconds"),
    until: str | None = typer.Option(
        None, "--until", help="Stop at this Beijing time, 'YYYY-MM-DD HH:MM' (default: run until Ctrl-C)"
    ),
    ignore_sessions: bool = typer.Option(
        False, "--ignore-sessions", help="Poll continuously, also outside sessions"
    ),
    roll_s: float = typer.Option(
        DEFAULT_ROLL_S, "--roll-s", help="Close a shard after this many seconds (bounds the loss on a kill)"
    ),
    roll_rows: int = typer.Option(10_000, "--roll-rows", help="... or after this many rows"),
    status_s: float = typer.Option(60.0, "--status-s", help="Seconds between status prints"),
    pre_margin_s: float = typer.Option(
        180.0, "--pre-margin-s", help="Start polling this long before a session opens"
    ),
    post_margin_s: float = typer.Option(
        300.0, "--post-margin-s", help="Keep polling this long after a session closes"
    ),
    future_tolerance_s: float = typer.Option(
        DEFAULT_FUTURE_TOLERANCE_S,
        "--future-tolerance-s",
        help="Reject quotes stamped more than this many seconds after their receive time",
    ),
    wire: bool = typer.Option(
        True, "--wire/--no-wire", help="Also archive the raw responses under <home>/ticks/wire"
    ),
    keep_awake_flag: bool = typer.Option(
        True, "--keep-awake/--no-keep-awake", help="Ask Windows not to idle-sleep during the run"
    ),
    trust_env: bool | None = typer.Option(
        None, "--trust-env/--no-trust-env", help="Honour HTTP(S)_PROXY (default: feeds.yaml sina.trust_env)"
    ),
) -> None:
    opts = RecordOptions(
        home=home,
        symbols=symbols,
        poll_ms=poll_ms,
        timeout_ms=timeout_ms,
        duration_s=duration_s,
        until=until,
        ignore_sessions=ignore_sessions,
        roll_s=roll_s,
        roll_rows=roll_rows,
        status_s=status_s,
        pre_margin_s=pre_margin_s,
        post_margin_s=post_margin_s,
        future_tolerance_s=future_tolerance_s,
        wire=wire,
        keep_awake=keep_awake_flag,
        trust_env=trust_env,
    )
    _parse_until(until)  # fail fast on a bad value
    console = Console(highlight=False)
    layout = HomeLayout(resolve_home(home))
    _setup_logs(layout)
    config = load_default_config()
    lock = InstanceLock(lock_path(layout))
    try:
        lock.acquire("recorder")
    except LockHeldError as exc:
        console.print(f"[red bold]{exc}[/]\nStop the other recorder first (or use another --home).")
        raise typer.Exit(EXIT_LOCKED) from exc
    outcome = RecordOutcome()
    printer = ConsolePrinter(console)

    async def main() -> None:
        client = _make_client(effective_trust_env(config, trust_env))
        try:
            await run_record(opts, printer, client, outcome=outcome, config=config)
        finally:
            await client.aclose()

    try:
        with keep_awake(opts.keep_awake) as awake:
            if opts.keep_awake:
                printer.print(
                    "keep-awake "
                    + ("on" if awake else "[red]NOT available[/]")
                    + ": lid close, power button, battery saver and update restarts can still stop the run "
                    "(AC power, lid close = do nothing, pause Windows Update)",
                    highlight=False,
                )
            asyncio.run(main())
    except KeyboardInterrupt:
        outcome.reason = "signal"
    finally:
        printer.close()
        lock.release()
    st = outcome.stats
    if outcome.flushed is True:
        console.print(
            f"stopped ({outcome.reason}): all buffered rows were written -- {st.rows} rows, {st.shards} tick shards, "
            f"{st.wire_shards} wire shards"
            + (
                f", [red]{st.spilled_shards} shards in spill directories (last {st.last_spill})[/]"
                if st.spilled_shards
                else ""
            )
            + (f", last shard {st.last_shard}" if st.last_shard else ""),
            highlight=False,
        )
    elif outcome.flushed is False:
        console.print(
            f"[red bold]FINAL FLUSH FAILED: {outcome.pending_rows} buffered rows could not be written to "
            f"{layout.ticks_raw_dir} nor to any spill directory -- see {layout.logs_dir}[/]"
        )
        raise typer.Exit(EXIT_FLUSH_FAILED)
    else:
        console.print(
            "[red bold]the recorder did not shut down cleanly; rows buffered since the last shard may be lost[/]"
        )
        raise typer.Exit(EXIT_FLUSH_FAILED)
    if outcome.reason == "signal":
        raise typer.Exit(EXIT_USER_STOP)


# ---------------------------------------------------------------------------
# sina-probe
# ---------------------------------------------------------------------------


async def _fetch_once(codes: Sequence[str], config: FullConfig, timeout_ms: int, trust_env: bool) -> SinaPoll:
    client = _make_client(trust_env)
    poller = SinaPoller(
        codes,
        url=config.feeds.sina.url,
        referer=config.feeds.sina.referer,
        timeout_ms=timeout_ms,
        trust_env=trust_env,
        client=client,
    )
    try:
        return await poller.fetch()
    finally:
        await poller.aclose()
        await client.aclose()


@app.command("sina-probe")
def sina_probe(
    symbols: str | None = typer.Option(
        None, "--symbols", help="Comma-separated contracts (default: feeds.yaml)"
    ),
    raw: bool = typer.Option(False, "--raw", help="Also print the raw response lines"),
    save: Path | None = typer.Option(
        None, "--save", help="Write the raw response bytes (GB18030) to this file"
    ),
    timeout_ms: int = typer.Option(5000, "--timeout-ms", help="HTTP timeout in ms"),
    trust_env: bool | None = typer.Option(
        None, "--trust-env/--no-trust-env", help="Honour HTTP(S)_PROXY (default: feeds.yaml sina.trust_env)"
    ),
) -> None:
    """Fetch Sina once and print the parsed quotes (connectivity and format check)."""
    console = Console(highlight=False)
    _setup_logs(None)
    config = load_default_config()
    normalizer = _resolve(config, symbols)
    poll = asyncio.run(
        _fetch_once(normalizer.codes, config, timeout_ms, effective_trust_env(config, trust_env))
    )
    console.print(f"GET {config.feeds.sina.url}{','.join(normalizer.codes)}", highlight=False)
    console.print(
        f"status {poll.status} in {poll.elapsed_ms:.0f} ms, {len(poll.body or b'')} bytes, "
        f"received {_fmt_ns(poll.recv_ns)} (Beijing)",
        highlight=False,
    )
    if save is not None and poll.body is not None:
        save.parent.mkdir(parents=True, exist_ok=True)
        save.write_bytes(poll.body)
        console.print(f"raw response saved to {save}", highlight=False)
    if not poll.ok:
        console.print(f"[red]poll failed: {poll.error}[/]")
        raise typer.Exit(1)
    if raw:
        for line in poll.text().splitlines():
            console.print(line, highlight=False, markup=False, soft_wrap=True)
    classifier = TickClassifier(config.calendar, normalizer.instruments.values())
    bucket = bucket_trading_day(config.calendar, poll.recv_ns)
    table = Table(title=f"Sina quotes (current trading day {bucket})", header_style="bold")
    for col in (
        "instrument",
        "code",
        "name",
        "exch time",
        "TD",
        "last",
        "bid x vol",
        "ask x vol",
        "volume",
        "OI",
        "turnover",
        "pre_settle",
        "limits",
        "flags",
        "mode",
    ):
        table.add_column(col, overflow="fold")
    bad = 0
    for result in normalizer.normalize_text(poll.text(), poll.recv_ns):
        mode, reason = classifier.classify(result, bucket)
        verdict = str(mode) + (f" {reason}" if reason else "")
        t, q = result.tick, result.quote
        if t is None:
            bad += 1
            blanks = [""] * 11
            table.add_row(
                result.instrument_id,
                result.code,
                q.name if q else "",
                *blanks,
                f"[red]{verdict}: {result.detail}[/]",
            )
            continue
        if result.detail:
            verdict += f" ({result.detail})"
        table.add_row(
            t.instrument_id,
            result.code,
            q.name if q else "",
            _fmt_ns(t.exch_ts_ns),
            t.trading_day,
            _fmt_px(t.last),
            f"{_fmt_px(t.bid1)} x {t.bid_vol1}",
            f"{_fmt_px(t.ask1)} x {t.ask_vol1}",
            str(t.volume),
            str(t.open_interest),
            f"{t.turnover:,.0f}",
            _fmt_px(t.pre_settle),
            f"{_fmt_px(t.lower_limit)}-{_fmt_px(t.upper_limit)}",
            _flags_str(t.flags),
            verdict,
        )
    console.print(table)
    if bad:
        raise typer.Exit(2)


# ---------------------------------------------------------------------------
# summary / rebuild
# ---------------------------------------------------------------------------


def _spilled_files(layout: HomeLayout) -> list[Path]:
    out: list[Path] = []
    for root in spill_dirs(layout):
        if root.is_dir():
            out.extend(sorted(root.glob(f"*/{SINA_DIR}/*.parquet")))
    return out


@app.command()
def summary(
    home: str | None = typer.Option(
        None, "--home", help="Runtime home (default: FUTRISK_HOME or <repo>/var)"
    ),
    td: str | None = typer.Option(None, "--td", help="Trading day YYYYMMDD (default: every recorded day)"),
) -> None:
    """Per-instrument row counts by mode for recorded Sina ticks."""
    console = Console(highlight=False)
    _setup_logs(None)
    layout = HomeLayout(resolve_home(home))
    spilled = _spilled_files(layout)
    if spilled:
        console.print(
            f"[red bold]{len(spilled)} spilled shard(s) found (e.g. {spilled[0]}); move part-*/wire-* files into "
            f"{layout.ticks_raw_dir}/<td>/sina and {layout.ticks_wire_dir}/<td>/sina[/]"
        )
    days = [td] if td else list_trading_days(layout.ticks_raw_dir)
    if not days:
        console.print(f"no recorded Sina ticks under {layout.ticks_raw_dir}")
        raise typer.Exit(1)
    for day in days:
        df = read_raw_ticks(layout.ticks_raw_dir, day)
        n_shards = len(list_shards(layout.ticks_raw_dir, day, SINA_DIR, TICK_PREFIX))
        n_wire = len(list_shards(layout.ticks_wire_dir, day, SINA_DIR, WIRE_PREFIX))
        table = Table(
            title=f"{day}: {len(df)} rows, {n_shards} tick shards, {n_wire} wire shards", header_style="bold"
        )
        for col in (
            "instrument",
            "LIVE",
            "BACKFILL",
            "DUPLICATE",
            "REJECTED",
            "first accepted",
            "last accepted",
            "last",
            "max volume",
        ):
            table.add_column(col, justify="left" if col == "instrument" else "right")
        if len(df):
            df["instrument_id"] = df["instrument_id"].astype(str)
            df["mode"] = df["mode"].astype(str)
            for iid, g in df.groupby("instrument_id", sort=True):
                counts = g["mode"].value_counts()
                acc = g[g["mode"].isin([str(TickMode.LIVE), str(TickMode.BACKFILL)])]
                table.add_row(
                    str(iid),
                    *(
                        str(int(counts.get(str(m), 0)))
                        for m in (TickMode.LIVE, TickMode.BACKFILL, TickMode.DUPLICATE, TickMode.REJECTED)
                    ),
                    _fmt_ns(int(acc["exch_ts_ns"].min()), "%m-%d %H:%M:%S") if len(acc) else "-",
                    _fmt_ns(int(acc["exch_ts_ns"].max()), "%m-%d %H:%M:%S") if len(acc) else "-",
                    _fmt_px(float(acc["last"].iloc[-1])) if len(acc) else "-",
                    str(int(acc["volume"].max())) if len(acc) else "-",
                )
        console.print(table)


@app.command()
def rebuild(
    td: str = typer.Option(..., "--td", help="Trading day YYYYMMDD to rebuild"),
    home: str | None = typer.Option(
        None, "--home", help="Runtime home (default: FUTRISK_HOME or <repo>/var)"
    ),
    symbols: str | None = typer.Option(None, "--symbols", help="Contracts to include (default: feeds.yaml)"),
    future_tolerance_s: float = typer.Option(
        DEFAULT_FUTURE_TOLERANCE_S,
        "--future-tolerance-s",
        help="Raise it if the recording machine's clock was slow (quotes were rejected as future_ts)",
    ),
) -> None:
    """Regenerate ticks/raw/<td>/sina from the archived raw responses (e.g. after a parser fix)."""
    console = Console(highlight=False)
    layout = HomeLayout(resolve_home(home))
    _setup_logs(layout)
    lock = InstanceLock(lock_path(layout))
    try:
        lock.acquire("recorder")
    except LockHeldError as exc:
        console.print(f"[red bold]{exc}[/]\nStop the recorder before rebuilding this home.")
        raise typer.Exit(EXIT_LOCKED) from exc
    try:
        config = load_default_config()
        normalizer = _resolve(config, symbols, future_tolerance_s)
        clock = SystemClock()
        stamp = clock.now_ns() // NS_PER_MS
        staging = layout.home / "ticks" / f".staging-{stamp}"
        report = rebuild_ticks_from_wire(layout.ticks_wire_dir, staging, td, normalizer, clock=clock)
        if report.polls == 0:
            shutil.rmtree(staging, ignore_errors=True)
            console.print(f"[red]no archived polls for {td} under {layout.ticks_wire_dir}[/]")
            raise typer.Exit(1)
        target = layout.ticks_raw_dir / td / SINA_DIR
        if target.exists():
            backup = layout.home / "ticks" / "rebuild-backup" / td / f"{SINA_DIR}-{stamp}"
            backup.parent.mkdir(parents=True, exist_ok=True)
            target.rename(backup)
            console.print(f"previous shards moved to {backup}", highlight=False)
        target.parent.mkdir(parents=True, exist_ok=True)
        (staging / td / SINA_DIR).rename(target)
        shutil.rmtree(staging, ignore_errors=True)
        console.print(
            f"rebuilt {td}: {report.polls} polls -> {report.rows} rows in {len(report.shards)} shards at {target}"
        )
    finally:
        lock.release()
