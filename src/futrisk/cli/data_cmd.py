"""``futrisk data ...``: real market data from Sina (02 §9.3, 03 §5.1, 01 §8.13).

- ``record``: md-gateway's Sina source and tick pipeline without the bus. It polls
  ``hq.sinajs.cn`` during trading sessions, runs every quote through the same validator and
  arbiter (Sina as the only, trusted source) and records every received state into
  ``<home>/ticks/raw/{trading_day}/sina/part-*.parquet`` (raw responses under ``ticks/wire``).
  Runs until Ctrl-C, ``--until`` or ``--duration-s`` and sleeps outside the session windows of
  the selected products. Built for unattended multi-day runs: 5 s shards, keep-awake, flush on
  Ctrl-C / Ctrl-Break / console close, single-instance lock, spill directories;
  ``scripts/record_forever.ps1`` restarts it after a crash;
- ``sina-probe``: fetch once and print the parsed quotes (``--save`` keeps the raw response);
- ``summary``: per-instrument row counts by mode for recorded days.

Exit codes of ``record``: 0 finished (``--until`` / ``--duration-s``), 130 stopped by Ctrl-C /
Ctrl-Break, 3 another recorder holds the lock, 4 the final flush failed (rows lost), 1 crash.
All times shown are Beijing time.
"""

from __future__ import annotations

import asyncio
import contextlib
import queue
import sys
import tempfile
import threading
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Final, Protocol

import httpx
import typer
from loguru import logger
from rich.console import Console
from rich.table import Table

from futrisk.cli.lifecycle import (
    ConsoleCloseGuard,
    InstanceLock,
    LockHeldError,
    install_stop_signals,
    keep_awake,
)
from futrisk.common.calendar import TradingCalendar
from futrisk.common.clock import BJ, NS_PER_MS, NS_PER_S, Clock, SystemClock, bj_to_ns, ns_to_bj
from futrisk.common.config import FullConfig, load_default_config
from futrisk.common.logging import setup_logging
from futrisk.common.paths import HomeLayout, resolve_home
from futrisk.messages.codes import MD_REJECT_MALFORMED
from futrisk.messages.enums import MdEventKind, SourceId, SourceState, TickMode
from futrisk.messages.md import FLAG_AUCTION, FLAG_LIMIT_DOWN_LOCKED, FLAG_LIMIT_UP_LOCKED, Tick
from futrisk.services.md_gateway.arbiter import Arbiter
from futrisk.services.md_gateway.model import Incoming
from futrisk.services.md_gateway.normalizer import InstrumentMeta, Normalizer
from futrisk.services.md_gateway.params import MdParams
from futrisk.services.md_gateway.pipeline import SKIPPED, MdPipeline
from futrisk.services.md_gateway.recording import TickRecorder
from futrisk.services.md_gateway.seqtracker import Hole
from futrisk.services.md_gateway.sessions import SessionIndex
from futrisk.services.md_gateway.shards import (
    DEFAULT_ROLL_S,
    TICK_PREFIX,
    WIRE_PREFIX,
    list_shards,
    list_trading_days,
    read_raw_ticks,
    reject_row,
    wire_row,
)
from futrisk.services.md_gateway.sina import (
    DEFAULT_FUTURE_TOLERANCE_S,
    SinaDecoder,
    SinaFormatError,
    parse_quote,
    resolve_sina_instruments,
)
from futrisk.services.md_gateway.sources import PollResult, PollTransport, sina_source
from futrisk.services.md_gateway.validator import Validator

app = typer.Typer(help="Real market data: record Sina ticks, probe, inspect", no_args_is_help=True)

INSTANCE = "data-recorder"
SINA = SourceId.SINA
SINA_DIR = "sina"
IDLE_STATUS_EVERY = 10  # while idle, print only every N-th status interval
HANDOFF_S: Final = 0.5  # hand buffered rows to the recorder thread (and close due shards) this often
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
    """Fallback roots used when the primary tick / wire directories cannot be written."""
    return [layout.home / "ticks" / "spill", Path(tempfile.gettempdir()) / "futrisk-spill"]


def _make_client(trust_env: bool) -> httpx.AsyncClient:
    """HTTP client for the poller; ``trust_env`` is already resolved (flag, else ``SinaConfig``). Tests patch this."""
    return httpx.AsyncClient(trust_env=trust_env, follow_redirects=False, limits=httpx.Limits(max_connections=2, max_keepalive_connections=1))


def effective_trust_env(config: FullConfig, flag: bool | None) -> bool:
    """``--trust-env/--no-trust-env`` when given, else ``feeds.yaml`` ``sina.trust_env``."""
    return config.feeds.sina.trust_env if flag is None else flag


def _parse_until(value: str | None) -> int | None:
    """``YYYY-MM-DD HH:MM[:SS]`` (Beijing time) -> UTC ns."""
    if not value:
        return None
    text = value.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        with contextlib.suppress(ValueError):
            return bj_to_ns(datetime.strptime(text, fmt).replace(tzinfo=BJ))
    raise typer.BadParameter(f"expected 'YYYY-MM-DD HH:MM' (Beijing time), got {value!r}", param_hint="--until")


def _instruments(config: FullConfig, symbols: str | None) -> dict[str, InstrumentMeta]:
    """``{sina_code: meta}`` of ``--symbols`` (any case) or every ``feeds.yaml`` Sina symbol."""
    wanted = [s.strip() for s in symbols.split(",") if s.strip()] if symbols else None
    try:
        metas = resolve_sina_instruments(config.products, config.feeds.sina.symbols, wanted)
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--symbols") from exc
    if not metas:
        raise typer.BadParameter("no symbols selected (feeds.yaml sina.symbols is empty)", param_hint="--symbols")
    return metas


def _fmt_ns(ns: int | None, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return "-" if ns is None else ns_to_bj(ns).strftime(fmt)


def _fmt_px(x: float | None) -> str:
    return "-" if x is None else f"{x:.3f}".rstrip("0").rstrip(".")


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


class Printer(Protocol):
    def print(self, *objects: Any, **kwargs: Any) -> None: ...


class ConsolePrinter:
    """Prints rich renderables from a daemon thread fed by a bounded queue.

    A console that blocks (e.g. text selected in the legacy Windows console) then only stalls
    this thread, never the polling event loop; when the queue is full, status messages are
    dropped (and counted) instead of blocking.
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
# the recorder: md-gateway's pipeline without the bus
# ---------------------------------------------------------------------------
class PollGate:
    """Poll only inside a session window of the selected products, widened by margins (02 §9.3)."""

    def __init__(self, calendar: TradingCalendar, templates: Iterable[str], *, pre_margin_s: float = 180.0, post_margin_s: float = 300.0) -> None:
        self._cal = calendar
        self._templates = list(dict.fromkeys(templates))
        if not self._templates:
            raise ValueError("PollGate needs at least one session template")
        self._pre_ns = int(pre_margin_s * NS_PER_S)
        self._post_ns = int(post_margin_s * NS_PER_S)

    def spans(self, now_ns: int) -> list[tuple[int, int]]:
        """Widened, merged ``(start_ns, end_ns)`` spans of the previous, current and next trading day."""
        cal = self._cal
        try:
            td = cal.current_or_next_trading_day(now_ns)
        except LookupError:
            return []
        days = [td]
        with contextlib.suppress(LookupError):
            days.insert(0, cal.prev_trading_day(td))
        with contextlib.suppress(LookupError):
            days.append(cal.next_trading_day(td))
        merged: list[tuple[int, int]] = []
        for start, end in sorted((w.start_ns - self._pre_ns, w.end_ns + self._post_ns) for d in days for w in cal.windows(self._templates, d)):
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))
        return merged

    def is_active(self, now_ns: int) -> bool:
        return any(start <= now_ns < end for start, end in self.spans(now_ns))

    def next_active_ns(self, now_ns: int) -> int | None:
        """``now_ns`` when active, else the start of the next widened span (``None`` past the calendar)."""
        future = []
        for start, end in self.spans(now_ns):
            if start <= now_ns < end:
                return now_ns
            if start > now_ns:
                future.append(start)
        if future:
            return min(future)
        w = self._cal.next_window_start(self._templates, now_ns)
        return None if w is None else w.start_ns - self._pre_ns


class _Trusted:
    """The recorder's only source is always trusted (the pipeline's trust view)."""

    def state(self, source: SourceId) -> SourceState:
        return SourceState.TRUSTED

    def on_outcome(self, source: SourceId, rejected: bool, instrument_id: str = "") -> None:
        pass

    def on_vote(self, source: SourceId, detail: dict[str, Any] | None = None) -> None:
        pass


@dataclass
class SymbolStats:
    instrument_id: str
    code: str
    counts: dict[str, int] = field(default_factory=dict)
    last_reason: str | None = None
    last_detail: str | None = None
    last_tick: Tick | None = None
    last_lag_ms: float | None = None

    def count(self, mode: TickMode | str) -> int:
        return self.counts.get(str(mode), 0)


@dataclass
class RecordStats:
    polls: int = 0
    failed_polls: int = 0
    rows: int = 0
    bucket_td: str | None = None
    symbols: dict[str, SymbolStats] = field(default_factory=dict)


class _Recorder:
    """SourceSink + pipeline effects: every received state goes through the validator / arbiter and into the shards."""

    def __init__(self, decoder: SinaDecoder, normalizer: Normalizer, recorder: TickRecorder, params: MdParams, clock: Clock, *, wire: bool) -> None:
        self.decoder = decoder
        self.recorder = recorder
        self.sessions = normalizer.sessions
        self.wire = wire
        self.stats = RecordStats(symbols={code: SymbolStats(m.instrument_id, code) for code, m in decoder.instruments.items()})
        self._code_of = {m.instrument_id: code for code, m in decoder.instruments.items()}
        validator = Validator(self.sessions, params, normalizer.known_meta)
        self.pipeline = MdPipeline(params, validator, Arbiter(params), _Trusted(), self, mono_ns=clock.monotonic_ns)

    def _symbol(self, inc: Incoming) -> SymbolStats | None:
        iid = inc.tick.instrument_id if inc.tick is not None else inc.instrument_id
        return self.stats.symbols.get(self._code_of.get(iid, ""))

    # ------------------------------------------------------------ SourceSink
    def on_incoming(self, inc: Incoming) -> None:
        mode = self.pipeline.process(inc)
        sym = self._symbol(inc)
        if sym is not None:
            key = SKIPPED if mode is None else str(mode)
            sym.counts[key] = sym.counts.get(key, 0) + 1
            if inc.tick is not None:
                sym.last_tick = inc.tick
                sym.last_lag_ms = (inc.recv_ns - inc.tick.exch_ts_ns) / 1e6

    def on_poll(self, source: SourceId, ok: bool, *, error: str | None = None, lag_ms: float | None = None, instruments: Iterable[str] = ()) -> None:
        self.stats.polls += 1
        if not ok:
            self.stats.failed_polls += 1

    def on_result(self, res: PollResult) -> None:
        """Raw archive of one poll (``--wire``)."""
        if self.wire:
            row = wire_row(
                recv_ns=res.recv_ns, sent_ns=res.sent_ns, poll_seq=res.seq, elapsed_ms=res.elapsed_ms, http_status=res.status, error=res.error,
                codes=",".join(self.decoder.codes), body=res.body,
            )
            self.recorder.add_wire(SINA, self.sessions.current_or_next(res.recv_ns), row)

    def on_connected(self, source: SourceId, *, epoch: str | None = None) -> None:
        pass

    def on_disconnected(self, source: SourceId, reason: str) -> None:
        pass

    def on_frame(self, source: SourceId, *, lag_ms: float | None = None) -> None:
        pass

    def on_gap(self, source: SourceId, hole: Hole, event: str) -> None:
        pass

    def on_epoch_change(self, source: SourceId, old: str | None, new: str) -> None:
        pass

    def on_resume_finished(self, source: SourceId) -> None:
        pass

    # ------------------------------------------------------- pipeline effects
    def publish_tick(self, tick: Tick, recv_mono_ns: int) -> None:
        pass

    def record(self, inc: Incoming, mode: TickMode, reason: str | None) -> None:
        bucket = self.sessions.current_or_next(inc.recv_ns)
        self.stats.bucket_td = bucket
        self.stats.rows += 1
        if inc.tick is not None:
            self.recorder.add_tick(SINA, bucket, inc.tick, mode, reason, None)
        else:
            row = reject_row(
                instrument_id=inc.instrument_id or "?", exchange=inc.exchange, trading_day=bucket, recv_ns=inc.recv_ns, source=str(SINA),
                reject_reason=reason or MD_REJECT_MALFORMED,
            )
            self.recorder.add(SINA, bucket, row)
        sym = self._symbol(inc)
        if sym is not None and reason:
            sym.last_reason = reason
            sym.last_detail = inc.detail or None

    def emit(self, kind: MdEventKind, **kw: Any) -> None:
        logger.info("recorder.event", kind=str(kind), **{k: str(v) for k, v in kw.items() if k != "detail"})


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
    stats: RecordStats = field(default_factory=RecordStats)
    pending_rows: int = 0


class _Status:
    def __init__(self, printer: Printer, rec: _Recorder, transport: PollTransport, clock: Clock, status_s: float) -> None:
        self.printer = printer
        self.rec = rec
        self.transport = transport
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
        st, ps, rec = self.rec.stats, self.transport.stats, self.rec.recorder
        totals = {str(m): sum(s.count(m) for s in st.symbols.values()) for m in TickMode}
        skipped = sum(s.count(SKIPPED) for s in st.symbols.values())
        state = self.state if self.state != "idle" else f"idle until {_fmt_ns(self.next_active_ns, '%m-%d %H:%M')}"
        alarms = ""
        if rec.write_errors:
            alarms += f" | [red]write errors {rec.write_errors}[/]"
        if rec.spilled_shards:
            alarms += f" | [red]spilled shards {rec.spilled_shards} (last {rec.last_spill})[/]"
        if rec.dropped_rows:
            alarms += f" | [red bold]DROPPED ROWS {rec.dropped_rows}[/]"
        return (
            f"[bold]{_fmt_ns(self.clock.now_ns(), '%H:%M:%S')}[/] TD {st.bucket_td or '-'} {state} | "
            f"polls {ps.polls} ok {ps.ok} fail {ps.failed} skip {ps.skipped} | "
            f"rows {st.rows} shards {rec.shards_written} pending {rec.buffered_rows()} | "
            f"live {totals['LIVE']} bf {totals['BACKFILL']} dup {totals['DUPLICATE']} rej {totals['REJECTED']} skipped {skipped}{alarms}"
        )

    def emit(self, table: bool) -> None:
        self.printer.print(self.headline(), highlight=False)
        if table:
            self.printer.print(symbol_table(self.rec.stats.symbols.values()))
        st, ps, rec = self.rec.stats, self.transport.stats, self.rec.recorder
        logger.bind(trading_day=st.bucket_td).info(
            "recorder.status", state=self.state, polls=ps.polls, failed=ps.failed, skipped=ps.skipped, rows=st.rows, shards=rec.shards_written,
            write_errors=rec.write_errors, spilled_shards=rec.spilled_shards, dropped_rows=rec.dropped_rows,
            counts={s.instrument_id: s.counts for s in st.symbols.values()},
        )


def symbol_table(symbols: Iterable[SymbolStats]) -> Table:
    """Per-symbol status: last price, volume, lag and row counts by mode."""
    table = Table(box=None, pad_edge=False, show_edge=False, header_style="bold")
    for col in ("instrument", "last", "volume", "bid / ask", "exch time", "lag s", "LIVE", "BF", "DUP", "REJ", "SKIP", "last reject"):
        table.add_column(col, justify="left" if col in ("instrument", "bid / ask", "last reject") else "right")
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
            str(s.count(SKIPPED)),
            reject,
        )
    return table


async def _handoff_loop(recorder: TickRecorder) -> None:
    """Hand rows to the recorder thread every 0.5 s (closing due shards), so a kill loses at most ~roll_s of rows."""
    while True:
        await asyncio.sleep(HANDOFF_S)
        await recorder.handoff({})


async def _wait(stop: asyncio.Event, timeout_s: float) -> None:
    if timeout_s > 0:
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

    ``clock`` drives sessions, receive times and the deadline (tests inject a shifted clock to
    place the run inside or outside a session); ``install_handlers`` installs the Ctrl-C /
    Ctrl-Break stop handlers and the console-close flush guard (main thread only).
    """
    outcome = outcome or RecordOutcome()
    clock = clock or SystemClock()
    config = config or load_default_config()
    layout = HomeLayout(resolve_home(opts.home))
    sina_cfg = config.feeds.sina
    instruments = _instruments(config, opts.symbols)
    normalizer = Normalizer(config.products, SessionIndex(config.calendar), future_tolerance_s=opts.future_tolerance_s)
    decoder = SinaDecoder(normalizer, instruments)
    recorder = TickRecorder(
        layout.ticks_raw_dir, sources=[SINA], wall_clock=clock, epoch_ms=clock.now_ns() // NS_PER_MS, roll_s=opts.roll_s, roll_rows=opts.roll_rows,
        spill_dirs=spill_dirs(layout), wire_dir=layout.ticks_wire_dir if opts.wire else None,
    )
    rec = _Recorder(decoder, normalizer, recorder, MdParams(close_grace_s=opts.close_grace_s), clock, wire=opts.wire)
    cfg = sina_cfg.model_copy(
        update={"poll_ms": opts.poll_ms or sina_cfg.poll_ms, "timeout_ms": opts.timeout_ms or sina_cfg.timeout_ms, "trust_env": effective_trust_env(config, opts.trust_env)}
    )
    transport = sina_source(cfg, decoder, rec, clock, client=client, on_result=rec.on_result)
    gate = PollGate(config.calendar, [t for m in instruments.values() for t in m.sessions], pre_margin_s=opts.pre_margin_s, post_margin_s=opts.post_margin_s)
    status = _Status(printer, rec, transport, clock, opts.status_s)
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

    restore_signals = None
    guard = ConsoleCloseGuard(recorder.emergency_flush)
    tasks: list[asyncio.Task[None]] = []
    outcome.reason = "cancelled"
    outcome.stats = rec.stats
    try:
        if install_handlers:
            restore_signals = install_stop_signals(asyncio.get_running_loop(), stop.set)
            guard.install()
        printer.print(
            f"[bold]futrisk data record[/] home={layout.home}\n"
            f"  symbols={','.join(m.instrument_id for m in instruments.values())}\n"
            f"  poll every {transport.poll_s:.1f}s; a shard every {opts.roll_s:g}s or {opts.roll_rows} rows -> {layout.ticks_raw_dir}"
            + (f" (+ raw responses in {layout.ticks_wire_dir})" if opts.wire else "")
            + f"\n  now {_fmt_ns(clock.now_ns())} Beijing"
            + (f"; stops at {_fmt_ns(until_ns)} Beijing" if until_ns else "")
            + f"; logs in {layout.logs_dir}; Ctrl-C to stop",
            highlight=False,
        )
        logger.info("recorder.start", home=str(layout.home), codes=decoder.codes)
        tasks = [asyncio.create_task(status.loop()), asyncio.create_task(_handoff_loop(recorder))]
        while (why := finished()) is None:
            if in_window():
                status.state = "polling"
                printer.print(f"{_fmt_ns(clock.now_ns())} polling (session window open)", highlight=False)
                await transport.run(stop=stop, keep_running=lambda: finished() is None and in_window())
                await recorder.handoff({}, flush=True)
                continue
            status.state = "idle"
            status.next_active_ns = gate.next_active_ns(clock.now_ns())
            printer.print(
                f"{_fmt_ns(clock.now_ns())} idle: no session open; next polling window starts {_fmt_ns(status.next_active_ns)} (Beijing)",
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
        outcome.reason = why
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        outcome.flushed = await recorder.close({})  # nothing may stand between the buffers and the disk
        outcome.pending_rows = recorder.buffered_rows()
        if restore_signals is not None:
            restore_signals()
        guard.uninstall()
        await transport.aclose()
        status.state = "stopped"
        status.emit(table=True)
        logger.info(
            "recorder.stop", reason=outcome.reason, flushed=outcome.flushed, rows=rec.stats.rows, shards=recorder.shards_written,
            write_errors=recorder.write_errors, spilled_shards=recorder.spilled_shards, dropped_rows=recorder.dropped_rows,
        )
    return outcome


@app.command(help=f"Record real Sina ticks into <home>/ticks/raw/<trading_day>/sina. {RECORD_TIP}")
def record(
    home: str | None = typer.Option(None, "--home", help="Runtime home (default: FUTRISK_HOME or <repo>/var)"),
    symbols: str | None = typer.Option(None, "--symbols", help="Comma-separated contracts, e.g. rb2701,IF2612 (default: every Sina symbol in feeds.yaml)"),
    poll_ms: int | None = typer.Option(None, "--poll-ms", help="Polling interval in ms, >= 1000 (default: feeds.yaml, 1500)"),
    timeout_ms: int | None = typer.Option(None, "--timeout-ms", help="HTTP read timeout in ms (default: feeds.yaml)"),
    duration_s: float | None = typer.Option(None, "--duration-s", help="Stop after N seconds"),
    until: str | None = typer.Option(None, "--until", help="Stop at this Beijing time, 'YYYY-MM-DD HH:MM' (default: run until Ctrl-C)"),
    ignore_sessions: bool = typer.Option(False, "--ignore-sessions", help="Poll continuously, also outside sessions"),
    roll_s: float = typer.Option(DEFAULT_ROLL_S, "--roll-s", help="Close a shard after this many seconds (bounds the loss on a kill)"),
    roll_rows: int = typer.Option(10_000, "--roll-rows", help="... or after this many rows"),
    status_s: float = typer.Option(60.0, "--status-s", help="Seconds between status prints"),
    pre_margin_s: float = typer.Option(180.0, "--pre-margin-s", help="Start polling this long before a session opens"),
    post_margin_s: float = typer.Option(300.0, "--post-margin-s", help="Keep polling this long after a session closes"),
    close_grace_s: float = typer.Option(300.0, "--close-grace-s", help="Closing snapshots stamped this long after a window end are still in session"),
    future_tolerance_s: float = typer.Option(DEFAULT_FUTURE_TOLERANCE_S, "--future-tolerance-s", help="Reject quotes stamped more than this many seconds after their receive time"),
    wire: bool = typer.Option(True, "--wire/--no-wire", help="Also archive the raw responses under <home>/ticks/wire"),
    keep_awake_flag: bool = typer.Option(True, "--keep-awake/--no-keep-awake", help="Ask Windows not to idle-sleep during the run"),
    trust_env: bool | None = typer.Option(None, "--trust-env/--no-trust-env", help="Honour HTTP(S)_PROXY (default: feeds.yaml sina.trust_env)"),
) -> None:
    opts = RecordOptions(
        home=home, symbols=symbols, poll_ms=poll_ms, timeout_ms=timeout_ms, duration_s=duration_s, until=until, ignore_sessions=ignore_sessions,
        roll_s=roll_s, roll_rows=roll_rows, status_s=status_s, pre_margin_s=pre_margin_s, post_margin_s=post_margin_s, close_grace_s=close_grace_s,
        future_tolerance_s=future_tolerance_s, wire=wire, keep_awake=keep_awake_flag, trust_env=trust_env,
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
                    "keep-awake " + ("on" if awake else "[red]NOT available[/]")
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
        console.print(f"stopped ({outcome.reason}): all buffered rows were written -- {st.rows} rows, {st.polls} polls", highlight=False)
    elif outcome.flushed is False:
        console.print(
            f"[red bold]FINAL FLUSH FAILED: {outcome.pending_rows} buffered rows could not be written to {layout.ticks_raw_dir} nor to any spill directory -- see {layout.logs_dir}[/]"
        )
        raise typer.Exit(EXIT_FLUSH_FAILED)
    else:
        console.print("[red bold]the recorder did not shut down cleanly; rows buffered since the last shard may be lost[/]")
        raise typer.Exit(EXIT_FLUSH_FAILED)
    if outcome.reason == "signal":
        raise typer.Exit(EXIT_USER_STOP)


# ---------------------------------------------------------------------------
# sina-probe
# ---------------------------------------------------------------------------
class _NoSink:
    """``SourceSink`` of a one-shot fetch (nothing is dispatched)."""

    def on_incoming(self, inc: Incoming) -> None:
        pass

    def on_connected(self, source: SourceId, *, epoch: str | None = None) -> None:
        pass

    def on_disconnected(self, source: SourceId, reason: str) -> None:
        pass

    def on_poll(self, source: SourceId, ok: bool, *, error: str | None = None, lag_ms: float | None = None, instruments: Iterable[str] = ()) -> None:
        pass

    def on_frame(self, source: SourceId, *, lag_ms: float | None = None) -> None:
        pass

    def on_gap(self, source: SourceId, hole: Hole, event: str) -> None:
        pass

    def on_epoch_change(self, source: SourceId, old: str | None, new: str) -> None:
        pass

    def on_resume_finished(self, source: SourceId) -> None:
        pass


async def _fetch_once(config: FullConfig, decoder: SinaDecoder, timeout_ms: int, trust_env: bool) -> PollResult:
    client = _make_client(trust_env)
    cfg = config.feeds.sina.model_copy(update={"timeout_ms": timeout_ms, "trust_env": trust_env})
    transport = sina_source(cfg, decoder, _NoSink(), SystemClock(), client=client)
    try:
        return await transport.fetch()
    finally:
        await client.aclose()


@app.command("sina-probe")
def sina_probe(
    symbols: str | None = typer.Option(None, "--symbols", help="Comma-separated contracts (default: feeds.yaml)"),
    raw: bool = typer.Option(False, "--raw", help="Also print the raw response lines"),
    save: Path | None = typer.Option(None, "--save", help="Write the raw response bytes (GB18030) to this file"),
    timeout_ms: int = typer.Option(5000, "--timeout-ms", help="HTTP timeout in ms"),
    trust_env: bool | None = typer.Option(None, "--trust-env/--no-trust-env", help="Honour HTTP(S)_PROXY (default: feeds.yaml sina.trust_env)"),
) -> None:
    """Fetch Sina once and print the parsed quotes (connectivity and format check)."""
    from futrisk.services.md_gateway.sina import decode_body, split_response

    console = Console(highlight=False)
    _setup_logs(None)
    config = load_default_config()
    instruments = _instruments(config, symbols)
    sessions = SessionIndex(config.calendar)
    decoder = SinaDecoder(Normalizer(config.products, sessions), instruments)
    res = asyncio.run(_fetch_once(config, decoder, timeout_ms, effective_trust_env(config, trust_env)))
    console.print(f"GET {config.feeds.sina.url}{','.join(decoder.codes)}", highlight=False)
    console.print(f"status {res.status} in {res.elapsed_ms:.0f} ms, {len(res.body or b'')} bytes, received {_fmt_ns(res.recv_ns)} (Beijing)", highlight=False)
    if save is not None and res.body is not None:
        save.parent.mkdir(parents=True, exist_ok=True)
        save.write_bytes(res.body)
        console.print(f"raw response saved to {save}", highlight=False)
    if not res.ok:
        console.print(f"[red]poll failed: {res.error}[/]")
        raise typer.Exit(1)
    text = decode_body(res.body or b"")
    if raw:
        for line in text.splitlines():
            console.print(line, highlight=False, markup=False, soft_wrap=True)
    payloads = split_response(text).payloads
    table = Table(title=f"Sina quotes (current trading day {sessions.current_or_next(res.recv_ns)})", header_style="bold")
    for col in ("instrument", "code", "name", "exch time", "TD", "last", "bid x vol", "ask x vol", "volume", "OI", "turnover", "pre_settle", "limits", "flags", "verdict"):
        table.add_column(col, overflow="fold")
    bad = 0
    for code, meta in instruments.items():
        inc = decoder.incoming(code, payloads.get(code), res.recv_ns, 0)
        name = ""
        with contextlib.suppress(SinaFormatError):
            name = parse_quote(code, payloads.get(code) or "").name
        t = inc.tick
        verdict = "ok" if inc.reject_reason is None else f"{inc.reject_reason} ({inc.detail.split(':')[0]})"
        if t is None:
            bad += 1
            table.add_row(meta.instrument_id, code, name, *([""] * 11), f"[red]{verdict}: {inc.detail}[/]")
            continue
        table.add_row(
            t.instrument_id, code, name, _fmt_ns(t.exch_ts_ns), t.trading_day, _fmt_px(t.last), f"{_fmt_px(t.bid1)} x {t.bid_vol1}",
            f"{_fmt_px(t.ask1)} x {t.ask_vol1}", str(t.volume), str(t.open_interest), f"{t.turnover:,.0f}", _fmt_px(t.pre_settle),
            f"{_fmt_px(t.lower_limit)}-{_fmt_px(t.upper_limit)}", _flags_str(t.flags), verdict,
        )
    console.print(table)
    if bad:
        raise typer.Exit(2)


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------
def _spilled_files(layout: HomeLayout) -> list[Path]:
    return [p for root in spill_dirs(layout) if root.is_dir() for p in sorted(root.glob(f"*/{SINA_DIR}/*.parquet"))]


@app.command()
def summary(
    home: str | None = typer.Option(None, "--home", help="Runtime home (default: FUTRISK_HOME or <repo>/var)"),
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
    days = [td] if td else list_trading_days(layout.ticks_raw_dir, SINA_DIR)
    if not days:
        console.print(f"no recorded Sina ticks under {layout.ticks_raw_dir}")
        raise typer.Exit(1)
    for day in days:
        df = read_raw_ticks(layout.ticks_raw_dir, day, SINA_DIR)
        n_shards = len(list_shards(layout.ticks_raw_dir, day, SINA_DIR, TICK_PREFIX))
        n_wire = len(list_shards(layout.ticks_wire_dir, day, SINA_DIR, WIRE_PREFIX))
        table = Table(title=f"{day}: {len(df)} rows, {n_shards} tick shards, {n_wire} wire shards", header_style="bold")
        for col in ("instrument", "LIVE", "BACKFILL", "DUPLICATE", "REJECTED", "first accepted", "last accepted", "last", "max volume"):
            table.add_column(col, justify="left" if col == "instrument" else "right")
        if len(df):
            df["instrument_id"] = df["instrument_id"].astype(str)
            df["mode"] = df["mode"].astype(str)
            for iid, g in df.groupby("instrument_id", sort=True):
                counts = g["mode"].value_counts()
                acc = g[g["mode"].isin([str(TickMode.LIVE), str(TickMode.BACKFILL)])]
                table.add_row(
                    str(iid),
                    *(str(int(counts.get(str(m), 0))) for m in (TickMode.LIVE, TickMode.BACKFILL, TickMode.DUPLICATE, TickMode.REJECTED)),
                    _fmt_ns(int(acc["exch_ts_ns"].min()), "%m-%d %H:%M:%S") if len(acc) else "-",
                    _fmt_ns(int(acc["exch_ts_ns"].max()), "%m-%d %H:%M:%S") if len(acc) else "-",
                    _fmt_px(float(acc["last"].iloc[-1])) if len(acc) else "-",
                    str(int(acc["volume"].max())) if len(acc) else "-",
                )
        console.print(table)
