"""ClockKeeper: the market clock owned by the supervisor (01 §8.2, ADR-017; 02 §6.6, §8.8; 03 §4.6, §7).

* **demo mode** (``app.mode=demo``): market time = wall clock + ``offset_ns``. On the very
  first start the offset puts market time at ``app.demo.start_at`` (ISO, Beijing time when
  naive); ``None`` means the next trading day at 08:58, just before the open / opening
  auction. The state is persisted in ``clock_state`` (critical write) and reused on every
  later start, so market time keeps running with the wall clock and never goes backwards.
* **prod mode**: ``REAL``, offset 0 (market time is the wall clock).
* ``sys.clock`` (LV ``ClockState``) is published on every change and every 5 s; services
  fetch the latest value with ``ctl.snapshot`` at start-up (02 §5, §8.6).
* ``svc.clock`` (demo only; the bus whitelist admits only api-gateway): targets
  ``PRE_CLOSE`` (``demo.pre_close_at`` of the current trading day), ``NEXT_OPEN`` (start of
  the next session window over the union of all product session templates, auctions
  included, minus ``demo.lead_s``) and ``AT`` (``at_ns``). Strictly forward (``CONFLICT``
  otherwise); version + 1; ``processed_commands`` + ``clock_state`` in one critical
  transaction, then the state is applied to the supervisor's own market clock and
  published. The pause / clock / resume choreography with sim-exchange is api-gateway's
  job (02 §8.8); sim-exchange gets the current offset on its command line at spawn.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, time

from loguru import logger
from sqlalchemy import Connection, insert, select, update

from futrisk.common.calendar import TradingCalendar
from futrisk.common.clock import BJ, NS_PER_S, Clock, MarketClock, bj_to_ns, ns_to_bj, td_str
from futrisk.common.config.app import AppConfig, DemoConfig
from futrisk.common.config.products import ProductsConfig
from futrisk.db import Database, ProcessedCommands, request_hash
from futrisk.messages import codes
from futrisk.messages.enums import ClockMode, ClockTarget
from futrisk.messages.envelope import Reply, err_reply, ok_reply
from futrisk.messages.system import ClockJumpRequest, ClockState
from futrisk.services.supervisor.schema import clock_state

DEMO_START_TIME = time(8, 58)
PUBLISH_INTERVAL_S = 5.0
SVC_CLOCK = "svc.clock"


class ClockJumpError(Exception):
    """A ``svc.clock`` target that cannot be reached; ``code`` is a 02 §11.1 code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


# ------------------------------------------------------------------ pure helpers
def fmt_ns(ns: int) -> str:
    """Beijing ISO time of an epoch-ns value (logs and messages)."""
    return ns_to_bj(ns).isoformat(timespec="seconds")


def parse_hhmm(text: str) -> time:
    """``"14:58"`` or ``"14:58:30"`` -> time."""
    parts = [int(p) for p in text.strip().split(":")]
    if len(parts) not in (2, 3):
        raise ValueError(f"expected HH:MM[:SS], got {text!r}")
    return time(parts[0], parts[1], parts[2] if len(parts) == 3 else 0)


def parse_market_time(text: str) -> datetime:
    """ISO date-time; naive values are Beijing time (``app.demo.start_at``)."""
    dt = datetime.fromisoformat(text.strip())
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=BJ)


def market_trading_day(calendar: TradingCalendar, market_ns: int) -> str | None:
    """Trading day ``YYYYMMDD`` of a market time (non-trading daytime: the next one)."""
    try:
        return td_str(calendar.current_or_next_trading_day(market_ns))
    except LookupError:
        return None


def session_templates(products: ProductsConfig) -> list[str]:
    """Union of the session templates used by the configured products (all when none)."""
    used = sorted({s for spec in products.products.values() for s in spec.sessions})
    return used or sorted(products.session_templates)


def next_demo_start_ns(calendar: TradingCalendar, wall_now_ns: int, at: time = DEMO_START_TIME) -> int:
    """The next trading-day ``at`` (08:58) strictly after ``wall_now_ns`` (01 §8.2)."""
    now = ns_to_bj(wall_now_ns)
    d = now.date()
    if not calendar.is_trading_day(d):
        d = calendar.next_trading_day(d)
    while True:
        candidate = datetime.combine(d, at, tzinfo=BJ)
        if candidate > now:
            return bj_to_ns(candidate)
        d = calendar.next_trading_day(d)  # LookupError past the end of the calendar


def initial_market_ns(calendar: TradingCalendar, start_at: str | None, wall_now_ns: int) -> int:
    """Market time of the first demo start: ``start_at`` or the next trading day's 08:58."""
    if start_at:
        return bj_to_ns(parse_market_time(start_at))
    return next_demo_start_ns(calendar, wall_now_ns)


def pre_close_target_ns(calendar: TradingCalendar, pre_close_at: str, market_now_ns: int) -> int:
    """``pre_close_at`` (14:58) on the trading day of the current market time.

    During a night session that is the next day's pre-close (the night belongs to it).
    """
    td = calendar.current_or_next_trading_day(market_now_ns)
    return bj_to_ns(datetime.combine(td, parse_hhmm(pre_close_at), tzinfo=BJ))


def next_open_target_ns(calendar: TradingCalendar, templates: Sequence[str], lead_s: int, market_now_ns: int) -> int:
    """Start of the next session window (auctions included) minus ``lead_s``."""
    window = calendar.next_window_start(list(templates), market_now_ns, include_auction=True)
    if window is None:
        raise ClockJumpError(codes.NOT_FOUND, "the trading calendar has no upcoming session")
    return window.start_ns - int(lead_s) * NS_PER_S


def jump_target_ns(
    target: ClockTarget | str,
    *,
    calendar: TradingCalendar,
    demo: DemoConfig,
    templates: Sequence[str],
    market_now_ns: int,
    at_ns: int | None = None,
) -> int:
    """Market time a ``svc.clock`` request moves to; raises :class:`ClockJumpError`.

    The result is strictly after ``market_now_ns`` (the market clock only moves forward).
    """
    kind = str(target)
    try:
        if kind == ClockTarget.PRE_CLOSE:
            t = pre_close_target_ns(calendar, demo.pre_close_at, market_now_ns)
        elif kind == ClockTarget.NEXT_OPEN:
            t = next_open_target_ns(calendar, templates, demo.lead_s, market_now_ns)
        elif kind == ClockTarget.AT:
            if at_ns is None:
                raise ClockJumpError(codes.BAD_REQUEST, "target AT needs at_ns")
            t = int(at_ns)
        else:
            raise ClockJumpError(codes.BAD_REQUEST, f"unknown clock target {kind!r}")
    except LookupError as exc:
        raise ClockJumpError(codes.NOT_FOUND, f"trading calendar: {exc}") from exc
    if t <= market_now_ns:
        raise ClockJumpError(
            codes.CONFLICT,
            f"{kind} target {fmt_ns(t)} is not after the current market time {fmt_ns(market_now_ns)}; "
            "the market clock only moves forward",
        )
    return t


# ------------------------------------------------------------------ persistence
@dataclass(frozen=True)
class ClockRecord:
    """The ``clock_state`` row."""

    mode: ClockMode
    offset_ns: int
    version: int
    updated_ns: int
    actor: str | None = None
    reason: str | None = None

    def same_clock(self, other: ClockRecord | None) -> bool:
        return other is not None and (self.mode, self.offset_ns, self.version) == (other.mode, other.offset_ns, other.version)


def read_clock_state(conn: Connection) -> ClockRecord | None:
    m = conn.execute(select(clock_state).where(clock_state.c.id == 1)).mappings().first()
    if m is None:
        return None
    return ClockRecord(
        mode=ClockMode(str(m["mode"])),
        offset_ns=int(m["offset_ns"]),
        version=int(m["version"]),
        updated_ns=int(m["updated_ns"]),
        actor=m["actor"],
        reason=m["reason"],
    )


def write_clock_state(conn: Connection, rec: ClockRecord) -> None:
    """Upsert the single ``clock_state`` row (inside a writer job)."""
    values = {
        "mode": str(rec.mode),
        "offset_ns": rec.offset_ns,
        "version": rec.version,
        "updated_ns": rec.updated_ns,
        "actor": rec.actor,
        "reason": rec.reason,
    }
    res = conn.execute(update(clock_state).where(clock_state.c.id == 1).values(**values))
    if not res.rowcount:
        conn.execute(insert(clock_state).values(id=1, **values))


# ------------------------------------------------------------------ keeper
class ClockKeeper:
    """Owns ``clock_state``, the supervisor's :class:`MarketClock` and ``sys.clock``.

    Args:
        db: supervisor.db (``clock_state`` and ``processed_commands``).
        app: ``app.yaml`` (mode, demo settings).
        calendar: trading calendar with the product session templates.
        templates: session templates for ``NEXT_OPEN`` (:func:`session_templates`).
        market_clock: the runtime's market clock; every state is applied to it.
        wall: wall clock (default: ``market_clock.wall``).
        publish: ``publish(ClockState)`` - the runtime's LV publish of ``sys.clock``.
        start_at_override: ``futrisk up --start-at``: initial market time on the first
            start; later it only moves the persisted clock forward.
    """

    def __init__(
        self,
        *,
        db: Database,
        app: AppConfig,
        calendar: TradingCalendar,
        templates: Sequence[str],
        market_clock: MarketClock,
        wall: Clock | None = None,
        publish: Callable[[ClockState], None] | None = None,
        start_at_override: str | None = None,
    ) -> None:
        self._db = db
        self._app = app
        self._calendar = calendar
        self._templates = list(templates)
        self._market = market_clock
        self._wall = wall or market_clock.wall
        self._publish = publish
        self._start_at_override = start_at_override
        self._processed = ProcessedCommands(db)
        self._record: ClockRecord | None = None
        self._lock = asyncio.Lock()

    # ----------------------------------------------------------- state
    @property
    def demo(self) -> bool:
        return self._app.mode == "demo"

    @property
    def started(self) -> bool:
        return self._record is not None

    @property
    def record(self) -> ClockRecord:
        if self._record is None:
            raise RuntimeError("ClockKeeper not started")
        return self._record

    @property
    def mode(self) -> ClockMode:
        return self.record.mode

    @property
    def offset_ns(self) -> int:
        return self.record.offset_ns

    @property
    def version(self) -> int:
        return self.record.version

    def market_now_ns(self) -> int:
        return self._wall.now_ns() + self.record.offset_ns

    def state(self) -> ClockState:
        """The current ``ClockState`` (``market_now_ns`` = now)."""
        r = self.record
        return ClockState(
            mode=r.mode,
            offset_ns=r.offset_ns,
            version=r.version,
            market_now_ns=self._wall.now_ns() + r.offset_ns,
            actor=r.actor,
            reason=r.reason,
        )

    def publish_now(self) -> None:
        """Publish ``sys.clock`` with the current state."""
        if self._publish is not None and self._record is not None:
            self._publish(self.state())

    async def publish_loop(self, interval_s: float = PUBLISH_INTERVAL_S) -> None:
        """Re-publish ``sys.clock`` every ``interval_s`` (02 §5); cancel to stop."""
        while True:
            await asyncio.sleep(interval_s)
            self.publish_now()

    def _apply(self) -> None:
        r = self.record
        self._market.apply(str(r.mode), r.offset_ns, r.version)

    # ----------------------------------------------------------- start
    async def start(self) -> ClockRecord:
        """Load or initialise ``clock_state``, apply it to the market clock and publish it."""
        row = await self._db.run_read(read_clock_state)
        wall_now = self._wall.now_ns()
        rec = self._initial(row, wall_now)
        if not rec.same_clock(row):
            await self._db.run_write(lambda c: write_clock_state(c, rec), critical=True)
        self._record = rec
        self._apply()
        logger.info(
            "clock.started",
            mode=str(rec.mode),
            offset_ns=rec.offset_ns,
            version=rec.version,
            market_now=fmt_ns(wall_now + rec.offset_ns),
            reused=rec.same_clock(row),
        )
        self.publish_now()
        return rec

    def _initial(self, row: ClockRecord | None, wall_now: int) -> ClockRecord:
        next_version = (row.version + 1) if row is not None else 1
        if not self.demo:
            if row is not None and row.mode == ClockMode.REAL:
                return row
            return ClockRecord(ClockMode.REAL, 0, next_version, wall_now, "supervisor", "prod mode: market time = wall clock")
        if row is None or row.mode != ClockMode.DEMO:
            start_at = self._start_at_override or self._app.demo.start_at
            try:
                target = initial_market_ns(self._calendar, start_at, wall_now)
            except (LookupError, ValueError) as exc:
                logger.error("clock.start_at_invalid", start_at=start_at, error=str(exc))
                try:
                    target = initial_market_ns(self._calendar, None, wall_now)
                except LookupError:
                    target = wall_now
            return ClockRecord(ClockMode.DEMO, target - wall_now, next_version, wall_now, "supervisor", f"first demo start at {fmt_ns(target)}")
        if self._start_at_override:
            try:
                target = bj_to_ns(parse_market_time(self._start_at_override))
            except ValueError as exc:
                logger.error("clock.start_at_invalid", start_at=self._start_at_override, error=str(exc))
                return row
            market_now = wall_now + row.offset_ns
            if target > market_now:
                return ClockRecord(ClockMode.DEMO, target - wall_now, next_version, wall_now, "supervisor", f"start-at {fmt_ns(target)}")
            logger.warning(
                "clock.start_at_ignored",
                start_at=fmt_ns(target),
                market_now=fmt_ns(market_now),
                reason="the persisted market clock is later; it never goes backwards",
            )
        return row

    # ----------------------------------------------------------- svc.clock
    async def jump(self, req: ClockJumpRequest, *, requester: str, received_ns: int) -> Reply:
        """Handle ``svc.clock`` (idempotent by ``command_id``)."""
        if not req.command_id:
            return err_reply(codes.BAD_REQUEST, "command_id is required")
        req_hash = request_hash(req)
        async with self._lock:
            # fast path for retries; the transaction below checks again and records atomically
            chk = await self._db.run_write(lambda c: self._processed.check(c, req.command_id, req_hash))
            if chk.reply is not None:
                return chk.reply
            cur = self.record
            wall_now = self._wall.now_ns()
            market_now = wall_now + cur.offset_ns
            new: ClockRecord | None = None
            if not self.demo:
                reply = err_reply(codes.PRECONDITION_FAILED, "svc.clock is only available in demo mode (app.mode=demo)")
            else:
                try:
                    target = jump_target_ns(
                        req.target,
                        calendar=self._calendar,
                        demo=self._app.demo,
                        templates=self._templates,
                        market_now_ns=market_now,
                        at_ns=req.at_ns,
                    )
                except ClockJumpError as exc:
                    reply = err_reply(exc.code, exc.message)
                else:
                    reason = f"{req.target} -> {fmt_ns(target)}"
                    new = ClockRecord(ClockMode.DEMO, target - wall_now, cur.version + 1, wall_now, req.actor, reason)
                    reply = ok_reply(
                        ClockState(
                            mode=new.mode,
                            offset_ns=new.offset_ns,
                            version=new.version,
                            market_now_ns=target,
                            actor=new.actor,
                            reason=new.reason,
                        )
                    )
            td = market_trading_day(self._calendar, market_now) or ns_to_bj(market_now).strftime("%Y%m%d")

            def tx(conn: Connection) -> Reply:
                again = self._processed.check(conn, req.command_id, req_hash)
                if again.reply is not None:
                    return again.reply
                if new is not None:
                    write_clock_state(conn, new)
                self._processed.record(
                    conn,
                    command_id=req.command_id,
                    command=SVC_CLOCK,
                    requester=requester,
                    request_hash=req_hash,
                    reply=reply,
                    trading_day=td,
                    received_ns=received_ns,
                )
                return reply

            result = await self._db.run_write(tx, critical=True)
            if result is reply and new is not None:
                self._record = new
                self._apply()
                self.publish_now()
                logger.info(
                    "clock.jumped",
                    target=str(req.target),
                    actor=req.actor,
                    version=new.version,
                    offset_ns=new.offset_ns,
                    market_from=fmt_ns(market_now),
                    market_to=fmt_ns(new.updated_ns + new.offset_ns),
                )
            elif result is reply:
                logger.warning("clock.jump_rejected", target=str(req.target), code=reply.code, message=reply.message)
            return result


__all__ = [
    "DEMO_START_TIME",
    "PUBLISH_INTERVAL_S",
    "ClockJumpError",
    "ClockKeeper",
    "ClockRecord",
    "fmt_ns",
    "initial_market_ns",
    "jump_target_ns",
    "market_trading_day",
    "next_demo_start_ns",
    "next_open_target_ns",
    "parse_hhmm",
    "parse_market_time",
    "pre_close_target_ns",
    "read_clock_state",
    "session_templates",
    "write_clock_state",
]
