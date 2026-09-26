"""Daily maintenance of clearing.db after ``sys.eod`` DONE (03 §8).

Steps, all off the event loop (worker threads / the writer thread) and outside
trading hours (EOD runs after the close):

1. ``PRAGMA wal_checkpoint(TRUNCATE)`` on a short-lived connection;
2. ``VACUUM INTO '<home>/backup/{td}/clearing.db.tmp'`` then an atomic rename to
   ``clearing.db`` (VACUUM INTO fails when the target exists, so a stale temp
   file is removed first; a second backup of the same day overwrites the first);
3. retention purges in batches: ``processed_commands`` older than the previous
   trading day, published ``outbox`` rows older than the 7-day replay window
   by the wall clock (``outbox_topics`` is never purged, so sequence numbers never go back), and
   backups beyond the last ``backup_keep_days`` trading days (only this
   service's ``clearing.db`` files are removed; empty day folders are deleted).

Settlement results, SOD snapshots, checks, reports and the trade/cash copies
are kept permanently (03 §8).
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from loguru import logger

from futrisk.common.calendar import TradingCalendar
from futrisk.common.clock import parse_td, td_str
from futrisk.db import Database, Outbox, ProcessedCommands
from futrisk.services.clearing.fsutil import replace_file
from futrisk.services.clearing.settings import ClearingSettings

DB_FILE = "clearing.db"


@dataclass
class MaintenanceReport:
    trading_day: str
    checkpoint: tuple[int, int, int] | None = None
    backup: str | None = None
    purged_commands: int = 0
    purged_outbox: int = 0
    pruned_backups: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def wal_checkpoint(db_path: Path) -> tuple[int, int, int]:
    """``PRAGMA wal_checkpoint(TRUNCATE)`` -> ``(busy, log_frames, checkpointed_frames)``."""
    con = sqlite3.connect(str(db_path), timeout=5.0)
    try:
        row = con.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        return (int(row[0]), int(row[1]), int(row[2])) if row else (0, 0, 0)
    finally:
        con.close()


def vacuum_into(db_path: Path, target: Path) -> Path:
    """Consistent copy of the live database: ``VACUUM INTO`` a temp file, then atomic rename."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    if tmp.exists():
        tmp.unlink()
    con = sqlite3.connect(str(db_path), timeout=5.0)
    try:
        con.execute("VACUUM INTO ?", (str(tmp),))
    finally:
        con.close()
    replace_file(tmp, target)
    return target


def prune_backups(backup_dir: Path, keep: int, *, db_file: str = DB_FILE) -> list[str]:
    """Delete this service's backups beyond the newest ``keep`` trading-day folders."""
    if not backup_dir.exists():
        return []
    days = sorted(p.name for p in backup_dir.iterdir() if p.is_dir() and p.name.isdigit() and len(p.name) == 8)
    removed: list[str] = []
    for day in days[: max(0, len(days) - keep)]:
        folder = backup_dir / day
        target = folder / db_file
        if target.exists():
            target.unlink()
            removed.append(day)
        stale = folder / (db_file + ".tmp")
        if stale.exists():
            stale.unlink()
        with contextlib.suppress(OSError):
            folder.rmdir()  # only when empty: other services keep their own files
    return removed


class Maintenance:
    """Runs the 03 §8 maintenance for clearing.db."""

    def __init__(
        self,
        db: Database,
        outbox: Outbox,
        settings: ClearingSettings,
        *,
        backup_dir: Path,
        calendar: TradingCalendar,
        wall_ns: Callable[[], int] | None = None,
    ) -> None:
        self._db = db
        self._outbox = outbox
        self._settings = settings
        self._backup_dir = backup_dir
        self._calendar = calendar
        self._wall_ns = wall_ns
        self._processed = ProcessedCommands(db)
        self._lock = asyncio.Lock()
        self.last: MaintenanceReport | None = None

    async def run(self, trading_day: str) -> MaintenanceReport:
        """Checkpoint, back up and purge for ``trading_day`` (serialized; errors are logged, not raised)."""
        async with self._lock:
            rep = MaintenanceReport(trading_day=trading_day)
            path = self._db.path
            try:
                await self._db.run_write(lambda c: None)  # everything queued so far is committed
                rep.checkpoint = await asyncio.to_thread(wal_checkpoint, path)
            except Exception as exc:
                rep.errors.append(f"checkpoint: {exc}")
            try:
                target = self._backup_dir / trading_day / DB_FILE
                rep.backup = str(await asyncio.to_thread(vacuum_into, path, target))
            except Exception as exc:
                rep.errors.append(f"backup: {exc}")
            try:
                rep.purged_commands = await self._purge_commands(trading_day)
                rep.purged_outbox = await self._purge_outbox()
            except Exception as exc:
                rep.errors.append(f"purge: {exc}")
            try:
                rep.pruned_backups = await asyncio.to_thread(
                    prune_backups, self._backup_dir, self._settings.backup_keep_days
                )
            except Exception as exc:
                rep.errors.append(f"prune: {exc}")
            if rep.errors:
                logger.warning("maintenance.done_with_errors", trading_day=trading_day, errors=rep.errors)
            else:
                logger.info(
                    "maintenance.done",
                    trading_day=trading_day,
                    backup=rep.backup,
                    purged_commands=rep.purged_commands,
                    purged_outbox=rep.purged_outbox,
                    pruned=rep.pruned_backups,
                )
            self.last = rep
            return rep

    async def _purge_commands(self, trading_day: str) -> int:
        """03 §8: keep the processed commands of the current and the previous trading day."""
        try:
            keep_from = td_str(self._calendar.prev_trading_day(parse_td(trading_day)))
        except LookupError:
            keep_from = trading_day
        return int(await self._db.run_write(lambda c: self._processed.purge_before(c, keep_from)))

    async def _purge_outbox(self) -> int:
        """Published rows older than the replay window, by the WALL clock (``published_ns`` is wall time)."""
        now = self._wall_ns() if self._wall_ns is not None else None
        days = float(self._settings.outbox_retention_days)
        total = 0
        while True:
            n = await self._db.run_write(
                lambda c: self._outbox.purge_older_than(c, days=days, now_wall_ns=now, limit=self._settings.purge_batch)
            )
            total += int(n)
            if n < self._settings.purge_batch:
                return total


__all__ = ["Maintenance", "MaintenanceReport", "prune_backups", "vacuum_into", "wal_checkpoint"]
