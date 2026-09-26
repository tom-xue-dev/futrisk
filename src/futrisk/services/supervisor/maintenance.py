"""Daily maintenance of supervisor.db after ``sys.eod`` DONE (03 §8; 02 §4.1).

The supervisor subscribes to clearing-worker's ``sys.eod`` only to trigger maintenance
(no cursor: a missed ``DONE`` just delays it by a day). On ``DONE`` of trading day T, in
the order of 03 §8:

1. ``PRAGMA wal_checkpoint(TRUNCATE)`` on a short-lived connection (best effort: busy
   readers only make it partial);
2. backup: ``VACUUM INTO <home>/backup/T/supervisor.db.tmp``, then an atomic rename to
   ``supervisor.db`` (a second backup of the same day overwrites the first);
3. retention purges, in batches of one writer job each:

   * ``processed_commands``: rows of trading days before T are deleted (today + previous
     trading day are kept);
   * ``outbox``: published rows older than ``bus.replay_window_days`` (7 d) by the **wall**
     clock (``published_ns`` is wall time, so demo clock jumps never shrink the replay window);
   * ``service_events``: older than 90 days; their ``ts_ns`` is market time and so is the
     cut-off (the market clock never goes backwards);
   * backups: only the newest :data:`BACKUP_KEEP_TRADING_DAYS` (20) trading-day folders keep
     a ``supervisor.db``; older copies of this database are deleted (other services' files
     stay; a folder is removed once empty).

The checkpoint and the backup run in worker threads. A failed step is logged and recorded
in the result; the following steps still run, and nothing stops the supervisor.
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import Connection, delete, select

from futrisk.common.clock import NS_PER_S
from futrisk.db import Database, ProcessedCommands
from futrisk.messages.base import decode
from futrisk.messages.enums import EodPhase
from futrisk.messages.envelope import Envelope
from futrisk.messages.eod import EodPhaseEvent
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.supervisor.schema import DB_NAME, service_events

SERVICE_EVENTS_RETENTION_DAYS = 90
BACKUP_KEEP_TRADING_DAYS = 20
DAY_NS = 86_400 * NS_PER_S
BATCH = 5_000
DB_FILE = f"{DB_NAME}.db"
STEPS = ("checkpoint", "backup", "purge", "prune_backups")


def _purge_events(conn: Connection, before_ns: int, limit: int) -> int:
    t = service_events
    ids = select(t.c.event_id).where(t.c.ts_ns < before_ns).order_by(t.c.event_id).limit(limit)
    return int(conn.execute(delete(t).where(t.c.event_id.in_(ids))).rowcount or 0)


def wal_checkpoint(db_path: Path) -> tuple[int, int, int]:
    """``PRAGMA wal_checkpoint(TRUNCATE)`` -> ``(busy, log_frames, checkpointed_frames)``."""
    con = sqlite3.connect(str(db_path), timeout=5.0)
    try:
        row = con.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        return (int(row[0]), int(row[1]), int(row[2])) if row else (0, 0, 0)
    finally:
        con.close()


def backup_database(src: Path, dest: Path) -> Path:
    """``VACUUM INTO`` a temporary file next to ``dest``, then rename (03 §8)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    tmp.unlink(missing_ok=True)
    con = sqlite3.connect(str(src), timeout=5.0)
    try:
        con.execute("VACUUM INTO ?", (str(tmp),))
    finally:
        con.close()
    tmp.replace(dest)
    return dest


def _is_day_folder(p: Path) -> bool:
    return p.is_dir() and len(p.name) == 8 and p.name.isdigit()


def prune_backups(backup_dir: Path, keep: int = BACKUP_KEEP_TRADING_DAYS, *, db_file: str = DB_FILE) -> list[str]:
    """Delete ``db_file`` backups beyond the newest ``keep`` trading days; returns the pruned days."""
    if not backup_dir.is_dir():
        return []
    days = sorted(p.name for p in backup_dir.iterdir() if _is_day_folder(p) and (p / db_file).exists())
    removed: list[str] = []
    for day in days[: max(0, len(days) - max(1, keep))]:
        folder = backup_dir / day
        for name in (db_file, db_file + ".tmp"):
            with contextlib.suppress(FileNotFoundError):
                (folder / name).unlink()
        removed.append(day)
        with contextlib.suppress(OSError):
            folder.rmdir()  # only when empty: other services keep their own files there
    return removed


class Maintenance:
    """Runs the 03 §8 steps once per trading day when ``sys.eod`` reports ``DONE``."""

    def __init__(
        self,
        rt: AsyncServiceRuntime,
        db: Database,
        *,
        spawn: Callable[[Awaitable[Any], str], Any],
        backup_keep: int = BACKUP_KEEP_TRADING_DAYS,
    ) -> None:
        self._rt = rt
        self._db = db
        self._spawn = spawn
        self._backup_keep = backup_keep
        self._done: set[str] = set()
        self._lock = asyncio.Lock()

    def on_eod(self, env: Envelope) -> None:
        """``sys.eod`` subscription handler (never raises: subscription errors are fatal)."""
        if env.type != "EodPhaseEvent":
            return
        try:
            ev = decode(env.payload, EodPhaseEvent)
        except Exception:
            logger.warning("maintenance.bad_eod_event")
            return
        if ev.phase != EodPhase.DONE or ev.trading_day in self._done:
            return
        self._done.add(ev.trading_day)
        self._spawn(self.run(ev.trading_day), f"maintenance:{ev.trading_day}")

    @property
    def backup_dir(self) -> Path | None:
        layout = self._rt.layout
        return layout.backup_dir if layout is not None else None

    # ---------------------------------------------------------------- steps
    async def checkpoint(self) -> tuple[int, int, int]:
        return await asyncio.to_thread(wal_checkpoint, self._db.path)

    async def backup(self, trading_day: str) -> str | None:
        backup_dir = self.backup_dir
        if backup_dir is None:
            return None
        dest = backup_dir / trading_day / DB_FILE
        return str(await asyncio.to_thread(backup_database, self._db.path, dest))

    async def purge(self, trading_day: str) -> dict[str, int]:
        rt, db = self._rt, self._db
        counts: dict[str, int] = {}
        processed = ProcessedCommands(db)
        counts["processed_commands"] = await db.run_write(lambda c: processed.purge_before(c, trading_day))
        outbox = rt.outbox
        if outbox is not None:
            window_days = rt.config.services.bus.replay_window_days
            wall_now = rt.market_clock.wall.now_ns()
            total = 0
            while True:
                n = await db.run_write(
                    lambda c: outbox.purge_older_than(c, days=window_days, now_wall_ns=wall_now, limit=BATCH)
                )
                total += n
                if n < BATCH:
                    break
            counts["outbox"] = total
        ev_cutoff = rt.market_clock.now_ns() - SERVICE_EVENTS_RETENTION_DAYS * DAY_NS
        total = 0
        while True:
            n = await db.run_write(lambda c: _purge_events(c, ev_cutoff, BATCH))
            total += n
            if n < BATCH:
                break
        counts["service_events"] = total
        return counts

    async def prune_backups(self) -> list[str]:
        backup_dir = self.backup_dir
        if backup_dir is None:
            return []
        return await asyncio.to_thread(prune_backups, backup_dir, self._backup_keep)

    async def run(self, trading_day: str) -> dict[str, Any]:
        """Checkpoint -> backup -> purge -> prune old backups; returns what each step did.

        ``result["steps"]`` lists the steps in the order they ran; ``result["errors"]`` maps a
        failed step to its error.
        """
        result: dict[str, Any] = {"trading_day": trading_day, "steps": [], "errors": {}}
        async with self._lock:
            for step in STEPS:
                try:
                    if step == "checkpoint":
                        result["checkpoint"] = await self.checkpoint()
                    elif step == "backup":
                        result["backup"] = await self.backup(trading_day)
                    elif step == "purge":
                        result.update(await self.purge(trading_day))
                    else:
                        result["pruned_backups"] = await self.prune_backups()
                except Exception as exc:
                    logger.opt(exception=exc).error("maintenance.step_failed", step=step, trading_day=trading_day)
                    result["errors"][step] = f"{type(exc).__name__}: {exc}"
                result["steps"].append(step)
        if result["errors"]:
            logger.warning("maintenance.done_with_errors", **result)
        else:
            logger.info("maintenance.done", **result)
        return result


__all__ = [
    "BACKUP_KEEP_TRADING_DAYS",
    "SERVICE_EVENTS_RETENTION_DAYS",
    "STEPS",
    "Maintenance",
    "backup_database",
    "prune_backups",
    "wal_checkpoint",
]
