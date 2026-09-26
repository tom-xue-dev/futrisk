"""Daily maintenance of risk-{shard}.db after ``sys.eod`` DONE (03 §8).

Runs on a worker thread (``rt.offload``) and only while the market is quiet (the shell checks that no session is
open or about to open, review M3), so it never competes with trading commits; every write is a short job of the
database writer thread, interleaved with the loop's own transactions:

1. ``PRAGMA wal_checkpoint(PASSIVE)`` on a short-lived connection: it checkpoints what it can without waiting for
   readers or writers and is skipped when the database is busy (a ``TRUNCATE`` checkpoint waits for readers and
   blocks new commits meanwhile);
2. ``VACUUM INTO <home>/backup/{trading_day}/risk-{shard}.db`` through a temporary file (atomic rename; a second
   backup of the same day overwrites the first; a read transaction, writers continue);
3. retention purges in jobs of at most :data:`~futrisk.services.risk_engine.store.PURGE_BATCH` rows:
   ``processed_commands`` before the settled day T (the shard already runs on T+1, so today and yesterday are kept),
   published ``outbox`` rows older than the 7-day replay window by the wall clock (``outbox_topics`` is never
   purged), ``metric_samples`` older than 90 days;
4. backups beyond the newest 20 trading-day folders are removed (this shard's file only).

Orders, fills, alerts, events, audit records, recon breaks, rule states and EOD states are kept permanently
(03 §8). ``vacuum_into`` / ``prune_backups`` are the clearing-worker's helpers, reused as they are.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from loguru import logger
from sqlalchemy import Connection

from futrisk.common.calendar import TradingCalendar
from futrisk.db import Database, Outbox
from futrisk.services.clearing.maintenance import prune_backups, vacuum_into
from futrisk.services.risk_engine.store import (
    METRIC_RETENTION_DAYS,
    OUTBOX_RETENTION_DAYS,
    PURGE_BATCH,
    RiskStore,
    metric_cutoff,
)

BACKUP_KEEP = 20
CHECKPOINT_BUSY_TIMEOUT_S = 0.1
_PURGE_COMMANDS = (
    "DELETE FROM processed_commands WHERE rowid IN "
    "(SELECT rowid FROM processed_commands WHERE trading_day < ? LIMIT ?)"
)


@dataclass
class MaintenanceReport:
    trading_day: str
    checkpoint: tuple[int, int, int] | None = None
    backup: str | None = None
    purged_commands: int = 0
    purged_outbox: int = 0
    purged_metric_samples: int = 0
    pruned_backups: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def passive_checkpoint(db_path: Path, *, busy_timeout_s: float = CHECKPOINT_BUSY_TIMEOUT_S) -> tuple[int, int, int] | None:
    """``PRAGMA wal_checkpoint(PASSIVE)`` -> ``(busy, log_frames, checkpointed_frames)``; ``None`` when it was skipped
    because the database was busy (it never waits for readers and never blocks a writer)."""
    con = sqlite3.connect(str(db_path), timeout=busy_timeout_s, isolation_level=None)
    try:
        row = con.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
        return (int(row[0]), int(row[1]), int(row[2])) if row else (0, 0, 0)
    except sqlite3.OperationalError as exc:
        logger.info("risk.maintenance_checkpoint_skipped", db=str(db_path), reason=str(exc))
        return None
    finally:
        con.close()


def _purge_commands(conn: Connection, keep_from: str, limit: int) -> int:
    cur = conn.exec_driver_sql(_PURGE_COMMANDS, (keep_from, limit))
    return int(cur.rowcount or 0)


def _batched(db: Database, job: Callable[[Connection], int], label: str, rep: MaintenanceReport) -> int:
    """Run ``job(conn) -> rows`` as separate writer jobs until a job deletes less than a batch."""
    total = 0
    try:
        while True:
            n = int(db.write(job))
            total += n
            if n < PURGE_BATCH:
                return total
    except Exception as exc:
        rep.errors.append(f"{label}: {exc}")
        return total


def run_maintenance(
    db: Database,
    outbox: Outbox | None,
    *,
    trading_day: str,
    backup_dir: Path,
    calendar: TradingCalendar | None,
    keep_backups: int = BACKUP_KEEP,
    metric_days: int = METRIC_RETENTION_DAYS,
    outbox_days: float = OUTBOX_RETENTION_DAYS,
) -> MaintenanceReport:
    """03 §8 maintenance of one shard database; blocking, call from a worker thread."""
    rep = MaintenanceReport(trading_day=trading_day)
    db_file = db.path.name
    rep.checkpoint = passive_checkpoint(db.path)
    try:
        rep.backup = str(vacuum_into(db.path, backup_dir / trading_day / db_file))
    except Exception as exc:
        rep.errors.append(f"backup: {exc}")
    # sys.eod DONE of T comes after the roll: the current day is T+1, so "today and yesterday" = T+1 and T
    keep_from = trading_day
    rep.purged_commands = _batched(db, lambda c: _purge_commands(c, keep_from, PURGE_BATCH), "processed_commands", rep)
    if outbox is not None:
        rep.purged_outbox = _batched(
            db, lambda c: outbox.purge_older_than(c, days=outbox_days, limit=PURGE_BATCH), "outbox", rep
        )
    cutoff = metric_cutoff(trading_day, metric_days)
    rep.purged_metric_samples = _batched(db, lambda c: RiskStore.purge_metric_samples(c, cutoff), "metric_samples", rep)
    try:
        rep.pruned_backups = prune_backups(backup_dir, keep_backups, db_file=db_file)
    except Exception as exc:
        rep.errors.append(f"prune_backups: {exc}")
    log = logger.warning if rep.errors else logger.info
    log(
        "risk.maintenance",
        db=db.name,
        trading_day=trading_day,
        checkpoint=rep.checkpoint,
        backup=rep.backup,
        purged_commands=rep.purged_commands,
        purged_outbox=rep.purged_outbox,
        purged_metric_samples=rep.purged_metric_samples,
        pruned=rep.pruned_backups,
        errors=rep.errors,
    )
    return rep


__all__ = ["BACKUP_KEEP", "MaintenanceReport", "passive_checkpoint", "run_maintenance"]
