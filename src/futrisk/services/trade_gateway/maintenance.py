"""Daily maintenance of td.db after ``sys.eod`` DONE (03 §8; 02 §4.1).

trade-gateway subscribes to clearing-worker's ``sys.eod`` only to trigger this (no cursor:
a missed ``DONE`` just delays the maintenance by a day). For trading day T:

1. ``PRAGMA wal_checkpoint(TRUNCATE)`` on a short-lived connection;
2. backup: ``VACUUM INTO <home>/backup/T/td.db`` through a temporary file (td.db is the
   source of truth for fills, 03 §4.2);
3. retention, in batches of one writer job each:
   ``processed_commands`` of trading days before T's previous trading day (today and the
   previous trading day are kept); published ``outbox`` rows older than the 7-day replay
   window by the **wall** clock (``published_ns`` is wall time; ``outbox_topics`` is never
   purged, so sequence numbers never go back); ``broker_order_events`` older than 90 days;
   ``routed_orders``, ``trades`` and ``route_tombstones`` are kept permanently (audit);
4. backups beyond the newest 20 trading days are pruned (only this service's ``td.db``).

The checkpoint / backup / prune helpers are the clearing-worker's
(:mod:`futrisk.services.clearing.maintenance`). :func:`run_maintenance` blocks: the
service runs it on the runtime's offload pool, never on the business thread.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

from loguru import logger
from sqlalchemy import Connection

from futrisk.common.calendar import TradingCalendar
from futrisk.common.clock import parse_td, td_str
from futrisk.db import Database, Outbox, ProcessedCommands
from futrisk.services.clearing.maintenance import prune_backups, vacuum_into, wal_checkpoint
from futrisk.services.trade_gateway import store
from futrisk.services.trade_gateway.schema import DB_NAME

DB_FILE = f"{DB_NAME}.db"
ORDER_EVENTS_RETENTION_DAYS = 90
BACKUP_KEEP_DAYS = 20
BATCH = 5_000


@dataclass
class MaintenanceResult:
    trading_day: str
    checkpoint: tuple[int, int, int] | None = None
    backup: str | None = None
    purged_commands: int = 0
    purged_outbox: int = 0
    purged_order_events: int = 0
    pruned_backups: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def _batched(db: Database, job: Callable[[Connection], int]) -> int:
    """Repeat one-batch writer jobs until a batch deletes fewer than ``BATCH`` rows."""
    total = 0
    while True:
        n = int(db.write(job))
        total += n
        if n < BATCH:
            return total


def run_maintenance(
    db: Database,
    outbox: Outbox | None,
    *,
    trading_day: str,
    calendar: TradingCalendar,
    backup_dir: Path | None,
    replay_window_days: float = 7.0,
    wall_now_ns: int | None = None,
    keep_backups: int = BACKUP_KEEP_DAYS,
) -> MaintenanceResult:
    """Checkpoint, back up, purge and prune for ``trading_day``; errors are collected, never raised."""
    res = MaintenanceResult(trading_day=trading_day)
    try:
        db.flush(timeout=30)  # everything queued so far is committed
        res.checkpoint = wal_checkpoint(db.path)
    except Exception as exc:
        res.errors.append(f"checkpoint: {exc}")
    if backup_dir is not None:
        try:
            res.backup = str(vacuum_into(db.path, backup_dir / trading_day / DB_FILE))
        except Exception as exc:
            res.errors.append(f"backup: {exc}")
    try:
        try:
            keep_from = td_str(calendar.prev_trading_day(parse_td(trading_day)))
        except LookupError:
            keep_from = trading_day
        processed = ProcessedCommands(db)
        res.purged_commands = int(db.write(lambda c: processed.purge_before(c, keep_from)))
        if outbox is not None:
            res.purged_outbox = _batched(
                db, lambda c: outbox.purge_older_than(c, days=replay_window_days, now_wall_ns=wall_now_ns, limit=BATCH)
            )
        cutoff = td_str(parse_td(trading_day) - timedelta(days=ORDER_EVENTS_RETENTION_DAYS))
        res.purged_order_events = _batched(db, lambda c: store.purge_order_events_before(c, cutoff, limit=BATCH))
    except Exception as exc:
        res.errors.append(f"purge: {exc}")
    if backup_dir is not None:
        try:
            res.pruned_backups = prune_backups(backup_dir, keep_backups, db_file=DB_FILE)
        except Exception as exc:
            res.errors.append(f"prune: {exc}")
    if res.errors:
        logger.warning("tg.maintenance_errors", trading_day=trading_day, errors=res.errors)
    else:
        logger.info("tg.maintenance_done", trading_day=trading_day, backup=res.backup, purged_commands=res.purged_commands,
                    purged_outbox=res.purged_outbox, purged_order_events=res.purged_order_events,
                    pruned_backups=res.pruned_backups)
    return res


__all__ = ["BACKUP_KEEP_DAYS", "ORDER_EVENTS_RETENTION_DAYS", "MaintenanceResult", "run_maintenance"]
