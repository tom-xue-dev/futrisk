"""Data access for api.db (03 §4.5, §7, §8).

Every write goes through the database's single writer thread
(``Database.run_write`` / ``submit``); reads use the ``query_only`` pool
(``run_read``), so the event loop is never blocked. api.db runs with
``synchronous=FULL`` (03 DB-03), so every commit is durable.

Transactions (03 §7, "api" rows):

* notifications: ``notifications`` + ``fact_cursors`` in one transaction per
  consumed FACT (see :mod:`.notifier`);
* config versions: one ``config_versions`` write per 2PC step (see :mod:`.config2pc`);
* REST idempotency: ``processed_commands`` keyed by the ``Idempotency-Key``;
* kill switch: the one ``kill_switch`` row (desired state + per-target
  confirmation), rewritten after every fan-out / retry round.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Iterable, Sequence
from concurrent.futures import Future
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

from loguru import logger
from sqlalchemy import Connection, delete, func, insert, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from futrisk.db import Database, ProcessedCommands, ProcessedEntry
from futrisk.messages.enums import ConfigStatus, Role, Severity
from futrisk.messages.envelope import Reply
from futrisk.services.api_gateway.schema import (
    NotificationStatus,
    api_audit,
    api_keys,
    config_versions,
    kill_switch,
    notifications,
)

DAY_NS = 86_400 * 1_000_000_000
AUDIT_RETENTION_DAYS = 180  # 03 §8
NOTIFICATION_RETENTION_DAYS = 90  # 03 §8


def dumps(obj: Any) -> str:
    """Compact JSON for JSON columns (UTF-8, no ASCII escaping)."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def _loads(text: Any, default: Any) -> Any:
    if text is None:
        return default
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return default


# ------------------------------------------------------------------ records
@dataclass(frozen=True)
class KeyRecord:
    """One ``api_keys`` row (never contains the plaintext key)."""

    key_id: str
    key_hash: str
    name: str
    role: Role
    created_ns: int
    expires_ns: int | None = None
    disabled: bool = False


@dataclass(frozen=True)
class AuditRecord:
    """One ``api_audit`` row."""

    ts_ns: int
    key_id: str | None
    role: str | None
    method: str
    path: str
    http_status: int
    result_code: str
    latency_us: int
    client_ip: str
    command: str | None = None
    command_id: str | None = None
    target: str | None = None


@dataclass
class ConfigVersionRow:
    """One ``config_versions`` row (02 §8.5 state of one 2PC round)."""

    config_version: int
    config_hash: str
    path: str
    files: dict[str, str]
    effective: dict[str, Any]
    status: ConfigStatus
    requested_by: str
    requested_ns: int
    acks: dict[str, Any] = field(default_factory=dict)
    finished_ns: int | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        return d


@dataclass
class NotificationRow:
    """One ``notifications`` row."""

    notify_key: str
    channel: str
    kind: str
    severity: Severity
    status: NotificationStatus
    payload: dict[str, Any]
    first_seen_ns: int
    attempts: int = 0
    last_error: str | None = None
    sent_ns: int | None = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["severity"] = self.severity.value
        d["status"] = self.status.value
        return d


class TargetStatus(StrEnum):
    """Confirmation state of one kill-switch target (stored in ``kill_switch.targets``)."""

    PENDING = "PENDING"  # not confirmed yet; retried while the code is retryable
    CONFIRMED = "CONFIRMED"  # the target answered ok for the current desired state
    FAILED = "FAILED"  # non-retryable answer, or retries exhausted (alerted CRITICAL)


@dataclass
class KillSwitchTarget:
    """Confirmation of the desired kill-switch state by one target."""

    status: TargetStatus = TargetStatus.PENDING
    code: str | None = None
    message: str = ""
    attempts: int = 0
    updated_ns: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {"status": self.status.value, "code": self.code, "message": self.message,
                "attempts": self.attempts, "updated_ns": self.updated_ns}


@dataclass
class KillSwitchRow:
    """The ``kill_switch`` row: the operator's latest decision and its per-target confirmation."""

    desired_on: bool
    command_id: str
    reason: str
    actor: str
    role: Role
    requested_ns: int
    updated_ns: int
    targets: dict[str, KillSwitchTarget] = field(default_factory=dict)

    def with_status(self, *statuses: TargetStatus) -> list[str]:
        return sorted(t for t, v in self.targets.items() if v.status in statuses)

    def to_dict(self) -> dict[str, Any]:
        return {
            "desired_on": self.desired_on,
            "command_id": self.command_id,
            "reason": self.reason,
            "actor": self.actor,
            "role": self.role.value,
            "requested_ns": self.requested_ns,
            "updated_ns": self.updated_ns,
            "targets": {t: v.to_dict() for t, v in sorted(self.targets.items())},
            "pending": self.with_status(TargetStatus.PENDING),
            "failed": self.with_status(TargetStatus.FAILED),
            "confirmed": self.with_status(TargetStatus.CONFIRMED),
        }


def _kill_switch_from(m: Any) -> KillSwitchRow:
    raw = _loads(m["targets"], {})
    targets: dict[str, KillSwitchTarget] = {}
    for name, t in (raw.items() if isinstance(raw, dict) else []):
        if not isinstance(t, dict):
            continue
        try:
            status = TargetStatus(str(t.get("status", "PENDING")))
        except ValueError:
            status = TargetStatus.PENDING
        targets[str(name)] = KillSwitchTarget(
            status=status,
            code=t.get("code"),
            message=str(t.get("message") or ""),
            attempts=int(t.get("attempts") or 0),
            updated_ns=int(t.get("updated_ns") or 0),
        )
    return KillSwitchRow(
        desired_on=bool(m["desired_on"]),
        command_id=str(m["command_id"]),
        reason=str(m["reason"]),
        actor=str(m["actor"]),
        role=Role(str(m["role"])),
        requested_ns=int(m["requested_ns"]),
        updated_ns=int(m["updated_ns"]),
        targets=targets,
    )


def _key_from(m: Any) -> KeyRecord:
    return KeyRecord(
        key_id=str(m["key_id"]),
        key_hash=str(m["key_hash"]),
        name=str(m["name"]),
        role=Role(str(m["role"])),
        created_ns=int(m["created_ns"]),
        expires_ns=int(m["expires_ns"]) if m["expires_ns"] is not None else None,
        disabled=bool(m["disabled"]),
    )


def _version_from(m: Any) -> ConfigVersionRow:
    return ConfigVersionRow(
        config_version=int(m["config_version"]),
        config_hash=str(m["config_hash"]),
        path=str(m["path"]),
        files=_loads(m["files"], {}),
        effective=_loads(m["effective"], {}),
        status=ConfigStatus(str(m["status"])),
        acks=_loads(m["acks"], {}),
        requested_by=str(m["requested_by"]),
        requested_ns=int(m["requested_ns"]),
        finished_ns=int(m["finished_ns"]) if m["finished_ns"] is not None else None,
        error=m["error"],
    )


def _notification_from(m: Any) -> NotificationRow:
    return NotificationRow(
        notify_key=str(m["notify_key"]),
        channel=str(m["channel"]),
        kind=str(m["kind"]),
        severity=Severity(str(m["severity"])),
        status=NotificationStatus(str(m["status"])),
        payload=_loads(m["payload"], {}),
        first_seen_ns=int(m["first_seen_ns"]),
        attempts=int(m["attempts"]),
        last_error=m["last_error"],
        sent_ns=int(m["sent_ns"]) if m["sent_ns"] is not None else None,
    )


class ApiStore:
    """Typed access to the api.db tables."""

    def __init__(self, db: Database) -> None:
        self.db = db
        self.processed = ProcessedCommands(db)

    # ------------------------------------------------------------- api_keys
    async def load_keys(self) -> list[KeyRecord]:
        def q(c: Connection) -> list[KeyRecord]:
            return [_key_from(m) for m in c.execute(select(api_keys).order_by(api_keys.c.key_id)).mappings()]

        return await self.db.run_read(q)

    async def upsert_key(self, rec: KeyRecord) -> None:
        """Insert or replace one key (by ``key_id``); a stale row with the same hash is removed first."""

        def tx(c: Connection) -> None:
            c.execute(delete(api_keys).where(api_keys.c.key_hash == rec.key_hash, api_keys.c.key_id != rec.key_id))
            values = {
                "key_hash": rec.key_hash,
                "name": rec.name,
                "role": rec.role.value,
                "expires_ns": rec.expires_ns,
                "disabled": 1 if rec.disabled else 0,
            }
            res = c.execute(update(api_keys).where(api_keys.c.key_id == rec.key_id).values(**values))
            if not res.rowcount:
                c.execute(insert(api_keys).values(key_id=rec.key_id, created_ns=rec.created_ns, **values))

        await self.db.run_write(tx, critical=True)

    async def set_key_disabled(self, key_id: str, disabled: bool) -> None:
        def tx(c: Connection) -> None:
            c.execute(update(api_keys).where(api_keys.c.key_id == key_id).values(disabled=1 if disabled else 0))

        await self.db.run_write(tx, critical=True)

    # ------------------------------------------------------------- api_audit
    def submit_audit(self, rec: AuditRecord) -> Future[None]:
        """Queue one audit row (fire and forget; failures are logged by the caller)."""

        def tx(c: Connection) -> None:
            c.execute(
                insert(api_audit).values(
                    ts_ns=rec.ts_ns,
                    key_id=rec.key_id,
                    role=rec.role,
                    method=rec.method,
                    path=rec.path[:2000],
                    command=rec.command,
                    command_id=rec.command_id,
                    target=rec.target,
                    http_status=rec.http_status,
                    result_code=rec.result_code,
                    latency_us=rec.latency_us,
                    client_ip=rec.client_ip,
                )
            )

        return self.db.submit(tx)

    async def audit_rows(self, limit: int = 1000) -> list[dict[str, Any]]:
        def q(c: Connection) -> list[dict[str, Any]]:
            rows = c.execute(select(api_audit).order_by(api_audit.c.audit_id.desc()).limit(limit)).mappings()
            return [dict(m) for m in rows][::-1]

        return await self.db.run_read(q)

    # --------------------------------------------------- processed_commands
    async def lookup_processed(self, command_id: str) -> ProcessedEntry | None:
        return await self.db.run_read(lambda c: self.processed.lookup(c, command_id))

    async def record_processed(
        self,
        *,
        command_id: str,
        command: str,
        requester: str,
        request_hash: str,
        reply: Reply,
        trading_day: str,
        received_ns: int,
    ) -> bool:
        """Store the first result of an idempotent REST request; False when the key already exists."""

        def tx(c: Connection) -> bool:
            if self.processed.lookup(c, command_id) is not None:
                return False
            self.processed.record(
                c,
                command_id=command_id,
                command=command,
                requester=requester,
                request_hash=request_hash,
                reply=reply,
                trading_day=trading_day,
                received_ns=received_ns,
            )
            return True

        return await self.db.run_write(tx)

    # ------------------------------------------------------- config_versions
    async def max_config_version(self) -> int:
        def q(c: Connection) -> int:
            return int(c.execute(select(func.max(config_versions.c.config_version))).scalar() or 0)

        return await self.db.run_read(q)

    async def insert_config_version(self, row: ConfigVersionRow) -> None:
        def tx(c: Connection) -> None:
            c.execute(
                insert(config_versions).values(
                    config_version=row.config_version,
                    config_hash=row.config_hash,
                    path=row.path,
                    files=dumps(row.files),
                    effective=dumps(row.effective),
                    status=row.status.value,
                    acks=dumps(row.acks),
                    requested_by=row.requested_by,
                    requested_ns=row.requested_ns,
                    finished_ns=row.finished_ns,
                    error=row.error,
                )
            )

        await self.db.run_write(tx, critical=True)

    async def update_config_version(self, row: ConfigVersionRow) -> None:
        """Persist the mutable part of a version row (status, acks, effective, finished, error)."""

        def tx(c: Connection) -> None:
            c.execute(
                update(config_versions)
                .where(config_versions.c.config_version == row.config_version)
                .values(
                    status=row.status.value,
                    acks=dumps(row.acks),
                    effective=dumps(row.effective),
                    finished_ns=row.finished_ns,
                    error=row.error,
                )
            )

        await self.db.run_write(tx, critical=True)

    async def get_config_version(self, version: int) -> ConfigVersionRow | None:
        def q(c: Connection) -> ConfigVersionRow | None:
            m = c.execute(select(config_versions).where(config_versions.c.config_version == version)).mappings().first()
            return _version_from(m) if m is not None else None

        return await self.db.run_read(q)

    async def list_config_versions(self, limit: int = 100) -> list[ConfigVersionRow]:
        def q(c: Connection) -> list[ConfigVersionRow]:
            rows = c.execute(
                select(config_versions).order_by(config_versions.c.config_version.desc()).limit(limit)
            ).mappings()
            return [_version_from(m) for m in rows]

        return await self.db.run_read(q)

    async def config_versions_with_status(self, statuses: Iterable[ConfigStatus]) -> list[ConfigVersionRow]:
        wanted = [s.value for s in statuses]

        def q(c: Connection) -> list[ConfigVersionRow]:
            rows = c.execute(
                select(config_versions)
                .where(config_versions.c.status.in_(wanted))
                .order_by(config_versions.c.config_version)
            ).mappings()
            return [_version_from(m) for m in rows]

        return await self.db.run_read(q)

    # --------------------------------------------------------- notifications
    @staticmethod
    def insert_notification(conn: Connection, row: NotificationRow) -> bool:
        """Insert inside the caller's transaction; False when ``(notify_key, channel)`` exists (duplicate)."""
        stmt = (
            sqlite_insert(notifications)
            .values(
                notify_key=row.notify_key,
                channel=row.channel,
                kind=row.kind,
                severity=row.severity.value,
                status=row.status.value,
                attempts=row.attempts,
                last_error=row.last_error,
                payload=dumps(row.payload),
                first_seen_ns=row.first_seen_ns,
                sent_ns=row.sent_ns,
            )
            .on_conflict_do_nothing(index_elements=["notify_key", "channel"])
        )
        return bool(conn.execute(stmt).rowcount)

    @staticmethod
    def was_notified(conn: Connection, notify_key: str, channel: str) -> bool:
        """True when ``notify_key`` has a row on ``channel`` that was (or is being) sent (caller's transaction)."""
        row = conn.execute(
            select(notifications.c.status).where(
                notifications.c.notify_key == notify_key, notifications.c.channel == channel
            )
        ).first()
        return row is not None and str(row[0]) != NotificationStatus.SUPPRESSED.value

    async def update_notification(
        self,
        notify_key: str,
        channel: str,
        *,
        status: NotificationStatus,
        attempts: int,
        last_error: str | None,
        sent_ns: int | None,
    ) -> None:
        def tx(c: Connection) -> None:
            c.execute(
                update(notifications)
                .where(notifications.c.notify_key == notify_key, notifications.c.channel == channel)
                .values(status=status.value, attempts=attempts, last_error=last_error, sent_ns=sent_ns)
            )

        await self.db.run_write(tx)

    async def notifications_with_status(self, statuses: Sequence[NotificationStatus]) -> list[NotificationRow]:
        wanted = [s.value for s in statuses]

        def q(c: Connection) -> list[NotificationRow]:
            rows = c.execute(
                select(notifications)
                .where(notifications.c.status.in_(wanted))
                .order_by(notifications.c.first_seen_ns)
            ).mappings()
            return [_notification_from(m) for m in rows]

        return await self.db.run_read(q)

    async def get_notification(self, notify_key: str, channel: str) -> NotificationRow | None:
        def q(c: Connection) -> NotificationRow | None:
            m = (
                c.execute(
                    select(notifications).where(
                        notifications.c.notify_key == notify_key, notifications.c.channel == channel
                    )
                )
                .mappings()
                .first()
            )
            return _notification_from(m) if m is not None else None

        return await self.db.run_read(q)

    async def list_notifications(self, limit: int = 200) -> list[NotificationRow]:
        def q(c: Connection) -> list[NotificationRow]:
            rows = c.execute(
                select(notifications).order_by(notifications.c.first_seen_ns.desc()).limit(limit)
            ).mappings()
            return [_notification_from(m) for m in rows]

        return await self.db.run_read(q)

    async def count_notifications(self, status: NotificationStatus) -> int:
        def q(c: Connection) -> int:
            return int(
                c.execute(
                    select(func.count()).select_from(notifications).where(notifications.c.status == status.value)
                ).scalar()
                or 0
            )

        return await self.db.run_read(q)

    async def count_notifications_by_status(self) -> dict[str, int]:
        """``{status: rows}`` over every status (``/system/status``)."""

        def q(c: Connection) -> dict[str, int]:
            rows = c.execute(
                select(notifications.c.status, func.count()).group_by(notifications.c.status)
            ).all()
            out = {s.value: 0 for s in NotificationStatus}
            out.update({str(r[0]): int(r[1]) for r in rows})
            return out

        return await self.db.run_read(q)

    # ----------------------------------------------------------- kill_switch
    async def load_kill_switch(self) -> KillSwitchRow | None:
        def q(c: Connection) -> KillSwitchRow | None:
            m = c.execute(select(kill_switch).where(kill_switch.c.id == 1)).mappings().first()
            return _kill_switch_from(m) if m is not None else None

        return await self.db.run_read(q)

    async def save_kill_switch(self, row: KillSwitchRow) -> None:
        """Replace the single ``kill_switch`` row (durable before the caller announces anything)."""
        values = {
            "desired_on": 1 if row.desired_on else 0,
            "command_id": row.command_id,
            "reason": row.reason,
            "actor": row.actor,
            "role": row.role.value,
            "requested_ns": row.requested_ns,
            "targets": dumps({t: v.to_dict() for t, v in row.targets.items()}),
            "updated_ns": row.updated_ns,
        }

        def tx(c: Connection) -> None:
            stmt = sqlite_insert(kill_switch).values(id=1, **values)
            c.execute(stmt.on_conflict_do_update(index_elements=["id"], set_=values))

        await self.db.run_write(tx, critical=True)

    # ----------------------------------------------------------- maintenance
    async def purge(self, *, keep_processed_from_td: str, now_ns: int) -> dict[str, int]:
        """Retention of 03 §8: processed_commands (today + previous day), audit 180 d, notifications 90 d."""
        audit_before = now_ns - AUDIT_RETENTION_DAYS * DAY_NS
        notif_before = now_ns - NOTIFICATION_RETENTION_DAYS * DAY_NS

        def tx(c: Connection) -> dict[str, int]:
            pc = self.processed.purge_before(c, keep_processed_from_td)
            au = c.execute(delete(api_audit).where(api_audit.c.ts_ns < audit_before)).rowcount or 0
            no = c.execute(delete(notifications).where(notifications.c.first_seen_ns < notif_before)).rowcount or 0
            return {"processed_commands": int(pc), "api_audit": int(au), "notifications": int(no)}

        return await self.db.run_write(tx)

    async def backup(self, dest: Path) -> Path:
        """Checkpoint, ``VACUUM INTO`` a temporary file, then rename it to ``dest`` (03 §8)."""
        src = self.db.path

        def run() -> Path:
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_suffix(dest.suffix + ".tmp")
            tmp.unlink(missing_ok=True)
            con = sqlite3.connect(str(src))
            try:
                con.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # 03 §8 step 1 (best effort while in use)
                con.execute("VACUUM INTO ?", (str(tmp),))
            finally:
                con.close()
            tmp.replace(dest)
            return dest

        loop = asyncio.get_running_loop()
        path = await loop.run_in_executor(None, run)
        logger.info("api_db.backup", path=str(path))
        return path


__all__ = [
    "AUDIT_RETENTION_DAYS",
    "NOTIFICATION_RETENTION_DAYS",
    "ApiStore",
    "AuditRecord",
    "ConfigVersionRow",
    "KeyRecord",
    "KillSwitchRow",
    "KillSwitchTarget",
    "NotificationRow",
    "TargetStatus",
    "dumps",
]
