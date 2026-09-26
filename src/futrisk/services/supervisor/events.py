"""Service events: ``service_events`` rows + ``sys.service`` FACTs (02 §5, §7.9; 03 §4.6, §7).

Every process state transition is one ``ServiceStateChanged``: the row and the outbox
entry are written in the same transaction (03 §7 "进程事件"), so the FACT is published
after the commit and can be replayed with ``ctl.replay``. ``svc.history`` reads the rows
back (daily report "系统运行" section, 01 §5.3.4). Rows also carry the names (never the
values) of the instance's environment overrides and a human-readable ``detail``.
"""

from __future__ import annotations

import json
from concurrent.futures import Future
from dataclasses import dataclass
from typing import Any

from sqlalchemy import Connection, func, insert, select

from futrisk.db import Database
from futrisk.messages.enums import ProcessState, ServiceChangeReason
from futrisk.messages.system import ServiceStateChanged
from futrisk.messages.topics import SYS_SERVICE
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.supervisor.schema import service_events

MAX_HISTORY = 2000


def _env_keys_column(keys: list[str] | None) -> str | None:
    return json.dumps(sorted(keys)) if keys else None


def _env_keys_value(raw: Any) -> list[str] | None:
    if not raw:
        return None
    try:
        keys = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return [str(k) for k in keys] if isinstance(keys, list) and keys else None


def row_to_event(m: Any) -> ServiceStateChanged:
    """A ``service_events`` row (mapping) as the ``sys.service`` payload."""
    return ServiceStateChanged(
        instance=str(m["instance"]),
        service=str(m["service"]),
        pid=int(m["pid"]) if m["pid"] is not None else None,
        from_state=ProcessState(str(m["from_state"])),
        to_state=ProcessState(str(m["to_state"])),
        exit_code=int(m["exit_code"]) if m["exit_code"] is not None else None,
        restarts=int(m["restarts"]),
        reason=ServiceChangeReason(str(m["reason"])),
        actor=m["actor"],
        trading_day=m["trading_day"],
        ts_ns=int(m["ts_ns"]),
        env_keys=_env_keys_value(m["env_keys"]),
        detail=m["detail"],
    )


@dataclass(frozen=True)
class LastState:
    """The latest recorded transition of an instance (supervisor start-up, 01 §5.3.6)."""

    instance: str
    to_state: ProcessState
    pid: int | None


class ServiceEventLog:
    """Writes and queries process events of one supervisor."""

    def __init__(self, rt: AsyncServiceRuntime, db: Database) -> None:
        self._rt = rt
        self._db = db

    def record(self, ev: ServiceStateChanged) -> Future[int]:
        """Queue the event row + ``sys.service`` FACT (ordered; a failed commit is fatal)."""

        def tx(conn: Connection) -> int:
            res = conn.execute(
                insert(service_events).values(
                    ts_ns=ev.ts_ns,
                    trading_day=ev.trading_day,
                    instance=ev.instance,
                    service=ev.service,
                    pid=ev.pid,
                    from_state=str(ev.from_state),
                    to_state=str(ev.to_state),
                    exit_code=ev.exit_code,
                    restarts=ev.restarts,
                    reason=str(ev.reason),
                    actor=ev.actor,
                    env_keys=_env_keys_column(ev.env_keys),
                    detail=ev.detail,
                )
            )
            self._rt.append_fact(conn, SYS_SERVICE, ev, td=ev.trading_day)
            pk = res.inserted_primary_key
            return int(pk[0]) if pk is not None else 0

        return self._rt.submit_write(self._db, tx)

    async def flush(self) -> None:
        """Wait until every event queued so far is committed."""
        await self._db.run_write(lambda c: None)

    async def history(
        self,
        *,
        trading_day: str | None = None,
        since_ns: int | None = None,
        limit: int = MAX_HISTORY,
    ) -> list[ServiceStateChanged]:
        """Events in commit order; the most recent ``limit`` when more match."""

        def q(conn: Connection) -> list[ServiceStateChanged]:
            t = service_events
            stmt = select(t)
            if trading_day:
                stmt = stmt.where(t.c.trading_day == trading_day)
            if since_ns is not None:
                stmt = stmt.where(t.c.ts_ns >= since_ns)
            rows = conn.execute(stmt.order_by(t.c.event_id.desc()).limit(max(1, limit))).mappings().all()
            return [row_to_event(m) for m in reversed(rows)]

        return await self._db.run_read(q)

    async def restart_counts(self, trading_day: str) -> dict[str, int]:
        """``{instance: restarts}`` recorded so far on ``trading_day`` (survives supervisor restarts)."""

        def q(conn: Connection) -> dict[str, int]:
            t = service_events
            rows = conn.execute(
                select(t.c.instance, func.max(t.c.restarts)).where(t.c.trading_day == trading_day).group_by(t.c.instance)
            )
            return {str(r[0]): int(r[1] or 0) for r in rows}

        return await self._db.run_read(q)

    async def last_states(self) -> dict[str, LastState]:
        """Latest transition of every instance ever recorded (``{instance: LastState}``)."""

        def q(conn: Connection) -> dict[str, LastState]:
            t = service_events
            latest = select(func.max(t.c.event_id)).group_by(t.c.instance).scalar_subquery()
            rows = conn.execute(select(t.c.instance, t.c.to_state, t.c.pid).where(t.c.event_id.in_(latest)))
            return {
                str(r[0]): LastState(str(r[0]), ProcessState(str(r[1])), int(r[2]) if r[2] is not None else None)
                for r in rows
            }

        return await self._db.run_read(q)


__all__ = ["MAX_HISTORY", "LastState", "ServiceEventLog", "row_to_event"]
