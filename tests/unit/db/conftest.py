"""Fixtures for the database layer tests: a Database factory over a small schema."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from dbtest_support import T0, BootClock, DbFactory, make_schema
from sqlalchemy import MetaData

from futrisk.common.clock import SimClock
from futrisk.db import Database, Migration


@pytest.fixture
def boot() -> BootClock:
    return BootClock()


@pytest.fixture
def db_factory(tmp_path: Path, boot: BootClock) -> Iterator[DbFactory]:
    opened: list[Database] = []

    def make(
        name: str = "svc",
        *,
        metadata: MetaData | None = None,
        synchronous: str = "NORMAL",
        migrations: list[Migration] | None = None,
        max_batch: int = 64,
    ) -> Database:
        db = Database(
            tmp_path / f"{name}.db",
            metadata if metadata is not None else make_schema(),
            name=name,
            synchronous=synchronous,  # type: ignore[arg-type]
            migrations=migrations,
            clock=SimClock(T0),
            boot_time_ns=boot,
            max_batch=max_batch,
        )
        opened.append(db)
        return db

    yield make
    for db in opened:
        if db.is_open:
            db.close(clean=False)
