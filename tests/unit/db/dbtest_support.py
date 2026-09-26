"""Helpers shared by the database layer tests (imported by conftest and test modules)."""

from __future__ import annotations

from collections.abc import Callable

from sqlalchemy import CheckConstraint, MetaData, Table

from futrisk.db import Database, add_common_tables
from futrisk.db.types import id_col, int_col, td_col

T0 = 1_790_000_000_000_000_000


def make_schema(*, outbox: bool = True, fact_cursors: bool = True) -> MetaData:
    md = MetaData()
    add_common_tables(md, outbox=outbox, fact_cursors=fact_cursors)
    Table(
        "items",
        md,
        id_col("item_id", primary_key=True),
        int_col("qty", CheckConstraint("qty>0"), nullable=False),
        td_col("trading_day", nullable=True),
    )
    return md


class BootClock:
    """Fake host boot time (``Database(boot_time_ns=...)``)."""

    def __init__(self, ns: int = 1_000_000_000_000) -> None:
        self.ns = ns

    def __call__(self) -> int:
        return self.ns


DbFactory = Callable[..., Database]
