"""Logical column types of 03 §3.1 as SQLAlchemy Core column factories.

Every service defines its tables with these helpers so that the SQLite DDL
matches 03 §3.1 (TEXT / INTEGER / REAL / BLOB with the documented CHECK
constraints) and a future PostgreSQL port gets the wider types for free
(DB-04). Example::

    from futrisk.db.types import id_col, td_col, ts_col, int_col, enum_col

    orders = Table(
        "orders", metadata,
        uuid_col("client_order_id", primary_key=True),
        id_col("account_id", nullable=False),
        td_col("trading_day", nullable=False),
        int_col("qty", CheckConstraint("qty>0"), nullable=False),
        enum_col("status", OrderStatus, nullable=False),
        ts_col("created_ns", nullable=False),
    )

Positional ``*args`` (extra constraints, ``ForeignKey``) and keyword
arguments (``nullable``, ``primary_key``, ``index`` ...) are passed through to
:class:`sqlalchemy.Column`.
"""

from __future__ import annotations

from collections.abc import Iterable
from enum import Enum
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    Column,
    Float,
    Integer,
    LargeBinary,
    String,
    Text,
    text,
)

# INT / TS: INTEGER in SQLite (so INTEGER PRIMARY KEY aliases the rowid), BIGINT elsewhere.
BIGINT = BigInteger().with_variant(Integer(), "sqlite")


def id_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``ID``: business identifier (account, instrument, instance)."""
    return Column(name, String(64), *args, **kw)


def uuid_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``UUID``: UUIDv7 string."""
    return Column(name, String(36), *args, **kw)


def text_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """Free ``TEXT``."""
    return Column(name, Text, *args, **kw)


def td_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``TD``: trading day ``YYYYMMDD`` with ``CHECK(length(x)=8)``."""
    return Column(name, String(8), CheckConstraint(f"length({name})=8"), *args, **kw)


def ts_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``TS``: UTC epoch nanoseconds."""
    return Column(name, BIGINT, *args, **kw)


def int_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``INT``: lots, sequence numbers, counters."""
    return Column(name, BIGINT, *args, **kw)


def real_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``REAL``: float64 (intraday metrics, bar prices)."""
    return Column(name, Float, *args, **kw)


def bool_col(name: str, *args: Any, default: bool = False, **kw: Any) -> Column[Any]:
    """``BOOL``: ``INTEGER CHECK(x IN (0,1))`` with a server default."""
    kw.setdefault("server_default", text("1" if default else "0"))
    return Column(name, Integer, CheckConstraint(f"{name} IN (0,1)"), *args, **kw)


def _enum_values(values: Iterable[str] | type[Enum]) -> list[str]:
    if isinstance(values, type) and issubclass(values, Enum):
        return [str(m.value) for m in values]
    return [str(v) for v in values]


def enum_col(name: str, values: Iterable[str] | type[Enum], *args: Any, **kw: Any) -> Column[Any]:
    """``ENUM(name)``: ``TEXT CHECK(x IN (...))``; ``values`` may be a StrEnum class."""
    vals = _enum_values(values)
    if not vals:
        raise ValueError(f"enum column {name} needs at least one value")
    for v in vals:
        if "'" in v:
            raise ValueError(f"enum value {v!r} must not contain quotes")
    quoted = ",".join(f"'{v}'" for v in vals)
    # A CHECK evaluating to NULL passes in SQL, so nullable enum columns accept NULL.
    return Column(name, Text, CheckConstraint(f"{name} IN ({quoted})"), *args, **kw)


def json_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``JSON``: ``TEXT CHECK(json_valid(x))`` (NULL passes for nullable columns)."""
    return Column(name, Text, CheckConstraint(f"json_valid({name})"), *args, **kw)


def blob_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``BLOB``: msgpack-encoded payload."""
    return Column(name, LargeBinary, *args, **kw)


def hash_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``HASH``: 64 lowercase hex chars (SHA-256)."""
    return Column(name, String(64), *args, **kw)


def dec_price_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``DEC_PRICE``: canonical decimal string (02 §4.3 DecStr)."""
    return Column(name, Text, *args, **kw)


def dec_money_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``DEC_MONEY``: canonical decimal string, 2 decimals."""
    return Column(name, Text, *args, **kw)


def dec_amount_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``DEC_AMOUNT``: canonical decimal string (unrounded amounts)."""
    return Column(name, Text, *args, **kw)


def dec_rate_col(name: str, *args: Any, **kw: Any) -> Column[Any]:
    """``DEC_RATE``: canonical decimal string (exact ratios)."""
    return Column(name, Text, *args, **kw)


__all__ = [
    "BIGINT",
    "blob_col",
    "bool_col",
    "dec_amount_col",
    "dec_money_col",
    "dec_price_col",
    "dec_rate_col",
    "enum_col",
    "hash_col",
    "id_col",
    "int_col",
    "json_col",
    "real_col",
    "td_col",
    "text_col",
    "ts_col",
    "uuid_col",
]
