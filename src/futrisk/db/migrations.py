"""Forward-only schema migrations with checksums (03 §9).

Each migration has a version, a description and a body: a SQL script, a
callable ``fn(conn)`` or a ``MetaData.create_all``. Applied migrations are
recorded in ``schema_version`` together with the SHA-256 of their body; when a
recorded checksum differs from the one in the code, or the database contains
a version this build does not know, :class:`MigrationRunner` raises
:class:`MigrationError` and the service refuses to start.

All pending migrations run inside the caller's transaction (``BEGIN IMMEDIATE``
from the writer thread), so a failure leaves the database untouched.

Checksums:

* SQL script: SHA-256 of the script with ``\\r\\n`` normalised to ``\\n``;
* ``create_all``: SHA-256 of :func:`schema_fingerprint`, a stable description
  of the tables (columns, types, keys, checks, indexes) that does not depend on
  how SQLAlchemy formats DDL. Databases recorded with the earlier DDL-text
  checksum are accepted and their record is upgraded (``alt_checksums``);
* callable: SHA-256 of ``checksum_source`` (defaults to the function source).

Freezing a released schema: while developing, ``Migration.create_all(1, ...)``
tracks the Table definitions. Before a release, write the v1 DDL once with
:func:`freeze_schema` (``src/futrisk/services/<svc>/migrations/0001_initial_schema.sql``),
commit it and switch to :func:`load_sql_migrations`; later schema changes are
new ``NNNN_*.sql`` files (and edits to the Table definitions). The frozen file
carries ``-- futrisk:accept-checksum`` headers so databases created by the
``create_all`` migration keep opening.

SQL files follow the 03 §9 naming ``NNNN_<description>.sql`` and can be loaded
with :func:`load_sql_migrations`.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import re
import sqlite3
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from loguru import logger
from sqlalchemy import (
    CheckConstraint,
    Column,
    Connection,
    ForeignKeyConstraint,
    MetaData,
    UniqueConstraint,
    insert,
    select,
    update,
)
from sqlalchemy.dialects import sqlite
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.schema import CreateIndex, CreateTable

from futrisk.db.common_tables import schema_version_table


class MigrationError(RuntimeError):
    """Schema history does not match this build; the service must not start."""


def _sha256(text: str) -> str:
    return hashlib.sha256(text.replace("\r\n", "\n").encode("utf-8")).hexdigest()


def render_ddl(metadata: MetaData, dialect: Dialect | None = None, *, if_not_exists: bool = False) -> str:
    """Render ``CREATE TABLE`` / ``CREATE INDEX`` statements for ``metadata``.

    Deterministic (dependency order, then index name); used for the legacy
    create_all checksum and (``if_not_exists=True``) by :func:`freeze_schema`.
    """
    d = dialect or sqlite.dialect()
    parts: list[str] = []
    for table in metadata.sorted_tables:
        parts.append(str(CreateTable(table, if_not_exists=if_not_exists).compile(dialect=d)).strip() + ";")
        for idx in sorted(table.indexes, key=lambda i: str(i.name)):
            parts.append(str(CreateIndex(idx, if_not_exists=if_not_exists).compile(dialect=d)).strip() + ";")
    text = "\n\n".join(parts)
    # normalise whitespace so cosmetic compiler changes do not alter checksums
    return "\n".join(line.rstrip() for line in text.splitlines()) + "\n"


def _type_desc(t: Any) -> str:
    name = type(t).__name__
    for attr in ("length", "precision", "scale"):
        v = getattr(t, attr, None)
        if v is not None:
            name += f"[{attr}={v}]"
    variants = getattr(t, "_variant_mapping", None) or {}
    if variants:
        name += "{" + ",".join(f"{k}={type(v).__name__}" for k, v in sorted(variants.items())) + "}"
    return name


def _column_desc(c: Column[Any]) -> dict[str, Any]:
    default = getattr(c.server_default, "arg", None)
    return {
        "name": c.name,
        "type": _type_desc(c.type),
        "nullable": bool(c.nullable),
        "pk": bool(c.primary_key),
        "autoincrement": str(c.autoincrement) if c.primary_key else "",
        "server_default": str(default) if default is not None else None,
    }


def schema_fingerprint(metadata: MetaData) -> str:
    """Stable JSON description of ``metadata`` (checksum source of ``create_all`` migrations)."""
    tables: list[dict[str, Any]] = []
    for table in sorted(metadata.tables.values(), key=lambda t: t.name):
        checks: set[str] = set()
        for con in list(table.constraints) + [k for col in table.columns for k in col.constraints]:
            if isinstance(con, CheckConstraint):
                checks.add(str(con.sqltext))
        uniques = sorted(
            [str(con.name or ""), [col.name for col in con.columns]]
            for con in table.constraints
            if isinstance(con, UniqueConstraint)
        )
        fks = sorted(
            [[col.name for col in con.columns], [str(e.target_fullname) for e in con.elements]]
            for con in table.constraints
            if isinstance(con, ForeignKeyConstraint)
        )
        indexes = sorted(
            [
                str(idx.name),
                [str(getattr(e, "name", e)) for e in idx.expressions],
                bool(idx.unique),
                str(idx.dialect_options["sqlite"].get("where")),
            ]
            for idx in table.indexes
        )
        tables.append(
            {
                "name": table.name,
                "columns": [_column_desc(c) for c in table.columns],
                "checks": sorted(checks),
                "uniques": uniques,
                "foreign_keys": fks,
                "indexes": indexes,
                "sqlite_autoincrement": bool(table.dialect_options["sqlite"].get("autoincrement")),
            }
        )
    return json.dumps(tables, sort_keys=True, separators=(",", ":"))


_ACCEPT = re.compile(r"^--\s*futrisk:accept-checksum\s+([0-9a-f]{64})\s*$", re.M)


def _strip_comments(sql: str) -> str:
    out = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
    return "\n".join(line.split("--", 1)[0] for line in out.splitlines())


def split_sql(script: str) -> list[str]:
    """Split a SQL script into complete statements (handles quoted ``;`` and triggers)."""
    statements: list[str] = []
    buf = ""
    for piece in script.replace("\r\n", "\n").split(";"):
        buf += piece + ";"
        if sqlite3.complete_statement(buf):
            if _strip_comments(buf).strip().strip(";").strip():
                statements.append(buf.strip())
            buf = ""
    rest = _strip_comments(buf).strip().strip(";").strip()
    if rest:
        raise MigrationError(f"incomplete SQL statement at end of script: {buf.strip()[:80]!r}")
    return statements


@dataclass(frozen=True)
class Migration:
    """One forward-only migration step."""

    version: int
    description: str
    kind: Literal["sql", "callable", "metadata"]
    checksum: str
    sql: str | None = None
    fn: Callable[[Connection], None] | None = field(default=None, compare=False)
    metadata: MetaData | None = field(default=None, compare=False)
    alt_checksums: tuple[str, ...] = ()
    """Older checksums of the same migration that are still accepted (the record is upgraded)."""

    # ----------------------------------------------------------- factories
    @classmethod
    def sql_script(cls, version: int, description: str, sql: str, *, alt_checksums: Sequence[str] = ()) -> Migration:
        accepted = tuple(alt_checksums) + tuple(_ACCEPT.findall(sql.replace("\r\n", "\n")))
        return cls(
            version=version,
            description=description,
            kind="sql",
            checksum=_sha256(sql),
            sql=sql,
            alt_checksums=accepted,
        )

    @classmethod
    def create_all(cls, version: int, description: str, metadata: MetaData) -> Migration:
        return cls(
            version=version,
            description=description,
            kind="metadata",
            checksum=_sha256(schema_fingerprint(metadata)),
            metadata=metadata,
            alt_checksums=(_sha256(render_ddl(metadata)),),
        )

    @classmethod
    def from_callable(
        cls,
        version: int,
        description: str,
        fn: Callable[[Connection], None],
        *,
        checksum_source: str | None = None,
    ) -> Migration:
        if checksum_source is None:
            try:
                checksum_source = inspect.getsource(fn)
            except (OSError, TypeError):
                checksum_source = f"{fn.__module__}.{fn.__qualname__}"
        return cls(
            version=version,
            description=description,
            kind="callable",
            checksum=_sha256(checksum_source),
            fn=fn,
        )

    @classmethod
    def from_sql_file(cls, path: str | Path) -> Migration:
        """Load ``NNNN_<description>.sql`` (UTF-8)."""
        p = Path(path)
        m = re.fullmatch(r"(\d+)_(.+)\.sql", p.name)
        if m is None:
            raise MigrationError(f"migration file name must be NNNN_<description>.sql: {p.name}")
        return cls.sql_script(int(m.group(1)), m.group(2).replace("_", " "), p.read_text(encoding="utf-8"))

    # ------------------------------------------------------------- applying
    def apply(self, conn: Connection) -> None:
        if self.kind == "sql":
            assert self.sql is not None
            for stmt in split_sql(self.sql):
                conn.exec_driver_sql(stmt)
        elif self.kind == "metadata":
            assert self.metadata is not None
            self.metadata.create_all(conn, checkfirst=True)
        else:
            assert self.fn is not None
            self.fn(conn)


def freeze_schema(
    metadata: MetaData,
    directory: str | Path,
    *,
    version: int = 1,
    description: str = "initial schema",
    overwrite: bool = False,
) -> Path:
    """Write the SQLite DDL of ``metadata`` as ``NNNN_<description>.sql`` for release.

    The file starts with ``-- futrisk:accept-checksum`` lines for the
    ``create_all`` checksums (current and legacy), so databases created during
    development with ``Migration.create_all(version, ...)`` keep opening.
    """
    d = Path(directory)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{version:04d}_{description.strip().replace(' ', '_')}.sql"
    if path.exists() and not overwrite:
        raise FileExistsError(f"{path} exists; released migrations must not change")
    accepted = [_sha256(schema_fingerprint(metadata)), _sha256(render_ddl(metadata))]
    header = [
        f"-- {description} (frozen from SQLAlchemy MetaData by futrisk.db.migrations.freeze_schema)",
        *[f"-- futrisk:accept-checksum {c}" for c in accepted],
        "",
    ]
    # IF NOT EXISTS: the runner creates schema_version itself before running migrations
    path.write_text("\n".join(header) + render_ddl(metadata, if_not_exists=True), encoding="utf-8")
    return path


def load_sql_migrations(directory: str | Path) -> list[Migration]:
    """Load every ``NNNN_*.sql`` file of ``directory`` ordered by version."""
    d = Path(directory)
    migrations = [Migration.from_sql_file(p) for p in sorted(d.glob("*.sql"))]
    return sorted(migrations, key=lambda m: m.version)


@dataclass(frozen=True)
class AppliedMigration:
    version: int
    description: str
    checksum: str
    applied_ns: int


class MigrationRunner:
    """Validates the recorded schema history and applies pending migrations."""

    def __init__(self, migrations: Sequence[Migration]) -> None:
        versions = [m.version for m in migrations]
        if any(v < 1 for v in versions):
            raise MigrationError("migration versions start at 1")
        if len(set(versions)) != len(versions):
            raise MigrationError(f"duplicate migration versions: {versions}")
        if versions != sorted(versions):
            raise MigrationError(f"migrations must be ordered by version: {versions}")
        self._migrations = list(migrations)
        self._table = schema_version_table()

    @property
    def migrations(self) -> list[Migration]:
        return list(self._migrations)

    @property
    def latest_version(self) -> int:
        return self._migrations[-1].version if self._migrations else 0

    def applied(self, conn: Connection) -> dict[int, AppliedMigration]:
        t = self._table
        self._table.create(conn, checkfirst=True)
        rows = conn.execute(select(t.c.version, t.c.description, t.c.checksum, t.c.applied_ns)).all()
        return {int(r[0]): AppliedMigration(int(r[0]), str(r[1]), str(r[2]), int(r[3])) for r in rows}

    def run(self, conn: Connection, *, now_ns: int) -> list[int]:
        """Verify checksums and apply pending migrations; returns applied versions.

        Must be called inside a write transaction.
        """
        applied = self.applied(conn)
        known = {m.version: m for m in self._migrations}
        for version, rec in sorted(applied.items()):
            mig = known.get(version)
            if mig is None:
                raise MigrationError(
                    f"database has migration {version} ({rec.description!r}) unknown to this build "
                    f"(latest known: {self.latest_version}); refusing to start"
                )
            if mig.checksum != rec.checksum and rec.checksum in mig.alt_checksums:
                conn.execute(update(self._table).where(self._table.c.version == version).values(checksum=mig.checksum))
                logger.info("migration.checksum_upgraded", version=version, description=mig.description)
            elif mig.checksum != rec.checksum:
                raise MigrationError(
                    f"checksum mismatch for migration {version} ({mig.description!r}): "
                    f"recorded {rec.checksum[:12]}..., code {mig.checksum[:12]}...; refusing to start. "
                    "Released migrations must not change: add a new migration instead"
                )
        max_applied = max(applied) if applied else 0
        done: list[int] = []
        for mig in self._migrations:
            if mig.version in applied:
                continue
            if mig.version < max_applied:
                raise MigrationError(
                    f"migration {mig.version} is older than the applied version {max_applied}; "
                    "migrations must be appended with increasing versions"
                )
            mig.apply(conn)
            conn.execute(
                insert(self._table).values(
                    version=mig.version,
                    description=mig.description,
                    checksum=mig.checksum,
                    applied_ns=now_ns,
                )
            )
            done.append(mig.version)
        return done


__all__ = [
    "AppliedMigration",
    "Migration",
    "MigrationError",
    "MigrationRunner",
    "freeze_schema",
    "load_sql_migrations",
    "render_ddl",
    "schema_fingerprint",
    "split_sql",
]
