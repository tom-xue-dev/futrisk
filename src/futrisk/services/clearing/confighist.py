"""Configuration in force per trading day (01 §6.7; 02 §7.10, §8.5; 03 §4.4 ``eod_runs.config_hash``).

Product parameters (margins, fees, ticks, limits, settlement rules) and the
calendar of a newly committed config version take effect from the NEXT trading
day (02 §7.10 ``NEXT_TRADING_DAY``), while the runtime swaps ``rt.config`` at
once. The clearing-worker therefore records every version it sees in
``config_history`` together with ``settle_from``, the first trading day its
settlement sections (``products.yaml`` and the calendar files) apply to, and
settles a day with the newest version in force on that day:

* the running version is recorded at start-up and each committed version on
  ``ctl.config.commit``; ``settle_from`` is inherited when the settlement files
  did not change, otherwise it is the ``trading_day`` that api-gateway stamped
  in ``active.json`` ``effective`` for ``products`` / ``calendar``, otherwise
  the next trading day (a commit now) or, for a change that happened while the
  worker was down, the current trading day;
* a run records the chosen version at BEGIN (``step_log.BEGIN.config_version``
  and ``eod_runs.config_hash``); steps 4 and 5 use exactly that version, taken
  from memory or loaded from ``<home>/config/versions/{N}/`` with the common
  loader (:func:`futrisk.common.config.load_config_dir`), so a commit during a
  run never changes the settlement in progress.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

from loguru import logger
from sqlalchemy import Connection

from futrisk.common.clock import td_str
from futrisk.common.config import (
    ConfigError,
    FullConfig,
    compute_config_hash,
    file_hashes,
    load_config_dir,
    read_active,
)
from futrisk.core import InstrumentRegistry
from futrisk.db import Database
from futrisk.messages.enums import ConfigEffect
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.clearing import store
from futrisk.services.clearing.store import ConfigRow

SETTLE_SECTIONS = frozenset({"products", "calendar"})
ZERO_HASH = "0" * 64


def _settle_files(files: Mapping[str, str]) -> dict[str, str]:
    return {k: v for k, v in files.items() if k == "products.yaml" or k.startswith("calendar/")}


def settle_hash(cfg: FullConfig) -> str:
    """SHA-256 over the settlement-relevant files (``products.yaml`` + calendar) of ``cfg``."""
    files = _settle_files(cfg.files)
    if files:
        return compute_config_hash(files)
    # a config assembled in memory (tests, tools): hash the parsed sections instead
    cal = cfg.calendar
    days = ",".join(td_str(d) for d in cal.trading_days_between(cal.first_day, cal.last_day))
    return compute_config_hash({"products": cfg.products.model_dump_json(), "calendar": days})


def stamped_from(active: Mapping[str, Any] | None, version: int) -> str | None:
    """``active.json`` ``effective``: the day the products/calendar of ``version`` apply from (NEXT_TRADING_DAY)."""
    if not active or int(active.get("config_version", -1)) != version:
        return None
    days = []
    for section, entry in (active.get("effective") or {}).items():
        if (
            section in SETTLE_SECTIONS
            and isinstance(entry, dict)
            and entry.get("trading_day")
            and str(entry.get("effect")) == ConfigEffect.NEXT_TRADING_DAY.value
        ):
            days.append(str(entry["trading_day"]))
    return max(days) if days else None


class ConfigHistory:
    """Records config versions and serves the configuration in force for a trading day."""

    def __init__(self, rt: AsyncServiceRuntime, db: Database) -> None:
        self._rt = rt
        self._db = db
        self._configs: dict[int, FullConfig] = {}
        self._registries: dict[int, tuple[FullConfig, InstrumentRegistry]] = {}
        self._remember(rt.config)

    def _remember(self, cfg: FullConfig) -> None:
        self._configs[cfg.config_version] = cfg

    def _active(self) -> dict[str, Any] | None:
        layout = self._rt.layout
        if layout is None:
            return None
        try:
            return read_active(layout)
        except (OSError, ValueError):
            logger.warning("config_history.active_json_unreadable")
            return None

    def _today(self, now_ns: int, *, next_day: bool) -> str | None:
        cal = self._rt.config.calendar
        try:
            td = cal.current_or_next_trading_day(now_ns)
            return td_str(cal.next_trading_day(td) if next_day else td)
        except LookupError:
            return None

    # --------------------------------------------------------------- record
    def record_tx(self, conn: Connection, cfg: FullConfig, now_ns: int, *, committed_now: bool) -> ConfigRow:
        """Record ``cfg`` (idempotent by version) and return its row."""
        rows = store.config_rows(conn)
        for r in rows:
            if r.config_version == cfg.config_version:
                return r
        latest = rows[-1] if rows else None
        sh = settle_hash(cfg)
        active = self._active()
        stamped = stamped_from(active, cfg.config_version)
        settle_from: str | None
        if latest is not None and latest.settle_hash == sh:
            settle_from = latest.settle_from
        elif stamped is not None:
            settle_from = stamped
        elif latest is None:
            settle_from = None
        else:
            settle_from = self._today(now_ns, next_day=committed_now)
        if latest is None and stamped is not None:
            self._record_previous_tx(conn, cfg.config_version - 1, now_ns)
        row = ConfigRow(
            config_version=cfg.config_version,
            config_hash=cfg.config_hash or ZERO_HASH,
            settle_hash=sh,
            settle_from=settle_from,
            recorded_ns=now_ns,
        )
        store.insert_config(conn, row)
        logger.info(
            "config_history.recorded", config_version=cfg.config_version, settle_from=settle_from, settle_hash=sh[:12]
        )
        return row

    def _record_previous_tx(self, conn: Connection, version: int, now_ns: int) -> None:
        """First start right after a NEXT_TRADING_DAY commit: the previous version is in force until then."""
        layout = self._rt.layout
        if version < 0 or layout is None:
            return
        path = layout.versions_dir / str(version)
        if not path.is_dir():
            return
        files = file_hashes(path)
        store.insert_config(
            conn,
            ConfigRow(
                config_version=version,
                config_hash=compute_config_hash(files),
                settle_hash=compute_config_hash(_settle_files(files)),
                settle_from=None,
                recorded_ns=now_ns,
            ),
        )

    async def start(self) -> ConfigRow:
        """Record the running version (after the market clock is known)."""
        cfg = self._rt.config
        now = self._rt.market_clock.now_ns()
        return await self._db.run_write(lambda c: self.record_tx(c, cfg, now, committed_now=False))

    def on_commit(self, new: FullConfig, effects: Mapping[str, ConfigEffect]) -> None:
        """``ctl.config.commit`` callback (loop thread): remember and record the new version."""
        self._remember(new)
        now = self._rt.market_clock.now_ns()
        logger.info(
            "config_history.commit",
            config_version=new.config_version,
            effects={k: str(v) for k, v in effects.items()},
        )
        self._rt.submit_write(self._db, lambda c: self.record_tx(c, new, now, committed_now=True))

    # ---------------------------------------------------------------- serve
    def for_day_tx(self, conn: Connection, trading_day: str) -> tuple[int, str]:
        """``(config_version, config_hash)`` in force on ``trading_day`` (the running one when nothing is recorded)."""
        row = store.config_for_day(conn, trading_day)
        if row is None:
            cfg = self._rt.config
            return cfg.config_version, cfg.config_hash or ZERO_HASH
        return row.config_version, row.config_hash

    async def config(self, version: int | None) -> tuple[FullConfig, str | None]:
        """The :class:`FullConfig` of ``version``; falls back to the running config with a note."""
        if version is None:
            return self._rt.config, None
        cfg = self._configs.get(version)
        if cfg is not None:
            return cfg, None
        layout = self._rt.layout
        path = layout.versions_dir / str(version) if layout is not None else None
        note: str
        if path is not None and path.is_dir():
            try:
                loaded = await asyncio.to_thread(load_config_dir, path, version)
            except ConfigError as exc:
                note = f"config version {version} cannot be loaded ({exc})"
            else:
                self._configs[version] = loaded
                return loaded, None
        else:
            note = f"config version {version} not found under {path}"
        running = self._rt.config
        note += f"; settling with the running version {running.config_version}"
        logger.warning("config_history.fallback", version=version, note=note)
        return running, note

    def registry(self, cfg: FullConfig) -> InstrumentRegistry:
        """Instrument registry of ``cfg`` (cached per version)."""
        cached = self._registries.get(cfg.config_version)
        if cached is not None and cached[0] is cfg:
            return cached[1]
        reg = InstrumentRegistry(cfg.products, cfg.calendar)
        self._registries[cfg.config_version] = (cfg, reg)
        return reg


__all__ = ["SETTLE_SECTIONS", "ConfigHistory", "settle_hash", "stamped_from"]
