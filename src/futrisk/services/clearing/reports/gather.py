"""Report data collection and registration in the service process (01 §5.3.4, 03 §5.3, §7).

:meth:`ReportService.generate` builds a JSON-able bundle for one settlement,
renders the files in the process pool (:func:`futrisk.services.clearing.jobs.render_reports_job`)
and registers each file in ONE transaction with its ``report.ready`` FACT.
Only one generation of a ``(trading_day, version)`` runs at a time (EOD step 7
and a later regeneration are serialized), and registration is idempotent by
``(trading_day, version, type, account_id, path)``: a regenerated file keeps its
``report_id`` (its hash, size and time are updated) and ``report.ready`` is
emitted only when the file is new or its content changed.

Data sources (01 §5.3.4 table): clearing.db (settlement result, SOD
snapshots, the trade copy, 60 days of ``account_settlements``) plus, fetched
over the bus with a short timeout, ``risk.query.alerts`` / ``orders`` /
``metrics`` from the shards, ``md.events`` from md-gateway and ``svc.history``
from the supervisor. A fetch that fails leaves its section empty with the
error recorded (the report still renders; EOD never fails because of it).
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import msgspec
from loguru import logger
from sqlalchemy import Connection

from futrisk.common.clock import bj_to_ns, parse_td
from futrisk.common.ids import uuid7
from futrisk.db import Database
from futrisk.messages.base import Msg
from futrisk.messages.enums import EodPhase, OrderStatus, ReportType
from futrisk.messages.eod import ReportReady
from futrisk.messages.md import MdEventBatch, MdEventsQuery
from futrisk.messages.risk import (
    MetricSeries,
    OrderBatch,
    QueryAlerts,
    QueryMetrics,
    QueryOrders,
    RiskEventBatch,
)
from futrisk.messages.system import ServiceEventBatch, SvcHistoryRequest
from futrisk.messages.topics import REPORT_READY
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.clearing import jobs, store
from futrisk.services.clearing.pool import WorkerPool
from futrisk.services.clearing.settings import ClearingSettings

MD_GATEWAY = "md-gateway"
SUPERVISOR = "supervisor"


def file_digest(path: Path) -> tuple[str, int]:
    data = path.read_bytes()
    return hashlib.sha256(data).hexdigest(), len(data)


class ReportService:
    """Builds, renders and registers reports."""

    def __init__(
        self,
        rt: AsyncServiceRuntime,
        db: Database,
        settings: ClearingSettings,
        *,
        shards: Sequence[str],
        pool: WorkerPool,
    ) -> None:
        self._rt = rt
        self._db = db
        self._settings = settings
        self._shards = list(shards)
        self._pool = pool
        self._locks: dict[tuple[str, int], asyncio.Lock] = {}

    @property
    def home(self) -> Path:
        assert self._rt.layout is not None
        return self._rt.layout.home

    # ------------------------------------------------------------ generation
    def _lock(self, trading_day: str, version: int) -> asyncio.Lock:
        key = (trading_day, version)
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    async def generate(self, trading_day: str, version: int) -> list[ReportReady]:
        """Render and register every report file of ``(trading_day, version)`` (one generation at a time)."""
        async with self._lock(trading_day, version):
            bundle = await self.bundle(trading_day, version)
            files: list[dict[str, Any]] = await self._pool.run(jobs.render_reports_job, bundle, str(self.home))
            out: list[ReportReady] = []
            for f in files:
                out.append(
                    await self.register(
                        ReportType(f["type"]),
                        f["path"],
                        trading_day=trading_day,
                        account_id=f.get("account_id"),
                        version=version,
                        sha256=f["sha256"],
                        size_bytes=int(f["size_bytes"]),
                    )
                )
        logger.info("reports.generated", trading_day=trading_day, version=version, files=len(out))
        return out

    async def register(
        self,
        kind: ReportType,
        rel_path: str,
        *,
        trading_day: str | None,
        account_id: str | None = None,
        version: int | None = None,
        sha256: str | None = None,
        size_bytes: int | None = None,
        extra_tx: Callable[[Connection, ReportReady], None] | None = None,
    ) -> ReportReady:
        """Register a ``reports`` row and its ``report.ready`` FACT in one transaction (03 §7 报表).

        Idempotent by ``(trading_day, version, type, account_id, path)`` (:func:`store.upsert_report`).
        """
        if sha256 is not None and size_bytes is not None:
            digest, size = sha256, size_bytes
        else:
            digest, size = await asyncio.to_thread(file_digest, self.home / rel_path)
        report = ReportReady(
            report_id=uuid7(),
            trading_day=trading_day,
            type=kind,
            account_id=account_id,
            path=rel_path,
            sha256=digest,
            size_bytes=size,
            version=version,
            created_ns=self._rt.market_clock.now_ns(),
        )

        def tx(conn: Connection) -> ReportReady:
            registered, changed = store.upsert_report(conn, report)
            if extra_tx is not None:
                extra_tx(conn, registered)
            if changed:
                self._rt.append_fact(conn, REPORT_READY, registered, td=trading_day, corr_id=registered.report_id)
            return registered

        return await self._db.run_write(tx)

    # ---------------------------------------------------------------- bundle
    async def bundle(self, trading_day: str, version: int) -> dict[str, Any]:
        cfg = self._rt.config
        days = self._settings.history_days

        def q(c: Connection) -> dict[str, Any]:
            run = store.get_run(c, trading_day, version)
            if run is None:
                raise LookupError(f"no EOD run {trading_day} v{version}")
            result = store.load_settlement(c, trading_day, version)
            if result is None:
                raise LookupError(f"no settlement for {trading_day} v{version}")
            sod_v = run.log(EodPhase.PRICES_FIXED).get("sod_version")
            next_v = run.log(EodPhase.SETTLED).get("sod_version")
            sod = store.load_sod(c, trading_day, int(sod_v)) if sod_v is not None else None
            nxt = store.load_sod(c, run.next_trading_day, int(next_v)) if next_v is not None else None
            return {
                "run": run,
                "settlement": result,
                "sod": sod,
                "next_sod": nxt,
                "trades": store.load_trades(c, trading_day),
                "history": store.account_history(c, trading_day, days),
                "max_dd": store.max_drawdowns(c, trading_day),
            }

        data = await self._db.run_read(q)
        run = data["run"]
        result = data["settlement"]
        accounts = [a.account_id for a in result.accounts]
        alerts, orders, metrics, md_events, svc = await asyncio.gather(
            self._alerts(trading_day),
            self._orders(trading_day, accounts),
            self._metrics(trading_day, accounts),
            self._fetch(MD_GATEWAY, "md.events", MdEventsQuery(trading_day=trading_day), MdEventBatch),
            self._fetch(SUPERVISOR, "svc.history", SvcHistoryRequest(trading_day=trading_day), ServiceEventBatch),
        )
        instruments = self._instrument_info({p.instrument_id for p in result.prices} | {
            p.instrument_id for p in result.positions
        })
        night_from, open_ns, close_ns = self._day_window(trading_day)
        return {
            "trading_day": trading_day,
            "version": version,
            "next_trading_day": run.next_trading_day,
            "generated_ns": self._rt.market_clock.now_ns(),
            "config_hash": run.config_hash,
            "config_version": run.log(EodPhase.BEGIN).get("config_version", cfg.config_version),
            "triggered_by": run.triggered_by,
            "phases": {k: v.get("ts_ns") for k, v in run.step_log.items() if isinstance(v, dict) and v.get("ts_ns")},
            "night_from": night_from,
            "open_ns": open_ns,
            "close_ns": close_ns,
            "settlement": msgspec.to_builtins(result),
            "trades": msgspec.to_builtins(data["trades"]),
            "sod": msgspec.to_builtins(data["sod"]) if data["sod"] is not None else None,
            "next_sod": msgspec.to_builtins(data["next_sod"]) if data["next_sod"] is not None else None,
            "accounts": {
                a: {
                    "name": spec.name,
                    "shard": spec.shard,
                    "initial_capital": str(spec.initial_capital),
                    "strategy": spec.strategy,
                }
                for a, spec in sorted(cfg.accounts.accounts.items())
            },
            "instruments": instruments,
            "rules": [r.model_dump(mode="json") for r in cfg.risk_rules.rules],
            "history": data["history"],
            "max_dd": data["max_dd"],
            "alerts": alerts,
            "orders": orders,
            "metrics": metrics,
            "md_events": _section(md_events, "items"),
            "svc_history": _section(svc, "items"),
        }

    def _instrument_info(self, extra: set[str]) -> dict[str, dict[str, Any]]:
        cfg = self._rt.config
        out: dict[str, dict[str, Any]] = {}
        ids = set(cfg.products.instrument_ids()) | extra
        for inst in sorted(ids):
            try:
                spec = cfg.products.product_of(inst)
            except KeyError:
                continue
            contract = cfg.products.instrument(inst)
            out[inst] = {
                "product": contract.product if contract is not None else "".join(ch for ch in inst if ch.isalpha()),
                "name": spec.name,
                "sector": spec.sector,
                "exchange": spec.exchange.value,
                "multiplier": spec.multiplier,
                "limit_pct": str(spec.limit_pct),
            }
        return out

    def _day_window(self, trading_day: str) -> tuple[str | None, int | None, int | None]:
        """Night session start label and the day's first open / last close (market time)."""
        cfg = self._rt.config
        cal = cfg.calendar
        d = parse_td(trading_day)
        opens = []
        closes = []
        for spec in cfg.products.products.values():
            o = cal.first_open(spec.sessions, d)
            c = cal.day_close(spec.sessions, d)
            if o is not None:
                opens.append(o)
            if c is not None:
                closes.append(c)
        night_from = None
        if cal.has_night_session(d):
            night = [o for o in opens if o.date() < d]
            if night:
                night_from = min(night).strftime("%Y-%m-%d %H:%M")
        return (
            night_from,
            bj_to_ns(min(opens)) if opens else None,
            bj_to_ns(max(closes)) if closes else None,
        )

    # --------------------------------------------------------------- fetches
    async def _fetch(self, target: str, command: str, payload: Msg, cls: type[Any]) -> tuple[Any | None, str | None]:
        try:
            reply = await self._rt.request(
                target,
                command,
                payload,
                timeout_ms=self._settings.report_fetch_timeout_ms,
                retries=self._settings.report_fetch_retries,
            )
        except Exception as exc:  # pragma: no cover - transport errors surface as replies
            return None, f"{target} {command}: {type(exc).__name__}: {exc}"
        if not reply.ok:
            return None, f"{target} {command}: {reply.code}{(' ' + reply.message) if reply.message else ''}"
        try:
            return reply.open_as(cls), None
        except (msgspec.DecodeError, msgspec.ValidationError) as exc:
            return None, f"{target} {command}: bad reply ({exc})"

    async def _alerts(self, trading_day: str) -> dict[str, Any]:
        results = await asyncio.gather(
            *(self._fetch(s, "risk.query.alerts", QueryAlerts(trading_day=trading_day), RiskEventBatch) for s in self._shards)
        )
        items: list[Any] = []
        errors: list[str] = []
        for batch, err in results:
            if err:
                errors.append(err)
            elif batch is not None:
                items.extend(msgspec.to_builtins(e) for e in batch.items)
        return {"items": items, "error": "; ".join(errors) or None}

    def _shard_of(self, account_id: str) -> str | None:
        cfg = self._rt.config
        spec = cfg.accounts.accounts.get(account_id)
        if spec is None:
            return None
        inst = cfg.services.shard_instance(spec.shard)
        return inst if inst in self._shards else None

    async def _orders(self, trading_day: str, accounts: Sequence[str]) -> dict[str, Any]:
        wanted = [OrderStatus.REJECTED, OrderStatus.BROKER_REJECTED]
        calls = []
        for acct in accounts:
            shard = self._shard_of(acct)
            if shard is not None:
                calls.append(
                    self._fetch(
                        shard,
                        "risk.query.orders",
                        QueryOrders(account_id=acct, trading_day=trading_day, status=wanted),
                        OrderBatch,
                    )
                )
        items: list[Any] = []
        errors: list[str] = []
        for batch, err in await asyncio.gather(*calls):
            if err:
                errors.append(err)
            elif batch is not None:
                items.extend(msgspec.to_builtins(o) for o in batch.items)
        return {"items": items, "error": "; ".join(sorted(set(errors))) or None}

    async def _metrics(self, trading_day: str, accounts: Sequence[str]) -> dict[str, Any]:
        calls = []
        keys = []
        for acct in accounts:
            shard = self._shard_of(acct)
            if shard is not None:
                keys.append(acct)
                calls.append(
                    self._fetch(
                        shard,
                        "risk.query.metrics",
                        QueryMetrics(account_id=acct, trading_day=trading_day, step_s=self._settings.metrics_step_s),
                        MetricSeries,
                    )
                )
        series: dict[str, Any] = {}
        errors: list[str] = []
        for acct, (res, err) in zip(keys, await asyncio.gather(*calls), strict=True):
            if err:
                errors.append(err)
            elif res is not None:
                series[acct] = msgspec.to_builtins(res.points)
        return {"series": series, "error": "; ".join(sorted(set(errors))) or None}


def _section(res: tuple[Any | None, str | None], attr: str) -> dict[str, Any]:
    obj, err = res
    items = msgspec.to_builtins(getattr(obj, attr)) if obj is not None else []
    return {"items": items, "error": err}


__all__ = ["ReportService", "file_digest"]
