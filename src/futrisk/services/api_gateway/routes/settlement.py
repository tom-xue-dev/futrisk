"""Settlement, trade import, report and backtest routes (02 §9.6, §6.5, §6.4).

Report files are read directly from the read-only ``reports/`` / ``backtests/``
volume (03 DB-01): ``report.get`` gives the path relative to ``<home>``; the
gateway only serves files inside those two directories, with
``X-Content-Type-Options: nosniff`` and ``Content-Security-Policy: sandbox``
(a report is rendered as an inert document: no scripts, no same-origin access).

``GET /settlement/{td}/statements/{account}`` answers
``data = {"format": "text", "text": ...}`` (the statement file) or, while no
statement file exists, ``{"format": "settlement", "settlement": SettlementResult}``.
"""

from __future__ import annotations

import asyncio
import mimetypes
from pathlib import Path
from typing import Any

import msgspec
from fastapi import APIRouter, Depends, Query, Request
from starlette.responses import FileResponse, Response

from futrisk.messages import codes
from futrisk.messages.base import to_builtins
from futrisk.messages.enums import ReportType
from futrisk.messages.envelope import Reply
from futrisk.messages.eod import (
    BacktestListRequest,
    BacktestRunRequest,
    BacktestStatusRequest,
    EodPhaseEvent,
    EodStatusRequest,
    OfficialPrice,
    OfficialPriceImport,
    PriceOverride,
    ReportGetRequest,
    ReportList,
    ReportListRequest,
    ReportReady,
    SeedSodRequest,
    SettleQuery,
    SettleRunRequest,
    SodAccount,
)
from futrisk.messages.orders import Trade, TradeImport
from futrisk.services.api_gateway.auth import Perm, Principal
from futrisk.services.api_gateway.gateway import Gateway
from futrisk.services.api_gateway.models import (
    BacktestBody,
    OfficialPricesBody,
    PriceOverrideBody,
    SeedSodBody,
    SettleRunBody,
    TradeImportBody,
)
from futrisk.services.api_gateway.rest import ApiError, ApiResult
from futrisk.services.api_gateway.router import CLEARING, TRADE_GATEWAY
from futrisk.services.api_gateway.routes.deps import Envelope, require

TD_PATH = r"^\d{8}$"
SETTLE_RUN_TIMEOUT_MS = 10_000  # one attempt: settle.run only starts the EOD saga (review L1)
REPORT_HEADERS = {"X-Content-Type-Options": "nosniff", "Content-Security-Policy": "sandbox"}


def _td(td: str) -> str:
    if len(td) != 8 or not td.isdigit():
        raise ApiError(codes.BAD_REQUEST, "trading day must be YYYYMMDD")
    return td


def _hardened(resp: Response) -> Response:
    """Report content (statements) is never sniffed or run as active content (L15)."""
    resp.headers.update(REPORT_HEADERS)
    return resp


def resolve_report_path(gw: Gateway, rel: str) -> Path:
    """Absolute path of a report inside ``reports/`` or ``backtests/`` (path traversal is refused)."""
    home = gw.layout.home.resolve()
    path = (home / rel).resolve()
    allowed = (gw.layout.reports_dir.resolve(), gw.layout.backtests_dir.resolve())
    if not any(path == a or path.is_relative_to(a) for a in allowed):
        raise ApiError(codes.FORBIDDEN, f"report path {rel!r} is outside the reports volume")
    return path


def build(gw: Gateway) -> APIRouter:
    r = APIRouter(tags=["settlement"])
    read = Depends(require(gw, Perm.READ))
    risk = Depends(require(gw, Perm.RISK))
    admin = Depends(require(gw, Perm.ADMIN))

    # ----------------------------------------------------------- settlement
    @r.post("/settlement/run", response_model=Envelope, summary="settle.run (ADMIN)")
    async def run_settlement(body: SettleRunBody, request: Request, p: Principal = admin) -> Response:
        async def run(cid: str) -> ApiResult:
            req = SettleRunRequest(command_id=cid, trading_day=body.td, force_new_version=body.force_new_version,
                                   actor=p.key_id)
            return ApiResult.from_reply(await gw.router.request(CLEARING, "settle.run", req,
                                                                timeout_ms=SETTLE_RUN_TIMEOUT_MS, retries=0))

        return await gw.mutate(request, p, command="settle.run", target=body.td,
                               body=body.model_dump(mode="json"), run=run)

    @r.post("/settlement/seed-sod", response_model=Envelope, summary="settle.seed_sod (ADMIN)")
    async def seed_sod(body: SeedSodBody, request: Request, p: Principal = admin) -> Response:
        try:
            accounts = msgspec.convert(body.accounts, list[SodAccount])
        except (msgspec.ValidationError, TypeError) as exc:
            raise ApiError(codes.BAD_REQUEST, f"accounts: {exc}") from exc

        async def run(cid: str) -> ApiResult:
            req = SeedSodRequest(command_id=cid, trading_day=body.trading_day, accounts=accounts,
                                 pre_settle=dict(body.pre_settle), reason=body.reason, actor=p.key_id, role=p.role)
            return ApiResult.from_reply(await gw.router.request(CLEARING, "settle.seed_sod", req))

        return await gw.mutate(request, p, command="settle.seed_sod", target=body.trading_day,
                               body=body.model_dump(mode="json"), run=run)

    @r.get("/settlement/days", response_model=Envelope,
           summary="Recent trading days with their latest EOD phase (settle.status per day)")
    async def settlement_days(limit: int = Query(30, ge=1, le=120), p: Principal = read) -> Response:
        tds = gw.recent_trading_days(limit)
        sem = asyncio.Semaphore(8)

        async def one(day: str) -> tuple[str, Reply]:
            async with sem:
                return day, await gw.router.request(CLEARING, "settle.status", EodStatusRequest(trading_day=day),
                                                    timeout_ms=2000, retries=0)

        items: dict[str, dict[str, Any]] = {}
        errors: dict[str, str] = {}
        rm = gw.readmodel
        for day, reply in await asyncio.gather(*(one(d) for d in tds)):
            ev = reply.open_as(EodPhaseEvent) if reply.ok else None
            if ev is not None:
                count = ev.detail.get("warning_count") if isinstance(ev.detail, dict) else None
                items[day] = {"trading_day": ev.trading_day, "version": ev.version, "phase": str(ev.phase),
                              "warning_count": int(count) if isinstance(count, int) else 0}
            elif reply.code != codes.NOT_FOUND:
                errors[day] = reply.code
        for day, ev in rm.eod_phase.items():  # live sys.eod, e.g. while clearing is busy
            if day in tds and day not in items:
                items[day] = {"trading_day": ev.trading_day, "version": ev.version, "phase": str(ev.phase),
                              "warning_count": len(rm.eod_warnings_of(day, ev.version))}
        ordered = [items[d] for d in sorted(items, reverse=True)]
        msg = f"clearing-worker unavailable for {len(errors)} day(s)" if errors else ""
        return ApiResult.success({"items": ordered, "errors": errors}, msg).response()

    @r.get("/settlement/{td}", response_model=Envelope, summary="Settlement result (settle.query)")
    async def settlement(td: str, account: str | None = None, version: int | None = None,
                         p: Principal = read) -> Response:
        req = SettleQuery(trading_day=_td(td), account_id=account, version=version)
        return ApiResult.from_reply(await gw.router.request(CLEARING, "settle.query", req)).response()

    @r.get("/settlement/{td}/status", response_model=Envelope,
           summary="Latest EOD phase with warning_count / warnings (settle.status; read model while it is down)")
    async def settlement_status(td: str, p: Principal = read) -> Response:
        day = _td(td)
        reply = await gw.router.request(CLEARING, "settle.status", EodStatusRequest(trading_day=day))
        res = ApiResult.from_reply(reply)  # detail.warning_count / detail.warnings pass through as sent
        ev = gw.readmodel.eod_phase.get(day)
        if not res.ok and res.code in codes.RETRYABLE and ev is not None:
            # clearing-worker unreachable: the latest sys.eod phase and the WARN events seen live
            data = to_builtins(ev)
            warns = gw.readmodel.eod_warnings_of(day, ev.version)
            data["detail"] = {**(data.get("detail") or {}), "warning_count": len(warns), "warnings": warns[-10:]}
            return ApiResult.success(data, f"clearing-worker {res.code}: latest phase seen on sys.eod").response()
        return res.response()

    @r.get("/settlement/{td}/statements/{account}", response_model=Envelope,
           summary="Statement: {format: text, text} from the statement file, else {format: settlement, settlement}")
    async def statement(td: str, account: str, p: Principal = read) -> Response:
        day = _td(td)
        listing, result = await asyncio.gather(
            gw.router.request(CLEARING, "report.list", ReportListRequest(trading_day=day)),
            gw.router.request(CLEARING, "settle.query", SettleQuery(trading_day=day, account_id=account)),
        )
        report: ReportReady | None = None
        if listing.ok:
            lst = listing.open_as(ReportList)
            cands = [x for x in (lst.items if lst else [])
                     if x.type == ReportType.STATEMENT_TXT and x.account_id == account]
            cands.sort(key=lambda x: (x.version or 0, x.created_ns))
            if cands:
                got = await gw.router.request(CLEARING, "report.get", ReportGetRequest(report_id=cands[-1].report_id))
                report = got.open_as(ReportReady) if got.ok else cands[-1]
        if report is not None:
            path = resolve_report_path(gw, report.path)
            try:
                text = await asyncio.to_thread(path.read_text, encoding="utf-8")
            except OSError:
                text = None
            if text is not None:
                return _hardened(ApiResult.success({"format": "text", "text": text}).response())
        res = ApiResult.from_reply(result)
        if res.ok:
            res.data = {"format": "settlement", "settlement": res.data}
            res.message = "statement file not available: settlement data instead"
        return _hardened(res.response())

    @r.post("/settlement/{td}/prices/{instrument}", response_model=Envelope,
            summary="settle.override_price (RISK_MANAGER)")
    async def override_price(td: str, instrument: str, body: PriceOverrideBody, request: Request,
                             p: Principal = risk) -> Response:
        day = _td(td)

        async def run(cid: str) -> ApiResult:
            req = PriceOverride(command_id=cid, trading_day=day, instrument_id=instrument, price=body.price,
                                reason=body.reason, actor=p.key_id, role=p.role)
            return ApiResult.from_reply(await gw.router.request(CLEARING, "settle.override_price", req))

        return await gw.mutate(request, p, command="settle.override_price", target=f"{day}/{instrument}",
                               body=body.model_dump(mode="json"), run=run)

    @r.post("/settlement/{td}/official-prices", response_model=Envelope, summary="settle.import_official (ADMIN)")
    async def official_prices(td: str, body: OfficialPricesBody, request: Request, p: Principal = admin) -> Response:
        day = _td(td)

        async def run(cid: str) -> ApiResult:
            req = OfficialPriceImport(
                command_id=cid,
                trading_day=day,
                prices=[OfficialPrice(instrument_id=x.instrument_id, price=x.price) for x in body.prices],
                source=body.source,
                actor=p.key_id,
                role=p.role,
            )
            return ApiResult.from_reply(await gw.router.request(CLEARING, "settle.import_official", req))

        return await gw.mutate(request, p, command="settle.import_official", target=day,
                               body=body.model_dump(mode="json"), run=run)

    @r.post("/trades/import", response_model=Envelope, summary="td.import_trades (ADMIN)")
    async def import_trades(body: TradeImportBody, request: Request, p: Principal = admin) -> Response:
        try:
            trades = msgspec.convert(body.trades, list[Trade])
        except (msgspec.ValidationError, TypeError) as exc:
            raise ApiError(codes.BAD_REQUEST, f"trades: {exc}") from exc

        async def run(cid: str) -> ApiResult:
            req = TradeImport(command_id=cid, trading_day=body.trading_day, trades=trades, source=body.source,
                              actor=p.key_id, role=p.role)
            return ApiResult.from_reply(await gw.router.request(TRADE_GATEWAY, "td.import_trades", req,
                                                                timeout_ms=10_000))

        return await gw.mutate(request, p, command="td.import_trades", target=body.trading_day,
                               body=body.model_dump(mode="json"), run=run)

    # --------------------------------------------------------------- reports
    @r.get("/reports", response_model=Envelope, summary="Report list (report.list), optional ?kind= filter")
    async def reports(td: str | None = Query(None, pattern=TD_PATH),
                      kind: str | None = Query(None, description=" | ".join(t.value for t in ReportType)),
                      p: Principal = read) -> Response:
        wanted: ReportType | None = None
        if kind is not None:
            try:
                wanted = ReportType(kind)
            except ValueError as exc:
                raise ApiError(codes.BAD_REQUEST, f"kind must be one of {', '.join(ReportType)}") from exc
        reply = await gw.router.request(CLEARING, "report.list", ReportListRequest(trading_day=td))
        if not reply.ok or wanted is None:
            return ApiResult.from_reply(reply).response()
        lst = reply.open_as(ReportList)
        items = [x for x in (lst.items if lst else []) if x.type == wanted]
        return ApiResult.success({"items": to_builtins(items)}).response()

    @r.get("/reports/{report_id}", summary="Report file (report.get + read-only reports volume)",
           responses={200: {"content": {"text/html": {}, "text/plain": {}, "application/octet-stream": {}}}})
    async def report_file(report_id: str, p: Principal = read) -> Response:
        reply = await gw.router.request(CLEARING, "report.get", ReportGetRequest(report_id=report_id))
        if not reply.ok:
            return ApiResult.from_reply(reply).response()
        rep = reply.open_as(ReportReady)
        if rep is None:
            raise ApiError(codes.INTERNAL, "report.get returned no ReportReady")
        path = resolve_report_path(gw, rep.path)
        if not path.is_file():
            raise ApiError(codes.NOT_FOUND, f"report file {rep.path} not found on the reports volume")
        media = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        if media.startswith("text/") or media in ("application/json",):
            media = f"{media}; charset=utf-8"
        return FileResponse(path, media_type=media, filename=path.name, content_disposition_type="inline",
                            headers={"X-Report-Sha256": rep.sha256, "X-Report-Id": rep.report_id, **REPORT_HEADERS})

    # ------------------------------------------------------------- backtests
    @r.get("/backtests", response_model=Envelope, summary="Backtest jobs (backtest.list)")
    async def backtests(p: Principal = read) -> Response:
        return ApiResult.from_reply(await gw.router.request(CLEARING, "backtest.list", BacktestListRequest())).response()

    @r.post("/backtests", response_model=Envelope, summary="backtest.run (ADMIN)")
    async def run_backtest(body: BacktestBody, request: Request, p: Principal = admin) -> Response:
        async def run(cid: str) -> ApiResult:
            req = BacktestRunRequest(command_id=cid, config_name=body.config_name, overrides=body.overrides,
                                     actor=p.key_id)
            return ApiResult.from_reply(await gw.router.request(CLEARING, "backtest.run", req))

        return await gw.mutate(request, p, command="backtest.run", target=body.config_name,
                               body=body.model_dump(mode="json"), run=run)

    @r.get("/backtests/{run_id}", response_model=Envelope, summary="Backtest status (backtest.status)")
    async def backtest(run_id: str, p: Principal = read) -> Response:
        cached = gw.readmodel.backtests.get(run_id)
        reply = await gw.router.request(CLEARING, "backtest.status", BacktestStatusRequest(run_id=run_id))
        if not reply.ok and cached is not None and reply.code != codes.NOT_FOUND:
            return ApiResult.success(to_builtins(cached), f"clearing-worker {reply.code}: cached status").response()
        return ApiResult.from_reply(reply).response()

    return r


__all__ = ["build", "resolve_report_path"]
