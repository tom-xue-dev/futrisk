"""Market routes: quotes, bars, feeds, events, data-source control (02 §9.6)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from starlette.responses import Response

from futrisk.messages import codes
from futrisk.messages.base import to_builtins
from futrisk.messages.enums import MdEventKind, SourceId
from futrisk.messages.md import BarsRequest, MdEventsQuery, SourceControl, SourcesRequest
from futrisk.services.api_gateway.auth import Perm, Principal
from futrisk.services.api_gateway.gateway import Gateway
from futrisk.services.api_gateway.models import SourceBody
from futrisk.services.api_gateway.rest import ApiError, ApiResult
from futrisk.services.api_gateway.router import MD_GATEWAY
from futrisk.services.api_gateway.routes.deps import Envelope, require


def _csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    items = [v.strip() for v in value.split(",") if v.strip()]
    return items or None


def build(gw: Gateway) -> APIRouter:
    r = APIRouter(tags=["market"])
    read = Depends(require(gw, Perm.READ))
    admin = Depends(require(gw, Perm.ADMIN))

    @r.get("/market/quotes", response_model=Envelope, summary="Latest LIVE ticks (read model md.tick.*)")
    async def quotes(instruments: str | None = Query(None, description="comma separated"),
                     p: Principal = read) -> Response:
        wanted = _csv(instruments)
        ticks = gw.readmodel.ticks
        names = sorted(ticks) if wanted is None else [i for i in wanted if i in ticks]
        return ApiResult.success([to_builtins(ticks[n]) for n in names]).response()

    @r.get("/market/bars", response_model=Envelope, summary="1-minute bars (md.bars)")
    async def bars(instrument: str = Query(...), td: str | None = Query(None, pattern=r"^\d{8}$"),
                   from_ns: int | None = None, to_ns: int | None = None, p: Principal = read) -> Response:
        req = BarsRequest(instrument_id=instrument, trading_day=td or gw.trading_day(), from_ns=from_ns, to_ns=to_ns)
        return ApiResult.from_reply(await gw.router.request(MD_GATEWAY, "md.bars", req)).response()

    @r.get("/market/feeds", response_model=Envelope,
           summary="Source trust states and instrument quality (md.status) + stale/age_ms of md-gateway")
    async def feeds(p: Principal = read) -> Response:
        live = gw.instance_liveness(MD_GATEWAY)  # the last md.status of a down md-gateway is not live data
        if gw.readmodel.md_status is not None:
            return ApiResult.success({**to_builtins(gw.readmodel.md_status), **live}).response()
        reply = await gw.router.request(MD_GATEWAY, "md.sources", SourcesRequest(), timeout_ms=1500, retries=0)
        res = ApiResult.from_reply(reply)
        if res.ok and isinstance(res.data, dict):
            res.data = {**res.data, **live}
        return res.response()

    @r.get("/market/events", response_model=Envelope, summary="Market-data events of a trading day (md.events)")
    async def events(td: str | None = Query(None, pattern=r"^\d{8}$"), kind: str | None = None,
                     instrument: str | None = None, p: Principal = read) -> Response:
        kinds = None
        if kind:
            try:
                kinds = [MdEventKind(k) for k in _csv(kind) or []]
            except ValueError as exc:
                raise ApiError(codes.BAD_REQUEST, f"unknown kind in {kind!r}") from exc
        req = MdEventsQuery(trading_day=td or gw.trading_day(), kinds=kinds, instrument_id=instrument)
        return ApiResult.from_reply(await gw.router.request(MD_GATEWAY, "md.events", req)).response()

    @r.post("/market/sources/{source}", response_model=Envelope, summary="Exclude / include a data source (ADMIN)")
    async def set_source(source: str, body: SourceBody, request: Request, p: Principal = admin) -> Response:
        try:
            sid = SourceId(source.upper())
        except ValueError as exc:
            raise ApiError(codes.BAD_REQUEST, f"unknown source {source!r} (feed_a, feed_b, sina)") from exc

        async def run(cid: str) -> ApiResult:
            req = SourceControl(command_id=cid, source=sid, action=body.action, reason=body.reason, actor=p.key_id)
            return ApiResult.from_reply(await gw.router.request(MD_GATEWAY, "md.source.set", req))

        return await gw.mutate(request, p, command="md.source.set", target=sid.value,
                               body=body.model_dump(mode="json"), run=run)

    return r


__all__ = ["build"]
