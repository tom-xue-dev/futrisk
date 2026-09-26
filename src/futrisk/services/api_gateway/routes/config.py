"""Config routes: effective config, version history, hot reload via 2PC (02 §9.6, §8.5)."""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter, Depends, Query, Request
from starlette.responses import Response

from futrisk.common.config import read_active
from futrisk.services.api_gateway.auth import Perm, Principal
from futrisk.services.api_gateway.gateway import Gateway
from futrisk.services.api_gateway.rest import ApiResult
from futrisk.services.api_gateway.routes.deps import Envelope, require


def config_document(gw: Gateway, active: dict[str, Any] | None) -> dict[str, Any]:
    cfg = gw.config()
    return {
        "config_version": cfg.config_version,
        "config_hash": cfg.config_hash,
        "path": (active or {}).get("path"),
        "effective": (active or {}).get("effective", {}),
        "active_version": (active or {}).get("config_version"),
        "files": dict(cfg.files),
        "sections": {
            "app": cfg.app.model_dump(mode="json"),
            "services": cfg.services.model_dump(mode="json"),
            "accounts": cfg.accounts.model_dump(mode="json"),
            "risk_rules": cfg.risk_rules.model_dump(mode="json"),
            "products": cfg.products.model_dump(mode="json"),
            "feeds": cfg.feeds.model_dump(mode="json"),
            "sim": cfg.sim.model_dump(mode="json"),
        },
    }


def build(gw: Gateway) -> APIRouter:
    r = APIRouter(tags=["config"])
    read = Depends(require(gw, Perm.READ))
    admin = Depends(require(gw, Perm.ADMIN))

    @r.get("/config", response_model=Envelope, summary="Effective configuration and its hash")
    async def get_config(p: Principal = read) -> Response:
        active = await asyncio.to_thread(read_active, gw.layout)
        return ApiResult.success(config_document(gw, active)).response()

    @r.get("/config/versions", response_model=Envelope, summary="config_versions (2PC history)")
    async def versions(limit: int = Query(100, ge=1, le=1000), p: Principal = read) -> Response:
        rows = await gw.store.list_config_versions(limit)
        return ApiResult.success([row.to_dict() for row in rows]).response()

    @r.post("/config/reload", response_model=Envelope, summary="Hot reload with two-phase commit (ADMIN)")
    async def reload(request: Request, p: Principal = admin) -> Response:
        async def run(cid: str) -> ApiResult:
            outcome = await gw.coordinator.reload(requested_by=p.key_id)
            return ApiResult(ok=outcome.ok, code=outcome.code, message=outcome.message,
                             data=outcome.row.to_dict())

        return await gw.mutate(request, p, command="config.reload", target="config", body={}, run=run)

    return r


__all__ = ["build", "config_document"]
