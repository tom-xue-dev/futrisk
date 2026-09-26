"""REST routes of api-gateway under ``/api/v1`` (02 §9.6)."""

from __future__ import annotations

from fastapi import APIRouter

from futrisk.services.api_gateway.gateway import Gateway
from futrisk.services.api_gateway.routes import accounts, config, demo, market, risk, settlement, system

API_PREFIX = "/api/v1"


def build_api_router(gw: Gateway) -> APIRouter:
    """All ``/api/v1`` routes bound to ``gw``."""
    api = APIRouter(prefix=API_PREFIX)
    for module in (system, market, accounts, risk, settlement, config, demo):
        api.include_router(module.build(gw))
    return api


__all__ = ["API_PREFIX", "build_api_router"]
