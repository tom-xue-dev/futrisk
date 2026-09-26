"""Shared FastAPI dependencies: API-key authentication, RBAC and the demo-mode guard (02 §9.6)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import Request, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel

from futrisk.messages import codes
from futrisk.services.api_gateway.auth import Perm, Principal
from futrisk.services.api_gateway.gateway import Gateway
from futrisk.services.api_gateway.rest import ApiError

api_key_header = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
    description="API key; its role (VIEWER / TRADER / RISK_MANAGER / ADMIN) decides the allowed operations",
)


class Envelope(BaseModel):
    """Uniform REST body (02 §9.6)."""

    ok: bool
    code: str
    message: str
    data: Any = None
    duplicate: bool | None = None


def require(gw: Gateway, perm: Perm) -> Callable[..., Awaitable[Principal]]:
    """Dependency: a valid ``X-API-Key`` (401 otherwise; 429 after repeated failures from the same
    address) whose role holds ``perm`` (403 otherwise)."""

    async def dependency(request: Request, api_key: str | None = Security(api_key_header)) -> Principal:
        client = request.client.host if request.client is not None else ""
        principal, error = gw.check_key(api_key, client)
        if principal is None:
            assert error is not None
            raise ApiError(error.code, error.message, retry_after_ms=error.retry_after_ms)
        request.state.principal = principal
        if not principal.can(perm):
            raise ApiError(codes.FORBIDDEN, f"role {principal.role.value} may not perform {perm.value} operations")
        return principal

    return dependency


def demo_only(gw: Gateway) -> Callable[[], Awaitable[None]]:
    """Dependency: ``/demo/*`` exists only when ``app.mode=demo`` (02 §9.5, §9.6)."""

    async def dependency() -> None:
        if not gw.demo_mode():
            raise ApiError(codes.FORBIDDEN, "demo endpoints are only available in demo mode (app.mode=demo)")

    return dependency


__all__ = ["Envelope", "api_key_header", "demo_only", "require"]
