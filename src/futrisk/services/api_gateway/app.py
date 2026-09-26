"""FastAPI application of api-gateway (02 §9.6, §9.7; 01 §5.3.5).

* ``/api/v1/*`` REST (uniform envelope, error code -> HTTP status, RBAC by
  ``X-API-Key``); ``/openapi.json`` and Swagger UI at ``/docs``; an unknown
  ``/api/`` path answers a ``404`` envelope (``405`` for a known path with
  another method);
* ``/ws`` WebSocket push (:mod:`.wshub`);
* ``/`` the built Vue dashboard (``futrisk/web_static/dist``) when present,
  mounted after every API route.

Request guard (:class:`GuardMiddleware`, review M7/L1):

* request bodies are limited to 1 MiB (32 MiB for ``/trades/import`` and
  ``/settlement/seed-sod``): a larger ``Content-Length`` or body answers
  ``413`` (code ``BAD_REQUEST``) without buffering it;
* on mutating ``/api/`` requests (POST/PUT/PATCH/DELETE) the API key is checked
  **before** the body is read: no key, no parsing;
* read-only ``/api/`` requests run under a read deadline (every bus call at
  most 5 s, no retries, 12 s for the whole request);
* every ``/api/`` response carries ``X-Content-Type-Options: nosniff`` and
  ``Referrer-Policy: no-referrer``.

Dashboard (SPA) responses carry a Content-Security-Policy (``default-src
'self'``; inline styles allowed for Element Plus; ``data:`` images; ``ws:`` /
``wss:`` for the push channel; no framing, no ``<base>``, no plugins), ``nosniff``
and ``Referrer-Policy: no-referrer``. A sandboxed ``srcdoc`` report iframe
inherits that policy: its inline styles and ``data:`` chart images render,
scripts do not run. ``/docs`` and ``/openapi.json`` (Swagger UI from its CDN)
are left without it. Hashed build assets (``/assets/<name>-<hash>.<ext>``) are
cached for a year (``immutable``); ``index.html`` is ``no-cache``. Responses of
1 KiB and more are gzip-compressed when the client accepts it.

Unexpected exceptions answer ``500 INTERNAL`` with a generic message and an
``error_id``; the details only go to the log under that id.

Every ``/api/`` request and every WebSocket session is written to ``api_audit``
(03 §4.5): caller key id and role, method, path, forwarded command, command id
(Idempotency-Key / client_order_id), target, HTTP status, result code, latency
and client address.
"""

from __future__ import annotations

import os
import re
import time
import uuid
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request, WebSocket
from fastapi.exceptions import RequestValidationError
from fastapi.staticfiles import StaticFiles
from loguru import logger
from starlette.datastructures import Headers
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.gzip import GZipMiddleware
from starlette.responses import HTMLResponse, Response
from starlette.routing import Match
from starlette.types import ASGIApp, Message, Receive, Scope, Send

import futrisk
from futrisk import __version__
from futrisk.messages import codes
from futrisk.services.api_gateway.auth import Principal
from futrisk.services.api_gateway.gateway import Gateway
from futrisk.services.api_gateway.rest import RESULT_CODE_HEADER, ApiError, ApiResult
from futrisk.services.api_gateway.router import read_deadline
from futrisk.services.api_gateway.routes import API_PREFIX, build_api_router
from futrisk.services.api_gateway.store import AuditRecord

SPA_CSP = (
    "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; connect-src 'self' ws: wss:; "
    "frame-ancestors 'none'; base-uri 'none'; object-src 'none'"
)
API_HEADERS = [(b"x-content-type-options", b"nosniff"), (b"referrer-policy", b"no-referrer")]
SPA_HEADERS = [(b"content-security-policy", SPA_CSP.encode("latin-1")), *API_HEADERS]
DOCS_PATHS = ("/docs", "/openapi.json", "/redoc")
IMMUTABLE = "public, max-age=31536000, immutable"
GZIP_MIN_BYTES = 1024
_HASHED_ASSET = re.compile(r"-[A-Za-z0-9_-]{6,}\.[A-Za-z0-9]+$")
DEFAULT_BODY_LIMIT = 1 << 20  # 1 MiB
LARGE_BODY_LIMIT = 32 << 20  # 32 MiB: trade imports and SOD seeds
LARGE_BODY_PATHS = frozenset({f"{API_PREFIX}/trades/import", f"{API_PREFIX}/settlement/seed-sod"})
MUTATING = frozenset({"POST", "PUT", "PATCH", "DELETE"})

_STATUS_CODES = {
    200: codes.OK,
    400: codes.BAD_REQUEST,
    401: codes.UNAUTHORIZED,
    403: codes.FORBIDDEN,
    404: codes.NOT_FOUND,
    405: codes.BAD_REQUEST,
    409: codes.CONFLICT,
    413: codes.BAD_REQUEST,
    422: codes.BAD_REQUEST,
    429: codes.BUSY,
    503: codes.UNAVAILABLE,
    504: codes.TIMEOUT,
}


def static_dir() -> Path:
    """Where the dashboard build lands (``web/`` builds into ``src/futrisk/web_static/dist``)."""
    return Path(futrisk.__file__).resolve().parent / "web_static" / "dist"


def code_of_status(status: int) -> str:
    if 200 <= status < 400:
        return codes.OK
    return _STATUS_CODES.get(status, codes.INTERNAL)


class AuditMiddleware:
    """Pure ASGI middleware writing one ``api_audit`` row per ``/api/`` request."""

    def __init__(self, app: ASGIApp, gw: Gateway) -> None:
        self.app = app
        self.gw = gw

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not str(scope.get("path", "")).startswith("/api/"):
            await self.app(scope, receive, send)
            return
        state: dict[str, Any] = scope.setdefault("state", {})
        started = time.perf_counter()
        status = 500
        result_code: str | None = None

        async def send_wrapper(message: Message) -> None:
            nonlocal status, result_code
            if message["type"] == "http.response.start":
                status = int(message["status"])
                for k, v in message.get("headers", []):
                    if k.decode("latin-1").lower() == RESULT_CODE_HEADER.lower():
                        result_code = v.decode("latin-1")
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            self._audit(scope, state, status, result_code, started)

    def _audit(self, scope: Scope, state: dict[str, Any], status: int, result_code: str | None, started: float) -> None:
        principal: Principal | None = state.get("principal")
        info: dict[str, Any] = state.get("audit") or {}
        client = scope.get("client")
        path = str(scope.get("path", ""))
        if scope.get("query_string"):
            path = f"{path}?{scope['query_string'].decode('latin-1')}"
        rec = AuditRecord(
            ts_ns=self.gw.clock.now_ns(),
            key_id=principal.key_id if principal else None,
            role=principal.role.value if principal else None,
            method=str(scope.get("method", "")),
            path=path,
            http_status=status,
            result_code=state.get("result_code") or result_code or code_of_status(status),
            latency_us=int((time.perf_counter() - started) * 1e6),
            client_ip=str(client[0]) if client else "",
            command=info.get("command"),
            command_id=info.get("command_id"),
            target=info.get("target"),
        )
        try:
            fut = self.gw.store.submit_audit(rec)
        except RuntimeError:  # database closed during shutdown
            return
        fut.add_done_callback(_log_audit_failure)


class GuardMiddleware:
    """Body-size limit, key check before any body is read, read deadline, ``nosniff`` (see module docstring)."""

    def __init__(
        self,
        app: ASGIApp,
        gw: Gateway,
        *,
        body_limit: int = DEFAULT_BODY_LIMIT,
        large_body_limit: int = LARGE_BODY_LIMIT,
        large_paths: frozenset[str] = LARGE_BODY_PATHS,
    ) -> None:
        self.app = app
        self.gw = gw
        self.body_limit = body_limit
        self.large_body_limit = large_body_limit
        self.large_paths = large_paths

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = str(scope.get("path", ""))
        method = str(scope.get("method", "GET")).upper()
        api = path.startswith("/api/")
        if api:
            send = _with_headers(send, API_HEADERS)
        elif not path.startswith(DOCS_PATHS):
            send = _with_headers(send, SPA_HEADERS)  # the dashboard and everything it loads
        headers = Headers(scope=scope)
        limit = self.large_body_limit if path in self.large_paths else self.body_limit
        length = headers.get("content-length")
        if length is not None:
            if not length.strip().isdigit():
                await _respond(scope, receive, send, ApiResult.failure(codes.BAD_REQUEST, "bad Content-Length"))
                return
            if int(length) > limit:
                await _respond(scope, receive, send, _too_large(limit))
                return
        if api and method in MUTATING:
            client = scope.get("client")
            principal, error = self.gw.check_key(headers.get("x-api-key"), str(client[0]) if client else "")
            if error is not None:
                await _respond(scope, receive, send, error)  # the body is never read without a valid key
                return
            body = bytearray()
            while True:
                message = await receive()
                if message["type"] == "http.disconnect":
                    return
                body += message.get("body", b"")
                if len(body) > limit:
                    await _respond(scope, receive, send, _too_large(limit))
                    return
                if not message.get("more_body", False):
                    break
            receive = _replay(bytes(body), receive)
        if api and method in ("GET", "HEAD"):
            with read_deadline():
                await self.app(scope, receive, send)
        else:
            await self.app(scope, receive, send)


def _too_large(limit: int) -> ApiResult:
    res = ApiResult.failure(codes.BAD_REQUEST, f"request body larger than {limit} bytes")
    res.status_override = 413
    return res


async def _respond(scope: Scope, receive: Receive, send: Send, res: ApiResult) -> None:
    scope.setdefault("state", {})["result_code"] = res.code
    await res.response()(scope, receive, send)


def _with_headers(send: Send, extra: list[tuple[bytes, bytes]]) -> Send:
    async def wrapper(message: Message) -> None:
        if message["type"] == "http.response.start":
            names = {k.lower() for k, _ in message.get("headers", [])}
            message = dict(message)
            message["headers"] = [*message.get("headers", []), *[(k, v) for k, v in extra if k not in names]]
        await send(message)

    return wrapper


def _replay(body: bytes, receive: Receive) -> Receive:
    """A ``receive`` that hands the already-read body to the app, then waits for the disconnect."""
    sent = False

    async def replay() -> Message:
        nonlocal sent
        if not sent:
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        return await receive()

    return replay


class SpaStaticFiles(StaticFiles):
    """The dashboard build: hashed assets cached for a year, ``index.html`` revalidated on every load."""

    def file_response(self, full_path: Any, stat_result: os.stat_result, scope: Scope,
                      status_code: int = 200) -> Response:
        resp = super().file_response(full_path, stat_result, scope, status_code)
        name = os.path.basename(str(full_path))
        rel = str(scope.get("path", ""))
        if "/assets/" in f"/{rel.lstrip('/')}" and _HASHED_ASSET.search(name):
            resp.headers["Cache-Control"] = IMMUTABLE
        else:
            resp.headers["Cache-Control"] = "no-cache"  # index.html and unhashed files: always revalidate
        return resp


class CatchAllMiddleware:
    """Turns unexpected exceptions into a scrubbed ``INTERNAL`` envelope inside the audit middleware."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = False

        async def send_wrapper(message: Message) -> None:
            nonlocal started
            if message["type"] == "http.response.start":
                started = True
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as exc:
            error_id = uuid.uuid4().hex[:12]
            logger.opt(exception=exc).error("api.unhandled", path=scope.get("path"), method=scope.get("method"),
                                            error_id=error_id)
            if started:
                raise
            scope.setdefault("state", {})["result_code"] = codes.INTERNAL
            # no exception text to the client (it may carry paths, SQL or payload values)
            response = ApiResult.failure(codes.INTERNAL, f"internal error (error id {error_id})",
                                         {"error_id": error_id}).response()
            await response(scope, receive, send)


def _log_audit_failure(fut: Any) -> None:
    exc = fut.exception()
    if exc is not None:
        logger.error("audit.write_failed", error=str(exc))


def _error(request: Request, res: ApiResult) -> Response:
    request.state.result_code = res.code
    return res.response()


def create_app(gw: Gateway, *, static: Path | None = None) -> FastAPI:
    """Build the FastAPI app bound to ``gw``; ``static`` overrides the dashboard directory."""
    app = FastAPI(
        title="futrisk api-gateway",
        version=__version__,
        description="REST + WebSocket API of the futrisk risk control and clearing system (02 §9.6, §9.7).",
        openapi_url="/openapi.json",
        docs_url="/docs",
        redoc_url=None,
    )

    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError) -> Response:
        return _error(request, exc.result)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> Response:
        detail = "; ".join(
            f"{'.'.join(str(p) for p in e.get('loc', ()))}: {e.get('msg')}" for e in exc.errors()
        )
        return _error(request, ApiResult.failure(codes.BAD_REQUEST, detail[:2000] or "invalid request"))

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> Response:
        res = ApiResult.failure(code_of_status(exc.status_code), str(exc.detail))
        if exc.status_code in (405, 413):
            res.status_override = exc.status_code
        return _error(request, res)

    # order: the last added middleware is the outermost -> GZip(Audit(Guard(CatchAll(app))))
    app.add_middleware(CatchAllMiddleware)
    app.add_middleware(GuardMiddleware, gw=gw)
    app.add_middleware(AuditMiddleware, gw=gw)
    app.add_middleware(GZipMiddleware, minimum_size=GZIP_MIN_BYTES)
    app.include_router(build_api_router(gw))

    api_routes = list(app.router.routes)  # everything registered so far (not the catch-all below)

    def allowed_methods(scope: Scope) -> list[str]:
        allowed = []
        for method in ("GET", "POST", "PUT", "PATCH", "DELETE"):
            probe = {**scope, "method": method}
            if any(r.matches(probe)[0] == Match.FULL for r in api_routes):
                allowed.append(method)
        return allowed

    @app.api_route("/api/{rest:path}", methods=["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
                   include_in_schema=False)
    async def unknown_api(request: Request, rest: str) -> Response:
        path = request.url.path
        allowed = allowed_methods(request.scope)
        if allowed:
            res = ApiResult.failure(codes.BAD_REQUEST, f"method {request.method} not allowed; use {', '.join(allowed)}")
            res.status_override = 405
            return _error(request, res)
        return _error(request, ApiResult.failure(codes.NOT_FOUND, f"unknown API path {path}"))

    @app.get("/health", include_in_schema=False)
    async def root_health() -> Response:
        return ApiResult.success({"status": "ok", "instance": gw.instance, "state": gw.state(),
                                  "home_id": gw.home_id, "pid": os.getpid(), "epoch": gw.epoch}).response()

    @app.websocket("/ws")
    async def ws_endpoint(websocket: WebSocket) -> None:
        await gw.hub.serve(websocket)

    dist = static if static is not None else static_dir()
    if (dist / "index.html").is_file():
        app.mount("/", SpaStaticFiles(directory=str(dist), html=True), name="dashboard")
    else:

        @app.get("/", include_in_schema=False)
        async def no_dashboard() -> Response:
            return HTMLResponse(
                "<h1>futrisk api-gateway</h1><p>The dashboard is not built "
                "(<code>web/</code> builds into <code>futrisk/web_static/dist</code>). "
                'API: <a href="/docs">/docs</a>.</p>'
            )

    return app


__all__ = [
    "DEFAULT_BODY_LIMIT",
    "LARGE_BODY_LIMIT",
    "SPA_CSP",
    "SpaStaticFiles",
    "AuditMiddleware",
    "CatchAllMiddleware",
    "GuardMiddleware",
    "code_of_status",
    "create_app",
    "static_dir",
]
