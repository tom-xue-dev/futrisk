"""HTTP clients of the tools CLI: api-gateway REST (02 §9.6) and the sim-exchange control API (02 §9.5).

api-gateway (:class:`ApiClient`):

* every call but ``GET /health`` sends ``X-API-Key``;
* every POST/DELETE sends a fresh UUID ``Idempotency-Key`` (UUIDv7, one per command invocation;
  02 §9.6). ``POST /orders`` is deduplicated by ``client_order_id`` instead; the CLI sends the same
  UUID in both;
* ``429`` (``BUSY`` / ``RISK_BUSY``, also the failed-authentication throttle) is retried with the
  *same* key after ``Retry-After`` (capped): a 429 is never recorded under the key (the
  ``record_all`` routes -- ``/demo/*``, the kill switch -- record the outcomes of commands that
  ran, and a 429 comes before that), so the retry cannot act twice;
* the body is the envelope ``{ok, code, message, data[, duplicate]}``; JSON floats are decoded as
  :class:`~decimal.Decimal` so the CLI prints numbers exactly as the API wrote them. ``ok=false``
  with HTTP 200 is a business outcome (``REJECT_*``, fan-out reports), not a transport error.

sim-exchange control (:class:`SimControlClient`, the fallback for an api-gateway without the
``/demo`` read routes): plain JSON documents, errors ``{"ok": false, "code", "message"}``; no key
(127.0.0.1 only, demo mode only).

Tests replace :data:`TRANSPORT` with an ``httpx.MockTransport`` and :data:`SLEEP` with a no-op.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

from futrisk.cli.ops_cmd import API_PREFIX
from futrisk.cli.toolkit import (
    TIMEOUT,
    UNAUTHORIZED,
    UNAVAILABLE,
    CliError,
    decode_json,
    dump_json,
    pretty_json,
)
from futrisk.common.ids import uuid7

DEFAULT_TIMEOUT_S = 30.0
BUSY_RETRIES = 2
MAX_RETRY_AFTER_S = 5.0

#: transport override for tests (``httpx.MockTransport``); ``None`` = real network
TRANSPORT: httpx.BaseTransport | None = None
#: sleep used between ``429`` retries and status polls (tests replace it)
SLEEP: Callable[[float], None] = time.sleep

_STATUS_CODES = {
    400: "BAD_REQUEST",
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    405: "BAD_REQUEST",
    409: "CONFLICT",
    413: "BAD_REQUEST",
    422: "BAD_REQUEST",
    429: "BUSY",
    503: "UNAVAILABLE",
    504: "TIMEOUT",
}
_ROUTER_404 = ("", "Not Found", "NOT_FOUND")  # a 404 made by the (older) gateway's router, not by a route


def new_idempotency_key() -> str:
    """A fresh UUID (v7, time-ordered) for one mutation."""
    return uuid7()


def seg(value: str) -> str:
    """A URL path segment with everything percent-encoded (``|``, ``/``, ``:`` ...)."""
    return quote(str(value), safe="")


def code_of_status(status: int) -> str:
    if 200 <= status < 400:
        return "OK"
    return _STATUS_CODES.get(status, "INTERNAL")


@dataclass
class ApiReply:
    """One REST answer (envelope fields, or synthesized ones for a non-envelope body)."""

    status: int
    ok: bool
    code: str
    message: str
    data: Any
    duplicate: bool
    raw: bytes
    envelope: bool

    def error(self, hint: str | None = None) -> CliError:
        return CliError(
            self.code,
            self.message or self.code,
            status=self.status,
            raw=self.raw if self.envelope else None,
            data=self.data,
            hint=hint,
        )

    @property
    def route_missing(self) -> bool:
        """This api-gateway has no such route (an older version): HTTP 405, or a 404 produced by
        its router (``Not Found`` / ``unknown API path ...``) -- never a route's own ``NOT_FOUND``
        such as "no scenario has been started"."""
        if self.status == 405:
            return True
        msg = self.message.strip()
        return self.status == 404 and (msg in _ROUTER_404 or msg.startswith("unknown API path"))


def parse_reply(resp: httpx.Response) -> ApiReply:
    raw = resp.content
    doc: Any = None
    if raw:
        try:
            doc = decode_json(raw)
        except ValueError:
            doc = None
    if isinstance(doc, dict) and "ok" in doc and "code" in doc:
        return ApiReply(
            status=resp.status_code,
            ok=bool(doc.get("ok")),
            code=str(doc.get("code") or ""),
            message=str(doc.get("message") or ""),
            data=doc.get("data"),
            duplicate=bool(doc.get("duplicate")),
            raw=raw,
            envelope=True,
        )
    ok = resp.is_success
    text = "" if ok else resp.text.strip()[:300]
    return ApiReply(
        status=resp.status_code,
        ok=ok,
        code=code_of_status(resp.status_code),
        message=text or ("" if ok else f"HTTP {resp.status_code}"),
        data=doc,
        duplicate=False,
        raw=raw,
        envelope=False,
    )


def retry_after_s(resp: httpx.Response) -> float:
    try:
        value = float(resp.headers.get("Retry-After", "1"))
    except ValueError:
        value = 1.0
    if not math.isfinite(value):
        value = 1.0
    return min(max(value, 0.0), MAX_RETRY_AFTER_S)


class ApiClient:
    """Blocking client of api-gateway's ``/api/v1`` (see the module docstring)."""

    def __init__(
        self,
        base_url: str,
        key: str | None,
        *,
        missing_key: str = "no API key",
        timeout_s: float = DEFAULT_TIMEOUT_S,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._key = key
        self._missing_key = missing_key
        self._timeout_s = timeout_s
        self._http = httpx.Client(base_url=self.base_url, timeout=timeout_s, trust_env=False, transport=TRANSPORT)

    def __enter__(self) -> ApiClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._http.close()

    def url(self, path: str) -> str:
        """Absolute URL of an API path (``/reports/x`` -> ``http://host:port/api/v1/reports/x``)."""
        return f"{self.base_url}{API_PREFIX}{path}"

    # ------------------------------------------------------------------ calls
    def get(self, path: str, params: Mapping[str, Any] | None = None, *, auth: bool = True, check: bool = True) -> ApiReply:
        return self.call("GET", path, params=params, auth=auth, check=check)

    def post(self, path: str, body: Any = None, *, idempotency_key: str | None = None, check: bool = True) -> ApiReply:
        return self.call(
            "POST", path, body=body if body is not None else {}, idempotency_key=idempotency_key or new_idempotency_key(),
            check=check,
        )

    def delete(
        self, path: str, params: Mapping[str, Any] | None = None, *, idempotency_key: str | None = None, check: bool = True
    ) -> ApiReply:
        return self.call(
            "DELETE", path, params=params, idempotency_key=idempotency_key or new_idempotency_key(), check=check
        )

    def call(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Any = None,
        idempotency_key: str | None = None,
        auth: bool = True,
        check: bool = True,
    ) -> ApiReply:
        """One REST call; raises :class:`CliError` on transport errors (and on ``ok=false`` with ``check``)."""
        resp = self._send(method, path, params=params, body=body, idempotency_key=idempotency_key, auth=auth)
        reply = parse_reply(resp)
        if check and not reply.ok:
            raise reply.error()
        return reply

    def download(self, path: str) -> httpx.Response:
        """A non-envelope GET (report files); an error answer raises :class:`CliError`."""
        resp = self._send("GET", path, auth=True)
        if not resp.is_success:
            raise parse_reply(resp).error()
        return resp

    def _send(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        body: Any = None,
        idempotency_key: str | None = None,
        auth: bool = True,
    ) -> httpx.Response:
        headers = {"Accept": "application/json"}
        if auth:
            if not self._key:
                raise CliError(UNAUTHORIZED, self._missing_key)
            headers["X-API-Key"] = self._key
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        query = {k: v for k, v in (params or {}).items() if v is not None}
        url = f"{API_PREFIX}{path}"
        attempt = 0
        while True:
            try:
                resp = self._http.request(method, url, params=query or None, json=body, headers=headers)
            except httpx.TimeoutException as exc:
                raise CliError(TIMEOUT, f"{method} {self.base_url}{url}: no answer within {self._timeout_s:g}s") from exc
            except httpx.HTTPError as exc:
                raise CliError(
                    UNAVAILABLE,
                    f"api-gateway unreachable at {self.base_url}: {exc}",
                    hint="is the system running? (`futrisk status`; `futrisk demo` starts it)",
                ) from exc
            if resp.status_code == 429 and attempt < BUSY_RETRIES:
                attempt += 1
                SLEEP(retry_after_s(resp))
                continue
            return resp


class SimControlClient:
    """Blocking client of ``http://{sim.control.host}:{sim.control.port}/control`` (02 §9.5)."""

    def __init__(self, base_url: str, *, timeout_s: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s
        self._http = httpx.Client(timeout=timeout_s, trust_env=False, transport=TRANSPORT)

    def __enter__(self) -> SimControlClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self._http.close()

    def call(self, method: str, path: str, body: Any = None) -> tuple[Any, bytes]:
        """``(decoded JSON, raw body)``; raises :class:`CliError` on failure."""
        url = f"{self.base_url}{path}"
        try:
            resp = self._http.request(method, url, json=body)
        except httpx.TimeoutException as exc:
            raise CliError(TIMEOUT, f"sim-exchange control {method} {path}: no answer within {self._timeout_s:g}s") from exc
        except httpx.HTTPError as exc:
            raise CliError(
                UNAVAILABLE,
                f"sim-exchange control API unreachable at {self.base_url}: {exc}",
                hint="is the demo running? (`futrisk ps`)",
            ) from exc
        raw = resp.content
        try:
            doc = decode_json(raw) if raw else None
        except ValueError:
            doc = None
        if resp.is_success:
            return doc, raw
        code, message = code_of_status(resp.status_code), f"sim-exchange answered HTTP {resp.status_code}"
        if isinstance(doc, dict):
            code = str(doc.get("code") or code)
            message = str(doc.get("message") or message)
        raise CliError(code, message, status=resp.status_code, raw=raw if isinstance(doc, dict) else None)


__all__ = [
    "BUSY_RETRIES",
    "MAX_RETRY_AFTER_S",
    "ApiClient",
    "ApiReply",
    "SimControlClient",
    "decode_json",
    "dump_json",
    "new_idempotency_key",
    "parse_reply",
    "pretty_json",
    "seg",
]
