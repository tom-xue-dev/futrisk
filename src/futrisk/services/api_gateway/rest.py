"""REST response envelope and error-code -> HTTP status mapping (02 §9.6).

Every REST answer has the uniform body ``{"ok", "code", "message", "data"}``
(plus ``"duplicate": true`` when an idempotent request was replayed). Every
code of :mod:`futrisk.messages.codes` has an explicit HTTP status (02 §9.6
table, completed for the codes it lists as "other"); ``INTERNAL`` is the only
500:

==============================================================  ====
code                                                            HTTP
==============================================================  ====
``OK``; business outcomes ``REJECT_*``, ``MD_REJECT_*``,         200
``BROKER_*``, ``ROUTE_*`` (``ok=false``)
``BAD_REQUEST``                                                 400
``UNAUTHORIZED``                                                401
``FORBIDDEN``                                                   403
``NOT_FOUND``                                                   404
``CONFLICT``, ``NOT_OWNER``, ``PRECONDITION_FAILED``            409
``BUSY``, ``RISK_BUSY``                                         429 + ``Retry-After``
``INTERNAL``                                                    500
``VERSION_UNSUPPORTED`` (a downstream service speaks another    502
protocol version)
``UNAVAILABLE``                                                 503 + ``Retry-After``
``TIMEOUT``                                                     504
==============================================================  ====

Exceptions: fan-out commands (kill switch, EOD force-unfreeze) always answer
HTTP 200: ``ok`` is true only when every target confirmed, ``code`` is the
worst target code and ``data.targets`` has the per-target results (a
target's code never becomes the HTTP status). A body over the size limit is
HTTP 413 with code ``BAD_REQUEST``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import msgspec
from starlette.responses import JSONResponse

from futrisk.messages import codes
from futrisk.messages.base import to_builtins
from futrisk.messages.envelope import Reply

_STATUS: dict[str, int] = {
    codes.OK: 200,
    codes.BAD_REQUEST: 400,
    codes.UNAUTHORIZED: 401,
    codes.FORBIDDEN: 403,
    codes.NOT_FOUND: 404,
    codes.CONFLICT: 409,
    codes.NOT_OWNER: 409,
    codes.PRECONDITION_FAILED: 409,
    codes.BUSY: 429,
    codes.RISK_BUSY: 429,
    codes.INTERNAL: 500,
    codes.VERSION_UNSUPPORTED: 502,
    codes.UNAVAILABLE: 503,
    codes.TIMEOUT: 504,
}
BUSINESS_PREFIXES = ("REJECT_", "MD_REJECT_", "BROKER_", "ROUTE_")
RETRY_AFTER_STATUSES = (429, 503)

_json_encoder = msgspec.json.Encoder()
RESULT_CODE_HEADER = "X-Result-Code"


def http_status(code: str) -> int:
    """HTTP status of a Reply/REST code (02 §9.6); an unknown code is treated as ``INTERNAL``."""
    if code.startswith(BUSINESS_PREFIXES):
        return 200
    return _STATUS.get(code, 500)


class MsgspecJSONResponse(JSONResponse):
    """JSON response rendered with msgspec (Structs, enums, non-finite floats as null)."""

    def render(self, content: Any) -> bytes:
        return _json_encoder.encode(content)


def reply_data(reply: Reply) -> Any:
    """Decoded ``Reply.data`` as JSON builtins (None when empty or of an unknown type)."""
    if reply.data is None or reply.data_type is None:
        return None
    try:
        return to_builtins(reply.open())
    except (KeyError, msgspec.DecodeError, msgspec.ValidationError):
        return None


@dataclass
class ApiResult:
    """Outcome of one REST operation, rendered as the uniform envelope."""

    ok: bool = True
    code: str = codes.OK
    message: str = ""
    data: Any = None
    retry_after_ms: int | None = None
    duplicate: bool = False
    status_override: int | None = None  # explicit HTTP status (fan-out reports, 413)

    @classmethod
    def success(cls, data: Any = None, message: str = "") -> ApiResult:
        return cls(ok=True, code=codes.OK, message=message, data=data)

    @classmethod
    def failure(cls, code: str, message: str = "", data: Any = None, retry_after_ms: int | None = None) -> ApiResult:
        return cls(ok=False, code=code, message=message or code, data=data, retry_after_ms=retry_after_ms)

    @classmethod
    def from_reply(cls, reply: Reply, data: Any = ...) -> ApiResult:
        """Map a bus Reply; ``data`` overrides the decoded ``Reply.data`` when given."""
        return cls(
            ok=reply.ok,
            code=reply.code,
            message=reply.message or ("" if reply.ok else reply.code),
            data=reply_data(reply) if data is ... else data,
            retry_after_ms=reply.retry_after_ms,
            duplicate=reply.duplicate,
        )

    @property
    def status(self) -> int:
        return self.status_override if self.status_override is not None else http_status(self.code)

    def body(self) -> dict[str, Any]:
        out: dict[str, Any] = {"ok": self.ok, "code": self.code, "message": self.message or "", "data": self.data}
        if self.duplicate:
            out["duplicate"] = True
        return out

    def response(self) -> MsgspecJSONResponse:
        headers: dict[str, str] = {RESULT_CODE_HEADER: self.code}
        if self.status in RETRY_AFTER_STATUSES:
            secs = max(1, math.ceil((self.retry_after_ms or 1000) / 1000))
            headers["Retry-After"] = str(secs)
        return MsgspecJSONResponse(self.body(), status_code=self.status, headers=headers)


class ApiError(Exception):
    """Raise from a handler to answer with an error envelope."""

    def __init__(self, code: str, message: str = "", data: Any = None, retry_after_ms: int | None = None,
                 status: int | None = None) -> None:
        super().__init__(message or code)
        self.result = ApiResult.failure(code, message, data, retry_after_ms)
        self.result.status_override = status


def encode_json(obj: Any) -> bytes:
    return _json_encoder.encode(obj)


def decode_json(data: bytes | str) -> Any:
    return msgspec.json.decode(data)


__all__ = [
    "ApiError",
    "ApiResult",
    "MsgspecJSONResponse",
    "decode_json",
    "encode_json",
    "http_status",
    "reply_data",
]
