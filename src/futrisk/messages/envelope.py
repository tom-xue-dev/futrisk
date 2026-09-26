"""Bus envelope and Reply (02 §4.2, §4.7).

PUB frames: ``[topic, envelope]``; DEALER sends ``[envelope]``; ROUTER receives
and replies with ``[identity, envelope]``. Requests use the command name as
``topic`` and ``seq=0``; replies use ``"<command>.reply"``.
"""

from __future__ import annotations

from typing import Any

import msgspec

from futrisk.messages import codes
from futrisk.messages.base import Msg, decode, decode_named, encode, payload, type_name, type_version

PROTO = 1


class Envelope(msgspec.Struct, kw_only=True, omit_defaults=True):
    proto: int = PROTO
    topic: str
    type: str
    v: int = 1
    src: str
    epoch: str
    stream: str | None = None
    seq: int = 0
    ts_ns: int
    td: str | None = None
    corr_id: str | None = None
    payload: bytes = b""


_env_encoder = msgspec.msgpack.Encoder()
_env_decoder = msgspec.msgpack.Decoder(Envelope)


def encode_envelope(env: Envelope) -> bytes:
    return _env_encoder.encode(env)


def decode_envelope(data: bytes) -> Envelope:
    return _env_decoder.decode(data)


def make_envelope(
    topic: str,
    body: Msg,
    *,
    src: str,
    epoch: str,
    ts_ns: int,
    seq: int = 0,
    stream: str | None = None,
    td: str | None = None,
    corr_id: str | None = None,
) -> Envelope:
    name = type_name(body)
    return Envelope(
        topic=topic,
        type=name,
        v=type_version(name),
        src=src,
        epoch=epoch,
        stream=stream,
        seq=seq,
        ts_ns=ts_ns,
        td=td,
        corr_id=corr_id,
        payload=encode(body),
    )


def open_payload(env: Envelope) -> Msg:
    """Decode the envelope payload with the registered type for ``env.type``."""
    return decode_named(env.payload, env.type)


@payload(v=1)
class Reply(Msg, kw_only=True):
    ok: bool
    code: str = codes.OK
    message: str | None = None
    data_type: str | None = None
    data: bytes | None = None
    duplicate: bool = False
    retry_after_ms: int | None = None

    def open(self) -> Msg | None:
        """Decode ``data`` with its registered type (None when empty)."""
        if self.data is None or self.data_type is None:
            return None
        return decode_named(self.data, self.data_type)

    def open_as(self, cls: type[Any]) -> Any:
        if self.data is None:
            return None
        return decode(self.data, cls)


def ok_reply(data: Msg | None = None, *, message: str | None = None, duplicate: bool = False) -> Reply:
    if data is None:
        return Reply(ok=True, code=codes.OK, message=message, duplicate=duplicate)
    return Reply(
        ok=True,
        code=codes.OK,
        message=message,
        data_type=type_name(data),
        data=encode(data),
        duplicate=duplicate,
    )


def err_reply(code: str, message: str | None = None, data: Msg | None = None, retry_after_ms: int | None = None) -> Reply:
    if data is None:
        return Reply(ok=False, code=code, message=message, retry_after_ms=retry_after_ms)
    return Reply(
        ok=False,
        code=code,
        message=message,
        data_type=type_name(data),
        data=encode(data),
        retry_after_ms=retry_after_ms,
    )
