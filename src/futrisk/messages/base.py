"""Payload base class, type registry and msgpack codec (02 §4.2-§4.3).

Every payload is a ``msgspec.Struct`` encoded as a **map** (field names on the
wire). Unknown fields are ignored on decode; missing optional fields take their
defaults. Payload types register themselves with a wire name (PascalCase) and a
schema version ``v`` so that envelopes can be decoded generically.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any, TypeVar

import msgspec

T = TypeVar("T", bound="Msg")


class Msg(msgspec.Struct, kw_only=True, omit_defaults=True):
    """Base class of all bus payloads."""


_REGISTRY: dict[str, type[Msg]] = {}
_VERSIONS: dict[str, int] = {}

_encoder = msgspec.msgpack.Encoder()
_decoders: dict[type[Any], msgspec.msgpack.Decoder[Any]] = {}


def register(cls: type[T], v: int = 1) -> type[T]:
    """Register a payload class under its class name."""
    name = cls.__name__
    existing = _REGISTRY.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(f"payload type {name} registered twice")
    _REGISTRY[name] = cls
    _VERSIONS[name] = v
    return cls


def payload(v: int = 1):  # type: ignore[no-untyped-def]
    """Class decorator: ``@payload(v=1)``."""

    def deco(cls: type[T]) -> type[T]:
        return register(cls, v)

    return deco


def type_name(obj: Msg | type[Msg]) -> str:
    cls = obj if isinstance(obj, type) else type(obj)
    return cls.__name__


def type_version(name: str) -> int:
    return _VERSIONS.get(name, 1)


def lookup(name: str) -> type[Msg]:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise KeyError(f"unknown payload type {name!r}") from None


def registered_types() -> dict[str, type[Msg]]:
    return dict(_REGISTRY)


def encode(obj: Any) -> bytes:
    """msgpack-encode a payload (or any msgspec-encodable object)."""
    return _encoder.encode(obj)


def _decoder_for(cls: type[Any]) -> msgspec.msgpack.Decoder[Any]:
    dec = _decoders.get(cls)
    if dec is None:
        dec = msgspec.msgpack.Decoder(cls)
        _decoders[cls] = dec
    return dec


def decode(data: bytes, cls: type[T]) -> T:
    """Decode bytes into a known payload class."""
    return _decoder_for(cls).decode(data)  # type: ignore[no-any-return]


def decode_named(data: bytes, name: str) -> Msg:
    """Decode bytes using the registered class for wire name ``name``."""
    return _decoder_for(lookup(name)).decode(data)  # type: ignore[no-any-return]


def to_builtins(obj: Any) -> Any:
    """Convert a payload to plain Python builtins (dicts/lists/str) for JSON APIs."""
    return msgspec.to_builtins(obj)


def convert(obj: Any, cls: type[T]) -> T:
    """Build a payload from builtins (e.g. a JSON request body)."""
    return msgspec.convert(obj, cls)


# ------------------------------------------------------------------ idempotency hash (03 §4.0 request_hash)
TIME_FIELDS: frozenset[str] = frozenset({"sent_ns", "requested_ns", "ts_ns", "received_ns"})
_KEEP_BYTES = (bytes, bytearray, memoryview)


def _canonical(obj: Any) -> Any:
    """JSON-ready form: string keys, lists for tuples, bytes as hex (every level)."""
    if isinstance(obj, dict):
        return {str(k): _canonical(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [_canonical(v) for v in obj]
    if isinstance(obj, bytes | bytearray | memoryview):
        return bytes(obj).hex()
    return obj


def request_hash(payload: object, *, ignore: frozenset[str] = TIME_FIELDS) -> str:
    """SHA-256 of the business content of a request payload - the one algorithm of the system (03 §4.0).

    ``payload`` may be a msgspec Struct, a mapping or msgpack bytes. Keys in
    ``ignore`` (the request's own time fields ``sent_ns``, ``requested_ns``,
    ``ts_ns``, ``received_ns``) are dropped from the **top level only**: a retry
    with a fresh ``sent_ns`` is still a duplicate, while nested data keeps its
    times (a ``td.import_trades`` whose trades differ only in ``ts_ns`` is a
    CONFLICT, not a replay). Fields equal to their default are omitted by the
    codec, so "explicit default" and "missing" hash the same; bytes are hashed as
    hex; canonical JSON with sorted keys.
    """
    if isinstance(payload, bytes | bytearray):
        data: Any = msgspec.msgpack.decode(bytes(payload))
    elif isinstance(payload, msgspec.Struct):
        data = msgspec.to_builtins(payload, builtin_types=_KEEP_BYTES)
    elif isinstance(payload, Mapping):
        data = msgspec.to_builtins(dict(payload), builtin_types=_KEEP_BYTES)
    elif payload is None:
        data = None
    else:
        data = msgspec.to_builtins(payload, builtin_types=_KEEP_BYTES)
    data = _canonical(data)
    if isinstance(data, dict):
        data = {k: v for k, v in data.items() if k not in ignore}
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
