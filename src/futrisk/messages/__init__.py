"""Versioned bus message contracts (02 §4-§7).

Importing this package registers every payload type so that envelopes can be
decoded generically by type name.
"""

from futrisk.messages import codes, common, eod, md, orders, risk, system, topics  # noqa: F401
from futrisk.messages.base import Msg, decode, decode_named, encode, lookup, registered_types, to_builtins
from futrisk.messages.envelope import (
    PROTO,
    Envelope,
    Reply,
    decode_envelope,
    encode_envelope,
    err_reply,
    make_envelope,
    ok_reply,
    open_payload,
)

__all__ = [
    "PROTO",
    "Envelope",
    "Msg",
    "Reply",
    "codes",
    "decode",
    "decode_envelope",
    "decode_named",
    "encode",
    "encode_envelope",
    "err_reply",
    "lookup",
    "make_envelope",
    "ok_reply",
    "open_payload",
    "registered_types",
    "to_builtins",
]
