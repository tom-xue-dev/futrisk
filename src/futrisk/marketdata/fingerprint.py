"""Cross-source snapshot fingerprint (02 §7.1 ``Tick.fingerprint``, 01 §8.13, 03 §5.1).

Algorithm (shared by every market-data adapter, so it lives in one place):

1. build the list ``[trading_day, volume, turnover, open_interest, last, bid1,
   ask1, bid_vol1, ask_vol1]`` from **normalized** values;
2. msgpack-encode it (msgspec; Python floats are always encoded as float64,
   ints with the shortest msgpack int form, ``None`` as nil);
3. take BLAKE2b with an 8-byte digest and read it as an unsigned 64-bit
   **big-endian** integer.

Normalization makes two sources describing the same snapshot agree even when
their float representations differ slightly:

- ``trading_day``: ``YYYYMMDD`` string;
- counts (``volume``, ``open_interest``, ``bid_vol1``, ``ask_vol1``): ``int``;
- prices (``last``, ``bid1``, ``ask1``): ``float`` rounded to 6 decimals;
  ``None`` (no quote) and NaN stay nil;
- ``turnover``: ``float`` in yuan (multiplier applied) rounded to 2 decimals;
- ``-0.0`` is folded into ``0.0``.

Timestamps (``exch_ts_ns``, ``recv_ns``) are deliberately excluded: the same
state seen by two sources, or twice by one polling source, has the same
fingerprint (consecutive identical snapshots are recorded as ``DUPLICATE``).
"""

from __future__ import annotations

import hashlib
import math
from typing import Final

import msgspec

from futrisk.messages.md import Tick

FINGERPRINT_FIELDS: Final[tuple[str, ...]] = (
    "trading_day",
    "volume",
    "turnover",
    "open_interest",
    "last",
    "bid1",
    "ask1",
    "bid_vol1",
    "ask_vol1",
)
PRICE_DECIMALS: Final = 6
MONEY_DECIMALS: Final = 2
DIGEST_SIZE: Final = 8

_encoder = msgspec.msgpack.Encoder()

FingerprintKey = list[str | int | float | None]


def _norm_float(value: float | None, decimals: int) -> float | None:
    if value is None:
        return None
    v = float(value)
    if math.isnan(v):
        return None
    v = round(v, decimals)
    return 0.0 if v == 0 else v


def fingerprint_key(
    *,
    trading_day: str,
    volume: int,
    turnover: float,
    open_interest: int,
    last: float,
    bid1: float | None,
    ask1: float | None,
    bid_vol1: int,
    ask_vol1: int,
) -> FingerprintKey:
    """The normalized value list that is hashed (exposed for tests and debugging)."""
    return [
        str(trading_day),
        int(volume),
        _norm_float(turnover, MONEY_DECIMALS),
        int(open_interest),
        _norm_float(last, PRICE_DECIMALS),
        _norm_float(bid1, PRICE_DECIMALS),
        _norm_float(ask1, PRICE_DECIMALS),
        int(bid_vol1),
        int(ask_vol1),
    ]


def compute_fingerprint(
    *,
    trading_day: str,
    volume: int,
    turnover: float,
    open_interest: int,
    last: float,
    bid1: float | None,
    ask1: float | None,
    bid_vol1: int,
    ask_vol1: int,
) -> int:
    """64-bit unsigned fingerprint of a normalized L1 snapshot (02 §7.1)."""
    key = fingerprint_key(
        trading_day=trading_day,
        volume=volume,
        turnover=turnover,
        open_interest=open_interest,
        last=last,
        bid1=bid1,
        ask1=ask1,
        bid_vol1=bid_vol1,
        ask_vol1=ask_vol1,
    )
    digest = hashlib.blake2b(_encoder.encode(key), digest_size=DIGEST_SIZE).digest()
    return int.from_bytes(digest, "big")


def tick_fingerprint(tick: Tick) -> int:
    """Fingerprint of a :class:`Tick` (its own ``fingerprint`` field is ignored)."""
    return compute_fingerprint(
        trading_day=tick.trading_day,
        volume=tick.volume,
        turnover=tick.turnover,
        open_interest=tick.open_interest,
        last=tick.last,
        bid1=tick.bid1,
        ask1=tick.ask1,
        bid_vol1=tick.bid_vol1,
        ask_vol1=tick.ask_vol1,
    )


def with_fingerprint(tick: Tick) -> Tick:
    """Copy of ``tick`` with ``fingerprint`` filled in."""
    return msgspec.structs.replace(tick, fingerprint=tick_fingerprint(tick))
