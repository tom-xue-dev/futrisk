"""Cross-source fingerprint tests (02 §7.1 ``Tick.fingerprint``)."""

from __future__ import annotations

import hashlib
from typing import Any

import msgspec
import pytest

from futrisk.marketdata.fingerprint import (
    FINGERPRINT_FIELDS,
    compute_fingerprint,
    fingerprint_key,
    tick_fingerprint,
    with_fingerprint,
)
from futrisk.marketdata.sina import SinaNormalizer, decode_body
from futrisk.messages.enums import Exchange, SourceId
from futrisk.messages.md import Tick

# rb2701 closing snapshot of 2026-09-24 as normalized from the captured Sina response
RB: dict[str, Any] = {
    "trading_day": "20260924",
    "volume": 570630,
    "turnover": 17757189599.1,
    "open_interest": 1615888,
    "last": 3118.0,
    "bid1": 3118.0,
    "ask1": 3119.0,
    "bid_vol1": 1,
    "ask_vol1": 857,
}
RB_FINGERPRINT = 0x585390AFDFC9BCFE  # golden value: changing it breaks cross-source dedup of recorded files


def fp(**changes: Any) -> int:
    """Fingerprint of the rb2701 snapshot with some fields changed."""
    v = {**RB, **changes}
    return compute_fingerprint(
        trading_day=v["trading_day"],
        volume=v["volume"],
        turnover=v["turnover"],
        open_interest=v["open_interest"],
        last=v["last"],
        bid1=v["bid1"],
        ask1=v["ask1"],
        bid_vol1=v["bid_vol1"],
        ask_vol1=v["ask_vol1"],
    )


def make_tick(**overrides: Any) -> Tick:
    base: dict[str, Any] = {
        **RB,
        "instrument_id": "rb2701",
        "exchange": Exchange.SHFE,
        "action_day": "20260924",
        "exch_ts_ns": 1_790_233_200_000_000_000,
        "upper_limit": 3272.0,
        "lower_limit": 2962.0,
        "pre_settle": 3117.0,
        "source": SourceId.FEED_A,
        "recv_ns": 1_790_233_200_500_000_000,
    }
    base.update(overrides)
    return Tick(**base)


class TestFingerprint:
    def test_field_list_follows_the_spec(self) -> None:
        assert FINGERPRINT_FIELDS == (
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

    def test_algorithm_is_msgpack_blake2b_8_bytes_big_endian(self) -> None:
        raw = msgspec.msgpack.encode([RB[f] for f in FINGERPRINT_FIELDS])
        expected = int.from_bytes(hashlib.blake2b(raw, digest_size=8).digest(), "big")
        assert fp() == expected == RB_FINGERPRINT
        assert 0 <= expected < 2**64

    def test_normalization_makes_equivalent_values_agree(self) -> None:
        assert fp(last=3118, bid1=3118, ask1=3119) == RB_FINGERPRINT
        assert fp(last=3118.0000000001) == RB_FINGERPRINT
        assert fp(turnover=17757189599.1000001) == RB_FINGERPRINT
        assert fp(volume=570630.0) == RB_FINGERPRINT
        key = fingerprint_key(**{**RB, "bid1": -0.0, "ask1": float("nan")})
        assert key[5] == 0.0 and str(key[5]) == "0.0" and key[6] is None

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("trading_day", "20260925"),
            ("volume", 570631),
            ("turnover", 17757189600.1),
            ("open_interest", 1615887),
            ("last", 3119.0),
            ("bid1", None),
            ("ask1", 3120.0),
            ("bid_vol1", 2),
            ("ask_vol1", 856),
        ],
    )
    def test_every_field_matters(self, field: str, value: object) -> None:
        assert fp(**{field: value}) != RB_FINGERPRINT

    def test_timestamps_and_other_fields_do_not_matter(self) -> None:
        a = make_tick()
        b = make_tick(
            exch_ts_ns=a.exch_ts_ns + 10**9, recv_ns=a.recv_ns + 7, high=3200.0, source=SourceId.SINA, flags=1
        )
        assert tick_fingerprint(a) == tick_fingerprint(b) == RB_FINGERPRINT

    def test_with_fingerprint_fills_the_field(self) -> None:
        t = with_fingerprint(make_tick(fingerprint=0))
        assert t.fingerprint == RB_FINGERPRINT

    def test_sina_ticks_carry_the_shared_fingerprint(
        self, normalizer: SinaNormalizer, sina_bytes: dict[str, bytes]
    ) -> None:
        text = decode_body(sina_bytes["all_configured.txt"])
        first = normalizer.normalize_text(text, 1_790_300_000_000_000_000)
        again = normalizer.normalize_text(text, 1_790_300_001_500_000_000)  # same snapshot, polled again
        for r1, r2 in zip(first, again, strict=True):
            assert r1.tick is not None and r2.tick is not None
            assert r1.tick.fingerprint == r2.tick.fingerprint == tick_fingerprint(r1.tick)
        rb = first[0].tick
        assert rb is not None and rb.fingerprint == RB_FINGERPRINT
