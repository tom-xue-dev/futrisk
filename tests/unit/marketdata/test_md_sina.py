"""Sina parser and normalizer tests on captured responses (02 §9.3, §7.1)."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal

import pytest

from futrisk.common.clock import BJ, bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.marketdata.fingerprint import tick_fingerprint
from futrisk.marketdata.normalize import derive_limits, limit_lock_flags
from futrisk.marketdata.sina import (
    SINA_REFERER,
    SinaFormatError,
    SinaNormalizer,
    build_url,
    decode_body,
    parse_quote,
    parse_time,
    request_headers,
    resolve_exch_datetime,
    resolve_sina_instruments,
    sina_code_for,
    split_response,
)
from futrisk.messages import codes
from futrisk.messages.enums import Exchange, SourceId
from futrisk.messages.md import FLAG_AUCTION, FLAG_COARSE_TS, FLAG_LIMIT_DOWN_LOCKED, FLAG_LIMIT_UP_LOCKED

ALL_CODES = [
    "nf_RB2701",
    "nf_RB2705",
    "nf_HC2701",
    "nf_CU2611",
    "nf_AU2612",
    "nf_I2701",
    "nf_J2701",
    "nf_M2701",
    "CFF_RE_IF2612",
    "CFF_RE_IC2612",
    "CFF_RE_T2612",
]


def bj_ns(*args: int) -> int:
    return bj_to_ns(datetime(*args, tzinfo=BJ))


def set_fields(payload: str, changes: dict[int, str]) -> str:
    fields = payload.split(",")
    for i, v in changes.items():
        fields[i] = v
    return ",".join(fields)


# when the fixtures were captured (the 2026-09-25 Mid-Autumn holiday)
RECV_CAPTURE = bj_ns(2026, 9, 25, 10, 46, 56)


# ---------------------------------------------------------------------------
# Decoding and response splitting
# ---------------------------------------------------------------------------


class TestDecoding:
    def test_names_decode_with_gb18030(self, sina_bytes: dict[str, bytes]) -> None:
        text = decode_body(sina_bytes["all_configured.txt"])
        assert "螺纹钢2701" in text
        assert "沪深300指数期货2612" in text
        assert "10年期国债期货2612" in text

    def test_body_is_not_utf8(self, sina_bytes: dict[str, bytes]) -> None:
        with pytest.raises(UnicodeDecodeError):
            sina_bytes["commodity.txt"].decode("utf-8")

    def test_split_all_configured_keeps_request_order(self, sina_bytes: dict[str, bytes]) -> None:
        split = split_response(decode_body(sina_bytes["all_configured.txt"]))
        assert list(split.payloads) == ALL_CODES
        assert split.junk == ()

    def test_unknown_and_delisted_codes_are_empty(self, sina_bytes: dict[str, bytes]) -> None:
        split = split_response(decode_body(sina_bytes["invalid.txt"]))
        for code in ("nf_XX9999", "nf_RB2509", "nf_rb2701", "CFF_RE_if2612"):
            assert split.payloads[code] == ""
        # an expired contract still returns its last (stale) snapshot
        assert split.payloads["CFF_RE_IF2609"].split(",")[36] == "2026-09-18"

    def test_split_tolerates_crlf_and_blank_lines(self, sina_bytes: dict[str, bytes]) -> None:
        text = decode_body(sina_bytes["commodity.txt"])
        crlf = "\r\n\r\n" + text.replace("\n", "\r\n\r\n")
        assert split_response(crlf).payloads == split_response(text).payloads

    def test_forbidden_and_empty_bodies(self, sina_bytes: dict[str, bytes]) -> None:
        forbidden = split_response(decode_body(sina_bytes["forbidden_no_referer.txt"]))
        assert forbidden.payloads == {} and forbidden.junk == ("Forbidden",)
        empty = split_response(decode_body(sina_bytes["empty_list.txt"]))
        assert empty.payloads == {} and empty.junk == ()

    def test_request_helpers(self) -> None:
        assert (
            build_url(["nf_RB2701", "CFF_RE_IF2612"]) == "https://hq.sinajs.cn/list=nf_RB2701,CFF_RE_IF2612"
        )
        with pytest.raises(ValueError):
            build_url([])
        with pytest.raises(ValueError):
            build_url(["nf_RB2701&x=1"])
        assert request_headers()["Referer"] == SINA_REFERER == "https://finance.sina.com.cn"
        assert sina_code_for("rb2701", Exchange.SHFE) == "nf_RB2701"
        assert sina_code_for("i2701", Exchange.DCE) == "nf_I2701"
        assert sina_code_for("IF2612", Exchange.CFFEX) == "CFF_RE_IF2612"


# ---------------------------------------------------------------------------
# Commodity layout (nf_)
# ---------------------------------------------------------------------------


class TestCommodityLayout:
    def test_rb2701_fields_match_official_shfe_data(self, payloads: dict[str, str]) -> None:
        # SHFE official 2026-09-24 rb2701: open 3115 high 3120 low 3100 close 3118 settle 3111
        # pre_settle 3117 volume 570630 open interest 1615888
        q = parse_quote("nf_RB2701", payloads["nf_RB2701"])
        assert q.kind == "commodity"
        assert q.name == "螺纹钢2701"
        assert (q.quote_date, q.quote_time) == (date(2026, 9, 24), time(15, 0, 0))
        assert (q.open, q.high, q.low) == (3115.0, 3120.0, 3100.0)
        assert q.pre_close == 3118.0  # after the close this field holds today's close
        assert (q.bid1, q.ask1, q.last) == (3118.0, 3119.0, 3118.0)
        assert q.settle_estimate == 3111.0
        assert q.pre_settle == 3117.0
        assert (q.bid_vol1, q.ask_vol1) == (1, 857)
        assert (q.open_interest, q.volume) == (1615888, 570630)
        assert (q.exchange_short, q.product_name) == ("沪", "螺纹钢")
        assert q.avg_price == 3111.857
        assert q.turnover_raw is None and q.upper_limit is None and q.pre_open_interest is None
        assert len(q.fields) == 44

    def test_every_captured_commodity_parses(self, payloads: dict[str, str]) -> None:
        shorts = {}
        for code in ALL_CODES[:8]:
            q = parse_quote(code, payloads[code])
            assert q.kind == "commodity" and q.last > 0 and q.pre_settle > 0 and q.volume > 0
            shorts[code] = q.exchange_short
        assert shorts["nf_CU2611"] == "沪" and shorts["nf_I2701"] == "连" and shorts["nf_M2701"] == "连"

    def test_dce_snapshot_carries_late_timestamp(self, payloads: dict[str, str]) -> None:
        assert parse_quote("nf_I2701", payloads["nf_I2701"]).quote_time == time(15, 4, 39)

    def test_parse_time_variants(self) -> None:
        assert parse_time("150000") == time(15, 0, 0)
        assert parse_time("93005") == time(9, 30, 5)
        assert parse_time("003000") == time(0, 30, 0)
        assert parse_time("15:00:00") == time(15, 0, 0)
        assert parse_time("15:00:00.5") == time(15, 0, 0, 500000)
        for bad in ("1500000", "25:00:00", "ab", "15:00", ""):
            with pytest.raises(SinaFormatError):
                parse_time(bad)


# ---------------------------------------------------------------------------
# CFFEX layout (CFF_RE_)
# ---------------------------------------------------------------------------


class TestCffexLayout:
    def test_if2612_fields(self, payloads: dict[str, str]) -> None:
        q = parse_quote("CFF_RE_IF2612", payloads["CFF_RE_IF2612"])
        assert q.kind == "cffex"
        assert q.name == "沪深300指数期货2612"
        assert (q.quote_date, q.quote_time) == (date(2026, 9, 24), time(15, 0, 0))
        assert (q.open, q.high, q.low, q.last) == (4439.0, 4443.6, 4367.0, 4375.0)
        assert (q.volume, q.turnover_raw, q.open_interest) == (56735, 249694034.6, 169008)
        assert (q.upper_limit, q.lower_limit) == (4908.4, 4016.0)
        # CFFEX official 2026-09-23: IF2612 close 4461.8, settlement 4462.2, open interest 160771
        assert q.pre_close == 4461.8  # field 13
        assert q.pre_settle == 4462.2  # field 14
        assert q.pre_open_interest == 160771
        assert (q.bid1, q.bid_vol1, q.ask1, q.ask_vol1) == (4375.0, 1, 4375.2, 1)
        assert len(q.bids) == len(q.asks) == 5
        assert q.avg_price == 4401.058
        assert len(q.fields) == 50

    @pytest.mark.parametrize(
        ("code", "limit_pct", "tick"),
        [
            ("CFF_RE_IF2612", "0.10", "0.2"),
            ("CFF_RE_IC2612", "0.10", "0.2"),
            ("CFF_RE_T2612", "0.02", "0.005"),
            ("CFF_RE_TS2612", "0.005", "0.002"),
            ("CFF_RE_IF2609", "0.20", "0.2"),  # last trading day: +-20%
        ],
    )
    def test_limit_prices_are_based_on_field_14(
        self, payloads: dict[str, str], code: str, limit_pct: str, tick: str
    ) -> None:
        q = parse_quote(code, payloads[code])
        assert derive_limits(q.pre_settle, Decimal(limit_pct), Decimal(tick)) == (
            q.upper_limit,
            q.lower_limit,
        )

    def test_turnover_times_multiplier_matches_official(
        self, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        official = {
            "CFF_RE_IF2612": 74908210380.0,
            "CFF_RE_IC2612": 123131412200.0,
            "CFF_RE_T2612": 76826469650.0,
        }
        for code, turnover in official.items():
            tick = normalizer.normalize(code, payloads[code], RECV_CAPTURE).tick
            assert tick is not None and tick.turnover == turnover


# ---------------------------------------------------------------------------
# Tick normalization
# ---------------------------------------------------------------------------


class TestToTick:
    def test_commodity_closing_snapshot(self, normalizer: SinaNormalizer, payloads: dict[str, str]) -> None:
        r = normalizer.normalize("nf_RB2701", payloads["nf_RB2701"], RECV_CAPTURE)
        assert r.ok and r.reject_reason is None
        t = r.tick
        assert t is not None
        assert (t.instrument_id, t.exchange, t.source, t.source_seq) == (
            "rb2701",
            Exchange.SHFE,
            SourceId.SINA,
            None,
        )
        assert (t.trading_day, t.action_day) == ("20260924", "20260924")
        assert t.exch_ts_ns == bj_ns(2026, 9, 24, 15, 0, 0)
        assert t.recv_ns == RECV_CAPTURE
        assert (t.last, t.volume, t.open_interest) == (3118.0, 570630, 1615888)
        # no limits from Sina: 3117 * (1 +- 5%) rounded inward -> 3272 / 2962
        assert (t.upper_limit, t.lower_limit) == (3272.0, 2962.0)
        # no turnover from Sina: VWAP * volume * multiplier (official 17,757,191,920)
        assert t.turnover == pytest.approx(3111.857 * 570630 * 10, abs=0.01)
        assert abs(t.turnover - 17_757_191_920) / 17_757_191_920 < 1e-6
        assert t.pre_close is None  # field 5 is today's close after the close
        assert t.pre_open_interest is None
        assert t.avg_price == 3111.857
        assert t.flags == FLAG_COARSE_TS
        assert t.fingerprint == tick_fingerprint(t)

    def test_cffex_tick(self, normalizer: SinaNormalizer, payloads: dict[str, str]) -> None:
        t = normalizer.normalize("CFF_RE_IF2612", payloads["CFF_RE_IF2612"], RECV_CAPTURE).tick
        assert t is not None
        assert (t.instrument_id, t.exchange, t.trading_day) == ("IF2612", Exchange.CFFEX, "20260924")
        assert (t.upper_limit, t.lower_limit, t.pre_settle, t.pre_close) == (4908.4, 4016.0, 4462.2, 4461.8)
        assert t.pre_open_interest == 160771
        assert t.turnover == 74908210380.0

    def test_every_configured_symbol_normalizes(
        self, normalizer: SinaNormalizer, sina_bytes: dict[str, bytes]
    ) -> None:
        results = normalizer.normalize_text(decode_body(sina_bytes["all_configured.txt"]), RECV_CAPTURE)
        assert [r.code for r in results] == ALL_CODES
        assert all(r.ok for r in results)
        assert len({r.tick.fingerprint for r in results if r.tick}) == len(ALL_CODES)

    def test_night_quote_belongs_to_next_trading_day(
        self, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        payload = set_fields(payloads["nf_RB2701"], {1: "210500", 17: "2026-09-28"})
        t = normalizer.normalize("nf_RB2701", payload, bj_ns(2026, 9, 28, 21, 5, 2)).tick
        assert t is not None
        assert (t.trading_day, t.action_day) == ("20260929", "20260928")
        assert t.exch_ts_ns == bj_ns(2026, 9, 28, 21, 5, 0)
        assert t.pre_close == 3118.0  # in session: field 5 is the previous close

    def test_saturday_early_morning_belongs_to_monday(
        self, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        payload = set_fields(payloads["nf_CU2611"], {1: "003000", 17: "2026-09-19"})
        t = normalizer.normalize("nf_CU2611", payload, bj_ns(2026, 9, 19, 0, 30, 1)).tick
        assert t is not None
        assert (t.trading_day, t.action_day) == ("20260921", "20260919")

    def test_trading_day_in_date_field_is_reanchored(
        self, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        # a DCE night quote carrying the trading day (09-29) instead of the natural date (09-28)
        payload = set_fields(payloads["nf_I2701"], {1: "213000", 17: "2026-09-29"})
        t = normalizer.normalize("nf_I2701", payload, bj_ns(2026, 9, 28, 21, 30, 1)).tick
        assert t is not None
        assert t.exch_ts_ns == bj_ns(2026, 9, 28, 21, 30, 0)
        assert (t.trading_day, t.action_day) == ("20260929", "20260928")

    def test_friday_night_with_monday_date(
        self, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        payload = set_fields(payloads["nf_I2701"], {1: "213000", 17: "2026-09-21"})
        t = normalizer.normalize("nf_I2701", payload, bj_ns(2026, 9, 18, 21, 30, 2)).tick
        assert t is not None
        assert t.exch_ts_ns == bj_ns(2026, 9, 18, 21, 30, 0)
        assert t.trading_day == "20260921"

    def test_reanchoring_rules(self) -> None:
        # stale snapshots keep their date
        assert resolve_exch_datetime(
            date(2026, 9, 24), time(15, 0), bj_ns(2026, 9, 28, 8, 50, 0)
        ) == datetime(2026, 9, 24, 15, 0, tzinfo=BJ)
        # quotes ahead of a slow local clock are not shifted to the previous day, day or night
        assert resolve_exch_datetime(
            date(2026, 9, 28), time(10, 30), bj_ns(2026, 9, 28, 10, 20, 0)
        ) == datetime(2026, 9, 28, 10, 30, tzinfo=BJ)
        assert resolve_exch_datetime(
            date(2026, 9, 28), time(21, 30), bj_ns(2026, 9, 28, 21, 20, 0)
        ) == datetime(2026, 9, 28, 21, 30, tzinfo=BJ)
        # just after midnight: yesterday's 23:59:59 stamped with the trading day
        assert resolve_exch_datetime(
            date(2026, 9, 29), time(23, 59, 59), bj_ns(2026, 9, 29, 0, 0, 2)
        ) == datetime(2026, 9, 28, 23, 59, 59, tzinfo=BJ)
        # missing date: anchored to the receive date
        assert resolve_exch_datetime(None, time(10, 0), bj_ns(2026, 9, 28, 10, 0, 3)) == datetime(
            2026, 9, 28, 10, 0, tzinfo=BJ
        )

    def test_auction_flags(self, normalizer: SinaNormalizer, payloads: dict[str, str]) -> None:
        # 2026-09-28 follows a holiday: no night session, commodity auction at 08:55-09:00
        rb = set_fields(payloads["nf_RB2701"], {1: "085900", 17: "2026-09-28"})
        t = normalizer.normalize("nf_RB2701", rb, bj_ns(2026, 9, 28, 8, 59, 1)).tick
        assert t is not None and t.flags == FLAG_COARSE_TS | FLAG_AUCTION
        fields = payloads["CFF_RE_IF2612"].split(",")
        fields[36], fields[37] = "2026-09-28", "09:29:00"
        t = normalizer.normalize("CFF_RE_IF2612", ",".join(fields), bj_ns(2026, 9, 28, 9, 29, 1)).tick
        assert t is not None and t.flags & FLAG_AUCTION
        # continuous trading: no auction flag
        rb = set_fields(payloads["nf_RB2701"], {1: "103000", 17: "2026-09-28"})
        t = normalizer.normalize("nf_RB2701", rb, bj_ns(2026, 9, 28, 10, 30, 1)).tick
        assert t is not None and t.flags == FLAG_COARSE_TS

    def test_zero_prices_before_the_open(self, normalizer: SinaNormalizer, payloads: dict[str, str]) -> None:
        zeros = {2: "0.000", 3: "0.000", 4: "0.000", 5: "0.000", 6: "0.000", 7: "0.000", 8: "0.000"}
        payload = set_fields(
            payloads["nf_RB2701"],
            {**zeros, 1: "084500", 17: "2026-09-28", 11: "0", 12: "0", 14: "0", 27: "0.000"},
        )
        t = normalizer.normalize("nf_RB2701", payload, bj_ns(2026, 9, 28, 8, 45, 1)).tick
        assert t is not None
        assert (t.open, t.high, t.low, t.bid1, t.ask1, t.avg_price, t.pre_close) == (None,) * 7
        assert (t.last, t.volume, t.turnover) == (0.0, 0, 0.0)
        assert t.pre_settle == 3117.0 and t.upper_limit == 3272.0

    def test_limit_lock_flags(self, normalizer: SinaNormalizer, payloads: dict[str, str]) -> None:
        # bids at the upper limit, no offers: limit-up lock
        up = set_fields(
            payloads["nf_RB2701"],
            {1: "103000", 17: "2026-09-28", 6: "3272.000", 7: "0.000", 8: "3272.000", 12: "0"},
        )
        t = normalizer.normalize("nf_RB2701", up, bj_ns(2026, 9, 28, 10, 30, 1)).tick
        assert t is not None and t.flags & FLAG_LIMIT_UP_LOCKED and not t.flags & FLAG_LIMIT_DOWN_LOCKED
        assert limit_lock_flags(None, 2962.0, 3272.0, 2962.0) == FLAG_LIMIT_DOWN_LOCKED
        assert limit_lock_flags(3118.0, 3119.0, 3272.0, 2962.0) == 0
        assert limit_lock_flags(None, None, 3272.0, 2962.0) == 0

    @pytest.mark.parametrize(
        ("hhmmss", "date_", "recv", "pre_close"),
        [
            ("101500", "2026-09-28", (2026, 9, 28, 10, 15, 1), 3118.0),  # end of the first morning window
            ("113000", "2026-09-28", (2026, 9, 28, 11, 30, 1), 3118.0),  # lunch break starts
            ("230000", "2026-09-28", (2026, 9, 28, 23, 0, 1), 3118.0),  # end of the night session
            ("102000", "2026-09-28", (2026, 9, 28, 10, 20, 1), None),  # inside the morning break
            (
                "150000",
                "2026-09-28",
                (2026, 9, 28, 15, 0, 1),
                None,
            ),  # the day's final close: field 5 is today's
        ],
    )
    def test_pre_close_at_window_ends(
        self,
        normalizer: SinaNormalizer,
        payloads: dict[str, str],
        hhmmss: str,
        date_: str,
        recv: tuple[int, ...],
        pre_close: float | None,
    ) -> None:
        payload = set_fields(payloads["nf_RB2701"], {1: hhmmss, 17: date_})
        t = normalizer.normalize("nf_RB2701", payload, bj_ns(*recv)).tick
        assert t is not None and t.pre_close == pre_close

    def test_limits_follow_the_date_effective_schedule(
        self, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        # products.yaml widens i to 8% from trading day 20260930 (DCE holiday notice); 6% before
        before = set_fields(payloads["nf_I2701"], {1: "103000", 17: "2026-09-29"})
        t = normalizer.normalize("nf_I2701", before, bj_ns(2026, 9, 29, 10, 30, 1)).tick
        assert t is not None and (t.upper_limit, t.lower_limit) == (756.0, 671.0)  # 713.5 * (1 +- 6%)
        after = set_fields(payloads["nf_I2701"], {1: "103000", 17: "2026-09-30"})
        t = normalizer.normalize("nf_I2701", after, bj_ns(2026, 9, 30, 10, 30, 1)).tick
        assert t is not None and (t.upper_limit, t.lower_limit) == (770.5, 656.5)  # 713.5 * (1 +- 8%)
        # the night session of 09-29 already belongs to trading day 09-30
        night = set_fields(payloads["nf_I2701"], {1: "213000", 17: "2026-09-29"})
        t = normalizer.normalize("nf_I2701", night, bj_ns(2026, 9, 29, 21, 30, 1)).tick
        assert t is not None and t.trading_day == "20260930" and t.upper_limit == 770.5


# ---------------------------------------------------------------------------
# Future timestamps
# ---------------------------------------------------------------------------


class TestFutureTimestamps:
    def test_quote_stamped_ahead_of_the_receive_time_is_rejected(
        self, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        payload = set_fields(payloads["nf_RB2701"], {1: "104000", 17: "2026-09-28"})
        r = normalizer.normalize("nf_RB2701", payload, bj_ns(2026, 9, 28, 10, 30, 0))  # 10 min ahead
        assert not r.ok and r.reject_reason == codes.MD_REJECT_FUTURE_TS
        assert r.detail.startswith("future_ts") and "600s" in r.detail
        assert r.tick is not None and r.tick.last == 3118.0  # kept so the recorder stores every field

    def test_within_tolerance_is_accepted(self, normalizer: SinaNormalizer, payloads: dict[str, str]) -> None:
        payload = set_fields(payloads["nf_RB2701"], {1: "103100", 17: "2026-09-28"})
        assert normalizer.normalize("nf_RB2701", payload, bj_ns(2026, 9, 28, 10, 30, 0)).ok  # 60 s ahead

    def test_tolerance_is_configurable(self, default_config: FullConfig, payloads: dict[str, str]) -> None:
        metas = resolve_sina_instruments(
            default_config.products, default_config.feeds.sina.symbols, ["rb2701"]
        )
        strict = SinaNormalizer(metas, default_config.calendar, future_tolerance_s=30)
        payload = set_fields(payloads["nf_RB2701"], {1: "103100", 17: "2026-09-28"})
        r = strict.normalize("nf_RB2701", payload, bj_ns(2026, 9, 28, 10, 30, 0))
        assert r.reject_reason == codes.MD_REJECT_FUTURE_TS and r.detail.startswith("future_ts")

    def test_dce_night_date_quirk_is_reanchored_not_rejected(
        self, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        payload = set_fields(payloads["nf_I2701"], {1: "213000", 17: "2026-09-29"})  # trading day as date
        r = normalizer.normalize("nf_I2701", payload, bj_ns(2026, 9, 28, 21, 30, 1))
        assert r.ok and r.tick is not None and r.tick.exch_ts_ns == bj_ns(2026, 9, 28, 21, 30, 0)

    def test_future_timestamp_outside_trading_days(
        self, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        payload = set_fields(payloads["nf_RB2701"], {1: "100000", 17: "2026-09-26"})  # Saturday, a day ahead
        r = normalizer.normalize("nf_RB2701", payload, bj_ns(2026, 9, 25, 10, 0, 0))
        assert r.tick is None and r.reject_reason == codes.MD_REJECT_FUTURE_TS
        assert r.detail.startswith("future_ts")


# ---------------------------------------------------------------------------
# Rejects
# ---------------------------------------------------------------------------


class TestRejects:
    def test_empty_payload_means_unknown_code(
        self, default_config: FullConfig, sina_bytes: dict[str, bytes]
    ) -> None:
        metas = resolve_sina_instruments(default_config.products, {"rb2509": "nf_RB2509"}, ["rb2509"])
        norm = SinaNormalizer(metas, default_config.calendar)
        [r] = norm.normalize_text(decode_body(sina_bytes["invalid.txt"]), RECV_CAPTURE)
        assert (r.code, r.instrument_id, r.empty) == ("nf_RB2509", "rb2509", True)
        assert r.reject_reason == codes.MD_REJECT_MALFORMED and r.quote is None and r.tick is None

    def test_code_missing_from_response(
        self, normalizer: SinaNormalizer, sina_bytes: dict[str, bytes]
    ) -> None:
        results = normalizer.normalize_text(decode_body(sina_bytes["commodity.txt"]), RECV_CAPTURE)
        missing = [r for r in results if not r.ok]
        assert [r.code for r in missing] == ALL_CODES[8:]
        assert all(r.reject_reason == codes.MD_REJECT_MALFORMED and "missing" in r.detail for r in missing)

    @pytest.mark.parametrize(
        ("code", "change"),
        [
            ("nf_RB2701", None),  # truncated
            ("nf_RB2701", {8: "abc"}),
            ("nf_RB2701", {1: "256000"}),
            ("nf_RB2701", {14: "-5"}),
            ("nf_RB2701", {8: "nan"}),
            ("nf_RB2701", {17: "2026-13-01"}),
            ("nf_RB2701", {10: "0.000"}),  # no pre-settlement price
            ("nf_RB2701", {15: "连"}),  # DCE short name on an SHFE contract
            ("CFF_RE_IF2612", {37: "25:00:00"}),
            ("CFF_RE_IF2612", None),  # truncated
        ],
    )
    def test_malformed_payloads(
        self, normalizer: SinaNormalizer, payloads: dict[str, str], code: str, change: dict[int, str] | None
    ) -> None:
        payload = payloads[code]
        payload = ",".join(payload.split(",")[:10]) if change is None else set_fields(payload, change)
        r = normalizer.normalize(code, payload, RECV_CAPTURE)
        assert r.tick is None
        assert r.reject_reason == codes.MD_REJECT_MALFORMED
        assert r.detail

    def test_layout_mismatch_is_malformed(self, normalizer: SinaNormalizer, payloads: dict[str, str]) -> None:
        r = normalizer.normalize("nf_RB2701", payloads["CFF_RE_IF2612"], RECV_CAPTURE)
        assert r.tick is None and r.reject_reason == codes.MD_REJECT_MALFORMED

    def test_unconfigured_or_unsupported_codes(
        self, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        r = normalizer.normalize("nf_RB0", payloads["nf_RB0"], RECV_CAPTURE)
        assert r.instrument_id == "" and r.quote is not None and r.reject_reason == codes.MD_REJECT_MALFORMED
        r = normalizer.normalize("sh600000", "x,y", RECV_CAPTURE)
        assert r.reject_reason == codes.MD_REJECT_MALFORMED and "prefix" in r.detail

    def test_timestamp_outside_any_trading_day(
        self, normalizer: SinaNormalizer, payloads: dict[str, str]
    ) -> None:
        payload = set_fields(payloads["nf_RB2701"], {1: "100000", 17: "2026-09-26"})  # Saturday daytime
        r = normalizer.normalize("nf_RB2701", payload, bj_ns(2026, 9, 26, 10, 0, 5))
        assert r.tick is None and r.quote is not None
        assert r.reject_reason == codes.MD_REJECT_TRADING_DAY


# ---------------------------------------------------------------------------
# Instrument resolution
# ---------------------------------------------------------------------------


class TestResolve:
    def test_defaults_follow_feeds_yaml(self, default_config: FullConfig) -> None:
        metas = resolve_sina_instruments(default_config.products, default_config.feeds.sina.symbols)
        assert list(metas) == ALL_CODES
        assert metas["nf_I2701"].exchange is Exchange.DCE
        assert metas["CFF_RE_T2612"].multiplier == 10000

    def test_symbols_in_any_case_and_derived_codes(self, default_config: FullConfig) -> None:
        metas = resolve_sina_instruments(
            default_config.products, default_config.feeds.sina.symbols, ["RB2701", "if2612", "rb2610"]
        )
        assert {code: m.instrument_id for code, m in metas.items()} == {
            "nf_RB2701": "rb2701",
            "CFF_RE_IF2612": "IF2612",
            "nf_RB2610": "rb2610",  # not in feeds.yaml: default code derived from the exchange
        }

    def test_unknown_product_raises(self, default_config: FullConfig) -> None:
        with pytest.raises(ValueError):
            resolve_sina_instruments(default_config.products, default_config.feeds.sina.symbols, ["zz2701"])
        with pytest.raises(ValueError):
            resolve_sina_instruments(default_config.products, default_config.feeds.sina.symbols, ["2701"])
