"""Sina parser and decoder on captured responses, and the shared fingerprint (02 §9.3, §7.1)."""

from __future__ import annotations

import hashlib
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any

import msgspec
import pytest

from futrisk.common.clock import BJ, bj_to_ns
from futrisk.common.config import FullConfig
from futrisk.messages import codes
from futrisk.messages.enums import Exchange, SourceId
from futrisk.messages.md import (
    FLAG_AUCTION,
    FLAG_COARSE_TS,
    FLAG_LIMIT_DOWN_LOCKED,
    FLAG_LIMIT_UP_LOCKED,
    Tick,
)
from futrisk.services.md_gateway.model import Incoming
from futrisk.services.md_gateway.normalizer import (
    Normalizer,
    derive_limits,
    fast_fingerprint,
    lock_flags,
    tick_fingerprint,
)
from futrisk.services.md_gateway.sessions import SessionIndex
from futrisk.services.md_gateway.sina import (
    SINA_REFERER,
    SinaDecoder,
    SinaFormatError,
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

ALL_CODES = [
    "nf_RB2701", "nf_RB2705", "nf_HC2701", "nf_CU2611", "nf_AU2612", "nf_I2701", "nf_J2701", "nf_M2701",
    "CFF_RE_IF2612", "CFF_RE_IC2612", "CFF_RE_T2612",
]


def bj_ns(*args: int) -> int:
    return bj_to_ns(datetime(*args, tzinfo=BJ))


def set_fields(payload: str, changes: dict[int, str]) -> str:
    fields = payload.split(",")
    for i, v in changes.items():
        fields[i] = v
    return ",".join(fields)


def norm(decoder: SinaDecoder, code: str, payload: str | None, recv_ns: int) -> Incoming:
    return decoder.incoming(code, payload, recv_ns, 0)


# when the fixtures were captured (the 2026-09-25 Mid-Autumn holiday)
RECV_CAPTURE = bj_ns(2026, 9, 25, 10, 46, 56)


class TestDecoding:
    def test_names_decode_with_gb18030(self, sina_bytes: dict[str, bytes]) -> None:
        text = decode_body(sina_bytes["all_configured.txt"])
        assert "螺纹钢2701" in text and "沪深300指数期货2612" in text and "10年期国债期货2612" in text
        with pytest.raises(UnicodeDecodeError):
            sina_bytes["commodity.txt"].decode("utf-8")

    def test_split_all_configured_keeps_request_order(self, sina_bytes: dict[str, bytes]) -> None:
        split = split_response(decode_body(sina_bytes["all_configured.txt"]))
        assert list(split.payloads) == ALL_CODES and split.junk == ()

    def test_unknown_and_delisted_codes_are_empty(self, sina_bytes: dict[str, bytes]) -> None:
        split = split_response(decode_body(sina_bytes["invalid.txt"]))
        for code in ("nf_XX9999", "nf_RB2509", "nf_rb2701", "CFF_RE_if2612"):
            assert split.payloads[code] == ""
        assert split.payloads["CFF_RE_IF2609"].split(",")[36] == "2026-09-18"  # an expired contract keeps its stale snapshot

    def test_split_tolerates_crlf_blank_lines_forbidden_and_empty_bodies(self, sina_bytes: dict[str, bytes]) -> None:
        text = decode_body(sina_bytes["commodity.txt"])
        crlf = "\r\n\r\n" + text.replace("\n", "\r\n\r\n")
        assert split_response(crlf).payloads == split_response(text).payloads
        forbidden = split_response(decode_body(sina_bytes["forbidden_no_referer.txt"]))
        assert forbidden.payloads == {} and forbidden.junk == ("Forbidden",)
        empty = split_response(decode_body(sina_bytes["empty_list.txt"]))
        assert empty.payloads == {} and empty.junk == ()

    def test_request_helpers(self) -> None:
        assert build_url(["nf_RB2701", "CFF_RE_IF2612"]) == "https://hq.sinajs.cn/list=nf_RB2701,CFF_RE_IF2612"
        with pytest.raises(ValueError):
            build_url([])
        with pytest.raises(ValueError):
            build_url(["nf_RB2701&x=1"])
        assert request_headers()["Referer"] == SINA_REFERER == "https://finance.sina.com.cn"
        assert sina_code_for("rb2701", Exchange.SHFE) == "nf_RB2701"
        assert sina_code_for("i2701", Exchange.DCE) == "nf_I2701"
        assert sina_code_for("IF2612", Exchange.CFFEX) == "CFF_RE_IF2612"


class TestLayouts:
    def test_rb2701_fields_match_official_shfe_data(self, payloads: dict[str, str]) -> None:
        # SHFE official 2026-09-24 rb2701: open 3115 high 3120 low 3100 close 3118 settle 3111, pre_settle 3117,
        # volume 570630, open interest 1615888
        q = parse_quote("nf_RB2701", payloads["nf_RB2701"])
        assert q.kind == "commodity" and q.name == "螺纹钢2701"
        assert (q.quote_date, q.quote_time) == (date(2026, 9, 24), time(15, 0, 0))
        assert (q.open, q.high, q.low) == (3115.0, 3120.0, 3100.0)
        assert q.pre_close == 3118.0  # after the close this field holds today's close
        assert (q.bid1, q.ask1, q.last) == (3118.0, 3119.0, 3118.0)
        assert (q.settle_estimate, q.pre_settle) == (3111.0, 3117.0)
        assert (q.bid_vol1, q.ask_vol1) == (1, 857)
        assert (q.open_interest, q.volume) == (1615888, 570630)
        assert (q.exchange_short, q.product_name) == ("沪", "螺纹钢")
        assert q.avg_price == 3111.857 and len(q.fields) == 44
        assert q.turnover_raw is None and q.upper_limit is None and q.pre_open_interest is None

    def test_every_captured_commodity_parses(self, payloads: dict[str, str]) -> None:
        shorts = {}
        for code in ALL_CODES[:8]:
            q = parse_quote(code, payloads[code])
            assert q.kind == "commodity" and q.last > 0 and q.pre_settle > 0 and q.volume > 0
            shorts[code] = q.exchange_short
        assert shorts["nf_CU2611"] == "沪" and shorts["nf_I2701"] == "连" and shorts["nf_M2701"] == "连"
        assert parse_quote("nf_I2701", payloads["nf_I2701"]).quote_time == time(15, 4, 39)  # DCE's late closing snapshot

    def test_parse_time_variants(self) -> None:
        assert parse_time("150000") == time(15, 0, 0) and parse_time("93005") == time(9, 30, 5)
        assert parse_time("003000") == time(0, 30, 0) and parse_time("15:00:00") == time(15, 0, 0)
        assert parse_time("15:00:00.5") == time(15, 0, 0, 500000)
        for bad in ("1500000", "25:00:00", "ab", "15:00", ""):
            with pytest.raises(SinaFormatError):
                parse_time(bad)

    def test_if2612_fields(self, payloads: dict[str, str]) -> None:
        q = parse_quote("CFF_RE_IF2612", payloads["CFF_RE_IF2612"])
        assert q.kind == "cffex" and q.name == "沪深300指数期货2612"
        assert (q.quote_date, q.quote_time) == (date(2026, 9, 24), time(15, 0, 0))
        assert (q.open, q.high, q.low, q.last) == (4439.0, 4443.6, 4367.0, 4375.0)
        assert (q.volume, q.turnover_raw, q.open_interest) == (56735, 249694034.6, 169008)
        assert (q.upper_limit, q.lower_limit) == (4908.4, 4016.0)
        # CFFEX official 2026-09-23: IF2612 close 4461.8, settlement 4462.2, open interest 160771
        assert (q.pre_close, q.pre_settle, q.pre_open_interest) == (4461.8, 4462.2, 160771)
        assert (q.bid1, q.bid_vol1, q.ask1, q.ask_vol1) == (4375.0, 1, 4375.2, 1)
        assert len(q.bids) == len(q.asks) == 5 and q.avg_price == 4401.058 and len(q.fields) == 50

    @pytest.mark.parametrize(
        ("code", "limit_pct", "tick"),
        [("CFF_RE_IF2612", "0.10", "0.2"), ("CFF_RE_IC2612", "0.10", "0.2"), ("CFF_RE_T2612", "0.02", "0.005"),
         ("CFF_RE_TS2612", "0.005", "0.002"), ("CFF_RE_IF2609", "0.20", "0.2")],  # IF2609: last trading day, +-20%
    )
    def test_limit_prices_are_based_on_field_14(self, payloads: dict[str, str], code: str, limit_pct: str, tick: str) -> None:
        q = parse_quote(code, payloads[code])
        assert derive_limits(q.pre_settle, Decimal(limit_pct), Decimal(tick)) == (q.upper_limit, q.lower_limit)

    def test_turnover_times_multiplier_matches_official(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        official = {"CFF_RE_IF2612": 74908210380.0, "CFF_RE_IC2612": 123131412200.0, "CFF_RE_T2612": 76826469650.0}
        for code, turnover in official.items():
            tick = norm(sina_decoder, code, payloads[code], RECV_CAPTURE).tick
            assert tick is not None and tick.turnover == turnover


class TestToTick:
    def test_commodity_closing_snapshot(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        r = norm(sina_decoder, "nf_RB2701", payloads["nf_RB2701"], RECV_CAPTURE)
        assert r.reject_reason is None
        t = r.tick
        assert t is not None
        assert (t.instrument_id, t.exchange, t.source, t.source_seq) == ("rb2701", Exchange.SHFE, SourceId.SINA, None)
        assert (t.trading_day, t.action_day) == ("20260924", "20260924")
        assert t.exch_ts_ns == bj_ns(2026, 9, 24, 15, 0, 0) and t.recv_ns == RECV_CAPTURE
        assert (t.last, t.volume, t.open_interest) == (3118.0, 570630, 1615888)
        assert (t.upper_limit, t.lower_limit) == (3272.0, 2962.0)  # no limits from Sina: 3117 * (1 +- 5%) rounded inward
        assert t.turnover == pytest.approx(3111.857 * 570630 * 10, abs=0.01)  # VWAP * volume * multiplier
        assert abs(t.turnover - 17_757_191_920) / 17_757_191_920 < 1e-6  # official 17,757,191,920
        assert t.pre_close is None  # field 5 is today's close after the close
        assert t.pre_open_interest is None and t.avg_price == 3111.857
        assert t.flags == FLAG_COARSE_TS and t.fingerprint == tick_fingerprint(t)

    def test_cffex_tick(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        t = norm(sina_decoder, "CFF_RE_IF2612", payloads["CFF_RE_IF2612"], RECV_CAPTURE).tick
        assert t is not None
        assert (t.instrument_id, t.exchange, t.trading_day) == ("IF2612", Exchange.CFFEX, "20260924")
        assert (t.upper_limit, t.lower_limit, t.pre_settle, t.pre_close) == (4908.4, 4016.0, 4462.2, 4461.8)
        assert t.pre_open_interest == 160771 and t.turnover == 74908210380.0

    def test_every_configured_symbol_decodes_in_request_order(self, sina_decoder: SinaDecoder, sina_bytes: dict[str, bytes]) -> None:
        results, lag = sina_decoder.decode(sina_bytes["all_configured.txt"], RECV_CAPTURE, 0)
        assert lag is None and [r.tick.instrument_id for r in results if r.tick] == [sina_decoder.instruments[c].instrument_id for c in ALL_CODES]
        assert all(r.reject_reason is None for r in results)
        assert len({r.tick.fingerprint for r in results if r.tick}) == len(ALL_CODES)

    def test_night_quote_belongs_to_next_trading_day(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        payload = set_fields(payloads["nf_RB2701"], {1: "210500", 17: "2026-09-28"})
        t = norm(sina_decoder, "nf_RB2701", payload, bj_ns(2026, 9, 28, 21, 5, 2)).tick
        assert t is not None
        assert (t.trading_day, t.action_day) == ("20260929", "20260928") and t.exch_ts_ns == bj_ns(2026, 9, 28, 21, 5, 0)
        assert t.pre_close == 3118.0  # in session: field 5 is the previous close

    def test_saturday_early_morning_belongs_to_monday(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        payload = set_fields(payloads["nf_CU2611"], {1: "003000", 17: "2026-09-19"})
        t = norm(sina_decoder, "nf_CU2611", payload, bj_ns(2026, 9, 19, 0, 30, 1)).tick
        assert t is not None and (t.trading_day, t.action_day) == ("20260921", "20260919")

    def test_trading_day_in_date_field_is_reanchored(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        # a DCE night quote carrying the trading day (09-29) instead of the natural date (09-28)
        payload = set_fields(payloads["nf_I2701"], {1: "213000", 17: "2026-09-29"})
        r = norm(sina_decoder, "nf_I2701", payload, bj_ns(2026, 9, 28, 21, 30, 1))
        assert r.reject_reason is None and r.tick is not None and r.tick.exch_ts_ns == bj_ns(2026, 9, 28, 21, 30, 0)
        assert (r.tick.trading_day, r.tick.action_day) == ("20260929", "20260928")
        friday = set_fields(payloads["nf_I2701"], {1: "213000", 17: "2026-09-21"})
        t = norm(sina_decoder, "nf_I2701", friday, bj_ns(2026, 9, 18, 21, 30, 2)).tick
        assert t is not None and t.exch_ts_ns == bj_ns(2026, 9, 18, 21, 30, 0) and t.trading_day == "20260921"

    def test_reanchoring_rules(self) -> None:
        assert resolve_exch_datetime(date(2026, 9, 24), time(15, 0), bj_ns(2026, 9, 28, 8, 50, 0)) == datetime(2026, 9, 24, 15, 0, tzinfo=BJ)  # stale snapshots keep their date
        assert resolve_exch_datetime(date(2026, 9, 28), time(10, 30), bj_ns(2026, 9, 28, 10, 20, 0)) == datetime(2026, 9, 28, 10, 30, tzinfo=BJ)  # a slow local clock: no shift
        assert resolve_exch_datetime(date(2026, 9, 28), time(21, 30), bj_ns(2026, 9, 28, 21, 20, 0)) == datetime(2026, 9, 28, 21, 30, tzinfo=BJ)
        assert resolve_exch_datetime(date(2026, 9, 29), time(23, 59, 59), bj_ns(2026, 9, 29, 0, 0, 2)) == datetime(2026, 9, 28, 23, 59, 59, tzinfo=BJ)  # just after midnight
        assert resolve_exch_datetime(None, time(10, 0), bj_ns(2026, 9, 28, 10, 0, 3)) == datetime(2026, 9, 28, 10, 0, tzinfo=BJ)  # missing date

    def test_auction_flags(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        # 2026-09-28 follows a holiday: no night session, commodity auction at 08:55-09:00
        rb = set_fields(payloads["nf_RB2701"], {1: "085900", 17: "2026-09-28"})
        t = norm(sina_decoder, "nf_RB2701", rb, bj_ns(2026, 9, 28, 8, 59, 1)).tick
        assert t is not None and t.flags == FLAG_COARSE_TS | FLAG_AUCTION
        fields = payloads["CFF_RE_IF2612"].split(",")
        fields[36], fields[37] = "2026-09-28", "09:29:00"
        t = norm(sina_decoder, "CFF_RE_IF2612", ",".join(fields), bj_ns(2026, 9, 28, 9, 29, 1)).tick
        assert t is not None and t.flags & FLAG_AUCTION
        rb = set_fields(payloads["nf_RB2701"], {1: "103000", 17: "2026-09-28"})
        t = norm(sina_decoder, "nf_RB2701", rb, bj_ns(2026, 9, 28, 10, 30, 1)).tick
        assert t is not None and t.flags == FLAG_COARSE_TS  # continuous trading: no auction flag

    def test_zero_prices_before_the_open(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        zeros = {2: "0.000", 3: "0.000", 4: "0.000", 5: "0.000", 6: "0.000", 7: "0.000", 8: "0.000"}
        payload = set_fields(payloads["nf_RB2701"], {**zeros, 1: "084500", 17: "2026-09-28", 11: "0", 12: "0", 14: "0", 27: "0.000"})
        t = norm(sina_decoder, "nf_RB2701", payload, bj_ns(2026, 9, 28, 8, 45, 1)).tick
        assert t is not None
        assert (t.open, t.high, t.low, t.bid1, t.ask1, t.avg_price, t.pre_close) == (None,) * 7
        assert (t.last, t.volume, t.turnover) == (0.0, 0, 0.0) and t.pre_settle == 3117.0 and t.upper_limit == 3272.0

    def test_limit_lock_flags(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        up = set_fields(payloads["nf_RB2701"], {1: "103000", 17: "2026-09-28", 6: "3272.000", 7: "0.000", 8: "3272.000", 12: "0"})
        t = norm(sina_decoder, "nf_RB2701", up, bj_ns(2026, 9, 28, 10, 30, 1)).tick
        assert t is not None and t.flags & FLAG_LIMIT_UP_LOCKED and not t.flags & FLAG_LIMIT_DOWN_LOCKED
        assert lock_flags(None, 2962.0, 3272.0, 2962.0) == FLAG_LIMIT_DOWN_LOCKED
        assert lock_flags(3118.0, 3119.0, 3272.0, 2962.0) == 0 and lock_flags(None, None, 3272.0, 2962.0) == 0

    @pytest.mark.parametrize(
        ("hhmmss", "recv", "pre_close"),
        [("101500", (2026, 9, 28, 10, 15, 1), 3118.0),  # end of the first morning window
         ("113000", (2026, 9, 28, 11, 30, 1), 3118.0),  # lunch break starts
         ("230000", (2026, 9, 28, 23, 0, 1), 3118.0),  # end of the night session
         ("102000", (2026, 9, 28, 10, 20, 1), None),  # inside the morning break
         ("150000", (2026, 9, 28, 15, 0, 1), None)],  # the day's final close: field 5 is today's
    )
    def test_pre_close_at_window_ends(self, sina_decoder: SinaDecoder, payloads: dict[str, str], hhmmss: str, recv: tuple[int, ...], pre_close: float | None) -> None:
        payload = set_fields(payloads["nf_RB2701"], {1: hhmmss, 17: "2026-09-28"})
        t = norm(sina_decoder, "nf_RB2701", payload, bj_ns(*recv)).tick
        assert t is not None and t.pre_close == pre_close

    def test_limits_follow_the_date_effective_schedule(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        # products.yaml widens i to 8% from trading day 20260930 (DCE holiday notice); 6% before
        before = set_fields(payloads["nf_I2701"], {1: "103000", 17: "2026-09-29"})
        t = norm(sina_decoder, "nf_I2701", before, bj_ns(2026, 9, 29, 10, 30, 1)).tick
        assert t is not None and (t.upper_limit, t.lower_limit) == (756.0, 671.0)  # 713.5 * (1 +- 6%)
        after = set_fields(payloads["nf_I2701"], {1: "103000", 17: "2026-09-30"})
        t = norm(sina_decoder, "nf_I2701", after, bj_ns(2026, 9, 30, 10, 30, 1)).tick
        assert t is not None and (t.upper_limit, t.lower_limit) == (770.5, 656.5)  # 713.5 * (1 +- 8%)
        night = set_fields(payloads["nf_I2701"], {1: "213000", 17: "2026-09-29"})  # already trading day 09-30
        t = norm(sina_decoder, "nf_I2701", night, bj_ns(2026, 9, 29, 21, 30, 1)).tick
        assert t is not None and t.trading_day == "20260930" and t.upper_limit == 770.5


class TestFutureTimestamps:
    def test_quote_stamped_ahead_of_the_receive_time_is_rejected_but_kept(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        payload = set_fields(payloads["nf_RB2701"], {1: "104000", 17: "2026-09-28"})
        r = norm(sina_decoder, "nf_RB2701", payload, bj_ns(2026, 9, 28, 10, 30, 0))  # 10 min ahead
        assert r.reject_reason == codes.MD_REJECT_FUTURE_TS and r.detail.startswith("future_ts") and "600s" in r.detail
        assert r.tick is not None and r.tick.last == 3118.0  # kept so the recorder stores every field
        within = set_fields(payloads["nf_RB2701"], {1: "103100", 17: "2026-09-28"})
        assert norm(sina_decoder, "nf_RB2701", within, bj_ns(2026, 9, 28, 10, 30, 0)).reject_reason is None  # 60 s ahead

    def test_tolerance_is_configurable(self, default_config: FullConfig, sessions: SessionIndex, payloads: dict[str, str]) -> None:
        metas = resolve_sina_instruments(default_config.products, default_config.feeds.sina.symbols, ["rb2701"])
        strict = SinaDecoder(Normalizer(default_config.products, sessions, future_tolerance_s=30), metas)
        payload = set_fields(payloads["nf_RB2701"], {1: "103100", 17: "2026-09-28"})
        r = norm(strict, "nf_RB2701", payload, bj_ns(2026, 9, 28, 10, 30, 0))
        assert r.reject_reason == codes.MD_REJECT_FUTURE_TS and r.detail.startswith("future_ts")

    def test_future_timestamp_outside_trading_days(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        payload = set_fields(payloads["nf_RB2701"], {1: "100000", 17: "2026-09-26"})  # Saturday, a day ahead
        r = norm(sina_decoder, "nf_RB2701", payload, bj_ns(2026, 9, 25, 10, 0, 0))
        assert r.reject_reason == codes.MD_REJECT_FUTURE_TS and r.detail.startswith("future_ts") and r.tick is not None


class TestRejects:
    def test_empty_payload_means_unknown_code(self, default_config: FullConfig, normalizer: Normalizer, sina_bytes: dict[str, bytes]) -> None:
        metas = resolve_sina_instruments(default_config.products, {"rb2509": "nf_RB2509"}, ["rb2509"])
        [r], _ = SinaDecoder(normalizer, metas).decode(sina_bytes["invalid.txt"], RECV_CAPTURE, 0)
        assert (r.instrument_id, r.reject_reason, r.tick) == ("rb2509", codes.MD_REJECT_MALFORMED, None) and "empty payload" in r.detail

    def test_code_missing_from_response(self, sina_decoder: SinaDecoder, sina_bytes: dict[str, bytes]) -> None:
        results, _ = sina_decoder.decode(sina_bytes["commodity.txt"], RECV_CAPTURE, 0)
        missing = [r for r in results if r.reject_reason]
        assert [r.instrument_id for r in missing] == ["IF2612", "IC2612", "T2612"]
        assert all(r.reject_reason == codes.MD_REJECT_MALFORMED and "missing" in r.detail for r in missing)

    @pytest.mark.parametrize(
        ("code", "change"),
        [("nf_RB2701", None), ("nf_RB2701", {8: "abc"}), ("nf_RB2701", {1: "256000"}), ("nf_RB2701", {14: "-5"}), ("nf_RB2701", {8: "nan"}),
         ("nf_RB2701", {17: "2026-13-01"}), ("nf_RB2701", {10: "0.000"}), ("nf_RB2701", {15: "连"}), ("CFF_RE_IF2612", {37: "25:00:00"}),
         ("CFF_RE_IF2612", None)],  # None = truncated; {10: 0} no pre-settlement; {15: 连} DCE short name on an SHFE contract
    )
    def test_malformed_payloads(self, sina_decoder: SinaDecoder, payloads: dict[str, str], code: str, change: dict[int, str] | None) -> None:
        payload = payloads[code]
        payload = ",".join(payload.split(",")[:10]) if change is None else set_fields(payload, change)
        r = norm(sina_decoder, code, payload, RECV_CAPTURE)
        assert r.tick is None and r.reject_reason == codes.MD_REJECT_MALFORMED and r.detail

    def test_layout_mismatch_unconfigured_and_unsupported_codes(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        r = norm(sina_decoder, "nf_RB2701", payloads["CFF_RE_IF2612"], RECV_CAPTURE)
        assert r.tick is None and r.reject_reason == codes.MD_REJECT_MALFORMED
        r = norm(sina_decoder, "nf_RB0", payloads["nf_RB0"], RECV_CAPTURE)
        assert r.instrument_id == "" and r.reject_reason == codes.MD_REJECT_MALFORMED and "not configured" in r.detail
        r = norm(sina_decoder, "sh600000", "x,y", RECV_CAPTURE)
        assert r.reject_reason == codes.MD_REJECT_MALFORMED and "prefix" in r.detail

    def test_timestamp_outside_any_trading_day_keeps_the_tick(self, sina_decoder: SinaDecoder, payloads: dict[str, str]) -> None:
        payload = set_fields(payloads["nf_RB2701"], {1: "100000", 17: "2026-09-26"})  # Saturday daytime
        r = norm(sina_decoder, "nf_RB2701", payload, bj_ns(2026, 9, 26, 10, 0, 5))
        assert r.reject_reason == codes.MD_REJECT_TRADING_DAY and r.tick is not None and r.tick.last == 3118.0


class TestResolve:
    def test_defaults_follow_feeds_yaml(self, default_config: FullConfig) -> None:
        metas = resolve_sina_instruments(default_config.products, default_config.feeds.sina.symbols)
        assert list(metas) == ALL_CODES
        assert metas["nf_I2701"].exchange is Exchange.DCE and metas["CFF_RE_T2612"].multiplier == 10000

    def test_symbols_in_any_case_and_derived_codes(self, default_config: FullConfig) -> None:
        metas = resolve_sina_instruments(default_config.products, default_config.feeds.sina.symbols, ["RB2701", "if2612", "rb2610"])
        assert {code: m.instrument_id for code, m in metas.items()} == {"nf_RB2701": "rb2701", "CFF_RE_IF2612": "IF2612", "nf_RB2610": "rb2610"}

    def test_unknown_product_raises(self, default_config: FullConfig) -> None:
        for bad in ("zz2701", "2701"):
            with pytest.raises(ValueError):
                resolve_sina_instruments(default_config.products, default_config.feeds.sina.symbols, [bad])


# ---------------------------------------------------------------- the shared fingerprint (02 §7.1)
RB: dict[str, Any] = {
    "trading_day": "20260924", "volume": 570630, "turnover": 17757189599.1, "open_interest": 1615888, "last": 3118.0, "bid1": 3118.0,
    "ask1": 3119.0, "bid_vol1": 1, "ask_vol1": 857,
}
RB_FINGERPRINT = 0x585390AFDFC9BCFE  # golden value: changing it breaks cross-source dedup of recorded files
FIELDS = ("trading_day", "volume", "turnover", "open_interest", "last", "bid1", "ask1", "bid_vol1", "ask_vol1")


def fp(**changes: Any) -> int:
    v = {**RB, **changes}
    return fast_fingerprint(*(v[f] for f in FIELDS))


def make_tick(**overrides: Any) -> Tick:
    base: dict[str, Any] = {
        **RB, "instrument_id": "rb2701", "exchange": Exchange.SHFE, "action_day": "20260924", "exch_ts_ns": 1_790_233_200_000_000_000,
        "upper_limit": 3272.0, "lower_limit": 2962.0, "pre_settle": 3117.0, "source": SourceId.FEED_A, "recv_ns": 1_790_233_200_500_000_000,
    }
    return Tick(**{**base, **overrides})


class TestFingerprint:
    def test_algorithm_is_msgpack_blake2b_8_bytes_big_endian(self) -> None:
        raw = msgspec.msgpack.encode([RB[f] for f in FIELDS])
        expected = int.from_bytes(hashlib.blake2b(raw, digest_size=8).digest(), "big")
        assert fp() == expected == RB_FINGERPRINT and 0 <= expected < 2**64

    def test_normalization_makes_equivalent_values_agree(self) -> None:
        assert fp(last=3118, bid1=3118, ask1=3119) == RB_FINGERPRINT
        assert fp(last=3118.0000000001) == fp(turnover=17757189599.1000001) == fp(volume=570630.0) == RB_FINGERPRINT
        assert fp(bid1=-0.0) == fp(bid1=0.0)

    @pytest.mark.parametrize(
        ("field", "value"),
        [("trading_day", "20260925"), ("volume", 570631), ("turnover", 17757189600.1), ("open_interest", 1615887), ("last", 3119.0),
         ("bid1", None), ("ask1", 3120.0), ("bid_vol1", 2), ("ask_vol1", 856)],
    )
    def test_every_field_matters(self, field: str, value: object) -> None:
        assert fp(**{field: value}) != RB_FINGERPRINT

    def test_timestamps_and_other_fields_do_not_matter(self) -> None:
        a = make_tick()
        b = make_tick(exch_ts_ns=a.exch_ts_ns + 10**9, recv_ns=a.recv_ns + 7, high=3200.0, source=SourceId.SINA, flags=1)
        assert tick_fingerprint(a) == tick_fingerprint(b) == RB_FINGERPRINT

    def test_sina_ticks_carry_the_shared_fingerprint(self, sina_decoder: SinaDecoder, sina_bytes: dict[str, bytes]) -> None:
        body = sina_bytes["all_configured.txt"]
        first, _ = sina_decoder.decode(body, 1_790_300_000_000_000_000, 0)
        again, _ = sina_decoder.decode(body, 1_790_300_001_500_000_000, 0)  # same snapshot, polled again
        for r1, r2 in zip(first, again, strict=True):
            assert r1.tick is not None and r2.tick is not None
            assert r1.tick.fingerprint == r2.tick.fingerprint == tick_fingerprint(r1.tick)
        assert first[0].tick is not None and first[0].tick.fingerprint == RB_FINGERPRINT
