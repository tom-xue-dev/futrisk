"""Unit tests for the shared foundation: calendar, decimals, clocks, messages, config."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import msgspec
import pytest

from futrisk.common.calendar import TradingCalendar, parse_templates, weekday_calendar
from futrisk.common.clock import BJ, MarketClock, SimClock, bj_to_ns, ns_to_bj, parse_td, td_str
from futrisk.common.config import compute_config_hash, load_config_dir
from futrisk.common.decimals import (
    D,
    dec_str,
    dec_str_strip,
    floor_to_tick,
    limit_prices,
    on_tick,
    q_money,
    round_to_tick,
)
from futrisk.common.ids import SeqIdGen, uuid7
from futrisk.messages import codes, decode_envelope, encode_envelope, make_envelope, ok_reply, open_payload
from futrisk.messages.enums import Direction, Offset, OrderStatus
from futrisk.messages.orders import OrderRequest, Trade, compute_account_digest
from futrisk.messages.topics import FACT, LV, matches, reliability

# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------

TEMPLATES = parse_templates(
    {
        "night_2100_0100": [["20:55", "21:00", "auction"], ["21:00", "01:00", "continuous"]],
        "day_commodity": [
            ["08:55", "09:00", "auction_no_night"],
            ["09:00", "10:15", "continuous"],
            ["10:30", "11:30", "continuous"],
            ["13:30", "15:00", "continuous"],
        ],
    }
)


@pytest.fixture
def cal() -> TradingCalendar:
    days = weekday_calendar(date(2026, 9, 1), date(2026, 10, 31), holidays=[date(2026, 9, 25), *[date(2026, 10, d) for d in range(1, 8)]])
    return TradingCalendar(days, TEMPLATES)


class TestCalendar:
    def test_night_timestamps_belong_to_next_trading_day(self, cal: TradingCalendar) -> None:
        assert cal.trading_day_for(datetime(2026, 9, 18, 22, 0, tzinfo=BJ)) == date(2026, 9, 21)
        # Saturday 00:30 belongs to Monday (Friday night session continued)
        assert cal.trading_day_for(datetime(2026, 9, 19, 0, 30, tzinfo=BJ)) == date(2026, 9, 21)
        assert cal.trading_day_for(datetime(2026, 9, 21, 10, 0, tzinfo=BJ)) == date(2026, 9, 21)

    def test_no_night_session_after_holiday(self, cal: TradingCalendar) -> None:
        assert cal.has_night_session(date(2026, 9, 22)) is True
        assert cal.has_night_session(date(2026, 9, 28)) is False  # 09-25 holiday in between
        assert cal.has_night_session(date(2026, 10, 8)) is False
        assert cal.has_night_session(date(2026, 9, 21)) is True  # weekend only

    def test_auction_moves_to_morning_without_night(self, cal: TradingCalendar) -> None:
        kinds = [(w.start.time().isoformat(), w.kind) for w in cal.windows(["night_2100_0100", "day_commodity"], date(2026, 9, 28))]
        assert ("08:55:00", "auction") in kinds
        kinds_normal = [w.start for w in cal.windows(["night_2100_0100", "day_commodity"], date(2026, 9, 22))]
        assert kinds_normal[0] == datetime(2026, 9, 21, 20, 55, tzinfo=BJ)

    def test_night_window_crosses_midnight(self, cal: TradingCalendar) -> None:
        ws = cal.windows(["night_2100_0100"], date(2026, 9, 22))
        cont = [w for w in ws if w.kind == "continuous"][0]
        assert cont.start == datetime(2026, 9, 21, 21, 0, tzinfo=BJ)
        assert cont.end == datetime(2026, 9, 22, 1, 0, tzinfo=BJ)

    def test_phase(self, cal: TradingCalendar) -> None:
        tpl = ["night_2100_0100", "day_commodity"]
        assert cal.phase_at(tpl, bj_to_ns(datetime(2026, 9, 22, 10, 20, tzinfo=BJ))).state == "CLOSED"
        assert cal.phase_at(tpl, bj_to_ns(datetime(2026, 9, 22, 10, 40, tzinfo=BJ))).is_continuous
        assert cal.phase_at(tpl, bj_to_ns(datetime(2026, 9, 21, 20, 57, tzinfo=BJ))).state == "AUCTION"

    def test_next_window_start_skips_holiday(self, cal: TradingCalendar) -> None:
        w = cal.next_window_start(["night_2100_0100", "day_commodity"], bj_to_ns(datetime(2026, 9, 30, 15, 30, tzinfo=BJ)))
        assert w is not None
        assert w.start == datetime(2026, 10, 8, 8, 55, tzinfo=BJ)

    def test_prev_next(self, cal: TradingCalendar) -> None:
        assert cal.next_trading_day(date(2026, 9, 24)) == date(2026, 9, 28)
        assert cal.prev_trading_day(date(2026, 10, 8)) == date(2026, 9, 30)
        assert cal.nth_trading_day_before(date(2026, 10, 8), 5) == date(2026, 9, 23)


# ---------------------------------------------------------------------------
# Decimals
# ---------------------------------------------------------------------------


class TestDecimals:
    def test_float_conversion_uses_str(self) -> None:
        assert D(0.1) == Decimal("0.1")
        assert D(3118.0) == Decimal("3118.0")

    def test_dec_str_is_plain(self) -> None:
        assert dec_str(Decimal("1E+3")) == "1000"
        assert dec_str(Decimal("-0.00")) == "0.00"
        assert dec_str_strip(Decimal("3118.0")) == "3118"
        assert dec_str_strip(Decimal("3100")) == "3100"
        assert dec_str_strip(Decimal("0.000023")) == "0.000023"

    def test_money_half_up(self) -> None:
        assert q_money(Decimal("307.045")) == Decimal("307.05")
        assert q_money(Decimal("-2.345")) == Decimal("-2.35")

    def test_tick_helpers(self) -> None:
        assert floor_to_tick(Decimal("4414.833"), Decimal("0.2")) == Decimal("4414.8")
        assert round_to_tick(Decimal("4414.9"), Decimal("0.2")) == Decimal("4415.0")
        assert on_tick(3100.0, Decimal("1"))
        assert not on_tick(3100.5, Decimal("1"))
        assert on_tick(850.02, Decimal("0.02"))

    def test_limit_prices_round_inward(self) -> None:
        up, dn = limit_prices(Decimal("4462.2"), Decimal("0.10"), Decimal("0.2"))
        assert up == Decimal("4908.4") and dn == Decimal("4016.0")


# ---------------------------------------------------------------------------
# Clocks and ids
# ---------------------------------------------------------------------------


class TestClocks:
    def test_market_clock_offset(self) -> None:
        wall = SimClock(1_000)
        mc = MarketClock(wall, mode="DEMO", offset_ns=500, version=1)
        assert mc.now_ns() == 1_500
        assert mc.apply("DEMO", 900, 2) is True
        assert mc.now_ns() == 1_900
        assert mc.apply("DEMO", 100, 1) is False  # stale version ignored
        assert mc.offset_ns == 900

    def test_real_mode_rejects_offset(self) -> None:
        with pytest.raises(ValueError):
            MarketClock(SimClock(0), mode="REAL", offset_ns=5)

    def test_bj_roundtrip(self) -> None:
        dt = datetime(2026, 9, 28, 9, 0, 0, tzinfo=BJ)
        assert ns_to_bj(bj_to_ns(dt)) == dt
        assert td_str(parse_td("20260928")) == "20260928"

    def test_uuid7_monotonic(self) -> None:
        ids = [uuid7() for _ in range(200)]
        assert ids == sorted(ids)
        assert len(set(ids)) == 200
        assert SeqIdGen("x").next_id() == "x-00000001"


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


class TestMessages:
    def test_envelope_roundtrip(self) -> None:
        req = OrderRequest(
            client_order_id=uuid7(), account_id="ACC3", instrument_id="rb2701", direction=Direction.BUY,
            offset=Offset.OPEN, price=3100.0, qty=2,
        )
        env = make_envelope("order.submit", req, src="strategy-sim", epoch="e1", ts_ns=5)
        back = decode_envelope(encode_envelope(env))
        assert back.type == "OrderRequest" and back.v == 1 and back.seq == 0
        assert open_payload(back) == req

    def test_unknown_fields_ignored(self) -> None:
        raw = msgspec.msgpack.encode({"ok": True, "code": "OK", "future_field": 1})
        from futrisk.messages import decode
        from futrisk.messages.envelope import Reply

        assert decode(raw, Reply).ok is True

    def test_reply_data(self) -> None:
        r = ok_reply(Trade(
            trading_day="20260928", exchange="SHFE", trade_id="T1", direction="BUY", account_id="ACC1",
            instrument_id="rb2701", broker_order_ref="1:1:1", offset="OPEN", price=3100.0, qty=1, trade_ts_ns=1,
        ))
        assert r.code == codes.OK
        assert r.open().trade_key == "20260928|SHFE|T1|BUY"  # type: ignore[union-attr]

    def test_terminal_status(self) -> None:
        assert OrderStatus.FILLED.is_terminal and not OrderStatus.ROUTED.is_terminal

    def test_trade_digest_is_order_independent(self) -> None:
        def t(i: str, px: float) -> Trade:
            return Trade(trading_day="20260928", exchange="SHFE", trade_id=i, direction="BUY", account_id="ACC1",
                         instrument_id="rb2701", broker_order_ref="x", offset="OPEN", price=px, qty=1, trade_ts_ns=1)
        a = compute_account_digest("ACC1", [t("1", 3118.0), t("2", 3119.0)])
        b = compute_account_digest("ACC1", [t("2", 3119.0), t("1", 3118.0)])
        assert a.sha256 == b.sha256 and a.count == 2

    def test_topics(self) -> None:
        assert reliability("td.exec.ACC1") == FACT
        assert reliability("md.tick.rb2701") == LV
        assert reliability("risk.event") == FACT
        assert reliability("risk.firm") == LV
        assert matches("td.exec.ACC1", "td.exec.ACC1") and not matches("td.exec.ACC10", "td.exec.ACC1")
        assert matches("risk.snapshot.ACC1", "risk.")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestConfig:
    def test_default_config_loads(self, default_config) -> None:  # type: ignore[no-untyped-def]
        cfg = default_config
        assert cfg.accounts.shard_of("ACC3") == 1
        assert cfg.products.product_of("IF2612").multiplier == 300
        assert cfg.services.coordinator() == "risk-engine-0"
        assert cfg.calendar.is_trading_day(date(2026, 9, 28))
        assert not cfg.calendar.is_trading_day(date(2026, 9, 25))
        assert len(cfg.config_hash) == 64

    def test_hash_is_order_independent(self) -> None:
        assert compute_config_hash({"a": "1", "b": "2"}) == compute_config_hash({"b": "2", "a": "1"})

    def test_bad_config_rejected(self, tmp_path, default_config) -> None:  # type: ignore[no-untyped-def]
        import shutil

        shutil.copytree(default_config.root, tmp_path / "cfg")
        p = tmp_path / "cfg" / "accounts.yaml"
        p.write_text("accounts:\n  ACC1: {name: x, shard: 0, initial_capital: '1', bogus: 1}\n", encoding="utf-8")
        from futrisk.common.config import ConfigError

        with pytest.raises(ConfigError):
            load_config_dir(tmp_path / "cfg")
