"""Unit tests for futrisk.core.margin: gross, big side by amount, exclusions, frozen margin (01 §8.15)."""

from __future__ import annotations

from decimal import Decimal

import pytest
from tests.golden.builders import inst, lot_fee, make_registry, margin, product, rate_fee

from futrisk.core.instruments import InstrumentRegistry, MarginMethod
from futrisk.core.margin import MarginEntry, frozen_margin, intraday_margin, margin_exact, settlement_margin
from futrisk.messages.enums import PosDirection

D = Decimal
L, SH = PosDirection.LONG, PosDirection.SHORT


@pytest.fixture(scope="module")
def reg() -> InstrumentRegistry:
    def cffex(mult: int, tick: str, group: str, physical: bool, rate: str) -> dict[str, object]:
        return product("CFFEX", multiplier=mult, tick=tick, fees=[rate_fee("0.000023")], margins=[margin(rate)],
                       big_side_group=group, physical_delivery=physical)

    return make_registry(
        {
            "rb": product("SHFE", multiplier=10, tick="1", fees=[rate_fee("0.0001")],
                          margins=[margin("0.07"), margin("0.10", contracts=["rb2610"])]),
            "hc": product("SHFE", multiplier=10, tick="1", fees=[rate_fee("0.0001")], margins=[margin("0.07")]),
            "m": product("DCE", multiplier=10, tick="1", fees=[lot_fee("1.5")], margins=[margin("0.07", broker_add="0.01")]),
            "IF": cffex(300, "0.2", "CFFEX:INDEX", False, "0.12"),
            "IH": cffex(300, "0.2", "CFFEX:INDEX", False, "0.12"),
            "T": cffex(10000, "0.005", "CFFEX:TREASURY", True, "0.02"),
            "TF": cffex(10000, "0.005", "CFFEX:TREASURY", True, "0.012"),
        },
        [
            inst("rb2610", "rb", "20261015", "202610"),
            inst("rb2701", "rb", "20270115", "202701"),
            inst("hc2701", "hc", "20270115", "202701"),
            inst("m2701", "m", "20270115", "202701"),
            inst("IF2612", "IF", "20261218", "202612"),
            inst("IH2612", "IH", "20261218", "202612"),
            inst("T2612", "T", "20261211", "202612"),
            inst("TF2612", "TF", "20261211", "202612"),
        ],
    )


class TestSettlementMargin:
    def test_big_side_by_amount_not_lots(self, reg: InstrumentRegistry) -> None:
        m = settlement_margin([("rb2610", L, 8), ("rb2701", SH, 10)], {"rb2610": D("3056"), "rb2701": D("3111")}, reg, "20260930")
        [g] = m.groups
        assert (g.margin_group, g.method) == ("SHFE:rb", MarginMethod.BIG_SIDE)
        assert (g.long_margin, g.short_margin, g.excluded_margin, g.charged_margin) == (
            D("24448.00"), D("21777.00"), D("0"), D("24448.00"),
        )
        assert m.total == D("24448.00")
        assert m.gross_by_position() == {("rb2610", L): D("24448.00"), ("rb2701", SH): D("21777.00")}

    def test_excluded_contract_charged_two_way(self, reg: InstrumentRegistry) -> None:
        m = settlement_margin([("rb2610", L, 8), ("rb2701", SH, 10)], {"rb2610": D("3060"), "rb2701": D("3100")}, reg, "20261008")
        [g] = m.groups
        assert (g.long_margin, g.short_margin, g.excluded_margin, g.charged_margin) == (
            D("0"), D("21700.00"), D("24480.00"), D("46180.00"),
        )
        assert g.detail()["contracts"][0] == {
            "instrument_id": "rb2610", "direction": "LONG", "qty": 8, "price": "3060", "rate": "0.1",
            "margin": "24480.00", "excluded": True,
        }

    def test_products_are_separate_groups_on_shfe(self, reg: InstrumentRegistry) -> None:
        m = settlement_margin([("rb2701", L, 1), ("hc2701", SH, 1)], {"rb2701": D("3000"), "hc2701": D("3200")}, reg, "20260930")
        assert [(g.margin_group, g.charged_margin) for g in m.groups] == [("SHFE:hc", D("2240.00")), ("SHFE:rb", D("2100.00"))]
        assert m.total == D("4340.00")

    def test_gross_group_for_dce(self, reg: InstrumentRegistry) -> None:
        m = settlement_margin([("m2701", L, 2), ("m2701", SH, 1)], {"m2701": D("3000")}, reg, "20260930")
        [g] = m.groups
        assert (g.margin_group, g.method, g.charged_margin) == ("GROSS:m2701", MarginMethod.GROSS, D("7200.00"))
        assert (g.long_margin, g.short_margin) == (D("4800.00"), D("2400.00"))

    def test_cffex_groups_across_products(self, reg: InstrumentRegistry) -> None:
        prices = {"IF2612": D("4414.8"), "IH2612": D("2858.2"), "T2612": D("101.5"), "TF2612": D("102.0")}
        m = settlement_margin([("IF2612", L, 1), ("IH2612", SH, 1), ("T2612", SH, 2), ("TF2612", L, 3)], prices, reg, "20260930")
        assert [(g.margin_group, g.long_margin, g.short_margin, g.charged_margin) for g in m.groups] == [
            ("CFFEX:INDEX", D("158932.80"), D("102895.20"), D("158932.80")),
            ("CFFEX:TREASURY", D("36720.00"), D("40600.00"), D("40600.00")),
        ]

    def test_treasury_leaves_group_before_delivery_month(self, reg: InstrumentRegistry) -> None:
        prices = {"T2612": D("101.5"), "TF2612": D("102.0")}
        m = settlement_margin([("T2612", SH, 2), ("TF2612", L, 3)], prices, reg, "20261130")
        [g] = m.groups
        assert (g.long_margin, g.short_margin, g.excluded_margin, g.charged_margin) == (
            D("0"), D("0"), D("77320.00"), D("77320.00"),
        )

    def test_entries_validation(self, reg: InstrumentRegistry) -> None:
        assert settlement_margin([("rb2701", L, 0)], {}, reg, "20260930").total == D("0")
        with pytest.raises(KeyError):
            settlement_margin([("rb2701", L, 1)], {}, reg, "20260930")
        with pytest.raises(ValueError):
            margin_exact([MarginEntry("rb2701", L, -1, D("1"))], reg, "20260930")


class TestIntradayMargin:
    def test_float_matches_exact(self, reg: InstrumentRegistry) -> None:
        pos = [("rb2610", L, 8), ("rb2701", SH, 10), ("IF2612", L, 1)]
        exact = settlement_margin(pos, {"rb2610": D("3056"), "rb2701": D("3111"), "IF2612": D("4414.8")}, reg, "20260930")
        approx = intraday_margin(pos, {"rb2610": 3056.0, "rb2701": 3111.0, "IF2612": 4414.8}, reg, "20260930")
        assert approx.total == pytest.approx(float(exact.total), abs=1e-6)

    def test_intraday_exclusion_starts_the_day_after_exit(self, reg: InstrumentRegistry) -> None:
        pos = [("rb2610", L, 8), ("rb2701", SH, 10)]
        px = {"rb2610": 3060.0, "rb2701": 3100.0}
        assert intraday_margin(pos, px, reg, "20261008").total == pytest.approx(24480.0)  # 盘中仍享受大边
        assert intraday_margin(pos, px, reg, "20261009").total == pytest.approx(46180.0)


class TestFrozenMargin:
    POS = [("rb2701", L, 10)]
    PX = {"rb2701": 3000.0}

    def test_small_side_order_freezes_nothing(self, reg: InstrumentRegistry) -> None:
        orders = [MarginEntry("rb2701", SH, 5, 3000.0)]
        assert frozen_margin(self.POS, self.PX, orders, reg, "20260930") == 0.0

    def test_flipping_the_big_side_freezes_the_excess(self, reg: InstrumentRegistry) -> None:
        orders = [MarginEntry("rb2701", SH, 15, 3000.0)]
        assert frozen_margin(self.POS, self.PX, orders, reg, "20260930") == pytest.approx(10500.0)
        assert frozen_margin(self.POS, self.PX, orders, reg, "20260930", mode="GROSS") == pytest.approx(31500.0)

    def test_big_side_order_freezes_its_margin(self, reg: InstrumentRegistry) -> None:
        orders = [MarginEntry("rb2701", L, 1, 3010.0)]  # 按委托价冻结
        assert frozen_margin(self.POS, self.PX, orders, reg, "20260930") == pytest.approx(2107.0)

    def test_gross_exchange_and_empty(self, reg: InstrumentRegistry) -> None:
        assert frozen_margin([("m2701", L, 5)], {"m2701": 3000.0}, [MarginEntry("m2701", SH, 1, 3000.0)], reg, "20260930") == pytest.approx(2400.0)
        assert frozen_margin(self.POS, self.PX, [], reg, "20260930") == 0.0
