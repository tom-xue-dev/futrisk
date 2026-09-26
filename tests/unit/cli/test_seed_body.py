"""``futrisk demo`` first-day books: ``accounts.yaml`` initial positions become yesterday lots."""

from __future__ import annotations

import dataclasses
from decimal import Decimal

from futrisk.cli.ops_cmd import seed_request_body
from futrisk.common.config import FullConfig
from futrisk.common.config.accounts import InitialLot
from futrisk.messages.enums import PosDirection


def _with_positions(cfg: FullConfig, account_id: str, lots: list[InitialLot]) -> FullConfig:
    spec = cfg.accounts.accounts[account_id].model_copy(update={"initial_positions": lots})
    accounts = cfg.accounts.model_copy(update={"accounts": {**cfg.accounts.accounts, account_id: spec}})
    return dataclasses.replace(cfg, accounts=accounts)


def test_default_books_are_flat(default_config: FullConfig) -> None:
    body = seed_request_body(default_config, "20260928")
    assert {a["account_id"] for a in body["accounts"]} == {"ACC1", "ACC2", "ACC3", "ACC4", "ACC5"}
    assert all(a["lots"] == [] and a["pre_balance"] == a["initial_capital"] for a in body["accounts"])
    assert body["pre_settle"]["rb2701"] == "3120"


def test_initial_positions_become_yesterday_lots_at_the_pre_settle(default_config: FullConfig) -> None:
    cfg = _with_positions(default_config, "ACC5", [
        InitialLot(instrument_id="rb2701", direction=PosDirection.LONG, qty=10, open_price=Decimal("3100")),
        InitialLot(instrument_id="rb2705", direction=PosDirection.SHORT, qty=10, open_price=Decimal("3140"),
                   open_td="20260923"),
    ])
    body = seed_request_body(cfg, "20260928")
    lots = next(a["lots"] for a in body["accounts"] if a["account_id"] == "ACC5")
    assert [(lot["instrument_id"], lot["direction"], lot["qty"], lot["lot_no"]) for lot in lots] == [
        ("rb2701", "LONG", 10, 1), ("rb2705", "SHORT", 10, 2)]
    assert lots[0]["open_td"] == "20260924"  # the trading day before 20260928 (09-25 is a holiday)
    assert lots[1]["open_td"] == "20260923"
    # a start-of-day lot is valued from yesterday's settlement (clearing refuses basis != pre_settle)
    assert [lot["basis_price"] for lot in lots] == [body["pre_settle"]["rb2701"], body["pre_settle"]["rb2705"]]
    assert [lot["open_price"] for lot in lots] == ["3100", "3140"]
