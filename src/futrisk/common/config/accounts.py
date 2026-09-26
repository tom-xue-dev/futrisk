"""``accounts.yaml``: strategy accounts and shard ownership."""

from __future__ import annotations

import re
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from futrisk.messages.enums import PosDirection

#: account ids appear in bus topics (``oms.order.{id}``, no ".", 02 §4.4) and in report file names
ACCOUNT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


class InitialLot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    instrument_id: str
    direction: PosDirection
    qty: int
    open_price: Decimal
    open_td: str | None = None


class AccountSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str
    shard: int
    initial_capital: Decimal
    strategy: str | None = None
    products: list[str] = Field(default_factory=list)
    initial_positions: list[InitialLot] = Field(default_factory=list)


class AccountsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    accounts: dict[str, AccountSpec]

    @field_validator("accounts")
    @classmethod
    def _valid_ids(cls, v: dict[str, AccountSpec]) -> dict[str, AccountSpec]:
        bad = [a for a in v if not ACCOUNT_ID_RE.match(a)]
        if bad:
            raise ValueError(f"invalid account id(s) {bad}: use 1-32 of A-Z a-z 0-9 _ -")
        return v

    def shard_of(self, account_id: str) -> int:
        return self.accounts[account_id].shard

    def accounts_of_shard(self, shard: int) -> list[str]:
        return sorted(a for a, spec in self.accounts.items() if spec.shard == shard)

    def shards(self) -> list[int]:
        return sorted({spec.shard for spec in self.accounts.values()})
