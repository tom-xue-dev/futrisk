"""EOD support of the risk engine (01 §6.6, §8.15; 02 §6.3 eod.*, §7.7, §8.4; 03 §4.3 eod_states).

- :class:`EodStatus` tracks one trading day of this shard: ``OPEN → FROZEN →
  ROLLED`` normally, ``PROVISIONAL`` when the next trading day's first session
  started before ``eod.rollover`` (the shard freezes itself, rolls the risk
  state and trades on a provisional SOD, closes only), ``FORCE_UNFROZEN`` after
  an audited ``eod.force_unfreeze``;
- :func:`cash_trading_day` implements the cash attribution rule of 02 §6.3:
  before the freeze → current day; frozen but not rolled → next trading day;
  rolled (even before the open, and in PROVISIONAL) → the new current day;
- :func:`account_digests` / :func:`cash_digests` build the ``EodFreezeAck``
  digests with the same algorithm as ``td.digest`` (02 §7.5);
- :func:`provisional_prices` and :func:`build_provisional_sod` build the SOD of
  the next day from the shard's own state (02 §8.4 构建临时快照): today lots
  become yesterday lots, basis = provisional settlement price (VWAP from
  ``md.settle_inputs`` rounded by the exchange rule, else the last mark, else
  the previous settlement), ``pre_balance`` = own end-of-day equity. It reuses
  :func:`futrisk.core.settlement.settle_day`, so the provisional numbers follow
  exactly the clearing formulas.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from futrisk.common.decimals import D, dec_str
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.settlement import settle_day
from futrisk.messages.enums import CashType, Direction, Exchange, Offset, TradeOrigin, TradingMode
from futrisk.messages.eod import EodFreezeAck, SodSnapshot
from futrisk.messages.md import SettleInput
from futrisk.messages.orders import AccountDigest, Trade, compute_account_digest
from futrisk.messages.risk import CashDigest, CashMovement

# RiskEodStatus (03 §3.3, storage only)
EOD_OPEN = "OPEN"
EOD_FROZEN = "FROZEN"
EOD_PROVISIONAL = "PROVISIONAL"
EOD_ROLLED = "ROLLED"
EOD_FORCE_UNFROZEN = "FORCE_UNFROZEN"

PROVISIONAL_SOD_VERSION = -1


@dataclass
class EodStatus:
    """One ``eod_states`` row."""

    trading_day: str
    status: str = EOD_OPEN
    freeze_version: int | None = None
    frozen_ns: int | None = None
    digest: dict[str, Any] | None = None
    provisional_ns: int | None = None
    next_trading_day: str | None = None
    sod_version: int | None = None
    rolled_ns: int | None = None
    forced_by: str | None = None
    forced_reason: str | None = None

    def row(self) -> dict[str, Any]:
        return {
            "trading_day": self.trading_day,
            "status": self.status,
            "freeze_version": self.freeze_version,
            "frozen_ns": self.frozen_ns,
            "digest": self.digest,
            "provisional_ns": self.provisional_ns,
            "next_trading_day": self.next_trading_day,
            "sod_version": self.sod_version,
            "rolled_ns": self.rolled_ns,
            "forced_by": self.forced_by,
            "forced_reason": self.forced_reason,
        }

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> EodStatus:
        digest = row.get("digest")
        if isinstance(digest, str):
            digest = json.loads(digest) if digest else None
        return cls(
            trading_day=str(row["trading_day"]),
            status=str(row["status"]),
            freeze_version=row.get("freeze_version"),
            frozen_ns=row.get("frozen_ns"),
            digest=digest,
            provisional_ns=row.get("provisional_ns"),
            next_trading_day=row.get("next_trading_day"),
            sod_version=row.get("sod_version"),
            rolled_ns=row.get("rolled_ns"),
            forced_by=row.get("forced_by"),
            forced_reason=row.get("forced_reason"),
        )


def cash_trading_day(mode: TradingMode, current_td: str, next_td: str | None) -> str:
    """Trading day a cash movement belongs to (02 §6.3 ``cash.move``, 01 §8.15).

    - ``OPEN`` (not frozen) and ``PROVISIONAL`` (already rolled): the current day;
    - ``EOD_FROZEN`` (frozen, not rolled): the next trading day, never later.
    """
    if mode == TradingMode.EOD_FROZEN:
        if next_td is None:
            raise ValueError("next trading day unknown while frozen")
        return next_td
    return current_td


def account_digests(trades_by_account: Mapping[str, Iterable[Trade]], accounts: Iterable[str]) -> list[AccountDigest]:
    """Per-account digests of applied fills, same algorithm as ``td.digest`` (02 §7.5)."""
    return [compute_account_digest(a, list(trades_by_account.get(a, ()))) for a in sorted(accounts)]


def cash_digests(cash_by_account: Mapping[str, Iterable[CashMovement]], accounts: Iterable[str]) -> list[CashDigest]:
    """``{account_id, count, amount_sum}``; withdrawals count negative in ``amount_sum``."""
    out: list[CashDigest] = []
    for a in sorted(accounts):
        items = list(cash_by_account.get(a, ()))
        total = Decimal(0)
        for c in items:
            amt = D(c.amount)
            total += amt if c.type == CashType.DEPOSIT else -amt
        out.append(CashDigest(account_id=a, count=len(items), amount_sum=dec_str(total.quantize(Decimal("0.01")))))
    return out


def trade_from_fill_row(row: Mapping[str, Any]) -> Trade:
    """``Trade`` from a risk ``fills`` row (03 §4.3; the columns are named like the Trade fields)."""
    return Trade(
        trading_day=str(row["trading_day"]),
        exchange=Exchange(str(row["exchange"])),
        trade_id=str(row["trade_id"]),
        direction=Direction(str(row["direction"])),
        account_id=str(row["account_id"]),
        instrument_id=str(row["instrument_id"]),
        client_order_id=row.get("client_order_id"),
        origin=TradeOrigin(str(row.get("origin") or TradeOrigin.INTERNAL.value)),
        broker_order_ref=str(row["broker_order_ref"]),
        offset=Offset(str(row["offset"])),
        price=float(D(str(row["price"]))),
        qty=int(row["qty"]),
        trade_ts_ns=int(row["trade_ts_ns"]),
        broker_seq=None if row.get("broker_seq") is None else int(row["broker_seq"]),
        trade_seq=int(row.get("trade_seq") or 0),
        received_ns=int(row.get("received_ns") or 0),
    )


def cash_from_row(row: Mapping[str, Any]) -> CashMovement:
    """``CashMovement`` from a risk ``cash_movements`` row (03 §4.3)."""
    return CashMovement(
        movement_id=str(row["movement_id"]),
        account_id=str(row["account_id"]),
        trading_day=str(row["trading_day"]),
        type=CashType(str(row["type"])),
        amount=str(row["amount"]),
        actor=str(row.get("actor") or ""),
        reason=str(row.get("reason") or ""),
        ts_ns=int(row.get("ts_ns") or 0),
    )


def digest_only_ack(
    *,
    trading_day: str,
    shard: int,
    account_ids: Iterable[str],
    trades: Iterable[Trade],
    cash: Iterable[CashMovement],
    frozen_ns: int,
) -> EodFreezeAck:
    """Digest-only ``EodFreezeAck`` for an older unsettled day (catch-up EOD, 02 §8.4).

    Used by the shell for ``eod.freeze{X}`` when ``EngineCore.freeze_digest_source(X)``
    is ``"STORE"``: the day is closed, nothing is frozen, and the digests are
    built from the persisted ``fills`` / ``cash_movements`` of X with the same
    algorithm as ``td.digest`` (02 §7.5). ``open_orders`` is 0 (orders of a
    closed day are expired at the roll).
    """
    accounts = sorted(set(account_ids))
    by_acct: dict[str, list[Trade]] = {}
    for t in trades:
        if t.trading_day == trading_day:
            by_acct.setdefault(t.account_id, []).append(t)
    cash_by: dict[str, list[CashMovement]] = {}
    for c in cash:
        if c.trading_day == trading_day:
            cash_by.setdefault(c.account_id, []).append(c)
    return EodFreezeAck(
        trading_day=trading_day,
        shard=shard,
        accounts=account_digests(by_acct, accounts),
        cash=cash_digests(cash_by, accounts),
        open_orders=0,
        frozen_ns=frozen_ns,
    )


def provisional_prices(
    registry: InstrumentRegistry,
    instruments: Iterable[str],
    *,
    settle_inputs: Iterable[SettleInput] = (),
    marks: Mapping[str, float] | None = None,
    pre_settle: Mapping[str, Decimal] | None = None,
) -> dict[str, Decimal]:
    """Provisional settlement prices: VWAP (exchange rounding) > last mark > previous settlement (02 §8.4)."""
    by_inst = {s.instrument_id: s for s in settle_inputs}
    marks = marks or {}
    pre_settle = pre_settle or {}
    out: dict[str, Decimal] = {}
    for inst in sorted(set(instruments)):
        si = by_inst.get(inst)
        if si is not None and si.vwap_raw is not None:
            out[inst] = registry.round_price(inst, D(si.vwap_raw), registry.settle_rounding(inst))
        elif inst in marks:
            out[inst] = D(marks[inst])
        elif si is not None and si.last is not None:
            out[inst] = D(si.last)
        elif inst in pre_settle:
            out[inst] = pre_settle[inst]
        else:
            raise KeyError(f"no provisional price for {inst}")
    return out


def build_provisional_sod(
    *,
    registry: InstrumentRegistry,
    sod: SodSnapshot,
    trades: Iterable[Trade],
    cash: Iterable[CashMovement],
    prices: Mapping[str, Decimal],
    next_trading_day: str,
    pre_settle: Mapping[str, Decimal] | None = None,
) -> SodSnapshot:
    """Next day's SOD from the shard's own day (version −1, 02 §8.4).

    ``pre_settle`` supplies previous settlement prices of contracts missing from
    the SOD's map (e.g. a contract first traded today; the engine takes them
    from the ticks' ``pre_settle``).
    """
    missing = {k: v for k, v in (pre_settle or {}).items() if k not in sod.pre_settle}
    res = settle_day(
        trading_day=sod.trading_day,
        version=0,
        sod=sod,
        trades=list(trades),
        cash_movements=list(cash),
        settle_prices=dict(prices),
        registry=registry,
        next_trading_day=next_trading_day,
        sod_version=PROVISIONAL_SOD_VERSION,
        pre_settle=missing,
        require_price_views=False,  # 临时结算价没有 SettlementPriceView（02 §8.4 构建临时快照）
    )
    nxt = res.next_sod
    return SodSnapshot(
        trading_day=nxt.trading_day,
        version=PROVISIONAL_SOD_VERSION,
        from_settlement=None,
        accounts=nxt.accounts,
        pre_settle=nxt.pre_settle,
    )


__all__ = [
    "EOD_FORCE_UNFROZEN",
    "EOD_FROZEN",
    "EOD_OPEN",
    "EOD_PROVISIONAL",
    "EOD_ROLLED",
    "PROVISIONAL_SOD_VERSION",
    "EodStatus",
    "account_digests",
    "build_provisional_sod",
    "cash_digests",
    "cash_from_row",
    "cash_trading_day",
    "digest_only_ack",
    "provisional_prices",
    "trade_from_fill_row",
]
