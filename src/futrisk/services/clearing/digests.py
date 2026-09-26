"""EOD step 3: three-way trade digests and cash digests (02 §7.5, §7.7, §8.4; 03 §6).

* ``TRADE_DIGEST``: per account, the local ``trades`` copy, trade-gateway's
  ``td.digest`` and the risk shards' ``EodFreezeAck.accounts`` must agree on
  ``(count, qty_sum, sha256)`` (all computed with the 02 §7.5 algorithm,
  :func:`futrisk.messages.orders.compute_account_digest`);
* ``CASH_DIGEST``: per account, the local ``cash_movements`` copy must agree
  with ``EodFreezeAck.cash`` on ``(count, amount_sum)``.

``amount_sum`` is the signed net of the day's movements: deposits minus
withdrawals (both are positive ``amount`` fields in 02 §7.6 ``CashMovement``),
the same rule as the risk shards' :func:`futrisk.core.eod.cash_digests`; a
movement recorded with the wrong type therefore breaks the digest. Amounts are
compared as decimals so that ``"100"`` equals ``"100.00"``. An account missing
from one side counts as zero trades / zero movements.

Re-settlement after the roll (``force_new_version`` of a day whose SOD the
shards already hold, 02 §8.4 换日之后又重新结算) does not freeze the shards
again - they trade the next day. :func:`compare_resettle` then reconciles the
trades two-way (local copy vs ``td.digest``, a difference FAILs) and compares
both copies with the digests pinned by the settled version: a difference there
(late facts now included) is a WARN, never a FAIL.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from futrisk.common.decimals import D, dec_str, q_money
from futrisk.messages.enums import CashType, CheckStatus, ReconCheck
from futrisk.messages.eod import CheckResult, EodFreezeAck
from futrisk.messages.orders import AccountDigest, Trade, TradeDigest, compute_account_digest
from futrisk.messages.risk import CashDigest, CashMovement

EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


def empty_digest(account_id: str) -> AccountDigest:
    return AccountDigest(account_id=account_id, count=0, qty_sum=0, max_trade_seq=0, sha256=EMPTY_SHA256)


def local_trade_digests(trades: Iterable[Trade]) -> dict[str, AccountDigest]:
    """Per-account digests of the local copy (same algorithm as td.digest and the risk shards)."""
    by_acct: dict[str, list[Trade]] = {}
    for t in trades:
        by_acct.setdefault(t.account_id, []).append(t)
    return {acct: compute_account_digest(acct, rows) for acct, rows in sorted(by_acct.items())}


def cash_digests(movements: Iterable[CashMovement]) -> dict[str, CashDigest]:
    """Per-account cash digest: count and Σ deposits − Σ withdrawals (money, two decimals)."""
    acc: dict[str, tuple[int, Decimal]] = {}
    for m in movements:
        n, s = acc.get(m.account_id, (0, Decimal(0)))
        amt = D(m.amount)
        acc[m.account_id] = (n + 1, s + (amt if m.type == CashType.DEPOSIT else -amt))
    return {
        acct: CashDigest(account_id=acct, count=n, amount_sum=dec_str(q_money(s))) for acct, (n, s) in sorted(acc.items())
    }


def _digest_view(d: AccountDigest | None) -> dict[str, Any]:
    if d is None:
        return {"count": 0, "qty_sum": 0, "max_trade_seq": 0, "sha256": EMPTY_SHA256}
    return {"count": d.count, "qty_sum": d.qty_sum, "max_trade_seq": d.max_trade_seq, "sha256": d.sha256}


def _same(a: AccountDigest | None, b: AccountDigest | None) -> bool:
    va, vb = _digest_view(a), _digest_view(b)
    return (va["count"], va["qty_sum"], va["sha256"]) == (vb["count"], vb["qty_sum"], vb["sha256"])


@dataclass(frozen=True)
class DigestComparison:
    """Outcome of one comparison round (checks for ``recon_checks`` plus mismatching accounts)."""

    checks: list[CheckResult]
    trade_mismatch: set[str]
    cash_mismatch: set[str]
    local_missing: set[str]  # accounts whose local copy disagrees with trade-gateway

    @property
    def ok(self) -> bool:
        return not self.trade_mismatch and not self.cash_mismatch


def ack_digests(acks: Mapping[str, EodFreezeAck]) -> tuple[dict[str, AccountDigest], dict[str, CashDigest], dict[str, str]]:
    """Merge the shards' acks: account -> trade digest / cash digest, and account -> shard instance."""
    trades: dict[str, AccountDigest] = {}
    cash: dict[str, CashDigest] = {}
    owner: dict[str, str] = {}
    for inst, ack in sorted(acks.items()):
        for d in ack.accounts:
            trades[d.account_id] = d
            owner[d.account_id] = inst
        for c in ack.cash:
            cash[c.account_id] = c
            owner.setdefault(c.account_id, inst)
    return trades, cash, owner


def compare(
    *,
    accounts: Iterable[str],
    local_trades: Mapping[str, AccountDigest],
    td_digest: TradeDigest,
    acks: Mapping[str, EodFreezeAck],
    local_cash: Mapping[str, CashDigest],
) -> DigestComparison:
    """Compare the three trade digests and the two cash digests for every account involved."""
    td_map = {d.account_id: d for d in td_digest.accounts}
    ack_trades, ack_cash, owner = ack_digests(acks)
    all_accounts = sorted(
        set(accounts) | set(local_trades) | set(td_map) | set(ack_trades) | set(local_cash) | set(ack_cash)
    )
    checks: list[CheckResult] = []
    trade_bad: set[str] = set()
    cash_bad: set[str] = set()
    local_missing: set[str] = set()
    for acct in all_accounts:
        loc, gw, rk = local_trades.get(acct), td_map.get(acct), ack_trades.get(acct)
        ok = _same(loc, gw) and _same(gw, rk)
        if not _same(loc, gw):
            local_missing.add(acct)
        if not ok:
            trade_bad.add(acct)
        lv, gv = _digest_view(loc), _digest_view(gw)
        checks.append(
            CheckResult(
                check=ReconCheck.TRADE_DIGEST,
                status=CheckStatus.PASS if ok else CheckStatus.FAIL,
                account_id=acct,
                detail={
                    "expected": str(gv["count"]),
                    "actual": str(lv["count"]),
                    "diff": str(lv["count"] - gv["count"]),
                    "local": lv,
                    "trade_gateway": gv,
                    "risk": _digest_view(rk),
                    "shard": owner.get(acct),
                },
            )
        )
        lc, rc = local_cash.get(acct), ack_cash.get(acct)
        l_n, l_s = (lc.count, D(lc.amount_sum)) if lc is not None else (0, Decimal(0))
        r_n, r_s = (rc.count, D(rc.amount_sum)) if rc is not None else (0, Decimal(0))
        c_ok = l_n == r_n and l_s == r_s
        if not c_ok:
            cash_bad.add(acct)
        checks.append(
            CheckResult(
                check=ReconCheck.CASH_DIGEST,
                status=CheckStatus.PASS if c_ok else CheckStatus.FAIL,
                account_id=acct,
                detail={
                    "expected": dec_str(q_money(r_s)),
                    "actual": dec_str(q_money(l_s)),
                    "diff": dec_str(q_money(l_s - r_s)),
                    "local": {"count": l_n, "amount_sum": dec_str(q_money(l_s))},
                    "risk": {"count": r_n, "amount_sum": dec_str(q_money(r_s))},
                    "shard": owner.get(acct),
                },
            )
        )
    return DigestComparison(checks=checks, trade_mismatch=trade_bad, cash_mismatch=cash_bad, local_missing=local_missing)


def compare_resettle(
    *,
    accounts: Iterable[str],
    local_trades: Mapping[str, AccountDigest],
    td_digest: TradeDigest,
    local_cash: Mapping[str, CashDigest],
    reference: Mapping[str, Mapping[str, list[Any]]] | None,
    reference_version: int,
) -> DigestComparison:
    """Digest checks of a re-settlement after the roll (no freeze): two-way trades, reference WARNs."""
    td_map = {d.account_id: d for d in td_digest.accounts}
    ref_t = dict((reference or {}).get("trades") or {})
    ref_c = dict((reference or {}).get("cash") or {})
    all_accounts = sorted(
        set(accounts) | set(local_trades) | set(td_map) | set(local_cash) | set(ref_t) | set(ref_c)
    )
    note = f"not frozen: re-settlement of v{reference_version} after the roll"
    checks: list[CheckResult] = []
    trade_bad: set[str] = set()
    for acct in all_accounts:
        loc, gw = local_trades.get(acct), td_map.get(acct)
        lv, gv = _digest_view(loc), _digest_view(gw)
        ok = _same(loc, gw)
        if not ok:
            trade_bad.add(acct)
        ref = ref_t.get(acct)
        rv = {"count": int(ref[0]), "qty_sum": int(ref[1]), "sha256": str(ref[2])} if ref else {
            "count": 0, "qty_sum": 0, "sha256": EMPTY_SHA256}
        changed = (lv["count"], lv["qty_sum"], lv["sha256"]) != (rv["count"], rv["qty_sum"], rv["sha256"])
        status = CheckStatus.FAIL if not ok else (CheckStatus.WARN if changed else CheckStatus.PASS)
        checks.append(
            CheckResult(
                check=ReconCheck.TRADE_DIGEST,
                status=status,
                account_id=acct,
                detail={
                    "expected": str(gv["count"]),
                    "actual": str(lv["count"]),
                    "diff": str(lv["count"] - gv["count"]),
                    "local": lv,
                    "trade_gateway": gv,
                    "risk": note,
                    "reference": rv,
                    "reference_version": reference_version,
                },
            )
        )
        lc, rc = local_cash.get(acct), ref_c.get(acct)
        l_n, l_s = (lc.count, D(lc.amount_sum)) if lc is not None else (0, Decimal(0))
        r_n, r_s = (int(rc[0]), D(str(rc[1]))) if rc is not None else (0, Decimal(0))
        c_ok = l_n == r_n and l_s == r_s
        checks.append(
            CheckResult(
                check=ReconCheck.CASH_DIGEST,
                status=CheckStatus.PASS if c_ok else CheckStatus.WARN,
                account_id=acct,
                detail={
                    "expected": dec_str(q_money(r_s)),
                    "actual": dec_str(q_money(l_s)),
                    "diff": dec_str(q_money(l_s - r_s)),
                    "local": {"count": l_n, "amount_sum": dec_str(q_money(l_s))},
                    "risk": note,
                    "reference_version": reference_version,
                },
            )
        )
    return DigestComparison(checks=checks, trade_mismatch=trade_bad, cash_mismatch=set(), local_missing=set(trade_bad))


def pin(trades: Iterable[Trade], cash: Iterable[CashMovement]) -> dict[str, dict[str, list[Any]]]:
    """Per-account digests of the reconciled rows, pinned in the ``TRADES_RECONCILED`` step log (H-2).

    ``{"trades": {acct: [count, qty_sum, sha256]}, "cash": {acct: [count, amount_sum]}}``.
    """
    t = {a: [d.count, d.qty_sum, d.sha256] for a, d in local_trade_digests(trades).items()}
    c = {a: [d.count, d.amount_sum] for a, d in cash_digests(cash).items()}
    return {"trades": t, "cash": c}


def changed_accounts(
    pinned: Mapping[str, Mapping[str, list[Any]]], trades: Iterable[Trade], cash: Iterable[CashMovement]
) -> list[str]:
    """Accounts whose rows differ from the pinned digests (step 5 settles only what step 3 reconciled)."""
    now = pin(trades, cash)
    bad: set[str] = set()
    for kind in ("trades", "cash"):
        old = pinned.get(kind) or {}
        new = now[kind]
        for acct in set(old) | set(new):
            a, b = old.get(acct), new.get(acct)
            if kind == "cash" and a is not None and b is not None:
                if int(a[0]) != int(b[0]) or D(str(a[1])) != D(str(b[1])):
                    bad.add(acct)
            elif (list(a) if a is not None else None) != (list(b) if b is not None else None):
                bad.add(acct)
    return sorted(bad)


def open_order_checks(acks: Mapping[str, EodFreezeAck]) -> list[CheckResult]:
    """``open_orders > 0`` at freeze time is a WARN of the trade reconciliation (02 §7.7)."""
    out: list[CheckResult] = []
    for inst, ack in sorted(acks.items()):
        if ack.open_orders > 0:
            out.append(
                CheckResult(
                    check=ReconCheck.TRADE_DIGEST,
                    status=CheckStatus.WARN,
                    account_id=None,
                    detail={"shard": inst, "open_orders": ack.open_orders, "reason": "open orders at freeze"},
                )
            )
    return out


__all__ = [
    "EMPTY_SHA256",
    "DigestComparison",
    "ack_digests",
    "changed_accounts",
    "cash_digests",
    "compare",
    "compare_resettle",
    "empty_digest",
    "local_trade_digests",
    "open_order_checks",
    "pin",
]
