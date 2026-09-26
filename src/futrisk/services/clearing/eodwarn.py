"""WARN events of EOD runs (02 §8.4; 01 §5.3.5 notifications).

A WARN never changes a run's phase: it is recorded in ``eod_runs.step_log
["WARNINGS"]`` (the run row is flagged) and published as a ``sys.eod``
``EodPhaseEvent`` that repeats the run's **current** phase and carries

* ``detail["warning"]``: the human-readable message (Chinese/English mix as the
  other operator texts);
* ``detail["kind"]``: one of :data:`WARN_KINDS`;
* ``detail["warn_id"]``: a UUIDv7, stable key for notification dedupe
  (suggested ``notify_key``: ``EODWARN|{trading_day}|{version}|{warn_id}``);
* kind-specific context (``account_id``, ``instrument``, ``shards`` ...).

Consumers that act on a phase (maintenance after ``DONE``) must keep deduping
per trading day, because a WARN on a finished run repeats ``phase=DONE``.

Kinds:

============================  =================================================================
``LATE_FACT``                 a trade / cash movement reached the local copy after the day was
                              settled; the settlement does not contain it (operator decides on
                              ``settle.run`` ``force_new_version``)
``OFFICIAL_PRICE_DIFF``       an imported official price differs from the price a settled version
                              used (02 §6.5 ``settle.import_official``)
``PUSH_INCOMPLETE``           a shard did not confirm ``eod.rollover`` after the retries; the push
                              goes on (heartbeat / periodic re-push)
``PRE_SETTLE_MISMATCH``       md-gateway's previous settlement price differs from the SOD's (the
                              SOD value is used); carried by the ``PRICES_FIXED`` phase event
============================  =================================================================
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from loguru import logger
from sqlalchemy import Connection

from futrisk.common.ids import uuid7
from futrisk.messages.enums import EodPhase
from futrisk.messages.eod import EodPhaseEvent
from futrisk.messages.orders import Trade
from futrisk.messages.risk import CashMovement
from futrisk.messages.topics import SYS_EOD
from futrisk.runtime import AsyncServiceRuntime
from futrisk.services.clearing import store
from futrisk.services.clearing.store import EodRun, phase_index

LATE_FACT = "LATE_FACT"
OFFICIAL_PRICE_DIFF = "OFFICIAL_PRICE_DIFF"
PUSH_INCOMPLETE = "PUSH_INCOMPLETE"
PRE_SETTLE_MISMATCH = "PRE_SETTLE_MISMATCH"
WARN_KINDS = (LATE_FACT, OFFICIAL_PRICE_DIFF, PUSH_INCOMPLETE, PRE_SETTLE_MISMATCH)


def warn_tx(
    conn: Connection,
    rt: AsyncServiceRuntime,
    run: EodRun,
    kind: str,
    message: str,
    *,
    now_ns: int,
    extra: Mapping[str, Any] | None = None,
) -> EodRun | None:
    """Flag ``run`` with a WARN and append its ``sys.eod`` event (inside a writer transaction)."""
    warn_id = uuid7()
    context = dict(extra or {})
    entry = {"kind": kind, "message": message, "warn_id": warn_id, "ts_ns": now_ns, **context}
    updated = store.add_run_warning(conn, run.trading_day, run.version, entry)
    if updated is None:
        return None
    detail = {"warning": message, "kind": kind, "warn_id": warn_id, **context}
    rt.append_fact(
        conn,
        SYS_EOD,
        EodPhaseEvent(
            trading_day=updated.trading_day,
            version=updated.version,
            phase=updated.phase,
            next_trading_day=updated.next_trading_day,
            detail=detail,
            error=None,
            ts_ns=now_ns,
        ),
        td=updated.trading_day,
    )
    logger.warning("eod.warn", trading_day=run.trading_day, version=run.version, kind=kind, message=message)
    return updated


def flag_late_facts(
    conn: Connection,
    rt: AsyncServiceRuntime,
    *,
    trades: Iterable[Trade] = (),
    cash: Iterable[CashMovement] = (),
    now_ns: int,
) -> list[tuple[str, int, str]]:
    """New copy rows of a day whose run is already SETTLED (or later): WARN ``LATE_FACT`` (02 §8.4).

    One WARN event per ``(run, account)``; later late facts of the same account
    only raise the counters of that entry. Rows of days whose run has not
    settled yet are fine: step 5 re-checks the copy against the digests pinned
    by step 3 and re-reconciles. Returns the flagged ``(day, version, account)``.
    """
    per_day: dict[str, dict[str, dict[str, list[str]]]] = {}
    for t in trades:
        per_day.setdefault(t.trading_day, {}).setdefault(t.account_id, {"trades": [], "cash": []})["trades"].append(
            t.trade_key
        )
    for c in cash:
        per_day.setdefault(c.trading_day, {}).setdefault(c.account_id, {"trades": [], "cash": []})["cash"].append(
            c.movement_id
        )
    flagged: list[tuple[str, int, str]] = []
    for td, accounts in sorted(per_day.items()):
        run = store.latest_valid_run(conn, td)
        if run is None or phase_index(run.phase) < phase_index(EodPhase.SETTLED):
            continue
        for acct, keys in sorted(accounts.items()):
            n_t, n_c = len(keys["trades"]), len(keys["cash"])
            existing = _late_entry(run, acct)
            if existing is not None:
                _bump_late_entry(conn, run, acct, n_t, n_c, now_ns)
            else:
                what = "; ".join(
                    x for x in (f"{n_t} trade(s)" if n_t else "", f"{n_c} cash movement(s)" if n_c else "") if x
                )
                message = (
                    f"{td} v{run.version} ({run.phase.value}): {what} of {acct} arrived after the settlement "
                    f"and are not settled; re-settle with settle.run force_new_version if needed"
                )
                warn_tx(
                    conn,
                    rt,
                    run,
                    LATE_FACT,
                    message,
                    now_ns=now_ns,
                    extra={
                        "account_id": acct,
                        "trades": n_t,
                        "cash": n_c,
                        "keys": (keys["trades"] + keys["cash"])[:20],
                    },
                )
                run = store.get_run(conn, run.trading_day, run.version) or run
            flagged.append((td, run.version, acct))
    return flagged


def _late_entry(run: EodRun, account_id: str) -> dict[str, Any] | None:
    for e in run.step_log.get(store.WARNINGS_KEY) or []:
        if isinstance(e, dict) and e.get("kind") == LATE_FACT and e.get("account_id") == account_id:
            return e
    return None


def _bump_late_entry(conn: Connection, run: EodRun, account_id: str, n_trades: int, n_cash: int, now_ns: int) -> None:
    current = store.get_run(conn, run.trading_day, run.version)
    if current is None:
        return
    items = []
    for e in current.step_log.get(store.WARNINGS_KEY) or []:
        if isinstance(e, dict) and e.get("kind") == LATE_FACT and e.get("account_id") == account_id:
            e = {**e, "trades": int(e.get("trades", 0)) + n_trades, "cash": int(e.get("cash", 0)) + n_cash,
                 "last_ns": now_ns}
        items.append(e)
    current.step_log = {**current.step_log, store.WARNINGS_KEY: items}
    store.update_step_log(conn, current)
    logger.warning("eod.late_fact_again", trading_day=run.trading_day, version=run.version, account_id=account_id,
                   trades=n_trades, cash=n_cash)


__all__ = [
    "LATE_FACT",
    "OFFICIAL_PRICE_DIFF",
    "PRE_SETTLE_MISMATCH",
    "PUSH_INCOMPLETE",
    "WARN_KINDS",
    "flag_late_facts",
    "warn_tx",
]
