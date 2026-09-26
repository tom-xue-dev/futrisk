"""The 9-step pre-trade pipeline (01 §5.3.1 事前风控流水线; 02 §11.2 拒单码 and 平仓委托的豁免).

Steps run in order and the first failure wins:

1. static - unknown instrument, not tradable, invalid fields (LIMIT without
   price, FORCE_CLOSE from a client, non-positive price), qty ≤ 0, max order
   qty per product and order type, tick grid. Unknown account / duplicate ids
   are handled by the engine before the pipeline (no row may be written);
2. system state - LOADING / STOPPING → ``REJECT_SYSTEM_NOT_READY`` (everything),
   ``EOD_FROZEN`` → ``REJECT_EOD_IN_PROGRESS``, session closed (calendar phase at
   market time), MD quality ≠ OK (opens only), kill switch manual OR rule
   (opens only), trade-gateway down;
3. price - limit band, fat finger vs the mark: |price − mark| > max(k ticks, x%)
   (closes skip the fat finger while the instrument's quality is not OK: a
   stale mark must not block risk reduction; the limit band still applies);
4. account & restrictions (opens only) - HALTED, BLOCK_OPEN (account scope →
   ``REJECT_ACCOUNT_BLOCKED`` with ``detail.source``; product / sector /
   instrument scope → ``REJECT_PRODUCT_BLOCKED``), ORDER_SIZE_FACTOR,
   NO_ADD_TO_LOSERS, EXPOSURE_CAP_FACTOR;
5. throttle - token bucket per account; per-contract daily open limit (opens);
6. position - close qty vs closable incl. in-flight closes (SHFE today/yd split
   through :meth:`Position.check_close`); position limit incl. in-flight opens;
7. funds (opens only) - incremental margin + fee ≤ available × (1 − buffer);
8. what-if (opens only) - rules with ``pretrade: true``: reject when the metric
   after the order would breach a level that blocks opens (circuit break or
   ``block_open``) and the order makes it worse. The what-if runs on the last
   known marks whatever their quality (it can only reject; the order's own
   instrument is already gated by step 2) and honours ``min_gross_pct``;
   metric values come from :class:`~futrisk.core.metrics.WhatIf` (scalar fast
   path, full :meth:`AccountValuation.whatif` fallback);
9. self-trade - crossing the account's own resting opposite orders.

Close orders skip the MD-quality and kill-switch checks of step 2 and steps 4,
7 and 8 entirely (02 §11.2 平仓委托的豁免), so a halted, blocked or
EOD-provisional account can always reduce risk.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from futrisk.common.calendar import TradingCalendar
from futrisk.common.config.risk_rules import PretradeConfig
from futrisk.common.decimals import on_tick
from futrisk.core.evaluation import rule_value
from futrisk.core.instruments import InstrumentRegistry
from futrisk.core.ledger import RestingOrder
from futrisk.core.lots import normalize_offset, pos_direction
from futrisk.core.metrics import AccountValuation, OrderDelta, StaticInfo, WhatIf
from futrisk.core.orders import OrderBook, OrderRecord
from futrisk.core.restrictions import OrderScope, is_account_wide, matching
from futrisk.core.risk_account import Lifecycle, RiskAccount
from futrisk.core.rules import (
    CompiledRule,
    acct_scope,
    instrument_scope,
    product_scope,
    sector_scope,
)
from futrisk.messages import codes
from futrisk.messages.enums import (
    AccountState,
    Direction,
    MdQuality,
    Offset,
    OrderType,
    PosDirection,
    RestrictionKind,
    TradingMode,
)
from futrisk.messages.orders import OrderRequest


class PretradeHost(Protocol):
    """What the pipeline reads from the engine (implemented by :class:`EngineCore`)."""

    @property
    def registry(self) -> InstrumentRegistry: ...

    @property
    def calendar(self) -> TradingCalendar | None: ...

    @property
    def trading_day(self) -> str: ...

    @property
    def lifecycle(self) -> Lifecycle: ...

    @property
    def trading_mode(self) -> TradingMode: ...

    @property
    def gateway_up(self) -> bool: ...

    @property
    def pretrade_cfg(self) -> PretradeConfig: ...

    @property
    def orders(self) -> OrderBook: ...

    def quality_of(self, instrument_id: str) -> MdQuality: ...

    def kill_switch_effective(self) -> bool: ...

    def mark_of(self, instrument_id: str) -> float | None: ...

    def session_open(self, instrument_id: str, now_ns: int) -> bool: ...

    def limits_of(self, instrument_id: str) -> tuple[float, float] | None: ...

    def pretrade_rules(self) -> Sequence[CompiledRule]: ...

    def valuation(self, account: RiskAccount) -> AccountValuation: ...

    def static_info(self, instrument_id: str) -> StaticInfo: ...


@dataclass
class PretradeResult:
    """Outcome of the pipeline; ``code`` None means accepted."""

    offset: Offset
    code: str | None = None
    detail: dict[str, Any] | None = None
    step: int = 0
    frozen_margin: float = 0.0
    frozen_fee: float = 0.0
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.code is None


class _Reject(Exception):
    def __init__(self, step: int, code: str, detail: dict[str, Any] | None = None) -> None:
        super().__init__(code)
        self.step = step
        self.code = code
        self.detail = detail


def _max_qty(host: PretradeHost, req: OrderRequest) -> int:
    spec = host.registry.product_spec(req.instrument_id)
    return int(spec.max_order_qty.get(req.order_type.value, host.pretrade_cfg.default_max_order_qty))


def check_order(host: PretradeHost, account: RiskAccount, req: OrderRequest, *, now_ns: int, mono_ns: int) -> PretradeResult:
    """Run the pipeline for ``req`` on ``account``; mutates only the throttle bucket."""
    offset = req.offset
    try:
        offset = _static(host, req)
        is_open = offset == Offset.OPEN
        _system(host, req, is_open, now_ns)
        _price(host, req, is_open)
        scope = _order_scope(host, account.account_id, req.instrument_id)
        val = host.valuation(account)
        if is_open:
            _account(host, account, req, scope, val)
        resting = host.orders.resting_on(account.account_id, req.instrument_id)
        _throttle(host, account, req, is_open, mono_ns, resting)
        _position(host, account, req, offset, is_open, resting)
        fm, fee = 0.0, 0.0
        if is_open:
            fm, fee = _funds(host, account, req, val)
            _whatif(host, account, req, scope, val, fm, fee)
        _self_trade(req, resting)
        return PretradeResult(offset=offset, frozen_margin=fm, frozen_fee=fee)
    except _Reject as r:
        return PretradeResult(offset=offset, code=r.code, detail=r.detail, step=r.step)


# ------------------------------------------------------------------ step 1
def _static(host: PretradeHost, req: OrderRequest) -> Offset:
    reg = host.registry
    if not reg.is_listed(req.instrument_id):
        raise _Reject(1, codes.REJECT_UNKNOWN_INSTRUMENT, {"instrument_id": req.instrument_id})
    info = reg.get(req.instrument_id)
    td = host.trading_day
    contract = info.contract
    if contract is not None and (td > contract.last_trade_day or td < contract.listed_day):
        raise _Reject(1, codes.REJECT_NOT_TRADABLE, {"last_trade_day": contract.last_trade_day, "listed_day": contract.listed_day})
    if req.offset == Offset.FORCE_CLOSE:
        raise _Reject(1, codes.REJECT_INVALID_ORDER, {"reason": "FORCE_CLOSE is broker-only"})
    if req.order_type == OrderType.LIMIT and req.price is None:
        raise _Reject(1, codes.REJECT_INVALID_ORDER, {"reason": "LIMIT order without price"})
    if req.price is not None and (not math.isfinite(req.price) or req.price <= 0):
        raise _Reject(1, codes.REJECT_INVALID_ORDER, {"reason": "price must be positive"})
    if req.qty <= 0:
        raise _Reject(1, codes.REJECT_INVALID_QTY, {"qty": req.qty})
    max_qty = _max_qty(host, req)
    if req.qty > max_qty:
        raise _Reject(1, codes.REJECT_QTY_EXCEEDS_MAX, {"qty": req.qty, "max": max_qty})
    if req.order_type == OrderType.LIMIT and req.price is not None and not on_tick(req.price, info.tick_size):
        raise _Reject(1, codes.REJECT_PRICE_OFF_TICK, {"price": req.price, "tick": str(info.tick_size)})
    return normalize_offset(info.rules, req.offset)


# ------------------------------------------------------------------ step 2
def _system(host: PretradeHost, req: OrderRequest, is_open: bool, now_ns: int) -> None:
    if host.lifecycle in (Lifecycle.LOADING, Lifecycle.STOPPING):
        raise _Reject(2, codes.REJECT_SYSTEM_NOT_READY, {"lifecycle": host.lifecycle.value})
    if host.trading_mode == TradingMode.EOD_FROZEN:
        raise _Reject(2, codes.REJECT_EOD_IN_PROGRESS, {"trading_day": host.trading_day})
    cal = host.calendar
    if cal is not None and not host.session_open(req.instrument_id, now_ns):
        # 慢路径只在拒单时走：给出日历判定的阶段作为拒单依据
        phase = cal.phase_at(host.registry.product_spec(req.instrument_id).sessions, now_ns)
        td = phase.trading_day.strftime("%Y%m%d") if phase.trading_day is not None else None
        raise _Reject(2, codes.REJECT_SESSION_CLOSED, {"phase": phase.state, "phase_trading_day": td})
    if is_open and host.quality_of(req.instrument_id) != MdQuality.OK:
        raise _Reject(2, codes.REJECT_MD_UNAVAILABLE, {"quality": host.quality_of(req.instrument_id).value})
    if is_open and host.kill_switch_effective():
        raise _Reject(2, codes.REJECT_FIRM_KILL_SWITCH)
    if not host.gateway_up:
        raise _Reject(2, codes.REJECT_GATEWAY_UNAVAILABLE)


# ------------------------------------------------------------------ step 3
def _price(host: PretradeHost, req: OrderRequest, is_open: bool) -> None:
    if req.order_type != OrderType.LIMIT or req.price is None:
        return
    limits = host.limits_of(req.instrument_id)
    if limits is not None:
        upper, lower = limits
        if req.price > upper + 1e-9 or req.price < lower - 1e-9:
            raise _Reject(3, codes.REJECT_PRICE_LIMIT, {"price": req.price, "upper": upper, "lower": lower})
    if not is_open and host.quality_of(req.instrument_id) != MdQuality.OK:
        return  # 平仓豁免：行情不可信时不按过期的标记价做胖手指检查（涨跌停带仍然检查）
    mark = host.mark_of(req.instrument_id)
    if mark is not None and mark > 0:
        cfg = host.pretrade_cfg
        tick = float(host.registry.tick_size(req.instrument_id))
        band = max(cfg.fat_finger_ticks * tick, cfg.fat_finger_pct * mark)
        if abs(req.price - mark) > band + 1e-9:
            raise _Reject(3, codes.REJECT_FAT_FINGER, {"price": req.price, "mark": mark, "band": band})


# ------------------------------------------------------------------ step 4
def _order_scope(host: PretradeHost, account_id: str, instrument_id: str) -> OrderScope:
    info = host.registry.get(instrument_id)
    return OrderScope(account_id=account_id, instrument_id=instrument_id, product=info.product, sector=info.sector)


def _account(host: PretradeHost, account: RiskAccount, req: OrderRequest, scope: OrderScope, val: AccountValuation) -> None:
    if account.state == AccountState.HALTED:
        raise _Reject(4, codes.REJECT_ACCOUNT_HALTED, {"halted_latched": account.halted_latched})
    rs = account.restrictions
    for r in matching(rs, scope, RestrictionKind.BLOCK_OPEN):
        if is_account_wide(r):
            raise _Reject(4, codes.REJECT_ACCOUNT_BLOCKED, {"source": r.source, "scope_key": r.scope_key})
    for r in matching(rs, scope, RestrictionKind.BLOCK_OPEN):
        raise _Reject(4, codes.REJECT_PRODUCT_BLOCKED, {"source": r.source, "scope_key": r.scope_key})
    factors = matching(rs, scope, RestrictionKind.ORDER_SIZE_FACTOR)
    if factors:
        r = min(factors, key=lambda x: x.value if x.value is not None else 1.0)
        factor = r.value if r.value is not None else 1.0
        cap = int(math.floor(_max_qty(host, req) * factor + 1e-9))
        if req.qty > cap:
            raise _Reject(4, codes.REJECT_ORDER_SIZE_RESTRICTED, {"qty": req.qty, "max": cap, "factor": factor, "source": r.source})
    side = pos_direction(req.direction, Offset.OPEN)
    losers = matching(rs, scope, RestrictionKind.NO_ADD_TO_LOSERS)
    if losers:
        pnl = val.product_pnl(scope.product, side)
        if pnl < 0:
            raise _Reject(4, codes.REJECT_ADD_TO_LOSER, {"product": scope.product, "direction": side.value,
                                                         "position_pnl": pnl, "source": losers[0].source})
    caps = matching(rs, scope, RestrictionKind.EXPOSURE_CAP_FACTOR)
    if caps:
        r = min(caps, key=lambda x: x.value if x.value is not None else 1.0)
        factor = r.value if r.value is not None else 1.0
        ev = val.exposures.get(scope.product)
        lp = host.static_info(req.instrument_id).limit_pct
        cap_pct = (0.03 / lp) * factor if lp > 0 else None
        mark = host.mark_of(req.instrument_id) or (req.price or 0.0)
        add = (1 if side == PosDirection.LONG else -1) * mark * host.registry.multiplier(req.instrument_id) * req.qty
        net_before = ev.net_notional if ev is not None else 0.0
        net_after = net_before + add
        equity = val.funds.equity
        if cap_pct is not None and abs(net_after) > abs(net_before):
            after_pct = abs(net_after) / equity if equity > 0 else float("inf")
            if after_pct > cap_pct + 1e-12:
                raise _Reject(4, codes.REJECT_EXPOSURE_CAP, {"product": scope.product, "after_pct": after_pct,
                                                             "cap_pct": cap_pct, "factor": factor, "source": r.source})


# ------------------------------------------------------------------ step 5
def _throttle(
    host: PretradeHost, account: RiskAccount, req: OrderRequest, is_open: bool, mono_ns: int, resting: list[OrderRecord]
) -> None:
    if not account.bucket.take(mono_ns):
        raise _Reject(5, codes.REJECT_THROTTLED, {"rate_per_s": account.bucket.rate_per_s, "burst": account.bucket.burst})
    if is_open:
        limit = host.registry.product_spec(req.instrument_id).open_limit_per_day
        if limit:
            done = account.open_volume.get(req.instrument_id, 0)
            inflight = sum(o.remaining for o in resting if o.is_open)
            if done + inflight + req.qty > limit:
                raise _Reject(5, codes.REJECT_OPEN_VOLUME_LIMIT, {"opened": done, "in_flight": inflight, "qty": req.qty, "limit": limit})


# ------------------------------------------------------------------ step 6
def _position(
    host: PretradeHost, account: RiskAccount, req: OrderRequest, offset: Offset, is_open: bool, resting: list[OrderRecord]
) -> None:
    book = account.ledger.book
    if not is_open:
        code = book.check_close(account.account_id, req.instrument_id, req.direction, offset, req.qty)
        if code is not None:
            side = pos_direction(req.direction, offset)
            pos = book.find(account.account_id, req.instrument_id, side)
            c = pos.closable() if pos is not None else None
            raise _Reject(6, code, {"qty": req.qty, "closable": c.total if c else 0, "closable_today": c.today if c else 0,
                                    "closable_yd": c.yd if c else 0})
        return
    limit = host.registry.product_spec(req.instrument_id).position_limit
    if limit:
        side = pos_direction(req.direction, Offset.OPEN)
        pos = book.find(account.account_id, req.instrument_id, side)
        held = pos.qty if pos is not None else 0
        inflight = sum(o.remaining for o in resting if o.is_open and pos_direction(o.direction, Offset.OPEN) == side)
        if held + inflight + req.qty > limit:
            raise _Reject(6, codes.REJECT_POSITION_LIMIT, {"held": held, "in_flight": inflight, "qty": req.qty, "limit": limit})


# ------------------------------------------------------------------ step 7
def order_price(host: PretradeHost, req: OrderRequest) -> float:
    """Price used for margin / fee of an order: the limit price, or the mark for MARKET orders."""
    if req.order_type == OrderType.LIMIT and req.price is not None:
        return req.price
    mark = host.mark_of(req.instrument_id)
    if mark is not None:
        return mark
    limits = host.limits_of(req.instrument_id)
    if limits is not None:
        return limits[0] if req.direction == Direction.BUY else limits[1]
    return req.price or 0.0


def resting_orders(host: PretradeHost, account_id: str) -> list[RestingOrder]:
    """Unfilled remainders of the account's working orders (frozen margin / fee inputs)."""
    out: list[RestingOrder] = []
    for o in host.orders.resting(account_id):
        if o.remaining <= 0 or not o.holds_frozen:
            continue
        price = o.price if o.price is not None else (host.mark_of(o.instrument_id) or 0.0)
        out.append(RestingOrder(o.instrument_id, o.direction, o.offset, o.remaining, price))
    return out


def _funds(host: PretradeHost, account: RiskAccount, req: OrderRequest, val: AccountValuation) -> tuple[float, float]:
    """Incremental (big-side aware) margin + fee of this order vs available × (1 − buffer).

    The resting orders of the account are registered in the ledger, so
    ``available`` already nets their frozen margin / fee and
    :meth:`IntradayLedger.order_cost` prices the new order on top of positions
    and resting orders (02 §8.3). ``available`` is read from the valuation:
    the engine evaluates a dirty account before the pipeline and keeps the
    valuation's funds in step with every order change.
    """
    price = order_price(host, req)
    margin, fee = account.ledger.order_cost(RestingOrder(req.instrument_id, req.direction, Offset.OPEN, req.qty, price))
    margin = max(0.0, margin)
    required = margin + fee
    available = val.funds.available
    budget = available * (1.0 - host.pretrade_cfg.funds_buffer)
    if required > budget + 1e-9:
        raise _Reject(7, codes.REJECT_INSUFFICIENT_FUNDS, {"required": round(required, 2), "available": round(available, 2),
                                                           "buffer": host.pretrade_cfg.funds_buffer})
    return margin, fee


# ------------------------------------------------------------------ step 8
def order_delta(host: PretradeHost, req: OrderRequest, margin: float, fee: float) -> OrderDelta:
    info = host.static_info(req.instrument_id)
    price = order_price(host, req)
    mark = host.mark_of(req.instrument_id) or price
    sign = 1 if req.direction == Direction.BUY else -1
    notional = mark * info.multiplier * req.qty
    return OrderDelta(
        instrument_id=req.instrument_id,
        product=info.product,
        sector=info.sector,
        exchange=info.exchange,
        qty=req.qty,
        signed_notional=sign * notional,
        pnl_delta=(mark - price) * info.multiplier * req.qty * sign,
        fee=fee,
        margin_delta=margin,
        limit_pct=info.limit_pct,
    )


def _scope_key_for(rule: CompiledRule, scope: OrderScope) -> tuple[str, str | None]:
    if rule.scope == "account/product":
        return product_scope(scope.account_id, scope.product), scope.product
    if rule.scope == "account/sector":
        return sector_scope(scope.account_id, scope.sector), scope.sector
    if rule.scope == "account/instrument":
        return instrument_scope(scope.account_id, scope.instrument_id), scope.instrument_id
    return acct_scope(scope.account_id), None


def _whatif(
    host: PretradeHost,
    account: RiskAccount,
    req: OrderRequest,
    scope: OrderScope,
    val: AccountValuation,
    margin: float,
    fee: float,
) -> None:
    rules = host.pretrade_rules()
    if not rules:
        return
    # 预估一律按最后已知的标记价计算，不看行情质量：它只会多拒单，不会放行（fail-safe）
    after = WhatIf(val, order_delta(host, req, margin, fee))
    for rule in rules:
        blocking = [lv for lv in rule.levels_for(account.account_id) if lv.blocks_open]
        if not blocking:
            continue
        scope_key, key = _scope_key_for(rule, scope)
        if rule.min_gross_pct is not None and after.gross_pct() < rule.min_gross_pct:
            continue  # 下单后仍低于 min_gross_pct：该规则不适用
        before_v = rule_value(rule, val, key)
        after_v = after.value(rule.metric, key)
        if after_v is None:
            continue
        for lv in blocking:
            if not rule.breached(after_v, lv.threshold):
                continue
            if before_v is not None and rule.breached(before_v, lv.threshold) and not rule.worse(after_v, before_v):
                continue
            raise _Reject(8, codes.REJECT_WHATIF_BREACH, {"rule_id": rule.rule_id, "level": lv.name, "scope_key": scope_key,
                                                          "metric": rule.metric, "value": after_v, "current": before_v,
                                                          "threshold": lv.threshold})


# ------------------------------------------------------------------ step 9
def _self_trade(req: OrderRequest, resting: list[OrderRecord]) -> None:
    new_px = req.price if (req.order_type == OrderType.LIMIT and req.price is not None) else None
    for o in resting:
        if o.direction == req.direction or o.remaining <= 0:
            continue
        other = o.price if o.order_type == OrderType.LIMIT else None
        if new_px is None or other is None:
            crosses = True
        elif req.direction == Direction.BUY:
            crosses = new_px >= other - 1e-9
        else:
            crosses = new_px <= other + 1e-9
        if crosses:
            raise _Reject(9, codes.REJECT_SELF_TRADE, {"resting_order": o.client_order_id, "resting_price": other,
                                                       "price": new_px})


__all__ = [
    "PretradeHost",
    "PretradeResult",
    "check_order",
    "order_delta",
    "order_price",
    "resting_orders",
]
