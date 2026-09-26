"""Settlement price determination (01 §8.15 结算价, 02 §7.1 SettleInput, §7.8 SettlementPriceView).

Priority (03 §4.4 ``official_prices``): **MANUAL > OFFICIAL > VWAP > fallback
chain** of the exchange (``ExchangeRules.settle_fallback``), with PRE_SETTLE as
the final safety net even if the configured chain omits it.

- VWAP = turnover / (volume × multiplier); ``VWAP_FULL_DAY`` (SHFE/DCE, day
  incl. night session) uses ``day_volume``/``day_turnover``, ``VWAP_LAST_HOUR``
  (CFFEX: index 14:00–15:00, T 14:15–15:15) uses ``window_volume``/``window_turnover``;
- rounding per exchange (``settle_rounding``): SHFE ``FLOOR_TICK``, otherwise
  ``ROUND_TICK`` (01 §8.3). MANUAL and OFFICIAL prices are taken as given
  (official delivery settlement prices may be off the tick grid).

Fallback steps (only when the VWAP has no volume):

- ``MEDIAN`` (SHFE/DCE): median of bid1, ask1 and pre-settle; needs both quotes;
- ``LIMIT`` (SHFE/DCE): the limit price when the contract closed limit-locked;
- ``HOURLY`` (CFFEX 结算细则): first the first-hour rule — when the day's last
  trade happened less than one trading hour after the open, the whole-day VWAP
  is used (source ``FALLBACK_FULLDAY``); otherwise walk back one **trading hour**
  at a time and take the first earlier hour with trades (``FALLBACK_HOURLY``).
  Trading hours are 60 trading minutes counted back from the close across
  session breaks (:func:`trading_hour_windows`): IF [14:00,15:00), [13:00,14:00),
  [10:30,11:30), [09:30,10:30); T [14:15,15:15), [13:15,14:15), then
  [10:45,11:30)+[13:00,13:15) spanning the lunch break, ...;
- ``FULLDAY`` (CFFEX): the whole-day VWAP (e.g. when no hourly data exists);
- ``BENCHMARK`` (CFFEX): pre-settle + (benchmark settle − benchmark pre-settle),
  clamped into the limit band;
- ``PRE_SETTLE``: the previous settlement price.

Input granularity: ``SettleInput.hourly`` (03 §4.1, ``{start_ns: [volume,
turnover]}``) should hold the buckets of :func:`trading_hour_segments` (the
trading-hour windows, additionally split where the first trading hour ends), as
:func:`build_settle_input` produces; each bucket is assigned to the window that
contains its start. Minute buckets work too. Coarser clock-hour buckets straddle
the :15/:30 window boundaries and make the walk-back and the first-hour test
approximate (RK-04). A known last-trade time (``last_trade_ns``) makes the
first-hour test exact regardless of the buckets.

``BENCHMARK`` needs the caller to supply the benchmark contract's move: per the
CFFEX 结算细则 art. 43 the benchmark is the contract of the same product that
traded today and is nearest to delivery (clearing picks it in
``services/clearing/prices.py`` ``_benchmark_for``); without it the step is
skipped. Official prices override every fallback.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, localcontext
from typing import Any

from futrisk.common.decimals import D
from futrisk.core._num import CLEARING_CTX, PRICE_QUANTUM, div, price_str
from futrisk.core.instruments import InstrumentRegistry
from futrisk.messages.enums import LimitLock, Rounding, SettleInputQuality, SettleMethod, SettlePriceSource
from futrisk.messages.eod import SettlementPriceView
from futrisk.messages.md import SettleInput

NS_PER_HOUR = 3_600 * 1_000_000_000
NS_PER_MINUTE = 60 * 1_000_000_000

FALLBACK_SOURCES: dict[str, SettlePriceSource] = {
    "HOURLY": SettlePriceSource.FALLBACK_HOURLY,
    "FULLDAY": SettlePriceSource.FALLBACK_FULLDAY,
    "MEDIAN": SettlePriceSource.FALLBACK_MEDIAN,
    "LIMIT": SettlePriceSource.FALLBACK_LIMIT,
    "BENCHMARK": SettlePriceSource.FALLBACK_BENCHMARK,
    "PRE_SETTLE": SettlePriceSource.PRE_SETTLE,
}

Interval = tuple[int, int]


@dataclass(frozen=True, slots=True)
class ManualPrice:
    """A ``settle.override_price`` value (03 §4.4 ``price_overrides``)."""

    price: Decimal
    operator: str
    reason: str


@dataclass(frozen=True, slots=True)
class BenchmarkMove:
    """Settlement move of the benchmark contract, for the CFFEX no-trade fallback."""

    settle: Decimal
    pre_settle: Decimal


@dataclass(frozen=True, slots=True)
class HourlyBucket:
    """Trades of one bucket starting at ``start_ns``; ``turnover`` in yuan (price × qty × multiplier)."""

    start_ns: int
    volume: int
    turnover: Decimal


def vwap(turnover: Decimal, volume: int, multiplier: int) -> Decimal | None:
    """Raw VWAP = turnover / (volume × multiplier); None without volume."""
    if volume <= 0:
        return None
    return div(turnover, Decimal(volume * multiplier))


def _median3(a: Decimal, b: Decimal, c: Decimal) -> Decimal:
    return sorted((a, b, c))[1]


def _coerce_hourly(raw: Any) -> list[HourlyBucket]:
    """Accept ``HourlyBucket``s, ``(start_ns, volume, turnover)`` tuples or the 03 §4.1 JSON mapping."""
    if raw is None:
        return []
    out: list[HourlyBucket] = []
    if isinstance(raw, Mapping):
        for k, v in raw.items():
            vol, turn = v[0], v[1]
            out.append(HourlyBucket(int(k), int(vol), D(turn)))
        return out
    for item in raw:
        if isinstance(item, HourlyBucket):
            out.append(item)
        else:
            start, vol, turn = item
            out.append(HourlyBucket(int(start), int(vol), D(turn)))
    return out


# ------------------------------------------------------------ trading hours
def trading_hour_windows(
    registry: InstrumentRegistry, instrument_id: str, trading_day: str, *, minutes: int = 60
) -> list[list[Interval]]:
    """Consecutive windows of ``minutes`` trading minutes counted back from the close.

    ``[0]`` is the last trading hour (the ``VWAP_LAST_HOUR`` window), ``[1]`` the
    hour before it, and so on; a window that spans a session break is a list of
    several intervals, and the earliest window may be shorter. Built from the
    product's session templates (:meth:`InstrumentRegistry.session_intervals`).
    """
    step = minutes * NS_PER_MINUTE
    windows: list[list[Interval]] = []
    cur: list[Interval] = []
    need = step
    for start, end in reversed(registry.session_intervals(instrument_id, trading_day)):
        hi = end
        while hi > start:
            take = min(need, hi - start)
            cur.append((hi - take, hi))
            hi -= take
            need -= take
            if need == 0:
                windows.append(sorted(cur))
                cur = []
                need = step
    if cur:
        windows.append(sorted(cur))
    return windows


def first_hour_end_ns(registry: InstrumentRegistry, instrument_id: str, trading_day: str, *, minutes: int = 60) -> int | None:
    """End of the first ``minutes`` trading minutes after the open (CFFEX first-hour rule)."""
    need = minutes * NS_PER_MINUTE
    for start, end in registry.session_intervals(instrument_id, trading_day):
        if end - start >= need:
            return start + need
        need -= end - start
    return None


def trading_hour_segments(registry: InstrumentRegistry, instrument_id: str, trading_day: str) -> list[Interval]:
    """Bucket boundaries for ``SettleInput.hourly``: trading-hour windows split at the first-hour end.

    Every segment lies inside exactly one trading-hour window and entirely before
    or after the end of the first trading hour, so buckets keyed by segment start
    make both the hourly walk-back and the first-hour rule exact.
    """
    fh = first_hour_end_ns(registry, instrument_id, trading_day)
    segs: list[Interval] = []
    for window in trading_hour_windows(registry, instrument_id, trading_day):
        for start, end in window:
            if fh is not None and start < fh < end:
                segs.extend([(start, fh), (fh, end)])
            else:
                segs.append((start, end))
    return sorted(segs)


def _locate(intervals: Sequence[Interval], t: int) -> int:
    """Index of the first interval ending after ``t``.

    That is the interval containing ``t``, or for a time in a session break (or
    before the open) the next interval; times at or after the close clamp to the
    last interval (a trade stamped exactly at 15:00:00 belongs to the last hour).
    """
    for i, (_start, end) in enumerate(intervals):
        if t < end:
            return i
    return len(intervals) - 1


def _window_of(windows: list[list[Interval]], t: int) -> int:
    """Index of the trading-hour window that contains time ``t`` (see :func:`_locate`)."""
    flat = sorted((iv, idx) for idx, w in enumerate(windows) for iv in w)
    pos = _locate([iv for iv, _ in flat], t)
    return flat[pos][1]


def trading_hour_buckets(
    registry: InstrumentRegistry,
    instrument_id: str,
    trading_day: str,
    trades: Iterable[tuple[int, Decimal | float | str, int]],
) -> list[HourlyBucket]:
    """Aggregate a trade tape ``(ts_ns, price, qty)`` into :func:`trading_hour_segments` buckets.

    Trades before the first segment (opening auction) count in the first one;
    trades at or after the close count in the last one; turnover is in yuan.
    """
    mult = registry.multiplier(instrument_id)
    segs = trading_hour_segments(registry, instrument_id, trading_day)
    if not segs:
        return []
    acc: dict[int, tuple[int, Decimal]] = {}
    for ts, price, qty in trades:
        start = segs[_locate(segs, ts)][0]
        vol, turn = acc.get(start, (0, Decimal(0)))
        acc[start] = (vol + qty, turn + D(price) * qty * mult)
    return [HourlyBucket(k, v[0], v[1]) for k, v in sorted(acc.items())]


# ------------------------------------------------------------------- decide
def determine_settlement_price(
    inp: SettleInput | None,
    registry: InstrumentRegistry,
    *,
    instrument_id: str | None = None,
    trading_day: str | None = None,
    pre_settle: Decimal | None = None,
    manual: ManualPrice | None = None,
    official: Decimal | None = None,
    hourly: Iterable[HourlyBucket] | Iterable[tuple[int, int, Any]] | Mapping[Any, Sequence[Any]] | None = None,
    benchmark: BenchmarkMove | None = None,
    version: int | None = None,
    last_trade_ns: int | None = None,
) -> SettlementPriceView:
    """Decide one contract's settlement price and describe how it was obtained.

    ``inp`` is the md-gateway ``SettleInput``; it may be None when only a manual
    or official price exists (then ``instrument_id``, ``trading_day`` and
    ``pre_settle`` must be given). ``hourly`` overrides ``inp.hourly``;
    ``last_trade_ns`` (time of the day's last trade) makes the CFFEX first-hour
    rule exact. ``version`` defaults to the input's version (settlement callers
    re-stamp it with the settlement version).
    """
    with localcontext(CLEARING_CTX):
        return _determine(
            inp, registry, instrument_id, trading_day, pre_settle, manual, official, hourly, benchmark, version, last_trade_ns
        )


def _determine(
    inp: SettleInput | None,
    registry: InstrumentRegistry,
    instrument_id: str | None,
    trading_day: str | None,
    pre_settle: Decimal | None,
    manual: ManualPrice | None,
    official: Decimal | None,
    hourly: Any,
    benchmark: BenchmarkMove | None,
    version: int | None,
    last_trade_ns: int | None,
) -> SettlementPriceView:
    inst = instrument_id or (inp.instrument_id if inp else None)
    td = trading_day or (inp.trading_day if inp else None)
    if inst is None or td is None:
        raise ValueError("instrument_id and trading_day are required without a SettleInput")
    info = registry.get(inst)
    pre = pre_settle if pre_settle is not None else (D(inp.pre_settle) if inp else None)
    method = inp.method if inp else info.spec.settle_method
    rounding = registry.settle_rounding(inst)
    quality = inp.quality if inp else None
    ver = version if version is not None else (inp.version if inp else 1)
    mult = inp.multiplier if inp else info.multiplier

    raw = _raw_vwap(inp, method, mult) if inp else None

    def view(
        price: Decimal,
        source: SettlePriceSource,
        rnd: Rounding,
        raw_used: Decimal | None,
        operator: str | None = None,
        reason: str | None = None,
    ) -> SettlementPriceView:
        return SettlementPriceView(
            trading_day=td,
            instrument_id=inst,
            price=price_str(price),
            source=source,
            method=method,
            vwap_raw=price_str(raw_used.quantize(PRICE_QUANTUM)) if raw_used is not None else None,
            rounding=rnd,
            input_quality=quality,
            version=ver,
            operator=operator,
            reason=reason,
        )

    # 1. 人工覆盖
    if manual is not None:
        if not registry.on_tick(inst, manual.price):
            raise ValueError(f"manual settlement price {manual.price} of {inst} is not on the tick grid")
        return view(manual.price, SettlePriceSource.MANUAL, Rounding.NONE, raw, manual.operator, manual.reason)
    # 2. 官方结算价
    if official is not None:
        return view(D(official), SettlePriceSource.OFFICIAL, Rounding.NONE, raw)
    # 3. VWAP（按交易所取整）
    if raw is not None:
        return view(registry.round_price(inst, raw, rounding), SettlePriceSource.VWAP, rounding, raw)
    if pre is None:
        raise ValueError(f"{inst}: no trades and no pre_settle to fall back on")
    # 4. 回退链（01 §8.15）
    chain = list(registry.settle_fallback(inst))
    if "PRE_SETTLE" not in chain:
        chain.append("PRE_SETTLE")
    buckets = _coerce_hourly(hourly if hourly is not None else getattr(inp, "hourly", None))
    last_ts = last_trade_ns if last_trade_ns is not None else getattr(inp, "last_trade_ns", None)
    for step in chain:
        if step not in FALLBACK_SOURCES:
            raise ValueError(f"unknown settle_fallback step {step!r} for {inst}")
        source = FALLBACK_SOURCES[step]
        if step == "HOURLY":
            found = _hourly_fallback(inp, registry, inst, td, buckets, last_ts, mult) if inp else None
            if found is not None:
                hour_raw, hour_source = found
                return view(registry.round_price(inst, hour_raw, rounding), hour_source, rounding, hour_raw)
        elif step == "FULLDAY":
            day_raw = vwap(D(inp.day_turnover), inp.day_volume, mult) if inp else None
            if day_raw is not None:
                return view(registry.round_price(inst, day_raw, rounding), source, rounding, day_raw)
        elif step == "MEDIAN":
            if inp and inp.bid1 is not None and inp.ask1 is not None:
                med = _median3(D(inp.bid1), D(inp.ask1), pre)
                return view(registry.round_price(inst, med, rounding), source, rounding, None)
        elif step == "LIMIT":
            lock = inp.limit_lock if inp else LimitLock.NONE
            if lock != LimitLock.NONE:
                upper, lower = registry.limit_prices(inst, pre, td)
                return view(upper if lock == LimitLock.UP else lower, source, rounding, None)
        elif step == "BENCHMARK":
            if benchmark is not None:
                moved = pre + (benchmark.settle - benchmark.pre_settle)
                upper, lower = registry.limit_prices(inst, pre, td)
                moved = min(max(moved, lower), upper)
                return view(registry.round_price(inst, moved, rounding), source, rounding, None)
        else:  # PRE_SETTLE
            return view(pre, source, rounding, None)
    raise AssertionError("unreachable: PRE_SETTLE always ends the chain")  # pragma: no cover


def _raw_vwap(inp: SettleInput, method: SettleMethod, mult: int) -> Decimal | None:
    if method == SettleMethod.VWAP_LAST_HOUR:
        if inp.window_volume is not None and inp.window_turnover is not None:
            return vwap(D(inp.window_turnover), inp.window_volume, mult)
        return D(inp.vwap_raw) if inp.vwap_raw is not None else None
    if inp.day_volume > 0:
        return vwap(D(inp.day_turnover), inp.day_volume, mult)
    return D(inp.vwap_raw) if inp.vwap_raw is not None else None


def _hourly_fallback(
    inp: SettleInput,
    registry: InstrumentRegistry,
    inst: str,
    td: str,
    buckets: list[HourlyBucket],
    last_ts: int | None,
    mult: int,
) -> tuple[Decimal, SettlePriceSource] | None:
    """CFFEX: first-hour rule, then walk back trading hour by trading hour."""
    windows = trading_hour_windows(registry, inst, td)
    if not windows:
        return None
    fh_end = first_hour_end_ns(registry, inst, td)
    if inp.day_volume > 0 and fh_end is not None:
        in_first_hour: bool | None = None
        if last_ts is not None:
            in_first_hour = last_ts < fh_end
        else:
            traded = [b.start_ns for b in buckets if b.volume > 0]
            if traded:
                segs = trading_hour_segments(registry, inst, td)
                in_first_hour = segs[_locate(segs, max(traded))][1] <= fh_end
        if in_first_hour:
            # 最后一笔成交距开盘不足一小时：取全天成交量加权平均价
            day_raw = vwap(D(inp.day_turnover), inp.day_volume, mult)
            if day_raw is not None:
                return day_raw, SettlePriceSource.FALLBACK_FULLDAY
    if not buckets:
        return None
    per_window: dict[int, tuple[int, Decimal]] = {}
    for b in buckets:
        idx = _window_of(windows, b.start_ns)
        vol, turn = per_window.get(idx, (0, Decimal(0)))
        per_window[idx] = (vol + b.volume, turn + b.turnover)
    # 最后一小时（窗口 0）无成交才会走到这里：从前一小时起逐小时向前回溯
    for idx in range(1, len(windows)):
        vol, turn = per_window.get(idx, (0, Decimal(0)))
        v = vwap(turn, vol, mult)
        if v is not None:
            return v, SettlePriceSource.FALLBACK_HOURLY
    return None


# ---------------------------------------------------------------- builders
def vwap_from_trades(
    trades: Iterable[tuple[int, Decimal | float | str, int]],
    *,
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> tuple[int, Decimal, Decimal | None]:
    """``(volume, Σ price × qty, vwap)`` over trades with ``start_ns <= ts <= end_ns``.

    ``trades`` are ``(ts_ns, price, qty)``; the turnover here is **not**
    multiplied by the contract multiplier.
    """
    vol = 0
    turn = Decimal(0)
    for ts, price, qty in trades:
        if start_ns is not None and ts < start_ns:
            continue
        if end_ns is not None and ts > end_ns:
            continue
        vol += qty
        turn += D(price) * qty
    return vol, turn, (div(turn, Decimal(vol)) if vol else None)


def hourly_buckets(trades: Iterable[tuple[int, Decimal | float | str, int]], multiplier: int) -> list[HourlyBucket]:
    """Clock-hour buckets of a trade tape (Beijing is UTC+8, so UTC hours align).

    Coarser than :func:`trading_hour_buckets`; see the module docstring.
    """
    acc: dict[int, tuple[int, Decimal]] = {}
    for ts, price, qty in trades:
        start = ts - ts % NS_PER_HOUR
        vol, turn = acc.get(start, (0, Decimal(0)))
        acc[start] = (vol + qty, turn + D(price) * qty * multiplier)
    return [HourlyBucket(k, v[0], v[1]) for k, v in sorted(acc.items())]


def build_settle_input(
    registry: InstrumentRegistry,
    instrument_id: str,
    trading_day: str,
    trades: Iterable[tuple[int, Decimal | float | str, int]],
    *,
    pre_settle: Decimal | float | str,
    last: Decimal | float | str | None = None,
    bid1: Decimal | float | str | None = None,
    ask1: Decimal | float | str | None = None,
    limit_lock: LimitLock = LimitLock.NONE,
    version: int = 1,
    computed_ns: int = 0,
) -> SettleInput:
    """Build a ``SettleInput`` from a trade tape ``(ts_ns, price, qty)`` (tests, backtester).

    The last-hour window comes from the product's ``settle_window`` on
    ``trading_day``; turnovers are in yuan (× multiplier) as in 02 §7.1. For
    ``VWAP_LAST_HOUR`` products ``hourly`` holds :func:`trading_hour_buckets`.
    """
    with localcontext(CLEARING_CTX):
        tape = [(ts, D(p), q) for ts, p, q in trades]
        info = registry.get(instrument_id)
        mult = info.multiplier
        day_vol, day_turn, day_vwap = vwap_from_trades(tape)
        window = registry.settle_window_ns(instrument_id, trading_day)
        w_start: int | None = None
        w_vol: int | None = None
        w_turn: str | None = None
        hourly: dict[str, list[str]] | None = None
        raw = day_vwap
        if window is not None:
            w_start = window[0]
            wv, wt, w_vwap = vwap_from_trades(tape, start_ns=window[0], end_ns=window[1])
            w_vol, w_turn, raw = wv, price_str(wt * mult), w_vwap
            buckets = trading_hour_buckets(registry, instrument_id, trading_day, tape)
            hourly = {str(b.start_ns): [str(b.volume), price_str(b.turnover)] for b in buckets} or None
        last_px = D(last) if last is not None else (max(tape, key=lambda t: t[0])[1] if tape else None)
        return SettleInput(
            trading_day=trading_day,
            instrument_id=instrument_id,
            exchange=info.exchange,
            method=info.spec.settle_method,
            multiplier=mult,
            tick_size=price_str(info.tick_size),
            day_volume=day_vol,
            day_turnover=price_str(day_turn * mult),
            window_start_ns=w_start,
            window_volume=w_vol,
            window_turnover=w_turn,
            hourly=hourly,
            vwap_raw=price_str(raw.quantize(PRICE_QUANTUM)) if raw is not None else None,
            last=price_str(last_px) if last_px is not None else None,
            bid1=price_str(D(bid1)) if bid1 is not None else None,
            ask1=price_str(D(ask1)) if ask1 is not None else None,
            limit_lock=limit_lock,
            pre_settle=price_str(D(pre_settle)),
            quality=SettleInputQuality.OK if day_vol > 0 else SettleInputQuality.NO_TRADE,
            version=version,
            computed_ns=computed_ns,
        )


__all__ = [
    "FALLBACK_SOURCES",
    "BenchmarkMove",
    "HourlyBucket",
    "ManualPrice",
    "build_settle_input",
    "determine_settlement_price",
    "first_hour_end_ns",
    "hourly_buckets",
    "trading_hour_buckets",
    "trading_hour_segments",
    "trading_hour_windows",
    "vwap",
    "vwap_from_trades",
]
