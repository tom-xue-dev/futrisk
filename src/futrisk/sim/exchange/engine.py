"""Correlated price engine of the virtual exchange (01 §8.13; the feeds are 02 §9.1 / §9.2).

Model, evaluated once per ``tick_interval_ms`` (500ms) and only for instruments whose
product is in a *continuous* session at that market time (calendar + market clock,
01 §8.2; auctions and breaks produce nothing):

- a latent log price follows GBM plus Poisson jumps. The diffusion shocks of all
  instruments are correlated through the Cholesky factor of a block matrix:
  ``market_correlation`` across sectors, ``sector_correlation`` inside a sector and
  :data:`SAME_PRODUCT_CORRELATION` between contract months of one product;
- the displayed last price is the latent price rounded to the tick grid and clamped to
  the daily limits (``pre_settle * (1 ± pct)`` rounded inward, 02 §7.1, with the
  percentage in force on that trading day: ``ProductSpec.limit_pct_on``). While the
  latent price sits beyond a limit the book is one-sided (limit lock: no offers at
  limit-up, no bids at limit-down);
- a contract rolls to a new trading day (new ``pre_settle`` = the sim's settlement of the
  old day, new limits, cumulative values reset) as soon as the first window of that day
  opens - the opening auction included - so orders sent during the auction see the new
  band; the first continuous step publishes the new day;
- cumulative volume, turnover (multiplier included) and open interest only move with
  trades, and the last price only changes together with a volume increase: this is the
  md-gateway validator rule "价格只能随成交变动" (01 §8.13 坏点防线 1,
  02 §11.3 ``MD_REJECT_PRICE_WITHOUT_TRADE``);
- randomness is counter based: step *k* of trading day *D* draws every variate of every
  instrument from ``default_rng([seed, D, k])``, so a path is a pure function of the seed
  and of the steps taken, and a restarted process continues the same stream;
- control-API price shocks (02 §9.5 ``POST /control/shock``) shift the latent log price
  by ``log(1 + pct)`` at once (``STEP``) or spread over ``duration_s`` (``LINEAR``).

Prices are integer tick counts internally so every published price is exactly on the
grid; turnover is kept as ``Σ qty × price_ticks`` and scaled by ``tick × multiplier``.
"""

from __future__ import annotations

import contextlib
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, time
from decimal import ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Any

import numpy as np
from loguru import logger

from futrisk.common.calendar import Phase, TradingCalendar
from futrisk.common.clock import BJ, NS_PER_MS, bj_to_ns, parse_td, td_str
from futrisk.common.config import FullConfig, ProductSpec, product_code
from futrisk.common.decimals import limit_prices, round_to_tick
from futrisk.messages.enums import LimitLock, Rounding, SettleMethod

#: correlation between two contract months of the same product (e.g. rb2701/rb2705).
SAME_PRODUCT_CORRELATION = 0.95
#: annualisation: 252 trading days of 4 trading hours.
TRADING_SECONDS_PER_DAY = 4 * 3600
TRADING_DAYS_PER_YEAR = 252
#: the latent price may wander this far beyond a limit, so a lock can persist and release.
LOCK_MARGIN = 0.003
#: trade rate while limit-locked, relative to the normal rate.
LOCKED_VOLUME_FACTOR = 0.1
#: mean level-1 size = max(2, volume_per_s * BOOK_SIZE_FACTOR); the locked side is much deeper.
BOOK_SIZE_FACTOR = 2.0
LOCKED_BOOK_FACTOR = 25
#: open interest moves by at most ±30% of the lots traded in a step.
OI_CHANGE_FACTOR = 0.6

SHAPES = ("STEP", "LINEAR")


# ---------------------------------------------------------------------------
# Static instrument data
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InstrumentMeta:
    """Static data of one simulated contract (products.yaml + sim.yaml)."""

    index: int
    instrument_id: str
    exchange: str
    product: str
    sector: str
    multiplier: int
    tick: Decimal
    limit_pct: Decimal
    sessions: tuple[str, ...]
    settle_method: SettleMethod
    settle_window_start: time | None
    settle_rounding: Rounding
    explicit_close_today: bool
    close_style: str
    max_limit_qty: int
    max_market_qty: int
    vol_annual: float
    jump_intensity_per_day: float
    jump_sigma: float
    volume_per_s: float
    init_pre_settle: Decimal
    init_open_interest: int
    spec: ProductSpec | None = field(default=None, compare=False, repr=False)

    def limit_pct_on(self, trading_day: str | None) -> Decimal:
        """Limit percentage in force on ``trading_day`` (``ProductSpec.limit_pct_on``: the
        date-effective ``limit_schedule``, e.g. DCE widening limits before a long holiday)."""
        return self.spec.limit_pct_on(trading_day) if self.spec is not None else self.limit_pct

    @property
    def tick_value(self) -> Decimal:
        """Money value of one tick for one lot (``tick × multiplier``)."""
        return self.tick * self.multiplier

    def price(self, ticks: int) -> Decimal:
        """Exact price of a tick count, e.g. ``42501 -> Decimal('850.02')`` for au."""
        return Decimal(ticks) * self.tick

    def to_ticks(self, price: Decimal | float | str) -> int | None:
        """Tick count of a price, ``None`` when it is off the grid."""
        q = Decimal(str(price)) / self.tick
        if q != q.to_integral_value():
            return None
        return int(q)


# ---------------------------------------------------------------------------
# Mutable state
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class InstrumentState:
    """Market state of one contract: an L1 snapshot plus the day's cumulative values."""

    meta: InstrumentMeta
    trading_day: str
    pre_settle: Decimal
    pre_close: Decimal
    pre_open_interest: int
    upper: int
    lower: int
    last: int
    bid: int | None
    ask: int | None
    bid_vol: int
    ask_vol: int
    open: int | None
    high: int | None
    low: int | None
    volume: int
    turnover_ticks: int
    open_interest: int
    ts_ns: int
    lock: LimitLock
    x: float  # latent log price (in price units, not ticks)
    win_volume: int | None = None  # cumulative volume at the settlement-window start
    win_turnover_ticks: int | None = None
    updates: int = 0  # number of published states in this trading day

    @property
    def instrument_id(self) -> str:
        return self.meta.instrument_id

    @property
    def turnover(self) -> Decimal:
        """Cumulative turnover in yuan (multiplier included, 02 §7.1)."""
        return Decimal(self.turnover_ticks) * self.meta.tick_value

    def vwap(self) -> Decimal | None:
        """Day VWAP in price units (turnover ÷ volume ÷ multiplier); ``None`` without trades."""
        if self.volume <= 0:
            return None
        return Decimal(self.turnover_ticks) * self.meta.tick / self.volume

    def copy(self) -> InstrumentState:
        return replace(self)

    def to_dict(self) -> dict[str, Any]:
        """JSON-able form persisted in ``sim_md_state`` (tick size recorded for safety)."""
        return {
            "tick": str(self.meta.tick),
            "trading_day": self.trading_day,
            "pre_settle": str(self.pre_settle),
            "pre_close": str(self.pre_close),
            "pre_open_interest": self.pre_open_interest,
            "last": self.last,
            "bid": self.bid,
            "ask": self.ask,
            "bid_vol": self.bid_vol,
            "ask_vol": self.ask_vol,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "volume": self.volume,
            "turnover_ticks": self.turnover_ticks,
            "open_interest": self.open_interest,
            "ts_ns": self.ts_ns,
            "lock": self.lock.value,
            "x": self.x,
            "win_volume": self.win_volume,
            "win_turnover_ticks": self.win_turnover_ticks,
            "updates": self.updates,
        }

    @classmethod
    def from_dict(cls, meta: InstrumentMeta, d: Mapping[str, Any]) -> InstrumentState:
        """Rebuild from :meth:`to_dict`; limits are recomputed with the current config."""
        if Decimal(str(d["tick"])) != meta.tick:
            raise ValueError(f"tick size changed for {meta.instrument_id}")
        pre_settle = Decimal(str(d["pre_settle"]))
        upper, lower = _limit_ticks(meta, pre_settle, str(d["trading_day"]))

        def opt(key: str) -> int | None:
            v = d.get(key)
            return None if v is None else int(v)

        last = min(max(int(d["last"]), lower), upper)
        return cls(
            meta=meta,
            trading_day=str(d["trading_day"]),
            pre_settle=pre_settle,
            pre_close=Decimal(str(d["pre_close"])),
            pre_open_interest=int(d["pre_open_interest"]),
            upper=upper,
            lower=lower,
            last=last,
            bid=opt("bid"),
            ask=opt("ask"),
            bid_vol=int(d["bid_vol"]),
            ask_vol=int(d["ask_vol"]),
            open=opt("open"),
            high=opt("high"),
            low=opt("low"),
            volume=int(d["volume"]),
            turnover_ticks=int(d["turnover_ticks"]),
            open_interest=int(d["open_interest"]),
            ts_ns=int(d["ts_ns"]),
            lock=LimitLock(str(d.get("lock", "NONE"))),
            x=float(d["x"]),
            win_volume=opt("win_volume"),
            win_turnover_ticks=opt("win_turnover_ticks"),
            updates=int(d.get("updates", 0)),
        )


@dataclass(frozen=True, slots=True)
class Quote:
    """Immutable book snapshot handed to the SimBroker after every engine step."""

    instrument_id: str
    trading_day: str
    ts_ns: int
    last: int
    bid: int | None
    ask: int | None
    bid_vol: int
    ask_vol: int
    upper: int
    lower: int
    lock: LimitLock
    volume: int


@dataclass
class Shock:
    """A control-API price shock (02 §9.5 ``POST /control/shock``)."""

    shock_id: str
    targets: tuple[str, ...]
    pct: float
    shape: str
    duration_s: float
    n_steps: int
    per_step: float  # log-price increment per active step of each target
    remaining: dict[str, int]
    created_ns: int

    @property
    def done(self) -> bool:
        return not self.remaining

    def to_dict(self) -> dict[str, Any]:
        return {
            "shock_id": self.shock_id,
            "targets": list(self.targets),
            "pct": self.pct,
            "shape": self.shape,
            "duration_s": self.duration_s,
            "n_steps": self.n_steps,
            "remaining_steps": dict(self.remaining),
            "done": self.done,
            "created_ns": self.created_ns,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _limit_ticks(meta: InstrumentMeta, pre_settle: Decimal, trading_day: str) -> tuple[int, int]:
    """Limit prices of ``trading_day`` in ticks: ``pre_settle × (1 ± pct)`` rounded inward,
    with the percentage in force on that day (same rule as ``core.InstrumentRegistry.limit_prices``)."""
    upper, lower = limit_prices(pre_settle, meta.limit_pct_on(trading_day), meta.tick)
    return int(upper / meta.tick), int(lower / meta.tick)


def _parse_hhmm(text: str) -> time:
    hh, mm = text.strip().split(":")[:2]
    return time(int(hh), int(mm))


def correlation_matrix(metas: Sequence[InstrumentMeta], sector_corr: float, market_corr: float) -> np.ndarray:
    """Block correlation: same product > same sector > market (01 §8.13)."""
    n = len(metas)
    c = np.eye(n)
    for i, a in enumerate(metas):
        for j in range(i + 1, n):
            b = metas[j]
            if a.product == b.product:
                rho = max(SAME_PRODUCT_CORRELATION, sector_corr)
            elif a.sector == b.sector:
                rho = sector_corr
            else:
                rho = market_corr
            c[i, j] = c[j, i] = rho
    return c


def cholesky_psd(c: np.ndarray) -> np.ndarray:
    """Cholesky factor; falls back to the nearest positive-definite correlation matrix."""
    if c.size == 0:
        return c
    try:
        return np.linalg.cholesky(c)
    except np.linalg.LinAlgError:
        w, v = np.linalg.eigh(c)
        w = np.clip(w, 1e-8, None)
        c2 = v @ np.diag(w) @ v.T
        d = np.sqrt(np.diag(c2))
        c2 = c2 / np.outer(d, d)
        logger.warning("sim correlation matrix not positive definite; using nearest PD matrix")
        return np.linalg.cholesky(c2)


def build_metas(cfg: FullConfig) -> list[InstrumentMeta]:
    """Instruments of ``sim.yaml`` that ``products.yaml`` knows, in sim.yaml order."""
    out: list[InstrumentMeta] = []
    for iid, sim in cfg.sim.price_engine.instruments.items():
        try:
            spec = cfg.products.product_of(iid)
        except (KeyError, ValueError):
            logger.warning("sim instrument {} has no product in products.yaml; skipped", iid)
            continue
        rules = cfg.products.exchanges[spec.exchange]
        window = None
        if spec.settle_method == SettleMethod.VWAP_LAST_HOUR and spec.settle_window:
            window = _parse_hhmm(spec.settle_window.split("-")[0])
        inst = cfg.products.instrument(iid)
        out.append(
            InstrumentMeta(
                index=len(out),
                instrument_id=iid,
                exchange=spec.exchange.value,
                product=inst.product if inst is not None else product_code(iid),
                sector=spec.sector,
                multiplier=spec.multiplier,
                tick=spec.tick_size,
                limit_pct=spec.limit_pct,
                sessions=tuple(spec.sessions),
                settle_method=spec.settle_method,
                settle_window_start=window,
                settle_rounding=rules.settle_rounding,
                explicit_close_today=rules.explicit_close_today,
                close_style=rules.close_style,
                max_limit_qty=int(spec.max_order_qty.get("LIMIT", 1000)),
                max_market_qty=int(spec.max_order_qty.get("MARKET", 1000)),
                vol_annual=float(sim.vol_annual),
                jump_intensity_per_day=float(sim.jump_intensity_per_day),
                jump_sigma=float(sim.jump_sigma),
                volume_per_s=float(sim.volume_per_s),
                init_pre_settle=Decimal(sim.pre_settle),
                init_open_interest=int(sim.open_interest),
                spec=spec,
            )
        )
    return out


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------


class PriceEngine:
    """Pure, synchronous market simulator; the async loop lives in :mod:`app`.

    ``step(t_ns)`` advances every instrument whose product is in a continuous session at
    market time ``t_ns`` and returns the states that changed (live objects: read or copy
    them synchronously).
    """

    def __init__(
        self,
        cfg: FullConfig,
        start_ns: int,
        *,
        restored: Mapping[str, Mapping[str, Any]] | None = None,
        rng_td: str | None = None,
        rng_step: int = 0,
    ) -> None:
        pe = cfg.sim.price_engine
        self._calendar: TradingCalendar = cfg.calendar
        self.seed = int(pe.seed)
        self.interval_ms = int(pe.tick_interval_ms)
        if self.interval_ms <= 0:
            raise ValueError("tick_interval_ms must be positive")
        self.interval_ns = self.interval_ms * NS_PER_MS
        self.dt = self.interval_ms / 1000.0
        self.metas: list[InstrumentMeta] = build_metas(cfg)
        self._by_id: dict[str, InstrumentMeta] = {m.instrument_id: m for m in self.metas}
        self._chol = cholesky_psd(correlation_matrix(self.metas, pe.sector_correlation, pe.market_correlation))
        year_s = TRADING_DAYS_PER_YEAR * TRADING_SECONDS_PER_DAY
        self._sigma = np.array([m.vol_annual * math.sqrt(self.dt / year_s) for m in self.metas], dtype=float)
        self._p_jump = np.array(
            [m.jump_intensity_per_day * self.dt / TRADING_SECONDS_PER_DAY for m in self.metas], dtype=float
        )
        self._lam = np.array([max(m.volume_per_s, 0.0) * self.dt for m in self.metas], dtype=float)
        self._book_mean = np.array([max(2.0, m.volume_per_s * BOOK_SIZE_FACTOR) for m in self.metas], dtype=float)

        try:
            start_td = td_str(self._calendar.current_or_next_trading_day(start_ns))
        except LookupError as exc:
            raise ValueError(f"market time {start_ns} is outside the trading calendar") from exc
        ts0 = start_ns - start_ns % self.interval_ns
        self.states: dict[str, InstrumentState] = {}
        for m in self.metas:
            st: InstrumentState | None = None
            if restored and m.instrument_id in restored:
                try:
                    st = InstrumentState.from_dict(m, restored[m.instrument_id])
                except (KeyError, ValueError, TypeError) as exc:
                    logger.warning("discarding persisted state of {}: {}", m.instrument_id, exc)
                if st is not None and st.trading_day > start_td:
                    logger.warning(
                        "persisted state of {} belongs to {} (after {}); starting fresh",
                        m.instrument_id, st.trading_day, start_td,
                    )
                    st = None
            self.states[m.instrument_id] = st or self._fresh_state(m, start_td, ts0)
        self.rng_td: str | None = rng_td
        self.rng_step = int(rng_step)
        self.rounds = 0  # engine steps with at least one active instrument (this process)
        self.shocks: dict[str, Shock] = {}
        self.frozen: set[str] = set()
        self._window_cache: dict[tuple[str, str], int] = {}

    # ------------------------------------------------------------------ lookups
    @property
    def instrument_ids(self) -> list[str]:
        return [m.instrument_id for m in self.metas]

    def meta(self, instrument_id: str) -> InstrumentMeta | None:
        return self._by_id.get(instrument_id)

    def sectors(self) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {}
        for m in self.metas:
            out.setdefault(m.sector, []).append(m.instrument_id)
        return out

    def quote(self, instrument_id: str) -> Quote:
        st = self.states[instrument_id]
        return Quote(
            instrument_id=instrument_id,
            trading_day=st.trading_day,
            ts_ns=st.ts_ns,
            last=st.last,
            bid=st.bid,
            ask=st.ask,
            bid_vol=st.bid_vol,
            ask_vol=st.ask_vol,
            upper=st.upper,
            lower=st.lower,
            lock=st.lock,
            volume=st.volume,
        )

    def prices(self) -> dict[str, float]:
        return {iid: float(st.meta.price(st.last)) for iid, st in self.states.items()}

    def dump_states(self, instrument_ids: Iterable[str] | None = None) -> dict[str, dict[str, Any]]:
        ids = self.instrument_ids if instrument_ids is None else list(instrument_ids)
        return {iid: self.states[iid].to_dict() for iid in ids}

    # ------------------------------------------------------------------ shocks
    def add_shock(
        self,
        shock_id: str,
        *,
        pct: float,
        instruments: Sequence[str] | None = None,
        sector: str | None = None,
        duration_s: float = 0.0,
        shape: str = "STEP",
        created_ns: int = 0,
    ) -> Shock:
        """Register a price shock; raises ``ValueError`` for bad arguments.

        ``pct`` is a ratio (README §3.4: −0.04 means −4%). Targets are the listed
        instruments plus the members of ``sector``; neither means every instrument.
        """
        if not math.isfinite(pct) or not -1.0 < pct < 1.0:
            raise ValueError(f"pct must be a ratio in (-1, 1), e.g. -0.04 for -4%; got {pct}")
        shape_u = shape.upper()
        if shape_u not in SHAPES:
            raise ValueError(f"shape must be one of {SHAPES}")
        if duration_s < 0 or not math.isfinite(duration_s):
            raise ValueError("duration_s must be >= 0")
        targets: list[str] = []
        for iid in instruments or []:
            if iid not in self._by_id:
                raise ValueError(f"unknown instrument {iid}")
            if iid not in targets:
                targets.append(iid)
        if sector is not None:
            members = [m.instrument_id for m in self.metas if m.sector == sector]
            if not members:
                raise ValueError(f"unknown sector {sector}; known: {sorted(self.sectors())}")
            targets.extend(i for i in members if i not in targets)
        if not instruments and sector is None:
            targets = self.instrument_ids
        n_steps = 1 if shape_u == "STEP" else max(1, round(duration_s * 1000.0 / self.interval_ms))
        shock = Shock(
            shock_id=shock_id,
            targets=tuple(targets),
            pct=pct,
            shape=shape_u,
            duration_s=duration_s,
            n_steps=n_steps,
            per_step=math.log1p(pct) / n_steps,
            remaining={iid: n_steps for iid in targets},
            created_ns=created_ns,
        )
        self.shocks[shock_id] = shock
        return shock

    def _take_shock(self, instrument_id: str) -> float:
        total = 0.0
        for sh in self.shocks.values():
            left = sh.remaining.get(instrument_id)
            if not left:
                continue
            total += sh.per_step
            if left <= 1:
                del sh.remaining[instrument_id]
            else:
                sh.remaining[instrument_id] = left - 1
        return total

    def prune_shocks(self, keep_done: int = 20) -> None:
        """Drop the oldest completed shocks, keeping ``keep_done`` for reporting."""
        done = [k for k, s in self.shocks.items() if s.done]
        for k in done[:-keep_done] if keep_done else done:
            del self.shocks[k]

    # ------------------------------------------------------------------ stepping
    def _phase(self, m: InstrumentMeta, t_ns: int, cache: dict[tuple[str, ...], Phase] | None = None) -> Phase:
        ph = cache.get(m.sessions) if cache is not None else None
        if ph is None:
            ph = self._calendar.phase_at(m.sessions, t_ns)
            if cache is not None:
                cache[m.sessions] = ph
        return ph

    def _sync(self, st: InstrumentState, ph: Phase, t_ns: int) -> None:
        """Roll ``st`` once the first window (auction included) of a later trading day is open."""
        if ph.is_open and ph.trading_day is not None:
            td = td_str(ph.trading_day)
            if td > st.trading_day:  # never rewrite a later day if market time went backwards
                self._roll(st, td, t_ns)

    def sync_day(self, instrument_id: str, t_ns: int) -> InstrumentState:
        """Bring a contract's day-static data (``pre_settle``, limits, trading day) up to date.

        Called by the SimBroker before validating an order, so an order sent during the
        opening auction of a new trading day is checked against the new price band even
        before the next engine step. Rolling does not publish anything: the first
        continuous step of the day does.
        """
        st = self.states[instrument_id]
        with contextlib.suppress(LookupError):  # beyond the trading calendar: keep the last state
            self._sync(st, self._phase(st.meta, t_ns), t_ns)
        return st

    def step(self, t_ns: int) -> list[InstrumentState]:
        """Advance to market time ``t_ns``; returns the states that changed.

        A contract is rolled to a new trading day when the first window of that day opens
        (the auction included, 01 §8.2), and it trades only in continuous windows.
        """
        phases: dict[tuple[str, ...], Phase] = {}
        active: list[InstrumentMeta] = []
        market_td: str | None = None
        for m in self.metas:
            if m.instrument_id in self.frozen:
                continue
            try:
                ph = self._phase(m, t_ns, phases)
            except LookupError:
                return []  # beyond the trading calendar
            st = self.states[m.instrument_id]
            self._sync(st, ph, t_ns)
            if not ph.is_continuous or ph.trading_day is None:
                continue
            td = td_str(ph.trading_day)
            if td != st.trading_day:
                continue  # market time went backwards: never rewrite a later day
            if st.updates > 0 and st.ts_ns >= t_ns:
                continue  # time did not advance for this instrument
            market_td = td
            active.append(m)
        if not active or market_td is None:
            return []
        if market_td != self.rng_td:
            self.rng_td = market_td
            self.rng_step = 0
        rng = np.random.default_rng([self.seed, int(market_td), self.rng_step])
        self.rng_step += 1
        self.rounds += 1
        n = len(self.metas)
        # fixed draw layout: the stream never depends on which instruments are active
        z = self._chol @ rng.standard_normal(n)
        u_jump = rng.random(n)
        jump_z = rng.standard_normal(n)
        n_trades = rng.poisson(self._lam)
        bid_sz = rng.poisson(self._book_mean)
        ask_sz = rng.poisson(self._book_mean)
        u_dir = rng.random(n)
        u_oi = rng.random(n)
        u_lock = rng.random(n)
        changed: list[InstrumentState] = []
        for m in active:
            i = m.index
            dx = -0.5 * self._sigma[i] ** 2 + self._sigma[i] * float(z[i])
            if u_jump[i] < self._p_jump[i]:
                dx += m.jump_sigma * float(jump_z[i])
            dx += self._take_shock(m.instrument_id)
            st = self.states[m.instrument_id]
            if self._apply(
                st, t_ns, dx, int(n_trades[i]), int(bid_sz[i]), int(ask_sz[i]),
                float(u_dir[i]), float(u_oi[i]), float(u_lock[i]),
            ):
                changed.append(st)
        return changed

    def _apply(
        self,
        st: InstrumentState,
        t_ns: int,
        dx: float,
        n_raw: int,
        bid_sz: int,
        ask_sz: int,
        u_dir: float,
        u_oi: float,
        u_lock: float,
    ) -> bool:
        m = st.meta
        tick_f = float(m.tick)
        lo_x = math.log(st.lower * tick_f * (1.0 - LOCK_MARGIN))
        hi_x = math.log(st.upper * tick_f * (1.0 + LOCK_MARGIN))
        x = min(max(st.x + dx, lo_x), hi_x)
        p_ticks = math.exp(x) / tick_f
        if p_ticks >= st.upper:
            lock, last = LimitLock.UP, st.upper
        elif p_ticks <= st.lower:
            lock, last = LimitLock.DOWN, st.lower
        else:
            lock = LimitLock.NONE
            last = min(max(math.floor(p_ticks + 0.5), st.lower), st.upper)
        direction = (last > st.last) - (last < st.last) or (1 if u_dir < 0.5 else -1)

        n = n_raw
        if lock is not LimitLock.NONE:
            n = math.floor(n_raw * LOCKED_VOLUME_FACTOR + u_lock)
        if last != st.last and n <= 0:
            n = 1  # 01 §8.13: the last price may only change together with a trade

        bid: int | None
        ask: int | None
        if lock is LimitLock.UP:
            bid, ask, bid_vol, ask_vol = st.upper, None, (1 + bid_sz) * LOCKED_BOOK_FACTOR, 0
        elif lock is LimitLock.DOWN:
            bid, ask, bid_vol, ask_vol = None, st.lower, 0, (1 + ask_sz) * LOCKED_BOOK_FACTOR
        else:
            if direction > 0:
                bid, ask = last - 1, last
            else:
                bid, ask = last, last + 1
            if ask > st.upper:
                bid, ask = st.upper - 1, st.upper
            if bid < st.lower:
                bid, ask = st.lower, st.lower + 1
            bid_vol, ask_vol = 1 + bid_sz, 1 + ask_sz

        st.x = x
        changed = (
            n > 0
            or last != st.last
            or bid != st.bid
            or ask != st.ask
            or bid_vol != st.bid_vol
            or ask_vol != st.ask_vol
        )
        if not changed:
            return False
        # CFFEX last-hour VWAP window: snapshot the cumulative values at the boundary
        if m.settle_window_start is not None and st.win_volume is None and t_ns >= self._window_start_ns(st):
            st.win_volume = st.volume
            st.win_turnover_ticks = st.turnover_ticks
        st.lock = lock
        st.last = last
        st.bid, st.ask, st.bid_vol, st.ask_vol = bid, ask, bid_vol, ask_vol
        if n > 0:
            st.volume += n
            st.turnover_ticks += n * last
            if st.open is None:
                st.open = st.high = st.low = last
            else:
                st.high = max(st.high if st.high is not None else last, last)
                st.low = min(st.low if st.low is not None else last, last)
            d_oi = round((u_oi - 0.5) * OI_CHANGE_FACTOR * n)
            st.open_interest = max(1, st.open_interest + d_oi)
        st.ts_ns = t_ns
        st.updates += 1
        return True

    def _window_start_ns(self, st: InstrumentState) -> int:
        key = (st.meta.instrument_id, st.trading_day)
        cached = self._window_cache.get(key)
        if cached is None:
            start = st.meta.settle_window_start
            assert start is not None
            cached = bj_to_ns(datetime.combine(parse_td(st.trading_day), start, tzinfo=BJ))
            self._window_cache[key] = cached
        return cached

    # ------------------------------------------------------------------ day roll
    def settlement_price(self, st: InstrumentState) -> Decimal:
        """The sim's own settlement of the state's day (next day's ``pre_settle``, 01 §8.15).

        VWAP (last hour for CFFEX when available, otherwise full day) rounded by the
        exchange rule; no trades -> the old ``pre_settle``.
        """
        m = st.meta
        if st.volume <= 0:
            return st.pre_settle
        vol, tv = st.volume, st.turnover_ticks
        if (
            m.settle_method == SettleMethod.VWAP_LAST_HOUR
            and st.win_volume is not None
            and st.win_turnover_ticks is not None
            and st.volume > st.win_volume
        ):
            vol, tv = st.volume - st.win_volume, st.turnover_ticks - st.win_turnover_ticks
        q = Decimal(tv) / Decimal(vol)
        rounding = ROUND_FLOOR if m.settle_rounding == Rounding.FLOOR_TICK else ROUND_HALF_UP
        ticks = int(q.to_integral_value(rounding=rounding))
        ticks = min(max(ticks, st.lower), st.upper)
        return m.price(ticks)

    def _roll(self, st: InstrumentState, td: str, t_ns: int) -> None:
        m = st.meta
        settle = self.settlement_price(st)
        if st.volume > 0:
            st.pre_close = m.price(st.last)
        st.pre_settle = settle
        st.pre_open_interest = st.open_interest
        st.upper, st.lower = _limit_ticks(m, settle, td)
        st.last = min(max(st.last, st.lower), st.upper)
        tick_f = float(m.tick)
        st.x = min(
            max(st.x, math.log(st.lower * tick_f * (1.0 - LOCK_MARGIN))),
            math.log(st.upper * tick_f * (1.0 + LOCK_MARGIN)),
        )
        st.bid = st.ask = None
        st.bid_vol = st.ask_vol = 0
        st.open = st.high = st.low = None
        st.volume = 0
        st.turnover_ticks = 0
        st.lock = LimitLock.NONE
        st.win_volume = st.win_turnover_ticks = None
        st.updates = 0
        st.trading_day = td
        st.ts_ns = t_ns
        logger.debug("sim day roll {} -> {} pre_settle={}", m.instrument_id, td, settle)

    def _fresh_state(self, m: InstrumentMeta, td: str, ts_ns: int) -> InstrumentState:
        pre_settle = m.init_pre_settle
        upper, lower = _limit_ticks(m, pre_settle, td)
        last = min(max(int(round_to_tick(pre_settle, m.tick) / m.tick), lower), upper)
        return InstrumentState(
            meta=m,
            trading_day=td,
            pre_settle=pre_settle,
            pre_close=m.price(last),
            pre_open_interest=m.init_open_interest,
            upper=upper,
            lower=lower,
            last=last,
            bid=None,
            ask=None,
            bid_vol=0,
            ask_vol=0,
            open=None,
            high=None,
            low=None,
            volume=0,
            turnover_ticks=0,
            open_interest=m.init_open_interest,
            ts_ns=ts_ns,
            lock=LimitLock.NONE,
            x=math.log(float(m.price(last))),
        )
