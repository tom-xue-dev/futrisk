"""Historical data loader for the backtest (layer A, ``data/hist``; 01 §8.16 数据分层).

Everything here is read-only: the files under ``data/hist`` are produced by
``scripts/fetch_hist.py`` (``data/hist/SOURCES.md``) and may be refreshed while
a backtest runs elsewhere, so every file is read once and hashed from the very
bytes that are parsed (:meth:`HistData.file_hashes`, recorded in the manifest);
a cached instance whose files changed on disk is reloaded (:meth:`HistData.stale`).

Layer A (daily, 2015-now):

- ``daily/{product}.parquet``: the main-contract series (one row per trading
  day: the day's main contract, its OHLC, settle, ``pre_settle`` of the previous
  row, ``contract_pre_settle`` = the same contract's previous settle, ``roll``);
- ``daily/{EXCHANGE}/{product}.parquet``: per-contract bars. SHFE/CFFEX carry
  the exchange settle and pre-settle of every contract; DCE carries settle only
  on the main rows and, after 2024-07-17, only the main contract.

Missing values are estimated and flagged (``Bar.estimated``), never invented
silently:

- a DCE non-main settle is ``round_to_tick((high + low) / 2)`` (the proxy
  SOURCES.md validates as the best of close / HLC3 / OHLC4 / HL2);
- a contract without a bar on a day it is needed (a DCE old main after
  2024-07-17 on the roll day) gets a flat bar at its previous settle, i.e. the
  overnight move of that contract on that day is assumed to be zero;
- a missing previous settle of a new main is the estimated settle of its
  previous-day bar, else its own open; a DCE roll's back-adjustment factor
  (new contract's previous settle ÷ old contract's settle) uses that estimate
  (:meth:`ProductSeries.roll_source` says ``estimated`` / ``open``).
"""

from __future__ import annotations

import hashlib
import io
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field, replace
from datetime import date
from decimal import Decimal
from functools import cached_property
from pathlib import Path

import pandas as pd

from futrisk.common.calendar import TradingCalendar, parse_templates
from futrisk.common.clock import parse_td, td_str
from futrisk.common.config.products import InstrumentSpec, ProductsConfig, product_code
from futrisk.common.decimals import D, round_to_tick
from futrisk.core.instruments import InstrumentRegistry, delivery_month_of
from futrisk.messages.enums import Exchange

DCE_ARCHIVE_END = "20240717"  # SOURCES.md: Sina's per-contract DCE archive froze on this day


def _f(x: object) -> float | None:
    """NaN / None / NA -> None, else float."""
    if x is None:
        return None
    try:
        v = float(x)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return None if math.isnan(v) else v


def _i(x: object) -> int:
    v = _f(x)
    return int(v) if v is not None else 0


def _col(df: pd.DataFrame, name: str) -> list[float | None]:
    """A numeric column as floats with NaN / NA -> None (vectorised)."""
    if name not in df.columns:
        return [None] * len(df)
    arr = pd.to_numeric(df[name], errors="coerce").astype("float64").to_numpy()
    return [None if v != v else v for v in arr.tolist()]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass(frozen=True, slots=True)
class Bar:
    """One contract on one trading day (layer A)."""

    trading_day: str
    instrument_id: str
    product: str
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    settle: float | None
    settle_source: str  # exchange | sina | estimated | hl2 | flat
    pre_settle: float | None  # the same contract's previous settle
    pre_settle_source: str = "exchange"  # exchange | series | estimated | open
    volume: int = 0
    open_interest: int = 0
    turnover: float | None = None
    is_main: bool = False

    @property
    def traded(self) -> bool:
        return self.open is not None and self.high is not None and self.low is not None and self.close is not None

    @property
    def estimated(self) -> bool:
        return self.settle_source in ("estimated", "hl2", "flat") or self.pre_settle_source in ("estimated", "open")


@dataclass
class ProductSeries:
    """The main-contract series of one product (``daily/{product}.parquet``)."""

    product: str
    exchange: Exchange
    days: list[str]
    mains: list[str]
    rolls: list[bool]
    open: list[float]
    high: list[float]
    low: list[float]
    close: list[float]
    settle: list[float]
    pre_settle: list[float]
    contract_pre_settle: list[float | None]
    settle_source: list[str]
    volume: list[int]
    index: dict[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.index = {d: i for i, d in enumerate(self.days)}

    def main_of(self, td: str) -> str | None:
        i = self.index.get(td)
        return self.mains[i] if i is not None else None

    def prev_day(self, td: str) -> str | None:
        i = self.index.get(td)
        return self.days[i - 1] if i is not None and i > 0 else None

    def next_day(self, td: str) -> str | None:
        i = self.index.get(td)
        return self.days[i + 1] if i is not None and i + 1 < len(self.days) else None

    # the new main's previous settle where the series has none (DCE): estimated by the loader, see roll_source
    est_contract_pre_settle: list[float | None] = field(default_factory=list)
    est_source: list[str] = field(default_factory=list)

    def roll_factor(self, td: str) -> float:
        """Multiplicative back-adjustment on a roll day: new contract's previous settle ÷ old contract's settle.

        The new contract's previous settle is the exchange's (``contract_pre_settle``) or, for DCE where the
        series has none, the loader's estimate (:meth:`roll_source` says which); 1.0 on other days.
        """
        i = self.index.get(td)
        if i is None or not self.rolls[i]:
            return 1.0
        cps = self.contract_pre_settle[i]
        if cps is None and i < len(self.est_contract_pre_settle):
            cps = self.est_contract_pre_settle[i]
        ps = self.pre_settle[i]
        if cps is None or not ps:
            return 1.0
        return cps / ps

    def roll_source(self, td: str) -> str:
        """``exchange`` | ``estimated`` (previous-day settle of the new contract) | ``open`` (its open) | ``none``."""
        i = self.index.get(td)
        if i is None or not self.rolls[i]:
            return "none"
        if self.contract_pre_settle[i] is not None:
            return "exchange"
        if i < len(self.est_source) and self.est_contract_pre_settle[i] is not None:
            return self.est_source[i]
        return "none"



@dataclass(frozen=True, slots=True)
class DailyBar:
    """Completed daily bar of the main series as the strategies see it (raw prices of that day's main)."""

    trading_day: str
    instrument_id: str
    open: float
    high: float
    low: float
    close: float
    settle: float
    roll_factor: float  # multiply earlier history by this on this day (back-adjustment to the new contract)
    roll: bool


class HistData:
    """Layer A data of a set of products, loaded read-only from ``data_dir``."""

    def __init__(
        self,
        data_dir: Path,
        products: Sequence[str],
        base_products: ProductsConfig,
        *,
        start: str | None = None,
        end: str | None = None,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.base_products = base_products
        self.notes: list[str] = []
        self.files: dict[str, Path] = {}
        self.file_sha: dict[str, str] = {}  # sha256 of the bytes actually parsed
        self.file_stat: dict[str, tuple[int, int]] = {}  # (mtime_ns, size) when read: staleness check of caches
        self.series: dict[str, ProductSeries] = {}
        self._bars: dict[tuple[str, str], Bar] = {}
        self._contract_days: dict[str, list[str]] = {}
        for p in products:
            if p not in base_products.products:
                self.notes.append(f"{p}: not in products.yaml, skipped")
                continue
            try:
                self._load_product(p)
            except FileNotFoundError as exc:
                self.notes.append(f"{p}: data missing ({exc.filename or exc}), skipped")
        self.start = start
        self.end = end

    # ---------------------------------------------------------------- loading
    def _read(self, rel: str) -> pd.DataFrame:
        """Read one file once: its SHA-256 is taken from the very bytes parsed (hash at read time)."""
        path = self.data_dir / rel
        if not path.exists():
            raise FileNotFoundError(2, "missing", str(path))
        st = path.stat()
        data = path.read_bytes()
        self.files[rel] = path
        self.file_sha[rel] = hashlib.sha256(data).hexdigest()
        self.file_stat[rel] = (st.st_mtime_ns, st.st_size)
        return pd.read_parquet(io.BytesIO(data))

    def stale(self) -> bool:
        """True when a file read by this instance changed on disk since (a cached instance must be reloaded)."""
        for rel, (mtime, size) in self.file_stat.items():
            try:
                st = (self.data_dir / rel).stat()
            except OSError:
                return True
            if (st.st_mtime_ns, st.st_size) != (mtime, size):
                return True
        return False

    def _load_product(self, p: str) -> None:
        spec = self.base_products.products[p]
        exch = spec.exchange
        tick = spec.tick_size
        cont = self._read(f"daily/{p}.parquet").sort_values("trading_day").reset_index(drop=True)
        cont = cont[cont["instrument_id"].notna()]
        s = ProductSeries(
            product=p,
            exchange=exch,
            days=[str(x) for x in cont["trading_day"]],
            mains=[str(x) for x in cont["instrument_id"]],
            rolls=[bool(x) for x in cont["roll"]],
            open=[float(x) for x in cont["open"]],
            high=[float(x) for x in cont["high"]],
            low=[float(x) for x in cont["low"]],
            close=[float(x) for x in cont["close"]],
            settle=[float(x) for x in cont["settle"]],
            pre_settle=[float(x) for x in cont["pre_settle"]],
            contract_pre_settle=_col(cont, "contract_pre_settle"),
            settle_source=[str(x) for x in cont["settle_source"]],
            volume=[int(v or 0) for v in _col(cont, "volume")],
        )
        self.series[p] = s
        mains = set(s.mains)
        try:
            pc = self._read(f"daily/{exch.value}/{p}.parquet")
        except FileNotFoundError:
            pc = None
            self.notes.append(f"{p}: per-contract file daily/{exch.value}/{p}.parquet missing; main series only")
        # main rows from the continuous series (authoritative settle / contract pre-settle)
        for i, td in enumerate(s.days):
            inst = s.mains[i]
            cps = s.contract_pre_settle[i]
            self._bars[(td, inst)] = Bar(
                trading_day=td,
                instrument_id=inst,
                product=p,
                open=s.open[i],
                high=s.high[i],
                low=s.low[i],
                close=s.close[i],
                settle=s.settle[i],
                settle_source=s.settle_source[i],
                pre_settle=cps,
                pre_settle_source="exchange" if cps is not None else "missing",
                volume=s.volume[i],
                is_main=True,
            )
        if pc is not None and len(pc):
            pc = pc[pc["instrument_id"].isin(mains)]  # the contracts that are a main on some day
            n = len(pc)
            tds = [str(x) for x in pc["trading_day"].tolist()]
            insts = [str(x) for x in pc["instrument_id"].tolist()]
            opens, highs, lows, closes = (_col(pc, c) for c in ("open", "high", "low", "close"))
            settles, pres, turns = _col(pc, "settle"), _col(pc, "pre_settle"), _col(pc, "turnover")
            vols, ois = _col(pc, "volume"), _col(pc, "open_interest")
            srcs = pc["settle_source"].tolist() if "settle_source" in pc.columns else [None] * n
            for k in range(n):
                td, inst = tds[k], insts[k]
                key = (td, inst)
                self._contract_days.setdefault(inst, []).append(td)
                ps = pres[k]
                existing = self._bars.get(key)
                if existing is not None:
                    # main row: keep the series' settle; fill turnover / OI from the per-contract row
                    self._bars[key] = replace(
                        existing,
                        turnover=turns[k],
                        open_interest=int(ois[k] or 0),
                        pre_settle=existing.pre_settle if existing.pre_settle is not None else ps,
                        pre_settle_source=existing.pre_settle_source
                        if existing.pre_settle is not None
                        else ("exchange" if ps is not None else "missing"),
                    )
                    continue
                h, lo = highs[k], lows[k]
                st = settles[k]
                src = srcs[k] if isinstance(srcs[k], str) else ""
                if st is None and h is not None and lo is not None:
                    st = float(round_to_tick(D((h + lo) / 2.0), tick))
                    src = "hl2"
                self._bars[key] = Bar(
                    trading_day=td,
                    instrument_id=inst,
                    product=p,
                    open=opens[k],
                    high=h,
                    low=lo,
                    close=closes[k],
                    settle=st,
                    settle_source=src or "exchange",
                    pre_settle=ps,
                    pre_settle_source="exchange" if ps is not None else "missing",
                    volume=int(vols[k] or 0),
                    open_interest=int(ois[k] or 0),
                    turnover=turns[k],
                    is_main=False,
                )
        for inst in self._contract_days:
            self._contract_days[inst].sort()
        # DCE: a main row's missing contract pre-settle = the previous day's bar settle of that contract (estimate)
        for i, td in enumerate(s.days):
            inst = s.mains[i]
            b = self._bars[(td, inst)]
            if b.pre_settle is not None:
                continue
            prev = s.days[i - 1] if i > 0 else None
            pb = self._bars.get((prev, inst)) if prev else None
            est: float | None
            if pb is not None and pb.settle is not None:
                est, src = pb.settle, "estimated"
            else:
                est, src = b.open, "open"
            self._bars[(td, inst)] = replace(b, pre_settle=est, pre_settle_source=src)
        # roll days without the new main's previous settle (DCE): the estimate above backs the roll factor
        s.est_contract_pre_settle = [None] * len(s.days)
        s.est_source = [""] * len(s.days)
        for i, td in enumerate(s.days):
            if s.rolls[i] and s.contract_pre_settle[i] is None:
                b = self._bars[(td, s.mains[i])]
                if b.pre_settle is not None and b.pre_settle_source in ("estimated", "open"):
                    s.est_contract_pre_settle[i] = b.pre_settle
                    s.est_source[i] = b.pre_settle_source

    # ----------------------------------------------------------------- access
    @property
    def products(self) -> list[str]:
        return list(self.series)

    @cached_property
    def all_days(self) -> list[str]:
        days: set[str] = set()
        for s in self.series.values():
            days.update(s.days)
        return sorted(days)

    def trading_days(self, start: str | None = None, end: str | None = None) -> list[str]:
        lo = start or self.start or "00000000"
        hi = end or self.end or "99999999"
        return [d for d in self.all_days if lo <= d <= hi]

    def main(self, product: str, td: str) -> str | None:
        s = self.series.get(product)
        return s.main_of(td) if s is not None else None

    def bar(self, td: str, instrument_id: str) -> Bar | None:
        return self._bars.get((td, instrument_id))

    def contract_last_day(self, instrument_id: str) -> str | None:
        days = self._contract_days.get(instrument_id)
        return days[-1] if days else None

    def contract_first_day(self, instrument_id: str) -> str | None:
        days = self._contract_days.get(instrument_id)
        return days[0] if days else None

    def instrument_bar(self, td: str, instrument_id: str, pre_settle: float | None) -> Bar:
        """The contract's bar on ``td``; a flat bar at its previous settle when the data has none."""
        b = self._bars.get((td, instrument_id))
        if b is not None and b.traded:
            if b.pre_settle is None and pre_settle is not None:
                return replace(b, pre_settle=pre_settle, pre_settle_source="series")
            return b
        ps = pre_settle if pre_settle is not None else (b.pre_settle if b is not None else None)
        if ps is None:
            raise KeyError(f"no data and no previous settle for {instrument_id} on {td}")
        return Bar(
            trading_day=td,
            instrument_id=instrument_id,
            product=product_code(instrument_id),
            open=ps,
            high=ps,
            low=ps,
            close=ps,
            settle=ps,
            settle_source="flat",
            pre_settle=ps,
            pre_settle_source="series",
            volume=0,
        )

    def daily_bars(self, product: str, *, until: str | None = None, since: str | None = None) -> list[DailyBar]:
        """The main series of ``product`` as :class:`DailyBar` (raw prices; ``roll_factor`` for back-adjustment)."""
        s = self.series[product]
        out: list[DailyBar] = []
        for i, td in enumerate(s.days):
            if since is not None and td < since:
                continue
            if until is not None and td > until:
                break
            out.append(
                DailyBar(
                    trading_day=td,
                    instrument_id=s.mains[i],
                    open=s.open[i],
                    high=s.high[i],
                    low=s.low[i],
                    close=s.close[i],
                    settle=s.settle[i],
                    roll_factor=s.roll_factor(td),
                    roll=s.rolls[i],
                )
            )
        return out

    # ------------------------------------------------------------ universe
    def calendar(self, base: TradingCalendar | None = None) -> TradingCalendar:
        """Trading days of the data (plus the base calendar's days) with the products' session templates."""
        days: set[date] = {parse_td(d) for d in self.all_days}
        if base is not None:
            days.update(base.trading_days_between(base.first_day, base.last_day))
        return TradingCalendar(sorted(days), parse_templates(self.base_products.session_templates))

    def listed_products(self, instruments: Iterable[str], calendar: TradingCalendar) -> ProductsConfig:
        """``products.yaml`` with the historical contracts listed (current specs applied to history)."""
        return list_instruments(self.base_products, instruments, calendar, first_day=self.contract_first_day,
                                last_day=self.contract_last_day)

    def all_mains(self) -> list[str]:
        out: set[str] = set()
        for s in self.series.values():
            out.update(s.mains)
        return sorted(out)

    def file_hashes(self) -> dict[str, str]:
        """SHA-256 of every file this instance read, taken when it was read (the data the run actually used)."""
        return dict(sorted(self.file_sha.items()))


def list_instruments(
    base: ProductsConfig,
    instruments: Iterable[str],
    calendar: TradingCalendar,
    *,
    first_day: object = None,
    last_day: object = None,
) -> ProductsConfig:
    """Add historical contracts to ``base.instruments`` (the pre-trade check needs listed contracts).

    ``last_trade_day`` = the later of the exchange-rule estimate (the registry's
    rule for unlisted contracts) and the last day the contract appears in the
    data; ``listed_day`` = its first data day (or 2000-01-01).
    """
    unlisted = InstrumentRegistry(base, calendar, allow_unlisted=True)
    known = {i.instrument_id for i in base.instruments}
    extra: list[InstrumentSpec] = []
    for inst in sorted(set(instruments)):
        if inst in known:
            continue
        try:
            info = unlisted.get(inst)
        except KeyError:
            continue
        est = info.last_trade_day
        data_last = last_day(inst) if callable(last_day) else None
        last = max(x for x in (est, data_last, "20000101") if x)
        extra.append(
            InstrumentSpec(
                instrument_id=inst,
                product=info.product,
                last_trade_day=last,
                listed_day="20000101",
                delivery_month=delivery_month_of(inst),
            )
        )
    if not extra:
        return base
    return ProductsConfig.model_validate({**base.model_dump(mode="python"), "instruments": [*base.instruments, *extra]})


def round_price(price: float, tick: Decimal) -> float:
    """Round to the tick grid (nearest)."""
    return float(round_to_tick(D(price), tick))


def td_range(days: Sequence[str], start: str | None, end: str | None) -> list[str]:
    lo = start or "00000000"
    hi = end or "99999999"
    return [d for d in days if lo <= d <= hi]


def weekday_td(d: date) -> str:
    return td_str(d)


__all__ = [
    "DCE_ARCHIVE_END",
    "Bar",
    "DailyBar",
    "HistData",
    "ProductSeries",
    "list_instruments",
    "round_price",
    "sha256_file",
    "td_range",
]
