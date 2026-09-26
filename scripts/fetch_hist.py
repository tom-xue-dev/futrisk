#!/usr/bin/env python
"""Fetch historical futures market data for futrisk backtests (data layers A and B).

Dev-time tool, not part of the runtime (docs/01-architecture.md §8.13, §8.16;
docs/03-database-schema.md §5.2). Run it with the isolated data virtualenv, never with
the project venv::

    py -3.12 -m venv .venv-data
    .venv-data/Scripts/python -m pip install akshare pyarrow
    .venv-data/Scripts/python scripts/fetch_hist.py                # fetch whatever is missing
    .venv-data/Scripts/python scripts/fetch_hist.py --update       # incremental refresh
    .venv-data/Scripts/python scripts/fetch_hist.py --products rb,IF --layers minute --force
    .venv-data/Scripts/python scripts/fetch_hist.py --verify-only  # re-check files, rewrite manifest

Outputs, all under ``data/hist`` (column definitions and caveats: ``data/hist/SOURCES.md``):

* ``daily/{EXCHANGE}/{product}.parquet``  per-contract daily bars with ``is_main`` (layer A);
* ``daily/{product}.parquet``             continuous main-contract daily series (layer A);
* ``minute/{product}_{period}m.parquet``  Sina 5/15/60-minute bars of the main contract (validation evidence; layer B of the full version);
* ``official/{EXCHANGE}.parquet``         official per-contract data of the last N trading days;
* ``manifest.json``                       row counts, date ranges, SHA-256, checks, failures.

Sources: SHFE and CFFEX daily bars come from the exchanges (akshare ``get_shfe_daily`` and
the CFFEX monthly archive that akshare's ``get_cffex_daily`` reads). DCE daily bars come from
the Sina main-continuous series (akshare ``futures_zh_daily_sina``) because dce.com.cn blocks
scripted access; the contract behind each DCE row is identified through Sina's legacy
per-contract archive (complete until 2024-07-17) and 100ppi's daily near/dominant contract
prices (the page behind akshare ``futures_spot_price``). Minute bars come from akshare
``futures_zh_minute_sina``.

Behaviour: idempotent (existing outputs are skipped unless ``--force``; ``--update`` appends
or merges new data), polite (at least ``--sleep`` seconds between two requests to the same
host), robust (retry with exponential backoff; a failure is logged, recorded in the manifest
and the run continues with the next product).
"""

from __future__ import annotations

import argparse
import bisect
import datetime as dt
import hashlib
import io
import json
import logging
import re
import sys
import time
import zipfile
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import requests.adapters

try:
    import akshare as ak
    from akshare.futures import cons as ak_cons
    from akshare.futures.symbol_var import chinese_to_english
except ImportError:  # pragma: no cover - guidance for people running the wrong interpreter
    sys.exit(
        "akshare is not installed in this interpreter. Use the data venv:\n"
        "  py -3.12 -m venv .venv-data && .venv-data/Scripts/python -m pip install akshare pyarrow\n"
        "  .venv-data/Scripts/python scripts/fetch_hist.py"
    )

log = logging.getLogger("fetch_hist")

# akshare calls requests without a timeout; one stalled socket would hang the whole run.
_HTTP_TIMEOUT = (10, 60)
_orig_send = requests.adapters.HTTPAdapter.send


def _send_with_default_timeout(self, request, **kwargs):  # type: ignore[no-untyped-def]
    if kwargs.get("timeout") is None:
        kwargs["timeout"] = _HTTP_TIMEOUT
    return _orig_send(self, request, **kwargs)


requests.adapters.HTTPAdapter.send = _send_with_default_timeout  # type: ignore[method-assign]

# --------------------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------------------

REPO = Path(__file__).resolve().parents[1]
DEFAULT_OUT = REPO / "data" / "hist"
PROJECT_CALENDAR = REPO / "config" / "calendar" / "trading_days.csv"
BJ_TZ = "Asia/Shanghai"
UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
}
# Legacy Sina endpoint: per-contract daily OHLCV, including expired contracts (the akshare
# endpoint InnerFuturesNewService.getDailyKLine only serves listed contracts).
SINA_LEGACY_URL = (
    "https://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesDailyKLine"
)
CFFEX_MONTH_ZIP_URL = "http://www.cffex.com.cn/sj/historysj/{ym}/zip/{ym}.zip"
CFFEX_DAY_CSV_URL = "http://www.cffex.com.cn/sj/hqsj/rtj/{ym}/{dd}/{day}_1.csv"
# 100ppi daily spot/futures page (source of akshare futures_spot_price): near and dominant
# contract codes with their closing prices; used to identify DCE contracts after 2024-07-17.
PPI_DAY_URL = "https://www.100ppi.com/sf/day-{y}-{m}-{d}.html"
TURNOVER_UNIT = 10_000.0  # SHFE and CFFEX publish turnover in units of 10,000 CNY
LAYERS = ("daily", "minute", "official")
DEFAULT_PERIODS = ("5", "15", "60")
PRICE_COLS = ["open", "high", "low", "close"]
CONT_COLUMNS = [
    "trading_day",
    "instrument_id",
    "open",
    "high",
    "low",
    "close",
    "settle",
    "pre_settle",
    "contract_pre_settle",
    "volume",
    "open_interest",
    "roll",
    "settle_source",
    "instrument_source",
    "sina_instrument_id",
]
PER_COLUMNS = [
    "trading_day",
    "instrument_id",
    "open",
    "high",
    "low",
    "close",
    "settle",
    "pre_settle",
    "volume",
    "open_interest",
    "turnover",
    "is_main",
    "settle_source",
]
MINUTE_COLUMNS = [
    "ts",
    "ts_ns",
    "trading_day",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_interest",
    "instrument_id",
    "roll",
    "quality",
]
OFFICIAL_COLUMNS = [
    "trading_day",
    "exchange",
    "product",
    "instrument_id",
    "open",
    "high",
    "low",
    "close",
    "settle",
    "pre_settle",
    "volume",
    "open_interest",
    "turnover",
]


@dataclass(frozen=True)
class Product:
    """Static product facts needed here; mirrors config/products.yaml (2026-09)."""

    code: str  # futrisk product code
    exchange: str  # SHFE | DCE | CFFEX
    sina: str  # Sina main-continuous symbol
    multiplier: int
    tick: float
    months: tuple[int, ...] = tuple(range(1, 13))  # listed delivery months (DCE legacy fetch)
    main_months: tuple[int, ...] = ()  # usual main months, fetched first (DCE legacy fetch)
    limit_pct: float = 0.0  # current daily limit, only used for stress-window annotations


PRODUCTS: dict[str, Product] = {
    p.code: p
    for p in (
        Product("rb", "SHFE", "RB0", 10, 1.0, limit_pct=0.05),
        Product("hc", "SHFE", "HC0", 10, 1.0, limit_pct=0.05),
        Product("cu", "SHFE", "CU0", 5, 10.0, limit_pct=0.07),
        Product("au", "SHFE", "AU0", 1000, 0.02, limit_pct=0.14),
        Product("i", "DCE", "I0", 100, 0.5, main_months=(1, 5, 9), limit_pct=0.06),
        Product("j", "DCE", "J0", 100, 0.5, main_months=(1, 5, 9), limit_pct=0.08),
        Product(
            "m",
            "DCE",
            "M0",
            10,
            1.0,
            months=(1, 3, 5, 7, 8, 9, 11, 12),
            main_months=(1, 5, 9),
            limit_pct=0.06,
        ),
        Product("IF", "CFFEX", "IF0", 300, 0.2, limit_pct=0.10),
        Product("IH", "CFFEX", "IH0", 300, 0.2, limit_pct=0.10),
        Product("IC", "CFFEX", "IC0", 200, 0.2, limit_pct=0.10),
        Product("IM", "CFFEX", "IM0", 200, 0.2, limit_pct=0.10),
        Product("T", "CFFEX", "T0", 10000, 0.005, limit_pct=0.02),
    )
}

# (label, first day, last day, products) — checked by the verification step.
STRESS_WINDOWS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    ("2015-07 equity crash", "20150701", "20150731", ("IF", "IH", "IC")),
    ("2016-11-14 black-sector crash", "20161110", "20161118", ("rb", "hc", "i", "j")),
    (
        "2020-02-03 post-holiday limit-down",
        "20200203",
        "20200204",
        ("rb", "hc", "cu", "au", "i", "j", "m", "IF", "IH", "IC", "T"),
    ),
    ("2021-05 iron ore", "20210506", "20210531", ("i", "rb", "hc")),
    ("2021-10 coke", "20211008", "20211029", ("j",)),
    ("2022-03 nickel squeeze (ni not tracked)", "20220301", "20220331", ("cu",)),
    ("2024-10-08 index limit-up open", "20240924", "20241009", ("IF", "IH", "IC", "IM")),
)


# --------------------------------------------------------------------------------------
# HTTP politeness / retry
# --------------------------------------------------------------------------------------


class FetchError(RuntimeError):
    """A request that still failed after all retries."""


def _short(exc: BaseException | None) -> str:
    text = f"{type(exc).__name__}: {exc}" if exc else "unknown error"
    return text if len(text) <= 240 else text[:237] + "..."


class Fetcher:
    """Per-host minimum interval between requests plus retry with exponential backoff."""

    def __init__(self, sleep: float, retries: int, backoff: float = 2.0) -> None:
        self.sleep = sleep
        self.retries = retries
        self.backoff = backoff
        self.requests = 0
        self.failures: list[dict[str, str]] = []
        self._next_ok: dict[str, float] = {}

    def call(
        self,
        host: str,
        what: str,
        fn: Callable[..., Any],
        *args: Any,
        retries: int | None = None,
        **kwargs: Any,
    ) -> Any:
        retries = self.retries if retries is None else retries
        last: BaseException | None = None
        for attempt in range(retries + 1):
            gap = self._next_ok.get(host, 0.0) - time.monotonic()
            if gap > 0:
                time.sleep(gap)
            try:
                self.requests += 1
                return fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001 - network/parse errors of all kinds are retried
                last = exc
                if attempt < retries:
                    pause = self.backoff * (2**attempt)
                    log.warning(
                        "%s: attempt %d/%d failed (%s); retrying in %.0fs",
                        what,
                        attempt + 1,
                        retries + 1,
                        _short(exc),
                        pause,
                    )
                    time.sleep(pause)
            finally:
                self._next_ok[host] = time.monotonic() + self.sleep
        raise FetchError(f"{what}: {_short(last)}")

    def fail(self, layer: str, product: str, what: str, exc: BaseException | str) -> None:
        err = exc if isinstance(exc, str) else _short(exc)
        log.error("FAILED [%s/%s] %s: %s", layer, product, what, err)
        self.failures.append(
            {
                "layer": layer,
                "product": product,
                "what": what,
                "error": err,
                "at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
            }
        )


# --------------------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------------------


def load_calendar() -> list[str]:
    """Trading days (YYYYMMDD): akshare's calendar, replaced by config/calendar/trading_days.csv
    for the date span the project calendar covers (both agree on 2025-2026)."""
    days = set(ak_cons.get_calendar())
    if PROJECT_CALENDAR.exists():
        lines = PROJECT_CALENDAR.read_text(encoding="utf-8").splitlines()
        proj = {s.strip() for s in lines if re.fullmatch(r"\d{8}", s.strip())}
        if proj:
            lo, hi = min(proj), max(proj)
            days = {d for d in days if not lo <= d <= hi} | proj
    return sorted(days)


def next_trading_day(cal: list[str], day: str) -> str | None:
    """First trading day strictly after ``day``."""
    i = bisect.bisect_right(cal, day)
    return cal[i] if i < len(cal) else None


def prev_trading_day(cal: list[str], day: str) -> str | None:
    """Last trading day strictly before ``day``."""
    i = bisect.bisect_left(cal, day)
    return cal[i - 1] if i > 0 else None


def now_bj() -> dt.datetime:
    return dt.datetime.now(dt.UTC).astimezone(dt.timezone(dt.timedelta(hours=8))).replace(tzinfo=None)


def last_published_day(cal: list[str]) -> str:
    """Latest trading day whose daily data is published (exchanges publish after ~17:00)."""
    now = now_bj()
    today = now.strftime("%Y%m%d")
    eligible = [d for d in cal if d < today or (d == today and now.hour >= 17)]
    return eligible[-1]


def add_months(d: dt.date, n: int) -> dt.date:
    y, m = divmod(d.month - 1 + n, 12)
    return dt.date(d.year + y, m + 1, 1)


def round_tick(x: pd.Series, tick: float) -> pd.Series:
    return (np.round(x / tick) * tick).round(6)


def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def write_parquet(df: pd.DataFrame, path: Path) -> None:
    """Atomic write via pyarrow (zstd). Strings are written as plain object columns so that
    the project stack (pandas 2.x, pyarrow >= 17) reads the files without pandas-3 dtypes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df = df.reset_index(drop=True).copy()
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_string_dtype(s) or s.dtype == object:
            df[col] = pd.Series([None if pd.isna(v) else str(v) for v in s], dtype=object)
    table = pa.Table.from_pandas(df, preserve_index=False)
    for i, f in enumerate(table.schema):  # an all-None string column must stay a string column
        if pa.types.is_null(f.type):
            table = table.set_column(i, pa.field(f.name, pa.string()), table.column(i).cast(pa.string()))
    tmp = path.with_name(path.name + ".tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(path)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def delivery_key(instrument_id: str) -> int:
    """yymm of a contract code as an int, e.g. rb2601 -> 2601 (orderable within 2000-2099)."""
    m = re.search(r"(\d{4})$", instrument_id)
    return int(m.group(1)) if m else -1


# --------------------------------------------------------------------------------------
# Run context
# --------------------------------------------------------------------------------------


@dataclass
class Ctx:
    out: Path
    cal: list[str]
    start: str
    end: str
    fetch_start: str  # one trading day before start, so the first day has a previous day
    force: bool
    update: bool
    fetcher: Fetcher
    stats: dict[str, Any] = field(default_factory=dict)
    ppi_cache: dict[str, pd.DataFrame] = field(default_factory=dict)

    @property
    def cal_set(self) -> set[str]:
        return set(self.cal)

    def cont_path(self, p: Product) -> Path:
        return self.out / "daily" / f"{p.code}.parquet"

    def per_path(self, p: Product) -> Path:
        return self.out / "daily" / p.exchange / f"{p.code}.parquet"

    def minute_path(self, p: Product, period: str) -> Path:
        return self.out / "minute" / f"{p.code}_{period}m.parquet"


# --------------------------------------------------------------------------------------
# Exchange per-contract daily data (SHFE, CFFEX)
# --------------------------------------------------------------------------------------


def normalize_shfe(raw: pd.DataFrame, codes: set[str] | None) -> pd.DataFrame:
    """akshare get_shfe_daily output -> normalized per-contract rows (turnover in CNY)."""
    df = raw.copy()
    df["product"] = df["variety"].astype(str).str.strip().str.lower()
    if codes is not None:
        df = df[df["product"].isin(codes)]
    out = pd.DataFrame(
        {
            "trading_day": df["date"].astype(str).str.replace("-", "", regex=False),
            "exchange": "SHFE",
            "product": df["product"],
            "instrument_id": df["symbol"].astype(str).str.strip().str.lower(),
            "open": num(df["open"]),
            "high": num(df["high"]),
            "low": num(df["low"]),
            "close": num(df["close"]),
            "settle": num(df["settle"]),
            "pre_settle": num(df["pre_settle"]),
            "volume": num(df["volume"]),
            "open_interest": num(df["open_interest"]),
            "turnover": num(df["turnover"]) * TURNOVER_UNIT,
        }
    )
    return out.reset_index(drop=True)


_CFFEX_COLS = {
    "合约代码": "instrument_id",
    "今开盘": "open",
    "最高价": "high",
    "最低价": "low",
    "成交量": "volume",
    "成交金额": "turnover",
    "持仓量": "open_interest",
    "今收盘": "close",
    "今结算": "settle",
    "前结算": "pre_settle",
}


def parse_cffex_csv(content: bytes, day: str, codes: set[str] | None) -> pd.DataFrame:
    """One CFFEX daily CSV (GBK) -> normalized futures rows; options and subtotals dropped."""
    df = pd.read_csv(io.BytesIO(content), encoding="gbk", dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    missing = set(_CFFEX_COLS) - set(df.columns)
    if missing:
        raise ValueError(f"CFFEX {day}: unexpected CSV header, missing {sorted(missing)}")
    df = df[list(_CFFEX_COLS)].rename(columns=_CFFEX_COLS)
    df["instrument_id"] = df["instrument_id"].astype(str).str.strip()
    parts = df["instrument_id"].str.extract(r"^([A-Z]+)(\d{4})$")
    df["product"] = parts[0]
    df = df[df["product"].notna()]
    if codes is not None:
        df = df[df["product"].isin(codes)]
    out = pd.DataFrame(
        {
            "trading_day": day,
            "exchange": "CFFEX",
            "product": df["product"],
            "instrument_id": df["instrument_id"],
            "open": num(df["open"]),
            "high": num(df["high"]),
            "low": num(df["low"]),
            "close": num(df["close"]),
            "settle": num(df["settle"]),
            "pre_settle": num(df["pre_settle"]),
            "volume": num(df["volume"]),
            "open_interest": num(df["open_interest"]),
            "turnover": num(df["turnover"]) * TURNOVER_UNIT,
        }
    )
    return out.reset_index(drop=True)


def _cffex_month_zip(ym: str) -> dict[str, bytes]:
    """All daily CSVs of one month from the CFFEX archive; {} when the month is not published."""
    r = requests.get(CFFEX_MONTH_ZIP_URL.format(ym=ym), headers=UA)
    if r.status_code == 404:
        return {}
    r.raise_for_status()
    if not r.content.startswith(b"PK"):
        return {}
    files: dict[str, bytes] = {}
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        for name in zf.namelist():
            m = re.search(r"(\d{8})_1\.csv$", name)
            if m:
                files[m.group(1)] = zf.read(name)
    return files


def _cffex_day_csv(day: str) -> bytes | None:
    r = requests.get(CFFEX_DAY_CSV_URL.format(ym=day[:6], dd=day[6:], day=day), headers=UA)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    if r.content.lstrip()[:1] == b"<":  # HTML error page instead of CSV
        return None
    return r.content


def fetch_cffex_contracts(ctx: Ctx, days: list[str], codes: set[str]) -> pd.DataFrame:
    """CFFEX per-contract rows for ``days``: one archive zip per month (the source behind
    akshare get_cffex_daily, downloaded once per month instead of once per day), with the
    single-day CSV as fallback for days the archive does not contain yet."""
    frames: list[pd.DataFrame] = []
    months = sorted({d[:6] for d in days})
    for n, ym in enumerate(months, 1):
        want = [d for d in days if d[:6] == ym]
        try:
            got = ctx.fetcher.call("cffex", f"CFFEX archive {ym}", _cffex_month_zip, ym)
        except FetchError as exc:
            ctx.fetcher.fail("daily", "CFFEX", f"archive {ym}", exc)
            got = {}
        for day in want:
            content = got.get(day)
            if content is None:
                try:
                    content = ctx.fetcher.call("cffex", f"CFFEX csv {day}", _cffex_day_csv, day)
                except FetchError as exc:
                    ctx.fetcher.fail("daily", "CFFEX", f"csv {day}", exc)
                    continue
                if content is None:
                    log.warning("CFFEX %s: no data published", day)
                    continue
            try:
                frames.append(parse_cffex_csv(content, day, codes))
            except Exception as exc:  # noqa: BLE001
                ctx.fetcher.fail("daily", "CFFEX", f"parse {day}", exc)
        if n % 12 == 0 or n == len(months):
            log.info("CFFEX: %d/%d months fetched", n, len(months))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def fetch_shfe_contracts(ctx: Ctx, days: list[str], codes: set[str], checkpoint: Path) -> pd.DataFrame:
    """SHFE per-contract rows via akshare get_shfe_daily (one request per trading day).
    Progress is checkpointed so an interrupted run resumes where it stopped."""
    frames: list[pd.DataFrame] = []
    done: set[str] = set()
    if checkpoint.exists():
        cp = pd.read_parquet(checkpoint)
        cp = cp[cp["trading_day"].isin(days)]
        frames.append(cp)
        done = set(cp["trading_day"])
        log.info("SHFE: resuming from checkpoint (%d days already fetched)", len(done))
    todo = [d for d in days if d not in done]
    t0 = time.monotonic()
    for n, day in enumerate(todo, 1):
        try:
            raw = ctx.fetcher.call("shfe", f"SHFE get_shfe_daily({day})", ak.get_shfe_daily, date=day)
        except FetchError as exc:
            ctx.fetcher.fail("daily", "SHFE", f"get_shfe_daily({day})", exc)
            continue
        if raw is None or raw.empty:
            log.warning("SHFE %s: no data returned", day)
            continue
        frames.append(normalize_shfe(raw, codes))
        if n % 50 == 0 or n == len(todo):
            eta = (time.monotonic() - t0) / n * (len(todo) - n) / 60
            log.info("SHFE: %d/%d days fetched (%s), ETA %.0f min", n, len(todo), day, eta)
            write_parquet(pd.concat(frames, ignore_index=True), checkpoint)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# --------------------------------------------------------------------------------------
# Sina data
# --------------------------------------------------------------------------------------


def sina_continuous(ctx: Ctx, p: Product) -> pd.DataFrame:
    """Sina main-continuous daily bars (akshare futures_zh_daily_sina), normalized.
    Rows on non-trading days (Sina artefacts, e.g. 20170102) are dropped; settle <= 0 -> NaN."""
    raw = ctx.fetcher.call(
        "sina", f"futures_zh_daily_sina({p.sina})", ak.futures_zh_daily_sina, symbol=p.sina
    )
    df = pd.DataFrame(
        {
            "trading_day": raw["date"].astype(str).str.replace("-", "", regex=False),
            "open": num(raw["open"]),
            "high": num(raw["high"]),
            "low": num(raw["low"]),
            "close": num(raw["close"]),
            "volume": num(raw["volume"]),
            "open_interest": num(raw["hold"]),
            "settle": num(raw["settle"]),
        }
    )
    df.loc[~(df["settle"] > 0), "settle"] = np.nan
    df = df[(df["trading_day"] >= ctx.fetch_start) & (df["trading_day"] <= ctx.end)]
    phantom = df[~df["trading_day"].isin(ctx.cal_set)]
    if len(phantom):
        log.info(
            "%s: dropped %d Sina rows on non-trading days: %s",
            p.sina,
            len(phantom),
            ",".join(phantom["trading_day"].tolist()),
        )
    df = df[df["trading_day"].isin(ctx.cal_set)]
    df = df.drop_duplicates("trading_day", keep="last").sort_values("trading_day")
    ctx.stats.setdefault(p.code, {})["sina_phantom_rows_dropped"] = phantom["trading_day"].tolist()
    return df.reset_index(drop=True)


def _sina_legacy_contract(symbol: str) -> pd.DataFrame:
    r = requests.get(
        SINA_LEGACY_URL, params={"symbol": symbol}, headers={**UA, "Referer": "https://finance.sina.com.cn/"}
    )
    r.raise_for_status()
    text = r.text.strip()
    cols = ["trading_day", "open", "high", "low", "close", "volume"]
    if not text or text == "null":
        return pd.DataFrame(columns=cols)
    data = json.loads(text)
    if not isinstance(data, list) or not data:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame([row[:6] for row in data], columns=cols)
    df["trading_day"] = df["trading_day"].astype(str).str.replace("-", "", regex=False)
    for c in cols[1:]:
        df[c] = num(df[c])
    return df


def legacy_symbols(p: Product, first_day: str, last_day: str) -> list[tuple[str, dt.date]]:
    """Contract symbols (e.g. I1605) whose listing window overlaps [first_day, last_day]."""
    first = dt.date(int(first_day[:4]), int(first_day[4:6]), 1)
    last = dt.date(int(last_day[:4]), int(last_day[4:6]), 1)
    out = []
    for year in range(first.year, last.year + 3):
        for month in p.months:
            deliv = dt.date(year, month, 1)
            if deliv < first or add_months(deliv, -14) > last:
                continue
            out.append((f"{p.code.upper()}{year % 100:02d}{month:02d}", deliv))
    return out


def fetch_legacy_contract(ctx: Ctx, p: Product, symbol: str, deliv: dt.date) -> pd.DataFrame:
    df = ctx.fetcher.call("sina", f"Sina legacy {symbol}", _sina_legacy_contract, symbol)
    lo = add_months(deliv, -14).strftime("%Y%m%d")
    hi = (add_months(deliv, 1) - dt.timedelta(days=1)).strftime("%Y%m%d")
    df = df[(df["trading_day"] >= lo) & (df["trading_day"] <= hi)].copy()  # guards symbol reuse
    df["instrument_id"] = symbol.lower()
    return df


# --------------------------------------------------------------------------------------
# Main contract selection, continuous series, contract matching
# --------------------------------------------------------------------------------------


def select_main(rows: pd.DataFrame) -> tuple[pd.Series, dict[str, int]]:
    """Main contract per trading day: the contract with the largest open interest on the
    previous trading day (docs/03 §5.2) among contracts trading today, refined so that the
    main never moves back to an earlier delivery month (prevents flip-flops)."""
    oi = rows.pivot(index="trading_day", columns="instrument_id", values="open_interest").astype(float)
    listed = rows.assign(_row=1.0).pivot(index="trading_day", columns="instrument_id", values="_row").notna()
    days = list(oi.index)
    main: dict[str, str] = {}
    prev: str | None = None
    overrides = 0
    for i, day in enumerate(days):
        cands = listed.columns[listed.loc[day].to_numpy()]
        ref = oi.loc[days[i - 1] if i else day, cands].dropna()
        ref = ref[ref > 0]
        if ref.empty:
            ref = oi.loc[day, cands].dropna()
        if ref.empty:
            continue
        best = str(ref.idxmax())
        if prev is not None and prev in cands and delivery_key(best) < delivery_key(prev):
            best = prev
            overrides += 1
        main[day] = best
        prev = best
    s = pd.Series(main, name="instrument_id")
    rolls = int((s != s.shift()).sum() - 1) if len(s) else 0
    return s, {"days": len(s), "rolls": rolls, "monotonic_overrides": overrides}


def match_contracts(cont: pd.DataFrame, per: pd.DataFrame) -> tuple[pd.Series, pd.Series, dict[str, int]]:
    """Contract behind each continuous row: the per-contract bar with identical OHLC and volume
    on the same day (pass 1), identical OHLC only (pass 2), or the only bar sharing at least three
    of the four prices (pass 3: Sina's two feeds sometimes differ by a tick or truncate a volume);
    unmatched rows enclosed by the same contract on both sides inherit it (pass 4). Returns
    (instrument, source) Series aligned with ``cont``: ``contract_bar`` for passes 1-3,
    ``inferred`` for pass 4."""

    def keyed(df: pd.DataFrame) -> pd.DataFrame:
        k = df[["trading_day", *PRICE_COLS, "volume"]].copy()
        for c in PRICE_COLS:
            k[c] = (k[c].astype(float) * 10000).round().astype("Int64")
        k["volume"] = k["volume"].astype(float).round().astype("Int64")
        return k

    a = keyed(cont).reset_index(names="_row")
    b = keyed(per)
    b["instrument_id"] = per["instrument_id"].to_numpy()
    b = b.dropna(subset=PRICE_COLS)
    result = pd.Series([None] * len(cont), index=cont.index, dtype=object)
    stats = {"strict": 0, "relaxed": 0, "near": 0, "enclosed": 0, "ambiguous": 0, "unmatched": 0}
    for keys, label in (
        (["trading_day", *PRICE_COLS, "volume"], "strict"),
        (["trading_day", *PRICE_COLS], "relaxed"),
    ):
        todo = a[a["_row"].map(result).isna()]
        if todo.empty:
            break
        m = todo.merge(b.drop_duplicates([*keys, "instrument_id"]), on=keys, how="inner", suffixes=("", "_b"))
        counts = m.groupby("_row")["instrument_id"].nunique()
        unique_rows = counts[counts == 1].index
        stats["ambiguous"] += int((counts > 1).sum()) if label == "relaxed" else 0
        pick = m[m["_row"].isin(unique_rows)].drop_duplicates("_row").set_index("_row")["instrument_id"]
        result.loc[pick.index] = pick.to_numpy()
        stats[label] += len(pick)
    todo = a[a["_row"].map(result).isna()]
    if not todo.empty:  # pass 3: >= 3 identical prices, or identical volume and >= 2 prices; unique
        m = todo.merge(b, on="trading_day", how="inner", suffixes=("", "_b"))
        same = sum((m[c] == m[f"{c}_b"]).fillna(False).astype(int) for c in PRICE_COLS)
        same_vol = (m["volume"] == m["volume_b"]).fillna(False)
        m = m[(same >= 3) | (same_vol & (same >= 2))]
        counts = m.groupby("_row")["instrument_id"].nunique()
        pick = (
            m[m["_row"].isin(counts[counts == 1].index)]
            .drop_duplicates("_row")
            .set_index("_row")["instrument_id"]
        )
        result.loc[pick.index] = pick.to_numpy()
        stats["near"] = len(pick)
        stats["ambiguous"] += int((counts > 1).sum())
    source = pd.Series(np.where(result.notna(), "contract_bar", None), index=cont.index, dtype=object)
    stats["enclosed"] = fill_enclosed(result, source)
    stats["unmatched"] = int(result.isna().sum())
    return result, source, stats


def fill_by_open_interest(cont: pd.DataFrame, inst: pd.Series, src: pd.Series) -> int:
    """In place: an unknown run between contract A (before) and B (after) is split where the
    open interest fits: each row goes to the contract whose adjacent known open interest is
    closer (OI is continuous within a contract and jumps at a switch); the switch to B is kept
    monotonic. Labelled ``inferred``."""
    filled = 0
    pos_unknown = np.flatnonzero(inst.isna().to_numpy())
    oi = cont["open_interest"].astype(float).to_numpy()
    idx = cont.index
    runs: list[list[int]] = []
    for pos in pos_unknown:
        if runs and runs[-1][-1] == pos - 1:
            runs[-1].append(pos)
        else:
            runs.append([pos])
    for run in runs:
        before, after = run[0] - 1, run[-1] + 1
        if before < 0 or after >= len(idx):
            continue
        a, b = inst.iloc[before], inst.iloc[after]
        if pd.isna(a) or pd.isna(b) or a == b or np.isnan(oi[before]) or np.isnan(oi[after]):
            continue
        switched = False
        for pos in run:
            if not switched and abs(oi[pos] - oi[after]) < abs(oi[pos] - oi[before]):
                switched = True
            inst.iloc[pos] = b if switched else a
            src.iloc[pos] = "inferred"
            filled += 1
    return filled


def fill_missing_days(ctx: Ctx, cont: pd.DataFrame, per: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Trading days missing from Sina's continuous series (e.g. 20190422) are rebuilt when the
    same contract is the main on both sides and the archive has its bar that day: settlement
    price cleared (estimated later), open interest interpolated, labelled ``gap_filled``."""
    if per.empty or cont.empty:
        return cont, []
    have = set(cont["trading_day"])
    first, last = cont["trading_day"].iloc[0], cont["trading_day"].iloc[-1]
    bars = per.set_index(["trading_day", "instrument_id"])
    days = cont["trading_day"].tolist()
    rows, filled = [], []
    for day in (d for d in ctx.cal if first < d < last and d not in have):
        k = bisect.bisect_left(days, day)
        before, after = cont.iloc[k - 1], cont.iloc[k]
        a = before["instrument_id"]
        if pd.isna(a) or a != after["instrument_id"] or (day, a) not in bars.index:
            continue
        bar = bars.loc[(day, a)]
        rows.append(
            {
                "trading_day": day,
                **{c: float(bar[c]) for c in (*PRICE_COLS, "volume")},
                "open_interest": round((float(before["open_interest"]) + float(after["open_interest"])) / 2),
                "settle": np.nan,
                "instrument_id": a,
                "instrument_source": "gap_filled",
            }
        )
        filled.append(day)
    if rows:
        cont = pd.concat([cont, pd.DataFrame(rows)], ignore_index=True).sort_values("trading_day")
        cont = cont.reset_index(drop=True)
    return cont, filled


def repair_excursions(cont: pd.DataFrame, per: pd.DataFrame, inst: pd.Series, src: pd.Series) -> list[str]:
    """In place: Sina's continuous series sometimes jumps for one or two days to another contract
    and back (A, X, A; typically on the first day after a holiday, e.g. i1509 instead of i1601 on
    2015-09-07). Such rows are rebuilt from contract A's bar in Sina's per-contract archive; the
    settlement price is cleared (estimated later) and the open interest interpolated. Labelled
    ``excursion_repaired``. Returns the repaired days."""
    repaired: list[str] = []
    values = inst.tolist()
    bars = per.set_index(["trading_day", "instrument_id"])
    n = len(values)
    for pos in range(1, n - 1):
        for width in (1, 2):
            end = pos + width
            if end >= n or pd.isna(values[pos - 1]) or values[pos - 1] != values[end]:
                continue
            run = values[pos:end]
            if any(pd.isna(v) or v == values[pos - 1] for v in run) or len(set(run)) != 1:
                continue
            a = values[pos - 1]
            for p in range(pos, end):
                day = cont["trading_day"].iloc[p]
                if (day, a) not in bars.index:
                    continue
                bar = bars.loc[(day, a)]
                row = cont.index[p]
                for c in (*PRICE_COLS, "volume"):
                    cont.at[row, c] = float(bar[c])
                lo_oi, hi_oi = cont["open_interest"].iloc[pos - 1], cont["open_interest"].iloc[end]
                cont.at[row, "open_interest"] = round((float(lo_oi) + float(hi_oi)) / 2)
                cont.at[row, "settle"] = np.nan
                inst.iloc[p], src.iloc[p] = a, "excursion_repaired"
                values[p] = a
                repaired.append(str(day))
            break
    return repaired


def fill_enclosed(inst: pd.Series, source: pd.Series) -> int:
    """In place: unknown rows enclosed by the same contract on both sides inherit it."""
    ffill, bfill = inst.ffill(), inst.bfill()
    enclosed = inst.isna() & ffill.notna() & bfill.notna() & (ffill.fillna("") == bfill.fillna(""))
    inst[enclosed] = ffill[enclosed]
    source[enclosed] = "inferred"
    return int(enclosed.sum())


def changed_from_prev(s: pd.Series) -> pd.Series:
    """True where the value is known, the previous value is known, and they differ."""
    prev = s.shift(1)
    return (s.notna() & prev.notna() & (s.fillna("") != prev.fillna(""))).astype(bool)


def same_as_prev(s: pd.Series) -> pd.Series:
    """True where the value is known and equal to the previous value."""
    prev = s.shift(1)
    return (s.notna() & prev.notna() & (s.fillna("") == prev.fillna(""))).astype(bool)


def finish_continuous(c: pd.DataFrame) -> pd.DataFrame:
    """Add pre_settle (previous row's settle; first row: the contract's own pre-settle) and the
    roll flag (first row whose contract differs from the last known contract before it)."""
    c = c.sort_values("trading_day").reset_index(drop=True)
    c["pre_settle"] = c["settle"].shift(1)
    if len(c) and pd.isna(c.loc[0, "pre_settle"]):
        c.loc[0, "pre_settle"] = c.loc[0, "contract_pre_settle"]
    inst = c["instrument_id"]
    prev_known = inst.shift(1).ffill()
    c["roll"] = (inst.notna() & prev_known.notna() & (inst.fillna("") != prev_known.fillna(""))).astype(bool)
    return c


FLOAT_COLS = {"open", "high", "low", "close", "settle", "pre_settle", "contract_pre_settle", "turnover"}


def to_output(df: pd.DataFrame, columns: list[str], int_cols: Iterable[str]) -> pd.DataFrame:
    """Select/order output columns with stable dtypes (float64 prices, nullable Int64 counts)."""
    out = df.reindex(columns=columns).copy()
    for c in columns:
        if c in FLOAT_COLS:
            out[c] = num(out[c]).astype("float64")
    for c in int_cols:
        out[c] = num(out[c]).round().astype("Int64")
    return out


# --------------------------------------------------------------------------------------
# Layer A: daily
# --------------------------------------------------------------------------------------


def daily_exchange_group(ctx: Ctx, exchange: str, requested: list[Product]) -> None:
    """SHFE/CFFEX: exchange per-contract data -> per-contract files -> continuous series.
    The exchange data is fetched once for all tracked products of the exchange."""
    tracked = [p for p in PRODUCTS.values() if p.exchange == exchange]

    def have(p: Product) -> bool:
        return ctx.cont_path(p).exists() and ctx.per_path(p).exists()

    if not ctx.force and not ctx.update and all(have(p) for p in requested):
        log.info("%s daily: outputs exist, skipping (use --force or --update)", exchange)
        return
    codes = {p.code for p in tracked}
    existing = pd.DataFrame()
    fetch_from = ctx.fetch_start
    if ctx.update and not ctx.force and all(have(p) for p in tracked):
        parts = []
        for p in tracked:
            df = pd.read_parquet(ctx.per_path(p))
            df["product"], df["exchange"] = p.code, exchange
            parts.append(df)
        existing = pd.concat(parts, ignore_index=True)
        fetch_from = next_trading_day(ctx.cal, str(existing["trading_day"].max())) or ctx.end
    days = [d for d in ctx.cal if fetch_from <= d <= ctx.end]
    log.info(
        "%s daily: fetching %d trading days %s..%s for %s",
        exchange,
        len(days),
        days[0] if days else "-",
        days[-1] if days else "-",
        ",".join(sorted(codes)),
    )
    checkpoint = ctx.out / "daily" / exchange / ".checkpoint.parquet"
    if exchange == "SHFE":
        new = fetch_shfe_contracts(ctx, days, codes, checkpoint) if days else pd.DataFrame()
    else:
        new = fetch_cffex_contracts(ctx, days, codes) if days else pd.DataFrame()
    rows = pd.concat([existing, new], ignore_index=True)
    if rows.empty:
        for p in tracked:
            ctx.fetcher.fail("daily", p.code, f"{exchange} per-contract data", "no rows fetched")
        return
    rows = rows.drop_duplicates(["trading_day", "instrument_id"], keep="last")
    ok = True
    for p in tracked:
        prows = rows[rows["product"] == p.code]
        if prows.empty:
            ctx.fetcher.fail("daily", p.code, f"{exchange} per-contract data", "no rows for product")
            ok = False
            continue
        try:
            build_exchange_product(ctx, p, prows)
        except Exception as exc:  # noqa: BLE001
            ctx.fetcher.fail("daily", p.code, "build daily outputs", exc)
            ok = False
    if ok and checkpoint.exists():
        checkpoint.unlink()


def build_exchange_product(ctx: Ctx, p: Product, rows: pd.DataFrame) -> None:
    rows = rows.sort_values(["trading_day", "instrument_id"]).reset_index(drop=True)
    main, diag = select_main(rows)
    key = pd.DataFrame({"trading_day": main.index, "instrument_id": main.to_numpy()})
    cont = key.merge(rows, on=["trading_day", "instrument_id"], how="left")
    cont["contract_pre_settle"] = cont["pre_settle"]
    cont["settle_source"] = "exchange"
    cont["instrument_source"] = "exchange"
    cont = finish_continuous(cont)
    # Cross-reference with Sina's own main-continuous series (used to label minute bars).
    cont["sina_instrument_id"] = None
    try:
        sina = sina_continuous(ctx, p)
        inst, _src, mstats = match_contracts(sina, rows)
        sina["sina_instrument_id"] = inst.to_numpy()
        cont = cont.drop(columns="sina_instrument_id").merge(
            sina[["trading_day", "sina_instrument_id", "settle"]].rename(columns={"settle": "_sina_settle"}),
            on="trading_day",
            how="left",
        )
        both = cont["sina_instrument_id"].notna()
        same = both & (cont["sina_instrument_id"].fillna("") == cont["instrument_id"].fillna(""))
        comparable = same & cont["_sina_settle"].notna()
        diag.update(
            {
                "sina_rows": len(sina),
                "sina_match": mstats,
                "sina_main_agree_days": int(same.sum()),
                "sina_main_compared_days": int(both.sum()),
                "sina_settle_equal": int(
                    (comparable & (cont["_sina_settle"] - cont["settle"]).abs().lt(1e-6)).sum()
                ),
                "sina_settle_compared": int(comparable.sum()),
            }
        )
        cont = cont.drop(columns="_sina_settle")
    except FetchError as exc:
        ctx.fetcher.fail("daily", p.code, "Sina cross-reference (non-fatal)", exc)
    cont = cont[cont["trading_day"] >= ctx.start]
    per = rows[rows["trading_day"] >= ctx.start].copy()
    main_keys = set(zip(key["trading_day"], key["instrument_id"], strict=True))
    per["is_main"] = [
        (d, i) in main_keys for d, i in zip(per["trading_day"], per["instrument_id"], strict=True)
    ]
    per["settle_source"] = "exchange"
    write_parquet(to_output(per, PER_COLUMNS, ["volume", "open_interest"]), ctx.per_path(p))
    write_parquet(to_output(cont, CONT_COLUMNS, ["volume", "open_interest"]), ctx.cont_path(p))
    diag["rolls_since_start"] = int(cont["roll"].sum())
    ctx.stats.setdefault(p.code, {}).update({"source": p.exchange.lower(), "main_selection": diag})
    log.info(
        "%s: %d continuous rows %s..%s, %d rolls; per-contract %d rows",
        p.code,
        len(cont),
        cont["trading_day"].min(),
        cont["trading_day"].max(),
        diag["rolls_since_start"],
        len(per),
    )


def calibrate_settle_proxy(df: pd.DataFrame, tick: float) -> tuple[str, dict[str, float]]:
    """Pick the daily-bar proxy that best reproduces the published settlement price on rows
    where it is known (the exchange settle is a full-day VWAP for DCE)."""
    known = df[df["settle"].notna()]
    proxies = {
        "close": known["close"],
        "hlc3": (known["high"] + known["low"] + known["close"]) / 3,
        "ohlc4": (known["open"] + known["high"] + known["low"] + known["close"]) / 4,
        "hl2": (known["high"] + known["low"]) / 2,
    }
    scores = {}
    for name, v in proxies.items():
        err = (round_tick(v, tick) - known["settle"]).abs()
        scores[name] = {
            "mae_ticks": float(err.mean() / tick),
            "p95_ticks": float(err.quantile(0.95) / tick),
            "mae_bp": float((err / known["settle"]).mean() * 1e4),
        }
    best = min(scores, key=lambda k: scores[k]["mae_ticks"])
    return best, {
        "n_known": len(known),
        **{f"{k}_{m}": round(v, 3) for k, s in scores.items() for m, v in s.items()},
    }


def settle_proxy(df: pd.DataFrame, name: str) -> pd.Series:
    if name == "close":
        return df["close"]
    if name == "hlc3":
        return (df["high"] + df["low"] + df["close"]) / 3
    if name == "ohlc4":
        return (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    return (df["high"] + df["low"]) / 2


def _to_float(x: Any) -> float:
    try:
        return float(str(x).replace(",", "").strip())
    except ValueError:
        return float("nan")


def _ppi_day(day: str) -> pd.DataFrame:
    """100ppi.com page of one day: near and dominant contract of each DCE product with their
    closing prices. akshare futures_spot_price reads the same page but can loop forever when the
    page of a day is missing, hence this minimal parser (akshare's name-to-code helper is reused)."""
    r = requests.get(
        PPI_DAY_URL.format(y=day[:4], m=day[4:6], d=day[6:]),
        headers={**UA, "Accept": "text/html,application/xhtml+xml"},
    )
    r.raise_for_status()
    r.encoding = "utf-8"
    tables = pd.read_html(io.StringIO(r.text))
    found = re.search(r"(\d{4})年(\d{2})月(\d{2})日", str(tables[0].iloc[1, 1])) if tables else None
    if not found or "".join(found.groups()) != day:
        return pd.DataFrame()  # the site serves another day's page when a day is missing
    wanted = {p.code.upper(): p.code for p in PRODUCTS.values() if p.exchange == "DCE"}
    rows = []
    for rec in tables[1].itertuples(index=False):
        name = "".join(re.findall(r"[一-龥]", str(rec[0])))
        try:
            code = str(chinese_to_english(name) or "").upper()
        except Exception:  # noqa: BLE001 - header and exchange-name rows
            continue
        if code not in wanted:
            continue

        def contract(x: Any, product: str = wanted[code]) -> str | None:
            s = re.sub(r"\.0$", "", str(x).strip())
            return product + s if re.fullmatch(r"\d{4}", s) else None

        rows.append(
            {
                "product": wanted[code],
                "near_contract": contract(rec[2]),
                "near_price": _to_float(rec[3]),
                "dominant_contract": contract(rec[5]),
                "dominant_price": _to_float(rec[6]),
            }
        )
    return pd.DataFrame(rows)


def fetch_ppi(ctx: Ctx, day: str) -> pd.DataFrame:
    """100ppi page of ``day`` (cached: one request serves all DCE products)."""
    if day not in ctx.ppi_cache:
        try:
            ctx.ppi_cache[day] = ctx.fetcher.call("100ppi", f"100ppi {day}", _ppi_day, day)
        except FetchError as exc:
            ctx.fetcher.fail("daily", "DCE", f"100ppi {day} (non-fatal)", exc)
            ctx.ppi_cache[day] = pd.DataFrame()
    return ctx.ppi_cache[day]


def resolve_with_ppi(
    ctx: Ctx, p: Product, cont: pd.DataFrame, inst: pd.Series, src: pd.Series, after: str
) -> dict[str, int]:
    """In place: identify rows after ``after`` (the last day of Sina's per-contract archive,
    2024-07-17). A row is on the contract whose 100ppi closing price (near or dominant contract)
    equals Sina's close. A row still unknown between contract A (before) and B (after) is on A
    when 100ppi shows B at a different price than Sina's close, and on B in the mirror case.
    Labels that would move the main back to an earlier delivery month are rejected; enclosed
    gaps are then filled."""
    stats = {"ppi_days": 0, "ppi_matched": 0, "two_candidate": 0, "rejected_non_monotonic": 0}
    todo = list(cont.index[inst.isna() & (cont["trading_day"] > after)])
    obs: dict[Any, list[tuple[str, float]]] = {}
    for n, idx in enumerate(todo, 1):
        table = fetch_ppi(ctx, str(cont.at[idx, "trading_day"]))
        stats["ppi_days"] += 1
        rec = table[table["product"] == p.code] if not table.empty else table
        if n % 100 == 0:
            log.info("%s: 100ppi %d/%d days", p.code, n, len(todo))
        if rec.empty:
            continue
        r0 = rec.iloc[0]
        pairs = [
            (str(c), float(px))
            for c, px in (
                (r0["dominant_contract"], r0["dominant_price"]),
                (r0["near_contract"], r0["near_price"]),
            )
            if c and pd.notna(px)
        ]
        obs[idx] = pairs
        close = float(cont.at[idx, "close"])
        hits = {c for c, px in pairs if abs(px - close) < 1e-6}
        if len(hits) == 1:
            inst[idx] = hits.pop()
            src[idx] = "100ppi"
            stats["ppi_matched"] += 1
    ffill, bfill = inst.ffill(), inst.bfill()
    for idx in todo:
        if pd.notna(inst[idx]):
            continue
        a, b = ffill[idx], bfill[idx]
        if pd.isna(a) or pd.isna(b) or a == b:
            continue
        close = float(cont.at[idx, "close"])
        verdict = set()
        for c, px in obs.get(idx, []):
            if abs(px - close) > 1e-6:
                if c == b:
                    verdict.add(a)
                elif c == a:
                    verdict.add(b)
        if len(verdict) == 1:
            inst[idx] = verdict.pop()
            src[idx] = "inferred"
            stats["two_candidate"] += 1
    max_seen = -1
    for idx in cont.index:
        v = inst[idx]
        if pd.isna(v):
            continue
        k = delivery_key(str(v))
        if k < max_seen and src[idx] != "contract_bar":
            inst[idx], src[idx] = None, None
            stats["rejected_non_monotonic"] += 1
            continue
        max_seen = max(max_seen, k)
    stats["enclosed"] = fill_enclosed(inst, src)
    return stats


def daily_dce(ctx: Ctx, p: Product) -> None:
    """DCE: Sina main-continuous series (dce.com.cn answers scripted requests with HTTP 412).
    The contract behind each row is identified by a bar match against Sina's legacy per-contract
    archive (complete until 2024-07-17), then by 100ppi's daily contract prices, then inferred
    (enclosure, two candidates, open-interest continuity). Sina's one/two-day excursions to
    another contract and days missing from its series are rebuilt from the archive bar of the
    main contract (estimated settle, interpolated open interest); missing Sina settlement prices
    are estimated with a proxy calibrated on the known ones. All repairs are flagged."""
    if not ctx.force and not ctx.update and ctx.cont_path(p).exists() and ctx.per_path(p).exists():
        log.info("%s daily: outputs exist, skipping (use --force or --update)", p.code)
        return
    sina = sina_continuous(ctx, p)
    if sina.empty:
        ctx.fetcher.fail("daily", p.code, "Sina continuous", "no rows")
        return
    # --update: reuse the immutable history instead of refetching it. Per-contract rows up to
    # the archive's last day are raw archive bars; identifications made after that day (100ppi,
    # inferred) are taken from the previous continuous file.
    cached: dict[str, pd.DataFrame] = {}
    prev_ids: dict[str, tuple[str, str]] = {}
    old_end = ""
    if ctx.update and not ctx.force and ctx.per_path(p).exists():
        old = pd.read_parquet(ctx.per_path(p))
        old_end = str(old.loc[~old["is_main"], "trading_day"].max())  # non-main rows = archive rows
        old = old[old["trading_day"] <= old_end]
        for inst_id, g in old.groupby("instrument_id"):
            cached[str(inst_id)] = g[["trading_day", *PRICE_COLS, "volume", "instrument_id"]]
    if ctx.update and not ctx.force and ctx.cont_path(p).exists():
        oc = pd.read_parquet(ctx.cont_path(p))
        oc = oc[oc["instrument_source"].isin(["100ppi", "inferred"])]
        prev_ids = {
            d: (i, s)
            for d, i, s in zip(oc["trading_day"], oc["instrument_id"], oc["instrument_source"], strict=True)
        }
    this_month = dt.date(int(ctx.end[:4]), int(ctx.end[4:6]), 1)
    symbols = legacy_symbols(p, sina["trading_day"].min(), sina["trading_day"].max())
    fetched: dict[str, pd.DataFrame] = {}

    def get(symbol: str, deliv: dt.date) -> None:
        inst_id = symbol.lower()
        if inst_id in cached and deliv < add_months(this_month, -1):  # expired: history is immutable
            fetched[symbol] = cached[inst_id]
            return
        try:
            fetched[symbol] = fetch_legacy_contract(ctx, p, symbol, deliv)
        except FetchError as exc:
            ctx.fetcher.fail("daily", p.code, f"Sina legacy {symbol} (non-fatal)", exc)

    for symbol, deliv in symbols:
        if deliv.month in p.main_months:
            get(symbol, deliv)
    per = pd.concat(fetched.values(), ignore_index=True) if fetched else pd.DataFrame()
    empty = pd.Series([None] * len(sina), index=sina.index, dtype=object)
    inst, src, mstats = match_contracts(sina, per) if not per.empty else (empty.copy(), empty.copy(), {})
    legacy_end = str(per["trading_day"].max()) if not per.empty else ""
    # Rows without a bar match (even if an enclosure filled them: Sina's series sometimes jumps
    # to another contract for a single day) trigger a fetch of the other delivery months.
    missing_days = {d for d in sina.loc[src != "contract_bar", "trading_day"] if d <= legacy_end}
    if missing_days:  # second pass: other delivery months covering the unmatched days
        for symbol, deliv in symbols:
            if symbol in fetched:
                continue
            lo = add_months(deliv, -14).strftime("%Y%m%d")
            hi = add_months(deliv, 1).strftime("%Y%m%d")
            if any(lo <= d < hi for d in missing_days):
                get(symbol, deliv)
        per = pd.concat(fetched.values(), ignore_index=True)
        inst, src, mstats = match_contracts(sina, per)
    if not per.empty:  # the archive also has rows on non-trading days (like the continuous series)
        per = per.drop_duplicates(["trading_day", "instrument_id"], keep="last")
        per = per[per["trading_day"].isin(ctx.cal_set)]
    reused = 0
    for idx in sina.index[inst.isna() & (sina["trading_day"] > legacy_end)]:
        hit = prev_ids.get(str(sina.at[idx, "trading_day"]))
        if hit:
            inst[idx], src[idx] = hit
            reused += 1
    pstats = resolve_with_ppi(ctx, p, sina, inst, src, after=legacy_end)
    oi_filled = fill_by_open_interest(sina, inst, src)
    repaired = repair_excursions(sina, per, inst, src) if not per.empty else []
    sina["instrument_id"] = inst.to_numpy()
    sina["instrument_source"] = src.to_numpy()
    sina, gap_filled = fill_missing_days(ctx, sina, per)
    # Settlement price: Sina's value; where Sina has none, a calibrated proxy (flagged).
    proxy, cal_stats = calibrate_settle_proxy(sina, p.tick)
    missing = sina["settle"].isna()
    sina["settle_source"] = np.where(missing, "estimated", "sina")
    sina.loc[missing, "settle"] = round_tick(settle_proxy(sina.loc[missing], proxy), p.tick)
    sina["contract_pre_settle"] = sina["settle"].shift(1).where(same_as_prev(sina["instrument_id"]))
    sina["sina_instrument_id"] = sina["instrument_id"]
    cont = finish_continuous(sina)
    cont = cont[cont["trading_day"] >= ctx.start]
    # Per-contract file: the raw archive bars of every contract (OHLCV; kept raw so that --update
    # can reuse them), with settle, pre-settle and open interest from the continuous series on
    # the main contract's rows; main-contract days without an archive bar (after 2024-07-17)
    # take the continuous bar.
    main_rows = cont[cont["instrument_id"].notna()].copy()
    main_rows["pre_settle"] = main_rows["contract_pre_settle"]
    main_rows["is_main"] = True
    raw = per[per["trading_day"] >= ctx.start].copy() if not per.empty else pd.DataFrame(columns=PER_COLUMNS)
    info = main_rows[
        ["trading_day", "instrument_id", "settle", "pre_settle", "open_interest", "settle_source", "is_main"]
    ]
    raw = raw.drop(
        columns=[c for c in info.columns if c in raw.columns and c not in ("trading_day", "instrument_id")]
    )
    raw = raw.merge(info, on=["trading_day", "instrument_id"], how="left")
    raw["is_main"] = raw["is_main"].fillna(False).astype(bool)
    have = set(zip(raw["trading_day"], raw["instrument_id"], strict=True))
    extra = main_rows[
        [
            (d, i) not in have
            for d, i in zip(main_rows["trading_day"], main_rows["instrument_id"], strict=True)
        ]
    ]
    per_out = pd.concat([raw, extra], ignore_index=True).sort_values(["trading_day", "instrument_id"])
    write_parquet(to_output(per_out, PER_COLUMNS, ["volume", "open_interest"]), ctx.per_path(p))
    write_parquet(to_output(cont, CONT_COLUMNS, ["volume", "open_interest"]), ctx.cont_path(p))
    n_est = int((cont["settle_source"] == "estimated").sum())
    src_counts = {str(k): int(v) for k, v in cont["instrument_source"].value_counts(dropna=False).items()}
    ctx.stats.setdefault(p.code, {}).update(
        {
            "source": "sina",
            "contract_match": mstats,
            "legacy_archive_last_day": legacy_end,
            "legacy_contracts_fetched": len(fetched),
            "reused_identifications": reused,
            "ppi": pstats,
            "open_interest_filled": oi_filled,
            "excursions_repaired": repaired,
            "gap_filled_days": gap_filled,
            "instrument_source_counts": src_counts,
            "settle_proxy": proxy,
            "settle_proxy_calibration": cal_stats,
            "settle_estimated_rows": n_est,
            "rolls_since_start": int(cont["roll"].sum()),
        }
    )
    log.info(
        "%s: %d continuous rows %s..%s, %d rolls, %d estimated settles (proxy %s); sources %s",
        p.code,
        len(cont),
        cont["trading_day"].min(),
        cont["trading_day"].max(),
        int(cont["roll"].sum()),
        n_est,
        proxy,
        src_counts,
    )


def run_daily(ctx: Ctx, products: list[Product]) -> None:
    for exchange in ("SHFE", "CFFEX"):
        group = [p for p in products if p.exchange == exchange]
        if group:
            try:
                daily_exchange_group(ctx, exchange, group)
            except Exception as exc:  # noqa: BLE001
                ctx.fetcher.fail("daily", exchange, "exchange group", exc)
    for p in products:
        if p.exchange == "DCE":
            try:
                daily_dce(ctx, p)
            except Exception as exc:  # noqa: BLE001
                ctx.fetcher.fail("daily", p.code, "DCE daily", exc)


# --------------------------------------------------------------------------------------
# Layer B: minute bars
# --------------------------------------------------------------------------------------


def minute_trading_days(t: pd.Series, cal: list[str]) -> pd.Series:
    """Trading day of each bar; ``t`` = bar end time (Beijing, naive). Night-session bars
    (end >= 18:00, or end < 04:00 after midnight) belong to the next trading day; a day-time
    label on a non-trading date (Sina's Saturday remainder bars) also maps to the next one."""
    hhmm = t.dt.hour * 100 + t.dt.minute
    base = t.dt.normalize().where(hhmm >= 400, t.dt.normalize() - pd.Timedelta(days=1))
    base_s = base.dt.strftime("%Y%m%d")
    night = (hhmm >= 1800) | (hhmm < 400)
    cal_set = set(cal)
    memo: dict[tuple[str, bool], str | None] = {}
    out = []
    for b, n in zip(base_s, night, strict=True):
        k = (b, bool(n))
        if k not in memo:
            memo[k] = next_trading_day(cal, b) if (n or b not in cal_set) else b
        out.append(memo[k])
    return pd.Series(out, index=t.index, dtype=object)


def normalize_minute(raw: pd.DataFrame, cal: list[str]) -> pd.DataFrame:
    """Sina minute bars -> normalized bars. Sina labels a bar with its END time and aggregates
    by trading minutes, so a bar may span a session break. Two Sina quirks are repaired:
    * the bar ending at 00:00 after a Friday night session is labelled with the next trading
      day's date (Monday 00:00); it is moved back to Saturday 00:00 (quality
      ``midnight_relabelled``);
    * after a Friday night session Sina emits remainder bars with day-time labels on Saturday
      (e.g. 09:30); empty ones are dropped, the rest are kept (quality ``offsession_label``)."""
    t = pd.to_datetime(raw["datetime"], format="%Y-%m-%d %H:%M:%S").copy()
    cal_set = set(cal)
    quality = pd.Series([None] * len(t), index=t.index, dtype=object)
    prev_date = (t - pd.Timedelta(days=1)).dt.strftime("%Y%m%d")
    midnight = (t.dt.hour == 0) & (t.dt.minute == 0) & ~prev_date.isin(cal_set)
    for idx in t.index[midnight]:
        ptd = prev_trading_day(cal, t[idx].strftime("%Y%m%d"))
        if ptd:
            t[idx] = pd.Timestamp(ptd) + pd.Timedelta(days=1)
            quality[idx] = "midnight_relabelled"
    hhmm = t.dt.hour * 100 + t.dt.minute
    offsession = (hhmm >= 400) & (hhmm < 1800) & ~t.dt.strftime("%Y%m%d").isin(cal_set)
    quality[offsession] = "offsession_label"
    utc_ns = t.dt.tz_localize(BJ_TZ).dt.tz_convert("UTC").dt.as_unit("ns").astype("int64")
    df = pd.DataFrame(
        {
            "ts": t.dt.strftime("%Y-%m-%dT%H:%M:%S+08:00"),
            "ts_ns": utc_ns,
            "trading_day": minute_trading_days(t, cal),
            "open": num(raw["open"]),
            "high": num(raw["high"]),
            "low": num(raw["low"]),
            "close": num(raw["close"]),
            "volume": num(raw["volume"]),
            "open_interest": num(raw["hold"]),
            "quality": quality,
        }
    )
    df = df[~(offsession & (df["volume"] == 0))]
    # A relabelled bar never replaces a bar that Sina labelled correctly.
    df = df.sort_values(["ts_ns", "quality"], na_position="first").drop_duplicates("ts_ns", keep="first")
    # A bar is labelled with its end time; drop a bar that has not closed yet.
    now_ns = pd.Timestamp.now(tz="UTC").value
    return df[df["ts_ns"] <= now_ns].reset_index(drop=True)


def annotate_minute(ctx: Ctx, p: Product, bars: pd.DataFrame) -> pd.DataFrame:
    """instrument_id = Sina's main contract of the bar's trading day (from the daily layer);
    roll = first bar whose contract differs from the previous bar's."""
    bars = bars.sort_values("ts_ns").reset_index(drop=True)
    mapping: dict[str, Any] = {}
    if ctx.cont_path(p).exists():
        d = pd.read_parquet(ctx.cont_path(p), columns=["trading_day", "sina_instrument_id"])
        mapping = dict(zip(d["trading_day"], d["sina_instrument_id"], strict=True))
    else:
        log.warning("%s minute: daily file missing, instrument_id left empty", p.code)
    bars["instrument_id"] = pd.Series([mapping.get(td) for td in bars["trading_day"]], dtype=object)
    bars["roll"] = changed_from_prev(bars["instrument_id"])
    return bars


def run_minute(ctx: Ctx, products: list[Product], periods: list[str]) -> None:
    for p in products:
        for period in periods:
            path = ctx.minute_path(p, period)
            if path.exists() and not ctx.force and not ctx.update:
                log.info("%s %sm: exists, skipping", p.code, period)
                continue
            try:
                raw = ctx.fetcher.call(
                    "sina",
                    f"futures_zh_minute_sina({p.sina},{period})",
                    ak.futures_zh_minute_sina,
                    symbol=p.sina,
                    period=period,
                )
                bars = normalize_minute(raw, ctx.cal)
                if ctx.update and not ctx.force and path.exists():
                    old = pd.read_parquet(path).reindex(columns=bars.columns)
                    bars = pd.concat([old, bars], ignore_index=True).drop_duplicates("ts_ns", keep="last")
                bars = annotate_minute(ctx, p, bars)
                write_parquet(to_output(bars, MINUTE_COLUMNS, ["volume", "open_interest"]), path)
                log.info(
                    "%s %sm: %d bars %s..%s",
                    p.code,
                    period,
                    len(bars),
                    bars["ts"].iloc[0],
                    bars["ts"].iloc[-1],
                )
            except Exception as exc:  # noqa: BLE001
                ctx.fetcher.fail("minute", p.code, f"{p.sina} {period}m", exc)


# --------------------------------------------------------------------------------------
# Official per-contract data of the last N trading days
# --------------------------------------------------------------------------------------


def run_official(ctx: Ctx, n_days: int) -> None:
    """Official per-contract data of the last ``n_days`` trading days (SHFE, CFFEX). DCE is not
    attempted: dce.com.cn answers scripted requests (akshare get_dce_daily) with HTTP 412 and a
    JavaScript challenge (checked 2026-09-25)."""
    days = [d for d in ctx.cal if d <= ctx.end][-n_days:]
    for exchange in ("SHFE", "CFFEX"):
        path = ctx.out / "official" / f"{exchange}.parquet"
        if path.exists() and not ctx.force and not ctx.update:
            log.info("official %s: exists, skipping", exchange)
            continue
        old = pd.read_parquet(path) if (ctx.update and not ctx.force and path.exists()) else pd.DataFrame()
        have = set(old["trading_day"]) if not old.empty else set()
        frames = [old] if not old.empty else []
        for day in [d for d in days if d not in have]:
            try:
                if exchange == "SHFE":
                    raw = ctx.fetcher.call("shfe", f"get_shfe_daily({day})", ak.get_shfe_daily, date=day)
                    df = normalize_shfe(raw, None) if raw is not None and not raw.empty else pd.DataFrame()
                else:
                    content = ctx.fetcher.call("cffex", f"CFFEX csv {day}", _cffex_day_csv, day)
                    df = parse_cffex_csv(content, day, None) if content else pd.DataFrame()
            except FetchError as exc:
                ctx.fetcher.fail("official", exchange, f"{day}", exc)
                continue
            if df.empty:
                log.warning("official %s %s: no data", exchange, day)
                continue
            frames.append(df)
        if frames:
            data = pd.concat(frames, ignore_index=True).sort_values(["trading_day", "instrument_id"])
            write_parquet(to_output(data, OFFICIAL_COLUMNS, ["volume", "open_interest"]), path)
            log.info("official %s: %d rows, days %s", exchange, len(data), sorted(set(data["trading_day"])))


# --------------------------------------------------------------------------------------
# Verification + manifest
# --------------------------------------------------------------------------------------


def _floor_tick(x: pd.Series, tick: float) -> pd.Series:
    return (np.floor(x / tick + 1e-9) * tick).round(6)


def verify(ctx: Ctx) -> dict[str, Any]:
    """Check every output file and build the manifest content."""
    report: dict[str, Any] = {
        "daily": {},
        "contracts": {},
        "minute": {},
        "official": {},
        "stress_windows": [],
    }
    key_cols = ["open", "high", "low", "close", "settle", "volume", "open_interest"]
    conts: dict[str, pd.DataFrame] = {}
    for p in PRODUCTS.values():
        path = ctx.cont_path(p)
        if not path.exists():
            continue
        c = pd.read_parquet(path)
        conts[p.code] = c
        first, last = c["trading_day"].min(), c["trading_day"].max()
        expected = [d for d in ctx.cal if first <= d <= last]
        missing = sorted(set(expected) - set(c["trading_day"]))
        inst = c["instrument_id"].fillna("")
        excursion = (
            (inst != "") & (inst != inst.shift(1)) & (inst.shift(1) == inst.shift(-1)) & (inst.shift(1) != "")
        )
        report["daily"][p.code] = {
            # single-day switch to another contract and back (A, X, A): a quirk of Sina's series
            "excursions": c.loc[excursion, "trading_day"].tolist(),
            "rows": len(c),
            "first": first,
            "last": last,
            "nan_key_cols": {k: int(c[k].isna().sum()) for k in key_cols if c[k].isna().any()},
            "nan_pre_settle": int(c["pre_settle"].isna().sum()),
            "nan_contract_pre_settle": int(c["contract_pre_settle"].isna().sum()),
            "settle_nonpositive": int((c["settle"] <= 0).sum()),
            "settle_source": {str(k): int(v) for k, v in c["settle_source"].value_counts().items()},
            "instrument_source": {
                str(k): int(v) for k, v in c["instrument_source"].value_counts(dropna=False).items()
            }
            if "instrument_source" in c
            else {},
            "rolls": int(c["roll"].sum()),
            "unknown_instrument": int(c["instrument_id"].isna().sum()),
            "duplicate_days": int(c["trading_day"].duplicated().sum()),
            "missing_trading_days": missing[:20],
            "missing_trading_days_count": len(missing),
            "ohlc_inconsistent": int(
                (
                    (c["high"] < c[["open", "close"]].max(axis=1))
                    | (c["low"] > c[["open", "close"]].min(axis=1))
                ).sum()
            ),
        }
        per_path = ctx.per_path(p)
        if per_path.exists():
            q = pd.read_parquet(per_path)
            mains = q.groupby("trading_day")["is_main"].sum()
            report["contracts"][p.code] = {
                "file": per_path.relative_to(ctx.out).as_posix(),
                "rows": len(q),
                "contracts": int(q["instrument_id"].nunique()),
                "first": q["trading_day"].min(),
                "last": q["trading_day"].max(),
                "days_with_one_main": int((mains == 1).sum()),
                "days": int(len(mains)),
                "settle_nan_rows": int(q["settle"].isna().sum()),
            }
    for p in PRODUCTS.values():
        for period in DEFAULT_PERIODS:
            path = ctx.minute_path(p, period)
            if not path.exists():
                continue
            b = pd.read_parquet(path)
            entry: dict[str, Any] = {
                "rows": len(b),
                "first": b["ts"].iloc[0] if len(b) else None,
                "last": b["ts"].iloc[-1] if len(b) else None,
                "trading_days": int(b["trading_day"].nunique()),
                "nan": {
                    k: int(b[k].isna().sum())
                    for k in ["open", "high", "low", "close", "volume", "open_interest", "trading_day"]
                    if b[k].isna().any()
                },
                "unknown_instrument": int(b["instrument_id"].isna().sum()),
                "rolls": int(b["roll"].sum()),
                "zero_volume_bars": int((b["volume"] == 0).sum()),
                "quality": {str(k): int(v) for k, v in b["quality"].value_counts().items()}
                if "quality" in b
                else {},
            }
            c = conts.get(p.code)
            if c is not None and len(b):
                # Complete trading days only (the first day in the file may be truncated).
                g = (
                    b[b["trading_day"] != b["trading_day"].iloc[0]]
                    .groupby("trading_day")
                    .agg(high=("high", "max"), low=("low", "min"), volume=("volume", "sum"))
                )
                d = c.set_index("trading_day")
                d = d[
                    d["sina_instrument_id"].notna()
                    & (d["sina_instrument_id"].fillna("") == d["instrument_id"].fillna(""))
                ]
                j = g.join(d[["high", "low", "volume"]], rsuffix="_d", how="inner")
                ratio = j["volume"].astype(float) / j["volume_d"].astype(float)
                near = ((j["high"] - j["high_d"]).abs() <= p.tick + 1e-9) & (
                    (j["low"] - j["low_d"]).abs() <= p.tick + 1e-9
                )
                entry["daily_consistency"] = {  # Sina minute bars vs the exchange/Sina daily bar
                    "days_compared": len(j),
                    "volume_ratio_median": round(float(ratio.median()), 3) if len(j) else None,
                    "volume_ratio_p10": round(float(ratio.quantile(0.1)), 3) if len(j) else None,
                    "days_volume_below_half": int((ratio < 0.5).sum()),
                    "days_high_low_within_1_tick": int(near.sum()),
                }
            report["minute"][f"{p.code}_{period}m"] = entry
    for exchange in ("SHFE", "CFFEX"):
        path = ctx.out / "official" / f"{exchange}.parquet"
        if not path.exists():
            continue
        o = pd.read_parquet(path)
        entry = {
            "rows": len(o),
            "days": sorted(set(o["trading_day"])),
            "products": int(o["product"].nunique()),
        }
        if exchange == "SHFE":
            checks = {}
            for p in PRODUCTS.values():
                if p.exchange != "SHFE":
                    continue
                x = o[(o["product"] == p.code) & (o["volume"] > 0) & o["turnover"].notna()]
                if x.empty:
                    continue
                vwap = x["turnover"] / (x["volume"].astype(float) * p.multiplier)
                checks[p.code] = {
                    "contracts": len(x),
                    "settle_eq_floor_tick_vwap": int((_floor_tick(vwap, p.tick) == x["settle"]).sum()),
                    "settle_eq_round_tick_vwap": int((round_tick(vwap, p.tick) == x["settle"]).sum()),
                }
            entry["settle_rounding_check"] = checks
        report["official"][exchange] = entry
    for label, lo, hi, codes in STRESS_WINDOWS:
        for code in codes:
            c = conts.get(code)
            if c is None:
                report["stress_windows"].append({"window": label, "product": code, "rows": 0})
                continue
            w = c[(c["trading_day"] >= lo) & (c["trading_day"] <= hi)].copy()
            ref = w["contract_pre_settle"].fillna(w["pre_settle"])
            w["settle_chg"] = w["settle"] / ref - 1
            w["close_chg"] = w["close"] / ref - 1
            item: dict[str, Any] = {"window": label, "product": code, "rows": len(w)}
            if len(w):
                lo_i, hi_i = w["close_chg"].idxmin(), w["close_chg"].idxmax()
                item.update(
                    {
                        "first": w["trading_day"].min(),
                        "last": w["trading_day"].max(),
                        "min_close_chg": [
                            w.loc[lo_i, "trading_day"],
                            round(float(w.loc[lo_i, "close_chg"]) * 100, 2),
                        ],
                        "max_close_chg": [
                            w.loc[hi_i, "trading_day"],
                            round(float(w.loc[hi_i, "close_chg"]) * 100, 2),
                        ],
                        "rolls_in_window": int(w["roll"].sum()),
                        "settle_sources": sorted(set(w["settle_source"].astype(str))),
                    }
                )
            report["stress_windows"].append(item)
    return report


def _repo_relative(path: Path) -> str:
    """Repository-relative POSIX path when inside the repo (keeps local paths out of the manifest)."""
    try:
        return path.resolve().relative_to(REPO).as_posix()
    except ValueError:
        return str(path)


def write_manifest(ctx: Ctx, report: dict[str, Any], args: argparse.Namespace, started: str) -> Path:
    files = {}
    for path in sorted(ctx.out.rglob("*.parquet")):
        if path.name.startswith("."):
            continue
        meta = pq.read_metadata(path)
        files[path.relative_to(ctx.out).as_posix()] = {
            "rows": meta.num_rows,
            "bytes": path.stat().st_size,
            "sha256": sha256(path),
        }
    manifest_path = ctx.out / "manifest.json"
    previous = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    now = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    run_args = {k: (_repo_relative(v) if isinstance(v, Path) else v) for k, v in vars(args).items()}
    run = {
        "started_at": started,
        "finished_at": now,
        "args": run_args,
        "requests": ctx.fetcher.requests,
        "failures": ctx.fetcher.failures,
    }
    manifest = {
        "generated_at": now,
        "run_started_at": started,
        "script": "scripts/fetch_hist.py",
        "args": run_args,
        "versions": {
            "akshare": ak.__version__,
            "pandas": pd.__version__,
            "pyarrow": pa.__version__,
            "python": sys.version.split()[0],
        },
        "requests_made": ctx.fetcher.requests,
        "failures": ctx.fetcher.failures,
        "runs": (previous.get("runs", []) + [run])[-20:],  # provenance across runs (last 20)
        # Fetch diagnostics are only produced when data is fetched; keep earlier ones otherwise.
        "fetch_stats": {**previous.get("fetch_stats", {}), **ctx.stats},
        "files": files,
        "checks": report,
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1, default=str), encoding="utf-8"
    )
    return manifest_path


def print_summary(report: dict[str, Any]) -> None:
    print("\n=== daily (continuous) ===")
    for code, r in report["daily"].items():
        print(
            f"{code:>3} rows={r['rows']:5d} {r['first']}..{r['last']} rolls={r['rolls']:3d} "
            f"settle={r['settle_source']} nan={r['nan_key_cols'] or 0} "
            f"missing_days={r['missing_trading_days_count']} unknown_inst={r['unknown_instrument']}"
        )
    print("=== daily (per contract) ===")
    for code, r in report["contracts"].items():
        print(
            f"{code:>3} {r['file']:22s} rows={r['rows']:6d} contracts={r['contracts']:4d} "
            f"{r['first']}..{r['last']} one-main-days={r['days_with_one_main']}/{r['days']}"
        )
    print("=== minute ===")
    for key, r in report["minute"].items():
        print(
            f"{key:>8} rows={r['rows']:5d} {r['first']} .. {r['last']} days={r['trading_days']} "
            f"nan={r['nan'] or 0} rolls={r['rolls']} consistency={r.get('daily_consistency')}"
        )
    print("=== official ===")
    for exch, r in report["official"].items():
        print(f"{exch} rows={r['rows']} days={r['days']} {r.get('settle_rounding_check', '')}")
    print("=== stress windows ===")
    for item in report["stress_windows"]:
        print(
            f"{item['window']:42s} {item['product']:>3} rows={item['rows']:3d} "
            f"min={item.get('min_close_chg')} max={item.get('max_close_chg')} "
            f"rolls={item.get('rolls_in_window')} settle={item.get('settle_sources')}"
        )


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument(
        "--products",
        default=",".join(PRODUCTS),
        help=f"comma-separated product codes (default: {','.join(PRODUCTS)})",
    )
    ap.add_argument("--layers", default=",".join(LAYERS), help="comma-separated: daily,minute,official")
    ap.add_argument("--start", default="20150101", help="first trading day for daily data (YYYYMMDD)")
    ap.add_argument("--end", default=None, help="last trading day (default: latest published day)")
    ap.add_argument("--periods", default=",".join(DEFAULT_PERIODS), help="minute periods (default 5,15,60)")
    ap.add_argument("--official-days", type=int, default=5, help="trading days for the official layer")
    ap.add_argument("--sleep", type=float, default=1.0, help="min seconds between requests to one host")
    ap.add_argument("--retries", type=int, default=4, help="retries per request (exponential backoff)")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--force", action="store_true", help="refetch everything and overwrite outputs")
    mode.add_argument("--update", action="store_true", help="append new days / merge new minute bars")
    ap.add_argument("--verify-only", action="store_true", help="only verify files and rewrite the manifest")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT, help="output directory (default data/hist)")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"
    )
    args = parse_args(argv)
    started = dt.datetime.now().astimezone().isoformat(timespec="seconds")
    unknown = [c for c in args.products.split(",") if c and c not in PRODUCTS]
    layers = [x for x in args.layers.split(",") if x]
    if unknown or any(x not in LAYERS for x in layers):
        log.error("unknown products %s or layers %s", unknown, [x for x in layers if x not in LAYERS])
        return 2
    products = [PRODUCTS[c] for c in args.products.split(",") if c]
    cal = load_calendar()
    end = args.end or last_published_day(cal)
    start = next((d for d in cal if d >= args.start), args.start)
    ctx = Ctx(
        out=args.out.resolve(),
        cal=cal,
        start=start,
        end=end,
        fetch_start=prev_trading_day(cal, start) or start,
        force=args.force,
        update=args.update,
        fetcher=Fetcher(sleep=args.sleep, retries=args.retries),
    )
    log.info(
        "out=%s start=%s end=%s products=%s layers=%s mode=%s",
        ctx.out,
        start,
        end,
        ",".join(p.code for p in products),
        ",".join(layers),
        "force" if args.force else "update" if args.update else "skip-existing",
    )
    if not args.verify_only:
        if "daily" in layers:
            run_daily(ctx, products)
        if "minute" in layers:
            run_minute(ctx, products, [x for x in args.periods.split(",") if x])
        if "official" in layers:
            run_official(ctx, args.official_days)
    report = verify(ctx)
    print_summary(report)
    manifest = write_manifest(ctx, report, args, started)
    log.info(
        "manifest: %s; %d requests; %d failures", manifest, ctx.fetcher.requests, len(ctx.fetcher.failures)
    )
    for f in ctx.fetcher.failures:
        log.info("  failure: %s", f)
    return 1 if ctx.fetcher.failures else 0


if __name__ == "__main__":
    sys.exit(main())
