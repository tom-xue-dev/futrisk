# Historical market data (`data/hist`)

Development-time market data for the futrisk backtest (docs/01-architecture.md §8.16):
layer **A** (daily bars since 2015 with settlement prices) and layer **B** (Sina 60/15/5-minute
bars). Layers C (recorded ticks) and D (synthetic stress scenarios) are produced elsewhere.
Nothing here is read by the runtime services; the backtest reads these files read-only.

- Generator: `scripts/fetch_hist.py`, run with the isolated virtualenv `.venv-data`
  (akshare + pyarrow; never the project venv).
- Fetched: 2026-09-25 (Beijing), data up to trading day **2026-09-24** (2026-09-25 is the
  Mid-Autumn holiday; no night session was held on 2026-09-24).
- `manifest.json` (written by every run) holds row counts, date ranges, SHA-256 of every file,
  per-product fetch diagnostics, verification results and the list of failed requests.

## Contents (fetch of 2026-09-25)

| Product | Exch. | `daily/{product}` rows (range) | Rolls | Settle source | `daily/{EXCH}/{product}` rows / contracts | Minute first bar 5m / 15m / 60m |
|---|---|---|---:|---|---|---|
| rb | SHFE | 2,852 (20150105-20260924) | 35 | exchange | 34,221 / 153 | 2026-09-03 / 2026-07-24 / 2026-01-12 |
| hc | SHFE | 2,852 (20150105-20260924) | 35 | exchange | 34,221 / 153 | 2026-09-03 / 2026-07-24 / 2026-01-12 |
| cu | SHFE | 2,852 (20150105-20260924) | 139 | exchange | 34,221 / 153 | 2026-09-09 / 2026-08-10 / 2026-03-21 |
| au | SHFE | 2,852 (20150105-20260924) | 40 | exchange | 22,746 / 149 | 2026-09-11 / 2026-08-18 / 2026-04-30 |
| i | DCE | 2,852 (20150105-20260924) | 35 | Sina 2,646, estimated 206 | 18,576 / 91 | 2026-09-03 / 2026-07-24 / 2026-01-12 |
| j | DCE | 2,852 (20150105-20260924) | 35 | Sina 2,645, estimated 207 | 13,304 / 85 | 2026-09-03 / 2026-07-24 / 2026-01-12 |
| m | DCE | 2,852 (20150105-20260924) | 35 | Sina 2,648, estimated 204 | 10,823 / 54 | 2026-09-03 / 2026-07-24 / 2026-01-12 |
| IF | CFFEX | 2,852 (20150105-20260924) | 86 | exchange | 11,408 / 145 | 2026-08-26 / 2026-06-29 / 2025-09-05 |
| IH | CFFEX | 2,785 (20150416-20260924) | 85 | exchange | 11,140 / 141 | 2026-08-26 / 2026-06-29 / 2025-09-05 |
| IC | CFFEX | 2,785 (20150416-20260924) | 76 | exchange | 11,140 / 141 | 2026-08-26 / 2026-06-29 / 2025-09-05 |
| IM | CFFEX | 1,015 (20220722-20260924) | 19 | exchange | 4,060 / 54 | 2026-08-26 / 2026-06-29 / 2025-09-05 |
| T | CFFEX | 2,803 (20150320-20260924) | 45 | exchange | 8,409 / 48 | 2026-08-27 / 2026-07-02 / 2025-11-25 |

- Every continuous series contains every trading day from its first day to 2026-09-24 (2,852
  trading days since 2015-01-05) with no NaN in the key columns; every per-contract file has
  exactly one main contract per day.
- Minute files hold 1,023 bars each, up to 2026-09-24 15:00 (T 15:15), except cu 15m (1,021),
  cu 60m (1,018) and au 15m (1,021), where empty Saturday remainder bars were dropped.
- `official/SHFE.parquet`: 1,520 rows. It has 26 products, including the INE products (sc, lu,
  nr, bc, ec) that the SHFE daily report also contains. `official/CFFEX.parquet`: 140 rows,
  8 products. Both cover trading days 20260918-20260924.
- 62 Parquet files, 7.3 MB (daily 6.1 MB, minute 1.2 MB, official 0.07 MB). Per-file sizes and
  SHA-256 are in `manifest.json`.

## Refresh

```bash
# once: isolated dev-time venv (Python 3.12)
py -3.12 -m venv .venv-data
.venv-data/Scripts/python -m pip install akshare pyarrow

# incremental refresh (a few minutes): new trading days, newest minute bars merged in
.venv-data/Scripts/python scripts/fetch_hist.py --update

# rebuild everything from scratch (about 2.5 h: ~1 h 55 min for SHFE, one request per
# trading day, plus ~30 min for the 100ppi pages)
.venv-data/Scripts/python scripts/fetch_hist.py --force

# subsets / checks
.venv-data/Scripts/python scripts/fetch_hist.py --products IF,T --layers daily,minute --force
.venv-data/Scripts/python scripts/fetch_hist.py --verify-only
```

SHFE and CFFEX data are fetched per exchange. Forcing one SHFE product refetches every SHFE
trading day and rewrites all four SHFE products.

Options: `--products` (default `rb,hc,cu,au,i,j,m,IF,IH,IC,IM,T`), `--layers`
(`daily,minute,official`), `--start` (default 20150101), `--end` (default: latest published
trading day), `--periods` (default `5,15,60`), `--official-days` (default 5), `--sleep`
(minimum seconds between two requests to the same host, default 1.0), `--retries` (default 4,
exponential backoff). Without `--force`/`--update` existing files are skipped (idempotent).
An interrupted SHFE download resumes from `daily/SHFE/.checkpoint.parquet`. Exit code 1 means
some request failed; the failures are listed in `manifest.json`.

Run `--update` regularly: Sina serves only the latest 1023 bars per period, and `--update`
merges each new window into the existing minute files, so the minute history grows with every
refresh.

## Sources

| Data | Source | Interface |
|---|---|---|
| SHFE daily per contract (rb, hc, cu, au) | Exchange daily report (JSON, one file per trading day) | akshare `get_shfe_daily(date)` → `https://www.shfe.com.cn/data/tradedata/future/dailydata/kx{YYYYMMDD}.dat` |
| CFFEX daily per contract (IF, IH, IC, IM, T) | Exchange monthly archive (one zip of daily CSVs per month); single-day CSV as fallback | `http://www.cffex.com.cn/sj/historysj/{YYYYMM}/zip/{YYYYMM}.zip`, the archive that akshare `get_cffex_daily` reads, downloaded once per month instead of once per day; fallback `http://www.cffex.com.cn/sj/hqsj/rtj/{YYYYMM}/{DD}/{YYYYMMDD}_1.csv` (akshare `futures_hist_daily_cffex`) |
| DCE daily (i, j, m) | Sina main-continuous series `I0`, `J0`, `M0` (dce.com.cn answers scripted requests with HTTP 412 and a JavaScript challenge, so akshare `get_dce_daily` fails) | akshare `futures_zh_daily_sina(symbol="I0")` |
| DCE contract identification | (1) Sina legacy per-contract daily archive, including expired contracts; frozen since **2024-07-17**. (2) 100ppi.com daily page with the near and dominant contract of each product and their closing prices | (1) `https://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesDailyKLine?symbol=I1605`; (2) `https://www.100ppi.com/sf/day-YYYY-MM-DD.html` (the page behind akshare `futures_spot_price`, parsed directly because that function can loop forever when a day's page is missing) |
| Sina cross-reference for SHFE/CFFEX | Sina main-continuous series `RB0` … `IF0` … `T0` | akshare `futures_zh_daily_sina(symbol=...)` |
| Minute bars | Sina main-continuous minute bars | akshare `futures_zh_minute_sina(symbol="RB0", period="5"|"15"|"60")` |
| Official sample (last 5 trading days) | SHFE and CFFEX daily reports | akshare `get_shfe_daily`; CFFEX single-day CSV. akshare `get_dce_daily` is not used: dce.com.cn answers with HTTP 412 (checked 2026-09-25) |
| Trading calendar | akshare calendar (1990–2026), replaced by `config/calendar/trading_days.csv` for 2025–2027 (both agree on 2025–2026) | `akshare.futures.cons.get_calendar()` |

Sina symbols: the product code in upper case followed by `0` (`RB0`, `HC0`, `CU0`, `AU0`,
`I0`, `J0`, `M0`, `IF0`, `IH0`, `IC0`, `IM0`, `T0`).

## File formats and columns

All files are Parquet (zstd) written with pyarrow: strings are plain UTF-8 columns, prices are
float64 and counts nullable int64, so they read with the project stack (pandas 2.x,
pyarrow ≥ 17). Contract codes follow the exchanges: lower case for SHFE/DCE (`rb2601`,
`i2601`), upper case for CFFEX (`IF2612`, `T2612`). `trading_day` is `YYYYMMDD`; a night
session belongs to the next trading day, as in the exchanges' own reports.

### `daily/{product}.parquet`: continuous main-contract series (layer A)

One row per trading day; the row is the daily bar of that day's main contract.

| Column | Type | Meaning |
|---|---|---|
| `trading_day` | str | Trading day `YYYYMMDD` |
| `instrument_id` | str, nullable | Contract behind the row; null only for DCE rows that could not be identified |
| `open`, `high`, `low`, `close` | float64 | Daily bar of that contract (the open is the night-session open where there is a night session) |
| `settle` | float64 | Settlement price of that contract; origin in `settle_source` |
| `pre_settle` | float64 | Previous row's `settle`. On a roll day this is the **old** contract's settle. First row of a series: the contract's own exchange pre-settle |
| `contract_pre_settle` | float64, nullable | Previous settle of the **same** contract. SHFE/CFFEX: the exchange-published pre-settle. DCE: the previous row's settle when the contract did not change; null on DCE roll days and next to unidentified rows. **Use this for mark-to-market** |
| `volume` | Int64 | Lots traded (bilateral before 2020-01-01 on SHFE/DCE, see caveats) |
| `open_interest` | Int64 | Open interest at the close (same caveat) |
| `roll` | bool | True on the first row of a new main contract, i.e. the row's contract differs from the last identified contract before it |
| `settle_source` | str | `exchange` (SHFE/CFFEX official), `sina` (Sina's settlement field), `estimated` (Sina has no settlement price that day; calibrated proxy, see validation) |
| `instrument_source` | str, nullable | `exchange` (main-contract rule applied to exchange data), `contract_bar` (daily-bar match against Sina's per-contract archive), `100ppi` (100ppi closing price of the near/dominant contract equals Sina's close), `inferred`, `excursion_repaired`, `gap_filled` (see roll detection), null (unknown) |
| `sina_instrument_id` | str, nullable | Contract behind Sina's own continuous series (`RB0`, `IF0`, ...) that day; used to label the minute bars. Null where Sina's series has no data (CFFEX before 2017-01-17) or no bar matched |

### `daily/{EXCHANGE}/{product}.parquet`: per-contract daily bars (layer A, layout of docs/03 §5.2)

| Column | Type | Meaning |
|---|---|---|
| `trading_day`, `instrument_id` | str | One row per listed contract and trading day |
| `open`, `high`, `low`, `close` | float64 | NaN when the contract did not trade that day |
| `settle`, `pre_settle` | float64 | SHFE/CFFEX: exchange-published. DCE: only on `is_main` rows (from the continuous series), null otherwise |
| `volume`, `open_interest` | Int64 | DCE: open interest only on `is_main` rows |
| `turnover` | float64, nullable | CNY (the exchanges publish units of 10,000 CNY; converted). Null where not published (SHFE before 2021-07-28) and for DCE |
| `is_main` | bool | SHFE/CFFEX: the main-contract rule below. DCE: the contract behind Sina's continuous series |
| `settle_source` | str, nullable | As above; null on DCE non-main rows |

The DCE files hold the legacy OHLCV rows of every contract up to 2024-07-17 (when Sina's
archive froze); after that date they only contain the identified main contract.

### `minute/{product}_{period}m.parquet`: Sina minute bars (layer B)

| Column | Type | Meaning |
|---|---|---|
| `ts` | str | Bar **end** time in Beijing time, ISO 8601 with offset, e.g. `2026-09-24T14:55:00+08:00` (the bar covering 14:50-14:55) |
| `ts_ns` | int64 | The same instant as UTC epoch nanoseconds |
| `trading_day` | str | Trading day of the bar (night bars belong to the next trading day) |
| `open`, `high`, `low`, `close` | float64 | Bar prices |
| `volume` | Int64 | Lots traded in the bar (single-sided) |
| `open_interest` | Int64 | Sina's open-interest snapshot at (about) the bar end |
| `instrument_id` | str, nullable | `sina_instrument_id` of the bar's trading day from `daily/{product}.parquet`; null when the daily file has no row for that day yet |
| `roll` | bool | First bar of a new contract |
| `quality` | str, nullable | null = as delivered by Sina; `midnight_relabelled` or `offsession_label` (see caveats) |

### `official/{EXCHANGE}.parquet`: official per-contract data of the last 5 trading days

All futures products of the exchange, not only the tracked ones: `trading_day`, `exchange`,
`product`, `instrument_id`, `open`, `high`, `low`, `close`, `settle`, `pre_settle`, `volume`,
`open_interest`, `turnover` (CNY). Intended as fixtures for settlement-price and rounding tests.
There is no `DCE.parquet` (blocked, see sources).

## Main contract and roll detection

**SHFE and CFFEX: exact, from exchange data.** The main contract of day *t* is the contract
with the largest open interest on the previous trading day among the contracts trading on
*t* (docs/03 §5.2), with one refinement: the main never moves back to an earlier delivery month.
That prevents flip-flops when two contracts have similar open interest. It overrode the plain
rule on IF 298, IH 354, IC 241 and IM 31 days: in index futures, open interest alternates
between the front month and the next quarterly contract. It never overrode rb, hc, cu, au or T.
`roll` marks the first day on the new contract. `contract_pre_settle` is the new contract's
exchange pre-settle, so mark-to-market across a roll is exact.

**DCE: Sina's own switching rule, identified after the fact.** Sina does not publish how it
switches `I0`/`J0`/`M0`. Each row's contract is identified, in this order:

1. `contract_bar`: the day's bar matches exactly one contract's bar in Sina's per-contract
   archive (which includes expired contracts). The match is on OHLC and volume, then OHLC
   only, then at least three identical prices or identical volume plus two identical prices,
   because Sina's two feeds sometimes differ by a tick or truncate a volume. The archive is
   complete until **2024-07-17**, when it stopped updating.
2. `100ppi` (after 2024-07-17): 100ppi.com's closing price of the product's near or dominant
   contract equals Sina's close that day. One page per day covers i, j and m.
3. `inferred`:
   - an unidentified gap enclosed by the same contract on both sides inherits it;
   - after 2024-07-17, between contract A (before) and B (after), a row is on A when 100ppi
     shows B at a price different from Sina's close, and on B in the mirror case;
   - a gap between two different contracts is split where the open interest fits: open interest
     is continuous within a contract and jumps at a switch;
   - labels that would move the main back to an earlier delivery month are rejected.
4. `excursion_repaired`: Sina's series sometimes jumps for one or two days to another contract
   and back, mostly on the first trading day after a holiday. Example: `I0` shows the expiring
   i1509 (open interest 3,576, settle 466.5) instead of i1601 (settle about 389) on 2015-09-07,
   a fake +20 % move and back. Such rows are rebuilt from the main contract's bar in the
   archive, with an estimated `settle` (`settle_source = estimated`) and interpolated open
   interest. Days: i 20150623, 20150907, 20151008, 20160405, 20171009, 20201222;
   j 20150907, 20171009, 20201120, 20201203, 20201209; m 20150623, 20150907, 20151008,
   20160405, 20171009.
5. `gap_filled`: trading days missing from Sina's continuous series, rebuilt the same way from
   the archive bar of the contract that is the main on both sides. Days: i 20150225, 20190422;
   j 20150225, 20150623, 20160405, 20190422; m 20190422.

Result: every DCE row is identified; no `instrument_id` is null in this fetch.

| Source | i | j | m |
|---|---:|---:|---:|
| `contract_bar` | 2,304 | 2,295 | 2,313 |
| `100ppi` | 514 | 523 | 509 |
| `inferred` | 26 | 25 | 24 |
| `excursion_repaired` | 6 | 5 | 5 |
| `gap_filled` | 2 | 4 | 1 |

Each product has 35 rolls in 2015-2026, strictly forward through the 01/05/09 contracts,
about three per year. `roll` is set on the first row identified with the new contract.

**Sina's main vs the futrisk rule.** `sina_instrument_id` records the contract behind Sina's own
continuous series. Where both exist, Sina and the futrisk rule pick the same contract on:

| Product | Days agreeing / compared |
|---|---|
| rb | 2,801 / 2,850 |
| hc | 2,825 / 2,851 |
| cu | 2,709 / 2,850 |
| au | 2,613 / 2,851 |
| T | 2,300 / 2,353 |
| IF | 1,534 / 2,353 |
| IH | 1,597 / 2,353 |
| IC | 1,367 / 2,353 |
| IM | 686 / 1,015 |

The index-futures series `IF0` and the others switch on a different schedule, mostly around
rolls and while the front month and the next quarterly contract have similar open interest.
The minute bars follow Sina's contract; the daily continuous series follows the futrisk rule.

## Validation

Every run re-checks all files (`manifest.json` → `checks`): row counts, date ranges, NaN in
key columns, missing or duplicate trading days, OHLC consistency, one main contract per day,
and the checks below.

- **SHFE settlement rounding.** In the official sample (2026-09-18 to 2026-09-24), the settle
  equals `floor_to_tick(turnover / (volume × multiplier))` for all 218 of 218 rb/hc/cu/au
  contract-days with volume. Rounding to the nearest tick reproduces only 114. This confirms
  `settle_rounding: FLOOR_TICK` for SHFE in `config/products.yaml`; SHFE settle is the full-day
  VWAP floored to the tick.
- **CFFEX.** Every settle lies on the tick grid (0.2 for index futures, 0.005 for T). The
  official settle is the last-hour VWAP (14:00-15:00, T 14:15-15:15), which daily data cannot
  reproduce; the minute files or recorded ticks are needed for that.
- **Sina vs exchange (SHFE).** On days where Sina's `RB0`/`HC0`/`CU0`/`AU0` is on the same
  contract and reports a settlement price, it equals the exchange's settle on 2,775 / 2,797
  (rb), 2,802 / 2,821 (hc), 2,682 / 2,705 (cu) and 2,596 / 2,611 (au) days, about 99.2 %. All
  mismatches fall in 2015-2018.
  - Most are 1-5 ticks: Sina reports an unrounded VWAP, e.g. cu values off the 10-yuan grid.
  - A few large ones fall on the first trading day after a holiday: hc 2015-02-25 (126 ticks)
    and 2015-06-23 (49 ticks), au 2017-10-09 (52.5 ticks). On those days Sina's continuous series
    itself is unreliable; they are the days where the DCE series needed excursion or gap repairs.
  - Sina also lacks a settlement price on 3-4 SHFE days (20190430, 20211230, 20230828,
    20240925).

  Read the Sina-sourced DCE settlement prices with the same small error rate in mind.
- **DCE settlement proxy.** Where Sina has no settlement price, `(high + low) / 2` rounded to the
  tick is used. On the 2,646-2,649 days per product where Sina has one, it is the best of close,
  HLC/3, OHLC/4 and HL/2, with a mean absolute error of 2.3 ticks / 17 bp (i), 7.0 ticks /
  17 bp (j) and 3.2 ticks / 10 bp (m); 95th percentile 7, 23 and 9 ticks. Using the close
  would cost 85, 82 and 39 bp.
- **DCE contract identification.** Each DCE product has 35 rolls in 2015-2026, strictly forward
  through the 01/05/09 contracts. After 2024-07-17 (100ppi-based), open interest stays
  continuous within a contract. Only one day on an unchanged contract changes by more than 25 %:
  j2609 on 2026-06-12, +50 % during a rally on heavy volume, with the contract confirmed by the
  price match.
- **Minute vs daily.** Sina's minute volumes are incomplete. Summed per trading day, they reach
  a median of 83-97 % of the daily volume for 5-minute bars, 76-94 % for 15-minute bars and only
  55-78 % for 60-minute bars (10th percentile 2-27 %). On some days a 60-minute bar carries only
  the volume of its last 5 minutes: rb on 2026-09-22 shows 192 lots for 21:00-22:00, while its
  5-minute bars add up to about 151,000. Daily highs and lows are reproduced within one tick on
  most days for rb, hc, i, m and T (60m: 152-178 of 171-204 days), but less often for cu, au, j
  and the index futures, whose bars miss some extremes. Use the minute bars for price paths, not for
  volume-weighted settlement estimates. Per-file figures are in `manifest.json`
  (`checks.minute.*.daily_consistency`).

## Stress-window coverage

Moves are closes (and intraday extremes) relative to the contract's own previous settle.
Settlement prices in these windows are exchange-published (SHFE/CFFEX) or Sina's (DCE); none is
estimated.

| Window | Products (days) | Main contracts | Largest moves |
|---|---|---|---|
| 2015-07 equity crash | IF, IH, IC (23 each) | IF1507, IH1507, IC1507, one roll late in the month | IF −10.00 % on 07-08 and +9.99 % on 07-09 (limit moves), also −10.00 % on 06-26; IC −10.00 % on 07-01, +10.00 % on 07-09 |
| 2016-11-14 black-sector crash | rb, hc, i, j (7: 11-10…11-18) | rb1701, hc1701, i1701, j1701 | 11-14 (includes the 11-11 night session): rb high +5.82 % / low −4.80 %, i high +9.05 % / low −1.16 %; then rb and hc −6.02 % (limit) and j −8.56 % on 11-15, i −6.48 % on 11-16 |
| 2020-02-03 post-holiday limit-down | 11 products (2) | rb2005, i2005, IF2003, ... | rb/hc −8.02 %, i −7.97 %, cu −6.48 %, j −5.89 %, m −3.01 %, IF/IC −10.00 %, IH −9.68 %; au +2.67 %, T +1.37 % |
| 2021-05 iron ore | i, rb, hc (18: 05-06…05-31) | i2109, rb2110, hc2110 | i +10.00 % on 05-10, −7.49 % on 05-13 and 05-14 (lows −9.51 % / −9.98 %); rb +5.99 % / −6.02 %; hc +6.88 % / −6.01 % |
| 2021-10 coke | j (16: 10-08…10-29) | j2201 | +9.00 % on 10-18, −8.99 % on 10-20, −12.00 % on 10-21, low −13.64 % on 10-22 (the exchange widened the limit) |
| 2022-03 nickel squeeze | ni not tracked; cu (23) | cu2204, one roll | cu +3.08 % on 03-07; noted only |
| 2024-10-08 index limit-up open | IF, IH, IC, IM (7: 09-24…10-09) | IF2412, IH2412, IC2412, IM2412 | IF opened at the +10.00 % limit on 10-08 and closed +3.13 %; +10.00 % closes on 09-27 (IC, IM) and 09-30 (IF); IF −8.00 % on 10-09 |

## Known caveats

1. **Roll gaps in the continuous series.** Prices in `daily/{product}.parquet` jump on roll
   days by the calendar spread. Example: IM on 2026-09-15, old IM2609 settle 7454.4 against
   IM2612's own pre-settle 7209.6 (−3.3 %, the index-futures discount). Never compute returns or
   P&L from consecutive continuous rows across a roll. Use `settle − contract_pre_settle`
   (exact for SHFE/CFFEX), or hold real contracts and roll them explicitly with the
   per-contract files. On DCE roll days `contract_pre_settle` is null (the new contract's
   previous settle is unknown); up to 2024-07-17 both contracts' OHLCV of the roll day are in
   `daily/DCE/{product}.parquet`.
2. **Bilateral volume and open interest before 2020.** SHFE and DCE (like CZCE) counted both
   sides until 2019-12-31 and one side from 2020-01-01 (exchange notices of 2019-10-29).
   Example: rb2005 open interest 2,948,358 on 2019-12-31 and 1,443,715 on 2020-01-02. CFFEX has
   always counted one side. Values are kept as published; halve SHFE/DCE values before 2020 for
   a consistent series.
3. **Regime change in index-futures volume.** After CFFEX's trading restrictions of
   September 2015, IF volume fell from about 2.0 million lots per day (June 2015) to about
   18,500 (October 2015). This is real, not a data error; liquidity-based sizing must not treat
   the two regimes alike.
4. **DCE settlement prices and gaps.** DCE settlement prices come from Sina because the
   exchange site is blocked.
   - Sina has no settlement price on 198 days per product: 2017-11-01 to 2018-03-09, most of
     2020-02-05 to 2020-07-17, and single days in 2019-12, 2020-01, 2021-12-30 and 2024-09-25.
   - These days, plus the rebuilt excursion and gap rows, carry an estimated settle
     (`settle_source = estimated`; i 206, j 207, m 204 rows). The estimate is `(high + low) / 2`
     rounded to the tick, with a typical error of 10-17 bp (see validation); exclude these rows
     where an exact settle matters.
   - Sina rows on non-trading days (20150903, 20151001, 20170102, 20171002, duplicates of the
     previous day) are dropped. Trading days missing from Sina's series are rebuilt
     (`gap_filled`), so the DCE continuous files contain every trading day.
   - `contract_pre_settle` is null on the 35 DCE roll days and the first row (36 rows per
     product).
5. **Sina minute depth and shape.**
   - Sina serves only the latest 1023 bars per period and symbol. In this fetch that is 2-4 weeks
     of 5-minute bars, 5 weeks to 3 months of 15-minute bars and 5-12 months of 60-minute bars.
     Products with long night sessions (au, cu) reach back least, CFFEX products furthest. Only
     `--update` runs extend the history.
   - Volumes are incomplete, and 60-minute volumes badly so (see validation). Highs and lows
     sometimes miss the day's extremes.
   - Bars are labelled with their **end** time and aggregated by trading minutes, so a bar can
     span a session break. The 60-minute rb bar labelled 11:15 covers 10:00-10:15 and
     10:30-11:15; the au bar labelled 09:30 covers 02:00-02:30 of the night and 09:00-09:30.
   - Friday nights: Sina labels the bar that ends at Saturday 00:00 as Monday 00:00. It is moved
     back and flagged `midnight_relabelled`; its open equals the previous bar's close, which
     confirms the placement. Sina also emits day-time "remainder" bars dated Saturday: empty ones
     are dropped, the rest keep their label with `offsession_label`.
   - Volumes of post-midnight Friday-night bars of cu and au are implausibly small. Sina's
     open-interest snapshots are not aligned with bar boundaries (|ΔOI| exceeds the bar volume
     in many bars). Do not derive open-interest flows from minute bars.
   - The series follows Sina's continuous contract, which switches on Sina's schedule
     (`instrument_id`, `roll`). A bar that has not closed yet is dropped when fetching during
     trading hours.
6. **CFFEX trading hours.**
   - T (10-year treasury futures) trades 09:30-11:30 and 13:00-15:15, 15 minutes longer than the
     index futures. The open was 09:15 until 2020-07-17; on its last trading day T trades only
     09:30-11:30. T's minute files therefore end at 15:15 (the 60-minute series has a
     15:00-15:15 stub bar labelled 15:15), and its settlement window is 14:15-15:15
     (config/products.yaml).
   - Index futures traded 09:15-11:30 and 13:00-15:15 until 2015-12-31, and 09:30-11:30 and
     13:00-15:00 from 2016-01-01. This matters when synthesizing intraday paths for the
     2015-07 window.
   - The index circuit breaker ended the sessions of 2016-01-04 and 2016-01-07 early; the
     daily bars of those days cover the shortened sessions.
7. **Listing dates.** IH and IC start 2015-04-16, T 2015-03-20, IM 2022-07-22.
8. **Sessions, limits and margins changed over time.** `config/products.yaml` holds 2026
   parameters. Exchanges widened limits and raised margins during stress episodes, and
   night-session hours changed over the years. The stress table reports raw price moves.
9. **Calendar.** Trading days before 2025 come from akshare's calendar, 2025-2027 from
   `config/calendar/trading_days.csv` (2027 is an estimate). All continuous series contain every
   trading day of that calendar from their first day.
10. **Nickel (2022-03).** ni is not a futrisk product and was not fetched; the LME nickel
    squeeze of March 2022 is noted only for completeness (the cu series covers that month).
11. Data is as published on the fetch date; exchanges occasionally correct past reports.

## Relation to docs/03-database-schema.md §5.2

- Per-contract daily files `daily/{exchange}/{product}.parquet`: as specified (SHFE/CFFEX from
  the exchanges, DCE from the Sina continuous series), plus `turnover` and `settle_source`.
  `is_main` follows the specified rule with the "never back to an earlier delivery month"
  refinement.
- Minute files: §5.2 specifies `minute/{period}/{symbol}.parquet` with columns `datetime` and
  `hold`. This directory uses `minute/{product}_{period}m.parquet` with `ts` (ISO, Beijing),
  `ts_ns` (UTC ns) and `open_interest`, plus `trading_day`, `instrument_id`, `roll` and
  `quality`. One of the two has to be aligned.
- The continuous series `daily/{product}.parquet`, `official/` and `manifest.json` are
  additions not listed in §5.2.
