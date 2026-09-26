# Golden clearing cases (reference for tests/golden)

Verified with exact Decimal arithmetic during design (2026-09-24). Fees are rounded half-up to 0.01 per trade.

## C. Golden cases

Test convention: fees are rounded half-up to 0.01 per trade (the exchange's own fee rounding is unverified).

### Case 1: SHFE rb2701

Parameters: multiplier 10; fee 0.0001 on every leg (close-today multiplier 1); margin 7%; opening balance 200,000. Today lots are matched FIFO.

| Day | Trade | Offset | Fee |
|---|---|---|---|
| T1 | Buy 10 @3100 (T0 21:05, night session → belongs to T1) | Open | 31.00 |
| T1 | Buy 5 @3110 (09:40) | Open | 15.55 |
| T1 | Sell 3 @3130 (14:10) | CloseToday '3' (closes the @3100 lots) | 9.39 |
| T2 | Buy 4 @3115 (T1 21:10, night → T2) | Open | 12.46 |
| T2 | Sell 6 @3140 | Close '1' = 平昨 | 18.84 |
| T2 | Sell 2 @3135 | CloseToday '3' | 6.27 |
| T3 | Sell 8 @3100 | Close '1' (all lots are yesterday lots now) | 24.80 |

| | T1 (S=3120) | T2 (P=3120, S=3132) | T3 (P=3132, S=3095) |
|---|---|---|---|
| 平仓盈亏 | 30·10·3 = 900 | 20·60 + 20·20 = 1,600 | −32·80 = −2,560 |
| 持仓盈亏 | 20·70 + 10·50 = 1,900 | 12·60 + 17·20 = 1,060 | 0 |
| Exchange-formula check (Art. 39) | 2,800 ✓ | (48+6+68+144)·10 = 2,660 ✓ | −2,560 ✓ |
| 手续费 | 55.94 | 37.57 | 24.80 |
| 权益 (= 期末结存) | 202,744.06 | 205,366.49 | 202,781.69 |
| 保证金 | 26,208.00 | 17,539.20 | 0 |
| 可用资金 | 176,536.06 | 187,827.29 | 202,781.69 |
| 风险度 | 12.9266% | 8.5404% | 0 |

- **Invariant**: Σ MTM = 2,900 = (59,300 − 59,010)·10; 200,000 + 2,900 − 118.31 = 202,781.69.
- **Negative tests**: CloseToday 6 on T2 must be rejected (only 4 today lots). Any CloseToday on T3 must be rejected.
- **Fee variant**: with close-today multiplier 2 (as for cu), the close-today fees become 18.78 and 12.54.

### Case 2: CFFEX IF2612

Parameters: multiplier 300; open/close-yesterday 0.23‱; close-today 2.3‱; margin 12%; opening balance 1,000,000.

**D1 trade tape** (includes our fills):
- 10:00 4400.0 × 2 (our BuyOpen 2)
- 13:55 4380.0 × 50 (outside the last hour)
- 14:05 4410.0 × 10
- 14:20 4415.0 × 20
- 14:30 4420.0 × 5 (includes our SellClose 1)
- 14:45 4418.0 × 15
- 14:59:30 4412.0 × 10

Last hour: 264,890 / 60 = 4414.833 → **S1 = 4414.8**. This is identical under 1-decimal, round-to-tick and floor-to-tick rounding. Whole-day VWAP would be 4399.02, so a wrong-window bug is caught.

**D2 last hour**: 14:10 4440.0 × 10, 14:40 4444.0 × 10, 14:55 4442.0 × 5 → **S2 = 4442.0** exactly.

| | D1 | D2 (1 yesterday lot @4414.8) | D3 |
|---|---|---|---|
| Trades | BuyOpen 2 @4400; SellClose 1 @4420 | 09:35 BuyOpen 1 @4400; 10:30 SellClose 1 @4450 | SellClose 1 @4500 |
| Fees | 60.72 + 304.98 = 365.70 | 30.36 + **307.05** = 337.41 (a same-day open precedes the close, so it is close-today; a FIFO fee engine would charge 30.71) | 31.05 (no same-day open → yesterday rate) |
| 平仓 / 持仓 | 6,000 / 4,440 | today-first: 15,000 / 8,160; FIFO: 10,560 / 12,600 | 17,400 / 0 |
| Day total (Art. 13 check) | 10,440 ✓ | 23,160 ✓ (both matchings) | 17,400 ✓ |
| 权益 | 1,010,074.30 | 1,032,896.89 | 1,050,265.84 |
| 保证金 | 158,932.80 | 159,912.00 | 0 |
| 可用资金 | 851,141.50 | 872,984.89 | 1,050,265.84 |
| 风险度 | 15.7348% | 15.4819% | 0 |

- **Invariant**: 51,000 = (13,370 − 13,200)·300; total fees 734.16.
- **Extra unit tests from the CFFEX FAQ**: hold 20 yesterday, sell 10 then buy 8 → 0 close-today lots. Hold 20, buy 8 then sell 10 → 8 close-today + 2 close-yesterday.

### Case 3: SHFE two-way position (big-side)

Parameters: rb2610 margin 10%, rb2701 7% (official 2026-09-24 rates); fee 0.0001; opening balance 500,000.

| | D1 | D2 (= L−5 for rb2610; no trades) |
|---|---|---|
| Trades | BuyOpen rb2610 8 @3050 (fee 24.40); SellOpen rb2701 10 @3110 (fee 31.10) | — |
| Settlement | 3056 / 3111 | 3060 / 3100 |
| P&L | 480 − 100 = 380 | 320 + 1,100 = 1,420 |
| Margin by side | long 24,448.00 / short 21,777.00 (gross 46,225.00) | rb2610 excluded: two-way 24,480.00, + big side of the rest, max(0, 21,700.00) |
| Charged margin | **24,448.00**: long is bigger by amount although short has more lots, so a lots-based rule fails | **46,180.00** |
| 权益 / 可用 | 500,324.50 / 475,876.50 | 501,744.50 / 455,564.50 |
| 风险度 | 4.8864% (9.2390% if charged gross) | 9.2039% (4.8790% if the exclusion is missed) |

- I read the exclusion as starting from the L−5 settlement, based on SHFE's "最后5个交易日不参加".
- **CFFEX variant**: IF long 1 (158,932.80) plus IH short 1 (102,895.20) is charged 158,932.80, and index futures are never excluded.
