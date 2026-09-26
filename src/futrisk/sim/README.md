# sim-exchange — the virtual exchange

`sim-exchange` is the external mock the whole demo trades against (01 §5.1, §8.13). It is
**not** on the internal ZeroMQ bus; it speaks its own external protocols (02 §9):

| Interface | Default endpoint (`config/sim.yaml`) | Spec |
|---|---|---|
| Feed A — CTP-style push feed (WebSocket, JSON text frames) | `ws://127.0.0.1:7001/feed` | 02 §9.1 |
| Feed B — vendor-style polled quotes (HTTP) | `http://127.0.0.1:7002/v1/quotes`, `/v1/health` | 02 §9.2 |
| SimBroker — CTP-like trading counter (ZeroMQ ROUTER, one JSON object per frame) | `tcp://127.0.0.1:7003` | 02 §9.4 |
| Control API (HTTP, only when `app.mode=demo`) | `http://127.0.0.1:7004/control` | 02 §9.5 |

Code: `src/futrisk/sim/exchange/` — `engine.py` (price engine), `feed_a.py`, `feed_b.py`,
`broker.py` (+ `router_io.py`, `store.py` for `<home>/db/sim.db`), `faults.py`,
`control.py`, `scenario.py`, `app.py` (wiring). Scenario scripts live in
`config/scenarios/*.yaml`.

## Running it

```powershell
# manual run; uses <home>/config/active.json (a fresh home gets the bundled config)
.venv\Scripts\python -m futrisk.sim.exchange --home var

# demo: start with market time = a Beijing time inside a session
.venv\Scripts\python -m futrisk.sim.exchange --home var --market-time 2026-09-28T09:30:00

# exact offset as passed by the supervisor (market time - wall time, ns)
.venv\Scripts\python -m futrisk.sim.exchange --home var --clock-offset-ns 252716088435800
```

Options: `--config DIR` (load a config directory directly), `--clock-version N`,
`--log-level`, `--no-file-log`. Logs go to `<home>/logs/sim-exchange_YYYYMMDD.log`
(JSON lines) and the console. Stop with Ctrl+C / Ctrl+Break.

From Python (what `futrisk svc sim-exchange` calls):

```python
from futrisk.sim.exchange import SimExchange, run_sim_exchange

await run_sim_exchange(cfg, layout, clock_offset_ns=offset, stop=stop_event)

async with SimExchange(cfg, layout, offset) as ex:   # tests: ports may be 0 (ephemeral)
    ex.feed_a_url, ex.feed_b_url, ex.broker_endpoint, ex.control_url
```

The component works under any asyncio loop (the ROUTER socket lives in its own I/O thread,
so the Windows Proactor loop is fine; `zmq.asyncio` is not used).

In other services' tests, `futrisk.sim.exchange.testing` gives a ready config
(`make_test_config(cfg)`: every port ephemeral, 50 ms ticks, fast heartbeats) and
`offset_for(MARKET_OPEN)`, a market-clock offset that puts market time at Monday
2026-09-28 09:35 Beijing, when every simulated product is in a continuous session.

## Time, sessions and the price engine

- **Market time** = wall clock + offset (01 §8.2). The offset comes from the command line
  and changes only through `POST /control/clock` while paused (02 §8.8 steps 1/3/5).
- A new market state is produced every `tick_interval_ms` (500 ms, aligned to market-time
  boundaries, never stamped before the boundary is reached) **only for contracts whose
  product is in a continuous session** of the trading calendar at that market time.
  Auctions, breaks, nights without a night session and non-trading days produce nothing
  (Feed A then only sends heartbeats).
- **Trading-day roll**: a contract rolls as soon as the **first window of the new trading
  day opens, the opening auction included** (e.g. 20:55 for rb's night session):
  `pre_settle` = the sim's own settlement of the old day (SHFE/DCE full-day VWAP, CFFEX
  last-hour VWAP, rounded per exchange rule), `pre_close`, `pre_open_interest`, new limit
  prices, cumulative values reset. The roll itself publishes nothing - the feeds show the
  new day with the first continuous tick - but the SimBroker validates orders sent during
  the auction against the new band (it triggers the roll itself if the engine has not
  stepped yet, e.g. while paused).
- **Limits** = `pre_settle × (1 ± pct)` rounded inward to the tick, where `pct` is the
  percentage in force on that trading day (`ProductSpec.limit_pct_on`, i.e. the
  `limit_schedule` of `products.yaml`: DCE i/m widen from 6 % to 8 % on 20260930) - the same
  rule as `core.InstrumentRegistry.limit_prices` and md-gateway.
- Model: correlated GBM + Poisson jumps (Cholesky of a block matrix: market 0.2, sector
  0.6, same product 0.95), `vol_annual` / `jump_*` / `volume_per_s` per contract from
  `sim.yaml`, seeded from `pre_settle`. Prices are on the tick grid, clamped to the daily
  limits; beyond a limit the book is one-sided (limit lock).
- **Invariants** used by md-gateway's validator: cumulative volume/turnover never
  decrease within a trading day, and **the last price only changes together with a
  volume increase**. Turnover includes the multiplier.
- **Deterministic**: step *k* of trading day *D* draws from `default_rng([seed, D, k])`.
- Each round is persisted (`sim_md_state`) **before** it is published, so a restarted
  process continues the day's cumulative volume (it still gets a new Feed A epoch).
  `POST /control/pause` and shutdown wait for a round in progress, so once pause returns
  `head_seq` and every price are frozen, and a stop never cuts a round in half.
- Background loops (engine rounds, heartbeats, broker timer and delay lines, fault reaper)
  are supervised: a crash is logged with its traceback and the loop restarts.

## Feed A quick reference (02 §9.1)

Client → server:

```json
{"op": "subscribe", "req": 1, "instruments": "*", "resume": null}
{"op": "subscribe", "req": 2, "instruments": "*", "resume": {"epoch": "01a0…", "from_seq": 81236}}
{"op": "replay", "req": 3, "from_seq": 81236, "to_seq": 81240}
```

Server → client:

```json
{"op": "welcome", "req": 1, "epoch": "01a0…", "head_seq": 81235, "ring_low": 1, "hb_ms": 1000, "trading_day": "20260928"}
{"op": "snapshot", "as_of_seq": 81235, "ticks": [FeedATick, …]}
{"op": "replay_begin", "req": 2, "from_seq": 81236, "to_seq": 81300}
{"op": "tick", "seq": 81236, "replay": true, "d": FeedATick}
{"op": "replay_end", "req": 2, "to_seq": 81300}
{"op": "resume_rejected", "req": 2, "reason": "EPOCH_CHANGED", "ring_low": 1, "head_seq": 900}
{"op": "resume_rejected", "req": 2, "reason": "BAD_RESUME", "ring_low": 1, "head_seq": 900}
{"op": "heartbeat", "ts": 1790559300000, "head_seq": 81301}
{"op": "error", "req": 3, "code": "SEQ_EVICTED", "message": "…", "ring_low": 5000, "head_seq": 9000}
```

`FeedATick` uses CTP field names: `InstrumentID, ExchangeID, TradingDay, ActionDay,
UpdateTime, UpdateMillisec, LastPrice, Volume, Turnover, OpenInterest, BidPrice1,
BidVolume1, AskPrice1, AskVolume1, OpenPrice, HighestPrice, LowestPrice, UpperLimitPrice,
LowerLimitPrice, PreSettlementPrice, PreClosePrice, PreOpenInterest, AveragePrice`.
Missing prices are `DBL_MAX` (1.7976931348623157e308); `TradingDay` follows the calendar
(night session → next trading day), `ActionDay` is the natural date; `AveragePrice` is
already divided by the multiplier (0.0 before the first trade). Timestamps are market time.

Behaviour details (clarifications of 02 §9.1):

- one global `seq` across contracts; every restart (and `SIM_RESTART`) starts a new
  `epoch` with an empty ring (`ring_size`, default 100k) and `seq` from 1;
- the snapshot is sent on the first subscribe and after every `resume_rejected`
  (`EPOCH_CHANGED`, `SEQ_EVICTED` or `BAD_RESUME` = `from_seq > head_seq + 1`); after
  `SEQ_EVICTED` the part still in the ring can be fetched with `replay` on the same
  connection;
- **resume** (`subscribe` with `resume`): a closed block - live data produced meanwhile is
  held back and follows `replay_end` from `to_seq + 1`. A resume with nothing missing
  (`from_seq = head_seq + 1`) gets an empty block: `replay_begin {from_seq: head_seq + 1,
  to_seq: head_seq}` directly followed by `replay_end {to_seq: head_seq}`;
- **in-connection `replay`**: the replayed ticks (`replay: true`, sent in chunks of 200)
  are interleaved with the live stream, whose `seq` just continues (it never goes back to
  `to_seq + 1`); merge by seq. The range is clamped to `[ring_low, head_seq]` (the
  answer's `replay_begin` shows what is served); a range entirely below `ring_low` gets
  `error SEQ_EVICTED`, a range starting after `head_seq` gets `error BAD_REQUEST`
  (nothing to replay yet - never a block with `from_seq > to_seq`);
- heartbeats keep coming every `hb_ms`, also in the middle of long replays; any frame
  counts as liveness;
- every connection has its own queue and sender: a slow client never delays the others;
  a client whose backlog exceeds 200k messages is closed with code 1008. The server's
  closing handshake times out after 2 s;
- `instruments` may be a list (then the global seq has gaps, as 02 §9.1 warns);
- error codes: `NOT_SUBSCRIBED`, `ALREADY_SUBSCRIBED`, `SEQ_EVICTED` (02 §11.3) and
  `BAD_REQUEST` for malformed or out-of-range requests. Other paths than `/feed` get HTTP
  404; during `FEED_DISCONNECT` the handshake gets HTTP 503.

## Feed B quick reference (02 §9.2)

`GET /v1/quotes?symbols=RB2701,IF2612` (optional, case-insensitive) →

```json
{"ts": 1790559300123, "td": "20260928",
 "data": [{"sym": "RB2701", "ex": "SHFE", "px": "3118", "bp": "3117", "ap": "3118", "bv": 12, "av": 5,
           "vol": 570630, "amt": "17771239560", "oi": 1615888, "up": "3276", "dn": "2964",
           "ps": "3120", "pc": "3118", "o": "3115", "h": "3120", "l": "3100", "t": 1790559300000}]}
```

Prices are decimal strings with the tick's precision (`"850.02"`, `"108.005"`), `null`
when absent; `t` is the state's market time in epoch ms; `td` is the trading day of the
current market time (a contract that has not traded yet in that day keeps its previous
state and `t`). `GET /v1/health` → `{"status": "ok", "ts": …, "epoch": …, "paused": false}`
and stays 200 during feed faults (it reports process health to the supervisor).

## SimBroker quick reference (02 §9.4)

DEALER → ROUTER, one JSON object per frame, `msg` names it. Wire codes: `direction`
`"0"` buy / `"1"` sell; `comb_offset_flag` / `offset_flag` `"0"` open, `"1"` close, `"2"`
force close (broker only), `"3"` close today, `"4"` close yesterday; `order_price_type`
`LIMIT` / `ANY`; `time_condition` `GFD` / `IOC`; `volume_condition` `AV` / `CV`
(FAK = `IOC+AV`, FOK = `IOC+CV`); `order_status` `ACCEPTED`, `PART_TRADED`, `ALL_TRADED`,
`CANCELED`, `REJECTED`; `posi_direction` `LONG` / `SHORT`. Types follow CTP: `front_id`
and `session_id` are integers, `order_ref`, `order_sys_id`, `trade_id`, `max_order_ref` are
strings (integer `order_ref`s are accepted).

```json
{"msg": "ReqUserLogin", "req_id": 1, "broker_id": "9999", "user_id": "tg", "app_id": "futrisk"}
{"msg": "RspUserLogin", "req_id": 1, "trading_day": "20260928", "front_id": 1, "session_id": 7,
 "max_order_ref": "12", "broker_seq_head": 41, "error": null}

{"msg": "ReqOrderInsert", "req_id": 2, "order_ref": "13", "client_order_id": "01a0…", "investor_id": "ACC3",
 "instrument_id": "rb2701", "exchange_id": "SHFE", "direction": "0", "comb_offset_flag": "0",
 "order_price_type": "LIMIT", "limit_price": 3120.0, "volume_total_original": 2,
 "time_condition": "GFD", "volume_condition": "AV"}
{"msg": "RtnOrder", "broker_seq": 42, "trading_day": "20260928", "front_id": 1, "session_id": 7,
 "order_ref": "13", "order_sys_id": "0000000031", "client_order_id": "01a0…", "investor_id": "ACC3",
 "instrument_id": "rb2701", "exchange_id": "SHFE", "direction": "0", "comb_offset_flag": "0",
 "order_price_type": "LIMIT", "time_condition": "GFD", "volume_condition": "AV", "limit_price": 3120.0,
 "volume_total_original": 2, "volume_traded": 0, "volume_total": 2, "order_status": "ACCEPTED",
 "status_msg": "ACCEPTED", "insert_time": "09:35:01", "update_ns": 1790559301000000000}
{"msg": "RtnTrade", "broker_seq": 43, "trading_day": "20260928", "trade_id": "0000000019", "front_id": 1,
 "session_id": 7, "order_ref": "13", "order_sys_id": "0000000031", "client_order_id": "01a0…",
 "investor_id": "ACC3", "instrument_id": "rb2701", "exchange_id": "SHFE", "direction": "0",
 "offset_flag": "0", "price": 3119.0, "volume": 2, "trade_date": "20260928", "trade_time": "09:35:01",
 "trade_ts_ns": 1790559301020000000}
{"msg": "ErrRtnOrderInsert", "req_id": 2, "order_ref": "13", "client_order_id": "…", "order_sys_id": "…",
 "front_id": 1, "session_id": 7, "error_id": "BROKER_REJECT_CLOSE_TODAY", "error_msg": "…"}

{"msg": "ReqOrderAction", "req_id": 3, "front_id": 1, "session_id": 7, "order_ref": "13", "action": "DELETE"}
{"msg": "ReqOrderAction", "req_id": 4, "order_sys_id": "0000000031", "action": "DELETE"}
{"msg": "ErrRtnOrderAction", "req_id": 4, "error_id": "BROKER_ORDER_FINISHED", "error_msg": "…", …}

{"msg": "ReqQryOrder", "req_id": 5, "trading_day": "20260928", "from_broker_seq": 40}
{"msg": "RspQryOrder", "req_id": 5, "items": [RtnOrder fields…], "is_last": true}
{"msg": "ReqQryTrade", "req_id": 6, "trading_day": "20260928", "from_broker_seq": 40}
{"msg": "RspQryTrade", "req_id": 6, "items": [RtnTrade fields…], "is_last": true}
{"msg": "ReqQryInvestorPosition", "req_id": 7, "trading_day": "20260928", "investor_id": "ACC3"}
{"msg": "RspQryInvestorPosition", "req_id": 7, "trading_day": "20260928", "is_last": true,
 "items": [{"investor_id": "ACC3", "instrument_id": "rb2701", "posi_direction": "LONG",
            "today_position": 2, "yd_position": 0, "position": 2}]}
{"msg": "Heartbeat", "ts": 1790559302000, "broker_seq_head": 43, "trading_day": "20260928"}
{"msg": "RspError", "req_id": 9, "error_id": "BROKER_NOT_LOGGED_IN", "error_msg": "login first"}
```

Semantics:

- `broker_seq` covers `RtnOrder` and `RtnTrade`, increases across sessions within a
  trading day and restarts at 1 on the next trading day (queries take `trading_day`).
  Every event is written to `sim.db` before its reports are sent, so `broker_seq`,
  `trade_id`, `order_sys_id` and `session_id` never repeat across restarts.
- Reports go to every logged-in session. A fill is reported `RtnTrade` first, then the
  order's new status. A rejected order gets both a `RtnOrder` (`REJECTED`,
  `status_msg` = reason code, it has a `broker_seq`) and an `ErrRtnOrderInsert`.
- `ReqQryOrder` returns each order's **current** state whose latest report has
  `broker_seq >= from_broker_seq` (like CTP), so the resynced `broker_seq`s have permanent
  holes (02 §9.4: advance the cursor to the largest one seen); `ReqQryTrade` returns trades
  with `broker_seq >= from_broker_seq`. Pages of 500 items (`is_last` on the last page).
  Past trading days are answered from `sim.db`.
- `ack_delay_ms` delays every request and `fill_delay_ms` the first match of a new order;
  both are FIFO delay lines in front of the worker: they never block other processing
  and never reorder (an insert followed at once by its cancel is processed in that order).
- `ReqQryInvestorPosition` for a trading day after the counter's current one shows
  today's lots as yesterday's (the roll happens when the market clock enters that day).
- Re-sending a known `client_order_id` never creates a second order: the current
  `RtnOrder` is sent back (same `broker_seq`).
- Matching: a new order's first match is aggressive - marketable orders fill at the
  opposite best price up to the level-1 size (sizes are consumed within one market
  state); market orders likewise, the rest is cancelled; FAK cancels the rest, FOK fills
  completely or is cancelled (IOC and market orders never rest). A resting GFD limit
  fills at its own price when the market crosses. No liquidity on a limit-locked side.
  Orders are matched only in continuous sessions: orders accepted during an opening
  auction are validated against the **new** trading day's band; GFD ones wait for the
  continuous session, IOC ones are cancelled (`NOT_IN_CONTINUOUS_SESSION`).
  `POST /control/pause` holds matching; orders keep arriving and are matched on resume.
  An order still waiting for its first match keeps that state across a restart
  (`sim_orders.pending_match`), so it is later matched at the opposite price, never
  as a resting order. At the last session close of the order's trading day every live
  order is cancelled (`status_msg=SESSION_CLOSE`).
- Positions: SHFE `"3"` closes today's position, `"1"`/`"4"` yesterday's (CTP);
  other exchanges close in `close_style` order (FIFO: yesterday first). Checks are net
  of pending closes. Force close (`"2"`) first cancels the pending close orders on that
  position (`status_msg=FORCE_CLOSE_CANCELED`, listed in the result's `canceled_orders`),
  then closes yesterday's lots first, then today's.
- Reason codes (`error_id`, `status_msg` of rejects): `BROKER_REJECT_CLOSE_TODAY`,
  `BROKER_REJECT_CLOSE_YD`, `BROKER_REJECT_PRICE` (missing / off-grid / outside limits),
  `BROKER_ORDER_NOT_FOUND`, `BROKER_ORDER_FINISHED` (02 §11.3), plus sim extensions:
  `BROKER_REJECT_POSITION` (non-SHFE close above the position), `BROKER_REJECT_SESSION_CLOSED`,
  `BROKER_REJECT_INVALID` (unknown contract, exchange mismatch, bad enum/quantity, above
  the max order size), `BROKER_DUPLICATE_ORDER_REF`, `BROKER_NOT_LOGGED_IN`,
  `BROKER_LOGIN_FAILED`, `BROKER_BAD_REQUEST`. Cancel reasons in `status_msg`:
  `USER_CANCELED`, `FAK_REMAINDER_CANCELED`, `FOK_CANCELED`, `MARKET_REMAINDER_CANCELED`,
  `NOT_IN_CONTINUOUS_SESSION`, `SESSION_CLOSE`, `FORCE_CLOSE_CANCELED`.
- The counter does not check funds (`BROKER_REJECT_FUNDS` is never produced) and accepts
  any `investor_id`.

## Control API quick reference (02 §9.5)

| Method and path | Body | Answer |
|---|---|---|
| `GET /control/state` | — | `{epoch, trading_day, faults, fault_history, scenario, prices, paused, market_now_ns, clock, shocks, instruments, engine, feed_a, feed_b, broker}` |
| `POST /control/faults` | `{type, target?, params, duration_s?}` | `{fault_id, state, fault}` |
| `DELETE /control/faults/{fault_id}` | — | `{ok}` |
| `POST /control/shock` | `{instruments?, sector?, pct, duration_s, shape: STEP\|LINEAR}` | `{shock_id, shock}` |
| `GET /control/scenarios` | — | `[{name, description}]` |
| `POST /control/scenario` | `{name, speed?, vars?}` | `{run_id, name}` |
| `DELETE /control/scenario` | — | the cancelled run (extension) |
| `POST /control/pause` | — | `{ok}` |
| `POST /control/clock` | `{offset_ns, version}` — only while paused, `version` must not go back | `{ok, market_now_ns}` |
| `POST /control/resume` | — | `{ok}` |

Errors: `{"ok": false, "code": "BAD_REQUEST"|"NOT_FOUND"|"CONFLICT", "message": …}` with
HTTP 400 / 404 / 409. Ratios are decimals (README §3.4): `pct: -0.04` is −4%.
Swagger UI: `/control/docs`.

Faults (`target` is `feed_a` / `feed_b`):

| `type` | `target` | `params` | Effect |
|---|---|---|---|
| `FEED_DISCONNECT` | `feed_a` \| `feed_b` | `refuse_reconnect_s`, `mode` (`close` \| `silent`, feed_a) | feed_a: close connections (or stop sending, so missed heartbeats reveal it) and answer handshakes with 503; feed_b: HTTP 503. Lasts `duration_s`, else `refuse_reconnect_s`; with neither, feed_a/`close` is a one-shot drop and the others last until deleted |
| `FEED_GAP` | `feed_a` | `drop_count` | the next N live ticks are not sent (they stay in the ring: replayable) |
| `FEED_DUPLICATE` | any feed (default feed_a) | `ratio` | feed_a: a tick is sent twice; feed_b: the previous body is served again |
| `FEED_REORDER` | any feed (default feed_a) | `window` | feed_a: live ticks shuffled in windows; feed_b: an older snapshot is served |
| `FEED_LATENCY` | any feed (default feed_a) | `ms` | every queued message (feed_a: ticks, heartbeats, snapshots, resume replays; not in-connection replay chunks) / answer (feed_b) is delayed |
| `FEED_FREEZE` | none (engine) \| feed | `instrument` | none: the contract stops trading; feed: that feed stops updating it |
| `BAD_TICK` | none (both) \| feed | `instrument`, `offset_pct`, `with_volume` | one bad tick (price × (1+offset), on the grid, not clamped; with or without a trade) |
| `MALFORMED` | `feed_b` | `ratio`, `mode` (`body` \| `record`) | truncated JSON body, or one record with `px: "N/A"` |
| `SIM_RESTART` | — | — | new Feed A epoch + empty ring + connections dropped; broker sessions dropped and state reloaded from sim.db |
| `BROKER_FORCE_CLOSE` | — | `account_id`, `instrument_id`, `qty?`, `posi_direction?` | cancels the pending close orders on that position, then an external order + trade (`offset_flag "2"`, `client_order_id null`) at the opposite best price; the answer's `fault.detail.result` has the trade and `canceled_orders` |
| `BROKER_DISCONNECT` | — | `seconds` | ROUTER closed for N s, sessions dropped (clients must log in again) |

`duration_s` omitted: until deleted (one-shot types — `FEED_GAP`, `BAD_TICK`, `SIM_RESTART`,
`BROKER_FORCE_CLOSE` — end by themselves).

## Scenarios (`config/scenarios/*.yaml`)

```yaml
name: breaker
description: shown by GET /control/scenarios
vars: {sector: black, leg1_pct: -0.024, ramp_s: 60}   # overridable: POST {"name": …, "vars": {…}}
steps:
  - {at_s: 0, action: shock, params: {sector: "${sector}", pct: "${leg1_pct}", duration_s: "${ramp_s}", shape: LINEAR}}
  - {at_s: 30, action: fault, ref: a_down, params: {type: FEED_DISCONNECT, target: feed_a, params: {refuse_reconnect_s: 20}}}
  - {at_s: 60, action: clear_fault, params: {ref: a_down}}
```

Actions: `shock`, `fault`, `clear_fault`, `clear_faults`, `pause`, `resume`, `log`.
`speed` divides `at_s`, `duration_s` and the duration-type fault params
(`refuse_reconnect_s`, `seconds`). One scenario runs at a time.

- `breaker` — QS-03: black sector −2.5 %, −0.48 %, −0.88 % in LINEAR legs a minute apart (30 s
  ramps) so ACC3 crosses the −2 % warn, −2.5 % restrict and −3 % halt levels one per leg
  (calibration notes in the file);
- `feed_outage` — QS-05: Feed A down 20 s with Feed B up, then a seq gap, duplicates and
  reordering on Feed A;
- `all_feeds_outage` — QS-06: both feeds down 35 s.

## sim.db (03 §4.7)

`sim_meta` (single row: `trading_day`, `last_broker_seq`, `last_trade_no`, `md_epoch`, plus
`last_order_no`, `last_session_id`, `md_rng_td`, `md_step`), `sim_orders` (plus
`pending_match`: waiting for the first aggressive match), `sim_trades`, `sim_positions` and
— an addition to 03 §4.7 — `sim_md_state` (per-contract cumulative day state of the price
engine, so a restart keeps volume monotonic). One writer thread, WAL, `BEGIN IMMEDIATE`;
`PRAGMA user_version` = 2 (a version-1 file gets the `pending_match` column on open).
