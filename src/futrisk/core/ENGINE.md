# EngineCore — notes for the risk-engine shell and the backtester

`futrisk.core.engine.EngineCore` is the single-writer brain of one risk-engine shard
(01 §4 "一致性域不拆", §5.3.1, ADR-004). It owns positions, funds, metrics, rule state
machines, restrictions, the pre-trade pipeline, orders, firm controls and the EOD state of
the shard's accounts. It is **pure and deterministic**: no I/O, no threads, no system
clock (time = injected `Clock.now_ns()` / `monotonic_ns()`), no random ids (injected
`IdGen`). Every input returns an ordered `list[Effect]`; the shell (sockets, SQLite,
timers) and the backtester (in-process bus) execute them. Same inputs + same clock/ids ⇒
identical effect lists (tested).

Module map: `effects` (Effect model), `engine` (facade), `metrics` (valuation, metric
registry, `WhatIf` scalar what-if, `StaticCache`), `rules` (compiled rules), `state_machine`
(alert state machine), `evaluation` (scope expansion, `min_gross_pct` gate, transitions →
rows/events, RuleStatus), `actions` (`@register_action`, liquidation planner),
`restrictions` (Restriction building, matching, state aggregation), `pretrade` (9 steps),
`orders` (order records, lifecycle helpers, order book with per-instrument index),
`alerts` (alerts, risk events, audit rows), `firm` (FirmState, coordinator aggregator),
`eod` (freeze digests, digest-only acks, cash attribution, provisional SOD),
`risk_account` (per-account runtime state).

## 1. Construction

```python
EngineCore(
    shard=1, registry=InstrumentRegistry(cfg.products, cfg.calendar), rules=cfg.risk_rules,
    accounts=cfg.accounts, clock=market_clock, ids=Uuid7Gen(), calendar=cfg.calendar,
    instance="risk-engine-1", coordinator=False,            # shard 0: coordinator=True
    config_hash=cfg.config_hash, config_version=cfg.config_version,
    coordinator_instance="risk-engine-0", auto_provisional=True,
    query_trades_on_roll=None,                              # None: same as auto_provisional
)
```

`clock.now_ns()` must be **market time** (demo offset applied, 01 §8.2); `monotonic_ns()`
only measures `risk_check_us` and drives the order throttle. The backtester passes a
`SimClock` and `SeqIdGen` and uses `auto_provisional=False` (it always calls `roll_day`
itself; this also switches `query_trades_on_roll` off). The coordinator's aggregator
expects every shard of `accounts.yaml` (`accounts.shards()`).

## 2. Main loop (02 §8.1) → engine calls

| Loop step | Shell does | Engine call |
|---|---|---|
| 0 heartbeats | md-gateway down / up; trade-gateway down / up; (coordinator) heartbeats of risk-engine-k | `set_md_down(bool)`, `set_gateway_status(bool)`, `note_shard_alive(shard, ts_ns)` |
| 1 market data | conflate `md.tick.*` per instrument; `md.status` | `apply_mark(tick)` (step 5; `live=False` for ticks of a `ctl.snapshot`), `apply_md_status(status)` (re-apply the latest one after a snapshot flush) |
| 2 `td.exec.{acct}` (FACT, seq order) | decode `Trade` / `BrokerOrderUpdate` | `apply_exec(msg)`; add your `fact_cursors` row to the returned Persist (write it alone when there is none, e.g. a duplicate) |
| 2' `risk.control.firm` (non-coordinator shards) | FACT subscription | `apply_firm_control(control)` (+ `fact_cursors`) |
| 2'' `risk.snapshot.*` of other shards (coordinator) | LV subscription | `ingest_snapshot(snapshot)` |
| 3 replies of outbound requests | match by token / client_order_id | `on_route_ack(coid, reply)`, `on_cancel_result(coid, reply)`, `on_resolve_result(coid, reply)`, `on_request_result(token, reply)`; `on_route_timeout(coid)` when the RPC client gives up on `route.order` |
| 4 commands (≤256 / loop) | build `CommandCtx(command, token, requester, received_ns)` | `handle(payload, ctx)` — evaluates a dirty account first, then the pre-trade pipeline |
| 5 marks | slots → ticks | `apply_mark(tick)` (no effects; marks holders dirty through the by_instrument index) |
| 6 dirty accounts | once per loop | `evaluate_dirty()` (or `evaluate(account_id)`) |
| 7 timers | poll timeout = `next_timer_ns()` | `on_timer(now_ns)` |

`apply_mark` ignores ticks whose `order_key` is not newer than the last applied one and
ticks of another trading day (they are kept and used after the roll; the day's last mark
per instrument is kept separately for the provisional SOD).

Quality (01 §8.13): **`md.status` is the authority.** The effective quality of an
instrument is its latest `md.status` quality; only for an instrument that has no
`md.status` entry does a **live** tick (`apply_mark(tick)`, the `md.tick` stream, LIVE-only
at md-gateway) make it OK; instruments never seen are UNKNOWN (fail-safe). A live tick never
raises the quality above the latest `md.status`. `apply_mark(tick, live=False)` (a tick
from `ctl.snapshot` of md-gateway's LV cache, possibly old) only moves the mark / price:
no quality change and no new LIVE market state. `set_md_down(True)` makes every
instrument STALE until `set_md_down(False)` — an `md.status` never clears it. A live tick
of a **held** instrument is a new LIVE market state for `confirm.states`; fills, cash
movements, snapshot ticks and ticks of instruments with working orders only re-evaluate
the account.

`next_timer_ns()` only reports timers that can make progress: before the first
`on_timer` the snapshot / sample / recon (/ firm) timers are due at once; quality-gated
rule timers, a provisional trigger already waiting for `md.settle_inputs` and order
checks while the trade-gateway is down are left out, so it never returns a past time with
nothing to do.

## 3. Effects and their execution contract (02 §8.1 "执行 Effect 的顺序")

Every list is already ordered: `Persist` (at most one) → external actions → `Publish` →
`ReplyTo`. Execute strictly in that order:

1. **`Persist(rows, outbox, critical, label)`** — one transaction: apply `rows` in order
   (see §6), append `outbox` messages to the transactional outbox (`OutboxMsg(topic,
   payload, corr_id, td, ts_ns)`; use `ts_ns` as `outbox.created_ns`), commit, wait for
   the commit. `critical=True` ⇒ `synchronous=FULL` for this commit (03 DB-03: latch /
   reset, manual halt, kill switch and firm controls, cash, EOD freeze / roll / provisional
   / catch-up / SOD load, recon breaks, liquidation plans, manual commands, order rebuild,
   config audit). Outbox rows are published by the outbox sink after the commit — never
   publish them twice.
2. **External actions** (async, never block the loop):
   - `Route(order: RouteOrder)` → `route.order` to trade-gateway; retry with the same
     `client_order_id` until `order.expire_ns`; result → `on_route_ack(coid, reply)`;
   - `CancelRoute(client_order_id, command_id, reason)` → `route.cancel{command_id,
     client_order_id, reason}`; result → `on_cancel_result(coid, reply)`. Retry transport
     failures with the **same** `command_id` only while `orders.get(coid).cancel_command_id
     == command_id` (the core re-sends a stuck cancel with a new id);
   - `ResolveRoute(client_order_id, trading_day)` → `route.resolve`; result →
     `on_resolve_result(coid, reply)` (also for TIMEOUT / UNAVAILABLE errors);
   - `Request(token, target, command, payload, timeout_ms)` → any other command
     (`settle.sod_snapshot` to clearing-worker, `md.settle_inputs` to md-gateway,
     `td.query.positions` / `td.query.trades` / `td.query.orders` to trade-gateway); pass
     the Reply — or an error Reply you build for TIMEOUT / UNAVAILABLE — to
     `on_request_result(token, reply)`. Pending tokens: `pending_requests()`.
3. **`Publish(topic, payload)`** — LV only: `risk.snapshot.{account}` (AccountSnapshot at
   `snapshot_hz` when changed, and **every account at least once per second** as a
   keep-alive for the coordinator) and `risk.firm` (FirmSnapshot, coordinator, 1 Hz).
4. **`ReplyTo(token, reply)`** — answer the command whose `CommandCtx.token` this is
   (`eod.rollover` replies only after its `settle.sod_snapshot` result arrived).

If the process dies before step 1 commits, nothing happened; after a restart the core
re-evaluates deterministically from the persisted state and produces the same
transitions, episodes and alert keys (no duplicate alerts). Between 1 and 3 the outbox
republishes; a `CancelRoute` lost between 1 and 2 is re-driven by `post_restore`.

## 4. Public API

Commands and loop hooks (all return `list[Effect]` unless stated):

| Method | Notes |
|---|---|
| `handle(payload: Msg, ctx: CommandCtx)` | every risk-engine command of 02 §6.3, see §5 |
| `apply_mark(tick: Tick, *, live=True) -> None` | tick; incremental valuation of holders only; `live=False` for snapshot ticks (mark only, see §2) |
| `apply_md_status(status: MdStatus) -> None` / `set_md_down(down=True) -> None` | quality per instrument; md-gateway lost ⇒ all STALE until `set_md_down(False)` |
| `apply_exec(msg: Trade \| BrokerOrderUpdate)` | fills (any order state, dedupe by trade key), broker order updates (a live state restated with a `broker_code` for a CANCEL_PENDING order = refused cancel → back to the working state) |
| `on_route_ack(client_order_id, reply)` / `on_route_timeout(client_order_id)` | RouteAck OK → ROUTED; ROUTE_* / GATEWAY errors → BROKER_REJECTED (CANCELED if a cancel was pending); TIMEOUT/UNAVAILABLE/BUSY → no change |
| `on_cancel_result(client_order_id, reply)` | OK (`CANCEL_PENDING`, durable at the gateway) → no change, outcome on `td.exec`; tombstone (`EXPIRED` / `ROUTE_LOST`) → CANCELED; `REJECTED` → BROKER_REJECTED; transport errors → no change (retry same id); other errors → stays CANCEL_PENDING, checked at the next timer |
| `on_resolve_result(client_order_id, reply)` | see §7 "Orders and cancels" |
| `on_request_result(token, reply)` | results of `Request` effects |
| `evaluate(account_id)`, `evaluate_dirty()`, `evaluate_all()`; `dirty_accounts() -> list[str]` | metrics → rules → state machines → actions; no-ops while LOADING (accounts stay dirty) |
| `on_timer(now_ns=None)`; `next_timer_ns() -> int \| None`; `next_rule_due_ns() -> int \| None` | provisional trigger, route expiry (ROUTING → UNKNOWN + ResolveRoute), UNKNOWN resolves, CANCEL_PENDING TTL checks, scheduled halt re-cancels, confirm / hold / cooldown timers (`next_rule_due_ns`), snapshots (+ keep-alive), metric samples, periodic position recon, coordinator firm tick. `snapshot_hz`, `metric_sample_s`, `recon_interval_s` = 0 switch those periodic timers **off**: `next_timer_ns()` is then only rule / order / provisional timers (None when nothing is pending), so a backtest can drive time with it alone |
| `set_lifecycle(Lifecycle)` | LOADING / WARMUP (`SYSTEM:STARTING`) / RUNNING / STOPPING |
| `set_system_restriction(account_id \| None, reason, on)`; `clear_system_restriction(account_id \| None, reason)` | `FILL_GAP`, `RECON_BREAK`, `EOD_NOT_SETTLED`, `STARTING`, `MD_UNKNOWN` (MD_UNKNOWN is also managed automatically). Clear FILL_GAP (also one restored from `account_states`) after the fact catch-up |
| `open_recon_break(account_id, kind, detail, *, blocking=True)` | shell-detected breaks (e.g. `REPLAY_EXPIRED`); blocking ⇒ `SYSTEM:RECON_BREAK`; an identical open break is not repeated |
| `set_gateway_status(up)` | down: new orders `REJECT_GATEWAY_UNAVAILABLE`, ROUTING → UNKNOWN, order checks pause; up: every UNKNOWN / CANCEL_PENDING order is resolved at once, due halt re-cancels sent |
| `request_position_recon()` | `td.query.positions` now (warm-up recon, 02 §8.6/§8.7) |
| `apply_firm_control(control)`; `ingest_snapshot(snapshot) -> None`; `note_shard_alive(shard, ts_ns=None) -> None`; `firm_snapshot() -> FirmSnapshot` | firm layer (§10); `RULE_STATE` replaces the whole RULE layer |
| `load_sod(sod)` | first load (seed push while LOADING); **the lifecycle is left unchanged** (nothing is evaluated while LOADING: the shell calls `set_lifecycle(WARMUP)` after its fact catch-up, the backtest `set_lifecycle(RUNNING)`) |
| `roll_day(sod, mode=None)` | ROLL / REBASE / PROVISIONAL (mode inferred when None); backtests call it once per day; raises `ValueError` (nothing changed) for an invalid SOD |
| `freeze(trading_day, version=0) -> EodFreezeAck` | backtest helper (no persistence); the shell uses `handle("eod.freeze")` |
| `freeze_digest_source(trading_day) -> "MEMORY" \| "STORE" \| None` | who answers `eod.freeze{X}` (§9) |
| `last_official_day() -> str \| None` | day of the newest official (clearing) SOD in the chain to the current day |
| `enter_provisional(prices=None)` | force the PROVISIONAL transition; `prices` are the provisional settlement prices of those instruments |
| `restore(tables, *, lifecycle=WARMUP, intraday_peaks=None, dayrolls=()) -> None`; `post_restore()` | recovery (§8) |
| `update_rules(rules, *, config_hash="", config_version=0)` | hot reload, **returns effects the shell must execute** (state refresh, audit rows, ALERT_CLEARED of removed rules) |

Read-only helpers: `snapshot(account_id) -> AccountSnapshot`, `account(account_id) ->
RiskAccount`, `account_ids()`, `holders(instrument_id)`, `quality_of(instrument_id)`,
`mark_of(instrument_id)`, `limits_of(instrument_id)`, `session_open(instrument_id,
now_ns)`, `kill_switch_effective()`, `valuation(account)`, `pretrade_rules()`,
`static_info(instrument_id)`, properties `trading_day`, `trading_mode`, `lifecycle`,
`sod_version`, `provisional`, `gateway_up`, `orders`, `registry`, `calendar`,
`pretrade_cfg`.

## 5. Commands (`handle`)

| Command | Payload → reply data | Idempotency | Notes |
|---|---|---|---|
| `order.submit` | `OrderRequest` → `OrderAck` | `client_order_id` + `order_request_hash` (in memory + `orders` rows) | 9-step pipeline; rejected orders are persisted as `REJECTED` **except** unknown account / instrument, duplicate ids and orders that cannot be stored (qty ≤ 0 or > 2³¹−1, price not a positive finite number: 03 §4.3 `CHECK(qty>0)`), which only get a reply; LOADING / STOPPING → `REJECT_SYSTEM_NOT_READY` without evaluation or row; accepted: `ROUTING` row + `Route`; same id + same content → first reply with `duplicate=true`; other content → `REJECT_DUPLICATE_ORDER_ID`; an id already used by an order of an earlier day (pruned from memory in this process) → `REJECT_DUPLICATE_ORDER_ID` without a row (ids from before the last restart: the shell's PK lookup); owned-by-other-shard → `NOT_OWNER` |
| `order.cancel` | `CancelRequest` → `OrderUpdate` | `command_id` | terminal → `CONFLICT`; cancel limits (`REJECT_CANCEL_LIMIT` / `REJECT_LARGE_CANCEL_LIMIT`); `CancelRoute` with the request's `command_id`; on a CANCEL_PENDING order: "cancel already pending" within its TTL, else (TTL passed or the last `route.cancel` answer was an error) the cancel is **re-sent** with this `command_id` ("cancel re-sent", not counted as a new cancel) |
| `order.cancel_all` | `CancelAllRequest` → `CancelAllAck` | `command_id` | `OPEN_ONLY` / `ALL`, optional product |
| `risk.reset` | `ResetRequest` → `ResetAck` | `command_id` | RISK_MANAGER / ADMIN, non-empty reason; latched levels → REARMED (rearm line), manual halt released; a latched level whose rule was removed by a hot reload is released and dropped |
| `risk.halt` | `HaltRequest` → `AccountStateChanged` | `command_id` | latched at once (machine `manual_halt/halt/acct:{A}`), MANUAL_HALT event, cancels resting opens |
| `risk.ack_alert` | `AckAlertRequest` → — | `command_id` | acknowledgement only (ALERT_ACKED, INFO) |
| `risk.liquidation.confirm` | `LiquidationConfirm` → `LiquidationPlan` | `command_id` | CONFIRM / CANCEL, records the decision only — never places orders (TD-08); emits `risk.event` LIQUIDATION_PLAN with `detail {plan_id, status, actor}` (WARNING for CONFIRMED, INFO for CANCELED; EXPIRED at the day roll: INFO) |
| `risk.recon.resolve` | `ReconResolveRequest` → — | `command_id` | lifts `SYSTEM:RECON_BREAK` when no blocking break remains |
| `cash.move` | `CashMoveRequest` → `CashMovement` | `movement_id` | ADMIN; trading day by EOD state (02 §6.3); a withdrawal must be ≤ current available − pending next-day withdrawals (also while EOD_FROZEN) |
| `firm.kill_switch` | `KillSwitchRequest` → `FirmControl` | `command_id` | MANUAL flag (`KILL_SWITCH_ON/OFF`, `restrictions=[]`); effective = manual OR rule; rejects new opens only (resting opens stay); a KILL_SWITCH_ON/OFF event only when the MANUAL flag changes |
| `eod.freeze` | `EodFreezeRequest` → `EodFreezeAck` | state (`eod_states`) | digests recomputed on every call; `freeze_digest_source(X)`: MEMORY → answered here (current day: freezes; PROVISIONAL's source day: digests only); STORE → the core replies `NOT_FOUND`, the shell answers (§9); None → `PRECONDITION_FAILED` |
| `eod.rollover` | `EodRolloverRequest` → `DayRoll` | `(next_trading_day, sod_version)` | emits `Request(settle.sod_snapshot{version})`; replies after the result: ROLL / REBASE / CATCH_UP (§9); contiguity enforced; same `(Y, V)` again → the first `DayRoll` with `duplicate=true` (also after a restart); while LOADING → loads the SOD; an invalid SOD → `PRECONDITION_FAILED`, nothing changed |
| `eod.force_unfreeze` | `ForceUnfreezeRequest` → — | `command_id` | only in PROVISIONAL |
| `risk.query.accounts` / `positions` / `orders` / `fills` / `cash` / `recon_breaks` / `alerts` / `rules` / `liquidation_plans` | 02 §6.3 batches; `QueryLiquidationPlans{trading_day?, account_id?, status?}` → `LiquidationPlanBatch{items}` | — | answered from memory for the current trading day |
| `risk.query.metrics`, and `orders` / `fills` / `alerts` / `cash` / `liquidation_plans` of other trading days | — | — | the core replies `NOT_FOUND`; **the shell answers them from risk.db** (`metric_samples`, `orders`, `fills`, `risk_events`, `cash_movements`, `liquidation_plans WHERE trading_day=? [AND account_id=?] [AND status=?] ORDER BY created_ns`) and should intercept them before calling the core |

Idempotent commands are remembered in memory and written to `processed_commands`
(`request_hash` is **the** system algorithm `futrisk.messages.base.request_hash`, also used
by `futrisk.db.processed`: the request's own time fields are ignored at the top level only,
nested data is hashed as it is; the stored `reply` is msgpack bytes of the first `Reply`).
Role checks (02 §4.11) are repeated in the core.

## 6. Persist rows → 03 §4.3 tables

Rows carry column names exactly as in 03 §4.3. `Row.op`: `INSERT` (append; autoincrement
key omitted), `INSERT_IGNORE` (`ON CONFLICT(key) DO NOTHING`), `UPSERT` (`ON
CONFLICT(key) DO UPDATE` all given columns), `UPDATE` (set non-key columns where key
matches), `DELETE` (rows matching the key columns). JSON columns hold Python lists /
dicts; money and prices are `DecStr`; booleans are `bool`; `processed_commands.reply` is
`bytes`.

| Table | Op / key | Written when |
|---|---|---|
| `orders` | **INSERT** (full row) when the order is created (submit incl. storable REJECTED, rebuild); afterwards **UPDATE** `client_order_id` with the **changed columns only** (from `status`, `filled_qty`, `avg_fill_price`, `reject_code`, `reject_detail`, `broker_order_ref`, `frozen_margin`, `frozen_fee`, `updated_ns`, `cancel_command_id`, `cancel_sent_ns`, `cancel_attempts`) | every status change, broker ref change, cancel re-send; `cancel_*` columns: `cancel_command_id TEXT NULL`, `cancel_sent_ns INT NULL`, `cancel_attempts INT DEFAULT 0`. An INSERT never meets an existing row: ids are never reused (see `order.submit`) |
| `order_events` | INSERT (`event_id` autoinc) | every status change (`from_status` null on creation); a cancel re-send writes a note with `from_status = to_status = CANCEL_PENDING`, `reason = CANCEL_RETRY` (or the manual cancel reason), `detail {attempt, command_id}`; a refused cancel's revert has `detail.cancel_refused = true` |
| `fills` | INSERT_IGNORE `(trading_day, exchange, trade_id, direction)`; `fill_seq` autoinc | every applied trade; `client_order_id` null for external / unknown orders; `close_today_qty`, `close_yd_qty`, `fee_est`, `applied_ns` |
| `cash_movements` | INSERT_IGNORE `movement_id` | `cash.move` |
| `rule_states` | UPSERT `(trading_day, rule_id, level, scope_key)` | any persisted machine change (transition, confirm count, clear timer incl. its reset by quality gating), reset, manual halt (`rule_id=manual_halt`), carried latch |
| `alerts` | INSERT_IGNORE `alert_key` on raise; UPDATE `alert_key` for `status` / `cleared_ns` / `reset_ns` / `reset_by` / `acked_ns` / `acked_by` | ALERT_RAISED, CLEARED (also when a hot reload removes an active non-latched level), RESET, ACK, ROLLED |
| `risk_events` | INSERT_IGNORE `event_id` | every `RiskEvent` (also in the outbox of `risk.event`) |
| `account_states` | UPSERT `account_id` | every state / latch / restriction change; carries `state_version` |
| `firm_controls` | INSERT (`control_id` autoinc) | manual kill switch (`KILL_SWITCH_ON/OFF`, `active` = the flag), applied `risk.control.firm` / coordinator's own controls (`RULE_STATE`: `active` = the rule kill switch, `restrictions` = every firm rule restriction in force; legacy `SET_RESTRICTIONS` rows have `active` 0) |
| `liquidation_plans` | UPSERT `plan_id` | creation, confirm / cancel, EXPIRED at the day roll |
| `audit_events` | INSERT (`audit_id` autoinc) | reset, halt, ack, kill switch, cash, liquidation decision, recon resolve, force unfreeze, day roll / rebase / provisional / catch-up (`DAYROLL` with `after.mode = CATCH_UP`), hot reload keeping a latch (`CONFIG_COMMIT`), order rebuilt from the broker (`RECON_RESOLVE` with `target = order:{coid}`, `after.kind = ORDER_REBUILD`) |
| `metric_samples` | INSERT_IGNORE `(account_id, ts_ns)` | every `metric_sample_s`; `rule_util` = `{"{rule_id}/{scope_key}": utilization}` |
| `eod_states` | UPSERT `trading_day` | freeze, provisional self-freeze / roll, roll, rebase, catch-up, force unfreeze |
| `recon_breaks` | UPSERT `break_id` | POSITION_MISMATCH (blocking: recon, late / unbookable / un-replayable fills with `detail.trade_key`; non-blocking: `detail.reason = SOD_VS_EOD`), ORDER_STATE_CONFLICT (non-blocking: fill after terminal, `CANCEL_STUCK`, `SESSION_CLOSE_STILL_WORKING`), shell breaks via `open_recon_break`, resolutions |
| `sod_accounts` / `sod_lots` / `sod_prices` | DELETE `trading_day` then INSERT | SOD load, roll, rebase, provisional (`sod_version=-1`, `provisional=true`), catch-up (official SOD of an older day) |
| `processed_commands` | INSERT_IGNORE `command_id` | every idempotent command (incl. error results) |
| `retro_breaches` | — | not produced (RETRO shadow evaluation is SHOULD; not implemented) |
| `fact_cursors` | — | the shell's (same transaction as `apply_exec` / `apply_firm_control`) |

Outbox (FACT) topics: `oms.order.{account}` (`OrderUpdate`, corr = client_order_id),
`risk.state.{account}` (`AccountStateChanged`), `risk.event` (`RiskEvent`, corr =
alert_key or event_id), `risk.cash.{account}` (`CashMovement`), `sys.dayroll`
(`DayRoll`, modes ROLL / REBASE / PROVISIONAL / CATCH_UP), `risk.control.firm`
(`FirmControl`, coordinator only).

## 7. Risk model implemented (01 §8.14)

- Metrics (`metrics.py`, 02 §7.6): daily P&L = equity − static equity, `% init`
  (acceptance), `% SOD`, `floating_pnl_pct_init`, perf equity, NAV, intraday peak / dd
  and HWM / dd on perf equity, gross / net notional, leverage, effective N = 1/HHI of
  |net notional| by product (None when flat), limit stress, orders / cancels, compliance
  utilization, per product net % and utilization = net% / (3% ÷ limit%), per sector net %.
  Names resolve through `@register_metric` (`account.*`, `product.*`, `sector.*`,
  `firm.*`). Quality = worst MdQuality of the instruments involved.
- Rules: per-rule `hysteresis` (e.g. nav_floor 0.02 → clear lines 0.918 / 0.867),
  `hold_ms`, `cooldown_s`; `latch: true` only on CIRCUIT_BREAK / LIQUIDATE (compile error
  otherwise); `min_gross_pct`: while the owner's gross notional / equity is below it the
  rule is "not applicable" (value None = not breached, machines clear through the normal
  hold path; a normal observation, not a quality freeze; the what-if uses the after-order
  gross); `confirm.states: 0` = time-only confirmation (`confirm.ms` must be > 0).
- State machine (`state_machine.py`): OK → PENDING → ACTIVE (distinct LIVE states of held
  instruments, or `confirm.ms`), hysteresis clear line on the safe side + `hold_ms`,
  cooldown **for ALERT levels only** (bypassed when a more severe level of the same
  rule/scope is breached; RESTRICT / CIRCUIT_BREAK / LIQUIDATE re-activate on every
  re-breach: new episode, one ALERT_RAISED), latch, `risk.reset` → REARMED with rearm line
  = min(threshold, value − extra) direction-aware, re-breach → PENDING → ACTIVE with a new
  episode, quality gating (no transition and no timer while quality ≠ OK; a running hold
  timer is reset), day roll per `reset_on_day_roll`. Alert key
  `{td}|{rule}|{level}|{scope}|{episode}`; exactly one ALERT_RAISED per episode.
- Actions: ALERT → WARNING; RESTRICT → RESTRICTED + restrictions (never latched);
  CIRCUIT_BREAK → HALTED **already in PENDING** (`halted_latched=false`), cancels of all
  resting open orders (incl. ROUTING / UNKNOWN) at the moment the account enters HALTED,
  latched when confirmed, revoked (cancelled orders stay cancelled) when the value returns
  inside the threshold first; LIQUIDATE → plan (losers first, `step`, SHFE CY then CT,
  `对手价±N跳`) awaiting confirmation. SYSTEM:* restrictions and firm restrictions →
  RESTRICTED; kill switch → RESTRICTED (cause `KILL_SWITCH`). `cause_alert_key` of a state
  change = the most severe effective level (CIRCUIT_BREAK > RESTRICT > ALERT >
  LIQUIDATE, then level index; null while that level is a PENDING circuit break).
- Pre-trade (`pretrade.py`): the 9 steps of 02 §11.2 in order with the close exemptions;
  closes skip the fat finger while the instrument's quality is not OK (the limit band
  still applies); the what-if runs on the last known marks whatever their quality and
  uses the scalar `WhatIf` (exactly the numbers of `AccountValuation.whatif`, which is
  the fallback for metrics without a scalar formula).

### Orders and cancels (02 §8.3)

- **Halt invariant**: an OPEN order that becomes working again while the account is
  HALTED (revert of a refused cancel, order rebuilt from the broker, resolve / RouteAck
  finding it working, restore) is cancelled; the first re-cancel after a refusal is
  immediate, repeated refusals back off (5 s, 10 s, … 60 s). Kill switch and BLOCK_OPEN
  restrictions reject new opens only; they never cancel resting orders.
- **CANCEL_PENDING TTL**: 5 s after a send, doubling per send up to 60 s, the order is
  checked with `route.resolve`: `CANCELED` → CANCELED (FILLED if fully filled);
  tombstone / `EXPIRED` → CANCELED; `REJECTED` → BROKER_REJECTED; `FILLED` → wait for the
  trades; still working → `route.cancel` again with a **new** command id; after 5 sends a
  non-blocking ORDER_STATE_CONFLICT break (`reason = CANCEL_STUCK`, HIGH event when an
  open order of a HALTED account) — once per order, retries continue. A resolve without
  answer is re-sent after 30 s. Every entry into CANCEL_PENDING (also a cancel reported by
  the broker) is one cancel of the day for the compliance counters; re-sends are not.
- A refused cancel is reported by trade-gateway as the order's last broker state with a
  `broker_code` (`BROKER_ORDER_FINISHED` / `BROKER_ORDER_NOT_FOUND`, `broker_seq` 0): a
  terminal state is applied, a live state reverts the order (SUBMITTED / RECEIVED →
  ROUTED, else ACCEPTED / PARTIALLY_FILLED). A live state without code is a report racing
  the cancel and is ignored.
- The order's own `frozen_margin` / `frozen_fee` follow the remaining quantity (no
  compounding over partial fills); the ledger's frozen amounts come from the registered
  resting orders.
- Orders of a closed day (GFD / FAK / FOK) are CANCELED with reason `SESSION_CLOSE` at
  ROLL / PROVISIONAL (reservations and frozen amounts released) and confirmed with
  `route.resolve`; a broker that still reports one working opens a non-blocking
  ORDER_STATE_CONFLICT break (`SESSION_CLOSE_STILL_WORKING`). A late fill is still applied
  by the usual rules.

## 8. Recovery (01 §6.5, 03 §4.3 重启恢复用到的表)

```python
core = EngineCore(...)                       # lifecycle LOADING: every order REJECT_SYSTEM_NOT_READY, no rows
core.restore(tables, lifecycle=Lifecycle.LOADING, intraday_peaks=peaks, dayrolls=outbox_dayrolls)
# (fresh database instead: execute(core.load_sod(sod)) - the lifecycle stays LOADING, nothing is evaluated)
execute(core.post_restore())                 # see below
# td.query.trades catch-up → apply_exec(...) each; ctl.snapshot md → apply_mark / apply_md_status
execute(core.clear_system_restriction(None, "FILL_GAP"))    # once the fact catch-up is complete
execute(core.set_lifecycle(Lifecycle.WARMUP))   # SYSTEM:STARTING; evaluation starts here
execute(core.request_position_recon())       # warm-up recon; when fine and marks fresh:
execute(core.evaluate_all()); execute(core.set_lifecycle(Lifecycle.RUNNING))
```

`restore(tables)` needs (rows as persisted; JSON columns may be text or decoded):

- `sod_accounts` / `sod_lots` / `sod_prices` of the latest trading day `td`;
- `eod_states` (recent rows, e.g. the last 30: trading mode, the provisional chain,
  catch-up contiguity, replies of re-pushed rollovers);
- `orders` of `td` plus every non-terminal order (with the `cancel_*` columns);
- `order_events` of the orders of `td` (compliance counters: `orders_today` = orders of
  `td` with a non-empty `request_hash`; cancels = events entering CANCEL_PENDING, i.e.
  `to_status = CANCEL_PENDING AND from_status <> CANCEL_PENDING`);
- `fills` (by `fill_seq`) and `cash_movements` of `td` — and, when `td` runs on a
  provisional SOD (`sod_version = -1`), also those of the day it was built from
  (`eod_states.next_trading_day = td`), which back `eod.freeze` of that day; cash of later
  days (attributed to the next day while frozen);
- `rule_states`, `alerts`, `risk_events` of `td` (a latched `rule_states` row whose rule is
  no longer configured is kept, rebuilt from its `alerts` row);
- `account_states`, `firm_controls` (all, by `control_id`), `liquidation_plans` (of `td`
  and pending ones), `recon_breaks` (of `td` and every OPEN one), `processed_commands`;
- `metric_samples` of `td` (max `perf_equity` of rows with `quality = OK` restores the
  intraday peak, RK-09) or `intraday_peaks`;
- optional `dayrolls`: the `sys.dayroll` facts of the outbox (exact duplicate replies;
  otherwise rebuilt from `eod_states`).

`post_restore()` expires orders of closed days (SESSION_CLOSE + `route.resolve`), turns
ROUTING into UNKNOWN, cancels every working OPEN order of a HALTED account (halt guard),
sends `route.resolve` for every UNKNOWN and CANCEL_PENDING order (a pending cancel still
working is then re-sent at once: the crash may have happened before it went out), adds
`SYSTEM:STARTING` in WARMUP and recomputes the account states (no duplicate alert, no
spurious state change). `restore` without SOD rows raises: request `settle.sod_snapshot`
(no version) first, or wait for the seed `eod.rollover`.

## 9. EOD (01 §6.6, 02 §8.4)

- `eod.freeze{T,v}` → `EOD_FROZEN` (all orders `REJECT_EOD_IN_PROGRESS`), digests via
  `compute_account_digest` over applied fills + cash digests; `eod_states` FROZEN.
- Cash attribution: OPEN → T; frozen and not rolled → next trading day (kept aside and
  applied after the roll); rolled / PROVISIONAL → the new current day.
- `eod.rollover{X→Y,V}` → `Request(clearing-worker, settle.sod_snapshot{Y, owned accounts,
  V})`, then:
  - `Y` later than the current day (and `X` the current day) → **ROLL**: the SOD is
    validated first (every owned account present, ledgers buildable; otherwise
    `PRECONDITION_FAILED` and nothing changes), orders of the closed day expire, state
    machines reset (latches per `reset_on_day_roll`, episodes 0), active alerts → ROLLED,
    pending plans → EXPIRED (event), SOD replaced, pending next-day cash and buffered
    next-day trades applied, `td.query.trades{Y, account}` per account (fills of the new
    day that were only buffered in memory; results are applied with trade-key dedupe),
    SOD lots compared with the engine's end-of-day lots (`SOD_VS_EOD` WARN break),
    `sys.dayroll`;
  - `Y` = the current day → **REBASE**: SOD replaced, today's fills and cash re-applied in
    order, rule state and episodes kept; from a provisional SOD also back to OPEN, the
    provisional lots (the engine's own EOD) compared with the official ones;
  - `Y` older than the current day → **CATCH_UP** (a shard that went PROVISIONAL over
    missed EODs): the official SOD of `Y` replaces its `sod_*` rows, `eod_states[X]`
    becomes ROLLED, the current state is **not** changed; reply `DayRoll(mode=CATCH_UP,
    from_trading_day=X, to_trading_day=Y, sod_version=V)`. A newer version of `Y`
    supersedes (another CATCH_UP); an older one is refused;
  - contiguity: `X` must be `last_official_day()` (the day of the newest official SOD in
    the chain to the current day) or an already settled `X → Y` (re-settlement), else
    `PRECONDITION_FAILED` ("settle {day} first");
  - the same `(Y, V)` again → the first `DayRoll` with `duplicate=true`.
- `freeze_digest_source(X)`: `"MEMORY"` for the current day and, while PROVISIONAL, the
  day the provisional SOD was built from (digests only, mode unchanged); `"STORE"` for
  any older day of this shard (it has an `eod_states` row: settled already → re-settlement,
  or unsettled → missed EODs): **the shell answers `eod.freeze{X}` digest-only from risk.db
  with `futrisk.core.eod.digest_only_ack(trading_day=X, shard, account_ids=core.owned,
  trades=[trade_from_fill_row(r) for fills of X by fill_seq], cash=[cash_from_row(r) for
  cash_movements of X], frozen_ns=eod_states.frozen_ns or now)`** (`open_orders = 0`,
  nothing is frozen); None (a later day, or a day the shard never ran) →
  `PRECONDITION_FAILED`. The core itself replies `NOT_FOUND` for STORE days.
- PROVISIONAL: when the market time reaches the first session (incl. auction) of the next
  trading day and the shard still runs on T, `on_timer` self-freezes T and requests
  `md.settle_inputs{T}`; with the result (or its failure) it builds the provisional SOD
  (VWAP rounded by the exchange rule > the day's last mark > previous settlement; today
  lots become yesterday lots; pre_balance = own EOD equity; computed by `settle_day`),
  rolls the risk state as for ROLL and sets `SYSTEM:EOD_NOT_SETTLED` (opens blocked,
  closes allowed). `eod.force_unfreeze` lifts the restriction; the official
  `eod.rollover` REBASEs (after CATCH_UPs of older missed days).

## 10. Firm layer (01 §5.3.1, 02 §7.6)

Every shard: `FirmState` keeps the MANUAL and RULE kill-switch flags separately
(effective = OR) and the firm restrictions (`firm`, `firm/prod:*`, `firm/sector:*`) that
apply to every account. Coordinator (`coordinator=True`): own snapshots are ingested when
published, other shards' via `ingest_snapshot`; every second `on_timer` builds the firm
valuation, publishes `risk.firm`, evaluates firm-scope rules (metrics `firm.*`,
`product.*` / `sector.*` on firm exposures, same state machine) and, when the RULE layer
changes, emits it on `risk.control.firm` and applies it locally.

`risk.control.firm` semantics (every consumer: risk-engine shards, trade-gateway,
api-gateway):

- **RULE origin: one fact, `FirmControl(action=RULE_STATE, origin=RULE, active=<rule kill
  switch on>, restrictions=<the complete set of firm rule restrictions in force>,
  cause_alert_key=<the firm circuit-break alert while active>)`.** A consumer **replaces**
  its whole RULE layer with it (an empty list lifts every rule restriction; `active=false`
  switches the rule kill switch off). The newest fact alone therefore restores the RULE
  layer — required by the `ctl.snapshot` fallback, which keeps only the newest message of
  the topic.
- **MANUAL origin** (api-gateway fan-out, not on this topic): `KILL_SWITCH_ON` /
  `KILL_SWITCH_OFF`, `restrictions=[]`; it only sets the MANUAL flag. Neither origin can
  change the other's flag; effective = manual OR rule.
- Legacy actions are still applied: RULE `KILL_SWITCH_ON/OFF` (rule flag only) and
  `SET_RESTRICTIONS` (restrictions only).
- `KILL_SWITCH_ON/OFF` risk events are emitted only when the origin's flag actually
  changes; every applied control is recorded in `firm_controls` and refreshes the account
  states.

- Freshness is judged per shard: a shard is fresh while its newest snapshot or heartbeat
  (`note_shard_alive`) is younger than 5 s (every shard re-publishes each account at
  least once per second). Only accounts with exposure can downgrade the firm quality;
  expected shards (`accounts.shards()`) that never reported are listed STALE in
  `FirmSnapshot.shards`.
- The firm intraday peak starts at the firm's SOD performance equity and follows the
  firm's own performance equity on trusted marks; the firm HWM is the running maximum of
  that peak across days, **in memory only** (after a restart it restarts from the current
  day).
- Latched firm-level machines have no reset command (`risk.reset` is per account): they
  are released by the day roll only.

## 11. Backtest usage (01 §8.16)

```python
core = EngineCore(..., clock=SimClock(t0), ids=SeqIdGen("bt"), auto_provisional=False)
# rules.system: snapshot_hz=0, metric_sample_s=0, recon_interval_s=0 -> next_timer_ns() = rule / order timers only
core.load_sod(sod); core.set_lifecycle(Lifecycle.RUNNING)
for day in days:
    for event in day:                   # ticks and strategy orders in time order
        clock.set(event.ts)
        core.apply_mark(tick) / core.handle(order, ctx) / core.apply_exec(fill) ...
        core.evaluate_dirty(); core.on_timer()
    core.freeze(day.td)                 # optional digests
    sod = settle_day(...).next_sod
    core.roll_day(sod)                  # before the clock reaches the next trading day
```

Effects can be executed by an in-process bus or simply collected; routing results are fed
back with `on_route_ack` / `apply_exec`.

## 12. Deviations and limits

- A re-breach from REARMED goes through PENDING (same confirmation as the first episode)
  instead of jumping to ACTIVE, so one bad tick cannot re-latch the account; revoking it
  returns to REARMED.
- The PENDING circuit break has no alert row; its `AccountStateChanged` has
  `cause_alert_key=null` until the level is confirmed.
- A manual halt is stored as the latched machine `manual_halt/halt/acct:{A}` (rule_states
  + alerts), so it survives restarts with the same machinery as rule latches.
- `RETRO_BREACH` shadow evaluation (SHOULD) and automatic liquidation (TD-08) are not
  implemented; `risk.query.metrics` and history queries are for the shell.
- A fill that cannot be booked (close larger than the book, or a trade of an already
  settled day) is not applied; a blocking POSITION_MISMATCH recon break (one per
  `trade_key`) blocks opens and the EOD digest check surfaces the difference.
- Next-day fills received before the roll are buffered in memory; the ROLL / PROVISIONAL
  asks `td.query.trades` for the new day, so a crash in between loses nothing.
- An INTERNAL fill of an unknown order (power loss before the intent was committed) is
  applied at once; the order record is rebuilt from `td.query.orders` (02 §8.3, audited)
  and the `fills` row keeps `client_order_id` null (no FK to a missing order).
- The order rebuild audit reuses the storage-only AuditAction `RECON_RESOLVE` (the
  `audit_events.action` CHECK list has no dedicated value); `after.kind = ORDER_REBUILD`.
- `Position` (core/lots.py) keeps running aggregates (quantity, today quantity, Σ open and
  basis notional, exact Decimals) so pre-trade and fills are O(1) in the number of lots
  (300 lots: `closable()` 91 → 2.4 µs, an opening fill 1.27 ms → 0.11 ms).
- Performance (dev laptop, Python 3.12, 11 default rules, 5 accounts / 13 instruments /
  20 resting orders, heavily loaded machine; A/B against the same code with the phase-2
  optimisations switched off, medians of interleaved rounds): pre-trade open check p50
  89 → 58 µs, `order.submit` 295 → 254 µs, tick of a 4-holder instrument + evaluation
  848 → 680 µs, `evaluate(ACC2)` 269 → 223 µs. Absolute numbers vary ±50 % with the
  machine load.
