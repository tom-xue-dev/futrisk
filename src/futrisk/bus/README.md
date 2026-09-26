# futrisk bus, db and runtime — how a service uses them

## Changes (2026-09-25, api-gateway / supervisor review follow-ups)

1. **Publisher restarts are no longer detected by comparing UUIDv7 ids.** Epochs and stream ids sort by the
   wall clock at creation, and Windows time sync can step it back: a restarted publisher (or recreated
   database) could sort *lower* and was then ignored forever. Now the publisher's heartbeat is the
   authority (`futrisk.bus.incarnation`):
   * `subscribe(..., snapshot=True)` watches the publisher's heartbeat by itself. After a restart the new
     process' LV messages are applied once its first heartbeat has arrived (≤ one heartbeat interval), and
     the Clone re-snapshots at that moment; messages of any other epoch are dropped.
   * FACT: a stream announced by the heartbeat runs the stream-change handling whatever its order
     (`on_stream_change`, or replay from seq 1), for every key of that publisher — keys whose topic has no
     row in the new stream yet get `next_seq=1`. Facts of a stream not (yet) announced are dropped and
     recovered by that replay / reconciliation.
   * A heartbeat of the previous process still queued on the socket is ignored (two consecutive ones switch
     back, which heals a wrong switch).
   * Before the first heartbeat — and for consumers that never see heartbeats — arrival order decides: an
     epoch/stream never seen replaces the current one, a replaced one is stale.
   * Using `LvTracker` / `CloneSync` / `FactTracker` directly: feed `sys.heartbeat.{publisher}` to
     `on_heartbeat(hb)`. Tests: `futrisk.runtime.testing.ids_clock_stepped_back(seconds)`.
   * **Service and test authors — the rule to remember:** *a publisher's stream (and epoch) is whatever its
     heartbeat announces.* A fake publisher in a test must announce a restart or a stream change through its
     heartbeat (`Heartbeat.epoch` / `Heartbeat.stream` / `fact_heads`, as `rt.build_heartbeat()` does). Once
     the subscriber has seen one heartbeat of that publisher, a stream or epoch change faked by envelopes
     alone is dropped as stale — send the heartbeat with the new stream first. (Only before any heartbeat,
     or for consumers without heartbeats, does an unseen stream in an envelope switch by itself.)
2. **`request_hash` ignores `sent_ns`/`requested_ns`/`ts_ns`/`received_ns` at the top level only.** Nested
   data keeps its times: the same Idempotency-Key / `command_id` with trades that differ only in `ts_ns` or
   `received_ns` is now a `CONFLICT` (was: replayed). A retry that only refreshes the request's own
   `sent_ns`/`requested_ns` is still a duplicate.
3. **`ctl.ping` answers `Pong.home_id` and `Pong.pid`**, and heartbeats carry `detail["home_id"]`
   (`rt.home_id`: `HomeLayout.home_id()`; `None`/absent for an injected config without `home=`).
4. `RpcServer.close()` / `AsyncRpcServer.close()` flush queued replies (e.g. the `ctl.shutdown` answer) for
   up to `REPLY_LINGER_MS = 200` before the socket closes.
5. **Hot-path persistence is fast by itself.** `Outbox.append` / `append_msg` and `FactCursors.upsert` run
   prepared sqlite3 statements on the job's connection (same rows, same `outbox_topics` sequence allocation
   in the same transaction, same after-commit hand-off): ~30 µs and ~9 µs instead of ~630 µs and ~260 µs.
   For your own hot statements use the public `futrisk.db.PreparedSql` (`INSERT_X = PreparedSql("INSERT ...
   VALUES (?, ?)")`, then `INSERT_X(conn, params)` inside a writer job; sqlite3 errors surface as the usual
   `sqlalchemy.exc` classes) or `driver_connection(conn)`. Do not use the outbox's private `_TX_ROWS` /
   `_dispatch`. `db.write` wakes the caller with a raw lock and group commits use raw savepoints:
   `db.write(append + upsert)` p50 ~1.28 ms → ~0.25 ms (NORMAL).
6. **WAL checkpoints run in the background** (new `Database(checkpoint="background")` default): the writer
   never checkpoints inside a commit (SQLite's auto-checkpoint cost 3-10 ms stalls every few hundred
   commits); a separate connection runs `wal_checkpoint(PASSIVE)` every 50 ms and a short `RESTART` only
   when the WAL passes `wal_limit_mb=128` under a continuous load. Same durability. `checkpoint="auto"`
   restores SQLite's behaviour. `outbox.submit_mark_published` calls are coalesced into one queued job.
7. **Loop stalls no longer mark live peers DOWN.** Sync runtime: before a peer is declared DOWN,
   `run_timers` drains the system socket (`pump_system`: heartbeats that arrived during a long step count);
   after a stall longer than the peer timeout, no peer goes DOWN for one more heartbeat interval. Asyncio
   runtime: after the event loop was blocked for more than a heartbeat interval, the same grace. A peer
   that really stopped is still reported DOWN (timeout + at most one interval). `rt.loop_stalls` counts
   stalls; each one logs `loop.stall`. No action — but a slow step still delays everything else: keep
   long work in `rt.offload(...)`.
8. **Parent watchdog survives wall-clock steps** (`runtime/parent.py`, used by both runtimes and
   md-gateway's `main`): the parent's `psutil.Process` is captured once and polled with `is_running()`
   (psutil's identity check uses ticks since boot on Linux, so a stepped clock in Docker is no longer a dead
   parent; a recycled pid is still not mistaken for the parent). No action.
9. **GC policy in both runtimes** (`runtime/gcpolicy.py`, 01 §11 RK-01): `gc.set_threshold(10000, 20, 100)` at
   start, and `gc.collect()` + `gc.freeze()` once per process right before the first `set_state("RUNNING")`
   heartbeat, so the start-up objects (config, the restored book, caches) are never traversed again. In the
   benches the risk-engine went from ~100-230 young, 10-21 middle and 1-2 full collections (34-110 ms each) per
   30 s window to ~0-7 young ones and no full collection. Pause
   statistics are in every heartbeat: `detail["gc"] = {collections, last_pause_ms, max_pause_ms, total_pause_ms,
   frozen, threshold}`. Configure with `services.yaml` `bus.gc: {enabled, threshold, freeze_on_running}`;
   `FUTRISK_GC=off | nofreeze | t0,t1,t2[,nofreeze]` overrides it for measurements. **Services: report RUNNING
   only once the start-up state is loaded** (what is allocated after that is not frozen).
10. **Bus hot paths**: PUB messages go out as two frames through pyzmq's Cython `send` with plain-int flags
    (`futrisk.bus.publisher.send_pub`; `send_multipart` cost twice as much), topic bytes are cached, LV cache
    entries are cheaper; the sync subscriber receives with `recv_frames` (Cython `recv`/`get`) and the topic
    filter caches its decision per topic. `bench bus --quick`, 4 interleaved rounds on the loaded machine:
    publisher send rate 84k → 180k msg/s, subscriber drain 140k → 226k msg/s, delivered 79k → 135k msg/s
    (target ≥ 100k). No action.

## Changes (2026-09-25, code review) — what service authors must adjust

All existing signatures still work; new behaviour is the safe default.

1. **FACT callbacks and DB jobs must be synchronous.** `apply`, `on_expired`, `on_stream_change`,
   `on_repair_*`, `db.submit/write/run_write` jobs, `rt.submit_write` jobs and `snapshot=True` handlers raise
   `TypeError` for `async def` (or for results that are awaitables). In asyncio services write
   `def apply(env, cur): rt.submit_write(db, tx)`.
2. **`rt.submit_write` failures poison the chain** (asyncio runtime). After one ordered write fails, every
   later `submit_write` raises `WriteChainBroken` without writing, the service stops with exit code 1 and its
   databases close with `clean_shutdown=0`, so a failed FACT is re-applied after the restart instead of being
   skipped. Do not catch `WriteChainBroken`. The sync runtime closes unclean after `rt.fatal(exc, where)`;
   `rt.run()` and `with rt:` call it for you — if you write your own loop, call `rt.fatal(...)` in its
   `except` before `rt.stop()`.
3. **Stream change without `on_stream_change` now replays the new stream from seq 1** (was: continue at the
   announced position, silently skipping the rest). Business keys must make re-application harmless. Pass
   `on_stream_change(key, old, new, next_seq, done)` to reconcile by domain query and continue at `next_seq`.
   Messages/heartbeats of a replaced stream are dropped as stale (decided by the heartbeat since the
   follow-up above, not by UUIDv7 order).
4. **Tail gaps are repaired when still present at the next heartbeat** (heartbeats can overtake facts on the
   other socket); a backlog is still fetched at once when the first live message shows the gap. The sync
   runtime applies already-received facts before it evaluates a heartbeat. No action (catch-up of an idle
   publisher's backlog after a restart takes up to one heartbeat interval).
5. **`NOT_FOUND` expires only the purged part:** `on_expired(key, lo, hi, done)` may get a narrower range and
   the rest is replayed. `ctl.replay` NOT_FOUND replies carry `ReplayBatch.next_seq` = first seq still stored.
6. **`await rt.wait_clock_ready()` returns `False` when a stop is requested** (was: blocked forever). Check it:
   `if not await rt.wait_clock_ready(): return await rt.run_until_stopped()`.
7. **New `parent_pid=` option** (both runtimes): pass the `--parent-pid` you receive from `futrisk svc`; the
   service stops itself when that process is gone (polled every 2 s).
8. **New `rt.idempotent(db, command, requester, command_id, payload, handler, trading_day=...)`** replaces the
   manual `check` → work → `record` pattern, which raced with retries on the concurrent asyncio server.
9. **`ctl.snapshot` returns FACT topics right after a restart** (seeded from the outbox), and LV consumers order
   FACT envelopes by `(stream, seq)` instead of epoch. No action.
10. **Sync signal handlers only record the signal**; `rt.running` turns False at once and the loop performs
    `request_stop` on its next pass (fixes a deadlock). No action unless you install your own handlers.
11. **Outbox retention uses the wall clock:** `published_ns` is wall-clock, `created_ns`/envelope `ts_ns` market
    time. Purge with `outbox.purge_older_than(conn, days=7)` (not with a market-time cutoff).
12. **Migrations:** `Migration.create_all` now checksums a stable schema fingerprint (older DDL checksums are
    accepted and upgraded automatically). Before a release, freeze v1 with `freeze_schema(...)` (see below).
13. `request_hash` hashes `bytes` fields as hex for Structs *and* wire bytes (only payloads with bytes fields
    change hash; `processed_commands` keeps 2 days). Time fields: top level only, see the follow-up above.
14. `UNAVAILABLE` for a peer that is down takes ~1.1 s with the default 3 retries; pass `retries=0` where you
    need a fast fail (e.g. `route.order` near its `expire_ns`, health probes).
15. **Fixed: concurrent `await rt.request(...)` to a peer that is down could hang forever** (asyncio client) and
    deliver the stale requests once the peer came up. Now every such request fails with `UNAVAILABLE` after at
    most the 0.3 s connect grace. No action.

---

Specs: 02 §4 (bus protocol), §8.1–§8.2 (loop, FACT repair), §8.5–§8.6 (2PC, start-up), 03 §4.0/§7/§8/§9.
Every service is built on one of two runtimes:

| service | runtime | why |
|---|---|---|
| risk-engine-k, trade-gateway | `SyncServiceRuntime` | single business thread driving a `zmq.Poller` (02 §8.1) |
| md-gateway, clearing-worker, api-gateway, supervisor | `AsyncServiceRuntime` | asyncio; run with `run_async()` (selector loop, required by `zmq.asyncio` on Windows) |

A runtime gives you: config (`active.json` or injected), logging, `epoch`, PUB + ROUTER bound from `services.yaml`,
lazy DEALER clients (identity = own instance), `sys.clock` following, heartbeats (1 Hz, `loop_lag_ms`, `stream`,
`fact_heads`), peer liveness, the standard `ctl.*` commands (ping, status, snapshot, replay, shutdown, config 2PC),
graceful shutdown, a parent-process watchdog and databases whose outbox is wired to the publisher.

## Pieces

| module | what |
|---|---|
| `futrisk.db` | `Database` (WAL, one writer thread: `write`/`submit`/`run_write`, `critical=True` → `synchronous=FULL`, `query_only` reads, background checkpoints), `PreparedSql` (hot statements), `add_common_tables`, `Migration` / `freeze_schema`, `Outbox`, `ProcessedCommands` + `request_hash`, `FactCursors`, `futrisk.db.types` column helpers |
| `bus.publisher` | `PublisherThread` (sync) / `AsyncPublisher`: LV seq per topic per epoch, FACT rows with persisted seq + stream, LV cache for `ctl.snapshot` |
| `bus.subscriber` | SUB with exact-topic filter (`td.exec.ACC1` ≠ `td.exec.ACC10`), frame/proto checks |
| `bus.fact` | `FactTracker`: 02 §8.2 (order, dedupe, replay, NOT_FOUND fallback, tail gaps, start policies, stream changes) |
| `bus.lv` | `LvTracker`, `CloneSync` (late joiner: buffer → snapshot → apply `seq >= expected`; re-snapshot on publisher restart) |
| `bus.incarnation` | `Incarnations`: which publisher epoch / stream is current (heartbeat authority, never UUIDv7 order) |
| `bus.rpc_server` / `rpc_client` | ROUTER servers (whitelist, BUSY, deferred replies) and DEALER clients (async / blocking / loop) with the 02 §4.7 retry policy |
| `bus.inproc` | `InprocBus`: deterministic in-process bus for backtests and unit tests |

## Sync poller service (risk-engine, trade-gateway)

```python
from futrisk.bus import DEFERRED
from futrisk.db import FactCursors
from futrisk.messages import codes, topics
from futrisk.messages.envelope import ok_reply, err_reply
from futrisk.runtime import SyncServiceRuntime

rt = SyncServiceRuntime("risk-engine-1", parent_pid=args.parent_pid)  # tests: config=..., home=tmp, market_clock=MarketClock(), setup_logs=False
rt.start()                                          # call on the business thread (sockets live there)
db = rt.open_database("risk-1", METADATA, synchronous="NORMAL")   # migrations, db_meta, outbox -> publisher
cursors = FactCursors(db)

def on_order(cmd, req, ctx):                        # handlers run on the business thread
    effects = core.handle(req)
    db.write(lambda c: persist(c, effects))         # business rows + rt.append_fact(c, topic, body) in ONE tx
    rt.request("trade-gateway", "route.order", route, on_reply=lambda r: on_route(r, ctx),
               deadline_ns=expire_mono_ns)          # non-blocking, retried until the deadline
    return DEFERRED                                 # answer later: rt.reply(ctx, ok_reply(ack))

def on_reset(cmd, req, ctx):                        # state-changing command, exactly once per command_id
    return rt.idempotent(db, cmd, ctx.requester, req.command_id, req,
                         lambda conn: do_reset(conn, req), trading_day=td)

rt.serve({"order.submit": on_order, "risk.reset": on_reset, "eod.rollover": on_rollover})

def apply_exec(env, cur):                           # FACT: effects + cursor in the same transaction (sync!)
    db.write(lambda c: (apply_to_book(c, env), cur.save(c, cursors, updated_ns=rt.market_clock.now_ns())))

md = rt.subscribe("md-gateway", ["md.tick.", "md.status"])          # LV, raw: md.drain()
rt.subscribe_fact("md-gateway", ["md.event"], policy="FROM_NOW", apply=apply_md_event, cursors=cursors.load_all())
rt.subscribe_fact("trade-gateway", [topics.td_exec(a) for a in my_accounts], policy="FROM_BEGINNING",
                  apply=apply_exec, cursors=cursors.load_all(),
                  on_expired=lambda key, lo, hi, done: fill_by_td_query(key, lo, hi, done),
                  on_stream_change=lambda key, old, new, nxt, done: resync_by_td_query(key, done),
                  on_repair_failed=lambda key, lo, hi, r: block_open(key, "FILL_GAP"),
                  on_repair_ok=lambda key: unblock(key, "FILL_GAP"))
rt.watch_peers(["md-gateway", "trade-gateway"], on_change=on_peer)   # DOWN after 3 missed heartbeats
if not rt.wait_clock_ready(timeout_s=30):           # sys.clock snapshot from the supervisor
    ...                                             # timed out or stopping: retry / exit

try:
    while rt.running:                               # 02 §8.1
        rt.poll(rt.next_timer_ms())
        rt.pump_system()                            # sys.clock + heartbeats (liveness, FACT heads)
        for env in md.drain(): slots[env.topic] = env   # LV: keep the latest per instrument
        rt.pump_fact()                              # md.event / td.exec in seq order, repairs issued asynchronously
        for c in rt.pump_replies(): on_completion(c)    # callback-less replies (c.context is yours)
        rt.pump_requests(limit=256)                 # commands; ctl.* handled by the runtime
        marks_and_evaluate()
        for c in rt.run_timers(): on_completion(c)  # rpc timeouts/retries, repair backoff, 2PC, clock, offloads
        rt.loop_done()                              # loop_lag_ms
except BaseException as exc:
    rt.fatal(exc, "business loop")                  # exit code 1, databases close with clean_shutdown=0
    raise
finally:
    rt.stop()                                       # STOPPING -> drain DB/publisher -> close DBs
```

Rules: nothing blocks the loop except `db.write` (commit confirmation). Slow work goes to
`rt.offload(fn, on_done)` (`on_done` runs back on the business thread; completion wakes the poller at once).
`rt.publish(topic, body)` is for LV; FACTs are only ever written through `rt.append_fact(conn, ...)` inside a DB
transaction and are published after the commit, in commit order. `rt.run(step)` is a ready-made loop for
simple services (it calls `rt.fatal` on exceptions).

## Asyncio service (md-gateway, clearing-worker, api-gateway, supervisor)

```python
from futrisk.runtime import AsyncServiceRuntime, run_async

async def main() -> int:
    async with AsyncServiceRuntime("clearing-worker", parent_pid=args.parent_pid) as rt:  # start() ... stop()
        rt.install_signal_handlers()                          # Ctrl-C / CTRL_BREAK -> graceful stop
        db = await rt.open_database("clearing", METADATA, synchronous="FULL")
        cursors = FactCursors(db)

        async def settle_run(cmd, req, ctx):                  # async or sync handlers
            # check + work + record in ONE writer job; concurrent duplicates wait for the first
            return await rt.idempotent(db, cmd, ctx.requester, req.command_id, req,
                                       lambda conn: start_saga(conn, req), trading_day=req.trading_day)

        rt.serve({"settle.run": settle_run})

        def apply_trade(env, cur):                            # sync; ordered non-blocking write
            rt.submit_write(db, lambda c: (copy_trade(c, env), cur.save(c, cursors, updated_ns=...)))

        rt.subscribe_fact("trade-gateway", ["td.exec."], policy="FROM_BEGINNING", apply=apply_trade,
                          cursors=cursors.load_all())
        rt.subscribe("risk-engine-0", ["risk.snapshot."], on_snapshot, snapshot=True)   # read model (Clone)
        if not await rt.wait_clock_ready():                   # False: a stop was requested meanwhile
            return await rt.run_until_stopped()
        reply = await rt.request("risk-engine-0", "eod.freeze", req)                    # 30 s, retried
        rt.set_state("RUNNING")
        return await rt.run_until_stopped()                   # ctl.shutdown / signal / parent gone / request_stop

raise SystemExit(run_async(main()))
```

Never block the loop: use `await db.run_write(...)` / `await db.run_read(...)`, executors or process pools.
Handlers of `subscribe(..., snapshot=True)` must be synchronous; plain subscription handlers may be `async`
(awaited in order). Errors in subscription handlers / FACT `apply` / `submit_write` are fatal (exit code 1,
databases closed with `clean_shutdown=0`, the supervisor restarts the service). To read your own FACT copies
after `submit_write`, await `db.run_write(lambda c: None)` first (writer barrier).

## Contracts worth knowing

* **Outbox**: `append` allocates `topic_seq` from `outbox_topics` in the caller's transaction; nothing is
  published for rolled-back jobs. On start `open_database` republishes rows left unpublished by a crash and
  seeds the `ctl.snapshot` cache with the newest row of every FACT topic. `ctl.replay` serves the outbox and
  answers `NOT_FOUND` (with `ReplayBatch.next_seq` = first seq still stored) once purged. Retention:
  `outbox.purge_older_than(conn, days=7)` from a writer job after `sys.eod` DONE (wall clock, 03 §8).
* **FactTracker** (`rt.fact_tracker`): `apply(env, cursor)` (sync) = persist effects + `cursor.save(...)` in
  one transaction; it may raise (cursor not advanced). `on_expired`/`on_stream_change` get a `done` callback;
  the key is held (buffered) until you call it. First subscription buffers until the publisher's heartbeat.
  Without `on_stream_change` a new publisher stream is replayed from seq 1. The stream announced by the
  publisher's heartbeat is the current one (any UUIDv7 order); facts of other streams are dropped.
* **Snapshot subscriptions** (`CloneSync`): the publisher's heartbeat says which epoch is current; a new one
  re-snapshots. LV messages of any other epoch are dropped (the new process' first messages are recovered by
  that re-snapshot).
* **Commands**: payload type is checked when registered with `add_handler(cmd, fn, payload_type=T)`.
  Unknown command → `BAD_REQUEST`; whitelist (02 §4.11, `bus/whitelist.py`) → `FORBIDDEN`; backlog/in-flight
  over 1k → `BUSY` (`RISK_BUSY` for `order.*` in risk-engine). Idempotency: `rt.idempotent(...)` (check, run,
  record in one writer transaction; `CONFLICT` for the same key with other content, `duplicate=True` for
  repeats); `request_hash` ignores `sent_ns`/`requested_ns`/`ts_ns`/`received_ns` at the top level of the
  payload only (nested data, e.g. imported trades, keeps its times).
* **Requests**: per-attempt timeout from `default_timeout_ms(command)`; retries only for
  TIMEOUT/UNAVAILABLE/BUSY/RISK_BUSY (max 3, 100 ms·2ⁿ + jitter), same payload and `corr_id`. A peer that is not
  connected fails the attempt at once (ZMQ_IMMEDIATE), so `UNAVAILABLE` arrives after the retries (~1.1 s);
  `retries=0` returns it immediately. One DEALER identity per caller: several clients with the same identity
  steal each other's ROUTER connection (`ROUTER_HANDOVER`).
* **State & heartbeat**: `rt.set_state("RUNNING"|"DEGRADED")`; `rt.set_heartbeat_fields(queues=..., rates=...,
  latency_us=..., deps=..., trading_day=..., trading_mode=...)` or `rt.heartbeat_provider(fn)`.
* **Config 2PC**: `rt.add_config_validator(fn(current, new) -> errors)`, `rt.on_config_commit(fn(new, effects))`
  (effects per section: `IMMEDIATE` / `NEXT_TRADING_DAY`; stage the latter yourself). Endpoint changes are
  rejected at prepare; a yes vote is persisted and resolved with `config.decision` after 60 s or a restart.
* **Clock**: `rt.market_clock` (market time) follows `sys.clock`; the supervisor owns it and publishes it
  with `rt.publish("sys.clock", ClockState(...))`.
* **Parent watchdog**: `parent_pid=` (+ `parent_poll_s=2.0`) stops the service when its parent exits.
* **Home identity**: `ctl.ping` → `Pong{instance, state, ts_ns, home_id, pid}`; heartbeats carry
  `detail["home_id"]`. The CLI / supervisor compare it with their own `HomeLayout.home_id()` before acting
  on a process that owns a port (cross-home guard).

## Migrations: freezing v1 before a release

During development `Database(path, METADATA)` uses `Migration.create_all(1, "initial schema", METADATA)` and
tracks your Table definitions. Before a release, freeze the schema once and commit the file:

```python
from futrisk.db import freeze_schema, load_sql_migrations
freeze_schema(METADATA, "src/futrisk/services/<svc>/migrations")      # -> 0001_initial_schema.sql
db = rt.open_database("<name>", METADATA, migrations=load_sql_migrations(MIGRATIONS_DIR))
```

The frozen file carries `-- futrisk:accept-checksum` headers, so databases created by the `create_all`
migration keep opening. Later schema changes: a new `NNNN_<description>.sql` file plus the matching change to
the Table definitions. A released migration must never change (checksum mismatch → the service refuses to start).

## Tests

* `InprocBus` (+ `SimClock`) for deterministic single-process tests and backtests; it also answers
  `ctl.snapshot` / `ctl.replay` so FactTracker/CloneSync logic can be tested without sockets.
* `futrisk.runtime.testing.config_with_free_ports(cfg, heartbeat_ms=100)` + `home=tmp_path` +
  `market_clock=MarketClock()` (no supervisor needed) + `setup_logs=False` for real runtimes in tests.
* Async tests need a selector loop on Windows: add to your test package's `conftest.py`
  `def pytest_asyncio_loop_factories(config, item): return {"selector": asyncio.SelectorEventLoop}`.
* Windows: the supervisor cannot use asyncio subprocesses on a selector loop; use `subprocess.Popen` + polling.
