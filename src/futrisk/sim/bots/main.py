"""``strategy-sim``: all five strategy bots in one process (01 §3.1, §5.1, §6.2, §7.1).

``futrisk svc strategy-sim`` calls :func:`run`. The process is an external client of
the bus - no ROUTER, no heartbeats, no database (02 §3, §4.1): it subscribes
``md.tick.*`` (md-gateway), ``oms.order.{account}`` plus ``risk.state.{account}`` /
``risk.snapshot.{account}`` (owning shards) and sends ``order.*`` with the DEALER identity
``strategy-sim``.

Life cycle (:func:`run_bots`):

1. start the client and subscribe; wait until **ticks are flowing** and every owning
   **risk shard answers** ``ctl.ping`` (retry with back-off; shards still ``STARTING``
   are waited for up to ``ready_timeout_s``);
2. start-up sync per account (best effort): cancel the **own** working orders a previous
   incarnation left (``risk.query.orders``, own order-id marker, one ``order.cancel`` each -
   manual and liquidation orders are never touched), then ``risk.query.accounts`` to seed
   positions (with the snapshot time), equity, state and restrictions;
3. trade: every tick drives the account's strategy and executor; ``risk.state`` /
   ``risk.snapshot`` keep HALTED, the restrictions and the equity current (a halt is known
   without an opening attempt); a 1 s housekeeping loop logs status, resolves missed
   updates (``risk.query.orders``) and resyncs; an idle account whose positions differ
   from consecutive snapshots is re-seeded from the snapshot;
4. stop on Ctrl-C / CTRL_BREAK / SIGTERM or when the parent process is gone
   (:class:`~futrisk.runtime.parent.ParentWatch`, ~2 s): disable trading, cancel the own
   working orders one by one, close the sockets.

Aggressive/scenario mode (ACC3 builds its whole book at once for the breaker demo,
01 §6.3): ``run(aggressive=True)``, ``FUTRISK_BOTS_AGGRESSIVE=1`` or
``python -m futrisk.sim.bots --aggressive``.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import signal
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

from futrisk.client.client import FutriskClient, OrderEvent
from futrisk.client.riskview import RiskUpdate
from futrisk.common.config import ConfigError, FullConfig, ensure_home_config, load_active_config
from futrisk.common.logging import setup_logging
from futrisk.common.paths import HomeLayout, resolve_home
from futrisk.messages.md import Tick
from futrisk.runtime.loop import run_async
from futrisk.runtime.parent import ParentWatch
from futrisk.sim.bots.bot import AccountBot
from futrisk.sim.bots.config import BotsConfig, load_bots_config
from futrisk.sim.bots.executor import Action, CancelAction, Executor, SubmitAction
from futrisk.sim.bots.gates import Gates
from futrisk.sim.bots.strategies import StrategyContext, make_strategy

DEFAULT_INSTANCE = "strategy-sim"
_ALIVE = ("RUNNING", "DEGRADED")


class BotHost:
    """Runs the account bots of one process over one :class:`FutriskClient`."""

    def __init__(
        self,
        config: FullConfig,
        bots_cfg: BotsConfig,
        *,
        client: FutriskClient | None = None,
        accounts: list[str] | None = None,
    ) -> None:
        self.config = config
        self.cfg = bots_cfg
        self.client = client or FutriskClient(config, identity=bots_cfg.identity, coid_tag=bots_cfg.order_tag)
        wanted = accounts or bots_cfg.accounts_filter
        self.accounts = self._select_accounts(wanted)
        self.bots: dict[str, AccountBot] = {a: self._build_bot(a) for a in self.accounts}
        self.by_instrument: dict[str, list[AccountBot]] = {}
        for bot in self.bots.values():
            for inst in bot.instruments:
                self.by_instrument.setdefault(inst, []).append(bot)
        self._tasks: set[asyncio.Task[Any]] = set()
        self._last_status = 0.0
        self._last_resync: dict[str, float] = {}
        self.ready = False

    # ---------------------------------------------------------------- construction
    def _select_accounts(self, wanted: list[str] | None) -> list[str]:
        configured = self.config.accounts.accounts
        out: list[str] = []
        for a in sorted(configured):
            if wanted is not None and a not in wanted:
                continue
            acfg = self.cfg.accounts.get(a)
            if acfg is not None and not acfg.enabled:
                continue
            if (acfg is None or acfg.strategy is None) and not configured[a].strategy:
                logger.warning("bots.no_strategy", account_id=a)
                continue
            out.append(a)
        for a in wanted or []:
            if a not in configured:
                logger.warning("bots.unknown_account", account_id=a)
        return out

    def _build_bot(self, account_id: str) -> AccountBot:
        spec = self.config.accounts.accounts[account_id]
        acfg = self.cfg.accounts.get(account_id)
        name = (acfg.strategy if acfg is not None else None) or spec.strategy or ""
        rng = random.Random(f"{self.cfg.seed}:{account_id}")
        params = self.cfg.params_for(account_id)
        strategy = make_strategy(name, params, account_id=account_id, rng=rng)
        gates = Gates(account_id, self.cfg.gates)
        ctx = StrategyContext(
            account_id,
            self.client.contracts,
            self.client.positions,
            equity=float(spec.initial_capital),
            gates=gates,
            rng=rng,
        )
        executor = Executor(
            account_id,
            contracts=self.client.contracts,
            positions=self.client.positions,
            orders=self.client.orders,
            gates=gates,
            cfg=self.cfg.execution_for(account_id),
            new_order=self.client.new_order,
            rng=rng,
            strategy_tag=f"{name}:{account_id}",
        )
        unknown = [i for i in strategy.instruments if not self.client.contracts.has(i)]
        if unknown:
            raise ValueError(f"{account_id}: instruments without product config: {unknown}")
        foreign = sorted({self.client.contracts.get(i).product for i in strategy.instruments} - set(spec.products))
        if spec.products and foreign:
            logger.warning("bots.products_outside_account", account_id=account_id, products=foreign)
        logger.info("bots.account", account_id=account_id, strategy=name, instruments=list(strategy.instruments),
                    aggressive=self.cfg.aggressive, equity=float(spec.initial_capital))
        return AccountBot(account_id, strategy=strategy, executor=executor, ctx=ctx)

    @property
    def instruments(self) -> list[str]:
        return sorted(self.by_instrument)

    # ---------------------------------------------------------------- life cycle
    async def start(self) -> None:
        await self.client.start()
        self.client.subscribe_orders(self.accounts, self._on_order)
        self.client.subscribe_risk(self.accounts, self._on_risk)
        self.client.subscribe_ticks(self.instruments, self._on_tick)
        logger.info("bots.started", accounts=self.accounts, instruments=self.instruments,
                    shards=self.client.router.instances(self.accounts))

    async def wait_ready(self, stop: asyncio.Event, *, max_delay_s: float = 5.0, stale_s: float = 10.0) -> bool:
        """Block until ticks flow and the owning shards answer (02 §8.6: they may still be starting)."""
        delay = 0.5
        started = time.monotonic()
        last_log = -1e9
        while not stop.is_set():
            age = self.client.seconds_since_last_tick()
            ticks_ok = age is not None and age < stale_s
            pongs = await self.client.ping_shards(self.accounts)
            states = {i: (str(p.state) if p is not None else None) for i, p in pongs.items()}
            answered = all(p is not None for p in pongs.values())
            running = all(p is not None and p.state in _ALIVE for p in pongs.values())
            waited = time.monotonic() - started
            if ticks_ok and answered and (running or waited >= self.cfg.ready_timeout_s):
                logger.info("bots.ready", shards=states, ticks=self.client.ticks_received, waited_s=round(waited, 1))
                self.ready = True
                return True
            if time.monotonic() - last_log >= 10.0:
                last_log = time.monotonic()
                logger.info("bots.waiting", ticks_flowing=ticks_ok, shards=states, waited_s=round(waited, 1))
            await _sleep_or_stop(stop, delay)
            delay = min(delay * 1.5, max_delay_s)
        return False

    async def startup_sync(self) -> None:
        await asyncio.gather(*(self._sync_account(a, startup=True) for a in self.accounts))

    async def _sync_account(self, account_id: str, *, startup: bool = False) -> None:
        bot = self.bots[account_id]
        try:
            if startup and self.cfg.startup_cancel_own:
                td = bot.ctx.trading_day or self.client.positions.trading_day
                if td is None:
                    logger.warning("bots.startup_cancel_skipped", account_id=account_id, why="trading day unknown")
                else:
                    done = await self.client.cancel_own_orders(account_id, td, reason="strategy-sim start")
                    if done:
                        await asyncio.sleep(0.5)  # let the cancels settle before the position snapshot
            if startup and not self.cfg.startup_sync:
                return
            await self.client.sync_account(account_id)  # the snapshot reaches the bot through _on_risk
            bot.gates.resync_wanted = False
            bot.drift = 0
            self.client.positions.drift_accounts.discard(account_id)
        except Exception:
            logger.exception("bots.sync_failed", account_id=account_id)
        finally:
            self._last_resync[account_id] = time.monotonic()

    def enable_trading(self) -> None:
        for bot in self.bots.values():
            bot.trading = True
        logger.info("bots.trading_enabled", accounts=self.accounts, aggressive=self.cfg.aggressive)

    async def run(self, stop: asyncio.Event) -> None:
        """Housekeeping until ``stop`` (the trading itself is driven by ticks)."""
        while not stop.is_set():
            try:
                await self._housekeeping()
            except Exception:
                logger.exception("bots.housekeeping_failed")
            await _sleep_or_stop(stop, 1.0)

    async def _housekeeping(self) -> None:
        now = time.monotonic()
        if self.cfg.status_log_s > 0 and now - self._last_status >= self.cfg.status_log_s:
            self._last_status = now
            for bot in self.bots.values():
                logger.info("bot.status", **bot.status())
        for account_id, bot in self.bots.items():
            if bot.executor.refresh_wanted and bot.ctx.trading_day:
                bot.executor.refresh_wanted = False
                n = await self.client.refresh_orders(account_id, bot.ctx.trading_day)
                logger.info("bots.orders_refreshed", account_id=account_id, orders=n)
            wanted = (
                bot.gates.resync_wanted
                or account_id in self.client.resync_wanted
                or account_id in self.client.positions.drift_accounts
            )
            periodic = self.cfg.resync_interval_s > 0 and now - self._last_resync.get(account_id, now) >= self.cfg.resync_interval_s
            if (wanted or periodic) and bot.executor.idle:
                trading, bot.trading = bot.trading, False  # no new order while the snapshot is taken
                try:
                    await self._sync_account(account_id)
                finally:
                    bot.trading = trading

    async def shutdown(self) -> None:
        for bot in self.bots.values():
            bot.trading = False
        if self._tasks:
            await asyncio.wait(set(self._tasks), timeout=2.0)
        if self.cfg.shutdown_cancel_own and self.ready:
            own = [(a, o.client_order_id) for a, b in self.bots.items() for o in b.executor.working_orders()
                   if o.cancellable]
            results = await asyncio.gather(
                *(self.client.cancel(a, coid, reason="strategy-sim stop", timeout_ms=500, retries=0) for a, coid in own),
                return_exceptions=True,
            )
            for (a, coid), r in zip(own, results, strict=True):
                code = r[0].code if isinstance(r, tuple) else repr(r)
                logger.info("bots.shutdown_cancel", account_id=a, client_order_id=coid, code=code)
        for bot in self.bots.values():
            logger.info("bot.status", **bot.status())
        await self.client.close()

    # ---------------------------------------------------------------- callbacks
    def _on_tick(self, tick: Tick) -> None:
        for bot in self.by_instrument.get(tick.instrument_id, ()):
            for action in bot.on_tick(tick):
                self._spawn(self._execute(bot, action))

    def _on_order(self, event: OrderEvent) -> None:
        bot = self.bots.get(event.account_id)
        if bot is not None:
            bot.on_order_event(event)

    def _on_risk(self, update: RiskUpdate) -> None:
        bot = self.bots.get(update.account_id)
        if bot is None:
            return
        bot.on_risk_update(update)
        snap = update.snapshot
        if snap is None or not self.cfg.snapshot_resync or not bot.trading or not bot.executor.idle:
            return
        if bot.positions_match(snap):
            bot.drift = 0
            return
        bot.drift += 1
        if bot.drift >= self.cfg.drift_confirm:
            logger.warning("bots.positions_resynced", account_id=update.account_id,
                           had=self.client.positions.snapshot(update.account_id), source=update.source)
            self.client.seed_from_snapshot(snap)
            bot.drift = 0

    def _spawn(self, coro: Awaitable[Any]) -> None:
        task: asyncio.Task[Any] = asyncio.ensure_future(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _execute(self, bot: AccountBot, action: Action) -> None:
        ex = bot.executor
        try:
            if isinstance(action, SubmitAction):
                reply, ack = await self.client.submit(action.request)
                ex.on_submit_result(action.request, reply, ack, bot.ctx.now_ns)
            elif isinstance(action, CancelAction):
                reply, upd = await self.client.cancel(action.account_id, action.client_order_id, reason=action.why)
                ex.on_cancel_result(action, reply, upd, bot.ctx.now_ns)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("bots.action_failed", account_id=bot.account_id, action=repr(action))
            ex.inflight.pop(action.instrument_id, None)


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), seconds)


async def watch_parent(pid: int, stop: asyncio.Event, poll_s: float = 2.0,
                       alive: Callable[[], bool] | None = None) -> None:
    """Set ``stop`` once the parent process is gone (01 §5.3.6; polled every ``poll_s``).

    Uses :class:`~futrisk.runtime.parent.ParentWatch` like the services: the parent's create
    time is recorded at start, so a recycled pid is not mistaken for the parent.
    """
    check = alive if alive is not None else ParentWatch(pid).alive
    while not stop.is_set():
        if not check():
            logger.warning("bots.parent_gone", parent_pid=pid)
            stop.set()
            return
        await _sleep_or_stop(stop, poll_s)


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stop: asyncio.Event) -> Callable[[], None]:
    """Ctrl-C / SIGTERM / (Windows) CTRL_BREAK -> ``stop``; returns a function restoring the old handlers."""
    previous: dict[int, Any] = {}

    def handler(signum: int, _frame: object) -> None:
        with contextlib.suppress(RuntimeError):
            loop.call_soon_threadsafe(stop.set)

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        with contextlib.suppress(ValueError, OSError):
            previous[sig] = signal.signal(sig, handler)

    def restore() -> None:
        for sig, old in previous.items():
            with contextlib.suppress(ValueError, OSError, TypeError):
                signal.signal(sig, old)

    return restore


async def run_bots(
    config: FullConfig,
    bots_cfg: BotsConfig,
    *,
    parent_pid: int | None = None,
    parent_poll_s: float = 2.0,
    stop: asyncio.Event | None = None,
    install_signals: bool = True,
    client: FutriskClient | None = None,
    host_ready: Callable[[BotHost], None] | None = None,
) -> int:
    """Run the bots until stopped (see the module docstring); returns the exit code."""
    stop = stop or asyncio.Event()
    loop = asyncio.get_running_loop()
    restore = _install_signal_handlers(loop, stop) if install_signals else None
    host = BotHost(config, bots_cfg, client=client)
    watcher = asyncio.ensure_future(watch_parent(parent_pid, stop, parent_poll_s)) if parent_pid else None
    try:
        if not host.accounts:
            logger.error("bots.no_accounts")
            return 2
        await host.start()
        if host_ready is not None:
            host_ready(host)
        if await host.wait_ready(stop):
            await host.startup_sync()
            host.enable_trading()
            await host.run(stop)
        return 0
    finally:
        if watcher is not None:
            watcher.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watcher
        await host.shutdown()
        if restore is not None:
            restore()
        logger.info("bots.stopped")


def _load_config(home: str | None) -> tuple[HomeLayout, FullConfig]:
    layout = HomeLayout(resolve_home(home)).ensure()
    if not layout.active_json.exists():
        ensure_home_config(layout)
    return layout, load_active_config(layout)


def run(
    home: str | None = None,
    instance: str | None = None,
    shard: int | None = None,
    parent_pid: int | None = None,
    *,
    aggressive: bool | None = None,
    accounts: list[str] | str | None = None,
    config_path: str | None = None,
    seed: int | None = None,
    log_level: str | None = None,
    parent_poll_s: float = 2.0,
    console_log: bool = True,
    **_: Any,
) -> int:
    """Service entry point (``futrisk svc strategy-sim``, 01 §7.1).

    ``shard`` limits the bots to the accounts of one risk shard; ``instance`` names the
    log file (the DEALER identity stays ``strategy-sim``, 02 §4.11).
    """
    instance = instance or DEFAULT_INSTANCE
    try:
        layout, cfg = _load_config(home)
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2
    setup_logging(
        instance,
        layout.logs_dir,
        level=log_level or cfg.app.logging.level,
        retention_days=cfg.app.logging.retention_days,
        rotation_mb=cfg.app.logging.rotation_mb,
        console=console_log,
    )
    try:
        bots_cfg = load_bots_config(
            home_config_dir=cfg.root, config_path=config_path, aggressive=aggressive, seed=seed, accounts=accounts
        )
    except Exception as exc:
        logger.error("bots.config_error", error=str(exc))
        return 2
    if shard is not None:
        shard_accounts = cfg.accounts.accounts_of_shard(shard)
        chosen = [a for a in shard_accounts if bots_cfg.accounts_filter is None or a in bots_cfg.accounts_filter]
        bots_cfg = bots_cfg.model_copy(update={"accounts_filter": chosen})
    logger.info("bots.starting", instance=instance, home=str(layout.home), parent_pid=parent_pid,
                aggressive=bots_cfg.aggressive, accounts=bots_cfg.accounts_filter, seed=bots_cfg.seed)
    try:
        return run_async(run_bots(cfg, bots_cfg, parent_pid=parent_pid, parent_poll_s=parent_poll_s))
    except KeyboardInterrupt:
        return 0


__all__ = ["DEFAULT_INSTANCE", "BotHost", "run", "run_bots", "watch_parent"]
