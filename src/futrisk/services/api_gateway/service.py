"""ApiGatewayService: wires api-gateway onto :class:`~futrisk.runtime.AsyncServiceRuntime` (01 §5.3.5, 02 §4.1, §8.6).

Start-up (02 §8.6):

0. the HTTP listening socket is bound **first** (``main`` binds it even before
   the runtime starts heartbeating): a busy port fails the start before the
   process looks alive or writes a key;
1. open ``<home>/db/api.db`` (migrations, ``synchronous=FULL``, no outbox);
2. sync API keys (``app.yaml`` + bootstrap key, 01 §8.8);
3. Clone subscriptions for the read model (02 §4.1) + heartbeat observer +
   peer liveness; every applied envelope also feeds the FACT-gap / LV-silence
   repair (:mod:`.repair`); a shard's ``risk.query.accounts`` seeds the account
   view once its heartbeat reports ``RUNNING`` (again after every restart --
   FACT snapshot items may be missing right after a publisher restart -- and
   when it returns from ``DEGRADED`` to ``RUNNING``); until then its accounts
   are shown ``stale``;
4. ``config.decision`` handler and 2PC restart recovery (02 §8.5 step 6);
5. the persisted kill-switch desired state is loaded and pushed again to every
   target that did not confirm it;
6. notifier FACT subscriptions with persisted cursors (``FROM_NOW``) and resend
   of ``PENDING`` notifications;
7. the uvicorn HTTP/WS server inside the runtime's event loop (no signal
   takeover: signals stay with the runtime);
8. state ``RUNNING`` once every read-model snapshot is in and every peer is up;
   ``DEGRADED`` while a peer is down or a snapshot is still missing after 10 s.

On every committed config version: keys are re-synced (WebSocket sessions of
revoked keys are closed) and the notification channel / webhook URL is
re-resolved.

``futrisk svc api-gateway`` -> :func:`futrisk.services.api_gateway.main.run`.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
from collections.abc import Awaitable, Mapping
from typing import Any
from urllib.parse import urlparse

import uvicorn
from loguru import logger

from futrisk.bus.fact import CursorUpdate, FactKey
from futrisk.bus.rpc_common import RequestContext
from futrisk.common.config import FullConfig
from futrisk.db import Database, FactCursors
from futrisk.messages import codes
from futrisk.messages.base import Msg
from futrisk.messages.enums import EodPhase, FactStartPolicy, ServiceState
from futrisk.messages.envelope import Envelope, Reply, err_reply, ok_reply
from futrisk.messages.risk import AccountSnapshotBatch, QueryAccounts
from futrisk.messages.system import DecisionQuery
from futrisk.runtime import AsyncServiceRuntime, AsyncSubscription
from futrisk.services.api_gateway import readmodel as rm
from futrisk.services.api_gateway.app import create_app
from futrisk.services.api_gateway.auth import AuthThrottle, KeyManager
from futrisk.services.api_gateway.clockjump import ClockJumpOrchestrator
from futrisk.services.api_gateway.config2pc import ConfigCoordinator
from futrisk.services.api_gateway.gateway import Gateway, ws_audit_writer
from futrisk.services.api_gateway.notifier import Notifier, Sender, WebhookSender, resolve_channel
from futrisk.services.api_gateway.ports import HttpSimControl, RuntimeBus, SimControl
from futrisk.services.api_gateway.repair import ReadModelRepair
from futrisk.services.api_gateway.router import CLEARING, MD_GATEWAY, SUPERVISOR, TRADE_GATEWAY, CommandRouter
from futrisk.services.api_gateway.schema import METADATA, MIGRATIONS
from futrisk.services.api_gateway.store import ApiStore
from futrisk.services.api_gateway.wshub import MAX_MESSAGE_BYTES, WsHub

READY_GRACE_S = 10.0
WATCHDOG_S = 5.0


class HttpPortInUse(OSError):
    """The configured HTTP port is taken (another process, maybe another home)."""


def bind_http_socket(host: str, port: int) -> socket.socket:
    """Bind the listening socket (not yet listening); raises :class:`HttpPortInUse` when it is taken."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if os.name != "nt":
        # POSIX: rebind across TIME_WAIT after a restart (never lets two listeners share the port). Windows
        # binds across TIME_WAIT without it, and there SO_REUSEADDR would let a second process take the port.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((host, port))
    except OSError as exc:
        sock.close()
        raise HttpPortInUse(exc.errno, f"HTTP port {port} on {host} is in use: {exc.strerror or exc}") from exc
    sock.setblocking(False)
    return sock


class _QuietServer(uvicorn.Server):
    """uvicorn server that never installs signal handlers (the runtime owns them)."""

    @contextlib.contextmanager
    def capture_signals(self):  # type: ignore[no-untyped-def]
        yield


class HttpServer:
    """uvicorn inside the current event loop on a socket bound up front (port 0 allowed)."""

    def __init__(self, app: Any, host: str, port: int, *, sock: socket.socket | None = None) -> None:
        self.host = host
        self.port = port
        self.config = uvicorn.Config(
            app,
            host=host,
            port=port,
            log_config=None,
            log_level="warning",
            access_log=False,
            lifespan="off",
            ws="websockets-sansio",
            ws_max_size=MAX_MESSAGE_BYTES,  # larger client messages close the session (1009)
            timeout_graceful_shutdown=3,
        )
        self.server = _QuietServer(self.config)
        self._sock = sock
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if not self.config.loaded:
            self.config.load()
        self.server.lifespan = self.config.lifespan_class(self.config)
        sock = self._sock or bind_http_socket(self.host, self.port)
        self._sock = sock
        self.port = sock.getsockname()[1]
        await self.server.startup(sockets=[sock])
        self._task = asyncio.ensure_future(self.server.main_loop())
        self._task.set_name("http-main-loop")
        logger.info("http.listening", url=f"http://{self.host}:{self.port}")

    async def stop(self) -> None:
        if self._task is None:
            if self._sock is not None:  # bound but never served
                self._sock.close()
                self._sock = None
            return
        self.server.should_exit = True
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        with contextlib.suppress(Exception):
            await self.server.shutdown(sockets=[self._sock] if self._sock else None)
        self._sock = None


def http_endpoint(cfg: FullConfig, instance: str) -> tuple[str, int]:
    """(host, port) of ``services.yaml`` ``instances.<api-gateway>.http`` (default 127.0.0.1:8000)."""
    url = cfg.services.instance(instance).http or "http://127.0.0.1:8000"
    parsed = urlparse(url)
    return parsed.hostname or "127.0.0.1", parsed.port if parsed.port is not None else 8000


class ApiGatewayService:
    """api-gateway on top of a started :class:`AsyncServiceRuntime`."""

    def __init__(
        self,
        rt: AsyncServiceRuntime,
        *,
        sim: SimControl | None = None,
        sender: Sender | None = None,
        serve_http: bool = True,
        http_port: int | None = None,
        http_socket: socket.socket | None = None,
        env: Mapping[str, str] | None = None,
        restrict_permissions: bool = True,
        notifier_backoff_s: float = 1.0,
        ready_grace_s: float = READY_GRACE_S,
        watchdog_s: float = WATCHDOG_S,
    ) -> None:
        self.rt = rt
        self._sim_arg = sim
        self._sender_arg = sender
        self._serve_http = serve_http
        self._http_port = http_port
        self._http_socket = http_socket
        self._env = env
        self._restrict = restrict_permissions
        self._notifier_backoff_s = notifier_backoff_s
        self._ready_grace_s = ready_grace_s
        self._watchdog_s = watchdog_s
        self.db: Database | None = None
        self.gateway: Gateway | None = None
        self.http: HttpServer | None = None
        self.repair: ReadModelRepair | None = None
        self.clones: dict[str, AsyncSubscription] = {}
        self._tasks: set[asyncio.Task[Any]] = set()
        self._shard_seen: dict[str, tuple[str, str]] = {}  # shard -> (epoch, heartbeat state)
        self._seeded: set[str] = set()  # shards whose current incarnation was seeded while RUNNING
        self._seeding: set[str] = set()
        self._maintained: set[str] = set()
        self._started_mono = 0
        self._owned_sim: HttpSimControl | None = None
        self._channel = "log"
        self._webhook: WebhookSender | None = None
        self._peer_states: dict[str, str] = {}

    # ------------------------------------------------------------ handlers
    def register_handlers(self) -> None:
        """Register bus handlers; call before ``rt.start()`` (``config.decision`` may come at once)."""
        self.rt.add_handler("config.decision", self._on_decision, payload_type=DecisionQuery)

    async def _on_decision(self, command: str, payload: Msg, ctx: RequestContext) -> Reply:
        assert isinstance(payload, DecisionQuery)
        gw = self.gateway
        if gw is None:
            return err_reply(codes.UNAVAILABLE, "api-gateway is starting", retry_after_ms=1000)
        decision = await gw.coordinator.decision(payload.config_version)
        if decision is None:
            return err_reply(codes.NOT_FOUND, f"unknown config version {payload.config_version}")
        return ok_reply(decision)

    # ------------------------------------------------------------- helpers
    @property
    def instance(self) -> str:
        return self.rt.instance

    def _config(self) -> FullConfig:
        return self.rt.config

    def liveness(self, instance: str) -> str:
        if instance == self.instance:
            return "UP"
        return self.rt.peers.state(instance)

    def _heartbeat_config(self, instance: str) -> tuple[int, str] | None:
        if instance == self.instance:
            return self.rt.config.config_version, self.rt.config.config_hash
        assert self.gateway is not None
        hb = self.gateway.readmodel.heartbeat_of(instance)
        return (hb.config_version, hb.config_hash) if hb is not None else None

    def _clock_versions(self) -> dict[str, int]:
        assert self.gateway is not None
        out: dict[str, int] = {}
        for inst, seen in self.gateway.readmodel.heartbeats.items():
            if inst != self.instance and self.liveness(inst) == "UP":
                out[inst] = seen.heartbeat.clock_version
        out[self.instance] = self.rt.market_clock.version
        return out

    def synced(self) -> dict[str, bool]:
        return {p: bool(s.clone is not None and s.clone.synced) for p, s in sorted(self.clones.items())}

    def _spawn(self, coro: Awaitable[Any], name: str) -> asyncio.Task[Any]:
        task: asyncio.Task[Any] = asyncio.ensure_future(coro)
        task.set_name(name)
        self._tasks.add(task)

        def done(t: asyncio.Task[Any]) -> None:
            self._tasks.discard(t)
            if not t.cancelled() and t.exception() is not None:
                logger.opt(exception=t.exception()).error("api.task_failed", task=name)

        task.add_done_callback(done)
        return task

    # --------------------------------------------------------------- start
    async def start(self) -> Gateway:
        rt = self.rt
        assert rt.layout is not None
        self._started_mono = rt.market_clock.monotonic_ns()
        host, port = http_endpoint(rt.config, self.instance)
        if self._http_port is not None:
            port = self._http_port
        if self._serve_http and self._http_socket is None:
            self._http_socket = bind_http_socket(host, port)  # fail fast, before any key is written
        db = await rt.open_database("api", METADATA, migrations=MIGRATIONS, synchronous="FULL", outbox=False)
        self.db = db
        store = ApiStore(db)
        keys = KeyManager(store, rt.layout, env=self._env, restrict_permissions=self._restrict)
        await keys.sync(rt.config.app)
        readmodel = rm.ReadModel(rt.market_clock)
        bus = RuntimeBus(rt)
        router = CommandRouter(bus, self._config)
        sim = self._sim_arg
        if sim is None:
            self._owned_sim = HttpSimControl.from_config(rt.config)
            sim = self._owned_sim
        self._channel, url = resolve_channel(rt.config, self._env)
        if self._sender_arg is None:
            self._webhook = WebhookSender(url)
        sender: Sender = self._sender_arg or self._webhook  # type: ignore[assignment]
        notifier = Notifier(
            db=db,
            store=store,
            config=self._config,
            clock=rt.market_clock,
            sender=sender,
            channel=lambda: self._channel,
            submit=lambda fn: rt.submit_write(db, fn),
            backoff_s=self._notifier_backoff_s,
        )
        notifier.bind_loop(asyncio.get_running_loop())
        instances = lambda: sorted(self._config().services.instances)  # noqa: E731
        gw_ref: dict[str, Gateway] = {}
        throttle = AuthThrottle()

        def trading_day() -> str:
            return gw_ref["gw"].trading_day()

        hub = WsHub(
            readmodel,
            keys=keys,
            router=router,
            config=self._config,
            clock=rt.market_clock,
            trading_day=trading_day,
            clock_state=lambda: gw_ref["gw"].clock_state(),
            liveness=self.liveness,
            audit=ws_audit_writer(store, rt.market_clock),
            throttle=throttle,
        )
        coordinator = ConfigCoordinator(
            store=store,
            bus=bus,
            layout=rt.layout,
            config=self._config,
            clock=rt.market_clock,
            instances=instances,
            heartbeat_config=self._heartbeat_config,
        )
        clockjump = ClockJumpOrchestrator(bus=bus, sim=sim, clock_versions=self._clock_versions, clock=rt.market_clock)
        self.repair = ReadModelRepair(
            readmodel=readmodel,
            router=router,
            hub=hub,
            clock=rt.market_clock,
            trading_day=trading_day,
            resync=self._resync,
            spawn=self._spawn,
        )
        gw = Gateway(
            instance=self.instance,
            layout=rt.layout,
            config=self._config,
            clock=rt.market_clock,
            store=store,
            keys=keys,
            readmodel=readmodel,
            router=router,
            hub=hub,
            coordinator=coordinator,
            clockjump=clockjump,
            sim=sim,
            notifier=notifier,
            state=lambda: str(rt.state),
            liveness=self.liveness,
            epoch=rt.epoch,
            synced=self.synced,
            repair=self.repair,
            auth_throttle=throttle,
            shard_ready=lambda shard: shard in self._seeded,
        )
        gw_ref["gw"] = gw
        self.gateway = gw
        rt.on_config_commit(self._on_config_commit)
        rt.heartbeat_provider(self._heartbeat_extras)
        rt.on_stop(self.stop)

        readmodel.add_listener(self._on_change)
        self._subscribe_readmodel(readmodel)
        await coordinator.recover()
        await gw.killswitch.load()
        await gw.killswitch.reconcile()
        self._subscribe_notifier(notifier, db)
        await notifier.resume_pending()
        hub.start()
        if self._serve_http:
            self.http = HttpServer(create_app(gw), host, port, sock=self._http_socket)
            await self.http.start()
        self._spawn(self._readiness_loop(), "readiness")
        self._spawn(self._watchdog(), "readmodel-watchdog")
        logger.info("api_gateway.started", instance=self.instance, http=self.http.port if self.http else None)
        return gw

    def _subscribe_readmodel(self, readmodel: rm.ReadModel) -> None:
        rt = self.rt
        eps = rt.endpoints
        plan: list[tuple[str, list[str]]] = [
            (MD_GATEWAY, ["md.tick.", "md.bar.1m.", "md.status", "md.event"]),
            (TRADE_GATEWAY, ["td.exec.", "td.status"]),
            *[(inst, ["risk.", "oms.order.", "sys.dayroll"]) for inst in rt.config.services.risk_instances()],
            (CLEARING, ["sys.eod", "clearing.settled", "report.ready", "backtest.status"]),
            (SUPERVISOR, ["sys.service"]),
        ]
        repair = self.repair
        assert repair is not None
        repair.watch_periodic(MD_GATEWAY, "md.status")  # 1 Hz LV topics: long silence -> ctl.snapshot
        repair.watch_periodic(TRADE_GATEWAY, "td.status")
        repair.watch_periodic(rt.config.services.coordinator(), "risk.firm")

        def apply(env: Envelope) -> None:
            repair.observe(env)
            readmodel.apply(env)

        for publisher, prefixes in plan:
            if not eps.has(publisher) or not eps.instance(publisher).pub:
                logger.warning("readmodel.no_publisher", publisher=publisher)
                continue
            self.clones[publisher] = rt.subscribe(publisher, prefixes, apply, snapshot=True)
        rt.on_heartbeat(readmodel.on_heartbeat)
        peers = [i for i in rt.config.services.instances if i != self.instance]
        rt.watch_peers(peers, on_change=self._on_peer)

    def _resync(self, publisher: str) -> None:
        sub = self.clones.get(publisher)
        if sub is not None and sub.clone is not None:
            sub.clone.resync()

    def _subscribe_notifier(self, notifier: Notifier, db: Database) -> None:
        rt = self.rt
        cursors = FactCursors(db).load_all()
        sources: list[tuple[str, str]] = [(inst, "risk.event") for inst in rt.config.services.risk_instances()]
        sources += [(MD_GATEWAY, "md.event"), (CLEARING, "sys.eod"), (SUPERVISOR, "sys.service")]
        for publisher, topic in sources:
            if not rt.endpoints.has(publisher) or not rt.endpoints.instance(publisher).pub:
                continue
            rt.subscribe_fact(
                publisher,
                [topic],
                policy=FactStartPolicy.FROM_NOW,
                apply=self._apply_notify,
                cursors=cursors,
                on_stream_change=self._on_stream_change,
            )

    def _apply_notify(self, env: Envelope, cursor: CursorUpdate) -> None:
        assert self.gateway is not None and self.gateway.notifier is not None
        self.gateway.notifier.apply(env, cursor)

    @staticmethod
    def _on_stream_change(key: FactKey, old: str | None, new: str, next_seq: int, done: Any) -> None:
        # the publisher's database was recreated/restored: continue on the new stream; duplicates
        # of already-notified events are suppressed by notify_key (02 §9.8)
        logger.warning("notify.stream_changed", src=key[0], topic=key[1], old=old, new=new, next_seq=next_seq)
        done()

    # ------------------------------------------------------------ callbacks
    def _on_change(self, ch: rm.Change) -> None:
        if ch.kind == rm.HEARTBEAT and ch.key in self.rt.config.services.risk_instances():
            self._on_shard_heartbeat(ch.key, ch.obj)
        elif ch.kind == rm.EOD and str(ch.obj.phase) == EodPhase.DONE.value:
            td = ch.obj.trading_day
            if td not in self._maintained:
                self._maintained.add(td)
                self._spawn(self._maintenance(td), f"maintenance-{td}")

    def _on_peer(self, peer: str, state: str) -> None:
        logger.info("api.peer", peer=peer, state=state)
        previous = self._peer_states.get(peer)
        self._peer_states[peer] = state
        gw = self.gateway
        if gw is not None:
            gw.on_peer_change(peer, state)
            if state == "UP" and previous == "DOWN" and self.repair is not None:
                self.repair.request_resync(peer, "peer back up")  # catch up what the outage hid

    def _on_config_commit(self, cfg: FullConfig, effects: dict[str, Any]) -> None:
        gw = self.gateway
        if gw is None:
            return
        self._spawn(gw.resync_keys(cfg.app), "keys-resync")
        channel, url = resolve_channel(cfg, self._env)
        if channel != self._channel:
            logger.info("notify.channel_changed", old=self._channel, channel=channel)  # never log the URL
        self._channel = channel
        if self._webhook is not None:
            self._webhook.set_url(url)

    def _heartbeat_extras(self) -> dict[str, Any]:
        gw = self.gateway
        if gw is None:
            return {}
        ws = gw.hub.stats()
        queues = {"ws.clients": ws["clients"], "ws.queued": ws["queued"]}
        detail: dict[str, Any] = {
            "synced": self.synced(),
            "seeded": sorted(self._seeded),
            "http_port": self.http.port if self.http else None,
            "kill_switch_pending": gw.killswitch.pending(),
            "kill_switch_failed": gw.killswitch.failed(),
        }
        if gw.notifier is not None:
            st = gw.notifier.stats
            detail["notifications"] = {"sent": st.sent, "failed": st.failed, "duplicates": st.duplicates,
                                       "suppressed": st.suppressed_stale + st.suppressed_rate}
        if self.repair is not None:
            detail["repair"] = self.repair.stats()
        return {"trading_day": gw.trading_day(), "queues": queues, "detail": detail}

    # ------------------------------------------------------------ seeding
    def _on_shard_heartbeat(self, shard: str, hb: Any) -> None:
        """Seed a shard's accounts once it reports RUNNING (new incarnation, or back from DEGRADED)."""
        state = str(hb.state)
        prev = self._shard_seen.get(shard)
        self._shard_seen[shard] = (hb.epoch, state)
        if prev is None or prev[0] != hb.epoch:
            if shard in self._seeded:  # a new incarnation: stale until it is seeded again
                self._seeded.discard(shard)
                self._accounts_changed(shard)
        elif prev[1] != ServiceState.RUNNING.value and state == ServiceState.RUNNING.value:
            self._seeded.discard(shard)  # back to RUNNING (e.g. from DEGRADED): seed again
        if state == ServiceState.RUNNING.value and shard not in self._seeded and shard not in self._seeding:
            self._seeding.add(shard)
            self._spawn(self._seed_shard(shard, hb.epoch), f"seed-{shard}")

    def _accounts_changed(self, shard: str) -> None:
        if self.gateway is not None:
            self.gateway.hub.mark_accounts(self.gateway.router.accounts_of(shard))

    async def _seed_shard(self, instance: str, epoch: str | None = None) -> bool:
        gw = self.gateway
        assert gw is not None
        try:
            reply = await gw.router.request(instance, "risk.query.accounts", QueryAccounts(), timeout_ms=3000,
                                            retries=1)
            if not reply.ok:
                logger.debug("readmodel.seed_failed", shard=instance, code=reply.code)
                return False  # the next RUNNING heartbeat tries again
            batch = reply.open_as(AccountSnapshotBatch)
            n = gw.readmodel.seed_snapshots(batch.items if batch else [])
            if epoch is None or self._shard_seen.get(instance, ("", ""))[0] == epoch:
                self._seeded.add(instance)
                self._accounts_changed(instance)
            logger.info("readmodel.seeded", shard=instance, accounts=len(batch.items) if batch else 0, applied=n)
            return True
        finally:
            self._seeding.discard(instance)

    async def _maintenance(self, trading_day: str) -> None:
        """03 §8 after ``sys.eod`` DONE: purge by retention, back up api.db."""
        gw = self.gateway
        assert gw is not None and self.rt.layout is not None
        try:
            # processed_commands: keep the new current day and the settled day T (its previous day)
            purged = await gw.store.purge(keep_processed_from_td=trading_day, now_ns=gw.clock.now_ns())
            await gw.store.backup(self.rt.layout.backup_dir / trading_day / "api.db")
            logger.info("api_db.maintenance", trading_day=trading_day, purged=purged)
        except Exception:
            logger.exception("api_db.maintenance_failed", trading_day=trading_day)

    # ----------------------------------------------------------- readiness
    async def _readiness_loop(self) -> None:
        rt = self.rt
        while True:
            synced = all(self.synced().values())
            elapsed = (rt.market_clock.monotonic_ns() - self._started_mono) / 1e9
            peers_up = all(self.liveness(p) == "UP" for p in rt.config.services.instances if p != self.instance)
            if synced and peers_up:
                rt.set_state(ServiceState.RUNNING)
            elif (synced and elapsed >= 2.0) or elapsed >= self._ready_grace_s:
                rt.set_state(ServiceState.DEGRADED)  # a peer is down or a snapshot is still missing
            await asyncio.sleep(0.5)

    async def _watchdog(self) -> None:
        """LV topics that went silent while their publisher is up -> ``ctl.snapshot`` (02 §4.5)."""
        while True:
            await asyncio.sleep(self._watchdog_s)
            if self.repair is not None:
                try:
                    self.repair.check_silence(lambda p: self.liveness(p) == "UP")
                except Exception:
                    logger.exception("readmodel.watchdog_failed")

    # ---------------------------------------------------------------- stop
    async def stop(self) -> None:
        gw = self.gateway
        if self.http is not None:
            await self.http.stop()
            self.http = None
        elif self._http_socket is not None:
            self._http_socket.close()
        self._http_socket = None
        for t in list(self._tasks):
            t.cancel()
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        if gw is not None:
            await gw.hub.stop()
            await gw.aclose()
            await gw.coordinator.close()
            if gw.notifier is not None:
                await gw.notifier.close()
        if self._owned_sim is not None:
            await self._owned_sim.aclose()
            self._owned_sim = None


__all__ = ["ApiGatewayService", "HttpPortInUse", "HttpServer", "bind_http_socket", "http_endpoint"]
