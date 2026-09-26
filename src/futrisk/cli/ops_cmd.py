"""Operations CLI: ``futrisk up | down | ps | logs | kill | status | demo`` (01 §7.1; 02 §4.1, §6.6, §9.6).

* ``up --clock-only`` runs a supervisor that only keeps the market clock (``sys.clock``,
  ``svc.clock``) for services started elsewhere (one container per service); ``ps`` and
  ``status`` show it as such;
* ``up``, ``down``, ``ps``, ``kill`` (and ``status``) talk to the supervisor over the bus
  with DEALER identity ``cli`` (02 §4.1: ``svc.*`` is whitelisted for ``cli``). The
  supervisor that answers must belong to this home: ``Pong.home_id`` / ``ServiceList.home_id``
  are compared with ``HomeLayout.home_id()`` first, and a supervisor of another home that
  owns the configured ports is refused (never stopped, killed or reported as ours);
* ``logs`` reads ``<home>/logs`` locally and never touches the bus (01 §8.7); ``-f`` opens the
  file with delete sharing (Windows), so loguru can rename and compress it on rotation while
  it is followed, and continues with the new file without losing lines;
* ``demo`` is the one-click start: it launches ``futrisk up`` in a child process when no
  supervisor runs, waits for **this home's** api-gateway (heartbeat ``RUNNING`` with the same
  pid on two polls, ``/health`` of the same home) before it reads
  ``<home>/secrets/bootstrap_admin.key``, seeds the first-day books once (``POST
  /api/v1/settlement/seed-sod`` from ``accounts.yaml``; never when any account snapshot, SOD
  or settlement exists), optionally starts a scenario (``POST /api/v1/demo/scenario``) and
  opens the dashboard. Business requests go through the api-gateway REST API (02 §9.6).
  A scenario (demo mode only) switches strategy-sim to ``FUTRISK_BOTS_MODE=scenario``
  (``svc.start`` + ``env``) just before it starts, and back when it cannot start;
  scenarios calibrated for a built book (:data:`SCENARIO_BOOKS`: ``breaker`` needs ACC3's
  long rb/hc/i at 0.7x equity) wait for it first. ``--bots normal|scenario`` sets the bots'
  mode of a running system explicitly.

Heavy modules (bus, httpx, rich) are imported inside the commands: ``futrisk.main`` loads
this module for every invocation, including each ``futrisk svc`` child process.
"""

from __future__ import annotations

import contextlib
import functools
import getpass
import json
import os
import re
import stat
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from datetime import time as dtime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any

import typer

from futrisk.common.paths import HomeLayout, is_frozen, resolve_home

if TYPE_CHECKING:
    import httpx
    from rich.console import Console
    from rich.text import Text

    from futrisk.common.config import FullConfig
    from futrisk.messages.common import Pong
    from futrisk.messages.envelope import Reply
    from futrisk.messages.system import ClockState, Heartbeat, ServiceInfo, ServiceList

CLI_IDENTITY = "cli"
SUPERVISOR = "supervisor"
API_GATEWAY = "api-gateway"
API_PREFIX = "/api/v1"
BOOTSTRAP_KEY_FILE = "bootstrap_admin.key"
API_KEY_ENV = "FUTRISK_API_KEY"
SEED_MARKER = "demo_seed.json"
SUPERVISOR_CONSOLE_LOG = "supervisor.console.log"
STRATEGY_SIM = "strategy-sim"
BOTS_MODE_ENV = "FUTRISK_BOTS_MODE"  # futrisk.sim.bots.config.ENV_MODE
SCENARIO_BOTS_MODE = "scenario"  # the bots' aggressive overrides (bots.yaml): ACC3 builds its book at once
NORMAL_BOTS_MODE = "normal"  # ``--bots normal``: the override is removed (the bots' default mode)
BOTS_MODES = (NORMAL_BOTS_MODE, SCENARIO_BOTS_MODE)
BOOK_POLL_S = 1.0
BOOK_NOTE_S = 5.0
API_POLL_S = 0.5
FOLLOW_POLL_S = 0.25
FOREIGN_HOME = "the supervisor on these ports belongs to another home ({other}); use that home or change ports"
_INSTANCE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_SVC_ENV_RE = re.compile(r"^(?P<inst>[A-Za-z0-9][A-Za-z0-9_.-]*):(?P<name>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>.*)$")
_RETRY_HTTP = frozenset({429, 502, 503, 504})
_LEVEL_STYLE = {
    "TRACE": "dim",
    "DEBUG": "blue",
    "INFO": "white",
    "SUCCESS": "green",
    "WARNING": "yellow",
    "ERROR": "bold red",
    "CRITICAL": "bold white on red",
}
_STATE_STYLE = {
    "RUNNING": "green",
    "STARTING": "cyan",
    "RESTARTING": "yellow",
    "STOPPING": "yellow",
    "STOPPED": "dim",
    "EXITED": "red",
    "FAILED": "bold red",
}

HomeOpt = typer.Option(None, "--home", help="Runtime home directory (default: FUTRISK_HOME or the default home)")


@dataclass(frozen=True)
class BookRequirement:
    """What a scenario is calibrated for: ``account_id``'s gross LONG notional in ``products``
    must reach ``min_ratio`` × equity before the scenario starts."""

    account_id: str
    products: tuple[str, ...]
    min_ratio: float

    def describe(self) -> str:
        return f"{self.account_id}'s {'/'.join(self.products)} long book"


# Scenarios whose price moves are sized for a built book (others start at once).
SCENARIO_BOOKS: dict[str, BookRequirement] = {
    # config/scenarios/breaker.yaml: legs sized for ACC3's full black-sector long book (~0.78x equity)
    "breaker": BookRequirement("ACC3", ("rb", "hc", "i"), 0.7),
}


def parse_svc_env(values: list[str] | None) -> dict[str, dict[str, str]]:
    """``["strategy-sim:FUTRISK_BOTS_MODE=scenario", ...]`` -> ``{instance: {NAME: value}}``."""
    out: dict[str, dict[str, str]] = {}
    for item in values or []:
        m = _SVC_ENV_RE.match(item.strip())
        if m is None:
            raise typer.BadParameter(f"expected INSTANCE:NAME=VALUE, got {item!r}", param_hint="--svc-env")
        out.setdefault(m["inst"], {})[m["name"]] = m["value"]
    return out


def svc_env_args(env: dict[str, dict[str, str]] | None) -> list[str]:
    """``--svc-env`` arguments reproducing ``env`` (inverse of :func:`parse_svc_env`)."""
    args: list[str] = []
    for inst, overrides in sorted((env or {}).items()):
        for name, value in sorted(overrides.items()):
            args += ["--svc-env", f"{inst}:{name}={value}"]
    return args


def check_svc_env(cfg: FullConfig, env: dict[str, dict[str, str]]) -> None:
    """The supervisor's per-instance allow-list (strategy-sim ``FUTRISK_BOTS_*`` only; never a
    name that holds a secret in the configuration); raises ``typer.BadParameter``."""
    from futrisk.bus.endpoints import service_of
    from futrisk.services.supervisor.process import secret_env_names, validate_env

    forbidden = secret_env_names(cfg)
    for inst, overrides in env.items():
        try:
            validate_env(service_of(inst), dict(overrides), forbidden=forbidden)
        except ValueError as exc:
            raise typer.BadParameter(f"{inst}: {exc}", param_hint="--svc-env") from exc


# ====================================================================== plumbing
def _console() -> Console:
    from rich.console import Console

    return Console(highlight=False, soft_wrap=True)


def _actor() -> str:
    try:
        return f"cli:{getpass.getuser()}"
    except Exception:
        return CLI_IDENTITY


@dataclass
class OpsContext:
    """Home layout and the committed configuration of that home."""

    layout: HomeLayout
    cfg: FullConfig

    @property
    def api_url(self) -> str | None:
        inst = self.cfg.services.instances.get(API_GATEWAY)
        return inst.http.rstrip("/") if inst is not None and inst.http else None

    @property
    def home_id(self) -> str:
        return self.layout.home_id()


def load_context(home: str | None) -> OpsContext:
    """Resolve ``<home>`` and load its committed config (a fresh home gets the defaults).

    A broken configuration ends the command with exit code 2.
    """
    from futrisk.common.config import ConfigError, ensure_home_config, load_active_config

    layout = HomeLayout(resolve_home(home))
    try:
        ensure_home_config(layout)
        return OpsContext(layout, load_active_config(layout))
    except ConfigError as exc:
        typer.secho(f"configuration error in {layout.config_dir}: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(2) from exc


#: see SupervisorClient: connect grace of the CLI's blocking client (ms)
CLI_CONNECT_GRACE_MS = 1500


class SupervisorClient:
    """Blocking DEALER with identity ``cli`` connected to the supervisor's ROUTER."""

    def __init__(self, cfg: FullConfig) -> None:
        from futrisk.bus.endpoints import Endpoints
        from futrisk.bus.rpc_client import BlockingRpcClient

        # a fresh CLI connection may take ~1 s to connect (e.g. Docker Desktop drops the first SYN):
        # wait longer than the services' 300 ms grace before answering "peer not connected"
        self._client = BlockingRpcClient(Endpoints(cfg.services), identity=CLI_IDENTITY,
                                         connect_grace_ms=CLI_CONNECT_GRACE_MS)

    def __enter__(self) -> SupervisorClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def request(self, command: str, payload: Any, *, timeout_ms: int = 2_000, retries: int = 0) -> Reply:
        return self._client.request(SUPERVISOR, command, payload, timeout_ms=timeout_ms, retries=retries)

    def ping(self, timeout_ms: int = 1_000) -> Pong | None:
        """``ctl.ping``; None when the supervisor does not answer. ``Pong.ts_ns`` is its market time."""
        from futrisk.messages.common import Ping, Pong

        reply = self.request("ctl.ping", Ping(), timeout_ms=timeout_ms)
        return reply.open_as(Pong) if reply.ok else None

    def status(self, timeout_ms: int = 2_000) -> Heartbeat | None:
        from futrisk.messages.common import StatusRequest
        from futrisk.messages.system import Heartbeat

        reply = self.request("ctl.status", StatusRequest(), timeout_ms=timeout_ms)
        return reply.open_as(Heartbeat) if reply.ok else None

    def ps(self, timeout_ms: int = 2_000) -> ServiceList | None:
        from futrisk.messages.system import PsRequest, ServiceList

        reply = self.request("svc.ps", PsRequest(), timeout_ms=timeout_ms)
        return reply.open_as(ServiceList) if reply.ok else None

    def clock(self, timeout_ms: int = 2_000) -> ClockState | None:
        """Latest ``sys.clock`` from the supervisor's LV cache (``ctl.snapshot``)."""
        from futrisk.messages import decode, decode_envelope
        from futrisk.messages.common import SnapshotBatch, SnapshotRequest
        from futrisk.messages.system import ClockState
        from futrisk.messages.topics import SYS_CLOCK

        reply = self.request("ctl.snapshot", SnapshotRequest(prefixes=[SYS_CLOCK]), timeout_ms=timeout_ms)
        if not reply.ok:
            return None
        batch = reply.open_as(SnapshotBatch)
        for item in batch.items if batch is not None else []:
            if item.topic == SYS_CLOCK:
                env = decode_envelope(item.envelope)
                if env.type == "ClockState":
                    return decode(env.payload, ClockState)
        return None


def foreign_home_message(other: str | None) -> str:
    return FOREIGN_HOME.format(other=other or "it reports no home id")


def supervisor_home_problem(ctx: OpsContext, pong: Pong | None, lst: ServiceList | None = None) -> str | None:
    """``None`` when the supervisor that answered belongs to ``ctx``'s home, else the refusal text.

    Both ``Pong.home_id`` (the runtime's ``ctl.ping``) and ``ServiceList.home_id`` (``svc.ps``)
    are checked when present; a supervisor that reports neither is not trusted either.
    """
    local = ctx.home_id
    seen = [h for h in (pong.home_id if pong is not None else None, lst.home_id if lst is not None else None) if h]
    for other in seen:
        if other != local:
            return foreign_home_message(other)
    return None if seen else foreign_home_message(None)


def _connect_own(ctx: OpsContext, sc: SupervisorClient, console: Console, *, timeout_ms: int = 1_000) -> Pong | None:
    """Ping the supervisor and verify its home; ``None`` when none answers, exit 1 when it is foreign."""
    pong = sc.ping(timeout_ms=timeout_ms)
    if pong is None:
        return None
    problem = supervisor_home_problem(ctx, pong, sc.ps(timeout_ms=max(1_000, timeout_ms)))
    if problem is not None:
        console.print(f"[bold red]{problem}[/]")
        raise typer.Exit(1)
    return pong


def _svc_request(instance: str, env: dict[str, str | None] | None = None) -> Any:
    from futrisk.common.ids import uuid7
    from futrisk.messages.system import SvcRequest

    return SvcRequest(command_id=uuid7(), instance=instance, actor=_actor(), env=env)


def _fmt_duration(ns: int | None) -> str:
    if ns is None or ns < 0:
        return "-"
    s = ns // 1_000_000_000
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h >= 48:
        return f"{h // 24}d{h % 24:02d}h"
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def _fmt_age(ns: int | None) -> str:
    if ns is None:
        return "-"
    s = max(0, ns) / 1_000_000_000
    return f"{s:.1f}s" if s < 60 else _fmt_duration(ns)


def _fmt_market(ns: int) -> str:
    from futrisk.common.clock import ns_to_bj

    return ns_to_bj(ns).strftime("%Y-%m-%d %H:%M:%S")


def _clock_only(hb: Heartbeat | None) -> bool:
    """The supervisor's heartbeat says it runs in clock-only mode (no managed processes)."""
    detail = hb.detail if hb is not None else None
    return bool(isinstance(detail, dict) and detail.get("clock_only"))


def _summary(items: list[ServiceInfo]) -> str:
    running = sum(1 for i in items if str(i.state) == "RUNNING")
    others = [f"{i.instance}={i.state}" for i in items if str(i.state) != "RUNNING"]
    text = f"{running}/{len(items)} running"
    return f"{text}; {', '.join(others)}" if others else text


# ====================================================================== up
def up(
    home: str | None = HomeOpt,
    headless: bool = typer.Option(False, "--headless", help="No console log output (JSON logs still go to <home>/logs)"),
    start_at: str | None = typer.Option(None, "--start-at", hidden=True, help="Initial demo market time (ISO, Beijing)"),
    parent_pid: int | None = typer.Option(None, "--parent-pid", hidden=True, help="Stop everything when this process exits"),
    svc_env: list[str] | None = typer.Option(
        None,
        "--svc-env",
        help="Environment override of one instance, kept across its restarts: INSTANCE:NAME=VALUE "
        "(repeatable; strategy-sim FUTRISK_BOTS_MODE/_SEED/_ACCOUNTS only), e.g. strategy-sim:FUTRISK_BOTS_MODE=scenario",
    ),
    clock_only: bool = typer.Option(
        False,
        "--clock-only",
        help="Only keep and publish the market clock (sys.clock, svc.clock); start no service - they run "
        "elsewhere, e.g. one container per service (docker compose)",
    ),
) -> None:
    """Run the supervisor in the foreground: start every service; Ctrl-C stops everything."""
    instance_env = parse_svc_env(svc_env)
    if clock_only and instance_env:
        raise typer.BadParameter("--svc-env has no effect with --clock-only (no service is started)", param_hint="--svc-env")
    ctx = load_context(home)
    check_svc_env(ctx.cfg, instance_env)
    with SupervisorClient(ctx.cfg) as sc:
        running = sc.ping(timeout_ms=800)
        problem = supervisor_home_problem(ctx, running, sc.ps(timeout_ms=1_000)) if running is not None else None
    if running is not None:
        text = problem or f"a supervisor is already running for {ctx.layout.home} (use `futrisk ps` / `futrisk down`)"
        typer.secho(text, fg=typer.colors.RED)
        raise typer.Exit(1)
    if not headless:
        if clock_only:
            typer.echo(f"futrisk up --clock-only: home {ctx.layout.home}; market clock only, no service is started")
        else:
            typer.echo(f"futrisk up: home {ctx.layout.home}; dashboard {ctx.api_url or '-'}; Ctrl-C stops every service")
    from futrisk.services.supervisor.main import run as run_supervisor

    code = run_supervisor(
        home=str(ctx.layout.home),
        parent_pid=parent_pid,
        log_console=not headless,
        start_at=start_at,
        instance_env=instance_env or None,
        clock_only=clock_only,
    )
    raise typer.Exit(code)


# ====================================================================== down
def _wait_pid_exit(pid: int | None, timeout_s: float) -> bool:
    if not pid:
        return True
    import psutil

    try:
        proc = psutil.Process(pid)
        proc.wait(timeout=timeout_s)
    except psutil.NoSuchProcess:
        return True
    except psutil.TimeoutExpired:
        return False
    except psutil.Error:
        return True
    return True


def down(
    home: str | None = HomeOpt,
    timeout: float = typer.Option(180.0, "--timeout", help="Seconds to wait for the whole system to stop"),
) -> None:
    """Stop every service in reverse start order, then the supervisor (``svc.stop *``)."""
    ctx = load_context(home)
    console = _console()
    with SupervisorClient(ctx.cfg) as sc:
        pong = _connect_own(ctx, sc, console, timeout_ms=1_500)
        if pong is None:
            console.print("supervisor is not running; nothing to stop")
            return
        pid = pong.pid
        if pid is None:
            hb = sc.status(timeout_ms=1_500)
            pid = hb.pid if hb is not None else None
        reply = sc.request("svc.stop", _svc_request("*"), timeout_ms=5_000, retries=2)
        if not reply.ok:
            console.print(f"[red]svc.stop * failed: {reply.code} {reply.message or ''}[/]")
            raise typer.Exit(1)
        console.print("stopping all services (reverse start order)...")
        deadline = time.monotonic() + timeout
        last = ""
        stopped = False
        while time.monotonic() < deadline:
            if sc.ping(timeout_ms=1_000) is None:
                stopped = True
                break
            lst = sc.ps(timeout_ms=1_000)
            if lst is not None:
                text = _summary(lst.items)
                if text != last:
                    console.print(f"  {text}")
                    last = text
            time.sleep(0.5)
    if not stopped:
        console.print(f"[red]the supervisor still answers after {timeout:.0f}s[/]")
        raise typer.Exit(1)
    if not _wait_pid_exit(pid, 15.0):
        console.print(f"[yellow]supervisor process {pid} is still exiting[/]")
    console.print("[green]all services stopped[/]")


# ====================================================================== ps
def _state_text(state: str) -> Text:
    from rich.text import Text

    return Text(state, style=_STATE_STYLE.get(state, ""))


def _detail_text(info: ServiceInfo) -> str:
    parts: list[str] = []
    if info.env_keys:
        parts.append(f"env {','.join(info.env_keys)}")
    if info.last_error and str(info.state) != "RUNNING":
        parts.append(info.last_error if len(info.last_error) <= 90 else info.last_error[:87] + "...")
    return "; ".join(parts)


def render_ps(console: Console, lst: ServiceList, hb: Heartbeat | None, market_now_ns: int | None) -> None:
    from rich import box
    from rich.table import Table

    table = Table(title="futrisk services", title_justify="left", header_style="bold", box=box.SIMPLE_HEAD, pad_edge=False)
    names = [SUPERVISOR, *(i.instance for i in lst.items)]
    table.add_column("INSTANCE", no_wrap=True, min_width=max(len(n) for n in names))
    table.add_column("STATE", no_wrap=True, min_width=10)
    for col in ("PID", "UPTIME", "RESTARTS", "EXIT", "HEARTBEAT"):
        table.add_column(col, justify="right" if col in ("PID", "RESTARTS", "EXIT") else "left", no_wrap=True)
    details = [_detail_text(i) for i in lst.items]
    with_detail = any(details)
    if with_detail:
        table.add_column("DETAIL", overflow="fold")
    if hb is not None:
        wall_up = time.time_ns() - hb.started_ns
        row: list[str | Text] = [
            SUPERVISOR,
            _state_text(str(hb.state)),
            str(hb.pid),
            _fmt_duration(wall_up if 0 <= wall_up < 30 * 86_400 * 10**9 else None),
            "-",
            "-",
            f"clock v{hb.clock_version}",
        ]
        table.add_row(*row, *([""] if with_detail else []))
    for info, detail in zip(lst.items, details, strict=True):
        uptime = market_now_ns - info.started_ns if (market_now_ns and info.started_ns) else None
        hb_text = "-"
        if info.heartbeat_state is not None:
            age = (market_now_ns - info.last_heartbeat_ns) if (market_now_ns and info.last_heartbeat_ns) else None
            hb_text = f"{info.heartbeat_state} {_fmt_age(age)}" if age is not None else str(info.heartbeat_state)
        table.add_row(
            info.instance,
            _state_text(str(info.state)),
            str(info.pid) if info.pid else "-",
            _fmt_duration(uptime),
            str(info.restarts),
            "-" if info.last_exit_code is None else str(info.last_exit_code),
            hb_text,
            *([detail] if with_detail else []),
        )
    console.print(table)


def ps(
    home: str | None = HomeOpt,
    as_json: bool = typer.Option(False, "--json", help="Print svc.ps as JSON"),
) -> None:
    """Show every service process as seen by the supervisor (``svc.ps``)."""
    ctx = load_context(home)
    console = _console()
    with SupervisorClient(ctx.cfg) as sc:
        pong = sc.ping()
        if pong is None:
            console.print("[red]supervisor is not running[/] (start it with `futrisk up` or `futrisk demo`)")
            raise typer.Exit(1)
        lst = sc.ps()
        problem = supervisor_home_problem(ctx, pong, lst)
        if problem is not None:
            console.print(f"[bold red]{problem}[/]")
            raise typer.Exit(1)
        hb = sc.status()
    if lst is None:
        console.print("[red]svc.ps failed[/]")
        raise typer.Exit(1)
    if as_json:
        from futrisk.messages import to_builtins

        typer.echo(json.dumps(to_builtins(lst), ensure_ascii=False, indent=2))
        return
    render_ps(console, lst, hb, pong.ts_ns)
    if _clock_only(hb):
        console.print("[dim]clock-only supervisor: it keeps the market clock; the services run outside it[/]")


# ====================================================================== kill
def kill(
    instance: str = typer.Argument(..., help="Instance to kill, e.g. md-gateway or risk-engine-1"),
    home: str | None = HomeOpt,
) -> None:
    """Demo only: hard-kill a service to simulate a crash; the supervisor restarts it (``svc.kill``)."""
    from futrisk.messages.system import ServiceInfo

    ctx = load_context(home)
    console = _console()
    with SupervisorClient(ctx.cfg) as sc:
        if _connect_own(ctx, sc, console) is None:
            console.print("[red]supervisor is not running[/]")
            raise typer.Exit(1)
        reply = sc.request("svc.kill", _svc_request(instance), timeout_ms=10_000, retries=1)
    if not reply.ok:
        console.print(f"[red]kill {instance} failed: {reply.code}[/] {reply.message or ''}")
        raise typer.Exit(1)
    info = reply.open_as(ServiceInfo)
    console.print(
        f"killed {instance}: now [bold]{info.state}[/] (exit code {info.last_exit_code}, restarts today {info.restarts}); "
        "the supervisor restarts it per its restart policy"
    )


# ====================================================================== status
def _get_json(client: httpx.Client, url: str) -> tuple[int | None, dict[str, Any]]:
    """``GET url`` -> ``(status code or None when unreachable, the envelope's data dict)``."""
    import httpx

    try:
        resp = client.get(url)
    except httpx.HTTPError:
        return None, {}
    data = _body(resp).get("data")
    return resp.status_code, data if isinstance(data, dict) else {}


def _health_problem(url: str, home_id: str, timeout_s: float = 2.0) -> str | None:
    """``None`` when ``url`` (``/health``) answers 200 for ``home_id``, else what is wrong."""
    import httpx

    with httpx.Client(trust_env=False, timeout=timeout_s) as client:
        code, data = _get_json(client, url)
    if code != 200:
        return "unreachable" if code is None else f"HTTP {code}"
    other = data.get("home_id")
    if isinstance(other, str) and other and other != home_id:
        return f"served by another home ({other})"
    return None


def status(home: str | None = HomeOpt) -> None:
    """Quick health summary: supervisor, services, market clock, dashboard; exit 1 when unhealthy."""
    ctx = load_context(home)
    console = _console()
    with SupervisorClient(ctx.cfg) as sc:
        pong = sc.ping()
        if pong is None:
            console.print("supervisor  [red]not running[/]")
            raise typer.Exit(1)
        lst = sc.ps()
        problem = supervisor_home_problem(ctx, pong, lst)
        if problem is not None:
            console.print(f"supervisor  [bold red]{problem}[/]")
            raise typer.Exit(1)
        hb = sc.status()
        clk = sc.clock()
    items = lst.items if lst is not None else []
    clock_only = _clock_only(hb)
    healthy = lst is not None and (bool(items) or clock_only) and all(str(i.state) == "RUNNING" for i in items)
    pid = pong.pid or (hb.pid if hb is not None else "?")
    console.print(f"supervisor  [green]running[/] (pid {pid})" + (" - clock only" if clock_only else ""))
    style = "green" if healthy else "yellow"
    if clock_only and not items:
        console.print("services    [green]none managed here[/] (clock-only supervisor: they run outside it)")
    else:
        console.print(f"services    [{style}]{_summary(items)}[/]")
    if clk is not None:
        market_now = time.time_ns() + clk.offset_ns
        console.print(f"market      {_fmt_market(market_now)} ({clk.mode}, clock v{clk.version})")
    api = ctx.api_url
    if api:
        api_problem = _health_problem(f"{api}{API_PREFIX}/health", ctx.home_id)
        healthy = healthy and api_problem is None
        console.print(f"dashboard   {api}  " + ("[green]ok[/]" if api_problem is None else f"[red]{api_problem}[/]"))
    if not healthy:
        raise typer.Exit(1)


# ====================================================================== logs
def open_shared(path: Path) -> IO[bytes]:
    """Open ``path`` for reading without blocking its writer's rename or delete.

    Windows: ``CreateFileW`` with ``FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE``
    (Python's ``open`` omits delete sharing, so loguru could not rotate or compress a file
    that is being followed); elsewhere a plain ``open``.
    """
    if sys.platform != "win32":
        return open(path, "rb")  # noqa: SIM115 - returned to the caller
    import ctypes
    import msvcrt
    from ctypes import wintypes

    kernel32 = _kernel32()
    generic_read, share_all, open_existing, normal = 0x80000000, 0x1 | 0x2 | 0x4, 3, 0x80
    handle = kernel32.CreateFileW(str(path), generic_read, share_all, None, open_existing, normal, None)
    if handle in (None, wintypes.HANDLE(-1).value):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        fd = msvcrt.open_osfhandle(handle, os.O_RDONLY | getattr(os, "O_BINARY", 0))
    except OSError:
        kernel32.CloseHandle(handle)
        raise
    return os.fdopen(fd, "rb")


@functools.cache
def _kernel32() -> Any:
    import ctypes
    from ctypes import wintypes

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)  # type: ignore[attr-defined,unused-ignore]
    kernel32.CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    kernel32.CreateFileW.restype = wintypes.HANDLE
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    return kernel32


def _mtime(p: Path) -> float | None:
    try:
        st = p.stat()
    except OSError:  # vanished, or a rotated file pending deletion (Windows)
        return None
    return st.st_mtime if stat.S_ISREG(st.st_mode) else None


def find_log_file(logs_dir: Path, instance: str, *, console_log: bool = False) -> Path | None:
    """The newest JSON log ``<instance>_<YYYYMMDD>.log`` (or ``<instance>.console.log``).

    Copies loguru is rotating (``<instance>_<date>.<timestamp>.log``) are only used when no
    daily file exists.
    """
    if console_log:
        path = logs_dir / f"{instance}.console.log"
        return path if path.exists() else None
    daily = re.compile(rf"{re.escape(instance)}_\d{{8}}\.log")
    found = [(m, p) for p in logs_dir.glob(f"{instance}_*.log") if (m := _mtime(p)) is not None]
    preferred = [(m, p) for m, p in found if daily.fullmatch(p.name)] or found
    if not preferred:
        return None
    return max(preferred, key=lambda mp: (mp[0], mp[1].name))[1]


def _tail(path: Path, n: int, block: int = 65_536) -> tuple[list[str], int]:
    """The last ``n`` lines of a UTF-8 text file and the file size they were read at."""
    with open_shared(path) as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        if n <= 0:
            return [], end
        data = b""
        pos = end
        while pos > 0 and data.count(b"\n") <= n:
            step = min(block, pos)
            pos -= step
            f.seek(pos)
            data = f.read(step) + data
    lines = data.decode("utf-8", errors="replace").splitlines()
    return lines[-n:], end


def tail_lines(path: Path, n: int, block: int = 65_536) -> list[str]:
    """The last ``n`` lines of a UTF-8 text file (read backwards in blocks)."""
    return _tail(path, n, block)[0]


def _fmt_value(value: Any) -> str:
    if isinstance(value, dict | list):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return str(value)


def format_log_line(line: str) -> Text:
    """Pretty form of one JSON log line (``ts level event k=v ...``); other lines stay as they are."""
    from rich.text import Text

    try:
        doc = json.loads(line)
    except ValueError:
        return Text(line)
    if not isinstance(doc, dict) or "event" not in doc:
        return Text(line)
    ts = str(doc.get("ts") or "")
    level = str(doc.get("level") or "")
    out = Text()
    out.append(ts[11:23] if len(ts) >= 23 else ts, style="green")
    out.append(" ")
    out.append(f"{level:<8}", style=_LEVEL_STYLE.get(level, ""))
    out.append(str(doc.get("event")), style="bold")
    detail = doc.get("detail")
    if isinstance(detail, dict) and detail:
        out.append(" " + " ".join(f"{k}={_fmt_value(v)}" for k, v in detail.items()), style="dim")
    for key in ("trading_day", "corr_id"):
        if doc.get(key):
            out.append(f" {key}={doc[key]}", style="cyan")
    if doc.get("exception"):
        out.append(f"\n    {doc['exception']}", style="red")
    return out


def _print_line(console: Console, line: str, raw: bool) -> None:
    from rich.text import Text

    console.print(Text(line) if raw else format_log_line(line))


def _file_id(path: Path) -> tuple[int, int] | None:
    try:
        st = path.stat()
    except OSError:
        return None
    return st.st_dev, st.st_ino


class LogFollower:
    """``futrisk logs -f`` without getting in the way of the writer's rotation.

    The file is opened with :func:`open_shared` (Windows: delete sharing), so loguru can rename
    it on rotation and delete it after compression while it is followed; the open handle keeps
    the old file readable until its last lines are read. Each poll reads what was appended, then
    looks at what the path names now: another file (identity changed) means a rotation - the old
    handle is drained and closed and the new file is followed from its start; a smaller size
    means it was truncated in place (a console log rotated while a reader blocked the rename).
    A newer daily file of the instance (``<instance>_<YYYYMMDD>.log``) is picked up as well.
    """

    def __init__(self, path: Path, logs_dir: Path, instance: str, *, console_log: bool, offset: int | None = None) -> None:
        self.path = path
        self.logs_dir = logs_dir
        self.instance = instance
        self.console_log = console_log
        self.ident: tuple[int, int] | None = None
        self.offset = 0
        self._file: IO[bytes] | None = None
        self._buf = b""
        self._last_scan = time.monotonic()
        self.notes: list[str] = []
        self._open(path, offset)

    def _open(self, path: Path, offset: int | None) -> bool:
        self.path = path
        try:
            f = open_shared(path)
        except OSError:
            return False
        st = os.fstat(f.fileno())
        self._file = f
        self.ident = (st.st_dev, st.st_ino)
        self.offset = st.st_size if offset is None else offset
        return True

    def close(self) -> None:
        f, self._file = self._file, None
        if f is not None:
            with contextlib.suppress(OSError):
                f.close()

    def _drain(self) -> bytes:
        """What was appended to the open file since the last read."""
        f = self._file
        if f is None:
            return b""
        try:
            size = os.fstat(f.fileno()).st_size
            if size < self.offset:  # truncated in place
                self.notes.append(f"--- {self.path.name} truncated ---")
                self.offset = 0
            if size <= self.offset:
                return b""
            f.seek(self.offset)
            chunk = f.read()
        except OSError:
            return b""
        self.offset += len(chunk)
        return chunk

    def _switch(self, path: Path, note: str) -> bytes:
        """The rest of the current file, then ``path`` from its start."""
        data = self._drain()
        self.close()
        self.notes.append(note)
        if self._open(path, 0):
            data += self._drain()
        return data

    def poll(self) -> list[str]:
        """Complete lines appended since the last poll."""
        data = b""
        if self._file is None:
            if self._open(self.path, 0):
                data += self._drain()
        else:
            data += self._drain()
            current = _file_id(self.path)
            if current is not None and current != self.ident:  # rotated: another file has our name now
                data += self._switch(self.path, f"--- {self.path.name} rotated ---")
        now = time.monotonic()
        if not self.console_log and now - self._last_scan >= 1.0:
            self._last_scan = now
            newest = find_log_file(self.logs_dir, self.instance)
            if newest is not None and newest.name > self.path.name:  # the next day's file
                data += self._switch(newest, f"--- {newest.name} ---")
        if not data:
            return []
        self._buf += data
        *complete, self._buf = self._buf.split(b"\n")
        return [line.decode("utf-8", errors="replace").rstrip("\r") for line in complete]


def _follow(
    console: Console,
    path: Path,
    logs_dir: Path,
    instance: str,
    *,
    raw: bool,
    console_log: bool,
    offset: int | None = None,
    poll_s: float = FOLLOW_POLL_S,
    stop: Callable[[], bool] | None = None,
) -> None:
    follower = LogFollower(path, logs_dir, instance, console_log=console_log, offset=offset)
    try:
        while stop is None or not stop():
            lines = follower.poll()
            for note in follower.notes:
                console.print(f"[dim]{note}[/]")
            follower.notes.clear()
            for line in lines:
                _print_line(console, line, raw)
            if not lines:
                time.sleep(poll_s)
    finally:
        follower.close()


def logs(
    instance: str = typer.Argument(..., help="Instance, e.g. md-gateway, risk-engine-0, supervisor, sim-exchange"),
    follow: bool = typer.Option(False, "--follow", "-f", help="Keep printing new lines"),
    lines: int = typer.Option(50, "--lines", "-n", help="Number of trailing lines to print"),
    home: str | None = HomeOpt,
    console_log: bool = typer.Option(False, "--console", help="Show <instance>.console.log (captured stdout/stderr)"),
    raw: bool = typer.Option(False, "--raw", help="Print the JSON lines unchanged"),
) -> None:
    """Print a service's log from ``<home>/logs`` (local files, no bus)."""
    if not _INSTANCE_RE.match(instance):
        raise typer.BadParameter(f"invalid instance name {instance!r}")
    logs_dir = HomeLayout(resolve_home(home)).logs_dir
    console = _console()
    path = find_log_file(logs_dir, instance, console_log=console_log)
    if path is None:
        console.print(f"[red]no log file for {instance} in {logs_dir}[/]")
        raise typer.Exit(1)
    tail, end = _tail(path, lines)
    for line in tail:
        _print_line(console, line, raw or console_log)
    if follow:
        try:
            _follow(console, path, logs_dir, instance, raw=raw or console_log, console_log=console_log, offset=end)
        except KeyboardInterrupt:
            return


# ====================================================================== demo
@dataclass
class DemoReport:
    """Step results of ``futrisk demo``."""

    console: Console
    steps: list[tuple[str, bool, str]] = field(default_factory=list)

    def step(self, name: str, ok: bool, message: str) -> bool:
        self.steps.append((name, ok, message))
        mark = "[green]ok[/]    " if ok else "[red]FAILED[/]"
        self.console.print(f"{mark} {name:<11} {message}")
        return ok

    def note(self, message: str) -> None:
        self.console.print(f"[yellow]note[/]   {message}")

    @property
    def ok(self) -> bool:
        return all(ok for _, ok, _ in self.steps)


def up_argv(
    layout: HomeLayout,
    *,
    start_at: str | None,
    parent_pid: int | None,
    svc_env: dict[str, dict[str, str]] | None = None,
) -> list[str]:
    """Command line of the ``futrisk up`` child started by ``futrisk demo``.

    Per-instance environment overrides travel explicitly as ``--svc-env`` (the CLI's own
    environment is not changed), so the supervisor applies them to that instance only.
    """
    base = [sys.executable, "up"] if is_frozen() else [sys.executable, "-m", "futrisk.main", "up"]
    argv = [*base, "--home", str(layout.home), "--headless"]
    if start_at:
        argv += ["--start-at", start_at]
    if parent_pid:
        argv += ["--parent-pid", str(parent_pid)]
    return argv + svc_env_args(svc_env)


def _spawn_up(
    layout: HomeLayout,
    *,
    start_at: str | None,
    parent_pid: int | None,
    svc_env: dict[str, dict[str, str]] | None = None,
) -> subprocess.Popen[bytes]:
    from futrisk.services.supervisor.process import rotate_console_log

    layout.logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = layout.logs_dir / SUPERVISOR_CONSOLE_LOG
    rotate_console_log(log_path)
    env = dict(os.environ, PYTHONUTF8="1", PYTHONIOENCODING="utf-8", PYTHONUNBUFFERED="1")
    env.pop("TERM", None)  # loguru would colour the redirected console output (Windows)
    kwargs: dict[str, Any] = {"stdin": subprocess.DEVNULL, "stderr": subprocess.STDOUT, "env": env, "cwd": str(layout.home)}
    if sys.platform == "win32":
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    argv = up_argv(layout, start_at=start_at, parent_pid=parent_pid, svc_env=svc_env)
    with open(log_path, "ab") as fh:
        return subprocess.Popen(argv, stdout=fh, **kwargs)


def _wait_supervisor(sc: SupervisorClient, child: subprocess.Popen[bytes] | None, timeout_s: float) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if child is not None and child.poll() is not None:
            return False
        if sc.ping(timeout_ms=500) is not None:
            return True
        time.sleep(0.3)
    return False


def _find(lst: ServiceList | None, instance: str) -> ServiceInfo | None:
    for info in lst.items if lst is not None else []:
        if info.instance == instance:
            return info
    return None


def _wait_api_ready(
    api: str,
    sc: SupervisorClient,
    child: subprocess.Popen[bytes] | None,
    deadline: float,
    console: Console,
    home_id: str,
) -> tuple[bool, str]:
    """Wait until the dashboard URL is served by **this home's** api-gateway (A-HIGH-1).

    * ``svc.ps`` must answer (a timeout proves nothing) and report the api-gateway with
      heartbeat state ``RUNNING`` and the same pid on two polls in a row;
    * ``GET /api/v1/health`` must answer 200 with this home's ``home_id``; a gateway of another
      home is refused at once. A ``/health`` without ``home_id`` (an older api-gateway) must at
      least report the pid ``svc.ps`` shows.

    Nothing is sent with the API key before this returns True.
    """
    import httpx

    url = f"{api}{API_PREFIX}/health"
    next_note = time.monotonic() + 5.0
    prev_pid: int | None = None
    waiting = f"{url} not ready"
    with httpx.Client(trust_env=False, timeout=2.0) as client:
        while True:
            code, data = _get_json(client, url)
            hid = data.get("home_id") if code == 200 else None
            if isinstance(hid, str) and hid and hid != home_id:
                return False, f"{url} is served by another home's api-gateway ({hid}); use that home or change ports"
            lst = sc.ps(timeout_ms=1_000)
            info = _find(lst, API_GATEWAY)
            if lst is None:
                prev_pid = None
                waiting = "the supervisor did not answer svc.ps"
            elif info is None:  # not managed by this supervisor: /health must name this home
                prev_pid = None
                if code == 200 and hid == home_id:
                    return True, f"{url} (api-gateway of this home, not managed by the supervisor)"
                waiting = f"{url}: no api-gateway of this home (not managed by the supervisor)"
            elif str(info.state) != "RUNNING" or str(info.heartbeat_state) != "RUNNING" or not info.pid:
                prev_pid = None
                shown = str(info.state) if str(info.state) != "RUNNING" else f"{info.heartbeat_state or 'silent'}"
                waiting = f"{url}: this supervisor's api-gateway is {shown}"
            else:
                if prev_pid == info.pid and code == 200:
                    if hid == home_id:
                        return True, url
                    hpid = data.get("pid")
                    if hid is None and hpid == info.pid:
                        return True, f"{url} (identity by pid {hpid}; /health reports no home_id)"
                    waiting = (
                        f"{url} answers from pid {hpid if hpid is not None else '?'} without a home id; "
                        f"this home's api-gateway is pid {info.pid}: another futrisk may be using the same ports"
                    )
                elif code != 200:
                    waiting = f"{url} not ready ({'unreachable' if code is None else f'HTTP {code}'})"
                prev_pid = info.pid
            if child is not None and child.poll() is not None:
                return False, f"the supervisor exited (code {child.returncode}); see logs/{SUPERVISOR_CONSOLE_LOG}"
            now = time.monotonic()
            if now >= deadline:
                return False, waiting
            if now >= next_note:
                next_note = now + 5.0
                console.print(f"[dim]       waiting for the api-gateway: {waiting}[/]")
                if lst is not None:
                    console.print(f"[dim]       {_summary(lst.items)}[/]")
            time.sleep(max(0.0, min(API_POLL_S, deadline - now)))


def read_api_key(layout: HomeLayout, wait_s: float = 0.0) -> str | None:
    """``FUTRISK_API_KEY`` or ``<home>/secrets/bootstrap_admin.key`` (waits up to ``wait_s``)."""
    env = os.environ.get(API_KEY_ENV, "").strip()
    if env:
        return env
    path = layout.secrets_dir / BOOTSTRAP_KEY_FILE
    deadline = time.monotonic() + wait_s
    while True:
        try:
            key = path.read_text(encoding="utf-8").strip()
        except OSError:
            key = ""
        if key:
            return key
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.3)


def _body(resp: httpx.Response) -> dict[str, Any]:
    try:
        doc = resp.json()
    except ValueError:
        return {}
    return doc if isinstance(doc, dict) else {}


def _api(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    key: str,
    body: Any = None,
    idempotency_key: str | None = None,
    retry_until: float | None = None,
) -> httpx.Response | None:
    """One REST call with ``X-API-Key`` (+ ``Idempotency-Key``); retries 429/5xx/unreachable until ``retry_until``."""
    import httpx

    headers = {"X-API-Key": key}
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
    delay = 0.5
    while True:
        try:
            resp: httpx.Response | None = client.request(method, url, headers=headers, json=body)
        except httpx.HTTPError:
            resp = None
        if resp is not None and resp.status_code not in _RETRY_HTTP:
            return resp
        if retry_until is None or time.monotonic() + delay > retry_until:
            return resp
        time.sleep(delay)
        delay = min(delay * 2, 4.0)


def seed_request_body(cfg: FullConfig, trading_day: str) -> dict[str, Any]:
    """``POST /settlement/seed-sod`` body: every account of ``accounts.yaml`` with its initial capital
    as ``pre_balance``; ``pre_settle`` from the sim price engine (02 §6.5 ``SeedSodRequest``).

    ``initial_positions`` of an account become its yesterday lots: opened on ``open_td`` (default:
    the trading day before ``trading_day``) at ``open_price``, with ``basis_price`` = the instrument's
    ``pre_settle`` (a start-of-day lot is valued from yesterday's settlement, 02 §7.8). An instrument
    the sim does not price uses the lot's ``open_price`` as its ``pre_settle``.
    """
    from futrisk.common.clock import BJ, bj_to_ns, parse_td, td_str
    from futrisk.common.decimals import dec_str

    pre_settle = {
        inst: dec_str(sim.pre_settle)
        for inst, sim in sorted(cfg.sim.price_engine.instruments.items())
        if cfg.products.instrument(inst) is not None
    }
    prev_td = td_str(cfg.calendar.prev_trading_day(parse_td(trading_day)))
    accounts = []
    for account_id, spec in sorted(cfg.accounts.accounts.items()):
        capital = dec_str(spec.initial_capital)
        lots: list[dict[str, Any]] = []
        for n, lot in enumerate(spec.initial_positions, start=1):
            open_td = lot.open_td or prev_td
            basis = pre_settle.setdefault(lot.instrument_id, dec_str(lot.open_price))
            open_dt = datetime.combine(parse_td(open_td), dtime(14, 59), tzinfo=BJ)
            lots.append({
                "instrument_id": lot.instrument_id,
                "direction": str(lot.direction),
                "lot_no": n,
                "open_td": open_td,
                "open_ts_ns": bj_to_ns(open_dt),
                "open_price": dec_str(lot.open_price),
                "qty": lot.qty,
                "basis_price": basis,
            })
        accounts.append(
            {
                "account_id": account_id,
                "pre_balance": capital,
                "initial_capital": capital,
                "cum_net_deposit": "0",
                "hwm_equity": capital,
                "lots": lots,
            }
        )
    return {
        "trading_day": trading_day,
        "accounts": accounts,
        "pre_settle": pre_settle,
        "reason": "futrisk demo: first-day books from accounts.yaml",
    }


def seed_idempotency_key(layout: HomeLayout, trading_day: str) -> str:
    """Deterministic key: a repeated seed of the same home and day is a duplicate, never a new version."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"futrisk:seed-sod:{layout.home.resolve().as_posix()}:{trading_day}"))


def _existing_books(client: httpx.Client, api: str, key: str, retry_until: float) -> tuple[str | None, str | None]:
    """``(what exists, None)`` / ``(None, None)`` when the books are empty / ``(None, error)``.

    Asks the api-gateway for account snapshots (``/accounts``: any account that is not
    ``missing``) and for settled days (``/settlement/days``).
    """
    resp = _api(client, "GET", f"{api}{API_PREFIX}/accounts", key=key, retry_until=retry_until)
    doc = _body(resp) if resp is not None else {}
    if resp is None or resp.status_code != 200 or not doc.get("ok", True) or not isinstance(doc.get("data"), list):
        return None, f"GET /accounts: {'unreachable' if resp is None else f'HTTP {resp.status_code}'}"
    held = [a.get("account_id") for a in doc["data"] if isinstance(a, dict) and a.get("missing") is False]
    if held:
        return f"account snapshots exist ({', '.join(str(a) for a in held[:5])})", None
    resp = _api(client, "GET", f"{api}{API_PREFIX}/settlement/days", key=key, retry_until=retry_until)
    doc = _body(resp) if resp is not None else {}
    data = doc.get("data")
    if resp is None or resp.status_code != 200 or not isinstance(data, dict):
        return None, f"GET /settlement/days: {'unreachable' if resp is None else f'HTTP {resp.status_code}'}"
    days = [d.get("trading_day") for d in data.get("items") or [] if isinstance(d, dict)]
    if days:
        return f"settlements exist ({', '.join(str(d) for d in days[:3])})", None
    if data.get("errors"):
        return None, f"GET /settlement/days: clearing-worker unavailable ({data['errors']})"
    return None, None


def seed_sod_once(
    client: httpx.Client,
    api: str,
    key: str,
    ctx: OpsContext,
    market_now_ns: int,
    retry_until: float,
) -> tuple[bool, str]:
    """Seed the first-day SOD unless this home already has books.

    Books exist when the marker file is there, any account has a snapshot, any trading day
    has an EOD run, or the settlement of today / the previous trading day exists. When that
    cannot be verified the step fails instead of seeding blindly.
    """
    from futrisk.common.clock import td_str

    marker = ctx.layout.db_dir / SEED_MARKER
    if marker.exists():
        return True, f"books already seeded ({marker.name})"
    cal = ctx.cfg.calendar
    try:
        td_date = cal.current_or_next_trading_day(market_now_ns)
    except LookupError as exc:
        return False, f"no trading day for the market clock: {exc}"
    td = td_str(td_date)
    found, error = _existing_books(client, api, key, retry_until)
    if error is not None:
        return False, f"cannot verify that this home has no books yet ({error}); not seeding"
    if found is not None:
        _write_marker(marker, {"trading_day": td, "source": found})
        return True, f"books already exist: {found}; not seeding"
    checks = [td]
    with contextlib.suppress(LookupError):
        checks.append(td_str(cal.prev_trading_day(td_date)))
    for check_td in checks:
        resp = _api(client, "GET", f"{api}{API_PREFIX}/settlement/{check_td}", key=key, retry_until=retry_until)
        doc = _body(resp) if resp is not None else {}
        if resp is not None and resp.status_code == 200 and doc.get("ok", True) and doc.get("data"):
            _write_marker(marker, {"trading_day": td, "source": f"settlement {check_td} exists"})
            return True, f"books already exist (settlement of {check_td}); not seeding"
    body = seed_request_body(ctx.cfg, td)
    resp = _api(
        client,
        "POST",
        f"{api}{API_PREFIX}/settlement/seed-sod",
        key=key,
        body=body,
        idempotency_key=seed_idempotency_key(ctx.layout, td),
        retry_until=retry_until,
    )
    if resp is None:
        return False, "api-gateway unreachable"
    doc = _body(resp)
    if resp.status_code == 200 and doc.get("ok"):
        _write_marker(marker, {"trading_day": td, "source": "seed-sod", "accounts": len(body["accounts"]), "reply": doc})
        dup = " (duplicate: already done)" if doc.get("duplicate") else ""
        return True, f"seeded the SOD of {td} for {len(body['accounts'])} accounts{dup}"
    return False, f"seed-sod failed: HTTP {resp.status_code} {doc.get('code', '')} {doc.get('message', '')}".rstrip()


def _write_marker(path: Path, doc: dict[str, Any]) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"written_at": time.strftime("%Y-%m-%dT%H:%M:%S"), **doc}
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    except OSError:
        pass


def start_scenario(client: httpx.Client, api: str, key: str, name: str, retry_until: float) -> tuple[bool, str]:
    resp = _api(
        client,
        "POST",
        f"{api}{API_PREFIX}/demo/scenario",
        key=key,
        body={"name": name},
        idempotency_key=str(uuid.uuid4()),
        retry_until=retry_until,
    )
    if resp is None:
        return False, "api-gateway unreachable"
    doc = _body(resp)
    if resp.status_code == 200 and doc.get("ok"):
        data = doc.get("data")
        run_id = data.get("run_id") if isinstance(data, dict) else None
        return True, f"scenario {name} started" + (f" (run {run_id})" if run_id else "")
    return False, f"scenario {name} failed: HTTP {resp.status_code} {doc.get('code', '')} {doc.get('message', '')}".rstrip()


@dataclass(frozen=True)
class BotsSwitch:
    """Result of :func:`switch_bots_mode`: ``ok`` None = strategy-sim is not managed here."""

    ok: bool | None
    message: str
    state: str | None = None

    @property
    def changed(self) -> bool:
        """The supervisor (re)started strategy-sim with the new environment."""
        return bool(self.ok) and self.state not in (None, "RUNNING")


def switch_bots_mode(sc: SupervisorClient, env: dict[str, str | None]) -> BotsSwitch:
    """``svc.start strategy-sim`` with ``env`` (``None`` unsets): the supervisor restarts it only
    when its environment changes."""
    from futrisk.messages import codes
    from futrisk.messages.system import ServiceInfo

    shown = " ".join(f"{k}={v}" if v is not None else f"{k} unset" for k, v in sorted(env.items()))
    reply = sc.request("svc.start", _svc_request(STRATEGY_SIM, dict(env)), timeout_ms=15_000, retries=1)
    if reply.ok:
        info = reply.open_as(ServiceInfo)
        state = str(info.state) if info is not None else "?"
        if state == "STOPPING":
            return BotsSwitch(True, f"{STRATEGY_SIM} restarting with {shown}", state)
        return BotsSwitch(True, f"{STRATEGY_SIM} runs with {shown} ({state})", state)
    if reply.code == codes.NOT_FOUND:
        return BotsSwitch(None, f"{STRATEGY_SIM} is not managed by this supervisor; its mode is unchanged")
    return BotsSwitch(False, f"svc.start {STRATEGY_SIM} ({shown}) failed: {reply.code} {reply.message or ''}".rstrip())


def _bots_env(mode: str) -> dict[str, str | None]:
    return {BOTS_MODE_ENV: SCENARIO_BOTS_MODE if mode == SCENARIO_BOTS_MODE else None}


def _note_bots_override(sc: SupervisorClient, report: DemoReport) -> None:
    info = _find(sc.ps(timeout_ms=1_500), STRATEGY_SIM)
    if info is not None and info.env_keys and BOTS_MODE_ENV in info.env_keys:
        report.note(
            f"{STRATEGY_SIM} runs with {BOTS_MODE_ENV} set (e.g. by an earlier `futrisk demo --scenario`); "
            f"`futrisk demo --bots {NORMAL_BOTS_MODE}` switches the bots back to their normal mode"
        )


def book_ratio(snapshot: Any, products: tuple[str, ...]) -> float | None:
    """Gross LONG notional in ``products`` / equity of an ``AccountSnapshot`` as JSON (02 §7.6).

    ``None`` while the account has no usable snapshot. Falls back to the positions'
    ``pct_equity`` when ``funds.equity`` is missing.
    """
    if not isinstance(snapshot, dict):
        return None
    positions = [p for p in snapshot.get("positions") or [] if isinstance(p, dict)]
    longs = [p for p in positions if p.get("direction") == "LONG" and p.get("product") in products]
    funds = snapshot.get("funds")
    equity = funds.get("equity") if isinstance(funds, dict) else None
    if isinstance(equity, int | float) and equity > 0:
        return sum(float(p.get("notional") or 0.0) for p in longs) / float(equity)
    if "positions" in snapshot:
        return sum(float(p.get("pct_equity") or 0.0) for p in longs)
    return None


def wait_for_book(
    client: httpx.Client,
    api: str,
    key: str,
    req: BookRequirement,
    *,
    timeout_s: float,
    console: Console,
) -> tuple[bool, str]:
    """Poll ``GET /api/v1/accounts/{account}`` until the book reaches ``req.min_ratio`` × equity."""
    url = f"{api}{API_PREFIX}/accounts/{req.account_id}"
    start = time.monotonic()
    deadline = start + max(0.0, timeout_s)
    next_note = start + BOOK_NOTE_S
    ratio: float | None = None
    status = "no answer"
    while True:
        resp = _api(client, "GET", url, key=key)
        if resp is not None:
            doc = _body(resp)
            if resp.status_code in (401, 403):
                return False, f"GET {url}: HTTP {resp.status_code} {doc.get('code', '')}".rstrip()
            if resp.status_code == 200 and doc.get("ok", True):
                got = book_ratio(doc.get("data"), req.products)
                status = "no snapshot yet" if got is None else f"{got:.2f}x equity"
                ratio = got if got is not None else ratio
                if got is not None and got >= req.min_ratio:
                    waited = time.monotonic() - start
                    return True, f"{req.describe()} at {got:.2f}x equity (need {req.min_ratio:.2f}x; waited {waited:.0f}s)"
            else:
                status = f"HTTP {resp.status_code} {doc.get('code', '')}".rstrip()
        now = time.monotonic()
        if now >= deadline:
            reached = f"{ratio:.2f}x" if ratio is not None else "nothing"
            return False, (
                f"{req.describe()} reached {reached} equity after {timeout_s:.0f}s (need {req.min_ratio:.2f}x; last: {status}); "
                f"is {STRATEGY_SIM} running in {SCENARIO_BOTS_MODE} mode and is the market open?"
            )
        if now >= next_note:
            next_note = now + BOOK_NOTE_S
            console.print(
                f"[dim]       waiting for {req.describe()}: {status} (need {req.min_ratio:.2f}x), {deadline - now:.0f}s left[/]"
            )
        time.sleep(max(0.0, min(BOOK_POLL_S, deadline - now)))


def _run_scenario(
    sc: SupervisorClient,
    client: httpx.Client,
    api: str,
    key: str,
    scenario: str,
    *,
    bots: str | None,
    book_timeout: float,
    retry_until: float,
    report: DemoReport,
    console: Console,
) -> None:
    """Switch the bots to scenario mode just before the scenario, wait for its book, start it;
    when it cannot start, the bots go back to their previous mode."""
    switch: BotsSwitch | None = None
    if bots != NORMAL_BOTS_MODE:
        switch = switch_bots_mode(sc, _bots_env(SCENARIO_BOTS_MODE))
        if switch.ok is None:
            report.note(switch.message)
        elif not report.step("bots", switch.ok, switch.message):
            report.step("scenario", False, f"{scenario} not started: the bots could not be switched to {SCENARIO_BOTS_MODE} mode")
            return

    def revert() -> None:
        if switch is not None and switch.changed:
            back = switch_bots_mode(sc, _bots_env(NORMAL_BOTS_MODE))
            if back.ok:
                report.note(f"{STRATEGY_SIM} switched back: {back.message}")
            else:
                report.note(f"could not switch {STRATEGY_SIM} back ({back.message}); use `futrisk demo --bots {NORMAL_BOTS_MODE}`")

    requirement = SCENARIO_BOOKS.get(scenario)
    if requirement is not None and not report.step(
        "book", *wait_for_book(client, api, key, requirement, timeout_s=book_timeout, console=console)
    ):
        report.step("scenario", False, f"{scenario} not started: it is calibrated for {requirement.describe()}")
        revert()
        return
    ok, msg = start_scenario(client, api, key, scenario, retry_until)
    if not report.step("scenario", ok, msg):
        revert()


def _stop_started_system(ctx: OpsContext, child: subprocess.Popen[bytes], console: Console) -> None:
    """Ctrl-C of an interactive demo: ``svc.stop *`` (this home's supervisor only), then
    CTRL_BREAK / kill of the child as a last resort."""
    ok = False
    with SupervisorClient(ctx.cfg) as sc:
        pong = sc.ping(timeout_ms=1_000)
        problem = supervisor_home_problem(ctx, pong, None) if pong is not None else "it does not answer"
        if problem is None:
            reply = sc.request("svc.stop", _svc_request("*"), timeout_ms=5_000, retries=1)
            ok = reply.ok
            if not ok:
                console.print(f"[yellow]svc.stop * failed ({reply.code}); signalling the supervisor[/]")
        else:
            console.print(f"[yellow]not sending svc.stop: {problem}; signalling the supervisor[/]")
    try:
        child.wait(timeout=180 if ok else 1)
        return
    except subprocess.TimeoutExpired:
        pass
    try:
        if sys.platform == "win32":
            import signal

            os.kill(child.pid, signal.CTRL_BREAK_EVENT)
        else:
            child.terminate()
        child.wait(timeout=60)
    except (OSError, subprocess.TimeoutExpired):
        child.kill()


def _iter_attached(child: subprocess.Popen[bytes]) -> Iterator[None]:
    while child.poll() is None:
        yield
        time.sleep(0.5)


def run_demo(
    *,
    home: str | None,
    scenario: str | None,
    headless: bool,
    start_at: str | None,
    timeout: float,
    book_timeout: float = 180.0,
    bots: str | None = None,
    console: Console | None = None,
) -> int:
    """``futrisk demo``: see the module docstring; returns the exit code.

    Order: app.mode (scenarios and ``--bots`` are demo-only) -> supervisor (started here or
    already running; must belong to this home) -> this home's api-gateway -> API key ->
    first-day books -> scenario: bots to scenario mode, the book it needs (``book_timeout``
    seconds at most), ``POST /demo/scenario``; the bots go back when it does not start.
    """
    import webbrowser

    import httpx

    if bots is not None and bots not in BOTS_MODES:
        raise typer.BadParameter(f"expected one of {', '.join(BOTS_MODES)}, got {bots!r}", param_hint="--bots")
    ctx = load_context(home)
    console = console or _console()
    report = DemoReport(console)
    demo_mode = ctx.cfg.app.mode == "demo"
    if not demo_mode and (scenario or bots):
        what = f"scenario {scenario}" if scenario else f"--bots {bots}"
        report.step("mode", False, f"{what}: only available in demo mode (app.mode={ctx.cfg.app.mode}); nothing was changed")
        return 1
    home_id = ctx.home_id
    deadline = time.monotonic() + timeout
    child: subprocess.Popen[bytes] | None = None

    def done(code: int) -> int:
        return code if headless or child is None else _attach(ctx, child, console, report)

    with SupervisorClient(ctx.cfg) as sc:
        pong = sc.ping(timeout_ms=800)
        if pong is None:
            child = _spawn_up(ctx.layout, start_at=start_at, parent_pid=None if headless else os.getpid())
            started = _wait_supervisor(sc, child, min(60.0, timeout))
            if not report.step("supervisor", started, f"futrisk up (pid {child.pid}), console log logs/{SUPERVISOR_CONSOLE_LOG}"):
                return 1
            problem = supervisor_home_problem(ctx, sc.ping(timeout_ms=1_000), sc.ps(timeout_ms=1_500))
            if problem is not None:
                report.step("identity", False, problem)
                return 1
        else:
            problem = supervisor_home_problem(ctx, pong, sc.ps(timeout_ms=1_500))
            if problem is not None:
                report.step("supervisor", False, problem)
                return 1
            report.step("supervisor", True, "already running")
            if start_at:
                report.note("--start-at ignored: the market clock is already running (jump with POST /api/v1/demo/clock)")
            if bots is None and not scenario:
                _note_bots_override(sc, report)
        if bots is not None and not scenario:
            switch = switch_bots_mode(sc, _bots_env(bots))
            if switch.ok is None:
                report.note(switch.message)
            else:
                report.step("bots", switch.ok, switch.message)
        api = ctx.api_url
        if api is None:
            report.step("dashboard", False, "services.yaml has no http endpoint for api-gateway")
            return 1
        if not report.step("dashboard", *_wait_api_ready(api, sc, child, deadline, console, home_id)):
            return done(1)
        key = read_api_key(ctx.layout, wait_s=min(30.0, max(1.0, deadline - time.monotonic())))
        if not report.step(
            "api key",
            key is not None,
            f"{ctx.layout.secrets_dir / BOOTSTRAP_KEY_FILE}" if key else f"missing {BOOTSTRAP_KEY_FILE} (or set {API_KEY_ENV})",
        ):
            return done(1)
        assert key is not None
        clk = sc.clock()
        market_now = time.time_ns() + (clk.offset_ns if clk is not None else 0)
        retry_until = max(deadline, time.monotonic() + 10.0)
        with httpx.Client(trust_env=False, timeout=35.0) as client:
            if demo_mode and ctx.cfg.app.demo.auto_seed:
                ok, msg = seed_sod_once(client, api, key, ctx, market_now, retry_until)
                report.step("books", ok, msg)
            elif not demo_mode:
                report.note("app.mode is prod: no automatic seeding")
            if scenario:
                _run_scenario(
                    sc,
                    client,
                    api,
                    key,
                    scenario,
                    bots=bots,
                    book_timeout=book_timeout,
                    retry_until=retry_until,
                    report=report,
                    console=console,
                )
    if not headless:
        opened = False
        try:
            opened = webbrowser.open(api)
        except Exception:
            opened = False
        report.step("browser", True, f"opened {api}" if opened else f"open {api} in a browser")
    if headless or child is None:
        return 0 if report.ok else 1
    return _attach(ctx, child, console, report)


def _attach(ctx: OpsContext, child: subprocess.Popen[bytes], console: Console, report: DemoReport) -> int:
    console.print(f"futrisk demo is running ({ctx.api_url}); press Ctrl-C to stop every service")
    try:
        for _ in _iter_attached(child):
            pass
    except KeyboardInterrupt:
        console.print("stopping every service...")
        _stop_started_system(ctx, child, console)
        return 0 if report.ok else 1
    console.print(f"the supervisor exited with code {child.returncode}")
    return int(child.returncode or 0) or (0 if report.ok else 1)


def demo(
    scenario: str | None = typer.Option(None, "--scenario", help="Scenario to start (config/scenarios), e.g. breaker"),
    headless: bool = typer.Option(False, "--headless", help="No browser; exit when set up (non-zero if a step failed)"),
    home: str | None = HomeOpt,
    start_at: str | None = typer.Option(None, "--start-at", help="Initial market time for a first start (ISO, Beijing)"),
    timeout: float = typer.Option(180.0, "--timeout", help="Seconds to wait for the dashboard"),
    book_timeout: float = typer.Option(
        180.0, "--book-timeout", help="Seconds to wait for the book a scenario needs (breaker: ACC3's long rb/hc/i)"
    ),
    bots: str | None = typer.Option(
        None,
        "--bots",
        help="Mode of the strategy-sim bots: normal (remove the override) or scenario; "
        "default: scenario while a --scenario starts, else unchanged",
    ),
) -> None:
    """One-click demo: start everything, seed the books once, open the dashboard.

    ``--scenario`` (demo mode) switches strategy-sim to scenario mode (``FUTRISK_BOTS_MODE=scenario``)
    right before the scenario and, for ``breaker``, waits until ACC3's long rb/hc/i book reaches
    0.7x equity; the bots go back when the scenario cannot start.
    """
    raise typer.Exit(
        run_demo(
            home=home,
            scenario=scenario,
            headless=headless,
            start_at=start_at,
            timeout=timeout,
            book_timeout=book_timeout,
            bots=bots,
        )
    )


# ====================================================================== registration
def register(app: typer.Typer) -> None:
    """Add the top-level operations commands to the ``futrisk`` CLI (``futrisk.main``)."""
    app.command("up")(up)
    app.command("down")(down)
    app.command("ps")(ps)
    app.command("logs")(logs)
    app.command("kill")(kill)
    app.command("status")(status)
    app.command("demo")(demo)


__all__ = [
    "API_KEY_ENV",
    "API_PREFIX",
    "BOOTSTRAP_KEY_FILE",
    "BotsSwitch",
    "DemoReport",
    "LogFollower",
    "OpsContext",
    "SupervisorClient",
    "check_svc_env",
    "find_log_file",
    "foreign_home_message",
    "format_log_line",
    "load_context",
    "open_shared",
    "read_api_key",
    "register",
    "render_ps",
    "run_demo",
    "seed_idempotency_key",
    "seed_request_body",
    "seed_sod_once",
    "supervisor_home_problem",
    "tail_lines",
    "up_argv",
]
