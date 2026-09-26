"""ProcessManager: instance specs, command lines, spawning and terminating children (01 §5.3.6, §6.8, §7.1).

Command line of an instance (see ``futrisk.cli.svc_cmd``)::

    dev:    [python, -m, futrisk.main, svc, <service>, --home, H, --parent-pid, P, (--shard, k), (--clock-offset-ns=N)]
    frozen: [futrisk.exe, svc, <service>, ...]

* ``risk-engine-k`` runs service ``risk-engine`` with ``--shard k``;
* ``sim-exchange`` gets the current market-clock offset (02 §9.5) and its version in
  ``FUTRISK_CLOCK_VERSION``;
* Windows: ``CREATE_NEW_PROCESS_GROUP``, so ``CTRL_BREAK_EVENT`` can target one child and
  the console's Ctrl-C reaches only the supervisor; POSIX: a new session (SIGTERM);
* stdout/stderr are piped to a :class:`ConsolePump` thread that writes
  ``<home>/logs/<instance>.console.log`` with ANSI escapes stripped and rotates it by size
  while the child runs (``.1`` kept; truncated instead when a reader blocks the rename);
  the environment adds ``PYTHONUTF8=1``, ``PYTHONIOENCODING=utf-8`` and
  ``PYTHONUNBUFFERED=1`` and drops ``TERM`` (loguru colours redirected output on Windows
  when it is set);
* per-instance environment overrides are limited to :data:`ENV_ALLOW` (strategy-sim:
  ``FUTRISK_BOTS_MODE``, ``FUTRISK_BOTS_SEED``, ``FUTRISK_BOTS_ACCOUNTS``), never a name
  that holds a secret in the configuration (``api_keys[].key_env``, ``webhook_url_env``);
* before a spawn the instance's listening ports (PUB/ROUTER, HTTP, sim-exchange feeds,
  broker, control) are checked: a port another process listens on is reported as
  ``port N in use by pid X (name)`` instead of starting a process that would crash;
* children exit on their own when the supervisor dies (``--parent-pid``); descendants seen
  while a child ran are reaped after it ended (e.g. process-pool workers).

Liveness kinds (02 §3, §4.8): ``bus`` instances heartbeat on the bus; ``sim-exchange`` is
checked by process liveness plus ``GET /v1/health`` on the Feed B port (``http``);
``strategy-sim`` by process liveness only (``process``).

On Windows a development venv's ``python.exe`` is a launcher that runs the real
interpreter as a child inside a kill-on-close job, so heartbeats carry the child's pid:
:meth:`ChildProcess.is_own_pid` accepts the spawned pid and its descendants only.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import signal
import socket
import subprocess
import sys
import threading
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import IO, Any
from urllib.parse import urlsplit

import psutil
from loguru import logger

from futrisk.bus.endpoints import service_of
from futrisk.common.clock import Clock, SystemClock
from futrisk.common.config import FullConfig
from futrisk.common.paths import is_frozen

KIND_BUS = "bus"
KIND_HTTP = "http"
KIND_PROCESS = "process"
SIM_EXCHANGE = "sim-exchange"
STRATEGY_SIM = "strategy-sim"
SUPERVISOR = "supervisor"
CLOCK_VERSION_ENV = "FUTRISK_CLOCK_VERSION"
CONSOLE_LOG_MAX_BYTES = 10 * 1024 * 1024
MAX_ENV_VALUE = 1024
_SECRET_HINTS = ("KEY", "SECRET", "TOKEN", "PASSWORD")
_DROPPED_ENV = ("TERM", "PYCHARM_HOSTED")  # loguru colours redirected stderr when these are set (Windows)

# Per-service environment overrides the supervisor accepts (svc.start/svc.restart ``env``,
# ``futrisk up --svc-env``); services not listed accept none.
ENV_ALLOW: Mapping[str, frozenset[str]] = {
    STRATEGY_SIM: frozenset({"FUTRISK_BOTS_MODE", "FUTRISK_BOTS_SEED", "FUTRISK_BOTS_ACCOUNTS"}),
}


def secret_env_names(cfg: FullConfig) -> frozenset[str]:
    """Environment names that carry secrets in the configuration (never overridable)."""
    names = {k.key_env for k in cfg.app.api_keys}
    if cfg.app.notify.webhook_url_env:
        names.add(cfg.app.notify.webhook_url_env)
    return frozenset(n for n in names if n)


def validate_env(
    service: str,
    env: Mapping[str, str | None],
    *,
    forbidden: Collection[str] = (),
) -> dict[str, str | None]:
    """Checked copy of environment overrides for ``service`` (``None`` unsets); raises ``ValueError``."""
    allowed = ENV_ALLOW.get(service, frozenset())
    out: dict[str, str | None] = {}
    for name, value in env.items():
        if not isinstance(name, str) or name not in allowed:
            if not allowed:
                raise ValueError(f"{service} accepts no environment overrides (got {name!r})")
            raise ValueError(f"{service} accepts only {', '.join(sorted(allowed))} as environment overrides, not {name!r}")
        if name in forbidden:
            raise ValueError(f"{name} holds a secret in the configuration and cannot be overridden")
        if value is not None and (not isinstance(value, str) or len(value) > MAX_ENV_VALUE or "\x00" in value):
            raise ValueError(f"environment value of {name} must be a string of at most {MAX_ENV_VALUE} characters")
        out[name] = value
    return out


def apply_env(current: Mapping[str, str], changes: Mapping[str, str | None]) -> dict[str, str]:
    """``current`` with ``changes`` applied (a ``None`` value removes the name)."""
    out = dict(current)
    for name, value in changes.items():
        if value is None:
            out.pop(name, None)
        else:
            out[name] = value
    return out


def loggable_env(env: Mapping[str, str]) -> dict[str, str]:
    """``env`` with the values of secret-looking names masked (logs, console-log headers)."""
    return {k: ("***" if any(h in k.upper() for h in _SECRET_HINTS) else v) for k, v in env.items()}


# ------------------------------------------------------------------ specs
@dataclass(frozen=True)
class InstanceSpec:
    """One managed instance: bus service, or external component (``http`` / ``process``).

    ``ports``: the ``(host, port)`` pairs the instance listens on (checked before a spawn).
    """

    instance: str
    service: str
    kind: str = KIND_BUS
    shard: int | None = None
    health_url: str | None = None
    ports: tuple[tuple[str, int], ...] = ()

    @property
    def is_bus(self) -> bool:
        return self.kind == KIND_BUS


def sim_health_url(cfg: FullConfig) -> str:
    """sim-exchange ``GET /v1/health`` on the Feed B HTTP port (02 §9.2)."""
    fb = cfg.sim.feed_b
    return f"http://{fb.host}:{fb.port}/v1/health"


def endpoint_port(address: str | None) -> tuple[str, int] | None:
    """``(host, port)`` of ``tcp://host:port`` / ``http://host:port/...`` (None when there is no port)."""
    if not address:
        return None
    try:
        parts = urlsplit(address)
        port = parts.port
    except ValueError:
        return None
    if port is None or port == 0:
        return None
    host = parts.hostname or "0.0.0.0"
    return ("0.0.0.0" if host == "*" else host), int(port)


def _instance_ports(cfg: FullConfig, inst: str, service: str) -> tuple[tuple[str, int], ...]:
    found: list[tuple[str, int] | None] = []
    if service == SIM_EXCHANGE:
        s = cfg.sim
        found += [(s.feed_a.host, s.feed_a.port), (s.feed_b.host, s.feed_b.port), endpoint_port(s.broker.endpoint)]
        if cfg.app.mode == "demo":  # the control API is only served in demo mode (02 §9.5)
            found.append((s.control.host, s.control.port))
    else:
        ic = cfg.services.instances.get(inst)
        if ic is not None:
            found += [endpoint_port(ic.pub), endpoint_port(ic.router), endpoint_port(ic.http)]
    out: list[tuple[str, int]] = []
    for item in found:
        if item is not None and item[1] and item not in out:
            out.append(item)
    return tuple(out)


def build_specs(cfg: FullConfig) -> list[InstanceSpec]:
    """Managed instances in ``supervisor.startup_order`` (duplicates and the supervisor skipped).

    Instances listed in ``supervisor.external`` are external components; any other instance
    must be configured in ``services.yaml`` ``instances`` with a PUB endpoint (heartbeats).
    """
    sup = cfg.services.supervisor
    external = set(sup.external)
    specs: list[InstanceSpec] = []
    seen: set[str] = set()
    for inst in sup.startup_order:
        if inst in seen or inst == SUPERVISOR:
            continue
        seen.add(inst)
        service = service_of(inst)
        ports = _instance_ports(cfg, inst, service)
        if inst in external:
            if service == SIM_EXCHANGE:
                specs.append(InstanceSpec(inst, service, KIND_HTTP, health_url=sim_health_url(cfg), ports=ports))
            else:
                specs.append(InstanceSpec(inst, service, KIND_PROCESS, ports=ports))
            continue
        ic = cfg.services.instances.get(inst)
        if ic is None or not ic.pub:
            logger.error("supervisor.unknown_instance", svc=inst, detail="not configured in services.yaml instances")
            continue
        shard = ic.shard
        if shard is None and service != inst:
            shard = int(inst.rsplit("-", 1)[1])
        specs.append(InstanceSpec(inst, service, KIND_BUS, shard=shard, ports=ports))
    return specs


def service_argv(
    spec: InstanceSpec,
    *,
    home: Path | str,
    parent_pid: int,
    clock_offset_ns: int | None = None,
    frozen: bool | None = None,
    executable: str | None = None,
) -> list[str]:
    """``futrisk svc`` command line of ``spec`` (dev: ``python -m futrisk.main``; frozen: the exe)."""
    exe = executable or sys.executable
    frozen = is_frozen() if frozen is None else frozen
    argv = [exe, "svc", spec.service] if frozen else [exe, "-m", "futrisk.main", "svc", spec.service]
    argv += ["--home", str(home), "--parent-pid", str(parent_pid)]
    if spec.shard is not None:
        argv += ["--shard", str(spec.shard)]
    if spec.service == SIM_EXCHANGE and clock_offset_ns is not None:
        argv.append(f"--clock-offset-ns={int(clock_offset_ns)}")  # one token: offsets may be negative
    return argv


# ------------------------------------------------------------------ port pre-check
@dataclass(frozen=True)
class PortConflict:
    """A port an instance needs that another process already listens on."""

    host: str
    port: int
    pid: int | None
    name: str | None

    def describe(self) -> str:
        if self.pid is None:
            return f"port {self.port} in use"
        return f"port {self.port} in use by pid {self.pid} ({self.name or '?'})"


def _process_name(pid: int | None) -> str | None:
    if pid is None:
        return None
    try:
        return str(psutil.Process(pid).name())
    except psutil.Error:
        return None


def _overlaps(listen_ip: str, host: str) -> bool:
    wildcard = ("0.0.0.0", "::", "")
    if listen_ip in wildcard or host in wildcard:
        return True
    if host == "localhost":
        return listen_ip in ("127.0.0.1", "::1")
    return listen_ip == host


def _bind_test(host: str, port: int) -> bool:
    """True when ``host:port`` can be bound (fallback when the socket table is unavailable)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if sys.platform == "win32":
            s.setsockopt(socket.SOL_SOCKET, getattr(socket, "SO_EXCLUSIVEADDRUSE", -5), 1)
        else:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # ignore TIME_WAIT leftovers
        try:
            s.bind((host if host != "localhost" else "127.0.0.1", port))
        except OSError:
            return False
    return True


def port_conflicts(ports: Sequence[tuple[str, int]], *, ignore_pids: Collection[int] = ()) -> list[PortConflict]:
    """Listening sockets that already hold one of ``ports`` (connections in TIME_WAIT do not count)."""
    if not ports:
        return []
    try:
        conns = psutil.net_connections(kind="inet")
    except (psutil.Error, OSError):
        return [PortConflict(h, p, None, None) for h, p in ports if not _bind_test(h, p)]
    out: dict[int, PortConflict] = {}
    for c in conns:
        if c.status != psutil.CONN_LISTEN or not c.laddr or c.pid in ignore_pids:
            continue
        ip, lport = str(c.laddr[0]), int(c.laddr[1])
        for host, port in ports:
            if port == lport and port not in out and _overlaps(ip, host):
                out[port] = PortConflict(host, port, c.pid, _process_name(c.pid))
    return [out[p] for _, p in ports if p in out]


# ------------------------------------------------------------------ console logs
def console_log_path(logs_dir: Path, instance: str) -> Path:
    return logs_dir / f"{instance}.console.log"


def rotate_console_log(path: Path, max_bytes: int = CONSOLE_LOG_MAX_BYTES) -> None:
    """Move a console log above ``max_bytes`` to ``<name>.1`` (one generation kept)."""
    try:
        if path.exists() and path.stat().st_size > max_bytes:
            os.replace(path, path.with_name(path.name + ".1"))
    except OSError as exc:
        logger.warning("svc.console_log_rotate_failed", path=str(path), error=str(exc))


_ANSI = re.compile(rb"\x1b\[[0-?]*[ -/]*[@-~]|\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)|\x1b[@-Z\\-_]")


def strip_ansi(data: bytes) -> bytes:
    """``data`` without terminal escape sequences (colours, cursor moves)."""
    return _ANSI.sub(b"", data)


class ConsolePump:
    """Copies a child's merged stdout/stderr into its console log (daemon thread).

    ANSI escapes are stripped. Before a write would take the file past ``max_bytes`` it is moved
    to ``<name>.1`` and a new one started - or, when a reader keeps it open without delete
    sharing (Windows), truncated in place - so the newest output always lands in the current
    file. Write errors never block the child: the pipe keeps being drained.
    """

    def __init__(self, stream: IO[bytes], path: Path, *, max_bytes: int = CONSOLE_LOG_MAX_BYTES, name: str = "") -> None:
        self._stream = stream
        self.path = path
        self._max = max(1024, max_bytes)
        self._file: IO[bytes] | None = None
        self.rotations = 0
        self.truncations = 0
        self.write_errors = 0
        self._thread = threading.Thread(target=self._run, name=f"console:{name or path.stem}", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout)

    @property
    def running(self) -> bool:
        return self._thread.is_alive()

    def _open(self) -> None:
        try:
            self._file = open(self.path, "ab")  # noqa: SIM115 - owned by the pump thread
        except OSError:
            self._file = None

    def _write(self, data: bytes) -> None:
        data = strip_ansi(data)
        if not data:
            return
        if self._file is None:
            self._open()
        f = self._file
        if f is not None:
            try:
                size = f.tell()  # append mode: the end of the file
            except OSError:
                size = 0
            if size > 0 and size + len(data) > self._max:
                self._rotate()
                f = self._file
        if f is None:
            self.write_errors += 1
            return
        try:
            f.write(data)
            f.flush()
        except OSError:
            self.write_errors += 1
            with contextlib.suppress(OSError):
                f.close()
            self._file = None

    def _rotate(self) -> None:
        if self._file is not None:
            with contextlib.suppress(OSError):
                self._file.close()
            self._file = None
        note = b"[console log rotated]\n"
        try:
            os.replace(self.path, self.path.with_name(self.path.name + ".1"))
            self.rotations += 1
        except OSError:  # a reader holds the file without delete sharing: truncate in place
            try:
                with open(self.path, "wb"):
                    pass
                self.truncations += 1
                note = b"[console log truncated: it could not be rotated while being read]\n"
            except OSError:
                note = b""
        self._open()
        if note and self._file is not None:
            with contextlib.suppress(OSError):
                self._file.write(note)
                self._file.flush()

    def _run(self) -> None:
        fd = self._stream.fileno()
        pending = b""
        try:
            while True:
                try:
                    chunk = os.read(fd, 65536)
                except OSError:
                    break
                if not chunk:
                    break
                pending += chunk
                cut = pending.rfind(b"\n")
                if cut >= 0:  # whole lines only, so no escape sequence is split
                    self._write(pending[: cut + 1])
                    pending = pending[cut + 1 :]
                elif len(pending) > 65536:
                    self._write(pending)
                    pending = b""
            self._write(pending)
        finally:
            if self._file is not None:
                with contextlib.suppress(OSError):
                    self._file.close()
                self._file = None
            with contextlib.suppress(OSError):
                self._stream.close()


# ------------------------------------------------------------------ children
class ChildProcess:
    """A spawned instance process (``subprocess.Popen`` polled from the event loop)."""

    def __init__(
        self,
        spec: InstanceSpec,
        popen: subprocess.Popen[bytes],
        argv: list[str],
        log_path: Path,
        spawned_mono_ns: int,
        *,
        pump: ConsolePump | None = None,
        env_overrides: Mapping[str, str] | None = None,
    ) -> None:
        self.spec = spec
        self.popen = popen
        self.argv = argv
        self.log_path = log_path
        self.spawned_mono_ns = spawned_mono_ns
        self.pump = pump
        self.env_overrides = dict(env_overrides or {})
        self._descendants: dict[int, float] = {}  # pid -> create time, seen while running

    @property
    def pid(self) -> int:
        return self.popen.pid

    @property
    def returncode(self) -> int | None:
        return self.popen.returncode

    def poll(self) -> int | None:
        """Exit code, or None while running (never blocks)."""
        return self.popen.poll()

    @property
    def alive(self) -> bool:
        return self.popen.poll() is None

    # -------------------------------------------------------------- process tree
    def refresh_descendants(self) -> int:
        """Record the current descendants (pid + create time); returns how many are known."""
        try:
            kids = psutil.Process(self.pid).children(recursive=True)
        except psutil.Error:
            return len(self._descendants)
        for k in kids:
            with contextlib.suppress(psutil.Error):
                self._descendants.setdefault(k.pid, k.create_time())
        return len(self._descendants)

    def descendant_pids(self) -> set[int]:
        return set(self._descendants)

    def tree_pids(self) -> set[int]:
        """This process and its recorded descendants."""
        return {self.pid, *self._descendants}

    def is_own_pid(self, pid: int) -> bool:
        """``pid`` is this process or one of its descendants (the venv launcher's interpreter)."""
        if pid == self.pid or pid in self._descendants:
            return True
        if self.poll() is not None:
            return False
        self.refresh_descendants()
        return pid in self._descendants

    def _recorded(self, pid: int, created: float) -> psutil.Process | None:
        """The recorded descendant ``pid`` if it still runs (same create time: not a reused pid)."""
        try:
            p = psutil.Process(pid)
            if abs(p.create_time() - created) > 0.001 or p.status() == psutil.STATUS_ZOMBIE:
                return None
            return p
        except psutil.Error:
            return None

    def alive_descendants(self) -> list[int]:
        """Recorded descendants that still run."""
        return [pid for pid, created in list(self._descendants.items()) if self._recorded(pid, created) is not None]

    def reap_descendants(self) -> list[int]:
        """Kill recorded descendants that outlived this process (same pid *and* create time)."""
        killed: list[int] = []
        for pid, created in list(self._descendants.items()):
            p = self._recorded(pid, created)
            if p is None:
                continue
            with contextlib.suppress(psutil.Error):
                p.kill()
                killed.append(pid)
        self._descendants.clear()
        return killed

    # -------------------------------------------------------------- stopping
    def send_ctrl_break(self) -> bool:
        """Graceful stop signal: ``CTRL_BREAK_EVENT`` to the child's process group (SIGTERM on POSIX)."""
        if self.popen.poll() is not None:
            return False
        try:
            if sys.platform == "win32":
                os.kill(self.pid, signal.CTRL_BREAK_EVENT)
            else:
                os.killpg(self.pid, signal.SIGTERM)  # the child leads its own session
        except (OSError, ValueError) as exc:
            logger.warning("svc.ctrl_break_failed", svc=self.spec.instance, pid=self.pid, error=str(exc))
            return False
        return True

    def terminate(self) -> None:
        """``TerminateProcess`` (Windows) / SIGTERM (POSIX); descendants are reaped separately."""
        self.refresh_descendants()
        with contextlib.suppress(OSError):
            self.popen.terminate()

    def kill_tree(self) -> None:
        """Hard kill of the process and all its descendants."""
        self.refresh_descendants()
        with contextlib.suppress(OSError):
            self.popen.kill()
        for pid in list(self._descendants):
            with contextlib.suppress(psutil.Error):
                psutil.Process(pid).kill()

    async def wait(self, timeout_s: float, interval_s: float = 0.02) -> int | None:
        """Poll until the process exits or ``timeout_s`` passes; the exit code or None."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.0, timeout_s)
        while True:
            code = self.popen.poll()
            if code is not None:
                return code
            if loop.time() >= deadline:
                return None
            await asyncio.sleep(interval_s)


ArgvFactory = Callable[[InstanceSpec, int | None], list[str]]
"""``argv_factory(spec, clock_offset_ns) -> argv`` (tests inject dummy children)."""


class ProcessManager:
    """Spawns instance processes with the supervisor's conventions.

    Args:
        home: ``<home>`` passed to every child (``--home``) and used as working directory.
        logs_dir: where ``<instance>.console.log`` files go.
        parent_pid: ``--parent-pid`` of the children (default: this process).
        argv_factory: replaces :func:`service_argv` (tests).
        extra_env: added to every child's environment.
        console_max_bytes: size at which a console log is rotated while its child runs.
    """

    def __init__(
        self,
        *,
        home: Path,
        logs_dir: Path,
        parent_pid: int | None = None,
        argv_factory: ArgvFactory | None = None,
        extra_env: Mapping[str, str] | None = None,
        clock: Clock | None = None,
        console_max_bytes: int = CONSOLE_LOG_MAX_BYTES,
    ) -> None:
        self.home = Path(home)
        self.logs_dir = Path(logs_dir)
        self.parent_pid = parent_pid if parent_pid is not None else os.getpid()
        self._argv_factory = argv_factory
        self._extra_env = dict(extra_env or {})
        self._clock = clock or SystemClock()
        self.console_max_bytes = console_max_bytes

    def argv(self, spec: InstanceSpec, clock_offset_ns: int | None = None) -> list[str]:
        if self._argv_factory is not None:
            return list(self._argv_factory(spec, clock_offset_ns))
        return service_argv(spec, home=self.home, parent_pid=self.parent_pid, clock_offset_ns=clock_offset_ns)

    def env(
        self,
        spec: InstanceSpec,
        clock_version: int | None = None,
        instance_env: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        """Child environment: inherited < ``extra_env`` < ``instance_env`` < the supervisor's keys."""
        env = dict(os.environ)
        for name in _DROPPED_ENV:
            env.pop(name, None)
        env.update(self._extra_env)
        env.update(instance_env or {})
        env["PYTHONUTF8"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUNBUFFERED"] = "1"
        env["FUTRISK_HOME"] = str(self.home)
        if clock_version is not None:
            env[CLOCK_VERSION_ENV] = str(int(clock_version))
        return env

    def spawn(
        self,
        spec: InstanceSpec,
        *,
        clock_offset_ns: int | None = None,
        clock_version: int | None = None,
        env: Mapping[str, str] | None = None,
    ) -> ChildProcess:
        """Start ``spec`` (``env``: its per-instance overrides); raises ``OSError`` when it cannot be created."""
        argv = self.argv(spec, clock_offset_ns)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        log_path = console_log_path(self.logs_dir, spec.instance)
        rotate_console_log(log_path, self.console_max_bytes)
        overrides = dict(env or {})
        kwargs: dict[str, Any] = {
            "stdin": subprocess.DEVNULL,
            "stdout": subprocess.PIPE,
            "stderr": subprocess.STDOUT,
            "env": self.env(spec, clock_version, overrides),
            "cwd": str(self.home),
            "bufsize": 0,
        }
        if sys.platform == "win32":
            kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            kwargs["start_new_session"] = True
        shown = loggable_env(overrides)
        env_note = f" (env {' '.join(f'{k}={v}' for k, v in sorted(shown.items()))})" if shown else ""
        header = (
            f"\n===== {datetime.now().isoformat(timespec='seconds')} spawn {spec.instance}{env_note}: "
            f"{subprocess.list2cmdline(argv)}\n"
        )
        with open(log_path, "ab") as fh:
            fh.write(header.encode("utf-8"))
        popen = subprocess.Popen(argv, **kwargs)
        pump: ConsolePump | None = None
        if popen.stdout is not None:
            pump = ConsolePump(popen.stdout, log_path, max_bytes=self.console_max_bytes, name=spec.instance)
            pump.start()
        logger.info("svc.spawned", svc=spec.instance, pid=popen.pid, argv=argv, env=shown)
        return ChildProcess(
            spec, popen, argv, log_path, self._clock.monotonic_ns(), pump=pump, env_overrides=overrides
        )


__all__ = [
    "CLOCK_VERSION_ENV",
    "ENV_ALLOW",
    "KIND_BUS",
    "KIND_HTTP",
    "KIND_PROCESS",
    "SIM_EXCHANGE",
    "STRATEGY_SIM",
    "SUPERVISOR",
    "ArgvFactory",
    "ChildProcess",
    "ConsolePump",
    "InstanceSpec",
    "PortConflict",
    "ProcessManager",
    "apply_env",
    "build_specs",
    "console_log_path",
    "endpoint_port",
    "loggable_env",
    "port_conflicts",
    "rotate_console_log",
    "secret_env_names",
    "service_argv",
    "sim_health_url",
    "strip_ansi",
    "validate_env",
]
