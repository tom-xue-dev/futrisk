"""``futrisk doctor``: local environment checks (01 §7.1 D1; plan §11 known pitfalls).

Every check is local and fast (nothing leaves localhost) and reports PASS / WARN / FAIL / INFO;
the exit code is 1 when any check FAILs.

========================  ==============================================================
check                     what
========================  ==============================================================
python, build             interpreter version (3.12), frozen exe or dev run
home, disk                home resolution (``--home`` / ``FUTRISK_HOME`` / default), writable, free space
non-ASCII path            create / read / SQLite-open a file under a Chinese-named directory
tz Asia/Shanghai          ``zoneinfo`` has the market time zone (``tzdata``); the OS zone as info
clock sync                Windows: ``w32tm /query /status`` (warn only)
pyzmq, sqlite             versions; SQLite >= 3.35 (RETURNING) and WAL in ``<home>/db``
config                    ``config validate`` of the working config and the active version
ports <instance>          every TCP endpoint of services.yaml (+ ``--api``) and sim.yaml: free,
                          or held by a futrisk process of this home (system up), or by another
                          program / a futrisk process of another ``--home`` / the OS (blocked)
api-gateway home          the api-gateway listening on the http endpoint serves this home
                          (``/health`` ``home_id`` = ``HomeLayout.home_id()``); asked only when
                          something listens there
api key, dashboard, logs  bootstrap key file (info), ``web_static/dist/index.html``, ``<home>/logs``
========================  ==============================================================
"""

from __future__ import annotations

import contextlib
import os
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer

from futrisk.cli.toolkit import ApiOpt, HomeOpt, JsonOpt, Out, state_style

if TYPE_CHECKING:
    from futrisk.common.config import FullConfig
    from futrisk.common.paths import HomeLayout

PASS, WARN, FAIL, INFO = "PASS", "WARN", "FAIL", "INFO"
_RANK = {INFO: 0, PASS: 1, WARN: 2, FAIL: 3}
MIN_FREE_WARN = 2 * 1024**3
MIN_FREE_FAIL = 200 * 1024**2
MIN_SQLITE = (3, 35, 0)  # UPDATE ... RETURNING (db/outbox.py)
_LOCAL_HOSTS = {"127.0.0.1", "localhost", "0.0.0.0", "*", "::1", "::", ""}
_WILDCARDS = {"0.0.0.0", "*", "::", ""}
_W32TM_TIMEOUT_S = 3.0


@dataclass
class Check:
    check: str
    status: str
    detail: str


# ====================================================================== helpers
def probe_writable(directory: Path) -> str | None:
    """None when a file can be created in ``directory``; else the error text."""
    import tempfile

    try:
        fd, name = tempfile.mkstemp(prefix=".doctor-", dir=directory)
        os.close(fd)
        os.unlink(name)
        return None
    except OSError as exc:
        return str(exc)


def existing_ancestor(path: Path) -> Path:
    p = path
    while not p.exists() and p.parent != p:
        p = p.parent
    return p


def home_source(explicit: str | None) -> str:
    from futrisk.common.paths import is_frozen

    if explicit:
        return "--home"
    if os.environ.get("FUTRISK_HOME"):
        return "FUTRISK_HOME"
    return "default: <exe dir>/var or %LOCALAPPDATA%/futrisk" if is_frozen() else "default: <repo>/var"


def _gib(n: float) -> str:
    return f"{n / 1024**3:.1f} GiB"


# ====================================================================== checks
def check_python() -> list[Check]:
    import platform

    from futrisk import __version__
    from futrisk.common.paths import is_frozen

    ok = sys.version_info[:2] == (3, 12)
    detail = f"{platform.python_version()} at {sys.executable}"
    return [
        Check("python", PASS if ok else WARN, detail if ok else f"{detail}; futrisk is built and tested on 3.12"),
        Check("build", INFO, f"futrisk {__version__}, " + ("frozen exe" if is_frozen() else "dev run (python -m futrisk.main)")),
    ]


def check_home(layout: HomeLayout, explicit: str | None) -> tuple[list[Check], bool]:
    """Home resolution, writability and free space; returns ``(rows, home is usable)``."""
    import shutil

    home = layout.home
    rows: list[Check] = []
    writable = False
    if home.is_dir():
        err = probe_writable(home)
        writable = err is None
        rows.append(Check("home", PASS if writable else FAIL,
                          f"{home} ({home_source(explicit)})" + ("" if writable else f": not writable: {err}")))
    else:
        base = existing_ancestor(home)
        err = probe_writable(base)
        rows.append(Check("home", INFO if err is None else FAIL,
                          f"{home} ({home_source(explicit)}) does not exist yet; the first start creates it"
                          + ("" if err is None else f", but {base} is not writable: {err}")))
    usage = shutil.disk_usage(existing_ancestor(home))
    status = FAIL if usage.free < MIN_FREE_FAIL else WARN if usage.free < MIN_FREE_WARN else PASS
    rows.append(Check("disk", status, f"{_gib(usage.free)} free of {_gib(usage.total)} on the home drive"))
    return rows, writable


def check_non_ascii(layout: HomeLayout, home_writable: bool) -> Check:
    """Create, read and SQLite-open files under a Chinese-named directory (01 §7.1: Chinese paths)."""
    import secrets
    import shutil
    import sqlite3
    import tempfile

    base = layout.home if home_writable else Path(tempfile.gettempdir())
    if home_writable:
        where = "home"
    else:
        where = "temp dir: home " + ("not writable" if layout.home.is_dir() else "not created yet")
    probe = base / f".doctor-中文路径-{secrets.token_hex(3)}"
    text = "期货风控 futrisk ✓"
    try:
        probe.mkdir(parents=True)
        f = probe / "结算单.txt"
        f.write_text(text, encoding="utf-8")
        ok = f.read_text(encoding="utf-8") == text and any(p.name == f.name for p in probe.iterdir())
        con = sqlite3.connect(probe / "测试.db")
        try:
            con.execute("CREATE TABLE t (x TEXT)")
            con.execute("INSERT INTO t VALUES (?)", (text,))
            con.commit()
            ok = ok and con.execute("SELECT x FROM t").fetchone()[0] == text
        finally:
            con.close()
    except (OSError, sqlite3.Error, UnicodeError) as exc:
        return Check("non-ASCII path", FAIL, f"{probe}: {exc}")
    finally:
        shutil.rmtree(probe, ignore_errors=True)
    return Check("non-ASCII path", PASS if ok else FAIL,
                 f"file and SQLite round trip under {base / '.doctor-中文路径-*'} ({where})" if ok
                 else f"{probe}: read back a different text")


def check_timezone() -> list[Check]:
    import time
    from datetime import datetime, timedelta

    rows: list[Check] = []
    try:
        from zoneinfo import ZoneInfo

        tz = ZoneInfo("Asia/Shanghai")
        off = datetime(2026, 9, 28, 12, 0, tzinfo=tz).utcoffset()
        try:
            from importlib.metadata import version

            source = f"tzdata {version('tzdata')}"
        except Exception:
            source = "the system time zone database"
        rows.append(Check("tz Asia/Shanghai", PASS if off == timedelta(hours=8) else FAIL,
                          f"UTC+08:00 from {source}" if off == timedelta(hours=8) else f"unexpected offset {off}"))
    except Exception as exc:
        rows.append(Check("tz Asia/Shanghai", FAIL, f"{exc}; install the tzdata package"))
    lt = time.localtime()
    gmtoff = int(lt.tm_gmtoff or 0)
    sign = "+" if gmtoff >= 0 else "-"
    h, m = divmod(abs(gmtoff) // 60, 60)
    rows.append(Check("os time zone", INFO,
                      f"UTC{sign}{h:02d}:{m:02d} ({lt.tm_zone}); market time always uses Asia/Shanghai"))
    return rows


def run_w32tm() -> tuple[int, str] | None:
    """``(exit code, output)`` of ``w32tm /query /status``; None when w32tm is missing."""
    import subprocess

    try:
        proc = subprocess.run(["w32tm", "/query", "/status"], capture_output=True, timeout=_W32TM_TIMEOUT_S,
                              stdin=subprocess.DEVNULL, check=False)
    except FileNotFoundError:
        return None
    except subprocess.TimeoutExpired:
        return -1, f"no answer within {_W32TM_TIMEOUT_S:g}s"
    raw = (proc.stdout or b"") + (proc.stderr or b"")
    for enc in ("oem", "mbcs", "utf-8"):
        try:
            return proc.returncode, raw.decode(enc)
        except (LookupError, UnicodeDecodeError):
            continue
    return proc.returncode, raw.decode("utf-8", "replace")


_SOURCE_LABELS = ("source", "源", "quelle", "ソース")
_LAST_SYNC_LABELS = ("last successful sync time", "上次成功同步时间", "最后成功同步时间")


def _w32tm_field(text: str, labels: Sequence[str]) -> str | None:
    for line in text.splitlines():
        key, sep, value = line.partition(":")
        if sep and key.strip().lower() in labels:
            return value.strip()
    return None


def check_clock_sync(runner: Callable[[], tuple[int, str] | None] | None = None) -> Check:
    if sys.platform != "win32" and runner is None:
        return Check("clock sync", INFO, "not checked (w32tm is Windows only)")
    res = (runner or run_w32tm)()
    if res is None:
        return Check("clock sync", INFO, "w32tm not found; clock synchronisation not checked")
    code, text = res
    first = next((ln.strip() for ln in text.splitlines() if ln.strip()), "")
    if code != 0:
        hexcode = f" (0x{code & 0xFFFFFFFF:08X})" if code < 0 else ""
        return Check("clock sync", WARN,
                     f"w32tm /query /status failed{hexcode}: {first or 'no output'}; the Windows Time service may "
                     "be stopped, so NTP synchronisation is unknown")
    source = _w32tm_field(text, _SOURCE_LABELS)
    last = _w32tm_field(text, _LAST_SYNC_LABELS)
    if source is None:
        return Check("clock sync", WARN, f"w32tm answered without a time source: {first}")
    if "local cmos clock" in source.lower() or "free-running" in source.lower():
        return Check("clock sync", WARN, f"not synchronised: source {source}")
    return Check("clock sync", PASS, f"source {source}" + (f", last sync {last}" if last else ""))


def check_pyzmq() -> Check:
    try:
        import zmq
    except ImportError as exc:
        return Check("pyzmq", FAIL, f"cannot import pyzmq: {exc}")
    return Check("pyzmq", PASS, f"pyzmq {zmq.pyzmq_version()}, libzmq {zmq.zmq_version()}")


def check_sqlite(layout: HomeLayout, home_writable: bool) -> Check:
    import secrets
    import sqlite3
    import tempfile

    version = sqlite3.sqlite_version
    if sqlite3.sqlite_version_info < MIN_SQLITE:
        return Check("sqlite", FAIL, f"SQLite {version} < 3.35 (UPDATE ... RETURNING is required)")
    candidates = [layout.db_dir, layout.home] if home_writable else []
    base = next((d for d in candidates if d.is_dir() and probe_writable(d) is None), Path(tempfile.gettempdir()))
    path = base / f".doctor-wal-{secrets.token_hex(3)}.db"
    try:
        con = sqlite3.connect(path)
        try:
            mode = str(con.execute("PRAGMA journal_mode=WAL").fetchone()[0]).lower()
            con.execute("CREATE TABLE t (x INTEGER)")
            con.execute("INSERT INTO t VALUES (1)")
            con.commit()
        finally:
            con.close()
    except sqlite3.Error as exc:
        return Check("sqlite", FAIL, f"SQLite {version}: {exc} in {base}")
    finally:
        for suffix in ("", "-wal", "-shm"):
            with contextlib.suppress(OSError):
                Path(f"{path}{suffix}").unlink(missing_ok=True)
    if mode != "wal":
        return Check("sqlite", FAIL, f"SQLite {version}: journal_mode=WAL gave {mode!r} in {base} (network drive?)")
    return Check("sqlite", PASS, f"SQLite {version}, WAL works in {base}")


def check_config(layout: HomeLayout) -> tuple[list[Check], FullConfig | None]:
    """``config validate`` of ``<home>/config`` and of the active version (what services load)."""
    from futrisk.common.config import ConfigError, load_active_config, load_config_dir, load_default_config

    rows: list[Check] = []
    working = active = None
    if (layout.config_dir / "app.yaml").exists():
        try:
            working = load_config_dir(layout.config_dir)
            rows.append(Check("config", PASS, f"{layout.config_dir} valid: {len(working.products.instruments)} "
                              f"instruments, {len(working.accounts.accounts)} accounts, {len(working.risk_rules.rules)} "
                              f"rules, hash {working.config_hash[:12]}"))
        except ConfigError as exc:
            rows.append(Check("config", FAIL, f"INVALID {layout.config_dir}: {exc}"))
    if layout.active_json.exists():
        try:
            active = load_active_config(layout)
            rows.append(Check("config active", PASS, f"version {active.config_version} valid "
                              f"(hash {active.config_hash[:12]})"))
        except (ConfigError, OSError, ValueError, KeyError, TypeError) as exc:
            rows.append(Check("config active", FAIL, f"INVALID active version ({layout.active_json}): {exc}"))
    if not rows:
        try:
            working = load_default_config()
            rows.append(Check("config", INFO, "the home has no config yet (the first start copies the bundled "
                              "defaults); bundled defaults valid"))
        except ConfigError as exc:
            rows.append(Check("config", FAIL, f"bundled defaults INVALID: {exc}"))
    cfg = active or working
    if cfg is not None:
        rows.append(Check("app mode", INFO, f"app.mode={cfg.app.mode}" + (" (demo commands enabled)"
                                                                          if cfg.app.mode == "demo" else "")))
    return rows, cfg


# ---------------------------------------------------------------- ports
@dataclass(frozen=True)
class Endpoint:
    owner: str
    label: str
    host: str
    port: int

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"


def parse_endpoint(url: str) -> tuple[str, int] | None:
    """``tcp://127.0.0.1:7101`` / ``http://127.0.0.1:8000`` -> ``("127.0.0.1", 7101)``."""
    from urllib.parse import urlsplit

    try:
        parts = urlsplit(url if "://" in url else f"tcp://{url}")
        port = parts.port
    except ValueError:
        return None
    if port is None:
        port = 443 if parts.scheme == "https" else 80 if parts.scheme == "http" else None
    return (parts.hostname or "", port) if port is not None else None


def config_endpoints(cfg: FullConfig, api: str | None = None) -> list[Endpoint]:
    """Every TCP endpoint futrisk binds: services.yaml (pub/router/http) and sim.yaml."""
    out: list[Endpoint] = []
    for name, inst in cfg.services.instances.items():
        for label in ("pub", "router", "http"):
            url = getattr(inst, label)
            if name == "api-gateway" and label == "http" and api:
                url = api
            parsed = parse_endpoint(url) if url else None
            if parsed is not None:
                out.append(Endpoint(name, label, *parsed))
    sim = cfg.sim
    out.append(Endpoint("sim-exchange", "feed_a", sim.feed_a.host, sim.feed_a.port))
    out.append(Endpoint("sim-exchange", "feed_b", sim.feed_b.host, sim.feed_b.port))
    broker = parse_endpoint(sim.broker.endpoint)
    if broker is not None:
        out.append(Endpoint("sim-exchange", "broker", *broker))
    out.append(Endpoint("sim-exchange", "control", sim.control.host, sim.control.port))
    return out


def listening_sockets() -> dict[int, list[tuple[str, int | None]]] | None:
    """``{port: [(ip, pid)]}`` of LISTEN TCP sockets (psutil); None when unavailable."""
    try:
        import psutil

        conns = psutil.net_connections(kind="tcp")
    except Exception:
        return None
    out: dict[int, list[tuple[str, int | None]]] = {}
    for c in conns:
        if c.status == psutil.CONN_LISTEN and c.laddr:
            out.setdefault(int(c.laddr.port), []).append((str(c.laddr.ip), c.pid))
    return out


def is_futrisk_cmdline(name: str, cmdline: Sequence[str]) -> bool:
    """``futrisk.exe ...``, ``futrisk ...``, ``python -m futrisk.main ...`` (or another ``futrisk.*`` module)."""
    if Path(name).stem.lower() == "futrisk":
        return True
    for i, tok in enumerate(cmdline):
        low = tok.lower().replace("\\", "/")
        base = low.rsplit("/", 1)[-1]
        if base in ("futrisk", "futrisk.exe") or low.endswith("futrisk/main.py"):
            return True
        if i > 0 and cmdline[i - 1] == "-m" and (low == "futrisk" or low.startswith("futrisk.")):
            return True
    return False


def service_of(cmdline: Sequence[str]) -> str:
    """Service name from a futrisk command line (``svc md-gateway`` / ``--instance`` / ``up``)."""
    toks = list(cmdline)
    for opt in ("--instance",):
        if opt in toks and toks.index(opt) + 1 < len(toks):
            return toks[toks.index(opt) + 1]
    if "svc" in toks and toks.index("svc") + 1 < len(toks):
        name = toks[toks.index("svc") + 1]
        if "--shard" in toks and toks.index("--shard") + 1 < len(toks):
            return f"{name}-{toks[toks.index('--shard') + 1]}"
        return name
    for cmd in ("up", "demo"):
        if cmd in toks:
            return "supervisor"
    return "process"


def home_of(cmdline: Sequence[str]) -> str | None:
    """The ``--home`` of a futrisk command line (``--home X`` or ``--home=X``), None when absent."""
    toks = list(cmdline)
    for i, tok in enumerate(toks):
        if tok == "--home" and i + 1 < len(toks):
            return toks[i + 1]
        if tok.startswith("--home="):
            return tok.split("=", 1)[1]
    return None


def same_path(a: str | Path, b: str | Path) -> bool:
    try:
        return os.path.normcase(str(Path(a).resolve())) == os.path.normcase(str(Path(b).resolve()))
    except OSError:
        return False


def process_info(pid: int) -> tuple[str, list[str]]:
    """``(name, cmdline)`` of a process (psutil; the command line may be ``[]`` when access is denied)."""
    import psutil

    proc = psutil.Process(pid)
    name = proc.name()
    try:
        cmdline = proc.cmdline()
    except (psutil.AccessDenied, psutil.ZombieProcess):
        cmdline = []
    return name, cmdline


@dataclass(frozen=True)
class Owner:
    """The process holding a port."""

    futrisk: bool | None  # None: unknown
    label: str
    home: str | None = None  # the ``--home`` of a futrisk process


def describe_pid(pid: int | None) -> Owner:
    if not pid:
        return Owner(None, "an unknown process")
    try:
        name, cmdline = process_info(pid)
    except Exception as exc:
        kind = type(exc).__name__
        if kind == "AccessDenied":
            return Owner(False, f"pid {pid} (access denied)")
        return Owner(None, f"pid {pid} ({kind})")
    if is_futrisk_cmdline(name, cmdline):
        return Owner(True, f"futrisk {service_of(cmdline)} (pid {pid})", home_of(cmdline))
    return Owner(False, f"{name} (pid {pid})")


def bind_probe(host: str, port: int) -> str | None:
    """None when ``host:port`` can be bound; ``"in_use"``, ``"blocked"`` or the error text otherwise."""
    import errno
    import socket

    bind_host = "0.0.0.0" if host in ("*", "") else "127.0.0.1" if host == "localhost" else host
    family = socket.AF_INET6 if ":" in bind_host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        if sys.platform == "win32":
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        sock.bind((bind_host, port))
        return None
    except OSError as exc:
        win = getattr(exc, "winerror", None)
        if exc.errno == errno.EADDRINUSE or win == 10048:
            return "in_use"
        if exc.errno == errno.EACCES or win == 10013:
            return "blocked"
        return str(exc)
    finally:
        sock.close()


def _matches(listen_ip: str, host: str) -> bool:
    if host in _WILDCARDS or listen_ip in _WILDCARDS:
        return True
    if host == "localhost":
        return listen_ip in ("127.0.0.1", "::1")
    return listen_ip == host


Listeners = dict[int, list[tuple[str, int | None]]]


def is_local(host: str) -> bool:
    return host in _LOCAL_HOSTS or host.startswith("127.")


def owners_of(listeners: Listeners | None, host: str, port: int) -> list[int | None]:
    return [pid for ip, pid in (listeners or {}).get(port, []) if _matches(ip, host)]


def classify(ep: Endpoint, listeners: Listeners | None, home: Path | None = None) -> tuple[str, str, str]:
    """``(status, state, text)``; state is free | futrisk | foreign | unknown | remote.

    A futrisk process whose command line names another ``--home`` than ``home`` is foreign: this
    home's system cannot bind the port and the CLI would talk to the other home's system.
    """
    if not is_local(ep.host):
        return INFO, "remote", f"{ep.addr} not local, not checked"
    owners = owners_of(listeners, ep.host, ep.port)
    if owners:
        owner = describe_pid(owners[0])
        if owner.futrisk and home is not None and owner.home is not None and not same_path(owner.home, home):
            return FAIL, "foreign", f"{ep.addr} held by {owner.label} of another home ({owner.home})"
        if owner.futrisk:
            return PASS, "futrisk", f"{ep.addr} held by {owner.label}"
        if owner.futrisk is None:
            return WARN, "unknown", f"{ep.addr} in use by {owner.label}"
        return FAIL, "foreign", f"{ep.addr} in use by {owner.label}, not futrisk"
    probe = bind_probe(ep.host, ep.port)
    if probe is None:
        return PASS, "free", f"{ep.addr} free"
    if probe == "in_use":
        return WARN, "unknown", f"{ep.addr} in use (owner unknown)"
    if probe == "blocked":
        return FAIL, "foreign", (f"{ep.addr} blocked by the OS (reserved port range? "
                                 "`netsh int ipv4 show excludedportrange protocol=tcp`)")
    return FAIL, "foreign", f"{ep.addr}: {probe}"


def check_ports(cfg: FullConfig, api: str | None, listeners: Listeners | None, home: Path | None = None) -> list[Check]:
    endpoints = config_endpoints(cfg, api)
    seen: dict[tuple[str, int], Endpoint] = {}
    per_owner: dict[str, list[tuple[str, str]]] = {}
    states: list[str] = []
    for ep in endpoints:
        status, state, text = classify(ep, listeners, home)
        dup = seen.get((ep.host, ep.port))
        if dup is not None:
            status, text = FAIL, f"{ep.addr} configured twice (also {dup.owner} {dup.label})"
        seen.setdefault((ep.host, ep.port), ep)
        per_owner.setdefault(ep.owner, []).append((status, f"{ep.label} {text}"))
        states.append(state)
    rows = []
    for owner, items in per_owner.items():
        worst = max((s for s, _ in items), key=lambda s: _RANK[s])
        rows.append(Check(f"ports {owner}", worst, "; ".join(t for _, t in items)))
    local = [s for s in states if s != "remote"]
    held = sum(1 for s in local if s == "futrisk")
    free = sum(1 for s in local if s == "free")
    if not local:
        summary = "no local endpoint (every host is remote, e.g. the docker profile): not checked"
    elif free == len(local):
        summary = f"down: all {len(local)} local endpoints are free"
    elif local and held == len(local):
        summary = f"up: all {len(local)} local endpoints are held by futrisk processes"
    else:
        others = len(local) - held - free
        summary = f"{held} of {len(local)} endpoints held by futrisk, {free} free" + (
            f", {others} held by other programs, another home or blocked" if others else "")
    if listeners is None:
        summary += " (psutil unavailable: owners not identified)"
    rows.insert(0, Check("system", INFO, summary))
    return rows


def fetch_health(url: str, timeout_s: float = 2.0) -> dict[str, Any]:
    """``GET {url}/api/v1/health`` data (public; the tools CLI's transport, so tests can fake it)."""
    import httpx

    from futrisk.cli import apiclient

    with httpx.Client(trust_env=False, timeout=timeout_s, transport=apiclient.TRANSPORT) as client:
        resp = client.get(f"{url}/api/v1/health")
    resp.raise_for_status()
    doc = resp.json()
    data = doc.get("data") if isinstance(doc, dict) else None
    return data if isinstance(data, dict) else {}


def check_gateway_home(layout: HomeLayout, cfg: FullConfig, api: str | None, listeners: Listeners | None) -> Check:
    """The api-gateway answering on this home's endpoint must serve this home (``/health`` ``home_id``).

    Asked only when something listens on the port (a closed localhost port costs seconds on
    Windows); the home id is read without creating it (a home never started has none).
    """
    name = "api-gateway home"
    inst = cfg.services.instances.get("api-gateway")
    url = (api or (inst.http if inst is not None else None) or "").rstrip("/")
    if url.endswith("/api/v1"):
        url = url[: -len("/api/v1")]
    parsed = parse_endpoint(url) if url else None
    if parsed is None:
        return Check(name, INFO, "no api-gateway http endpoint configured")
    host, port = parsed
    if not is_local(host):
        return Check(name, INFO, f"{url} is not local: not checked")
    if listeners is not None and not owners_of(listeners, host, port):
        return Check(name, INFO, f"no api-gateway listening at {url}: nothing to compare")
    try:
        health = fetch_health(url if "://" in url else f"http://{url}")
    except Exception as exc:
        return Check(name, WARN, f"{url} holds the api-gateway port but /health failed: {type(exc).__name__}: {exc}")
    theirs = health.get("home_id")
    pid = f", pid {health.get('pid')}" if health.get("pid") else ""
    if not isinstance(theirs, str) or not theirs:
        return Check(name, WARN, f"the api-gateway at {url}{pid} reports no home_id (older version): cannot tell "
                                 "which home it serves")
    ours = layout.home_id() if layout.home_id_file.is_file() else None
    if theirs == ours:
        return Check(name, PASS, f"the api-gateway at {url}{pid} serves this home (home_id {ours})")
    mine = f"this home is {ours}" if ours else "this home was never started (no home_id)"
    return Check(name, FAIL, f"the api-gateway at {url}{pid} serves another home (home_id {theirs}; {mine}): "
                             "commands of this home would reach the other system (the CLI refuses to send it "
                             "this home's key)")


# ---------------------------------------------------------------- the rest
def check_api_key(layout: HomeLayout) -> Check:
    from futrisk.cli.ops_cmd import API_KEY_ENV, BOOTSTRAP_KEY_FILE

    path = layout.secrets_dir / BOOTSTRAP_KEY_FILE
    env = " ; FUTRISK_API_KEY is set and takes precedence" if os.environ.get(API_KEY_ENV, "").strip() else ""
    if path.is_file():
        return Check("api key", INFO, f"bootstrap ADMIN key present: {path}{env}")
    return Check("api key", INFO, f"no bootstrap key yet ({path}; api-gateway writes it at first start){env}")


def check_dashboard() -> Check:
    import futrisk

    index = Path(futrisk.__file__).resolve().parent / "web_static" / "dist" / "index.html"
    if index.is_file():
        return Check("dashboard", PASS, f"{index}")
    return Check("dashboard", WARN, f"{index} missing: the dashboard is not built (API only; build web/)")


def check_logs(layout: HomeLayout) -> Check:
    logs = layout.logs_dir
    if not logs.is_dir():
        return Check("logs", INFO, f"{logs} does not exist yet (services create it at start)")
    err = probe_writable(logs)
    if err is not None:
        return Check("logs", FAIL, f"{logs} not writable: {err}")
    files = [p for p in logs.iterdir() if p.is_file() and p.suffix == ".log"]
    size = sum(p.stat().st_size for p in files)
    return Check("logs", PASS, f"{logs}: {len(files)} log files, {size / 1024**2:.1f} MiB")


def _safe(name: str, fn: Callable[[], Any]) -> list[Check]:
    try:
        res = fn()
    except Exception as exc:
        return [Check(name, FAIL, f"check crashed: {type(exc).__name__}: {exc}")]
    if isinstance(res, Check):
        return [res]
    return list(res)


def run_checks(home: str | None, api: str | None = None) -> list[Check]:
    from futrisk.common.paths import HomeLayout, resolve_home

    rows: list[Check] = []
    rows += _safe("python", check_python)
    layout = HomeLayout(resolve_home(home))
    writable = False

    def home_check() -> list[Check]:
        nonlocal writable
        found, writable = check_home(layout, home)
        return found

    rows += _safe("home", home_check)
    rows += _safe("non-ASCII path", lambda: check_non_ascii(layout, writable))
    rows += _safe("tz Asia/Shanghai", check_timezone)
    rows += _safe("clock sync", check_clock_sync)
    rows += _safe("pyzmq", check_pyzmq)
    rows += _safe("sqlite", lambda: check_sqlite(layout, writable))
    cfg_holder: list[FullConfig | None] = [None]

    def config_check() -> list[Check]:
        found, cfg_holder[0] = check_config(layout)
        return found

    rows += _safe("config", config_check)
    cfg = cfg_holder[0]
    if cfg is not None:
        listeners = listening_sockets()  # once: the port owners and the api-gateway home check share it
        rows += _safe("ports", lambda: check_ports(cfg, api, listeners, layout.home))
        rows += _safe("api-gateway home", lambda: check_gateway_home(layout, cfg, api, listeners))
    else:
        rows.append(Check("ports", WARN, "not checked: no valid configuration"))
    rows += _safe("api key", lambda: check_api_key(layout))
    rows += _safe("dashboard", check_dashboard)
    rows += _safe("logs", lambda: check_logs(layout))
    return rows


def doctor(home: str | None = HomeOpt, api: str | None = ApiOpt, as_json: bool = JsonOpt) -> None:
    """Check the local environment; exit code 1 when a check fails.

    Python, home, Chinese paths, tzdata, clock sync, pyzmq, SQLite/WAL, config, ports, the api-gateway's
    home (/health home_id), key, dashboard, logs.
    """
    checks = run_checks(home, api)
    failed = [c for c in checks if c.status == FAIL]
    out = Out(as_json)
    if as_json:
        out.obj({"ok": not failed, "checks": [asdict(c) for c in checks]})
    else:
        out.table(["CHECK", "STATUS", "DETAIL"], [[c.check, (c.status, state_style(c.status)), c.detail] for c in checks],
                  title="futrisk doctor")
        warned = sum(1 for c in checks if c.status == WARN)
        passed = sum(1 for c in checks if c.status == PASS)
        style = "bold red" if failed else "yellow" if warned else "green"
        out.line(f"{passed} passed, {warned} warning(s), {len(failed)} failed", style)
    if failed:
        raise typer.Exit(1)


__all__ = [
    "Check",
    "Endpoint",
    "check_clock_sync",
    "check_ports",
    "classify",
    "config_endpoints",
    "describe_pid",
    "doctor",
    "is_futrisk_cmdline",
    "run_checks",
    "service_of",
]
