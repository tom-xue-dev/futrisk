"""The multi-process world of the risk-engine benches, seen from the bench process.

``BenchWorld`` owns a temporary runtime home (free loopback ports only - the default ports of a running
installation are never touched), the ``fakes`` child (supervisor clock, clearing-worker, trade-gateway,
md-gateway doubles) and the risk-engine child(ren), and cleans every process it started up again:
``ctl.shutdown`` / ``stop`` first, ``kill`` after a timeout, an ``atexit`` hook as the last resort, and
every child also exits by itself when the bench process disappears (``--parent-pid`` / closed stdin).
Processes it did not start are never touched.
"""

from __future__ import annotations

import atexit
import contextlib
import itertools
import json
import os
import queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import IO, Any

from futrisk.bench.core import now_ns
from futrisk.bench.scenario import MARKET_OPEN, RE_INSTANCE, SHARD, WorldSpec, write_home
from futrisk.bus import BlockingRpcClient
from futrisk.common.config import FullConfig
from futrisk.messages.common import ShutdownRequest, StatusRequest
from futrisk.messages.enums import ServiceState
from futrisk.messages.system import Heartbeat
from futrisk.sim.exchange.testing import offset_for

PREFIX = "@@"
_NL = "\n"
_LIVE: dict[int, Child] = {}
_LIVE_LOCK = threading.Lock()


class ChildError(RuntimeError):
    """A child process failed, exited early or answered with an error."""


def _kill_leftovers() -> None:  # atexit: never leave a child of the bench running
    with _LIVE_LOCK:
        children = list(_LIVE.values())
    for c in children:
        c.kill()


atexit.register(_kill_leftovers)


def python_argv(*args: str) -> list[str]:
    """``[python, -m, futrisk.bench.probe, *args]`` with the interpreter running the bench."""
    from futrisk.common.paths import is_frozen

    if is_frozen():
        raise ChildError("the multi-process benches need a Python installation (source checkout / venv), "
                         "not the frozen futrisk.exe")
    return [sys.executable, "-m", "futrisk.bench.probe", *args]


class Child:
    """A child process of the bench. ``protocol=True``: JSON commands on stdin, ``@@{json}`` lines on stdout."""

    def __init__(self, name: str, argv: list[str], *, log: Path, env: dict[str, str] | None = None,
                 cwd: Path | None = None, protocol: bool = True) -> None:
        self.name = name
        self.log_path = log
        log.parent.mkdir(parents=True, exist_ok=True)
        self._log: IO[bytes] = open(log, "ab")  # noqa: SIM115 - closed in stop()
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
        full_env = dict(os.environ, PYTHONUTF8="1", PYTHONUNBUFFERED="1")
        full_env.update(env or {})
        self.protocol = protocol
        self._ids = itertools.count(1)
        self._q: queue.Queue[dict[str, Any]] = queue.Queue()
        self._stash: list[dict[str, Any]] = []
        self.t_spawn_ns = now_ns()
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE if protocol else subprocess.DEVNULL,
            stdout=subprocess.PIPE if protocol else self._log,
            stderr=self._log,
            cwd=str(cwd) if cwd is not None else None,
            env=full_env,
            creationflags=flags,
        )
        with _LIVE_LOCK:
            _LIVE[id(self)] = self
        self._reader: threading.Thread | None = None
        if protocol:
            self._reader = threading.Thread(target=self._read, name=f"child:{name}", daemon=True)
            self._reader.start()

    def _read(self) -> None:
        out = self.proc.stdout
        assert out is not None
        for raw in out:
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            if line.startswith(PREFIX):
                try:
                    self._q.put(json.loads(line[len(PREFIX):]))
                    continue
                except ValueError:
                    pass
            with contextlib.suppress(ValueError, OSError):
                self._log.write(raw)
        self._q.put({"_eof": True})

    @property
    def pid(self) -> int:
        return int(self.proc.pid)

    def alive(self) -> bool:
        return self.proc.poll() is None

    def log_tail(self, lines: int = 30) -> str:
        with contextlib.suppress(OSError, ValueError):
            self._log.flush()
        try:
            text = self.log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""
        return "\n".join(text.splitlines()[-lines:])

    def recv(self, timeout: float, want: Callable[[dict[str, Any]], bool] | None = None) -> dict[str, Any]:
        """The next message matching ``want`` (others are kept for later calls); raises on timeout / exit."""
        msg = self.try_recv(timeout, want)
        if msg is None:
            raise ChildError(f"{self.name}: no answer within {timeout:.0f}s{_NL}{self.log_tail()}")
        return msg

    def try_recv(self, timeout: float, want: Callable[[dict[str, Any]], bool] | None = None) -> dict[str, Any] | None:
        """Like :meth:`recv` but ``None`` on timeout (still raises when the child is gone)."""
        for i, m in enumerate(self._stash):
            if want is None or want(m):
                return self._stash.pop(i)
        deadline = now_ns() + int(timeout * 1e9)
        while True:
            left = (deadline - now_ns()) / 1e9
            if left <= 0:
                return None
            try:
                msg = self._q.get(timeout=min(left, 0.5))
            except queue.Empty:
                if not self.alive() and self._q.empty():
                    raise ChildError(f"{self.name} exited with {self.proc.returncode}{_NL}{self.log_tail()}") from None
                continue
            if msg.get("_eof"):
                self._q.put(msg)
                code = self.wait(5)
                raise ChildError(f"{self.name} exited with {code}{_NL}{self.log_tail()}")
            if want is None or want(msg):
                return msg
            self._stash.append(msg)

    def send(self, obj: dict[str, Any]) -> None:
        stdin = self.proc.stdin
        if stdin is None:
            raise ChildError(f"{self.name} has no command channel")
        stdin.write((json.dumps(obj) + "\n").encode("utf-8"))
        stdin.flush()

    def call(self, cmd: str, timeout: float = 120.0, **kw: Any) -> dict[str, Any]:
        rid = next(self._ids)
        self.send({"cmd": cmd, "id": rid, **kw})
        msg = self.recv(timeout, lambda m: m.get("id") == rid and "ok" in m)
        if not msg.get("ok"):
            raise ChildError(f"{self.name} {cmd}: {msg.get('error')}\n{msg.get('trace', '')}")
        return msg

    def wait(self, timeout: float) -> int | None:
        try:
            return int(self.proc.wait(timeout))
        except subprocess.TimeoutExpired:
            return None

    def kill(self) -> None:
        if self.alive():
            with contextlib.suppress(OSError):
                self.proc.kill()
            with contextlib.suppress(subprocess.TimeoutExpired, OSError):
                self.proc.wait(10)
        self._close_pipes()

    def stop(self, timeout: float = 20.0, graceful: Callable[[], None] | None = None) -> int | None:
        """Ask the child to stop (``graceful``), wait ``timeout``, kill it if still running; returns the exit code."""
        code = None
        if self.alive():
            if graceful is not None:
                with contextlib.suppress(Exception):
                    graceful()
            code = self.wait(timeout)
            if code is None:
                self.kill()
                code = self.proc.returncode
        else:
            code = self.proc.returncode
        self._close_pipes()
        return code

    def _close_pipes(self) -> None:
        with contextlib.suppress(OSError, ValueError):
            if self.proc.stdin is not None:
                self.proc.stdin.close()
        if self._reader is not None:
            self._reader.join(2)
        with contextlib.suppress(OSError, ValueError):
            self._log.close()
        with _LIVE_LOCK:
            _LIVE.pop(id(self), None)


@contextlib.contextmanager
def bench_dir(ctx: Any, name: str) -> Iterator[Path]:
    """A temporary directory for one bench (under ``ctx.tmp_root`` when set); removed afterwards unless ``ctx.keep``."""
    base = Path(ctx.tmp_root) if getattr(ctx, "tmp_root", None) else None
    if base is not None:
        base.mkdir(parents=True, exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix=f"futrisk-bench-{name}-", dir=str(base) if base is not None else None))
    try:
        yield path
    finally:
        if not getattr(ctx, "keep", False):
            _rmtree(path)
        elif getattr(ctx, "echo", None) is not None:
            ctx.echo(f"{name}: kept {path}")


def _rmtree(path: Path) -> None:
    for _ in range(10):  # Windows: files of just-exited processes can stay locked for a moment
        shutil.rmtree(path, ignore_errors=True)
        if not path.exists():
            return
        time.sleep(0.3)


def check_clock(t_before_ns: int, t_child_ns: int, t_after_ns: int, what: str) -> None:
    """The child's ``perf_counter_ns`` must fall between two readings of ours (one system-wide timeline)."""
    if not t_before_ns <= t_child_ns <= t_after_ns:
        raise ChildError(f"{what}: perf_counter_ns is not comparable across processes "
                         f"({t_before_ns} <= {t_child_ns} <= {t_after_ns} failed); cross-process latencies are invalid")


def wait_until(pred: Callable[[], Any], timeout: float, step: float = 0.01, what: str = "condition") -> Any:
    end = now_ns() + int(timeout * 1e9)
    while True:
        got = pred()
        if got:
            return got
        if now_ns() > end:
            raise ChildError(f"{what} not met within {timeout:.0f}s")
        time.sleep(step)


class BenchWorld:
    """Temporary home + fakes child + risk-engine child; see the module docstring."""

    def __init__(self, spec: WorldSpec, *, base: Path | str | None = None, name: str = "world", keep: bool = False,
                 echo: Callable[[str], None] | None = None) -> None:
        self.spec = spec
        if base is not None:
            Path(base).mkdir(parents=True, exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix=f"futrisk-bench-{name}-", dir=str(base) if base is not None else None))
        self.keep = keep
        self.echo = echo or (lambda _s: None)
        self.home = self.root / "home"
        self.logs = self.root / "logs"
        self.cfg: FullConfig = write_home(self.home, spec)
        self.offset_ns = offset_for(MARKET_OPEN)
        self.fakes: Child | None = None
        self.re: Child | None = None
        self.re_out: Path | None = None
        self._re_n = 0
        self._clients: dict[str, BlockingRpcClient] = {}

    def __enter__(self) -> BenchWorld:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ clients
    def router_of(self, target: str) -> str:
        return self.cfg.services.instance(target).router or ""

    def client(self, identity: str, *, fresh: bool = False) -> BlockingRpcClient:
        c = self._clients.get(identity)
        if c is not None and fresh:
            c.close()
            c = None
        if c is None:
            c = BlockingRpcClient(self.router_of, identity=identity)
            self._clients[identity] = c
        return c

    def status(self, *, timeout_ms: int = 1000) -> Heartbeat | None:
        r = self.client("cli").request(RE_INSTANCE, "ctl.status", StatusRequest(), timeout_ms=timeout_ms, retries=0)
        if not r.ok:
            return None
        hb = r.open_as(Heartbeat)
        return hb if isinstance(hb, Heartbeat) else None

    # ------------------------------------------------------------------ fakes
    def start_fakes(self, timeout: float = 90.0) -> dict[str, Any]:
        args = ["fakes", "--home", str(self.home), "--work", str(self.root / "fakes"),
                "--spec", json.dumps(self.spec.to_args()), "--offset-ns", str(self.offset_ns),
                "--parent-pid", str(os.getpid())]
        self.fakes = Child("fakes", python_argv(*args), log=self.logs / "fakes.log", cwd=self.root)
        ready = self.fakes.recv(timeout, lambda m: bool(m.get("ready")))
        check_clock(self.fakes.t_spawn_ns, int(ready["t_probe_start_ns"]), now_ns(), "fakes process")
        self.echo(f"fakes ready (pid {ready['pid']})")
        return ready

    # ------------------------------------------------------------------ risk-engine
    def start_re(self, *, ticks: bool = False, profile: str | None = None) -> Child:
        """Spawn the risk-engine (production CLI + :mod:`.rehooks`); ``profile``: ``running`` | ``startup`` | None."""
        self._re_n += 1
        self.re_out = self.root / f"re-probe-{self._re_n}.json"
        args = ["re", "--home", str(self.home), "--shard", str(SHARD), "--parent-pid", str(os.getpid()),
                "--out", str(self.re_out)]
        if ticks:
            args.append("--ticks")
        if profile:
            args += ["--profile", profile]
        self.re = Child(f"{RE_INSTANCE}#{self._re_n}", python_argv(*args), log=self.logs / f"re-{self._re_n}.console.log",
                        env={"FUTRISK_HOME": str(self.home)}, cwd=self.root, protocol=False)
        return self.re

    def wait_running(self, timeout: float = 180.0) -> tuple[Heartbeat, int]:
        """Poll ``ctl.status`` until the shard reports phase RUNNING; returns the heartbeat and when it was seen."""
        child = self.re
        assert child is not None

        def running() -> Heartbeat | None:
            if not child.alive():
                raise ChildError(f"{child.name} exited with {child.proc.returncode}\n{child.log_tail()}")
            hb = self.status(timeout_ms=500)
            if hb is None or (hb.detail or {}).get("phase") != "RUNNING":
                return None
            return hb if hb.state in (ServiceState.RUNNING, ServiceState.DEGRADED) else None

        hb: Heartbeat = wait_until(running, timeout, 0.01, f"{RE_INSTANCE} RUNNING")
        return hb, now_ns()

    def stop_re(self, timeout: float = 30.0) -> dict[str, Any] | None:
        """Graceful ``ctl.shutdown`` (identity ``cli``); returns the probe's output (``None`` if none was written)."""
        child = self.re
        if child is None:
            return None

        def shutdown() -> None:
            self.client("cli").request(RE_INSTANCE, "ctl.shutdown", ShutdownRequest(reason="bench"), timeout_ms=3000,
                                       retries=0)

        code = child.stop(timeout, graceful=shutdown)
        self.re = None
        out = self.re_out
        if out is not None and out.exists():
            doc: dict[str, Any] = json.loads(out.read_text(encoding="utf-8"))
            doc["exit_code_observed"] = code
            doc["t_spawn_ns"] = child.t_spawn_ns
            return doc
        return None

    def kill_re(self) -> None:
        """Hard kill (``TerminateProcess`` on Windows): no shutdown hook, risk.db left unclean."""
        child = self.re
        if child is not None:
            child.kill()
            self.re = None

    def risk_db(self) -> Path:
        return self.home / "db" / f"risk-{SHARD}.db"

    def db_counts(self, tables: tuple[str, ...] = ("fills", "orders", "order_events", "outbox")) -> dict[str, int]:
        """Row counts of risk.db tables through a read-only connection (WAL: readers never block the writer)."""
        import sqlite3

        path = self.risk_db()
        if not path.exists():
            return {}
        con = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=5)
        try:
            out = {}
            for t in tables:
                try:
                    out[t] = int(con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0])  # table names are constants
                except sqlite3.Error:
                    out[t] = -1
            return out
        finally:
            con.close()

    # ------------------------------------------------------------------ teardown
    def close(self) -> None:
        if self.re is not None:
            with contextlib.suppress(Exception):
                self.stop_re(timeout=20.0)
        if self.fakes is not None:
            fakes = self.fakes

            def stop_fakes() -> None:
                fakes.send({"cmd": "stop", "id": 0})

            fakes.stop(20.0, graceful=stop_fakes)
            self.fakes = None
        for c in self._clients.values():
            with contextlib.suppress(Exception):
                c.close()
        self._clients.clear()
        if not self.keep:
            _rmtree(self.root)
        else:
            self.echo(f"kept {self.root}")


__all__ = ["BenchWorld", "Child", "ChildError", "bench_dir", "check_clock", "python_argv", "wait_until"]
