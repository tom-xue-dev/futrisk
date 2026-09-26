"""Machine description and load sampling for benchmark results.

:func:`machine_info` records what the numbers depend on: CPU model and cores, OS, Python, pyzmq /
libzmq, msgspec and SQLite versions, the Windows power plan, and the code identity (git SHA or, without
commits, a hash of ``src/futrisk``). :class:`LoadSampler` samples CPU utilisation while a bench runs:
the whole machine, this process tree (the bench and every child it started), and the difference - the
background load of other programs. A bench taken while ``background_mean_pct`` is high is pessimistic.
"""

from __future__ import annotations

import contextlib
import os
import platform
import re
import sqlite3
import subprocess
import sys
import threading
from typing import Any

import psutil

#: mean background CPU (other programs, % of the whole machine) above which a run counts as "under load"
LOADED_BACKGROUND_PCT = 10.0


def _cpu_model() -> str:
    if sys.platform == "win32":
        try:
            import winreg

            with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DESCRIPTION\System\CentralProcessor\0") as key:
                name, _ = winreg.QueryValueEx(key, "ProcessorNameString")
                return str(name).strip()
        except OSError:
            pass
    elif sys.platform.startswith("linux"):
        try:
            with open("/proc/cpuinfo", encoding="utf-8") as f:
                for line in f:
                    if line.lower().startswith("model name"):
                        return line.split(":", 1)[1].strip()
        except OSError:
            pass
    elif sys.platform == "darwin":
        try:
            out = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True, timeout=2,
                                 check=False)
            if out.stdout.strip():
                return out.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            pass
    return platform.processor() or platform.machine()


def power_plan() -> str | None:
    """Active Windows power plan (``powercfg /getactivescheme``), ``None`` elsewhere or when unavailable."""
    if sys.platform != "win32":
        return None
    try:
        out = subprocess.run(["powercfg", "/getactivescheme"], capture_output=True, timeout=3, check=False)
    except (OSError, subprocess.SubprocessError):
        return None
    text = out.stdout.decode("utf-8", errors="replace") if out.stdout else ""
    if "�" in text:
        text = out.stdout.decode("mbcs", errors="replace")
    m = re.search(r"([0-9a-fA-F-]{36})\s*\((.*)\)", text)
    if m:
        return f"{m.group(2).strip()} ({m.group(1)})"
    return text.strip() or None


def machine_info(*, code: bool = True) -> dict[str, Any]:
    """Static description of the machine and the software stack."""
    import msgspec
    import zmq

    freq = None
    try:
        f = psutil.cpu_freq()
        if f is not None:
            freq = {"current_mhz": round(f.current), "max_mhz": round(f.max)}
    except (OSError, NotImplementedError, AttributeError):
        freq = None
    vm = psutil.virtual_memory()
    info: dict[str, Any] = {
        "cpu": _cpu_model(),
        "cores_physical": psutil.cpu_count(logical=False),
        "cores_logical": psutil.cpu_count(logical=True),
        "cpu_freq": freq,
        "memory_gb": round(vm.total / 2**30, 1),
        "memory_available_gb": round(vm.available / 2**30, 1),
        "os": platform.platform(),
        "python": f"{platform.python_implementation()} {platform.python_version()} ({platform.python_compiler()})",
        "pyzmq": zmq.__version__,
        "libzmq": zmq.zmq_version(),
        "msgspec": msgspec.__version__,
        "sqlite": sqlite3.sqlite_version,
        "power_plan": power_plan(),
        "pid": os.getpid(),
    }
    if code:
        from futrisk.backtest.manifest import code_hash, git_sha

        info["git_sha"] = git_sha()
        total, per = code_hash()
        info["code_hash"] = total[:16]
        info["code_hash_packages"] = {k: v[:12] for k, v in per.items()}
    return info


class LoadSampler:
    """CPU utilisation while a bench runs, sampled every ``interval_s`` by a separate process (``probe load``).

    Machine = all logical CPUs (100 % = all busy); own = the bench process and every process it started;
    background = machine - own (other programs). Sampling runs out of process because enumerating the
    process tree takes ~15 ms of GIL-holding work that would stall the measured thread here. Without a
    Python installation (frozen exe) it falls back to a thread sampling the machine and this process only.
    """

    def __init__(self, interval_s: float = 0.5) -> None:
        self.interval_s = interval_s
        self.system: list[float] = []
        self.own: list[float] = []
        self.mode = "process"
        self._child: Any = None
        self._me: psutil.Process | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> LoadSampler:
        self.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.stop()

    def start(self) -> None:
        try:
            import tempfile
            from pathlib import Path

            from futrisk.bench.world import Child, python_argv

            log = Path(tempfile.gettempdir()) / f"futrisk-bench-load-{os.getpid()}.log"
            self._child = Child("load-sampler", python_argv("load", "--pid", str(os.getpid()), "--interval",
                                                             str(self.interval_s)), log=log)
            self._child.recv(60, lambda m: bool(m.get("ready")))
        except Exception:
            if self._child is not None:
                self._child.kill()
            self._child = None
            self.mode = "thread (this process only)"
            psutil.cpu_percent(interval=None)
            self._me = psutil.Process()
            self._me.cpu_percent(interval=None)
            self._thread = threading.Thread(target=self._run, name="bench-load-sampler", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        child = self._child
        if child is not None:
            self._child = None
            try:
                child.send({"cmd": "stop"})
                got = child.recv(30, lambda m: m.get("id") == "load")
                self.system, self.own = list(got["system"]), list(got["own"])
            except Exception:
                pass
            child.stop(10.0)
            with contextlib.suppress(OSError):
                child.log_path.unlink()
            return
        self._stop.set()
        if self._thread is not None:
            self._thread.join(5)
            self._sample()
            self._thread = None

    def _sample(self) -> None:  # fallback: machine + this process (no process enumeration)
        ncpu = psutil.cpu_count(logical=True) or 1
        self.system.append(psutil.cpu_percent(interval=None))
        own = self._me.cpu_percent(interval=None) if self._me is not None else 0.0
        self.own.append(min(100.0, own / ncpu))

    def _run(self) -> None:
        while not self._stop.wait(self.interval_s):
            self._sample()

    def summary(self) -> dict[str, Any]:
        n = min(len(self.system), len(self.own))
        if n == 0:
            return {"samples": 0, "mode": self.mode}
        sysv, own = self.system[:n], self.own[:n]
        bg = [max(0.0, s - o) for s, o in zip(sysv, own, strict=True)]
        bg_mean = sum(bg) / n
        return {
            "samples": n,
            "interval_s": self.interval_s,
            "mode": self.mode,
            "system_mean_pct": round(sum(sysv) / n, 1),
            "system_max_pct": round(max(sysv), 1),
            "own_mean_pct": round(sum(own) / n, 1),
            "background_mean_pct": round(bg_mean, 1),
            "background_max_pct": round(max(bg), 1),
            "loaded": bg_mean > LOADED_BACKGROUND_PCT,
        }


def baseline_load(seconds: float = 1.0) -> dict[str, Any]:
    """Machine CPU utilisation over ``seconds`` before any bench runs (the background load), then the scheduler
    jitter seen by one busy thread (:func:`jitter_probe`)."""
    per_cpu = psutil.cpu_percent(interval=seconds, percpu=True)
    total = sum(per_cpu) / max(1, len(per_cpu))
    busy = sum(1 for x in per_cpu if x > 50.0)
    return {"system_pct": round(total, 1), "busy_cpus": busy, "cpus": len(per_cpu),
            "loaded": total > LOADED_BACKGROUND_PCT, "jitter": jitter_probe(2 * seconds)}


def jitter_probe(seconds: float = 1.0) -> dict[str, Any]:
    """A thread reading the clock in a tight loop for ``seconds``: how often (and how long) it did not run.

    Gaps are time the OS gave the CPU to something else (other programs, interrupts). On a quiet machine they
    stay well below 1 ms; every latency tail of the benches is bounded below by them.
    """
    from futrisk.bench.core import now_ns

    gaps_1ms = gaps_5ms = 0
    worst = 0
    last = start = now_ns()
    end = start + int(seconds * 1e9)
    while last < end:
        t = now_ns()
        gap = t - last
        if gap > worst:
            worst = gap
        if gap > 1_000_000:
            gaps_1ms += 1
            if gap > 5_000_000:
                gaps_5ms += 1
        last = t
    span = (last - start) / 1e9
    return {"seconds": round(span, 2), "gaps_over_1ms": gaps_1ms, "gaps_over_5ms": gaps_5ms,
            "max_gap_ms": round(worst / 1e6, 3)}


__all__ = ["LOADED_BACKGROUND_PCT", "LoadSampler", "baseline_load", "jitter_probe", "machine_info", "power_plan"]
