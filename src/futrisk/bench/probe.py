"""Child processes of the multi-process benches: ``python -m futrisk.bench.probe <role> ...``.

* ``fakes`` - every fake peer of the risk-engine (:class:`futrisk.bench.fakes.Fakes`) in one process,
  driven by JSON commands on stdin (``burst``, ``routes``, ``tg_stats``, ``ping``, ``stop``);
* ``re`` - the real risk-engine through the production CLI (``futrisk svc risk-engine``) with the
  measurement hooks of :mod:`futrisk.bench.rehooks` installed first;
* ``bus-sub`` - a :class:`futrisk.bus.subscriber.Subscriber` that counts / timestamps what a publisher
  in the bench process sends (JSON commands ``measure``, ``stop``);
* ``load`` - CPU sampling of the machine and of the bench's process tree (in its own process: enumerating
  processes takes ~15 ms and would stall the measured thread of the bench process).

Protocol lines on stdout start with ``@@`` followed by one JSON object; anything else is log noise.
Every child exits when its stdin closes (the bench process went away) or its parent pid disappears.
"""

from __future__ import annotations

from futrisk.bench.core import now_ns

_T0 = now_ns()  # interpreter up, before the heavy imports

import argparse  # noqa: E402
import json  # noqa: E402
import os  # noqa: E402
import sys  # noqa: E402
import threading  # noqa: E402
import traceback  # noqa: E402
from pathlib import Path  # noqa: E402
from typing import Any  # noqa: E402

PREFIX = "@@"
_out_lock = threading.Lock()


def emit(obj: dict[str, Any]) -> None:
    line = PREFIX + json.dumps(obj, separators=(",", ":"))
    with _out_lock:
        sys.stdout.write(line + "\n")
        sys.stdout.flush()


def _watch_parent(pid: int | None) -> None:
    """Exit the process when the bench process is gone (its stdin pipe normally closes first)."""
    if not pid:
        return
    import psutil

    try:
        parent = psutil.Process(pid)
    except psutil.Error:
        os._exit(3)

    def loop() -> None:
        import time

        while True:
            time.sleep(1.0)
            try:
                if not parent.is_running():
                    os._exit(3)
            except psutil.Error:
                os._exit(3)

    threading.Thread(target=loop, name="parent-watch", daemon=True).start()


# ---------------------------------------------------------------------------------------- fakes
def run_fakes(args: argparse.Namespace) -> int:
    from futrisk.bench.fakes import Fakes
    from futrisk.bench.scenario import WorldSpec
    from futrisk.common.config import load_active_config
    from futrisk.common.paths import HomeLayout

    _watch_parent(args.parent_pid)
    spec = WorldSpec(**json.loads(args.spec))
    cfg = load_active_config(HomeLayout(Path(args.home)))
    fakes = Fakes(cfg, Path(args.work), spec, args.offset_ns, parent_pid=args.parent_pid).start()
    emit({"ready": True, "t_ns": now_ns(), "t_probe_start_ns": _T0, "pid": os.getpid()})
    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            req = json.loads(line)
            cmd = req.get("cmd")
            rid = req.get("id")
            try:
                if cmd == "stop":
                    emit({"id": rid, "ok": True})
                    break
                res = _fakes_command(fakes, cmd, req)
                emit({"id": rid, "ok": True, **res})
            except Exception as exc:  # report and keep serving
                emit({"id": rid, "ok": False, "error": f"{type(exc).__name__}: {exc}", "trace": traceback.format_exc()})
    finally:
        fakes.stop()
    return 0


def _fakes_command(fakes: Any, cmd: str, req: dict[str, Any]) -> dict[str, Any]:
    if cmd == "ping":
        return {"t_ns": now_ns(), "alive": fakes.alive()}
    if cmd == "burst":
        res: dict[str, Any] = fakes.md.burst(float(req["rate"]), int(req["n"]), str(req.get("tag", "m")))
        return res
    if cmd == "md_idle":
        fakes.md.idle_s = max(0.0, float(req["ms"]) / 1000.0)
        return {}
    if cmd == "record_routes":
        fakes.tg.record_routes = bool(req.get("on", True))
        if req.get("clear", True):
            fakes.tg.route_recv_ns.clear()
        return {}
    if cmd == "routes":
        return {"routes": dict(fakes.tg.route_recv_ns)}
    if cmd == "tg_stats":
        stats: dict[str, Any] = fakes.tg.stats()
        return stats
    if cmd == "tg_config":
        if "auto_fill" in req:
            fakes.tg.auto_fill = bool(req["auto_fill"])
        if "fill_parts" in req:
            fakes.tg.fill_parts = max(1, int(req["fill_parts"]))
        return {}
    raise ValueError(f"unknown command {cmd!r}")


# ---------------------------------------------------------------------------------------- risk-engine
def run_re(args: argparse.Namespace) -> int:
    from futrisk.bench import rehooks

    rehooks.install(Path(args.out), ticks=args.ticks, profile=args.profile, t_probe_start_ns=_T0)
    argv = ["futrisk", "svc", "risk-engine", "--shard", str(args.shard), "--home", args.home]
    if args.parent_pid:
        argv += ["--parent-pid", str(args.parent_pid)]
    sys.argv = argv
    from futrisk.main import main as futrisk_main

    try:
        futrisk_main()
    except SystemExit as exc:
        return int(exc.code or 0) if isinstance(exc.code, int | None) else 1
    return 0


# ---------------------------------------------------------------------------------------- bus subscriber
def run_bus_sub(args: argparse.Namespace) -> int:
    import zmq

    from futrisk.bus.subscriber import Subscriber

    _watch_parent(args.parent_pid)
    ctx = zmq.Context()
    sub = Subscriber(ctx, name="bench-sub")
    sub.connect(args.publisher, args.endpoint, [args.prefix])
    poller = zmq.Poller()
    poller.register(sub.socket, zmq.POLLIN)
    emit({"ready": True, "t_ns": now_ns(), "t_probe_start_ns": _T0, "pid": os.getpid()})
    stdin_q: list[dict[str, Any]] = []
    stdin_ev = threading.Event()
    closed = threading.Event()

    def read_stdin() -> None:
        for line in sys.stdin:
            line = line.strip()
            if line:
                stdin_q.append(json.loads(line))
                stdin_ev.set()
        closed.set()
        stdin_ev.set()

    threading.Thread(target=read_stdin, name="stdin", daemon=True).start()
    synced = False
    try:
        while not closed.is_set():
            # idle: drain warm-up messages, report the first one (the subscription has propagated)
            if poller.poll(20):
                for env in sub.drain(limit=10_000):
                    if not synced:
                        synced = True
                        emit({"synced": True, "corr": env.corr_id})
            if not stdin_q:
                continue
            req = stdin_q.pop(0)
            cmd = req.get("cmd")
            if cmd == "stop":
                emit({"id": req.get("id"), "ok": True})
                break
            if cmd == "measure":
                emit({"id": req.get("id"), "armed": True})
                if req.get("wait_go"):  # backlog mode: do not read until the publisher is done
                    while not closed.is_set() and not any(q.get("cmd") == "go" for q in stdin_q):
                        stdin_ev.wait(0.05)
                        stdin_ev.clear()
                    stdin_q[:] = [q for q in stdin_q if q.get("cmd") != "go"]
                emit({"id": req.get("id"), "ok": True, **_measure(sub, poller, req)})
    finally:
        sub.close()
        ctx.term()
    return 0


def _measure(sub: Any, poller: Any, req: dict[str, Any]) -> dict[str, Any]:
    """Count the next ``n`` non-warm-up messages (or until ``idle_s`` without any); stamped mode records the
    one-way latency ``now - int(corr_id)`` of every message (drained one by one)."""
    n = int(req["n"])
    idle_ms = int(float(req.get("idle_s", 2.0)) * 1000)
    stamped = bool(req.get("stamped", False))
    prof = None
    if req.get("profile"):
        import cProfile

        prof = cProfile.Profile()
        prof.enable()
    received = 0
    t_first = t_last = 0
    lat: list[int] = []
    stats0 = dict(sub.stats.__dict__)
    t_start = now_ns()
    while received < n:
        if not poller.poll(idle_ms):
            break
        batch = sub.drain(limit=1 if stamped else 5_000)
        t = now_ns()
        for env in batch:
            c = env.corr_id
            if c == "w":
                continue
            if received == 0:
                t_first = t
            received += 1
            if stamped and c:
                lat.append(t - int(c))
        if batch:
            t_last = t
    stats1 = dict(sub.stats.__dict__)
    out: dict[str, Any] = {"received": received, "t_start_ns": t_start, "t_first_ns": t_first, "t_last_ns": t_last,
                           "lat_ns": lat,
                           "sub_stats": {k: stats1[k] - stats0.get(k, 0) for k in stats1}}
    if prof is not None:
        from futrisk.bench.core import profile_lines

        prof.disable()
        out["profile"] = profile_lines(prof)
    return out


# ---------------------------------------------------------------------------------------- load sampler
def run_load(args: argparse.Namespace) -> int:
    """Every ``interval`` s: machine CPU % and the CPU % of ``--pid`` and its descendants (this sampler excluded),
    normalised to the machine; the lists are emitted when stdin says ``stop`` (or closes)."""
    import psutil

    ncpu = psutil.cpu_count(logical=True) or 1
    me = os.getpid()
    try:
        target = psutil.Process(args.pid)
    except psutil.Error:
        emit({"ready": True, "t_probe_start_ns": _T0, "pid": me, "error": f"no process {args.pid}"})
        return 1
    procs: dict[int, Any] = {}
    stop = threading.Event()

    def read_stdin() -> None:
        for line in sys.stdin:
            if line.strip():
                break
        stop.set()

    def tree_pct() -> float:
        try:
            tree = [target, *target.children(recursive=True)]
        except psutil.Error:
            tree = [target]
        total = 0.0
        alive: dict[int, Any] = {}
        for p in tree:
            if p.pid == me:
                continue
            known = procs.get(p.pid, p)
            try:
                total += known.cpu_percent(interval=None)  # the first reading of a new process is 0.0
                alive[p.pid] = known
            except psutil.Error:
                continue
        procs.clear()
        procs.update(alive)
        return min(100.0, total / ncpu)

    psutil.cpu_percent(interval=None)
    tree_pct()
    threading.Thread(target=read_stdin, name="stdin", daemon=True).start()
    emit({"ready": True, "t_probe_start_ns": _T0, "pid": me})
    system: list[float] = []
    own: list[float] = []
    while not stop.wait(args.interval):
        system.append(psutil.cpu_percent(interval=None))
        own.append(tree_pct())
    system.append(psutil.cpu_percent(interval=None))  # the tail since the last sample
    own.append(tree_pct())
    emit({"id": "load", "ok": True, "system": system, "own": own})
    return 0


# ---------------------------------------------------------------------------------------- main
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m futrisk.bench.probe")
    sub = parser.add_subparsers(dest="role", required=True)
    f = sub.add_parser("fakes")
    f.add_argument("--home", required=True)
    f.add_argument("--work", required=True)
    f.add_argument("--spec", required=True, help="WorldSpec as JSON")
    f.add_argument("--offset-ns", type=int, required=True)
    f.add_argument("--parent-pid", type=int)
    r = sub.add_parser("re")
    r.add_argument("--home", required=True)
    r.add_argument("--shard", type=int, default=1)
    r.add_argument("--parent-pid", type=int)
    r.add_argument("--out", required=True)
    r.add_argument("--ticks", action="store_true")
    r.add_argument("--profile", choices=["running", "startup"], default=None)
    b = sub.add_parser("bus-sub")
    b.add_argument("--endpoint", required=True)
    b.add_argument("--publisher", default="bench-pub")
    b.add_argument("--prefix", default="md.tick.")
    b.add_argument("--parent-pid", type=int)
    ld = sub.add_parser("load")
    ld.add_argument("--pid", type=int, required=True)
    ld.add_argument("--interval", type=float, default=0.5)
    args = parser.parse_args(argv)
    if args.role == "fakes":
        return run_fakes(args)
    if args.role == "re":
        return run_re(args)
    if args.role == "load":
        return run_load(args)
    return run_bus_sub(args)


if __name__ == "__main__":
    raise SystemExit(main())
