"""Dummy child process for the supervisor tests (run as a script by the test argv factory).

Plays a bus service (``sys.heartbeat.{instance}`` on a PUB socket, ``ctl.shutdown`` on a
ROUTER) or an external component (``GET /v1/health``, or just a live process). The
behaviour of each incarnation comes from ``--plan``: comma-separated steps, one per start
(the last one repeats); the incarnation index is kept in the ``--counter`` file.

=============  ===============================================================
step           behaviour
=============  ===============================================================
``ok``         heartbeat forever; exit 0 on ``ctl.shutdown`` or CTRL_BREAK
``exit:T:C``   heartbeat for T seconds, then exit with code C
``crash:C``    exit with code C immediately (never live)
``silent``     never heartbeat, stay alive
``hang:T``     heartbeat for T seconds, then stop heartbeating (stay alive)
``lag:T``      heartbeat; after T seconds report ``loop_lag_ms`` = 10**7
``stubborn``   heartbeat, ignore ``ctl.shutdown`` and CTRL_BREAK
``http:PORT``  serve ``GET /v1/health`` on PORT (no bus)
``proc``       stay alive, nothing else
=============  ===============================================================

A ``+pool`` suffix (e.g. ``ok+pool``, ``stubborn+pool``) first starts a grandchild that does
not watch this process (like a process-pool worker; it only exits when the test process is
gone, or after 120 s) and traces ``grandchild <pid>``.

Every notable event is appended to ``--trace`` as ``<wall s> <instance> <event> <pid>``;
``--env-trace NAME`` (repeatable) records ``env NAME=<value or ->`` at start. The process
exits (code 5) when ``--parent-pid`` disappears, so a failed test leaves no orphans behind.
"""

from __future__ import annotations

import argparse
import http.server
import os
import signal
import subprocess
import sys
import threading
import time
import uuid

import psutil
import zmq

from futrisk.messages.base import encode
from futrisk.messages.enums import ServiceState
from futrisk.messages.envelope import Envelope, Reply, decode_envelope, encode_envelope, make_envelope
from futrisk.messages.system import Heartbeat


def parse() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--instance", required=True)
    p.add_argument("--plan", default="ok")
    p.add_argument("--counter", required=True)
    p.add_argument("--trace", default=None)
    p.add_argument("--pub", default=None)
    p.add_argument("--router", default=None)
    p.add_argument("--hb-ms", type=int, default=100)
    p.add_argument("--parent-pid", type=int, default=None)
    p.add_argument("--env-trace", action="append", default=[])
    return p.parse_args()


class Child:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.instance = args.instance
        self.pid = os.getpid()
        self.epoch = uuid.uuid4().hex
        self.broken = threading.Event()
        steps = [s.strip() for s in args.plan.split(",") if s.strip()] or ["ok"]
        n = self._incarnation()
        self.step = steps[min(n, len(steps) - 1)]
        self.pool = self.step.endswith("+pool")
        self.kind, _, rest = self.step.removesuffix("+pool").partition(":")
        self.params = rest.split(":") if rest else []
        self.parent = psutil.Process(args.parent_pid) if args.parent_pid else None

    def _incarnation(self) -> int:
        path = self.args.counter
        try:
            with open(path, encoding="utf-8") as f:
                n = int(f.read().strip() or 0)
        except (OSError, ValueError):
            n = 0
        with open(path, "w", encoding="utf-8") as f:
            f.write(str(n + 1))
        return n

    def trace(self, event: str) -> None:
        if self.args.trace:
            with open(self.args.trace, "a", encoding="utf-8") as f:
                f.write(f"{time.time():.3f} {self.instance} {event} {self.pid}\n")

    def parent_gone(self) -> bool:
        if self.parent is None:
            return False
        try:
            return not self.parent.is_running()
        except psutil.Error:
            return True

    def on_break(self, signum: int, _frame: object) -> None:
        self.trace("break")
        if self.kind != "stubborn":
            self.broken.set()

    # ----------------------------------------------------------------- modes
    def run(self) -> int:
        for name in ("SIGBREAK", "SIGTERM"):
            sig = getattr(signal, name, None)
            if sig is not None:
                signal.signal(sig, self.on_break)
        self.trace(f"start {self.step}")
        for name in self.args.env_trace:
            self.trace(f"env {name}={os.environ.get(name, '-')}")
        if self.pool:
            self.spawn_grandchild()
        if self.kind == "crash":
            code = int(self.params[0]) if self.params else 1
            self.trace(f"exit {code}")
            return code
        if self.kind == "http":
            return self.run_http(int(self.params[0]))
        if self.kind == "proc":
            return self.idle()
        return self.run_bus()

    def spawn_grandchild(self) -> None:
        code = (
            "import sys, time, psutil\n"
            "watch = psutil.Process(int(sys.argv[1]))\n"
            "end = time.monotonic() + 120\n"
            "while time.monotonic() < end and watch.is_running():\n"
            "    time.sleep(0.2)\n"
        )
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
        watch = self.args.parent_pid or os.getpid()
        proc = subprocess.Popen([sys.executable, "-c", code, str(watch)], creationflags=flags, stdin=subprocess.DEVNULL)
        self.trace(f"grandchild {proc.pid}")

    def idle(self) -> int:
        while not self.broken.is_set():
            if self.parent_gone():
                return 5
            time.sleep(0.05)
        self.trace("exit 0")
        return 0

    def run_http(self, port: int) -> int:
        child = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - http.server API
                body = b'{"status": "ok"}' if self.path.startswith("/v1/health") else b"{}"
                self.send_response(200 if self.path.startswith("/v1/health") else 404)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: object) -> None:
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        child.trace("http up")
        code = self.idle()
        server.shutdown()
        return code

    def run_bus(self) -> int:
        args = self.args
        ctx = zmq.Context()
        pub = ctx.socket(zmq.PUB)
        pub.setsockopt(zmq.LINGER, 0)
        pub.bind(args.pub)
        router = ctx.socket(zmq.ROUTER)
        router.setsockopt(zmq.LINGER, 500)
        router.bind(args.router)
        topic = f"sys.heartbeat.{self.instance}"
        started = time.time_ns()
        t0 = time.monotonic()
        interval = args.hb_ms / 1000
        next_beat = t0
        seq = 0
        code: int | None = None
        try:
            while code is None:
                now = time.monotonic()
                elapsed = now - t0
                if self.parent_gone():
                    code = 5
                    break
                if self.broken.is_set():
                    code = 0
                    break
                if self.kind == "exit" and elapsed >= float(self.params[0]):
                    code = int(self.params[1]) if len(self.params) > 1 else 1
                    break
                beating = self.kind != "silent" and not (self.kind == "hang" and elapsed >= float(self.params[0]))
                if beating and now >= next_beat:
                    lag = 10**7 if (self.kind == "lag" and elapsed >= float(self.params[0])) else 0
                    seq += 1
                    hb = Heartbeat(
                        instance=self.instance,
                        service=self.instance,
                        state=ServiceState.RUNNING,
                        pid=self.pid,
                        epoch=self.epoch,
                        app_version="test",
                        started_ns=started,
                        loop_lag_ms=lag,
                    )
                    env = make_envelope(topic, hb, src=self.instance, epoch=self.epoch, ts_ns=time.time_ns(), seq=seq)
                    pub.send_multipart([topic.encode(), encode_envelope(env)])
                    next_beat += interval
                    if next_beat < now:
                        next_beat = now + interval
                if router.poll(20):
                    frames = router.recv_multipart()
                    env_in = decode_envelope(frames[-1])
                    if env_in.topic == "ctl.shutdown":
                        self.trace("shutdown-request")
                        reply = Envelope(
                            topic="ctl.shutdown.reply",
                            type="Reply",
                            src=self.instance,
                            epoch=self.epoch,
                            ts_ns=time.time_ns(),
                            corr_id=env_in.corr_id,
                            payload=encode(Reply(ok=True)),
                        )
                        router.send_multipart([frames[0], encode_envelope(reply)])
                        if self.kind != "stubborn":
                            time.sleep(0.05)
                            code = 0
        finally:
            pub.close(0)
            router.close()
            ctx.term()
        self.trace(f"exit {code}")
        return int(code or 0)


if __name__ == "__main__":
    sys.exit(Child(parse()).run())
