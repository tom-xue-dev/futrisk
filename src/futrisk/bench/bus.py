"""``bus``: PUB/SUB throughput of one publisher / subscriber pair (target ≥ 100k msg/s).

The project's own classes on both ends, over TCP loopback, in two processes (like two services):

* publisher (this process): :class:`~futrisk.bus.publisher.PublisherThread` with the md-gateway send
  HWM; the bench thread calls ``publish_raw`` (LV seq, envelope encode, LV cache, queue) and the
  publisher thread sends - both share this process' GIL, as in a service;
* subscriber (child process): :class:`~futrisk.bus.subscriber.Subscriber` - ``recv_multipart`` +
  envelope decode + frame / proto / exact-topic checks for every message.

Payload: a real ``Tick`` (msgpack, ~250 bytes) on ``md.tick.{instrument}`` for 20 instruments.

1. **throughput** (primary): ``n`` messages as fast as the publisher can queue them; delivered rate =
   messages received / (last receipt - first publish), one timeline (QPC). Messages beyond the HWMs are
   dropped by PUB/SUB, so ``lost`` is reported next to the rate.
2. **latency**: one-way latency at a paced rate below saturation (timestamps in ``corr_id``; the
   subscriber drains one message at a time and stamps it after decoding).
3. **subscriber capacity**: 50k messages queued while the subscriber waits, then drained at full speed
   (the subscriber side alone; phase 1 is bounded by the slower end).
"""

from __future__ import annotations

import cProfile
import os
import time
from pathlib import Path
from typing import Any

import zmq

from futrisk.bench.core import BenchContext, BenchResult, Target, latency_ns, now_ns, profile_lines, scalar
from futrisk.bench.scenario import INSTRUMENTS, TD, free_port, product_of
from futrisk.bench.world import Child, bench_dir, check_clock, python_argv
from futrisk.bus.publisher import PublisherCore, PublisherThread
from futrisk.common.clock import MarketClock
from futrisk.common.config import load_default_config
from futrisk.common.ids import new_epoch
from futrisk.messages.base import encode, type_version
from futrisk.messages.enums import SourceId
from futrisk.messages.md import Tick

NAME = "bus"
TITLE = "Bus PUB/SUB throughput (one pair, real publisher/subscriber classes)"
TARGET = "≥ 100,000 msg/s"


def _payloads(n_inst: int = 20) -> list[tuple[str, bytes]]:
    products = load_default_config().products.products
    out = []
    for k, (inst, px) in enumerate(list(INSTRUMENTS.items())[:n_inst]):
        spec = products[product_of(inst)]
        tick_size, exch = float(spec.tick_size), spec.exchange
        t = Tick(instrument_id=inst, exchange=exch, trading_day=TD, action_day=TD, exch_ts_ns=1_790_000_000_000_000_000 + k,
                 last=px, volume=1000 + k, turnover=px * 10_000.0, open_interest=100_000, bid1=px - tick_size,
                 ask1=px + tick_size, bid_vol1=5, ask_vol1=7, upper_limit=px * 1.05, lower_limit=px * 0.95,
                 pre_settle=px, source=SourceId.FEED_A, recv_ns=1_790_000_000_000_000_000 + k)
        out.append((f"md.tick.{inst}", encode(t)))
    return out


def run(ctx: BenchContext) -> BenchResult:
    n = int(ctx.size("n", full=500_000, quick=100_000, tiny=2_000))
    lat_rate = float(ctx.size("latency_rate", full=20_000, quick=20_000, tiny=2_000))
    lat_s = float(ctx.size("latency_s", full=5.0, quick=1.5, tiny=0.3))
    res = BenchResult(name=NAME, title=TITLE, target=TARGET, quick=ctx.quick,
                      params={"messages": n, "latency_rate_per_s": lat_rate, "latency_seconds": lat_s,
                              "payload": "Tick (msgpack) in the bus envelope", "topics": 20,
                              "sndhwm": 100_000, "rcvhwm": 10_000, "transport": "tcp loopback, 2 processes"})
    with bench_dir(ctx, NAME) as tmp:
        return _run(ctx, res, tmp, n, lat_rate, lat_s)


def _run(ctx: BenchContext, res: BenchResult, tmp: Path, n: int, lat_rate: float, lat_s: float) -> BenchResult:
    endpoint = f"tcp://127.0.0.1:{free_port()}"
    zctx: zmq.Context[Any] = zmq.Context()
    core = PublisherCore("bench-pub", new_epoch(), MarketClock())
    pub = PublisherThread(zctx, endpoint, core, sndhwm=100_000, linger_ms=0, name="bench-pub")
    pub.start()
    payloads = _payloads()
    v = type_version("Tick")
    sub = Child("bus-sub", python_argv("bus-sub", "--endpoint", endpoint, "--publisher", "bench-pub",
                                       "--parent-pid", str(os.getpid())),
                log=tmp / "bus-sub.log")
    prof = cProfile.Profile() if ctx.profile else None
    try:
        ready = sub.recv(60, lambda m: bool(m.get("ready")))
        check_clock(sub.t_spawn_ns, int(ready["t_probe_start_ns"]), now_ns(), "bus subscriber")
        # slow joiner: warm-up messages until the subscription has reached the publisher
        deadline = now_ns() + 30_000_000_000
        k = 0
        while sub.try_recv(0.005, lambda m: bool(m.get("synced"))) is None:
            topic, data = payloads[k % len(payloads)]
            pub.publish_raw(topic, "Tick", v, data, corr_id="w")
            k += 1
            if now_ns() > deadline:
                raise RuntimeError("the subscriber never received a message")
        time.sleep(0.2)  # let the rest of the warm-up drain

        # ---------------------------------------------------------------- 1. throughput
        sub.send({"cmd": "measure", "id": 1, "n": n, "idle_s": 3.0, "stamped": False})
        sub.recv(30, lambda m: m.get("id") == 1 and bool(m.get("armed")))
        ctx.say(f"bus: publishing {n:,} messages")
        if prof is not None:
            prof.enable()
        t0 = now_ns()
        np_ = len(payloads)
        for i in range(n):
            topic, data = payloads[i % np_]
            pub.publish_raw(topic, "Tick", v, data)
            if i % 2048 == 2047 and pub.queue_size() > 50_000:
                while pub.queue_size() > 20_000:  # the sender thread is behind: bounded memory
                    time.sleep(0.0005)
        t_queued = now_ns()
        pub.flush(120.0)
        t_sent = now_ns()
        if prof is not None:
            prof.disable()
        r = sub.recv(180, lambda m: m.get("id") == 1 and "ok" in m)
        received = int(r["received"])
        t_last = int(r["t_last_ns"])
        delivered = received / ((t_last - t0) / 1e9) if received and t_last > t0 else 0.0
        res.add(scalar("delivered throughput (first publish -> last receipt)", delivered, "msg/s", n=received,
                       targets=[Target("value", ">=", 100_000, "msg/s")], primary=True,
                       note=f"{received:,} of {n:,} received, {n - received:,} lost (PUB/SUB drops beyond the HWMs)"))
        res.add(scalar("publisher send rate (handed to the socket)", n / ((t_sent - t0) / 1e9), "msg/s", n=n))
        res.add(scalar("publish_raw call rate (queued)", n / ((t_queued - t0) / 1e9), "msg/s", n=n))
        res.add(scalar("lost messages", n - received, "msgs", kind="count", n=n))
        res.extra["subscriber_stats"] = r.get("sub_stats")

        # ---------------------------------------------------------------- 2. latency (paced)
        n_lat = max(10, int(lat_rate * lat_s))
        sub.send({"cmd": "measure", "id": 2, "n": n_lat, "idle_s": 3.0, "stamped": True})
        sub.recv(30, lambda m: m.get("id") == 2 and bool(m.get("armed")))
        period = int(1e9 / lat_rate)
        start = now_ns()
        sent = 0
        while sent < n_lat:
            due = start + sent * period
            t = now_ns()
            if t < due:
                time.sleep(0.0002)
                continue
            topic, data = payloads[sent % np_]
            ts = now_ns()
            pub.publish_raw(topic, "Tick", v, data, corr_id=str(ts))
            sent += 1
        pub.flush(60.0)
        r2 = sub.recv(120, lambda m: m.get("id") == 2 and "ok" in m)
        lat = [int(x) for x in r2["lat_ns"]]
        res.add(latency_ns(f"one-way latency at {lat_rate:,.0f} msg/s (publish -> decoded)", lat,
                           note=f"{len(lat):,} of {n_lat:,} received; paced by schedule (~0.5 ms sleep granularity)"))

        # ---------------------------------------------------------------- 3. subscriber capacity (backlog)
        n_back = min(n, 50_000)  # fits the send HWM (100k): nothing is dropped while the subscriber waits
        sub.send({"cmd": "measure", "id": 3, "n": n_back, "idle_s": 3.0, "stamped": False, "wait_go": True,
                  "profile": ctx.profile})
        sub.recv(30, lambda m: m.get("id") == 3 and bool(m.get("armed")))
        for i in range(n_back):
            topic, data = payloads[i % np_]
            pub.publish_raw(topic, "Tick", v, data)
        pub.flush(120.0)
        time.sleep(0.3)
        sub.send({"cmd": "go"})
        r3 = sub.recv(180, lambda m: m.get("id") == 3 and "ok" in m)
        got = int(r3["received"])
        span = (int(r3["t_last_ns"]) - int(r3["t_start_ns"])) / 1e9  # from "go": the whole backlog was waiting
        res.add(scalar(f"subscriber drain rate (backlog of {n_back:,})", got / span if got > 1 and span > 0 else None,
                       "msg/s", n=got, note="recv + envelope decode + topic checks, one process"))
        if r3.get("profile"):
            res.profile += ["(subscriber process: drain of the backlog)", *r3["profile"]]
    finally:
        sub.stop(15.0, graceful=lambda: sub.send({"cmd": "stop", "id": 99}))
        pub.stop()
        zctx.term()
    if prof is not None:
        res.profile = ["(publisher process: publish_raw loop and publisher thread)", *profile_lines(prof), *res.profile]
    return res


__all__ = ["NAME", "TARGET", "TITLE", "run"]
