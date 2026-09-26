"""``order_rtt``: order round trip client -> risk-engine -> trade-gateway (QS-08: p99 ≤ 2 ms).

Processes on loopback: the bench process is the client (a DEALER with the ``strategy-sim`` identity, one
blocking request at a time, nothing else running in that process); the **real risk-engine** (production
entry, risk db ``synchronous=NORMAL`` as in 03 DB-03) owns the 50 bench accounts; the fake trade-gateway
(``fakes`` child) answers ``route.order`` at once with a RouteAck, records when it received the route, and
then reports the order ACCEPTED and FILLED through its outbox (``td.exec`` FACTs), like the real gateway.

Per order: ``t0`` before the request (envelope encode included) -> the OrderAck is back (the risk-engine
answers after the pre-trade pipeline, the ONE commit of its batch and the ``route.order`` send), and
``t0`` -> the trade-gateway's handler received the route (QPC timeline shared by the processes).
Orders are 1 lot, alternately opening and closing each position of the book (positions stay bounded),
priced at the pre-settlement. Paced (``gap_ms`` between orders, the fills of the previous order are
applied meanwhile) and back to back.
"""

from __future__ import annotations

import time
from typing import Any

from futrisk.bench.core import (
    BenchContext,
    BenchResult,
    Target,
    from_summary,
    gc_window,
    latency_ns,
    now_ns,
    scalar,
)
from futrisk.bench.scenario import INSTRUMENTS, RE_INSTANCE, WorldSpec, book, product_of
from futrisk.bench.world import BenchWorld, ChildError
from futrisk.common.ids import uuid7
from futrisk.messages.enums import Direction, Offset, OrderType, PosDirection
from futrisk.messages.orders import OrderRequest

NAME = "order_rtt"
TITLE = "Order round trip: client -> risk-engine (pre-trade, persist) -> trade-gateway"
TARGET = "p99 ≤ 2 ms"
SHFE = {"rb", "hc", "cu", "au"}


class OrderFlow:
    """1-lot orders cycling over every position: open, then close it again (``CLOSE_TODAY`` on SHFE)."""

    def __init__(self, spec: WorldSpec, *, qty: int = 1, products: set[str] | None = None) -> None:
        self.qty = qty
        self.positions = [(a, inst, d) for a, rows in book(spec).items() for inst, d, _q in rows
                          if products is None or product_of(inst) in products]
        if not self.positions:
            raise ValueError("no position to trade")
        self.k = 0

    def next(self) -> OrderRequest:
        n = len(self.positions)
        a, inst, d = self.positions[self.k % n]
        opening = (self.k // n) % 2 == 0
        self.k += 1
        long = d == PosDirection.LONG
        if opening:
            direction, offset = (Direction.BUY if long else Direction.SELL), Offset.OPEN
        else:
            direction = Direction.SELL if long else Direction.BUY
            offset = Offset.CLOSE_TODAY if product_of(inst) in SHFE else Offset.CLOSE
        return OrderRequest(client_order_id=uuid7(), account_id=a, instrument_id=inst, direction=direction,
                            offset=offset, order_type=OrderType.LIMIT, price=INSTRUMENTS[inst], qty=self.qty)


def send_orders(w: BenchWorld, flow: OrderFlow, n: int, gap_s: float) -> list[dict[str, Any]]:
    """``n`` sequential ``order.submit`` requests; per order: coid, t0, t1 (ns), ok, code."""
    bot = w.client("strategy-sim")
    out = []
    for _ in range(n):
        req = flow.next()
        t0 = now_ns()
        r = bot.request(RE_INSTANCE, "order.submit", req, timeout_ms=10_000, retries=0)
        t1 = now_ns()
        out.append({"coid": req.client_order_id, "t0": t0, "t1": t1, "ok": r.ok, "code": r.code})
        if gap_s > 0:
            time.sleep(gap_s)
    return out


def run(ctx: BenchContext) -> BenchResult:
    n = int(ctx.size("n", full=3000, quick=600, tiny=30))
    n_b2b = int(ctx.size("n_back_to_back", full=1000, quick=300, tiny=20))
    warmup = int(ctx.size("warmup", full=300, quick=100, tiny=10))
    gap_ms = float(ctx.size("gap_ms", full=2.0, quick=2.0, tiny=2.0))
    accounts = int(ctx.size("accounts", full=50, quick=50, tiny=10))
    hb_ms = ctx.heartbeat_ms or 1000
    spec = WorldSpec(accounts=accounts, heartbeat_ms=hb_ms, tg_auto_fill=True, tg_fill_parts=1)
    res = BenchResult(name=NAME, title=TITLE, target=TARGET, quick=ctx.quick,
                      params={"orders_paced": n, "gap_ms": gap_ms, "orders_back_to_back": n_b2b, "warmup": warmup,
                              "accounts": accounts, "risk_db_synchronous": "NORMAL (critical commits FULL)",
                              "trade_gateway": "fake: RouteAck at once, ACCEPTED + FILLED FACTs after the reply",
                              "heartbeat_ms": hb_ms})
    with BenchWorld(spec, base=ctx.tmp_root, name=NAME, keep=ctx.keep, echo=ctx.say) as w:
        w.start_fakes()
        w.start_re(profile="running" if ctx.profile else None)
        w.wait_running()
        assert w.fakes is not None
        flow = OrderFlow(spec)
        ctx.say(f"order_rtt: {warmup} warm-up orders, then {n:,} paced ({gap_ms:g} ms gap) and {n_b2b:,} back to back")
        send_orders(w, flow, warmup, gap_ms / 1000)
        w.fakes.call("record_routes", on=True, clear=True)
        paced = send_orders(w, flow, n, gap_ms / 1000)
        t_b0 = now_ns()
        b2b = send_orders(w, flow, n_b2b, 0.0)
        t_b1 = now_ns()
        time.sleep(0.5)
        routes = {k: int(v) for k, v in w.fakes.call("routes", timeout=60)["routes"].items()}
        tg = w.fakes.call("tg_stats")
        time.sleep(1.2)  # one heartbeat-fields refresh after the last order
        hb = w.status(timeout_ms=3000)
        doc = w.stop_re()
    if doc is None:
        raise ChildError("the risk-engine probe wrote no results")
    ok = [o for o in paced if o["ok"]]
    rejects: dict[str, int] = {}
    for o in paced + b2b:
        if not o["ok"]:
            rejects[o["code"]] = rejects.get(o["code"], 0) + 1
    res.add(latency_ns(f"order.submit round trip, client -> OrderAck (paced, {gap_ms:g} ms gap)",
                       [o["t1"] - o["t0"] for o in ok], targets=[Target("p99", "<=", 2, "ms")], primary=True,
                       note=f"{len(ok):,} accepted of {len(paced):,}"))
    to_tg = [routes[o["coid"]] - o["t0"] for o in ok if o["coid"] in routes]
    res.add(latency_ns("client -> trade-gateway received route.order (one way, paced)", to_tg,
                       targets=[Target("p99", "<=", 2, "ms")],
                       note="informative (the target is set on the round trip); the fake trade-gateway shares its "
                            "process and GIL with the other fakes (md publisher, clock, clearing)"))
    ok_b = [o for o in b2b if o["ok"]]
    res.add(latency_ns("order.submit round trip, back to back", [o["t1"] - o["t0"] for o in ok_b],
                       targets=[Target("p99", "<=", 2, "ms")], note=f"{len(ok_b):,} accepted of {len(b2b):,}"))
    res.add(scalar("back-to-back order rate (one client)", len(b2b) / ((t_b1 - t_b0) / 1e9), "orders/s", n=len(b2b)))
    if hb is not None:
        lat = hb.latency_us or {}
        for key, what in (("order_submit", "dispatch -> committed, routed, answered"), ("pretrade", "core.handle"),
                          ("persist", "risk.db commit"), ("batch", "one pump: commit + effects")):
            if key in lat:
                res.add(from_summary(f"heartbeat {key} ({what})", lat[key], note="risk-engine's own sliding window"))
        res.extra["heartbeat"] = {"latency_us": {k: {"p50": v.p50, "p99": v.p99, "max": v.max, "n": v.n}
                                                 for k, v in lat.items()},
                                  "rates": dict(hb.rates or {}), "queues": dict(hb.queues or {})}
    res.extra["rejects"] = rejects
    if paced:
        res.extra["risk_engine_gc"] = gc_window(doc.get("gc_events", []), paced[0]["t0"], (b2b or paced)[-1]["t1"])
    res.extra["trade_gateway"] = {k: v for k, v in tg.items() if k not in ("id", "ok")}
    if rejects:
        res.notes.append(f"rejected orders (excluded from the latencies): {rejects}")
    if doc.get("profile"):
        res.profile = ["(risk-engine process, from RUNNING to stop)", *doc["profile"]]
    return res


__all__ = ["NAME", "TARGET", "TITLE", "OrderFlow", "run", "send_orders"]
