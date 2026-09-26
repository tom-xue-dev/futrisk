"""Latency sanity of the shell over loopback (01 §1.2 / §8.12 design goals): order.submit round trip and tick -> evaluation.

The fakes run in this test process (they share the GIL with the shell), so the numbers are pessimistic; the
subprocess test measures the round trip against a separate process too. Orders are filled at once, so
the book holds few resting orders (the pre-trade check is O(resting orders)). Numbers
are printed (``pytest -s``) and written to ``<tmp>/latency.json``; the bounds are loose to avoid flakiness.
"""

from __future__ import annotations

import json
import statistics
import time
from typing import Any

import pytest

from futrisk.messages.enums import Direction, Offset, OrderType
from futrisk.messages.orders import OrderRequest

from .re_support import request, uuid7, wait_for

pytestmark = pytest.mark.timeout(120)


def summary(xs: list[float]) -> dict[str, float]:
    data = sorted(xs)
    n = len(data)
    return {"p50": round(data[n // 2], 1), "p99": round(data[min(n - 1, int(0.99 * (n - 1)))], 1),
            "max": round(data[-1], 1), "mean": round(statistics.fmean(data), 1), "n": n}


def submit_rtts(c: Any, target: str, account: str, n: int, gap_s: float) -> list[float]:
    """``n`` one-lot opening orders (filled by the fake broker); round trips in microseconds (a retried request,
    as a production client would retry it, counts with its retries)."""
    out: list[float] = []
    for _ in range(n):
        req = OrderRequest(client_order_id=uuid7(), account_id=account, instrument_id="rb2701", direction=Direction.BUY,
                           offset=Offset.OPEN, order_type=OrderType.LIMIT, price=3100.0, qty=1, sent_ns=time.time_ns())
        t0 = time.perf_counter_ns()
        r = request(c, target, "order.submit", req)
        out.append((time.perf_counter_ns() - t0) / 1000)
        assert r.ok, r
        if gap_s:
            time.sleep(gap_s)
    return out


def test_order_submit_round_trip_and_tick_to_evaluation(env_factory: Any, tmp_path: Any) -> None:
    env = env_factory(pretrade={"throttle_rate_per_s": 100_000.0, "throttle_burst": 100_000})
    env.tg.auto_fill = True
    sh = env.shell()
    submit_rtts(env.bot, "risk-engine-1", "ACC5", 40, 0.005)  # warm-up
    for w in sh.svc.lat.values():
        w._samples.clear()
    spaced = submit_rtts(env.bot, "risk-engine-1", "ACC5", 200, 0.005)
    shell_spaced = {k: w.summary() for k, w in sh.svc.lat.items()}
    back_to_back = submit_rtts(env.bot, "risk-engine-1", "ACC5", 200, 0.0)
    before = sh.svc.lat["tick_eval"].count
    for i in range(300):
        env.md.tick("rb2701", 3100.0 + (i % 7))
        time.sleep(0.002)  # pacing of the tick stream, not a wait for a result
    wait_for(lambda: sh.svc.lat["tick_eval"].count - before >= 50, what="tick evaluations")
    result: dict[str, Any] = {
        "order_submit_rtt_us_spaced": summary(spaced),
        "order_submit_rtt_us_back_to_back": summary(back_to_back),
        "shell_us_spaced": {k: (v.p50, v.p99, v.max, v.n) if v is not None else None for k, v in shell_spaced.items()},
        "tick_eval_us": (lambda s: (s.p50, s.p99, s.max, s.n) if s is not None else None)(sh.svc.lat["tick_eval"].summary()),
    }
    (tmp_path / "latency.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print("\nrisk-engine latency:", json.dumps(result))
    assert result["order_submit_rtt_us_spaced"]["p50"] < 50_000
    assert result["tick_eval_us"] is not None and result["tick_eval_us"][0] < 50_000
