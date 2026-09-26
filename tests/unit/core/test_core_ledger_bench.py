"""Micro-benchmark of the IntradayLedger hot path (01 §8.12, QS-08: pre-trade p50 ≤ 50 µs).

Not a strict performance gate: the bounds only catch pathological regressions
(e.g. a return to per-call recomputation). The measured µs are printed (run with
``-s``) and recorded as test properties (``--junitxml``).
"""

from __future__ import annotations

import time
from collections.abc import Callable

import pytest
from tests.golden.builders import TradeFactory

from futrisk.common.config import FullConfig
from futrisk.core import InstrumentRegistry, IntradayLedger, RestingOrder, seed_sod
from futrisk.messages.enums import Direction, Offset

TD = "20260928"
PRICES = {
    "rb2610": 3050.0, "rb2701": 3110.0, "rb2705": 3120.0, "hc2701": 3300.0, "cu2611": 78000.0,
    "au2612": 850.0, "i2701": 780.0, "j2701": 2000.0, "m2701": 3000.0, "IF2612": 4400.0,
}


def _per_call_us(fn: Callable[[], object], reps: int) -> float:
    fn()
    best = float("inf")
    for _ in range(3):  # 取三轮中最好的一轮，减少调度抖动
        t0 = time.perf_counter()
        for _ in range(reps):
            fn()
        best = min(best, (time.perf_counter() - t0) / reps * 1e6)
    return best


def test_hot_path_microbenchmark(default_config: FullConfig, record_property: Callable[[str, object], None]) -> None:
    reg = InstrumentRegistry(default_config.products, default_config.calendar)
    sod = seed_sod(TD, {"ACC1": "100000000"}, pre_settle={i: str(p) for i, p in PRICES.items()})
    led = IntradayLedger(reg, TD, sod.accounts[0], sod.pre_settle)
    tf = TradeFactory()
    n = 0
    for inst_id, px in PRICES.items():
        for side in (Direction.BUY, Direction.SELL):
            n += 1
            led.apply_fill(tf("ACC1", inst_id, side, Offset.OPEN, px, 3, TD, 1_790_000_000_000_000_000 + n))
    insts = list(PRICES)
    for k in range(10):
        inst_id = insts[k]
        led.add_order(f"o{k}", RestingOrder(inst_id, Direction.BUY if k % 2 else Direction.SELL, Offset.OPEN, 2, PRICES[inst_id]))
    assert len(led.held()) == 20

    tick = [0]

    def set_mark_and_funds() -> None:
        tick[0] += 1
        inst_id = insts[tick[0] % len(insts)]
        led.set_mark(inst_id, PRICES[inst_id] + tick[0] % 7)
        led.funds()

    probe = RestingOrder("rb2701", Direction.BUY, Offset.OPEN, 1, 3110.0)
    close_probe = RestingOrder("rb2701", Direction.SELL, Offset.CLOSE_TODAY, 1, 3110.0)
    orders = [RestingOrder(insts[k], Direction.SELL, Offset.OPEN, 2, PRICES[insts[k]]) for k in range(10)]
    results = {
        "set_mark_funds_us": _per_call_us(set_mark_and_funds, 2000),
        "funds_us": _per_call_us(led.funds, 2000),
        "order_cost_open_us": _per_call_us(lambda: led.order_cost(probe), 2000),
        "order_cost_close_us": _per_call_us(lambda: led.order_cost(close_probe), 2000),
        "frozen_for_orders_10_us": _per_call_us(lambda: led.frozen_for_orders(orders), 500),
        "position_views_20_us": _per_call_us(led.position_views, 200),
    }
    for name, us in results.items():
        record_property(name, round(us, 2))
        print(f"{name:<26} {us:8.2f} us")
    # 宽松上限：只防回退到逐次全量重算（旧实现约 90 / 250 / 260 µs）
    assert results["set_mark_funds_us"] < 50
    assert results["order_cost_open_us"] < 50
    assert results["frozen_for_orders_10_us"] < 200
    assert results["position_views_20_us"] < 1000


@pytest.mark.parametrize("n_positions", [2, 20])
def test_funds_cost_does_not_depend_on_ticks(default_config: FullConfig, n_positions: int) -> None:
    """A tick revalues one instrument: set_mark must not scale with the number of positions."""
    reg = InstrumentRegistry(default_config.products, default_config.calendar)
    sod = seed_sod(TD, {"ACC1": "100000000"}, pre_settle={i: str(p) for i, p in PRICES.items()})
    led = IntradayLedger(reg, TD, sod.accounts[0], sod.pre_settle)
    tf = TradeFactory()
    for n, inst_id in enumerate(list(PRICES)[: n_positions // 2 or 1]):
        for side in (Direction.BUY, Direction.SELL):
            led.apply_fill(tf("ACC1", inst_id, side, Offset.OPEN, PRICES[inst_id], 1, TD, 1_790_000_000_000_000_000 + 2 * n + (side == Direction.SELL)))
    us = _per_call_us(lambda: led.set_mark("rb2610", 3051.0), 2000)
    assert us < 25
