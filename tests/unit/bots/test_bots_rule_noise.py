"""Normal-mode rule noise (01 §8.14, risk_rules.yaml): the bots' steady-state books against the real risk engine.

The default target books of all five accounts - sized exactly as the strategies size them - are
opened product by product in ``EngineCore`` and held for a minute (longer than every time-only
confirmation). Expected: no alert at all, except the documented ACC4 ``product_exposure`` WARN
(2 lots of IF and IC = ~0.88 of the 30% cap at the 0.55x leverage target, bots.yaml). The
concentration rule must not react to the transient N = 1 of a multi-product build (its
``min_gross_pct`` gate and time-only confirmation), while a book that really stays concentrated
is still flagged.
"""

from __future__ import annotations

import random
from collections.abc import Iterator

import pytest
from loguru import logger

from futrisk.client.positions import PositionTracker
from futrisk.client.pricing import ContractTable
from futrisk.common.config import FullConfig
from futrisk.messages.base import Msg
from futrisk.messages.enums import Direction, RiskEventKind
from futrisk.messages.risk import RiskEvent
from futrisk.sim.bots.calibration import STEP_NS, EngineHarness
from futrisk.sim.bots.config import load_bots_config
from futrisk.sim.bots.gates import Gates
from futrisk.sim.bots.strategies import (
    BreakoutParams,
    CounterTrendAdd,
    CounterTrendParams,
    IndexDirectional,
    IndexParams,
    SpreadParams,
    StrategyContext,
    TrendParams,
    _sized_lots,
)

BUY, SELL = Direction.BUY, Direction.SELL
Book = list[tuple[str, Direction, int]]


@pytest.fixture(autouse=True)
def _quiet_logs() -> Iterator[None]:
    logger.disable("futrisk")
    yield
    logger.enable("futrisk")


def prices(cfg: FullConfig) -> dict[str, float]:
    return {i: float(v.pre_settle) for i, v in cfg.sim.price_engine.instruments.items()}


def context(cfg: FullConfig, account: str, px: dict[str, float]) -> StrategyContext:
    contracts = ContractTable(cfg.products)
    ctx = StrategyContext(account, contracts, PositionTracker(contracts, "20260929"),
                          equity=float(cfg.accounts.accounts[account].initial_capital), gates=Gates(account),
                          rng=random.Random(1))
    ctx.trading_day = "20260929"
    return ctx


def steady_books(cfg: FullConfig) -> dict[str, Book]:
    """Every account's default target book at the sim's reference prices (long where a strategy may go either way)."""
    px = prices(cfg)
    bots = load_bots_config(env={})
    books: dict[str, Book] = {}
    b = BreakoutParams.model_validate(bots.params_for("ACC1"))
    ctx = context(cfg, "ACC1", px)
    books["ACC1"] = [(i, BUY, _sized_lots(ctx, i, px[i], b.notional_frac, b.cap_util)) for i in b.instruments]
    t = TrendParams.model_validate(bots.params_for("ACC2"))
    ctx = context(cfg, "ACC2", px)
    books["ACC2"] = [(i, BUY, _sized_lots(ctx, i, px[i], t.notional_frac, t.cap_util)) for i in t.instruments]
    c = CounterTrendParams.model_validate({**bots.params_for("ACC3"), "aggressive": True})
    ctx = context(cfg, "ACC3", px)
    acc3 = CounterTrendAdd(c, account_id="ACC3")
    from futrisk.client.testing import make_tick

    for k, i in enumerate(c.weights):
        tick = make_tick(i, px[i], ts_ns=1_790_637_000_000_000_000 + k, trading_day="20260929")
        ctx.update_tick(tick)
        acc3.on_tick(tick, ctx)
    books["ACC3"] = [(i, BUY, n) for i, n in acc3.targets(ctx).items()]
    x = IndexParams.model_validate(bots.params_for("ACC4"))
    ctx = context(cfg, "ACC4", px)
    for i in x.weights:
        ctx.update_tick(make_tick(i, px[i], ts_ns=1_790_637_000_000_000_000, trading_day="20260929"))
    lots = IndexDirectional(x, account_id="ACC4")._size(ctx, x.leverage_max)  # the strongest signal
    books["ACC4"] = [(i, BUY, n) for i, n in lots.items()]
    sp = SpreadParams.model_validate(bots.params_for("ACC5"))
    books["ACC5"] = [(sp.near, BUY, sp.max_lots), (sp.far, SELL, sp.max_lots)]  # the widest spread book
    return books


def hold(cfg: FullConfig, shard: int, books: dict[str, Book], *, gap_steps: int = 1, hold_steps: int = 120) -> list[RiskEvent]:
    """Open ``books`` product by product on one shard, hold ``hold_steps`` x 500 ms; the alerts raised."""
    px = prices(cfg)
    capital = {a: float(cfg.accounts.accounts[a].initial_capital) for a in books}
    h = EngineHarness(cfg, capital, pre_settle=px, shard=shard)
    raised: list[RiskEvent] = []

    def on_message(topic: str, payload: Msg) -> None:
        if isinstance(payload, RiskEvent) and payload.kind == RiskEventKind.ALERT_RAISED:
            raised.append(payload)

    h.on_message = on_message
    instruments = sorted({i for legs in books.values() for i, _, _ in legs})

    def step(n: int) -> None:
        for _ in range(n):
            h.clock.advance(STEP_NS)
            for i in instruments:
                h.tick(i, px[i])
            h.evaluate()
            h.timer()

    step(2)
    for account, legs in books.items():
        for inst, direction, lots in legs:
            reply = h.open_position(account, inst, direction, lots, px[inst])
            assert reply.ok, (account, inst, reply.code, reply.message)
            step(gap_steps)
    step(hold_steps)
    return raised


def test_steady_state_books_raise_no_warn_except_acc4(default_config: FullConfig) -> None:
    books = steady_books(default_config)
    assert {i: n for i, _, n in books["ACC3"]} == {"rb2701": 125, "hc2701": 52, "i2701": 28}  # 0.78x equity
    assert {i: n for i, _, n in books["ACC4"]} == {"IF2612": 2, "IC2612": 2}
    alerts = hold(default_config, 0, {a: books[a] for a in ("ACC1", "ACC2")})
    alerts += hold(default_config, 1, {a: books[a] for a in ("ACC3", "ACC4", "ACC5")})
    raised = sorted({(e.account_id, f"{e.rule_id}/{e.level}") for e in alerts})
    assert set(raised) <= {("ACC4", "product_exposure/warn")}, raised  # the documented exception (bots.yaml)


def test_concentration_ignores_the_build_transient_but_flags_a_real_concentration(default_config: FullConfig) -> None:
    books = steady_books(default_config)
    acc3 = books["ACC3"]  # rb first: N = 1 for a moment, then 1.73 (rb+hc), then 2.65
    alerts = hold(default_config, 1, {"ACC3": acc3}, gap_steps=1, hold_steps=120)
    assert not [e for e in alerts if e.rule_id == "concentration"], [(e.rule_id, e.level) for e in alerts]
    # control: a book that stays at rb+hc (N = 1.73 < ACC3's warn line 2.0) is flagged after the 30 s confirmation
    alerts = hold(default_config, 1, {"ACC3": acc3[:2]}, hold_steps=80)
    assert [(e.rule_id, e.level) for e in alerts if e.rule_id == "concentration"] == [("concentration", "warn")]
