"""Performance benchmarks of the design targets (01 §8.12, §10.2 QS-07 / QS-08 / QS-11; plan §10.4).

``futrisk bench all|bus|ingest|tick2decision|order_rtt|pretrade|replay|recovery [--quick] [--out FILE] [--json]``

==============  ===========================================================================  ==================
bench           what is measured                                                             design target
==============  ===========================================================================  ==================
bus             PUB/SUB throughput, one pair, project publisher/subscriber, 2 processes      ≥ 100k msg/s
ingest          md-gateway Feed A frame -> decode/normalise/validate/arbitrate -> publish    ≥ 10k msg/s
pretrade        EngineCore pre-trade check (``check_order``), demo book                      p50 ≤ 50 µs, p99 ≤ 250 µs
replay          backtest replay through the EngineCore (``futrisk.backtest.bench``)          ≥ 100k ticks/s
tick2decision   md publisher process -> real risk-engine process, 1k ticks/s, 50 × 20        p99 ≤ 3 ms
order_rtt       client -> real risk-engine -> trade-gateway accepted (round trip)            p99 ≤ 2 ms
recovery        risk-engine cold start with 10k fills in risk.db until RUNNING               < 3 s
==============  ===========================================================================  ==================

Modules: :mod:`.core` (result model, statistics, stopwatch), :mod:`.machine` (machine info, load
sampling), :mod:`.scenario` (the 50 × 20 book and its runtime home), :mod:`.fakes` / :mod:`.probe` /
:mod:`.rehooks` / :mod:`.world` (the multi-process world), one module per bench, :mod:`.runner`.
This package is a development tool: it is never imported by the services.
"""

NAMES = ("bus", "ingest", "pretrade", "replay", "tick2decision", "order_rtt", "recovery")

__all__ = ["NAMES"]
