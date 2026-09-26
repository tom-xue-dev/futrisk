"""Strategy SDK (``futrisk.client``): order entry and market data for strategy processes.

Strategies (the ``strategy-sim`` bots, scripted end-to-end scenarios) are external
clients of the internal bus (01 §3.1, §5.1): they subscribe ``md.tick.*`` from
md-gateway and ``oms.order.{account}`` from the owning risk-engine shard, and send
``order.*`` commands to that shard - the only order entry (01 §6.2 R2, 02 §4.1, §6.3).

- :class:`FutriskClient` - async client (routing, retries, subscriptions, helpers);
- :class:`OrderBook` / :class:`TrackedOrder` - per-order status and fills;
- :class:`PositionTracker` - today/yesterday lots from fills (SHFE close offsets);
- :class:`RiskView` - the account's ``risk.state`` / ``risk.snapshot`` (state, restrictions, equity);
- :class:`ContractTable`, :func:`aggressive_price`, :func:`passive_price` - pricing.
"""

from futrisk.client.client import (
    DEFAULT_IDENTITY,
    BuildLeg,
    BuildResult,
    FutriskClient,
    OrderEvent,
    OrderResult,
    TickClock,
    coid_marker,
    has_marker,
    tagged_uuid7,
)
from futrisk.client.orders import ClientState, FillDelta, OrderBook, TrackedOrder
from futrisk.client.positions import (
    Leg,
    PendingCloses,
    PositionTracker,
    close_direction,
    open_direction,
    side_closed,
    side_opened,
)
from futrisk.client.pricing import (
    CAP_LOSS_FRAC,
    ContractInfo,
    ContractTable,
    aggressive_price,
    mid_price,
    passive_price,
    to_tick,
)
from futrisk.client.riskview import RiskUpdate, RiskView
from futrisk.client.routing import ShardRouter, UnknownAccountError

__all__ = [
    "CAP_LOSS_FRAC",
    "DEFAULT_IDENTITY",
    "BuildLeg",
    "BuildResult",
    "ClientState",
    "ContractInfo",
    "ContractTable",
    "FillDelta",
    "FutriskClient",
    "Leg",
    "OrderBook",
    "OrderEvent",
    "OrderResult",
    "PendingCloses",
    "PositionTracker",
    "RiskUpdate",
    "RiskView",
    "ShardRouter",
    "TickClock",
    "TrackedOrder",
    "UnknownAccountError",
    "aggressive_price",
    "close_direction",
    "coid_marker",
    "has_marker",
    "mid_price",
    "open_direction",
    "passive_price",
    "side_closed",
    "side_opened",
    "tagged_uuid7",
    "to_tick",
]
