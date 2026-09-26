"""Broker adapters of trade-gateway (02 §9.4, §10): interface, SimBroker client, CTP placeholder.

Importing this package registers every adapter (``sim``, ``ctp``) with
:func:`~futrisk.services.trade_gateway.broker.base.register_broker`.
"""

from futrisk.services.trade_gateway.broker.base import (
    ActionError,
    BrokerAdapter,
    BrokerCallbacks,
    BrokerReport,
    BrokerSession,
    BrokerUnavailable,
    InsertError,
    InvalidReport,
    OrderReport,
    PositionReport,
    QueryDone,
    QueryResult,
    TradeReport,
    available_brokers,
    broker_factory,
    make_broker_order_ref,
    parse_broker_order_ref,
    register_broker,
)
from futrisk.services.trade_gateway.broker.ctp import CtpBrokerAdapter
from futrisk.services.trade_gateway.broker.sim import AdapterStats, SimBrokerAdapter

__all__ = [
    "ActionError",
    "AdapterStats",
    "BrokerAdapter",
    "BrokerCallbacks",
    "BrokerReport",
    "BrokerSession",
    "BrokerUnavailable",
    "CtpBrokerAdapter",
    "InsertError",
    "InvalidReport",
    "OrderReport",
    "PositionReport",
    "QueryDone",
    "QueryResult",
    "SimBrokerAdapter",
    "TradeReport",
    "available_brokers",
    "broker_factory",
    "make_broker_order_ref",
    "parse_broker_order_ref",
    "register_broker",
]
