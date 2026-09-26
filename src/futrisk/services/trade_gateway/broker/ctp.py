"""``CtpBrokerAdapter``: reserved interface for a real CTP counter (01 §5.3.3, 02 §10).

Not implemented in this release: every operation raises ``NotImplementedError``. The
docstrings record how each :class:`~futrisk.services.trade_gateway.broker.base.BrokerAdapter`
operation maps onto the CTP trader API (``CThostFtdcTraderApi`` /
``CThostFtdcTraderSpi``), so a future implementation only has to fill them in:

=====================================  ===============================================
adapter                                CTP
=====================================  ===============================================
``start`` / ``login``                  ``RegisterFront`` + ``Init`` →
                                       ``OnFrontConnected`` → ``ReqAuthenticate``
                                       (AppID / AuthCode) → ``ReqUserLogin`` →
                                       ``ReqSettlementInfoConfirm``; the login reply
                                       gives ``TradingDay``, ``FrontID``, ``SessionID``,
                                       ``MaxOrderRef``
liveness                               ``OnFrontDisconnected`` / ``OnHeartBeatWarning``
``insert_order``                       ``ReqOrderInsert`` (``CThostFtdcInputOrderField``:
                                       ``OrderRef``, ``CombOffsetFlag``,
                                       ``OrderPriceType``, ``TimeCondition``,
                                       ``VolumeCondition``); errors via
                                       ``OnRspOrderInsert`` / ``OnErrRtnOrderInsert``
``cancel``                             ``ReqOrderAction`` (``ActionFlag = Delete``) by
                                       ``FrontID + SessionID + OrderRef`` or
                                       ``ExchangeID + OrderSysID``; errors via
                                       ``OnRspOrderAction`` / ``OnErrRtnOrderAction``
``on_order`` / ``on_trade``            ``OnRtnOrder`` / ``OnRtnTrade``; the resume mode
                                       (``SubscribePrivateTopic(THOST_TERT_RESUME)``)
                                       replaces ``broker_seq`` cursors with CTP's
                                       private-flow sequence
``query_orders`` / ``query_trades``    ``ReqQryOrder`` / ``ReqQryTrade`` (+ ``OnRspQry*``
                                       pages with ``bIsLast``); CTP queries are limited
                                       to about one per second
``query_positions``                    ``ReqQryInvestorPosition``
                                       (``TodayPosition``, ``YdPosition``,
                                       ``PosiDirection``: '2' long, '3' short)
=====================================  ===============================================
"""

from __future__ import annotations

from typing import Any

from futrisk.messages.enums import BrokerConnState
from futrisk.messages.orders import RouteOrder
from futrisk.services.trade_gateway.broker.base import (
    BrokerCallbacks,
    BrokerSession,
    OrderReport,
    PositionReport,
    QueryDone,
    TradeReport,
    register_broker,
)

_MSG = "CtpBrokerAdapter is a reserved interface (01 §5.3.3); use broker 'sim'"


@register_broker("ctp")
class CtpBrokerAdapter:
    """Placeholder for a CTP trader-API adapter; see the module docstring for the mapping."""

    def __init__(self, front: str = "", *, broker_id: str = "", user_id: str = "", app_id: str = "",
                 auth_code: str = "", **_: Any) -> None:
        self.front = front
        self.broker_id = broker_id
        self.user_id = user_id
        self.app_id = app_id
        self.auth_code = auth_code

    @property
    def broker(self) -> str:
        return "ctp"

    @property
    def state(self) -> BrokerConnState:
        return BrokerConnState.DISCONNECTED

    @property
    def session(self) -> BrokerSession | None:
        return None

    def bind(self, callbacks: BrokerCallbacks) -> None:
        """Would forward ``CThostFtdcTraderSpi`` callbacks to ``callbacks`` on the business thread."""
        raise NotImplementedError(_MSG)

    def start(self) -> None:
        """``RegisterFront`` + ``Init``; then ``ReqAuthenticate`` → ``ReqUserLogin`` on ``OnFrontConnected``."""
        raise NotImplementedError(_MSG)

    def stop(self) -> None:
        """``Release`` the trader API."""
        raise NotImplementedError(_MSG)

    def login(self) -> None:
        """``ReqAuthenticate`` + ``ReqUserLogin`` + ``ReqSettlementInfoConfirm``."""
        raise NotImplementedError(_MSG)

    def insert_order(self, order: RouteOrder) -> str:
        """``ReqOrderInsert``; returns ``{FrontID}:{SessionID}:{OrderRef}``."""
        raise NotImplementedError(_MSG)

    def cancel(self, client_order_id: str, *, broker_order_ref: str | None = None, order_sys_id: str | None = None) -> None:
        """``ReqOrderAction`` with ``THOST_FTDC_AF_Delete``."""
        raise NotImplementedError(_MSG)

    def query_orders(self, trading_day: str, from_broker_seq: int, on_done: QueryDone[OrderReport]) -> None:
        """``ReqQryOrder`` (pages via ``OnRspQryOrder``)."""
        raise NotImplementedError(_MSG)

    def query_trades(self, trading_day: str, from_broker_seq: int, on_done: QueryDone[TradeReport]) -> None:
        """``ReqQryTrade`` (pages via ``OnRspQryTrade``)."""
        raise NotImplementedError(_MSG)

    def query_positions(
        self, trading_day: str, on_done: QueryDone[PositionReport], *, account_id: str | None = None
    ) -> None:
        """``ReqQryInvestorPosition`` (``TodayPosition`` / ``YdPosition`` per ``PosiDirection``)."""
        raise NotImplementedError(_MSG)

    def pump(self) -> int:
        """CTP delivers callbacks on its own thread; an implementation would queue them here."""
        raise NotImplementedError(_MSG)

    def poll_timers(self, now_mono_ns: int) -> None:
        raise NotImplementedError(_MSG)

    def next_timer_ns(self) -> int | None:
        raise NotImplementedError(_MSG)


__all__ = ["CtpBrokerAdapter"]
