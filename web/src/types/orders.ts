/**
 * Order, execution and broker payloads mirrored from `src/futrisk/messages/orders.py`
 * (02 §7.4-§7.5). Optional (`?`) = has a Python default (omitted by `omit_defaults`).
 */
import type {
  BrokerConnState,
  BrokerOrderStatus,
  Direction,
  Exchange,
  Offset,
  OrderSource,
  OrderStatus,
  OrderType,
  PosDirection,
  TimeInForce,
  TradeOrigin,
} from './enums'

/** `order.submit` request; `POST /orders` body (client_order_id is mandatory there). */
export interface OrderRequest {
  client_order_id: string
  account_id: string
  instrument_id: string
  direction: Direction
  offset: Offset
  order_type?: OrderType
  tif?: TimeInForce
  price?: number | null
  qty: number
  source?: OrderSource
  strategy_tag?: string | null
  actor?: string | null
  sent_ns?: number
}

export interface OrderAck {
  client_order_id: string
  accepted: boolean
  status: OrderStatus
  reject_code?: string | null
  reject_detail?: Record<string, unknown> | null
  risk_check_us?: number
  trading_day?: string
}

export interface FillRef {
  trade_id: string
  price: number
  qty: number
  trade_ts_ns: number
}

/** `oms.order.{account_id}` payload (FACT); also `risk.query.orders` items. */
export interface OrderUpdate {
  client_order_id: string
  account_id: string
  instrument_id: string
  exchange: Exchange
  direction: Direction
  offset: Offset
  order_type: OrderType
  tif: TimeInForce
  price?: number | null
  qty: number
  status: OrderStatus
  filled_qty?: number
  avg_fill_price?: number | null
  last_fill?: FillRef | null
  reject_code?: string | null
  reject_detail?: Record<string, unknown> | null
  broker_order_ref?: string | null
  source: OrderSource
  trading_day: string
  created_ns: number
  updated_ns: number
  reason?: string | null
}

export interface CancelAllAck {
  count: number
}

export interface BrokerOrderUpdate {
  client_order_id?: string | null
  origin?: TradeOrigin
  account_id: string
  instrument_id: string
  broker_order_ref: string
  order_sys_id?: string | null
  status: BrokerOrderStatus
  filled_qty?: number
  remaining_qty?: number
  broker_code?: string | null
  broker_msg?: string | null
  broker_seq: number
  trading_day: string
  updated_ns: number
}

/** `td.exec.{account_id}` trade (FACT); also `risk.query.fills` items. */
export interface Trade {
  trading_day: string
  exchange: Exchange
  trade_id: string
  direction: Direction
  account_id: string
  instrument_id: string
  client_order_id?: string | null
  origin?: TradeOrigin
  broker_order_ref: string
  order_sys_id?: string | null
  offset: Offset
  price: number
  qty: number
  trade_ts_ns: number
  broker_seq?: number | null
  trade_seq?: number
  received_ns?: number
}

export interface AccountDigest {
  account_id: string
  count: number
  qty_sum: number
  max_trade_seq: number
  sha256: string
}

export interface BrokerPosition {
  account_id: string
  instrument_id: string
  direction: PosDirection
  td_qty: number
  yd_qty: number
}

export interface BrokerStatus {
  broker: string
  state: BrokerConnState
  trading_day?: string | null
  session_id?: string | null
  front_id?: number | null
  last_broker_seq?: number
  pending_orders?: number
  since_ns: number
}
