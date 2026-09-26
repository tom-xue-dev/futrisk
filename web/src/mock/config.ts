/**
 * Static data of the mock backend, mirroring config/sim.yaml, products.yaml,
 * accounts.yaml, risk_rules.yaml, services.yaml and the trading calendar.
 * Values are the demo defaults of 01 §7.1; positions are sized so that ACC3 (the
 * breaker demo) starts with only its black-sector warning and every other account sits
 * in the state the real rules give it (e.g. ACC1's four products keep N < 5: warn).
 */
import type { Exchange } from '@/types'

export type SessionKind = 'night2300' | 'night0100' | 'night0230' | 'index' | 'treasury'

export interface MockInstrument {
  id: string
  product: string
  exchange: Exchange
  sector: string
  multiplier: number
  tick: number
  limitPct: number
  preSettle: number
  volAnnual: number
  volumePerS: number
  openInterest: number
  /** company margin rate = exchange rate + broker add-on */
  marginRate: number
  fee: { type: 'rate' | 'per_lot'; open: number; closeYd: number; closeTodayMult: number }
  sessions: SessionKind
  /** SHFE same-product big-side margin (01 §8.15) */
  bigSide: boolean
}

export const INSTRUMENTS: MockInstrument[] = [
  { id: 'rb2701', product: 'rb', exchange: 'SHFE', sector: 'black', multiplier: 10, tick: 1, limitPct: 0.05, preSettle: 3120, volAnnual: 0.22, volumePerS: 40, openInterest: 1_800_000, marginRate: 0.08, fee: { type: 'rate', open: 0.0001, closeYd: 0.0001, closeTodayMult: 1 }, sessions: 'night2300', bigSide: true },
  { id: 'rb2705', product: 'rb', exchange: 'SHFE', sector: 'black', multiplier: 10, tick: 1, limitPct: 0.05, preSettle: 3150, volAnnual: 0.21, volumePerS: 12, openInterest: 600_000, marginRate: 0.08, fee: { type: 'rate', open: 0.0001, closeYd: 0.0001, closeTodayMult: 1 }, sessions: 'night2300', bigSide: true },
  { id: 'hc2701', product: 'hc', exchange: 'SHFE', sector: 'black', multiplier: 10, tick: 1, limitPct: 0.05, preSettle: 3260, volAnnual: 0.22, volumePerS: 15, openInterest: 900_000, marginRate: 0.08, fee: { type: 'rate', open: 0.0001, closeYd: 0.0001, closeTodayMult: 1 }, sessions: 'night2300', bigSide: true },
  { id: 'cu2611', product: 'cu', exchange: 'SHFE', sector: 'nonferrous', multiplier: 5, tick: 10, limitPct: 0.07, preSettle: 78000, volAnnual: 0.18, volumePerS: 6, openInterest: 180_000, marginRate: 0.12, fee: { type: 'rate', open: 0.00005, closeYd: 0.00005, closeTodayMult: 2 }, sessions: 'night0100', bigSide: true },
  { id: 'au2612', product: 'au', exchange: 'SHFE', sector: 'precious', multiplier: 1000, tick: 0.02, limitPct: 0.14, preSettle: 850, volAnnual: 0.16, volumePerS: 8, openInterest: 300_000, marginRate: 0.17, fee: { type: 'per_lot', open: 20, closeYd: 20, closeTodayMult: 0 }, sessions: 'night0230', bigSide: true },
  { id: 'i2701', product: 'i', exchange: 'DCE', sector: 'black', multiplier: 100, tick: 0.5, limitPct: 0.06, preSettle: 780, volAnnual: 0.28, volumePerS: 20, openInterest: 700_000, marginRate: 0.09, fee: { type: 'rate', open: 0.0001, closeYd: 0.0001, closeTodayMult: 1 }, sessions: 'night2300', bigSide: false },
  { id: 'j2701', product: 'j', exchange: 'DCE', sector: 'black', multiplier: 100, tick: 0.5, limitPct: 0.08, preSettle: 1650, volAnnual: 0.3, volumePerS: 4, openInterest: 60_000, marginRate: 0.13, fee: { type: 'rate', open: 0.0001, closeYd: 0.0001, closeTodayMult: 1.4 }, sessions: 'night2300', bigSide: false },
  { id: 'm2701', product: 'm', exchange: 'DCE', sector: 'agri', multiplier: 10, tick: 1, limitPct: 0.06, preSettle: 3000, volAnnual: 0.18, volumePerS: 25, openInterest: 1_500_000, marginRate: 0.08, fee: { type: 'per_lot', open: 1.5, closeYd: 1.5, closeTodayMult: 1 }, sessions: 'night2300', bigSide: false },
  { id: 'IF2612', product: 'IF', exchange: 'CFFEX', sector: 'equity_index', multiplier: 300, tick: 0.2, limitPct: 0.1, preSettle: 4414.8, volAnnual: 0.2, volumePerS: 3, openInterest: 90_000, marginRate: 0.12, fee: { type: 'rate', open: 0.000023, closeYd: 0.000023, closeTodayMult: 10 }, sessions: 'index', bigSide: false },
  { id: 'IC2612', product: 'IC', exchange: 'CFFEX', sector: 'equity_index', multiplier: 200, tick: 0.2, limitPct: 0.1, preSettle: 6500, volAnnual: 0.24, volumePerS: 2, openInterest: 80_000, marginRate: 0.12, fee: { type: 'rate', open: 0.000023, closeYd: 0.000023, closeTodayMult: 10 }, sessions: 'index', bigSide: false },
  { id: 'T2612', product: 'T', exchange: 'CFFEX', sector: 'rates', multiplier: 10000, tick: 0.005, limitPct: 0.02, preSettle: 108, volAnnual: 0.03, volumePerS: 2, openInterest: 150_000, marginRate: 0.02, fee: { type: 'per_lot', open: 3, closeYd: 3, closeTodayMult: 0 }, sessions: 'treasury', bigSide: false },
]

export const INSTRUMENT_MAP: Record<string, MockInstrument> = Object.fromEntries(INSTRUMENTS.map((i) => [i.id, i]))

/** [start, end) minutes after midnight (Shanghai); night windows past midnight use +1440. */
export const SESSION_WINDOWS: Record<SessionKind, { day: [number, number][]; night: [number, number] | null }> = {
  night2300: { day: [[540, 615], [630, 690], [810, 900]], night: [1260, 1380] },
  night0100: { day: [[540, 615], [630, 690], [810, 900]], night: [1260, 1500] },
  night0230: { day: [[540, 615], [630, 690], [810, 900]], night: [1260, 1590] },
  index: { day: [[570, 690], [780, 900]], night: null },
  treasury: { day: [[570, 690], [780, 915]], night: null },
}

export interface MockLotSpec {
  instrument: string
  direction: 'LONG' | 'SHORT'
  yd: number
  /** yesterday lots' open price (FIFO cost); basis is pre_settle */
  ydOpen: number
}

/** Morning trade (today's lots) applied during the history simulation. */
export interface MockMorningTrade {
  at: string // "HH:MM:SS" market time
  instrument: string
  direction: 'BUY' | 'SELL'
  offset: 'OPEN' | 'CLOSE' | 'CLOSE_TODAY' | 'CLOSE_YESTERDAY'
  qty: number
}

export interface MockAccountSpec {
  id: string
  name: string
  shard: number
  initialCapital: number
  preBalance: number
  cumNetDeposit: number
  hwmEquity: number
  lots: MockLotSpec[]
  morning: MockMorningTrade[]
  /** settlement-history profile (daily mean / vol of returns, breaker days, cap on today's drawdown from the high-water mark) */
  history: { mu: number; sigma: number; breakerDays: Record<string, number>; depositDay?: { td: string; amount: number }; maxEndDd?: number }
  /** bot cadence (seconds between orders) */
  botEveryS: number
}

export const ACCOUNTS: MockAccountSpec[] = [
  {
    id: 'ACC1',
    name: '黑色系通道突破',
    shard: 0,
    initialCapital: 10_000_000,
    preBalance: 10_386_540.2,
    cumNetDeposit: 0,
    hwmEquity: 10_521_300,
    lots: [
      { instrument: 'rb2701', direction: 'SHORT', yd: 110, ydOpen: 3176 },
      { instrument: 'hc2701', direction: 'SHORT', yd: 60, ydOpen: 3302 },
      { instrument: 'i2701', direction: 'SHORT', yd: 25, ydOpen: 794.5 },
      { instrument: 'j2701', direction: 'LONG', yd: 5, ydOpen: 1631 },
    ],
    morning: [{ at: '09:36:12', instrument: 'rb2701', direction: 'SELL', offset: 'OPEN', qty: 20 }],
    history: { mu: 0.0005, sigma: 0.007, breakerDays: {} },
    botEveryS: 11,
  },
  {
    id: 'ACC2',
    name: '多品种趋势',
    shard: 0,
    initialCapital: 20_000_000,
    preBalance: 22_517_380.55,
    cumNetDeposit: 2_000_000,
    hwmEquity: 20_902_500,
    lots: [
      { instrument: 'rb2701', direction: 'LONG', yd: 60, ydOpen: 3088 },
      { instrument: 'cu2611', direction: 'LONG', yd: 10, ydOpen: 77120 },
      { instrument: 'au2612', direction: 'LONG', yd: 3, ydOpen: 838.4 },
      { instrument: 'm2701', direction: 'SHORT', yd: 150, ydOpen: 3036 },
      { instrument: 'IF2612', direction: 'LONG', yd: 2, ydOpen: 4380.2 },
      { instrument: 'T2612', direction: 'LONG', yd: 6, ydOpen: 107.62 },
    ],
    morning: [{ at: '09:41:03', instrument: 'IF2612', direction: 'BUY', offset: 'OPEN', qty: 1 }],
    history: { mu: 0.0004, sigma: 0.005, breakerDays: {}, depositDay: { td: '20260826', amount: 2_000_000 } },
    botEveryS: 13,
  },
  {
    id: 'ACC3',
    name: '日内逆势加仓',
    shard: 1,
    initialCapital: 10_000_000,
    preBalance: 9_874_200,
    cumNetDeposit: 0,
    hwmEquity: 10_210_000,
    // accounts.yaml products [rb, hc, i]; the base book plus the morning dip-buys below is
    // ~40% rb / 40% hc / 30% i of equity (black sector ~1.1x: sector warn, below the 1.2 block)
    lots: [
      { instrument: 'rb2701', direction: 'LONG', yd: 97, ydOpen: 3151 },
      { instrument: 'hc2701', direction: 'LONG', yd: 96, ydOpen: 3284 },
      { instrument: 'i2701', direction: 'LONG', yd: 30, ydOpen: 786 },
    ],
    morning: [
      { at: '09:18:40', instrument: 'rb2701', direction: 'BUY', offset: 'OPEN', qty: 10 },
      { at: '09:22:05', instrument: 'hc2701', direction: 'BUY', offset: 'OPEN', qty: 10 },
      { at: '09:43:17', instrument: 'rb2701', direction: 'BUY', offset: 'OPEN', qty: 10 },
      { at: '09:47:52', instrument: 'i2701', direction: 'BUY', offset: 'OPEN', qty: 8 },
      { at: '09:58:30', instrument: 'hc2701', direction: 'BUY', offset: 'OPEN', qty: 15 },
      { at: '10:06:11', instrument: 'rb2701', direction: 'BUY', offset: 'OPEN', qty: 10 },
    ],
    history: { mu: 0.0002, sigma: 0.01, breakerDays: { '20260812': 1, '20260903': 1 }, maxEndDd: 0.03 },
    botEveryS: 4,
  },
  {
    id: 'ACC4',
    name: '股指杠杆方向',
    shard: 1,
    initialCapital: 10_000_000,
    preBalance: 10_612_900,
    cumNetDeposit: 0,
    hwmEquity: 10_752_000,
    lots: [
      { instrument: 'IF2612', direction: 'LONG', yd: 1, ydOpen: 4371.6 },
      { instrument: 'IC2612', direction: 'LONG', yd: 2, ydOpen: 6452.4 },
    ],
    morning: [{ at: '09:31:26', instrument: 'IF2612', direction: 'BUY', offset: 'OPEN', qty: 1 }],
    history: { mu: 0.0003, sigma: 0.011, breakerDays: { '20260716': 1 } },
    botEveryS: 17,
  },
  {
    id: 'ACC5',
    name: '螺纹跨期套利',
    shard: 1,
    initialCapital: 5_000_000,
    preBalance: 5_061_250,
    cumNetDeposit: 0,
    hwmEquity: 5_083_000,
    lots: [
      { instrument: 'rb2701', direction: 'LONG', yd: 100, ydOpen: 3104 },
      { instrument: 'rb2705', direction: 'SHORT', yd: 100, ydOpen: 3139 },
    ],
    morning: [],
    history: { mu: 0.00015, sigma: 0.0025, breakerDays: {} },
    botEveryS: 15,
  },
]

/** config/risk_rules.yaml as GET /risk/rules returns it (generated, see rules.gen.ts). */
export { RULES } from './rules.gen'

/** `restartDelayS`: a killed instance is back after ~that long (risk shards reload their state: slower) */
export interface MockServiceSpec {
  instance: string
  service: string
  external?: boolean
  restartDelayS: number
  queues: Record<string, number>
  rates: Record<string, number>
  latency: Record<string, [number, number]>
}

export const SERVICES: MockServiceSpec[] = [
  { instance: 'supervisor', service: 'supervisor', restartDelayS: 0, queues: { db_writer: 0 }, rates: { heartbeats_in: 7 }, latency: {} },
  { instance: 'sim-exchange', service: 'sim-exchange', external: true, restartDelayS: 3, queues: { feed_a_out: 2 }, rates: { feed_a_msgs: 22, feed_b_polls: 1 }, latency: { match: [180, 900] } },
  { instance: 'md-gateway', service: 'md-gateway', restartDelayS: 6, queues: { normalize: 3, recorder: 12 }, rates: { ticks_in: 44, ticks_live: 22, rejects: 0 }, latency: { normalize: [32, 140], arbitrate: [18, 95] } },
  { instance: 'trade-gateway', service: 'trade-gateway', restartDelayS: 3, queues: { fills_out: 0, db_writer: 1 }, rates: { routes: 0.6, fills: 0.4 }, latency: { route: [210, 780] } },
  { instance: 'clearing-worker', service: 'clearing-worker', restartDelayS: 3, queues: { jobs: 0 }, rates: { fills_copied: 0.4 }, latency: {} },
  { instance: 'risk-engine-0', service: 'risk-engine', restartDelayS: 10, queues: { marks: 1, commands: 0, db_writer: 2 }, rates: { marks: 14, evaluations: 4, orders: 0.2 }, latency: { tick_to_decision: [185, 920], pretrade: [41, 205] } },
  { instance: 'risk-engine-1', service: 'risk-engine', restartDelayS: 10, queues: { marks: 1, commands: 0, db_writer: 3 }, rates: { marks: 17, evaluations: 6, orders: 0.5 }, latency: { tick_to_decision: [196, 980], pretrade: [44, 230] } },
  { instance: 'api-gateway', service: 'api-gateway', restartDelayS: 3, queues: { ws_clients: 1, notify: 0 }, rates: { ws_frames: 38, rest: 1.2 }, latency: { rest: [900, 4200] } },
  { instance: 'strategy-sim', service: 'strategy-sim', external: true, restartDelayS: 3, queues: {}, rates: { orders: 0.8 }, latency: {} },
]

/** Trading days (config/calendar/trading_days.csv) before and after the demo day 20260928. */
export const PAST_TRADING_DAYS = '20260701,20260702,20260703,20260706,20260707,20260708,20260709,20260710,20260713,20260714,20260715,20260716,20260717,20260720,20260721,20260722,20260723,20260724,20260727,20260728,20260729,20260730,20260731,20260803,20260804,20260805,20260806,20260807,20260810,20260811,20260812,20260813,20260814,20260817,20260818,20260819,20260820,20260821,20260824,20260825,20260826,20260827,20260828,20260831,20260901,20260902,20260903,20260904,20260907,20260908,20260909,20260910,20260911,20260914,20260915,20260916,20260917,20260918,20260921,20260922,20260923,20260924'.split(',')
export const NEXT_TRADING_DAYS = ['20260928', '20260929', '20260930', '20261008', '20261009', '20261012', '20261013', '20261014']

export const DEMO_TRADING_DAY = '20260928'
/** Mock market clock starts right after the 10:15-10:30 commodity break (the breach lands near 10:32, as in 01 §8.14's report example). */
export const DEMO_START = { y: 2026, mo: 9, d: 28, h: 10, mi: 30, s: 5 }
export const EOD_START_MIN = 15 * 60 + 30
export const PRE_CLOSE_MIN = 14 * 60 + 58

export const SECTOR_OF_PRODUCT: Record<string, string> = Object.fromEntries(INSTRUMENTS.map((i) => [i.product, i.sector]))
export const LIMIT_OF_PRODUCT: Record<string, number> = Object.fromEntries(INSTRUMENTS.map((i) => [i.product, i.limitPct]))
