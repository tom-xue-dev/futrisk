/**
 * Chinese UI labels and color tokens.
 *
 * Status colors are reserved for state (good / warning / serious / critical) and are
 * always shown together with an icon or text label, never as color alone.
 * Account series colors are a fixed categorical order (color follows the entity).
 */
import { reactive } from 'vue'
import type {
  AccountState,
  Action,
  BacktestState,
  EodPhase,
  LimitLock,
  Liveness,
  MdEventKind,
  MdQuality,
  OrderStatus,
  ProcessState,
  ReportType,
  RestrictionKind,
  RiskEventKind,
  RuleSmState,
  ServiceState,
  Severity,
  SourceState,
  TradingMode,
} from '@/types'

// ---------------------------------------------------------------- color tokens

export const STATUS_COLORS = {
  good: '#0ca30c',
  warning: '#fab219',
  serious: '#ec835a',
  critical: '#d03b3b',
  neutral: '#909399',
} as const
export type StatusTone = keyof typeof STATUS_COLORS

/** Fixed categorical order for accounts (validated palette, adjacent CVD ΔE >= 8). */
export const SERIES_COLORS = ['#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#4a3aa7', '#e34948']

const accountColorCache = new Map<string, string>()
/** Stable color per account id: ACC1 -> slot 1 ... (color follows the entity, never its rank). */
export function accountColor(accountId: string): string {
  const cached = accountColorCache.get(accountId)
  if (cached) return cached
  const m = /(\d+)$/.exec(accountId)
  let idx: number
  if (m) idx = (Number(m[1]) - 1) % SERIES_COLORS.length
  else {
    let h = 0
    for (const ch of accountId) h = (h * 31 + ch.charCodeAt(0)) >>> 0
    idx = h % SERIES_COLORS.length
  }
  const color = SERIES_COLORS[(idx + SERIES_COLORS.length) % SERIES_COLORS.length] ?? SERIES_COLORS[0]!
  accountColorCache.set(accountId, color)
  return color
}

/** Chart chrome (hairline grid, muted axis ink). */
export const CHART_INK = {
  primary: '#1f2329',
  secondary: '#52514e',
  muted: '#898781',
  grid: '#e8e7e1',
  axis: '#c3c2b7',
  surface: '#ffffff',
}

// ---------------------------------------------------------------- account / risk

export const ACCOUNT_STATE_LABELS: Record<AccountState, string> = {
  NORMAL: '正常',
  WARNING: '告警',
  RESTRICTED: '限制下单',
  HALTED: '熔断',
}

export const ACCOUNT_STATE_TONE: Record<AccountState, StatusTone> = {
  NORMAL: 'good',
  WARNING: 'warning',
  RESTRICTED: 'serious',
  HALTED: 'critical',
}

export const TRADING_MODE_LABELS: Record<TradingMode, string> = {
  OPEN: '交易中',
  EOD_FROZEN: 'EOD 冻结',
  PROVISIONAL: '临时快照',
}

export const SM_STATE_LABELS: Record<RuleSmState, string> = {
  OK: '正常',
  PENDING: '待确认',
  ACTIVE: '生效',
  REARMED: '重新布防',
}

export const ACTION_LABELS: Record<Action, string> = {
  ALERT: '告警',
  RESTRICT: '限制下单',
  CIRCUIT_BREAK: '熔断',
  LIQUIDATE: '减仓计划',
}

export const SEVERITY_LABELS: Record<Severity, string> = {
  INFO: '信息',
  WARNING: '警告',
  HIGH: '高',
  CRITICAL: '严重',
}

export const SEVERITY_TONE: Record<Severity, StatusTone> = {
  INFO: 'neutral',
  WARNING: 'warning',
  HIGH: 'serious',
  CRITICAL: 'critical',
}

export const RISK_EVENT_LABELS: Record<RiskEventKind, string> = {
  ALERT_RAISED: '告警触发',
  ALERT_CLEARED: '告警解除',
  ALERT_ACKED: '告警确认',
  RESET: '熔断重置',
  MANUAL_HALT: '人工熔断',
  KILL_SWITCH_ON: '全局禁开 开启',
  KILL_SWITCH_OFF: '全局禁开 解除',
  RETRO_BREACH: '补数区间越线',
  LIQUIDATION_PLAN: '减仓计划',
  RECON_BREAK: '对账异常',
  FILL_GAP: '成交缺口',
  EXTERNAL_FILL: '外部成交',
  EOD_FORCE_UNFREEZE: 'EOD 强制放行',
}

export const RESTRICTION_LABELS: Record<RestrictionKind, string> = {
  ORDER_SIZE_FACTOR: '单笔上限系数',
  NO_ADD_TO_LOSERS: '禁止亏损加仓',
  EXPOSURE_CAP_FACTOR: '敞口上限系数',
  BLOCK_OPEN: '禁开',
}

export const STATE_CAUSE_LABELS: Record<string, string> = {
  RULE: '规则',
  MANUAL: '人工',
  SYSTEM: '系统',
  RESET: '重置',
  DAYROLL: '换日',
  KILL_SWITCH: '全局禁开',
}

/** Rule ids of risk_rules.yaml (01 §8.14). Unknown ids are shown verbatim. */
export const RULE_LABELS: Record<string, string> = {
  daily_loss: '当日亏损',
  intraday_dd: '日内回撤',
  hwm_dd: '高水位回撤',
  nav_floor: '单位净值',
  risk_degree: '风险度',
  product_exposure: '单品种敞口',
  concentration: '有效品种数',
  sector_exposure: '板块敞口',
  leverage: '杠杆',
  limit_stress: '涨跌停压力',
  compliance: '合规',
  /** pseudo rule of a manual halt (risk.halt): a latched level `halt` on the account scope */
  manual_halt: '人工熔断',
}

export const LEVEL_LABELS: Record<string, string> = {
  warn: '告警',
  restrict: '限制',
  halt: '熔断',
  derisk: '减仓',
  block: '禁开',
  halve: '敞口减半',
}

export function ruleLabel(id: string | null | undefined): string {
  if (!id) return '—'
  return RULE_LABELS[id] ?? id
}

export function levelLabel(name: string | null | undefined): string {
  if (!name) return '—'
  return LEVEL_LABELS[name] ?? name
}

/** Human form of a scope key: "acct:ACC3/prod:rb" -> "rb"; "acct:ACC3" -> "账户". */
export function scopeLabel(scope: string | null | undefined): string {
  if (!scope) return '—'
  if (scope === 'firm') return '公司'
  const m = /\/(prod|sector|inst):(.+)$/.exec(scope)
  if (m) {
    const kind = m[1]
    const key = m[2] ?? ''
    if (kind === 'sector') return `板块 ${SECTOR_LABELS[key] ?? key}`
    return key
  }
  if (scope.startsWith('acct:')) return '账户'
  return scope
}

// ---------------------------------------------------------------- orders

export const ORDER_STATUS_LABELS: Record<OrderStatus, string> = {
  ROUTING: '路由中',
  ROUTED: '已路由',
  ACCEPTED: '已报',
  PARTIALLY_FILLED: '部成',
  FILLED: '全成',
  CANCEL_PENDING: '撤单中',
  CANCELED: '已撤',
  REJECTED: '拒单',
  BROKER_REJECTED: '柜台拒单',
  UNKNOWN: '未知',
}

export const DIRECTION_LABELS: Record<string, string> = { BUY: '买', SELL: '卖', LONG: '多', SHORT: '空' }
export const OFFSET_LABELS: Record<string, string> = {
  OPEN: '开仓',
  CLOSE: '平仓',
  CLOSE_TODAY: '平今',
  CLOSE_YESTERDAY: '平昨',
  FORCE_CLOSE: '强平',
}
export const ORDER_SOURCE_LABELS: Record<string, string> = {
  BOT: '机器人',
  MANUAL: '人工',
  API: 'API',
  LIQUIDATION: '减仓',
}

/** Reject codes (02 §11.2) and route/broker codes (§11.3). */
export const REJECT_CODE_LABELS: Record<string, string> = {
  REJECT_UNKNOWN_ACCOUNT: '账户不存在',
  REJECT_UNKNOWN_INSTRUMENT: '合约不存在',
  REJECT_NOT_TRADABLE: '合约不在交易期',
  REJECT_INVALID_ORDER: '字段组合不合法',
  REJECT_INVALID_QTY: '手数不合法',
  REJECT_QTY_EXCEEDS_MAX: '超过单笔上限',
  REJECT_PRICE_OFF_TICK: '价格不在最小变动价位网格上',
  REJECT_DUPLICATE_ORDER_ID: '重复的委托编号',
  REJECT_SYSTEM_NOT_READY: '系统未就绪（加载或停机中）',
  REJECT_EOD_IN_PROGRESS: 'EOD 冻结中',
  REJECT_SESSION_CLOSED: '不在交易时段',
  REJECT_MD_UNAVAILABLE: '行情质量不可用（只拦开仓）',
  REJECT_FIRM_KILL_SWITCH: '全局禁开已开启（只拦开仓）',
  REJECT_GATEWAY_UNAVAILABLE: '交易网关不可用',
  REJECT_PRICE_LIMIT: '价格超出涨跌停',
  REJECT_FAT_FINGER: '偏离标记价过大（胖手指）',
  REJECT_ACCOUNT_HALTED: '账户已熔断，禁止开仓',
  REJECT_ACCOUNT_BLOCKED: '账户禁开',
  REJECT_PRODUCT_BLOCKED: '品种 / 板块 / 合约禁开',
  REJECT_ORDER_SIZE_RESTRICTED: '超过打折后的单笔上限',
  REJECT_ADD_TO_LOSER: '禁止在亏损品种上加仓',
  REJECT_EXPOSURE_CAP: '超过打折后的敞口上限',
  REJECT_THROTTLED: '报单速率超限',
  REJECT_OPEN_VOLUME_LIMIT: '超过当日开仓手数限制',
  REJECT_CANCEL_LIMIT: '撤单次数达到上限',
  REJECT_LARGE_CANCEL_LIMIT: '大额撤单次数达到上限',
  REJECT_CLOSE_EXCEEDS_POSITION: '平仓数量超过可平量',
  REJECT_CLOSE_TODAY_EXCEEDS: '平今数量超过今仓可平量',
  REJECT_CLOSE_YD_EXCEEDS: '平昨数量超过昨仓可平量',
  REJECT_POSITION_LIMIT: '超过限仓',
  REJECT_INSUFFICIENT_FUNDS: '可用资金不足',
  REJECT_WHATIF_BREACH: 'what-if 预估将越过禁开阈值',
  REJECT_SELF_TRADE: '可能自成交',
  ROUTE_EXPIRED: '路由已过期',
  ROUTE_KILL_SWITCH: '全局禁开：交易网关拒绝开仓',
  ROUTE_LOST: '委托已判定丢失',
  BROKER_DISCONNECTED: '与柜台断开',
  BROKER_REJECT_CLOSE_TODAY: '柜台：平今仓位不足',
  BROKER_REJECT_CLOSE_YD: '柜台：平昨仓位不足',
  BROKER_REJECT_FUNDS: '柜台：资金不足',
  BROKER_REJECT_PRICE: '柜台：价格不合法',
  BROKER_ORDER_NOT_FOUND: '柜台：委托不存在',
  BROKER_ORDER_FINISHED: '柜台：委托已终结',
}

/** Market-data validation codes (md-gateway, 02 §11.3): reasons of rejected ticks. */
export const MD_CODE_LABELS: Record<string, string> = {
  MD_REJECT_TRADING_DAY: '交易日不符',
  MD_REJECT_OUT_OF_SESSION: '不在交易时段',
  MD_REJECT_PRICE_LIMIT: '价格超出涨跌停',
  MD_REJECT_OFF_TICK: '价格不在最小变动价位上',
  MD_REJECT_NON_MONOTONIC: '成交量 / 时间倒退',
  MD_REJECT_PRICE_WITHOUT_TRADE: '无成交却变价',
  MD_REJECT_MALFORMED: '报文格式错误',
  MD_REJECT_FUTURE_TS: '时间戳超前',
  MD_REJECT_INTEGRITY: '与另一可信源同成交量不同价',
}

/** Generic Reply codes (02 §11.1) for error toasts. */
export const ERROR_CODE_LABELS: Record<string, string> = {
  OK: '成功',
  BAD_REQUEST: '请求不合法',
  UNAUTHORIZED: '未认证（检查 API key）',
  FORBIDDEN: '权限不足',
  NOT_FOUND: '对象不存在',
  CONFLICT: '状态冲突',
  NOT_OWNER: '账户不属于该分片',
  PRECONDITION_FAILED: '前置条件不满足',
  TIMEOUT: '超时',
  UNAVAILABLE: '依赖不可用',
  BUSY: '服务繁忙',
  RISK_BUSY: '风控繁忙',
  VERSION_UNSUPPORTED: '版本不支持',
  INTERNAL: '内部错误',
}

export function codeLabel(code: string | null | undefined): string {
  if (!code) return ''
  return REJECT_CODE_LABELS[code] ?? ERROR_CODE_LABELS[code] ?? MD_CODE_LABELS[code] ?? ''
}

// ---------------------------------------------------------------- market data

export const MD_QUALITY_LABELS: Record<MdQuality, string> = { OK: '正常', STALE: '陈旧', UNKNOWN: '未知' }
export const MD_QUALITY_TONE: Record<MdQuality, StatusTone> = { OK: 'good', STALE: 'warning', UNKNOWN: 'critical' }

export const SOURCE_STATE_LABELS: Record<SourceState, string> = { TRUSTED: '可信', PROBATION: '观察', EXCLUDED: '排除' }
export const SOURCE_STATE_TONE: Record<SourceState, StatusTone> = {
  TRUSTED: 'good',
  PROBATION: 'warning',
  EXCLUDED: 'critical',
}

export const SOURCE_LABELS: Record<string, string> = { FEED_A: 'Feed A（推送）', FEED_B: 'Feed B（轮询）', SINA: '新浪' }
export const SOURCE_SHORT: Record<string, string> = { FEED_A: 'Feed A', FEED_B: 'Feed B', SINA: '新浪' }

export const LIMIT_LOCK_LABELS: Record<LimitLock, string> = { NONE: '—', UP: '涨停封板', DOWN: '跌停封板' }

export const MD_EVENT_LABELS: Record<MdEventKind, string> = {
  SOURCE_STATE: '源状态变化',
  QUALITY_CHANGE: '质量变化',
  GAP_OPEN: '缺口打开',
  GAP_FILLED: '缺口补齐',
  GAP_UNRECOVERABLE: '缺口不可恢复',
  MD_DOWN: '行情中断',
  MD_RECOVERED: '行情恢复',
  MD_QUIET: '行情静默',
  MD_INTEGRITY: '交叉校验不一致',
  BAD_TICK: '坏点',
}

export const MD_EVENT_TONE: Partial<Record<MdEventKind, StatusTone>> = {
  GAP_OPEN: 'warning',
  GAP_FILLED: 'good',
  GAP_UNRECOVERABLE: 'critical',
  MD_DOWN: 'critical',
  MD_RECOVERED: 'good',
  MD_INTEGRITY: 'critical',
  BAD_TICK: 'serious',
}

export const EXCHANGE_LABELS: Record<string, string> = { SHFE: '上期所', DCE: '大商所', CFFEX: '中金所', INE: '能源中心', CZCE: '郑商所', GFEX: '广期所' }

export const SECTOR_LABELS: Record<string, string> = {
  black: '黑色',
  nonferrous: '有色',
  precious: '贵金属',
  agri: '农产品',
  equity_index: '股指',
  rates: '利率',
}

export const PRODUCT_LABELS: Record<string, string> = {
  rb: '螺纹钢',
  hc: '热轧卷板',
  cu: '铜',
  au: '黄金',
  i: '铁矿石',
  j: '焦炭',
  m: '豆粕',
  IF: '沪深300',
  IH: '上证50',
  IC: '中证500',
  IM: '中证1000',
  T: '10年国债',
}

/** Product code of an instrument id: "rb2701" -> "rb", "IF2612" -> "IF". */
export function productOf(instrumentId: string): string {
  const m = /^([A-Za-z]+)/.exec(instrumentId)
  return m ? (m[1] ?? instrumentId) : instrumentId
}

/**
 * Display names of the accounts. The gateway adds `name` (accounts.yaml) to
 * `GET /accounts[/id]` and to the WS `accounts` items; the accounts store writes them
 * here through `setAccountName`. The defaults are the demo accounts' names, shown until
 * the first answer arrives; unknown accounts show only their id.
 */
export const ACCOUNT_NAMES: Record<string, string> = reactive({
  ACC1: '黑色系通道突破',
  ACC2: '多品种趋势',
  ACC3: '日内逆势加仓',
  ACC4: '股指杠杆方向',
  ACC5: '螺纹跨期套利',
})

export function setAccountName(id: string, name: string | null | undefined): void {
  if (name && ACCOUNT_NAMES[id] !== name) ACCOUNT_NAMES[id] = name
}

// ---------------------------------------------------------------- system / eod

export const SERVICE_STATE_LABELS: Record<ServiceState, string> = {
  STARTING: '启动中',
  RUNNING: '运行',
  DEGRADED: '降级',
  STOPPING: '停止中',
}

export const PROCESS_STATE_LABELS: Record<ProcessState, string> = {
  STARTING: '启动中',
  RUNNING: '运行',
  EXITED: '已退出',
  RESTARTING: '重启中',
  STOPPING: '停止中',
  STOPPED: '已停止',
  FAILED: '失败',
}

/** The gateway's view of a peer (heartbeat age vs `peer_timeout_missed`). */
export const LIVENESS_LABELS: Record<Liveness, string> = { UP: '在线', DOWN: '离线', UNKNOWN: '未知' }
export const LIVENESS_TONE: Record<Liveness, StatusTone> = { UP: 'good', DOWN: 'critical', UNKNOWN: 'neutral' }

export const SERVICE_LABELS: Record<string, string> = {
  supervisor: '进程守护',
  'sim-exchange': '虚拟交易所',
  'md-gateway': '行情网关',
  'trade-gateway': '交易网关',
  'clearing-worker': '清算 / 报表',
  'risk-engine-0': '风控分片 0（coordinator）',
  'risk-engine-1': '风控分片 1',
  'api-gateway': 'API 网关',
  'strategy-sim': '策略机器人',
}

export const SERVICE_REASON_LABELS: Record<string, string> = {
  STARTUP: '启动',
  SHUTDOWN: '停机',
  EXITED: '进程退出',
  HEARTBEAT_LOST: '心跳丢失',
  LOOP_HANG: '主循环卡死',
  KILLED_BY_DEMO: '演示 kill',
  MANUAL: '人工',
  GAVE_UP: '多次失败后放弃（待人工启动）',
  SUPERVISOR_RESTART: 'supervisor 重启后收尾',
}

/** EOD WARN kinds (02 §8.4): a warning never changes the run's phase. */
export const EOD_WARNING_LABELS: Record<string, string> = {
  LATE_FACT: '结算后到达的成交 / 出入金',
  OFFICIAL_PRICE_DIFF: '官方结算价与所用价格不同',
  PUSH_INCOMPLETE: '日切推送未全部确认',
  PRE_SETTLE_MISMATCH: '昨结算价与 SOD 不一致',
  EOD_WARNING: 'EOD 警告',
}

export const EOD_PHASE_LABELS: Record<EodPhase, string> = {
  BEGIN: '开始',
  FROZEN: '冻结',
  TRADES_RECONCILED: '成交核对',
  PRICES_FIXED: '结算价确定',
  SETTLED: '结算完成',
  ROLLED: '换日',
  REPORTED: '报表生成',
  DONE: '完成',
  FAILED: '失败',
}

export const REPORT_TYPE_LABELS: Record<ReportType, string> = {
  RISK_DAILY_HTML: '风控日报',
  STATEMENT_TXT: '结算单（TXT）',
  STATEMENT_CSV: '结算单（CSV）',
  SETTLEMENT_JSON: '结算数据（JSON）',
  BACKTEST_HTML: '回测报告',
}

export const BACKTEST_STATE_LABELS: Record<BacktestState, string> = {
  QUEUED: '排队',
  RUNNING: '运行中',
  DONE: '完成',
  FAILED: '失败',
}

/** Fault types of `POST /demo/faults` (02 §9.5). */
export interface FaultParamDef {
  key: string
  label: string
  default: number | boolean | string
  min?: number
  max?: number
  step?: number
  /** fixed choices of a string parameter */
  options?: string[]
  /** a string parameter picked from the instrument / account list */
  kind?: 'instrument' | 'account'
}

export interface FaultTypeDef {
  value: string
  label: string
  /** allowed targets; '' = no target (all feeds / the exchange) */
  targets: string[]
  params: FaultParamDef[]
}

/** Sim-exchange fault types and their parameter rules (sim/exchange/faults.py `_validate`). */
export const FAULT_TYPES: FaultTypeDef[] = [
  {
    value: 'FEED_DISCONNECT',
    label: '行情源断线',
    targets: ['feed_a', 'feed_b'],
    params: [
      { key: 'refuse_reconnect_s', label: '拒绝重连（秒）', default: 20, min: 0, step: 5 },
      { key: 'mode', label: '方式', default: 'close', options: ['close', 'silent'] },
    ],
  },
  { value: 'FEED_GAP', label: '序号缺口（丢包）', targets: ['feed_a'], params: [{ key: 'drop_count', label: '丢弃条数', default: 40, min: 1, step: 10 }] },
  { value: 'FEED_DUPLICATE', label: '重复推送', targets: ['feed_a', 'feed_b'], params: [{ key: 'ratio', label: '重复比例', default: 0.3, min: 0, max: 1, step: 0.05 }] },
  { value: 'FEED_LATENCY', label: '推送延迟', targets: ['feed_a', 'feed_b'], params: [{ key: 'ms', label: '延迟（ms）', default: 4000, min: 0, step: 500 }] },
  { value: 'FEED_REORDER', label: '乱序', targets: ['feed_a', 'feed_b'], params: [{ key: 'window', label: '乱序窗口', default: 8, min: 2, step: 1 }] },
  { value: 'FEED_FREEZE', label: '合约冻结（不再推送）', targets: ['', 'feed_a', 'feed_b'], params: [{ key: 'instrument', label: '合约', default: 'rb2701', kind: 'instrument' }] },
  {
    value: 'BAD_TICK',
    label: '坏点',
    targets: ['', 'feed_a', 'feed_b'],
    params: [
      { key: 'instrument', label: '合约', default: 'rb2701', kind: 'instrument' },
      { key: 'offset_pct', label: '偏离比例', default: 0.03, min: -0.99, max: 10, step: 0.01 },
      { key: 'with_volume', label: '带成交量', default: false },
    ],
  },
  {
    value: 'MALFORMED',
    label: '格式错误',
    targets: ['feed_b'],
    params: [
      { key: 'ratio', label: '错误比例', default: 0.2, min: 0, max: 1, step: 0.05 },
      { key: 'mode', label: '方式', default: 'body', options: ['body', 'record'] },
    ],
  },
  { value: 'SIM_RESTART', label: '交易所重启', targets: [], params: [] },
  { value: 'BROKER_DISCONNECT', label: '柜台断线', targets: [], params: [{ key: 'seconds', label: '断线（秒）', default: 15, min: 1, step: 5 }] },
  {
    value: 'BROKER_FORCE_CLOSE',
    label: '柜台强平',
    targets: [],
    params: [
      { key: 'account_id', label: '账户', default: 'ACC3', kind: 'account' },
      { key: 'instrument_id', label: '合约', default: 'rb2701', kind: 'instrument' },
      { key: 'qty', label: '手数（0 = 全部）', default: 0, min: 0, step: 1 },
    ],
  },
]

export const SECTOR_OPTIONS = Object.entries(SECTOR_LABELS).map(([value, label]) => ({ value, label }))
