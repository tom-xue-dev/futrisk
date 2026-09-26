/**
 * Typed wrappers of the api-gateway REST endpoints used by the dashboard (02 §9.6;
 * `src/futrisk/services/api_gateway/routes/*.py`). Return types are the real answer
 * shapes (types/api.ts); the `*Full` variants also return the envelope message,
 * which carries partial-answer notes such as "partial: risk-engine-1=UNAVAILABLE".
 */
import type {
  AckAlertBody,
  AccountListItem,
  AccountSnapshotView,
  AccountStateChanged,
  AuthMe,
  BacktestRunBody,
  BacktestStatus,
  ClockJumpResult,
  CompareHistory,
  CompareView,
  DemoClockBody,
  DemoFaultBody,
  DemoFaultResult,
  DemoFaultsResponse,
  DemoScenarioBody,
  DemoScenarioInfo,
  DemoScenarioResult,
  DemoState,
  DemoShockBody,
  DemoShockResult,
  EodPhaseEvent,
  HaltBody,
  HealthInfo,
  FanoutResult,
  ForceUnfreezeBody,
  KillSwitchBody,
  KillSwitchResult,
  LiquidationConfirmBody,
  LiquidationPlan,
  LiquidationPlansResponse,
  LiquidationStatus,
  MdEvent,
  MdFeedsView,
  MetricSeries,
  OrderUpdate,
  PositionGroupBy,
  PositionsResponse,
  PositionView,
  ReportReady,
  ReportType,
  ResetAck,
  ResetBody,
  RiskEventsResponse,
  RiskStateRow,
  RuleSet,
  ScenarioRunState,
  ServiceInfo,
  SettleRunBody,
  SettlementDaysResponse,
  SettlementResult,
  StatementResponse,
  SystemServicesResponse,
  SystemStatus,
  Tick,
  Trade,
} from '@/types'
import type { EodPhase } from '@/types'
import type { ApiClient } from './http'

const enc = encodeURIComponent

export interface PositionsQuery {
  account?: string
  product?: string
  instrument?: string
  exchange?: string
  sector?: string
  group_by?: PositionGroupBy
}

export function createEndpoints(api: ApiClient) {
  return {
    raw: api,

    // ---- system
    health: () => api.get<HealthInfo>('/health'),
    authMe: () => api.get<AuthMe>('/auth/me'),
    systemServices: () => api.get<SystemServicesResponse>('/system/services'),
    systemStatus: () => api.get<SystemStatus>('/system/status'),
    restartService: (instance: string, key?: string) => api.postFull<ServiceInfo>(`/services/${enc(instance)}/restart`, {}, { idempotencyKey: key }),

    // ---- market data
    quotes: (instruments?: string[]) => api.get<Tick[]>('/market/quotes', { query: { instruments } }),
    /** md.status + `stale` / `age_ms` of md-gateway (its last status is not live data while it is down) */
    feeds: () => api.get<MdFeedsView>('/market/feeds'),
    marketEvents: (td?: string, kind?: string) => api.get<{ items: MdEvent[] }>('/market/events', { query: { td, kind } }),

    // ---- accounts & positions
    /** every configured account: its snapshot, or `{account_id, name, shard, missing: true, …}` */
    accounts: () => api.get<AccountListItem[]>('/accounts'),
    account: (id: string) => api.get<AccountSnapshotView>(`/accounts/${enc(id)}`),
    positions: (id: string) => api.get<PositionView[]>(`/accounts/${enc(id)}/positions`),
    orders: (id: string, td?: string) => api.get<{ items: OrderUpdate[] }>(`/accounts/${enc(id)}/orders`, { query: { td } }),
    trades: (id: string, td?: string) => api.get<{ items: Trade[] }>(`/accounts/${enc(id)}/trades`, { query: { td } }),
    equityCurve: (id: string, td?: string, stepS = 5) => api.get<MetricSeries>(`/accounts/${enc(id)}/equity-curve`, { query: { td, step_s: stepS } }),
    positionsQuery: (q: PositionsQuery) => api.get<PositionsResponse>('/positions', { query: { ...q } }),

    // ---- risk
    rules: () => api.getFull<RuleSet>('/risk/rules'),
    riskState: (account?: string) => api.get<RiskStateRow[]>('/risk/state', { query: { account } }),
    riskEvents: (td?: string, account?: string) => api.getFull<RiskEventsResponse>('/risk/events', { query: { td, account } }),
    ackAlert: (alertKey: string, body: AckAlertBody, key?: string) => api.postFull<null>(`/risk/events/${enc(alertKey)}/ack`, body, { idempotencyKey: key }),
    compare: () => api.get<CompareView>('/risk/compare'),
    compareHistory: (days: number) => api.getFull<CompareHistory>('/risk/compare/history', { query: { days } }),
    resetAccount: (id: string, body: ResetBody, key?: string) => api.postFull<ResetAck>(`/risk/accounts/${enc(id)}/reset`, body, { idempotencyKey: key }),
    haltAccount: (id: string, body: HaltBody, key?: string) => api.postFull<AccountStateChanged>(`/risk/accounts/${enc(id)}/halt`, body, { idempotencyKey: key }),
    killSwitch: (body: KillSwitchBody, key?: string) => api.postFull<KillSwitchResult>('/risk/firm/kill-switch', body, { idempotencyKey: key }),
    /** fan-out to every risk shard: lifts EOD_NOT_SETTLED of a PROVISIONAL shard (HTTP 200, per-target results) */
    forceUnfreeze: (td: string, body: ForceUnfreezeBody, key?: string) => api.postFull<FanoutResult>(`/risk/eod/${enc(td)}/force-unfreeze`, body, { idempotencyKey: key }),
    /** plans of every shard (or the owner); a shard that did not answer: read-model plans marked `stale` */
    liquidationPlans: (q: { account?: string; status?: LiquidationStatus; td?: string } = {}) =>
      api.getFull<LiquidationPlansResponse>('/risk/liquidation-plans', { query: { ...q } }),
    confirmLiquidation: (planId: string, body: LiquidationConfirmBody, key?: string) =>
      api.post<LiquidationPlan>(`/risk/liquidation-plans/${enc(planId)}/confirm`, body, { idempotencyKey: key }),

    // ---- settlement & reports
    settlementDays: (limit = 30) => api.getFull<SettlementDaysResponse>('/settlement/days', { query: { limit } }),
    settlement: (td: string) => api.get<SettlementResult>(`/settlement/${enc(td)}`),
    settlementStatus: (td: string) => api.get<EodPhaseEvent>(`/settlement/${enc(td)}/status`),
    /** `{format: 'text', text}` from the statement file, else `{format: 'settlement', settlement}` with a message */
    statement: (td: string, account: string) => api.getFull<StatementResponse>(`/settlement/${enc(td)}/statements/${enc(account)}`),
    runSettlement: (body: SettleRunBody, key?: string) =>
      api.postFull<{ trading_day: string; version: number; phase: EodPhase }>('/settlement/run', body, { idempotencyKey: key }),
    reports: (td?: string, kind?: ReportType) => api.get<{ items: ReportReady[] }>('/reports', { query: { td, kind } }),
    reportFile: (reportId: string) => api.getBlob(`/reports/${enc(reportId)}`),
    backtests: () => api.get<{ items: BacktestStatus[] }>('/backtests'),
    backtest: (runId: string) => api.getFull<BacktestStatus>(`/backtests/${enc(runId)}`),
    runBacktest: (body: BacktestRunBody, key?: string) => api.postFull<BacktestStatus>('/backtests', body, { idempotencyKey: key }),

    // ---- demo controls (ADMIN; 403 FORBIDDEN unless app.mode=demo; mutations record every outcome
    //      under their Idempotency-Key: a retry with the same key gets the recorded answer)
    demoState: () => api.get<DemoState>('/demo/state'),
    demoFaults: () => api.get<DemoFaultsResponse>('/demo/faults'),
    demoScenarios: () => api.get<DemoScenarioInfo[]>('/demo/scenarios'),
    demoCancelScenario: (key?: string) => api.delFull<ScenarioRunState>('/demo/scenario', { idempotencyKey: key }),
    demoFault: (body: DemoFaultBody, key?: string) => api.postFull<DemoFaultResult>('/demo/faults', body, { idempotencyKey: key }),
    demoDeleteFault: (faultId: string, key?: string) => api.delFull<unknown>(`/demo/faults/${enc(faultId)}`, { idempotencyKey: key }),
    demoShock: (body: DemoShockBody, key?: string) => api.postFull<DemoShockResult>('/demo/shock', body, { idempotencyKey: key }),
    demoScenario: (body: DemoScenarioBody, key?: string) => api.postFull<DemoScenarioResult>('/demo/scenario', body, { idempotencyKey: key }),
    demoClock: (body: DemoClockBody, key?: string) => api.postFull<ClockJumpResult>('/demo/clock', body, { idempotencyKey: key }),
    demoKill: (instance: string, key?: string) => api.postFull<ServiceInfo>(`/demo/kill/${enc(instance)}`, {}, { idempotencyKey: key }),
  }
}

export type Endpoints = ReturnType<typeof createEndpoints>
