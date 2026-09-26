/**
 * Runtime wiring: REST endpoints + WebSocket client, backed either by the real
 * api-gateway (same origin, `/api/v1` and `/ws`) or by the in-browser mock backend.
 * The mock is loaded with a dynamic import so it is a separate chunk that live
 * deployments never download.
 */
import { createEndpoints, type Endpoints } from '@/api/endpoints'
import { ApiClient, fetchTransport, type Transport } from '@/api/http'
import { WsClient, type ResyncReason, type WebSocketLike, type WsStats, type WsStatus } from '@/api/ws'
import type { WsServerMessage } from '@/types'

/** Controls the mock exposes to the settings dialog (demonstrate WS gap / reconnect handling). */
export interface MockControls {
  dropNextSeq(topic?: string): void
  disconnectAll(): void
  restartScenario(): void
  /** send a server `resync` frame (what the gateway does when a slow client overflows an event topic) */
  serverResync(topic?: string): void
  /** app.mode reported by the mock: outside 'demo' the /demo/* endpoints answer 403 like the real gateway */
  setAppMode(mode: 'demo' | 'live'): void
  appMode(): 'demo' | 'live'
}

export interface Runtime {
  api: Endpoints
  ws: WsClient
  mock: MockControls | null
  isMock: boolean
}

let current: Runtime | null = null

export function rt(): Runtime {
  if (!current) throw new Error('runtime not initialized')
  return current
}

export function hasRuntime(): boolean {
  return current !== null
}

/** Install a runtime directly (tests: stores over a fixture-backed ApiClient). */
export function setRuntime(r: Runtime | null): void {
  current = r
}

export interface RuntimeOptions {
  mock: boolean
  getKey: () => string
  onMessage: (msg: WsServerMessage) => void
  onResyncRequired: (topic: string, reason: ResyncReason) => void
  onStatus: (status: WsStatus, stats: Readonly<WsStats>) => void
  /** a REST answer 401 UNAUTHORIZED */
  onUnauthorized?: (message: string) => void
}

export function wsUrl(): string {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${proto}//${location.host}/ws`
}

export async function initRuntime(opts: RuntimeOptions): Promise<Runtime> {
  let transport: Transport = fetchTransport
  let createSocket = (url: string): WebSocketLike => new WebSocket(url) as unknown as WebSocketLike
  let mock: MockControls | null = null
  if (opts.mock) {
    const m = await import('@/mock')
    const backend = m.getMockBackend()
    transport = backend.transport
    createSocket = backend.createSocket
    mock = backend.controls
  }
  const client = new ApiClient({ getKey: opts.getKey, transport, onUnauthorized: opts.onUnauthorized })
  const ws = new WsClient({
    url: wsUrl,
    getKey: opts.getKey,
    createSocket,
    onMessage: opts.onMessage,
    onResyncRequired: opts.onResyncRequired,
    onStatus: opts.onStatus,
    requireKey: !opts.mock,
    // the gateway answers every sub with a snapshot: no extra resync on (re)subscribe
    resyncOnSubscribe: false,
    resyncOnReconnect: false,
    // event topics resume from the snapshot of the new connection; stores reload
    // what a snapshot cannot rebuild (e.g. orders beyond the read model)
    reloadEventTopicsOnReconnect: true,
  })
  current = { api: createEndpoints(client), ws, mock, isMock: opts.mock }
  return current
}
