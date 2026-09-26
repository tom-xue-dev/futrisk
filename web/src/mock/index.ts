/**
 * Mock backend entry (loaded lazily by runtime.ts when mock mode is on).
 * One engine + hub per page load; "restart scenario" simply reloads the page.
 */
import type { Transport } from '@/api/http'
import type { WebSocketLike } from '@/api/ws'
import type { MockControls } from '@/runtime'
import { MockEngine } from './engine'
import { MockHub, MockSocket } from './hub'
import { createRestTransport } from './rest'

export interface MockBackend {
  transport: Transport
  createSocket: (url: string) => WebSocketLike
  controls: MockControls
  engine: MockEngine
}

let backend: MockBackend | null = null

export function getMockBackend(): MockBackend {
  if (backend) return backend
  const hub = new MockHub()
  const engine = new MockEngine((topic, type, data) => hub.publish(topic, type, data))
  hub.setProviders(
    (topic) => engine.snapshotFor(topic),
    () => engine.tradingDay,
    () => engine.nowMs(),
    (id) => engine.hasAccount(id),
  )
  engine.onApiRestart = () => hub.disconnectAll()
  engine.start()
  const created: MockBackend = {
    engine,
    transport: createRestTransport(engine),
    createSocket: () => new MockSocket(hub, Date.now() < engine.apiDownUntil),
    controls: {
      dropNextSeq: (topic?: string) => hub.dropNextSeq(topic),
      disconnectAll: () => hub.disconnectAll(),
      restartScenario: () => location.reload(),
      serverResync: (topic?: string) => hub.serverResync(topic),
      setAppMode: (mode) => {
        engine.appMode = mode
      },
      appMode: () => engine.appMode,
    },
  }
  backend = created
  return created
}
