/**
 * Dashboard bootstrap: Pinia, runtime (live api-gateway or in-browser mock), WS
 * frames -> throttled batcher -> stores, router. Element Plus components are imported on
 * demand (unplugin-vue-components, see vite.config.ts); the locale comes from the
 * `<el-config-provider>` in App.vue, the services used from scripts get their styles here.
 */
import { createApp } from 'vue'
import { createPinia } from 'pinia'
import 'element-plus/theme-chalk/base.css'
import 'element-plus/es/components/message/style/css'
import 'element-plus/es/components/notification/style/css'
import './styles/main.css'
import './charts'
import App from './App.vue'
import { router } from './router'
import { initRuntime } from './runtime'
import { Batcher } from './realtime/batcher'
import { reloadTopic, routeFrames } from './realtime/dispatcher'
import { useAuth } from './stores/auth'
import { useConnection } from './stores/connection'
import { useSettings } from './stores/settings'
import { useSystem } from './stores/system'

async function bootstrap(): Promise<void> {
  const app = createApp(App)
  const pinia = createPinia()
  app.use(pinia)
  const settings = useSettings()
  settings.applyColorScheme()
  const conn = useConnection()
  const batcher = new Batcher(routeFrames, () => settings.throttleMs)
  const runtime = await initRuntime({
    mock: settings.mock,
    getKey: () => settings.apiKey.trim(),
    onMessage: (m) => {
      // market time of the frame (`mts`): noted on arrival, not after batching
      if (typeof m.mts === 'number') useSystem().noteMts(m.mts)
      batcher.push(m)
    },
    onResyncRequired: (topic, reason) => reloadTopic(topic, reason),
    onStatus: (status, stats) => conn.update(status, stats),
    // a REST 401: the key is rejected; polling stops until a new key is saved
    onUnauthorized: (message) => useAuth().markUnauthorized(message),
  })
  app.use(router)
  app.mount('#app')
  runtime.ws.start()
}

void bootstrap()
