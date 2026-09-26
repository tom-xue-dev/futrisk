import { fileURLToPath, URL } from 'node:url'
import vue from '@vitejs/plugin-vue'
import Components from 'unplugin-vue-components/vite'
import { ElementPlusResolver } from 'unplugin-vue-components/resolvers'
import { defineConfig } from 'vitest/config'

/**
 * futrisk dashboard build config.
 *
 * - Dev and preview servers proxy `/api` (REST, 02 §9.6) and `/ws` (WebSocket, 02 §9.7)
 *   to the api-gateway (default http://127.0.0.1:8000, override with FUTRISK_BACKEND).
 * - The production bundle is written to `src/futrisk/web_static/dist`, which the
 *   api-gateway serves as static files (01 §5.3.5). `base: './'` + hash routing keeps
 *   the bundle relocatable and independent of any SPA fallback on the server.
 * - Element Plus components (and their styles, and `v-loading`) are imported on demand by
 *   unplugin-vue-components: only what the templates use ends up in the bundle.
 * - No inline scripts or styles in index.html (the gateway serves the page with a CSP).
 */
const backend = process.env.FUTRISK_BACKEND ?? 'http://127.0.0.1:8000'
const proxy = {
  '/api': { target: backend, changeOrigin: true },
  '/ws': { target: backend.replace(/^http/, 'ws'), ws: true, changeOrigin: true },
}

export default defineConfig({
  base: './',
  plugins: [vue(), Components({ resolvers: [ElementPlusResolver({ importStyle: 'css', directives: true })], dts: false })],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  server: {
    host: '127.0.0.1',
    port: 5173,
    proxy,
  },
  preview: {
    host: '127.0.0.1',
    port: 4173,
    proxy,
  },
  build: {
    outDir: '../src/futrisk/web_static/dist',
    emptyOutDir: true,
    rollupOptions: {
      output: {
        // vendor chunks by package: only the modules actually imported end up in them
        manualChunks(id: string): string | undefined {
          if (!id.includes('node_modules')) return undefined
          if (/node_modules[\/](echarts|zrender|vue-echarts)[\/]/.test(id)) return 'echarts'
          if (/node_modules[\/](element-plus|@element-plus)[\/]/.test(id)) return 'element-plus'
          if (/node_modules[\/](@vue|vue|vue-router|pinia)[\/]/.test(id)) return 'vue'
          return undefined
        },
      },
    },
  },
  test: {
    environment: 'node',
    include: ['tests/**/*.spec.ts'],
  },
})
