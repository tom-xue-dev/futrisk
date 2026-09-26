/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** "1" / "true": start in mock mode (in-browser backend); the settings dialog can override it */
  readonly VITE_MOCK?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}

declare module '*.vue' {
  import type { DefineComponent } from 'vue'
  const component: DefineComponent<object, object, unknown>
  export default component
}
