/**
 * Wire rendering of the mock payloads, as the gateway's `msgspec.to_builtins` does it:
 * every futrisk message struct is declared with `omit_defaults=True`, so a field equal to
 * its default (None, 0, false, "", [], {}, "OK", "INFO", …) is left out of the JSON.
 * The dashboard must cope with those absent fields; the mock reproduces them so that
 * mock mode exercises the same code paths as the real api-gateway.
 */
import { WIRE } from './wire.gen'

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true
  if (a === null || b === null || typeof a !== 'object' || typeof b !== 'object') return false
  if (Array.isArray(a) !== Array.isArray(b)) return false
  if (Array.isArray(a) && Array.isArray(b)) return a.length === b.length && a.every((x, i) => deepEqual(x, b[i]))
  const ka = Object.keys(a as object)
  const kb = Object.keys(b as object)
  return ka.length === kb.length && ka.every((k) => deepEqual((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k]))
}

/** A copy of `obj` (a struct of type `type`) as the gateway would send it. */
export function wire<T>(type: string, obj: T): T {
  if (obj === null || obj === undefined || typeof obj !== 'object') return obj
  if (Array.isArray(obj)) return obj.map((x) => wire(type, x)) as unknown as T
  const spec = WIRE[type]
  const src = obj as unknown as Record<string, unknown>
  if (!spec) return { ...src } as T
  const out: Record<string, unknown> = {}
  for (const [k, v0] of Object.entries(src)) {
    if (v0 === undefined) continue
    let v = v0
    const nested = spec.nested[k]
    if (nested && v !== null && typeof v === 'object') {
      if (nested.endsWith('{}')) {
        const inner = nested.slice(0, -2)
        v = Object.fromEntries(Object.entries(v as Record<string, unknown>).map(([dk, dv]) => [dk, wire(inner, dv)]))
      } else {
        v = wire(nested, v)
      }
    }
    if (spec.omit && k in spec.defaults && deepEqual(v, spec.defaults[k])) continue
    out[k] = v
  }
  return out as T
}

/** `wire` for each element of a list. */
export function wireList<T>(type: string, list: readonly T[]): T[] {
  return list.map((x) => wire(type, x))
}
