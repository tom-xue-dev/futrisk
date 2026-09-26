/**
 * The caller's identity (`GET /auth/me` → `{key_id, name, role}`), used to hide actions
 * the gateway would reject with 403 FORBIDDEN (02 §9.6 matrix: RISK = RISK_MANAGER or
 * ADMIN; ADMIN = ADMIN only; TRADER and RISK_MANAGER do not include each other). While
 * the role is unknown nothing is hidden: the gateway checks every request anyway.
 */
import { defineStore } from 'pinia'
import type { AuthMe, Role } from '@/types'
import { ApiError } from '@/api/http'
import { rt } from '@/runtime'

const RISK_ROLES: readonly Role[] = ['RISK_MANAGER', 'ADMIN']

export const ROLE_LABELS: Record<Role, string> = {
  VIEWER: '只读',
  TRADER: '交易员',
  RISK_MANAGER: '风控',
  ADMIN: '管理员',
}

export const useAuth = defineStore('auth', {
  state: () => ({
    me: null as AuthMe | null,
    /** '' = not asked yet, 'OK', or the error code of the last attempt (UNAUTHORIZED, UNAVAILABLE, …) */
    code: '' as string,
    message: '',
  }),
  getters: {
    role: (s): Role | null => s.me?.role ?? null,
    /** reset / halt / kill switch / ack / liquidation confirm */
    canRisk: (s): boolean => !s.me || RISK_ROLES.includes(s.me.role),
    /** demo controls, settlement run, backtests, service restart */
    canAdmin: (s): boolean => !s.me || s.me.role === 'ADMIN',
    unauthorized: (s): boolean => s.code === 'UNAUTHORIZED',
  },
  actions: {
    async load(): Promise<void> {
      try {
        this.me = await rt().api.authMe()
        this.code = 'OK'
        this.message = ''
      } catch (e) {
        this.me = null
        this.code = e instanceof ApiError ? e.code : 'UNAVAILABLE'
        this.message = e instanceof Error ? e.message : String(e)
      }
    },
    clear(): void {
      this.me = null
      this.code = ''
      this.message = ''
    },
    /** A REST 401: the key was rejected (revoked, disabled, expired or wrong). */
    markUnauthorized(message = ''): void {
      if (this.code === 'UNAUTHORIZED') return
      this.me = null
      this.code = 'UNAUTHORIZED'
      this.message = message
    },
  },
})

/** Tooltip for a hidden / disabled action. */
export function needRoleText(perm: 'RISK' | 'ADMIN'): string {
  return perm === 'RISK' ? '需要 RISK_MANAGER 或 ADMIN 角色' : '需要 ADMIN 角色'
}
