/** Number / money / percent / time formatting (ratios are decimals; UI shows percents; Asia/Shanghai). */
import { describe, expect, it } from 'vitest'
import {
  DASH,
  decimalsForTick,
  fmtBytes,
  fmtDateTime,
  fmtDuration,
  fmtInt,
  fmtLag,
  fmtMicros,
  fmtMoney,
  fmtMoneyCompact,
  fmtNum,
  fmtPct,
  fmtPrice,
  fmtSignedMoney,
  fmtTd,
  fmtTime,
  fmtTimes,
  shanghaiToNs,
  shortId,
  signClass,
  toNs,
  toNum,
  toShanghaiIso,
} from '@/utils/format'

describe('numbers', () => {
  it('parses numbers and DecStr, rejects garbage', () => {
    expect(toNum(3.5)).toBe(3.5)
    expect(toNum('3118.50')).toBe(3118.5)
    expect(toNum('')).toBeNull()
    expect(toNum('abc')).toBeNull()
    expect(toNum(Number.NaN)).toBeNull()
    expect(toNum(null)).toBeNull()
  })

  it('groups thousands with fixed decimals and never prints -0', () => {
    expect(fmtNum(1234567.891)).toBe('1,234,567.89')
    expect(fmtNum(-1234567.891, 1)).toBe('-1,234,567.9')
    expect(fmtNum(999.996, 2)).toBe('1,000.00')
    expect(fmtNum(-0.001)).toBe('0.00')
    expect(fmtNum('3118.5')).toBe('3,118.50')
    expect(fmtNum(null)).toBe(DASH)
    expect(fmtInt(1800000)).toBe('1,800,000')
  })

  it('formats money plain, signed and compact (万 / 亿)', () => {
    expect(fmtMoney(58464029.05)).toBe('58,464,029.05')
    expect(fmtSignedMoney(44687.38)).toBe('+44,687.38')
    expect(fmtSignedMoney(-38808.15)).toBe('-38,808.15')
    expect(fmtSignedMoney(0)).toBe('0.00')
    expect(fmtMoneyCompact(58464029.05)).toBe('5846.40万')
    expect(fmtMoneyCompact(123450000)).toBe('1.23亿')
    expect(fmtMoneyCompact(-4050.61)).toBe('-4,050.61')
    expect(fmtMoneyCompact(11758.3, 2, true)).toBe('+1.18万')
    expect(fmtMoneyCompact(-4502800, 1)).toBe('-450.3万')
    expect(fmtMoneyCompact(0.001, 2, true)).toBe('0.00')
    expect(fmtMoneyCompact(undefined)).toBe(DASH)
  })

  it('formats decimal ratios as percents with optional sign', () => {
    expect(fmtPct(0.0302)).toBe('3.02%')
    expect(fmtPct(-0.0302)).toBe('-3.02%')
    expect(fmtPct(0.0302, 2, true)).toBe('+3.02%')
    expect(fmtPct(-0.0302, 2, true)).toBe('-3.02%')
    expect(fmtPct(-0.00001)).toBe('0.00%')
    expect(fmtPct(0, 2, true)).toBe('0.00%')
    expect(fmtPct(0.12345, 1)).toBe('12.3%')
    expect(fmtPct('0.35', 0)).toBe('35%')
    expect(fmtPct(null)).toBe(DASH)
  })

  it('formats multipliers', () => {
    expect(fmtTimes(1.254)).toBe('1.25×')
    expect(fmtTimes(null)).toBe(DASH)
  })

  it('derives price decimals from the tick size', () => {
    expect(decimalsForTick(1)).toBe(0)
    expect(decimalsForTick(10)).toBe(0)
    expect(decimalsForTick(0.5)).toBe(1)
    expect(decimalsForTick(0.2)).toBe(1)
    expect(decimalsForTick(0.02)).toBe(2)
    expect(decimalsForTick(0.005)).toBe(3)
    expect(decimalsForTick(undefined)).toBe(2)
    expect(fmtPrice(3118, 1)).toBe('3,118')
    expect(fmtPrice(4414.8, 0.2)).toBe('4,414.8')
    expect(fmtPrice(108.005, 0.005)).toBe('108.005')
    expect(fmtPrice(850.02, 0.02)).toBe('850.02')
    expect(fmtPrice(null, 1)).toBe(DASH)
  })

  it('formats latency, durations and sizes', () => {
    expect(fmtLag(35.4)).toBe('35 ms')
    expect(fmtLag(1234)).toBe('1.23 s')
    expect(fmtMicros(850)).toBe('850 µs')
    expect(fmtMicros(2400)).toBe('2.40 ms')
    expect(fmtDuration(45_000)).toBe('45s')
    expect(fmtDuration(725_000)).toBe('12m 05s')
    expect(fmtDuration((3 * 60 + 7) * 60_000)).toBe('3h 07m')
    expect(fmtDuration(-1)).toBe(DASH)
    expect(fmtBytes(512)).toBe('512 B')
    expect(fmtBytes(203_900)).toBe('199.1 KB')
  })

  it('classifies signs for the up/down color scheme', () => {
    expect(signClass(1)).toBe('up')
    expect(signClass(-0.5)).toBe('down')
    expect(signClass(0)).toBe('')
    expect(signClass(null)).toBe('')
  })
})

describe('time (Asia/Shanghai)', () => {
  const ns = shanghaiToNs(2026, 9, 28, 10, 32, 15)

  it('converts Shanghai wall clock to epoch ns (UTC+8, no DST)', () => {
    expect(ns / 1e6).toBe(Date.UTC(2026, 8, 28, 2, 32, 15))
  })

  it('formats ns timestamps in Shanghai time regardless of the host time zone', () => {
    expect(fmtTime(ns)).toBe('10:32:15')
    expect(fmtTime(ns + 500e6, true)).toBe('10:32:15.500')
    expect(fmtDateTime(ns)).toBe('2026-09-28 10:32:15')
    expect(fmtDateTime(shanghaiToNs(2026, 9, 28, 23, 59, 59))).toBe('2026-09-28 23:59:59')
    expect(fmtTime(0)).toBe(DASH)
  })

  it('formats trading days and ISO strings with +08:00', () => {
    expect(fmtTd('20260928')).toBe('2026-09-28')
    expect(fmtTd(null)).toBe(DASH)
    expect(toShanghaiIso(shanghaiToNs(2026, 9, 28, 15, 31, 0) / 1e6)).toBe('2026-09-28T15:31:00+08:00')
  })

  it('normalizes timestamps of unknown unit to ns', () => {
    const ms = Date.UTC(2026, 8, 28)
    expect(toNs(ms)).toBe(ms * 1e6)
    expect(toNs(ms / 1000)).toBe(ms * 1e6)
    expect(toNs(ms * 1e6)).toBe(ms * 1e6)
  })
})

describe('ids', () => {
  it('shortens long ids and supports a zero-length tail', () => {
    expect(shortId('0192f3a1-aaaa-bbbb-cccc-9c2e9c2e9c2e')).toBe('0192f3a1…9c2e')
    expect(shortId('abcdef0123456789', 6, 0)).toBe('abcdef…')
    expect(shortId('short')).toBe('short')
    expect(shortId(null)).toBe(DASH)
  })
})
