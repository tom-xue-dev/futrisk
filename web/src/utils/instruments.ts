/**
 * Display-only product metadata (tick sizes from config/products.yaml) used to format
 * prices with the right number of decimals; quotes do not carry the tick size.
 */
import { productOf } from './labels'

export const PRODUCT_TICKS: Record<string, number> = {
  rb: 1,
  hc: 1,
  cu: 10,
  au: 0.02,
  i: 0.5,
  j: 0.5,
  m: 1,
  IF: 0.2,
  IH: 0.2,
  IC: 0.2,
  IM: 0.2,
  T: 0.005,
}

export function tickOf(instrumentId: string): number | undefined {
  return PRODUCT_TICKS[productOf(instrumentId)]
}
