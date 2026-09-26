/**
 * GENERATED from config/risk_rules.yaml (RiskRulesConfig, `model_dump(mode="json")` with every
 * default filled in) — the same objects GET /risk/rules returns in `rules`. Do not edit by hand;
 * regenerate with web/scripts/gen_mock_schema.py when the config changes.
 */
import type { RuleDef } from '@/types'

export const RULES: RuleDef[] = [
  {
    "id": "daily_loss",
    "metric": "account.daily_pnl_pct_init",
    "op": "<=",
    "scope": "account",
    "hysteresis": 0.2,
    "cooldown_s": 60,
    "hold_ms": 3000,
    "pretrade": true,
    "enabled": true,
    "reset_on_day_roll": true,
    "min_gross_pct": null,
    "levels": [
      {
        "name": "warn",
        "threshold": -0.02,
        "action": "ALERT",
        "params": {},
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      },
      {
        "name": "restrict",
        "threshold": -0.025,
        "action": "RESTRICT",
        "params": {
          "max_order_factor": 0.5,
          "no_add_to_losers": true
        },
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      },
      {
        "name": "halt",
        "threshold": -0.03,
        "action": "CIRCUIT_BREAK",
        "params": {},
        "latch": true,
        "confirm": {
          "states": 2,
          "ms": 1000
        },
        "rearm": {
          "extra": 0.01
        }
      },
      {
        "name": "derisk",
        "threshold": -0.05,
        "action": "LIQUIDATE",
        "params": {
          "step": 0.5
        },
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      }
    ],
    "overrides": {}
  },
  {
    "id": "intraday_dd",
    "metric": "account.intraday_dd",
    "op": ">=",
    "scope": "account",
    "hysteresis": 0.2,
    "cooldown_s": 60,
    "hold_ms": 3000,
    "pretrade": false,
    "enabled": true,
    "reset_on_day_roll": true,
    "min_gross_pct": null,
    "levels": [
      {
        "name": "warn",
        "threshold": 0.025,
        "action": "ALERT",
        "params": {},
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      },
      {
        "name": "block",
        "threshold": 0.04,
        "action": "RESTRICT",
        "params": {
          "block_open": true
        },
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      }
    ],
    "overrides": {}
  },
  {
    "id": "hwm_dd",
    "metric": "account.hwm_dd",
    "op": ">=",
    "scope": "account",
    "hysteresis": 0.2,
    "cooldown_s": 60,
    "hold_ms": 3000,
    "pretrade": false,
    "enabled": true,
    "reset_on_day_roll": true,
    "min_gross_pct": null,
    "levels": [
      {
        "name": "warn",
        "threshold": 0.08,
        "action": "ALERT",
        "params": {},
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      },
      {
        "name": "halve",
        "threshold": 0.12,
        "action": "RESTRICT",
        "params": {
          "exposure_cap_factor": 0.5
        },
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      },
      {
        "name": "block",
        "threshold": 0.15,
        "action": "RESTRICT",
        "params": {
          "block_open": true
        },
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      }
    ],
    "overrides": {}
  },
  {
    "id": "nav_floor",
    "metric": "account.nav",
    "op": "<=",
    "scope": "account",
    "hysteresis": 0.02,
    "cooldown_s": 60,
    "hold_ms": 3000,
    "pretrade": false,
    "enabled": true,
    "reset_on_day_roll": true,
    "min_gross_pct": null,
    "levels": [
      {
        "name": "warn",
        "threshold": 0.9,
        "action": "ALERT",
        "params": {},
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      },
      {
        "name": "derisk",
        "threshold": 0.85,
        "action": "LIQUIDATE",
        "params": {
          "step": 0.5
        },
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      }
    ],
    "overrides": {}
  },
  {
    "id": "risk_degree",
    "metric": "account.risk_degree",
    "op": ">=",
    "scope": "account",
    "hysteresis": 0.2,
    "cooldown_s": 60,
    "hold_ms": 3000,
    "pretrade": true,
    "enabled": true,
    "reset_on_day_roll": true,
    "min_gross_pct": null,
    "levels": [
      {
        "name": "warn",
        "threshold": 0.35,
        "action": "ALERT",
        "params": {},
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      },
      {
        "name": "block",
        "threshold": 0.5,
        "action": "RESTRICT",
        "params": {
          "block_open": true
        },
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      },
      {
        "name": "derisk",
        "threshold": 0.7,
        "action": "LIQUIDATE",
        "params": {
          "step": 0.3
        },
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      }
    ],
    "overrides": {}
  },
  {
    "id": "product_exposure",
    "metric": "product.utilization",
    "op": ">=",
    "scope": "account/product",
    "hysteresis": 0.2,
    "cooldown_s": 60,
    "hold_ms": 3000,
    "pretrade": true,
    "enabled": true,
    "reset_on_day_roll": true,
    "min_gross_pct": null,
    "levels": [
      {
        "name": "warn",
        "threshold": 0.7,
        "action": "ALERT",
        "params": {},
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      },
      {
        "name": "block",
        "threshold": 1.0,
        "action": "RESTRICT",
        "params": {
          "block_open": true
        },
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      }
    ],
    "overrides": {}
  },
  {
    "id": "concentration",
    "metric": "account.effective_n",
    "op": "<",
    "scope": "account",
    "hysteresis": 0.1,
    "cooldown_s": 60,
    "hold_ms": 3000,
    "pretrade": false,
    "enabled": true,
    "reset_on_day_roll": true,
    "min_gross_pct": 0.5,
    "levels": [
      {
        "name": "warn",
        "threshold": 5.0,
        "action": "ALERT",
        "params": {},
        "latch": false,
        "confirm": {
          "states": 0,
          "ms": 30000
        },
        "rearm": null
      },
      {
        "name": "restrict",
        "threshold": 3.0,
        "action": "RESTRICT",
        "params": {
          "block_open_largest_product": true
        },
        "latch": false,
        "confirm": {
          "states": 0,
          "ms": 30000
        },
        "rearm": null
      }
    ],
    "overrides": {
      "ACC3": {
        "warn": {
          "threshold": 2.0
        },
        "restrict": {
          "threshold": 1.5
        }
      },
      "ACC4": {
        "warn": {
          "threshold": 1.5
        },
        "restrict": {
          "threshold": 1.0
        }
      },
      "ACC5": {
        "warn": {
          "threshold": 1.0
        },
        "restrict": {
          "threshold": 1.0
        }
      }
    }
  },
  {
    "id": "sector_exposure",
    "metric": "sector.net_pct_equity",
    "op": ">=",
    "scope": "account/sector",
    "hysteresis": 0.2,
    "cooldown_s": 60,
    "hold_ms": 3000,
    "pretrade": false,
    "enabled": true,
    "reset_on_day_roll": true,
    "min_gross_pct": null,
    "levels": [
      {
        "name": "warn",
        "threshold": 0.8,
        "action": "ALERT",
        "params": {},
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      },
      {
        "name": "block",
        "threshold": 1.2,
        "action": "RESTRICT",
        "params": {
          "block_open": true
        },
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      }
    ],
    "overrides": {}
  },
  {
    "id": "leverage",
    "metric": "account.leverage",
    "op": ">=",
    "scope": "account",
    "hysteresis": 0.2,
    "cooldown_s": 60,
    "hold_ms": 3000,
    "pretrade": true,
    "enabled": true,
    "reset_on_day_roll": true,
    "min_gross_pct": null,
    "levels": [
      {
        "name": "warn",
        "threshold": 3.0,
        "action": "ALERT",
        "params": {},
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      },
      {
        "name": "block",
        "threshold": 4.0,
        "action": "RESTRICT",
        "params": {
          "block_open": true
        },
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      }
    ],
    "overrides": {}
  },
  {
    "id": "limit_stress",
    "metric": "account.limit_stress",
    "op": ">=",
    "scope": "account",
    "hysteresis": 0.2,
    "cooldown_s": 60,
    "hold_ms": 3000,
    "pretrade": false,
    "enabled": true,
    "reset_on_day_roll": true,
    "min_gross_pct": null,
    "levels": [
      {
        "name": "warn",
        "threshold": 0.1,
        "action": "ALERT",
        "params": {},
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      },
      {
        "name": "block",
        "threshold": 0.15,
        "action": "RESTRICT",
        "params": {
          "block_open": true
        },
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      }
    ],
    "overrides": {}
  },
  {
    "id": "compliance",
    "metric": "account.compliance_util",
    "op": ">=",
    "scope": "account",
    "hysteresis": 0.2,
    "cooldown_s": 60,
    "hold_ms": 3000,
    "pretrade": false,
    "enabled": true,
    "reset_on_day_roll": true,
    "min_gross_pct": null,
    "levels": [
      {
        "name": "warn",
        "threshold": 0.8,
        "action": "ALERT",
        "params": {},
        "latch": false,
        "confirm": {
          "states": 1,
          "ms": 0
        },
        "rearm": null
      }
    ],
    "overrides": {}
  }
]
