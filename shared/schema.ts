import { z } from "zod";

// SSE data types - no database needed, all in-memory

export const strategySignalSchema = z.object({
  signal: z.enum(["yes", "no", "none"]),
  confidence: z.number(),
  reason: z.string(),
});

// Live per-coin momentum values the bot uses for RR entry gating.
// Included so the dashboard shows the exact numbers the bot sees,
// not just proxies from BTC-only data.
export const assetMomentumSchema = z.object({
  price: z.number(),
  mom_1m: z.number(),
  mom_5m: z.number(),
  mom_cell: z.number().nullable(),
  mom_window: z.number(),
  mom_periods: z.number(),
  mom_gate: z.number().nullable().optional(),
  realized_vol: z.number().nullable().optional(),
  vol_gate: z.number().nullable().optional(),
  vol_lookback: z.number().optional(),
});

export const tradeSchema = z.object({
  time: z.string(),
  ticker: z.string(),
  strategy: z.string(),
  side: z.enum(["yes", "no"]),
  price: z.number(),
  contracts: z.number(),
  stake: z.number(),
  outcome: z.enum(["win", "loss", "pending"]),
  profit: z.number(),
  fees: z.number(),
  profit_after_fees: z.number(),
  order_type: z.enum(["maker", "taker"]).optional(),
  order_id: z.string().optional(),
});

export const marketDataSchema = z.object({
  ticker: z.string(),
  yes_bid: z.number(),
  yes_ask: z.number(),
  seconds_remaining: z.number(),
  volume: z.number(),
  floor_strike: z.number().nullable().optional(),
  cap_strike: z.number().nullable().optional(),
});

export const tickDataSchema = z.object({
  timestamp: z.string(),
  btc_price: z.number(),
  btc_momentum_1m: z.number(),
  btc_momentum_5m: z.number(),
  btc_prices: z.array(z.tuple([z.number(), z.number()])),
  eth_price: z.number(),
  eth_momentum_1m: z.number(),
  eth_momentum_5m: z.number(),
  eth_prices: z.array(z.tuple([z.number(), z.number()])),
  sol_price: z.number(),
  sol_momentum_1m: z.number(),
  sol_momentum_5m: z.number(),
  sol_prices: z.array(z.tuple([z.number(), z.number()])),
  current_market: marketDataSchema.nullable(),
  last_settled: z.object({
    ticker: z.string(),
    result: z.enum(["yes", "no"]),
  }).nullable(),
  strategies: z.object({
    resolution_rider: strategySignalSchema,
  }),
  // Per-asset payload. Keys are the bot's asset keys: btc/eth/sol/doge/xrp/bnb/hype
  // for 15M markets, plus btc_daily/eth_daily/... for the daily/hourly variants.
  markets: z.record(marketDataSchema.nullable()),
  settled: z.record(
    z.object({ ticker: z.string(), result: z.enum(["yes", "no"]) }).nullable(),
  ),
  strategies_by_asset: z.record(
    z.object({ resolution_rider: strategySignalSchema }),
  ),
  asset_momentum: z.record(assetMomentumSchema).optional(),
  // enabled_assets removed 2026-04-14 — the only runtime control is
  // the global `trading_enabled` pause.
  trading_enabled: z.boolean(),
  vol_regime: z.enum(["low", "medium", "high"]),
  vol_reading: z.number(),
  ofi: z.number(),
  exchange_data: z.object({
    btc: z.object({
      divergence_pct: z.number(),
      exchange_lead: z.string().nullable(),
      santiment: z.record(z.number()).optional(),
    }),
    eth: z.object({
      divergence_pct: z.number(),
      exchange_lead: z.string().nullable(),
      santiment: z.record(z.number()).optional(),
    }),
    sol: z.object({
      divergence_pct: z.number(),
      exchange_lead: z.string().nullable(),
      santiment: z.record(z.number()).optional(),
    }),
  }),
  strategy_matrix: z.array(z.object({
    asset: z.string(),
    strategy: z.string(),
    enabled: z.boolean(),
    edge: z.number().nullable(),
    shadow_edge: z.number().nullable(),
    win_rate: z.number().nullable(),
    trades: z.number(),
    total_trades: z.number(),
    shadow_trades: z.number(),
    recent_pnl: z.number(),
    days_disabled_7d: z.number(),
    status: z.enum(["enabled", "disabled", "shadow"]),
  })).optional(),
  rr_config: z.object({
    defaults: z.object({
      min_contract_price: z.number(),
      max_entry_price: z.number(),
      min_seconds: z.number(),
      max_seconds: z.number(),
      min_price_buffer_pct: z.number(),
      max_adverse_momentum: z.number(),
      max_stake_usd: z.number(),
    }),
    per_cell: z.record(z.object({
      // Raw numeric thresholds (units documented in bot.py _publish_tick)
      min_contract_price: z.number().optional(),
      max_entry_price: z.number().optional(),
      min_seconds: z.number().nullable().optional(),
      max_seconds: z.number().optional(),
      min_price_buffer_pct: z.number().optional(),
      // Display-formatted convenience strings
      price: z.string(),
      max_secs: z.number(),
      buffer: z.string(),
      mom_gate: z.number().nullable().optional(),
      mom_window: z.number().nullable().optional(),
      mom_periods: z.number().nullable().optional(),
      vol_gate: z.number().nullable().optional(),
      cv_wr: z.number().nullable(),
      cv_trades: z.number(),
    })),
  }).optional(),
  recent_skips: z.array(z.object({
    timestamp: z.string(),
    ticker: z.string(),
    strategy: z.string(),
    side: z.string(),
    ask_price: z.union([z.number(), z.string()]),
    max_price: z.union([z.number(), z.string()]),
    yes_bid: z.union([z.number(), z.string()]).nullable(),
    yes_ask: z.union([z.number(), z.string()]).nullable(),
    reason: z.string(),
  })).optional(),
  hit_outcomes_summary: z.object({
    window_hours: z.number(),
    counts: z.record(z.number()),
    total: z.number(),
    fills: z.number(),
  }).optional(),
  gate_matrix: z.object({
    gates: z.array(z.string()),
    rows: z.array(z.object({
      ticker: z.string(),
      cell: z.string(),
      blocked_at: z.string(),
      detail: z.record(z.union([z.number(), z.string()])),
      age_s: z.number(),
    })),
  }).optional(),
  trades: z.array(tradeSchema),
  stats: z.object({
    total_trades: z.number(),
    pending: z.number(),
    wins: z.number(),
    losses: z.number(),
    win_rate: z.number(),
    total_pnl: z.number(),
    total_fees: z.number(),
    total_pnl_after_fees: z.number(),
    daily_pnl: z.number(),
    daily_pnl_after_fees: z.number(),
    daily_fees: z.number().optional(),
    weekly_pnl: z.number().optional(),
    weekly_pnl_net: z.number().optional(),
    weekly_fees: z.number().optional(),
    monthly_pnl: z.number().optional(),
    monthly_pnl_net: z.number().optional(),
    monthly_fees: z.number().optional(),
    alltime_pnl: z.number().optional(),
    alltime_pnl_net: z.number().optional(),
    alltime_fees: z.number().optional(),
    bot_paused: z.boolean(),
    paper_balance: z.number().nullable(),
    live_balance: z.number().nullable(),
    is_paper: z.boolean(),
  }),
});

export type StrategySignal = z.infer<typeof strategySignalSchema>;
export type Trade = z.infer<typeof tradeSchema>;
export type MarketData = z.infer<typeof marketDataSchema>;
export type AssetMomentum = z.infer<typeof assetMomentumSchema>;
export type TickData = z.infer<typeof tickDataSchema>;
