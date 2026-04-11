import { z } from "zod";

// SSE data types - no database needed, all in-memory

export const strategySignalSchema = z.object({
  signal: z.enum(["yes", "no", "none"]),
  confidence: z.number(),
  reason: z.string(),
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
    momentum: strategySignalSchema,
    mean_reversion: strategySignalSchema,
    consensus: strategySignalSchema,
    resolution_rider: strategySignalSchema,
    favorite_bias: strategySignalSchema,
  }),
  markets: z.object({
    btc: marketDataSchema.nullable(),
    eth: marketDataSchema.nullable(),
    sol: marketDataSchema.nullable(),
  }),
  settled: z.object({
    btc: z.object({ ticker: z.string(), result: z.enum(["yes", "no"]) }).nullable(),
    eth: z.object({ ticker: z.string(), result: z.enum(["yes", "no"]) }).nullable(),
    sol: z.object({ ticker: z.string(), result: z.enum(["yes", "no"]) }).nullable(),
  }),
  strategies_by_asset: z.object({
    btc: z.object({
      momentum: strategySignalSchema,
      mean_reversion: strategySignalSchema,
      consensus: strategySignalSchema,
      resolution_rider: strategySignalSchema,
      favorite_bias: strategySignalSchema,
    }),
    eth: z.object({
      momentum: strategySignalSchema,
      mean_reversion: strategySignalSchema,
      consensus: strategySignalSchema,
      resolution_rider: strategySignalSchema,
      favorite_bias: strategySignalSchema,
    }),
    sol: z.object({
      momentum: strategySignalSchema,
      mean_reversion: strategySignalSchema,
      consensus: strategySignalSchema,
      resolution_rider: strategySignalSchema,
      favorite_bias: strategySignalSchema,
    }),
  }),
  enabled_assets: z.object({
    btc: z.boolean(),
    eth: z.boolean(),
    sol: z.boolean(),
  }),
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
      price: z.string(),
      max_secs: z.number(),
      buffer: z.string(),
      cv_wr: z.number().nullable(),
      cv_trades: z.number(),
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
export type TickData = z.infer<typeof tickDataSchema>;
