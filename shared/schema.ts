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
});

export const marketDataSchema = z.object({
  ticker: z.string(),
  yes_bid: z.number(),
  yes_ask: z.number(),
  seconds_remaining: z.number(),
  volume: z.number(),
});

export const tickDataSchema = z.object({
  timestamp: z.string(),
  btc_price: z.number(),
  btc_momentum_1m: z.number(),
  btc_momentum_5m: z.number(),
  btc_prices: z.array(z.tuple([z.number(), z.number()])),
  current_market: marketDataSchema.nullable(),
  last_settled: z.object({
    ticker: z.string(),
    result: z.enum(["yes", "no"]),
  }).nullable(),
  strategies: z.object({
    momentum: strategySignalSchema,
    mean_reversion: strategySignalSchema,
    consensus: strategySignalSchema,
  }),
  trades: z.array(tradeSchema),
  stats: z.object({
    total_trades: z.number(),
    wins: z.number(),
    losses: z.number(),
    win_rate: z.number(),
    total_pnl: z.number(),
    daily_pnl: z.number(),
  }),
});

export type StrategySignal = z.infer<typeof strategySignalSchema>;
export type Trade = z.infer<typeof tradeSchema>;
export type MarketData = z.infer<typeof marketDataSchema>;
export type TickData = z.infer<typeof tickDataSchema>;
