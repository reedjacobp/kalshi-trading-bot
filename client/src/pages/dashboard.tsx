import { useState, useEffect, useRef, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts";
import type { TickData } from "@shared/schema";
import {
  TrendingUp,
  TrendingDown,
  Activity,
  Wifi,
  WifiOff,
  Clock,
  BarChart3,
  Zap,
  Target,
  GitMerge,
} from "lucide-react";

// ── SSE Hook ────────────────────────────────────────────────────
const API_BASE = "__PORT_5000__".startsWith("__") ? "" : "__PORT_5000__";

function useSSE() {
  const [data, setData] = useState<TickData | null>(null);
  const [connected, setConnected] = useState(false);
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    const connect = () => {
      const es = new EventSource(`${API_BASE}/api/stream`);
      eventSourceRef.current = es;

      es.onopen = () => setConnected(true);

      es.onmessage = (event) => {
        try {
          const parsed = JSON.parse(event.data);
          setData(parsed);
          setConnected(true);
        } catch (e) {
          console.error("SSE parse error", e);
        }
      };

      es.onerror = () => {
        setConnected(false);
        es.close();
        setTimeout(connect, 3000);
      };
    };

    connect();
    return () => {
      eventSourceRef.current?.close();
    };
  }, []);

  return { data, connected };
}

// ── Animated Number ─────────────────────────────────────────────
function AnimatedNumber({
  value,
  prefix = "",
  suffix = "",
  decimals = 2,
  className = "",
  showSign = false,
}: {
  value: number;
  prefix?: string;
  suffix?: string;
  decimals?: number;
  className?: string;
  showSign?: boolean;
}) {
  const [display, setDisplay] = useState(value);
  const prevRef = useRef(value);

  useEffect(() => {
    const prev = prevRef.current;
    const diff = value - prev;
    if (Math.abs(diff) < 0.0001) {
      setDisplay(value);
      prevRef.current = value;
      return;
    }

    let frame: number;
    const duration = 400;
    const start = performance.now();

    const animate = (now: number) => {
      const t = Math.min(1, (now - start) / duration);
      const eased = 1 - Math.pow(1 - t, 3);
      setDisplay(prev + diff * eased);
      if (t < 1) frame = requestAnimationFrame(animate);
      else prevRef.current = value;
    };

    frame = requestAnimationFrame(animate);
    return () => cancelAnimationFrame(frame);
  }, [value]);

  const sign = showSign && display > 0 ? "+" : "";
  return (
    <span className={`tabular-nums ${className}`}>
      {prefix}{sign}{display.toFixed(decimals)}{suffix}
    </span>
  );
}

// ── KPI Card ────────────────────────────────────────────────────
function KPICard({
  label,
  value,
  prefix = "",
  suffix = "",
  decimals = 2,
  icon: Icon,
  trend,
  showSign = false,
}: {
  label: string;
  value: number;
  prefix?: string;
  suffix?: string;
  decimals?: number;
  icon: React.ElementType;
  trend?: "up" | "down" | "neutral";
  showSign?: boolean;
}) {
  const trendColor =
    trend === "up"
      ? "text-emerald-400"
      : trend === "down"
        ? "text-red-400"
        : "text-slate-400";

  return (
    <motion.div
      className="bg-[hsl(222,33%,7%)] border border-[hsl(220,20%,12%)] rounded-lg p-4 flex flex-col gap-1"
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
    >
      <div className="flex items-center justify-between">
        <span className="text-xs text-slate-400 uppercase tracking-wider font-medium">
          {label}
        </span>
        <Icon size={14} className="text-slate-500" />
      </div>
      <div className={`text-xl font-semibold ${trendColor}`}>
        <AnimatedNumber
          value={value}
          prefix={prefix}
          suffix={suffix}
          decimals={decimals}
          showSign={showSign}
        />
      </div>
    </motion.div>
  );
}

// ── BTC Chart ───────────────────────────────────────────────────
function BTCChart({ prices }: { prices: [number, number][] }) {
  const chartData = prices.map(([t, p]) => ({
    time: t,
    price: p,
  }));

  const minPrice = chartData.length > 0 ? Math.min(...chartData.map((d) => d.price)) : 0;
  const maxPrice = chartData.length > 0 ? Math.max(...chartData.map((d) => d.price)) : 100;
  const padding = (maxPrice - minPrice) * 0.1 || 10;

  return (
    <div className="bg-[hsl(222,33%,7%)] border border-[hsl(220,20%,12%)] rounded-lg p-4 h-full">
      <div className="flex items-center gap-2 mb-3">
        <Activity size={14} className="text-cyan-400" />
        <span className="text-xs text-slate-400 uppercase tracking-wider font-medium">
          BTC Price — Last 15 min
        </span>
      </div>
      <div className="h-[180px]">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={chartData} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
            <defs>
              <linearGradient id="priceGradient" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#22d3ee" stopOpacity={0.3} />
                <stop offset="95%" stopColor="#22d3ee" stopOpacity={0} />
              </linearGradient>
            </defs>
            <XAxis
              dataKey="time"
              tick={{ fill: "#64748b", fontSize: 10 }}
              tickFormatter={(t) => {
                const d = new Date(t);
                return `${d.getHours()}:${d.getMinutes().toString().padStart(2, "0")}`;
              }}
              axisLine={{ stroke: "#1e293b" }}
              tickLine={false}
              minTickGap={60}
            />
            <YAxis
              domain={[minPrice - padding, maxPrice + padding]}
              tick={{ fill: "#64748b", fontSize: 10 }}
              tickFormatter={(v) => `$${Math.round(v).toLocaleString()}`}
              axisLine={false}
              tickLine={false}
              width={68}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: "hsl(222, 33%, 10%)",
                border: "1px solid hsl(220, 20%, 18%)",
                borderRadius: "6px",
                color: "#e2e8f0",
                fontSize: "12px",
              }}
              labelFormatter={(t) => new Date(t).toLocaleTimeString()}
              formatter={(value: number) => [`$${value.toLocaleString(undefined, { minimumFractionDigits: 2 })}`, "BTC"]}
            />
            <Area
              type="monotone"
              dataKey="price"
              stroke="#22d3ee"
              strokeWidth={2}
              fill="url(#priceGradient)"
              dot={false}
              isAnimationActive={false}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

// ── Current Market Panel ────────────────────────────────────────
function CurrentMarketPanel({
  market,
  lastSettled,
}: {
  market: TickData["current_market"];
  lastSettled: TickData["last_settled"];
}) {
  const [countdown, setCountdown] = useState(market?.seconds_remaining ?? 0);

  useEffect(() => {
    if (market) setCountdown(market.seconds_remaining);
  }, [market?.seconds_remaining]);

  useEffect(() => {
    const interval = setInterval(() => {
      setCountdown((c) => Math.max(0, c - 1));
    }, 1000);
    return () => clearInterval(interval);
  }, []);

  const minutes = Math.floor(countdown / 60);
  const seconds = countdown % 60;
  const progress = market ? Math.max(0, Math.min(100, (countdown / 900) * 100)) : 0;

  return (
    <div className="bg-[hsl(222,33%,7%)] border border-[hsl(220,20%,12%)] rounded-lg p-4 h-full flex flex-col">
      <div className="flex items-center gap-2 mb-3">
        <Clock size={14} className="text-cyan-400" />
        <span className="text-xs text-slate-400 uppercase tracking-wider font-medium">
          Current Market
        </span>
      </div>

      {market ? (
        <>
          <div className="text-xs text-slate-500 font-mono mb-3 truncate" data-testid="text-ticker">
            {market.ticker}
          </div>

          <div className="flex items-center gap-4 mb-4">
            <div className="flex-1 text-center">
              <div className="text-xs text-slate-500 mb-1">YES</div>
              <div className="text-2xl font-bold text-emerald-400 tabular-nums" data-testid="text-yes-bid">
                {market.yes_bid}¢
              </div>
              <div className="text-xs text-slate-500">
                bid / <span className="text-slate-400">{market.yes_ask}¢</span> ask
              </div>
            </div>
            <div className="w-px h-12 bg-[hsl(220,20%,15%)]" />
            <div className="flex-1 text-center">
              <div className="text-xs text-slate-500 mb-1">NO</div>
              <div className="text-2xl font-bold text-red-400 tabular-nums" data-testid="text-no-bid">
                {100 - market.yes_ask}¢
              </div>
              <div className="text-xs text-slate-500">
                bid / <span className="text-slate-400">{100 - market.yes_bid}¢</span> ask
              </div>
            </div>
          </div>

          {/* Timer */}
          <div className="mb-3">
            <div className="flex justify-between items-center mb-1">
              <span className="text-xs text-slate-500">Time Remaining</span>
              <span className="text-sm font-mono text-slate-300 tabular-nums" data-testid="text-countdown">
                {minutes}:{seconds.toString().padStart(2, "0")}
              </span>
            </div>
            <div className="h-1.5 bg-[hsl(220,20%,12%)] rounded-full overflow-hidden">
              <motion.div
                className="h-full rounded-full"
                style={{
                  background: progress > 20
                    ? "linear-gradient(90deg, #22d3ee, #06b6d4)"
                    : "linear-gradient(90deg, #f59e0b, #ef4444)",
                }}
                animate={{ width: `${progress}%` }}
                transition={{ duration: 0.5 }}
              />
            </div>
          </div>

          {/* Volume */}
          <div className="flex items-center justify-between text-xs mt-auto">
            <span className="text-slate-500">Volume</span>
            <span className="text-slate-300 tabular-nums" data-testid="text-volume">
              {Math.round(market.volume).toLocaleString()}
            </span>
          </div>
        </>
      ) : (
        <div className="flex-1 flex items-center justify-center text-slate-500 text-sm">
          Waiting for market data...
        </div>
      )}

      {/* Last settled */}
      {lastSettled && (
        <div className="mt-3 pt-3 border-t border-[hsl(220,20%,12%)]">
          <div className="flex items-center justify-between text-xs">
            <span className="text-slate-500">Last Settled</span>
            <span
              className={`font-semibold px-1.5 py-0.5 rounded text-[10px] uppercase tracking-wider ${
                lastSettled.result === "yes"
                  ? "bg-emerald-400/10 text-emerald-400"
                  : "bg-red-400/10 text-red-400"
              }`}
              data-testid="text-last-result"
            >
              {lastSettled.result}
            </span>
          </div>
          <div className="text-[10px] text-slate-600 font-mono mt-0.5 truncate">
            {lastSettled.ticker}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Strategy Signals Panel ──────────────────────────────────────
function StrategySignalsPanel({
  strategies,
}: {
  strategies: TickData["strategies"];
}) {
  const items = [
    { name: "Momentum", icon: Zap, ...strategies.momentum },
    { name: "Mean Reversion", icon: GitMerge, ...strategies.mean_reversion },
    { name: "Consensus", icon: Target, ...strategies.consensus },
  ];

  return (
    <div className="bg-[hsl(222,33%,7%)] border border-[hsl(220,20%,12%)] rounded-lg p-4 h-full flex flex-col">
      <div className="flex items-center gap-2 mb-3">
        <BarChart3 size={14} className="text-cyan-400" />
        <span className="text-xs text-slate-400 uppercase tracking-wider font-medium">
          Strategy Signals
        </span>
      </div>

      <div className="flex flex-col gap-3 flex-1">
        {items.map((item) => {
          const dotColor =
            item.signal === "yes"
              ? "bg-emerald-400"
              : item.signal === "no"
                ? "bg-red-400"
                : "bg-slate-600";
          const textColor =
            item.signal === "yes"
              ? "text-emerald-400"
              : item.signal === "no"
                ? "text-red-400"
                : "text-slate-500";

          return (
            <div
              key={item.name}
              className="flex flex-col gap-1.5"
              data-testid={`strategy-${item.name.toLowerCase().replace(/\s/g, "-")}`}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <div className={`w-2 h-2 rounded-full ${dotColor} ${item.signal !== "none" ? "pulse-dot" : ""}`} />
                  <item.icon size={12} className="text-slate-500" />
                  <span className="text-xs font-medium text-slate-300">{item.name}</span>
                </div>
                <span className={`text-xs font-semibold uppercase ${textColor}`}>
                  {item.signal === "none" ? "—" : item.signal}
                </span>
              </div>

              {/* Confidence bar */}
              <div className="flex items-center gap-2">
                <div className="flex-1 h-1 bg-[hsl(220,20%,12%)] rounded-full overflow-hidden">
                  <motion.div
                    className={`h-full rounded-full ${
                      item.signal === "yes"
                        ? "bg-emerald-400"
                        : item.signal === "no"
                          ? "bg-red-400"
                          : "bg-slate-600"
                    }`}
                    animate={{ width: `${item.confidence * 100}%` }}
                    transition={{ duration: 0.5 }}
                  />
                </div>
                <span className="text-[10px] text-slate-500 tabular-nums w-8 text-right">
                  {item.confidence > 0 ? `${(item.confidence * 100).toFixed(0)}%` : ""}
                </span>
              </div>

              <p className="text-[10px] text-slate-500 leading-relaxed">{item.reason}</p>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ── Trade History Table ─────────────────────────────────────────
function TradeHistoryTable({ trades }: { trades: TickData["trades"] }) {
  return (
    <div className="bg-[hsl(222,33%,7%)] border border-[hsl(220,20%,12%)] rounded-lg p-4">
      <div className="flex items-center gap-2 mb-3">
        <Activity size={14} className="text-cyan-400" />
        <span className="text-xs text-slate-400 uppercase tracking-wider font-medium">
          Trade History
        </span>
        <span className="text-[10px] text-slate-600 ml-auto">Paper Trading</span>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs" data-testid="table-trades">
          <thead>
            <tr className="text-slate-500 uppercase tracking-wider border-b border-[hsl(220,20%,12%)]">
              <th className="text-left py-2 pr-3 font-medium">Time</th>
              <th className="text-left py-2 pr-3 font-medium">Strategy</th>
              <th className="text-left py-2 pr-3 font-medium">Side</th>
              <th className="text-right py-2 pr-3 font-medium">Price</th>
              <th className="text-right py-2 pr-3 font-medium">Contracts</th>
              <th className="text-right py-2 pr-3 font-medium">Stake</th>
              <th className="text-right py-2 pr-3 font-medium">P&L</th>
              <th className="text-right py-2 font-medium">Status</th>
            </tr>
          </thead>
          <tbody>
            {trades.length === 0 ? (
              <tr>
                <td colSpan={8} className="text-center py-8 text-slate-600">
                  No trades yet — waiting for signals
                </td>
              </tr>
            ) : (
              trades.map((trade, i) => (
                <tr
                  key={`${trade.time}-${i}`}
                  className={`border-b border-[hsl(220,20%,8%)] ${
                    i % 2 === 0 ? "bg-transparent" : "bg-[hsl(222,33%,6%)]"
                  }`}
                  data-testid={`row-trade-${i}`}
                >
                  <td className="py-2 pr-3 text-slate-400 font-mono tabular-nums">
                    {new Date(trade.time).toLocaleTimeString([], {
                      hour: "2-digit",
                      minute: "2-digit",
                      second: "2-digit",
                    })}
                  </td>
                  <td className="py-2 pr-3 text-slate-300 capitalize">{trade.strategy}</td>
                  <td className="py-2 pr-3">
                    <span
                      className={`px-1.5 py-0.5 rounded text-[10px] uppercase font-semibold tracking-wider ${
                        trade.side === "yes"
                          ? "bg-emerald-400/10 text-emerald-400"
                          : "bg-red-400/10 text-red-400"
                      }`}
                    >
                      {trade.side}
                    </span>
                  </td>
                  <td className="py-2 pr-3 text-right text-slate-300 tabular-nums">{trade.price}¢</td>
                  <td className="py-2 pr-3 text-right text-slate-300 tabular-nums">{trade.contracts}</td>
                  <td className="py-2 pr-3 text-right text-slate-300 tabular-nums">${trade.stake.toFixed(2)}</td>
                  <td
                    className={`py-2 pr-3 text-right tabular-nums font-medium ${
                      trade.profit >= 0 ? "text-emerald-400" : "text-red-400"
                    }`}
                  >
                    {trade.profit >= 0 ? "+" : ""}${trade.profit.toFixed(2)}
                  </td>
                  <td className="py-2 text-right">
                    <span
                      className={`px-1.5 py-0.5 rounded text-[10px] uppercase font-semibold tracking-wider ${
                        trade.outcome === "win"
                          ? "bg-emerald-400/10 text-emerald-400"
                          : trade.outcome === "loss"
                            ? "bg-red-400/10 text-red-400"
                            : "bg-yellow-400/10 text-yellow-400"
                      }`}
                    >
                      {trade.outcome}
                    </span>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Main Dashboard ──────────────────────────────────────────────
export default function Dashboard() {
  const { data, connected } = useSSE();

  return (
    <div className="min-h-screen bg-[hsl(222,47%,5%)] flex flex-col">
      {/* Header */}
      <header className="flex items-center justify-between px-4 py-3 border-b border-[hsl(220,20%,10%)]">
        <div className="flex items-center gap-3">
          {/* SVG Logo */}
          <svg
            width="28"
            height="28"
            viewBox="0 0 32 32"
            fill="none"
            aria-label="Kalshi Trading Bot"
          >
            <rect x="2" y="2" width="28" height="28" rx="6" stroke="#22d3ee" strokeWidth="2" />
            <path
              d="M10 22V10l6 6 6-6v12"
              stroke="#22d3ee"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
          <div className="min-w-0">
            <h1 className="text-sm font-semibold text-slate-200 tracking-wide whitespace-nowrap">
              KALSHI TRADING BOT
            </h1>
            <span className="text-[10px] text-slate-500 uppercase tracking-widest hidden sm:inline">
              KXBTC15M Dashboard
            </span>
          </div>
        </div>

        <div className="flex items-center gap-4">
          {/* BTC Price */}
          {data && (
            <div className="flex items-center gap-2" data-testid="text-btc-header">
              <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
                <circle cx="8" cy="8" r="7" stroke="#f59e0b" strokeWidth="1.5" />
                <text
                  x="8"
                  y="11"
                  textAnchor="middle"
                  fill="#f59e0b"
                  fontSize="9"
                  fontWeight="bold"
                  fontFamily="Inter, sans-serif"
                >
                  ₿
                </text>
              </svg>
              <span className="text-sm font-semibold text-slate-200 tabular-nums">
                ${data.btc_price.toLocaleString(undefined, {
                  minimumFractionDigits: 2,
                  maximumFractionDigits: 2,
                })}
              </span>
              <span
                className={`text-xs tabular-nums ${
                  data.btc_momentum_1m >= 0 ? "text-emerald-400" : "text-red-400"
                }`}
              >
                {data.btc_momentum_1m >= 0 ? "+" : ""}
                {data.btc_momentum_1m.toFixed(3)}%
              </span>
            </div>
          )}

          {/* Connection indicator */}
          <div className="flex items-center gap-1.5" data-testid="status-connection">
            <div
              className={`w-2 h-2 rounded-full ${
                connected ? "bg-emerald-400 pulse-dot" : "bg-red-400"
              }`}
            />
            {connected ? (
              <Wifi size={12} className="text-emerald-400" />
            ) : (
              <WifiOff size={12} className="text-red-400" />
            )}
            <span className="text-[10px] text-slate-500 uppercase tracking-wider">
              {connected ? "Live" : "Disconnected"}
            </span>
          </div>
        </div>
      </header>

      {/* Main content */}
      <main className="flex-1 p-4 overflow-y-auto">
        {!data ? (
          <div className="flex items-center justify-center h-full">
            <div className="flex flex-col items-center gap-3">
              <div className="w-8 h-8 border-2 border-cyan-400 border-t-transparent rounded-full animate-spin" />
              <span className="text-sm text-slate-400">Connecting to market data...</span>
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-4 max-w-[1400px] mx-auto">
            {/* KPI Row */}
            <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
              <KPICard
                label="Win Rate"
                value={data.stats.win_rate}
                suffix="%"
                decimals={1}
                icon={Target}
                trend={
                  data.stats.total_trades === 0
                    ? "neutral"
                    : data.stats.win_rate >= 60
                      ? "up"
                      : data.stats.win_rate >= 40
                        ? "neutral"
                        : "down"
                }
              />
              <KPICard
                label="Total P&L"
                value={data.stats.total_pnl}
                prefix="$"
                icon={TrendingUp}
                trend={data.stats.total_pnl === 0 ? "neutral" : data.stats.total_pnl > 0 ? "up" : "down"}
                showSign
              />
              <KPICard
                label="Trades"
                value={data.stats.total_trades}
                decimals={0}
                icon={BarChart3}
                trend="neutral"
              />
              <KPICard
                label="Daily P&L"
                value={data.stats.daily_pnl}
                prefix="$"
                icon={Activity}
                trend={data.stats.daily_pnl === 0 ? "neutral" : data.stats.daily_pnl > 0 ? "up" : "down"}
                showSign
              />
            </div>

            {/* Chart */}
            <BTCChart prices={data.btc_prices} />

            {/* Market + Strategy Row */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              <CurrentMarketPanel
                market={data.current_market}
                lastSettled={data.last_settled}
              />
              <StrategySignalsPanel strategies={data.strategies} />
            </div>

            {/* Trade History */}
            <TradeHistoryTable trades={data.trades} />
          </div>
        )}
      </main>
    </div>
  );
}
