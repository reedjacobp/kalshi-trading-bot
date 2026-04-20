import React, { useMemo, useState } from "react";
import type { TickData, Trade } from "@shared/schema";
import { AnimatedNumber, Annot, Sparkline } from "@/lib/hud";

type PeriodKey = "daily" | "24h" | "7d" | "30d" | "all";
const PERIODS: { key: PeriodKey; label: string }[] = [
  { key: "daily", label: "Daily" },
  { key: "24h", label: "24h" },
  { key: "7d", label: "7d" },
  { key: "30d", label: "30d" },
  { key: "all", label: "all" },
];

// Midnight today in America/Los_Angeles, expressed as a UTC ms value.
// Uses Intl to pull current LA wall-clock and subtracts back to 00:00:00.
// DST-safe — works whether LA is in PST (UTC-8) or PDT (UTC-7).
function pacificMidnightMs(): number {
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Los_Angeles",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).formatToParts(now);
  const g = (t: string) => Number(parts.find((p) => p.type === t)!.value);
  // en-CA formats midnight as "24" sometimes — normalize to 0.
  const hour = g("hour") === 24 ? 0 : g("hour");
  const minute = g("minute");
  const second = g("second");
  const msSinceMidnight = ((hour * 60 + minute) * 60 + second) * 1000;
  return now.getTime() - msSinceMidnight;
}

function cutoffMs(period: PeriodKey): number | null {
  switch (period) {
    case "daily":
      return pacificMidnightMs();
    case "24h":
      return Date.now() - 24 * 3600_000;
    case "7d":
      return Date.now() - 7 * 24 * 3600_000;
    case "30d":
      return Date.now() - 30 * 24 * 3600_000;
    case "all":
    default:
      return null;
  }
}

function filterTrades(trades: Trade[], cutoff: number | null): Trade[] {
  if (cutoff == null) return trades;
  return trades.filter((t) => new Date(t.time).getTime() >= cutoff);
}

function PeriodStats({
  trades,
  fallbackStats,
}: {
  trades: Trade[];
  fallbackStats?: { net: number; fees: number; wins: number; losses: number };
}) {
  const settled = trades.filter((t) => t.outcome !== "pending");
  const wins = settled.filter((t) => t.outcome === "win").length;
  const losses = settled.filter((t) => t.outcome === "loss").length;
  const gross = settled.reduce((s, t) => s + t.profit, 0);
  const fees = settled.reduce((s, t) => s + t.fees, 0);
  const net = gross - fees;
  const wr = wins + losses > 0 ? wins / (wins + losses) : 0;
  return { settled, wins, losses, gross, fees, net, wr, fallbackStats };
}

export default function PnLPage({ data }: { data: TickData | null }) {
  const [period, setPeriod] = useState<PeriodKey>("daily");
  const trades = data?.trades ?? [];
  const stats = data?.stats;

  const filtered = useMemo(
    () => filterTrades(trades, cutoffMs(period)),
    [trades, period]
  );
  const clientStats = useMemo(() => PeriodStats({ trades: filtered }), [filtered]);

  // Server-authoritative totals for windows the bot pre-aggregates.
  // The SSE trades array includes reconcile-imported "phantom" sell rows that
  // skew client-side sums — use the server numbers where they exist.
  const headline = useMemo(() => {
    if (!stats) return { net: clientStats.net, gross: clientStats.gross, fees: clientStats.fees, source: "client" as const };
    switch (period) {
      case "daily":
        return {
          net: stats.daily_pnl_after_fees,
          gross: stats.daily_pnl,
          fees: stats.daily_fees ?? 0,
          source: "server" as const,
        };
      case "7d":
        return stats.weekly_pnl_net != null
          ? { net: stats.weekly_pnl_net, gross: stats.weekly_pnl ?? 0, fees: stats.weekly_fees ?? 0, source: "server" as const }
          : { ...clientStats, source: "client" as const };
      case "30d":
        return stats.monthly_pnl_net != null
          ? { net: stats.monthly_pnl_net, gross: stats.monthly_pnl ?? 0, fees: stats.monthly_fees ?? 0, source: "server" as const }
          : { ...clientStats, source: "client" as const };
      case "all":
        return {
          net: stats.alltime_pnl_net ?? stats.total_pnl_after_fees,
          gross: stats.alltime_pnl ?? stats.total_pnl,
          fees: stats.alltime_fees ?? stats.total_fees,
          source: "server" as const,
        };
      case "24h":
      default:
        return { ...clientStats, source: "client" as const };
    }
  }, [period, stats, clientStats]);

  const s = { ...clientStats, net: headline.net, gross: headline.gross, fees: headline.fees };
  const headlineSource = headline.source;

  // Cumulative P&L curve (after fees)
  const curve = useMemo(() => {
    const sorted = [...s.settled].sort(
      (a, b) => new Date(a.time).getTime() - new Date(b.time).getTime()
    );
    if (sorted.length === 0) return [0];
    let cum = 0;
    return sorted.map((t) => (cum += t.profit_after_fees));
  }, [s.settled]);

  // Max drawdown calc
  const drawdown = useMemo(() => {
    let peak = 0;
    let maxDd = 0;
    for (const v of curve) {
      peak = Math.max(peak, v);
      maxDd = Math.min(maxDd, v - peak);
    }
    return maxDd;
  }, [curve]);

  // Expectancy per trade
  const expectancy = s.settled.length > 0 ? s.net / s.settled.length : 0;

  const maxP = Math.max(1, ...curve);
  const minP = Math.min(0, ...curve);
  const rng = maxP - minP || 1;

  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">
            <span className="idx">02</span> Trade History & P&L
          </h1>
          <div className="page-sub">
            Every entry, exit, outcome and fee — with a cumulative curve and slice-and-dice filters.
          </div>
        </div>
        <div style={{ display: "flex", gap: 6 }}>
          {PERIODS.map((p) => (
            <span
              key={p.key}
              className={`chip chip--btn ${period === p.key ? "chip--accent" : ""}`}
              onClick={() => setPeriod(p.key)}
            >
              {p.label}
            </span>
          ))}
        </div>
      </div>

      <div
        className="hud-grid"
        style={{ gridTemplateColumns: "repeat(5, 1fr)", gap: 14, marginBottom: 16 }}
      >
        <div className="panel">
          <div className="panel-body">
            <div className="kpi-label">Net P&L</div>
            <AnimatedNumber
              value={s.net}
              decimals={2}
              prefix="$"
              className={`num num--xl ${s.net >= 0 ? "hud-pos" : "hud-neg"}`}
              showSign
            />
            <div className="kpi-delta">
              {s.settled.length} trades · ${s.fees.toFixed(2)} fees
              {headlineSource === "server" && (
                <span style={{ color: "var(--hud-accent)", marginLeft: 6 }}>· server</span>
              )}
            </div>
          </div>
        </div>
        <div className="panel">
          <div className="panel-body">
            <div className="kpi-label">Win Rate</div>
            <div className="num num--xl">{(s.wr * 100).toFixed(0)}%</div>
            <div className="kpi-delta">
              {s.wins}W / {s.losses}L
            </div>
          </div>
        </div>
        <div className="panel">
          <div className="panel-body">
            <div className="kpi-label">Expectancy</div>
            <div
              className={`num num--xl ${expectancy >= 0 ? "hud-pos" : "hud-neg"}`}
            >
              {expectancy >= 0 ? "+" : ""}${expectancy.toFixed(2)}
            </div>
            <div className="kpi-delta">per trade</div>
          </div>
        </div>
        <div className="panel">
          <div className="panel-body">
            <div className="kpi-label">Pending</div>
            <div className="num num--xl">{stats?.pending ?? 0}</div>
            <div className="kpi-delta">open contracts</div>
          </div>
        </div>
        <div className="panel">
          <div className="panel-body">
            <div className="kpi-label">Max Drawdown</div>
            <div className="num num--xl hud-neg">${drawdown.toFixed(2)}</div>
            <div className="kpi-delta">trough from peak</div>
          </div>
        </div>
      </div>

      <div
        className="hud-grid"
        style={{ gridTemplateColumns: "2fr 1fr", gap: 16, marginBottom: 16 }}
      >
        <div className="panel panel--accent">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">02.01</span> Cumulative P&L · after fees
            </div>
            <div style={{ display: "flex", gap: 6 }}>
              <span className={`chip ${s.net >= 0 ? "chip--pos" : "chip--neg"}`}>
                net {s.net >= 0 ? "+" : ""}${s.net.toFixed(2)}
              </span>
              <span className="chip">fees -${s.fees.toFixed(2)}</span>
            </div>
          </div>
          <div
            className="panel-body panel-body--chart"
            style={{ height: 280, position: "relative" }}
          >
            {curve.length < 2 ? (
              <div
                style={{
                  display: "grid",
                  placeItems: "center",
                  height: "100%",
                  color: "var(--ink-4)",
                  fontSize: 12,
                }}
              >
                not enough settled trades in this window
              </div>
            ) : (
              <svg
                width="100%"
                height="100%"
                viewBox="0 0 800 280"
                preserveAspectRatio="none"
                style={{ overflow: "visible" }}
              >
                <defs>
                  <linearGradient id="pnlGrad" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor="var(--pos)" stopOpacity="0.35" />
                    <stop offset="100%" stopColor="var(--pos)" stopOpacity="0" />
                  </linearGradient>
                </defs>
                <line
                  x1="0"
                  y1={260 - ((0 - minP) / rng) * 240}
                  x2="800"
                  y2={260 - ((0 - minP) / rng) * 240}
                  stroke="var(--line)"
                  strokeDasharray="3 5"
                />
                {[0, 0.25, 0.5, 0.75, 1].map((p) => (
                  <line
                    key={p}
                    x1={p * 800}
                    y1="0"
                    x2={p * 800}
                    y2="280"
                    stroke="var(--line-soft)"
                    strokeWidth="0.5"
                  />
                ))}
                <path
                  d={
                    curve
                      .map((v, i) => {
                        const x = (i / (curve.length - 1)) * 800;
                        const y = 260 - ((v - minP) / rng) * 240;
                        return `${i === 0 ? "M" : "L"}${x},${y}`;
                      })
                      .join(" ") + ` L800,280 L0,280 Z`
                  }
                  fill="url(#pnlGrad)"
                />
                <path
                  d={curve
                    .map((v, i) => {
                      const x = (i / (curve.length - 1)) * 800;
                      const y = 260 - ((v - minP) / rng) * 240;
                      return `${i === 0 ? "M" : "L"}${x},${y}`;
                    })
                    .join(" ")}
                  fill="none"
                  stroke={s.net >= 0 ? "var(--pos)" : "var(--neg)"}
                  strokeWidth="1.6"
                  vectorEffect="non-scaling-stroke"
                />
                {curve.map((v, i) => {
                  if (i % Math.max(1, Math.floor(curve.length / 8)) !== 0) return null;
                  const x = (i / (curve.length - 1)) * 800;
                  const y = 260 - ((v - minP) / rng) * 240;
                  return (
                    <circle
                      key={i}
                      cx={x}
                      cy={y}
                      r="3"
                      fill="var(--hud-accent)"
                      stroke="var(--bg-0)"
                      strokeWidth="2"
                    />
                  );
                })}
              </svg>
            )}
            {curve.length >= 2 && (
              <Annot x="38%" y={90} arrow="down">
                each dot = trade
              </Annot>
            )}
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">02.02</span> By Asset
            </div>
          </div>
          <div className="panel-body hud-stack">
            {(["BTC", "ETH", "SOL"] as const).map((t) => {
              const assetTrades = s.settled.filter((x) =>
                x.ticker.toUpperCase().includes(`KX${t}`)
              );
              const wins = assetTrades.filter((x) => x.outcome === "win").length;
              const total = assetTrades.length;
              const wr = total > 0 ? (wins / total) * 100 : 0;
              const net = assetTrades.reduce(
                (acc, x) => acc + x.profit_after_fees,
                0
              );
              return (
                <div key={t}>
                  <div className="hud-between" style={{ fontSize: 11, color: "var(--ink-3)" }}>
                    <span>{t}</span>
                    <span className="num num--sm">
                      {total} trades · {wr.toFixed(0)}%
                    </span>
                  </div>
                  <div
                    style={{
                      height: 6,
                      background: "var(--bg-2)",
                      borderRadius: 3,
                      marginTop: 4,
                      overflow: "hidden",
                    }}
                  >
                    <div
                      style={{
                        height: "100%",
                        width: `${Math.min(100, wr)}%`,
                        background:
                          "linear-gradient(90deg, var(--accent-soft), var(--hud-accent))",
                        boxShadow: "0 0 8px var(--accent-glow)",
                      }}
                    ></div>
                  </div>
                  <div
                    style={{
                      fontSize: 10,
                      color: net >= 0 ? "var(--pos)" : "var(--neg)",
                      marginTop: 4,
                      fontFamily: "var(--font-hud-mono)",
                    }}
                  >
                    {net >= 0 ? "+" : ""}${net.toFixed(2)}
                  </div>
                </div>
              );
            })}
          </div>
          <div
            className="panel-body"
            style={{ borderTop: "1px solid var(--line-soft)" }}
          >
            <div className="kpi-label">Alltime (server)</div>
            <div className="num num--lg">
              ${(stats?.alltime_pnl_net ?? stats?.total_pnl_after_fees ?? 0).toFixed(2)}
            </div>
          </div>
        </div>
      </div>

      <div className="panel">
        <div className="panel-header">
          <div className="panel-title">
            <span className="idx">02.03</span> Trades · {Math.min(filtered.length, 20)} most recent
          </div>
        </div>
        <div style={{ overflowX: "auto" }}>
          <table className="tbl">
            <thead>
              <tr>
                <th>Time</th>
                <th>Ticker</th>
                <th>Strat</th>
                <th>Side</th>
                <th>Price</th>
                <th>Ct</th>
                <th>Stake</th>
                <th>Outcome</th>
                <th>Fees</th>
                <th style={{ textAlign: "right" }}>P&L</th>
              </tr>
            </thead>
            <tbody>
              {[...filtered]
                .sort(
                  (a, b) =>
                    new Date(b.time).getTime() - new Date(a.time).getTime()
                )
                .slice(0, 20)
                .map((t, i) => {
                  const d = new Date(t.time);
                  const time = `${String(d.getHours()).padStart(2, "0")}:${String(
                    d.getMinutes()
                  ).padStart(2, "0")}:${String(d.getSeconds()).padStart(2, "0")}`;
                  const outcomeChip =
                    t.outcome === "win" ? (
                      <span className="chip chip--pos">WIN</span>
                    ) : t.outcome === "loss" ? (
                      <span className="chip chip--neg">LOSS</span>
                    ) : (
                      <span className="chip chip--warn">PEND</span>
                    );
                  const net = t.profit_after_fees;
                  return (
                    <tr key={i}>
                      <td>{time}</td>
                      <td>{t.ticker}</td>
                      <td>{t.strategy}</td>
                      <td>
                        <span className={t.side === "yes" ? "hud-pos" : "hud-neg"}>
                          {t.side.toUpperCase()}
                        </span>
                      </td>
                      <td>{t.price}¢</td>
                      <td>{t.contracts}</td>
                      <td>${t.stake.toFixed(2)}</td>
                      <td>{outcomeChip}</td>
                      <td>-${t.fees.toFixed(2)}</td>
                      <td
                        style={{ textAlign: "right" }}
                        className={net >= 0 ? "hud-pos" : "hud-neg"}
                      >
                        {net >= 0 ? "+" : ""}${net.toFixed(2)}
                      </td>
                    </tr>
                  );
                })}
              {filtered.length === 0 && (
                <tr>
                  <td colSpan={10} style={{ textAlign: "center", color: "var(--ink-4)" }}>
                    no trades in this window
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
