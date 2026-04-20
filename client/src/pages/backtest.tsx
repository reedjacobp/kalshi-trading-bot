import React, { useMemo } from "react";
import type { TickData } from "@shared/schema";
import { Sparkline } from "@/lib/hud";

// Given rr_config.per_cell (key looks like "<asset>:<bucket>" or similar),
// render a heatmap of CV win rate × trade count.
function parsePerCell(
  rrCfg: TickData["rr_config"] | undefined
): {
  cells: { key: string; wr: number | null; trades: number; price: string; maxSecs: number; buffer: string }[];
} {
  if (!rrCfg) return { cells: [] };
  const cells = Object.entries(rrCfg.per_cell).map(([key, cell]) => ({
    key,
    wr: cell.cv_wr,
    trades: cell.cv_trades,
    price: cell.price,
    maxSecs: cell.max_secs,
    buffer: cell.buffer,
  }));
  return { cells };
}

export default function BacktestPage({ data }: { data: TickData | null }) {
  const rrCfg = data?.rr_config;
  const matrix = data?.strategy_matrix ?? [];

  const { cells } = useMemo(() => parsePerCell(rrCfg), [rrCfg]);

  // KPIs: best WR cell, best coverage, total cells
  const withWR = cells.filter((c) => c.wr != null && c.trades >= 5);
  const bestWR = withWR.length
    ? withWR.reduce((best, c) => ((c.wr ?? 0) > (best.wr ?? 0) ? c : best))
    : null;
  const totalCells = cells.length;
  const activeCells = cells.filter((c) => c.trades > 0).length;
  const totalCVTrades = cells.reduce((s, c) => s + c.trades, 0);
  const avgWR =
    withWR.length > 0
      ? withWR.reduce((s, c) => s + (c.wr ?? 0), 0) / withWR.length
      : 0;

  // Matrix live vs shadow PnL totals
  const livePnl = matrix
    .filter((m) => m.status === "enabled")
    .reduce((s, m) => s + m.recent_pnl, 0);
  const shadowPnl = matrix
    .filter((m) => m.status === "shadow")
    .reduce((s, m) => s + m.recent_pnl, 0);

  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">
            <span className="idx">05</span> Backtest · Analytics
          </h1>
          <div className="page-sub">
            Per-cell cross-validation from the optimizer, strategy matrix roll-up, and
            parameter importance.
          </div>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <span className="chip chip--accent">CV</span>
          <span className="chip">live</span>
          <span className="chip">shadow</span>
        </div>
      </div>

      <div
        className="hud-grid"
        style={{ gridTemplateColumns: "repeat(4, 1fr)", gap: 14, marginBottom: 16 }}
      >
        {[
          [
            "Best CV WR",
            bestWR?.wr != null ? `${(bestWR.wr * 100).toFixed(0)}%` : "—",
            bestWR
              ? `${bestWR.key} · ${bestWR.trades}t`
              : "no cells yet",
          ],
          [
            "Avg CV WR",
            withWR.length > 0 ? `${(avgWR * 100).toFixed(0)}%` : "—",
            `${withWR.length} cells ≥5 trades`,
          ],
          [
            "Cell coverage",
            `${activeCells} / ${totalCells}`,
            totalCells > 0
              ? `${((activeCells / totalCells) * 100).toFixed(0)}% explored`
              : "",
          ],
          [
            "CV trades",
            String(totalCVTrades),
            `${matrix.length} matrix rows`,
          ],
        ].map(([k, v, s], i) => (
          <div key={i} className="panel">
            <div className="panel-body">
              <div className="kpi-label">{k}</div>
              <div className="num num--xl" style={{ margin: "4px 0" }}>
                {v}
              </div>
              <div className="kpi-delta">{s}</div>
            </div>
          </div>
        ))}
      </div>

      <div
        className="hud-grid"
        style={{ gridTemplateColumns: "1.3fr 1fr", gap: 16, marginBottom: 16 }}
      >
        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">05.01</span> Per-cell CV heatmap
            </div>
            <span className="chip">WR × trades</span>
          </div>
          <div className="panel-body">
            {cells.length === 0 ? (
              <div style={{ color: "var(--ink-4)", fontSize: 12, padding: 8 }}>
                no per_cell data in rr_config
              </div>
            ) : (
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns:
                    "repeat(auto-fill, minmax(110px, 1fr))",
                  gap: 6,
                }}
              >
                {cells.slice(0, 60).map((c) => {
                  const wr = c.wr ?? 0;
                  const sig =
                    c.wr != null && c.trades >= 10 ? Math.min(1, wr) : 0;
                  const isBest = bestWR?.key === c.key && c.wr != null;
                  return (
                    <div
                      key={c.key}
                      title={`${c.key} · WR ${
                        c.wr != null ? (c.wr * 100).toFixed(0) + "%" : "—"
                      } · ${c.trades}t`}
                      style={{
                        aspectRatio: "1.6",
                        background: `oklch(0.82 ${
                          0.05 + sig * 0.14
                        } 220 / ${0.12 + sig * 0.55})`,
                        border: isBest
                          ? "1.5px solid var(--hud-accent)"
                          : "1px solid var(--line-soft)",
                        borderRadius: 4,
                        display: "grid",
                        placeItems: "center",
                        fontFamily: "var(--font-hud-mono)",
                        fontSize: 10,
                        color: sig > 0.5 ? "var(--ink)" : "var(--ink-3)",
                        padding: 4,
                        textAlign: "center",
                        boxShadow: isBest
                          ? "0 0 20px -6px var(--accent-glow)"
                          : "none",
                        lineHeight: 1.2,
                      }}
                    >
                      <div>
                        <div style={{ fontSize: 10 }}>
                          {c.wr != null ? (c.wr * 100).toFixed(0) + "%" : "—"}
                        </div>
                        <div
                          style={{ fontSize: 8, color: "var(--ink-4)", marginTop: 2 }}
                        >
                          {c.trades}t · {c.price}
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
            <div
              style={{
                fontSize: 10,
                color: "var(--ink-4)",
                marginTop: 10,
                letterSpacing: "0.12em",
                textTransform: "uppercase",
              }}
            >
              intensity = wr (trades ≥ 10) · first 60 cells shown
            </div>
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">05.02</span> Live vs Shadow
            </div>
          </div>
          <div
            className="panel-body"
            style={{
              display: "flex",
              justifyContent: "space-between",
              fontSize: 11,
              color: "var(--ink-3)",
              gap: 12,
              padding: 16,
            }}
          >
            <div style={{ flex: 1, textAlign: "center" }}>
              <div className="kpi-label">Live</div>
              <div
                className={`num num--xl ${livePnl >= 0 ? "hud-pos" : "hud-neg"}`}
              >
                {livePnl >= 0 ? "+" : ""}${livePnl.toFixed(0)}
              </div>
              <div style={{ fontSize: 10, marginTop: 4 }}>
                {matrix.filter((m) => m.status === "enabled").length} enabled
              </div>
            </div>
            <div style={{ flex: 1, textAlign: "center" }}>
              <div className="kpi-label">Shadow</div>
              <div
                className={`num num--xl ${shadowPnl >= 0 ? "hud-pos" : "hud-neg"}`}
                style={{ borderBottom: "1px dashed var(--pos)", display: "inline-block", padding: "0 4px" }}
              >
                {shadowPnl >= 0 ? "+" : ""}${shadowPnl.toFixed(0)}
              </div>
              <div style={{ fontSize: 10, marginTop: 4 }}>
                {matrix.filter((m) => m.status === "shadow").length} shadow
              </div>
            </div>
          </div>
          <div
            className="panel-body"
            style={{ borderTop: "1px solid var(--line-soft)" }}
          >
            <table className="tbl">
              <thead>
                <tr>
                  <th>Strategy × Asset</th>
                  <th>Edge</th>
                  <th>Shadow</th>
                  <th>WR</th>
                  <th style={{ textAlign: "right" }}>7d P&L</th>
                </tr>
              </thead>
              <tbody>
                {matrix.slice(0, 12).map((m, i) => (
                  <tr key={i}>
                    <td style={{ color: "var(--ink-3)" }}>
                      {m.asset.toUpperCase()} · {m.strategy}
                    </td>
                    <td
                      className={
                        m.edge != null && m.edge >= 0 ? "hud-pos" : "hud-neg"
                      }
                    >
                      {m.edge != null ? m.edge.toFixed(2) : "—"}
                    </td>
                    <td style={{ color: "var(--ink-3)" }}>
                      {m.shadow_edge != null ? m.shadow_edge.toFixed(2) : "—"}
                    </td>
                    <td>
                      {m.win_rate != null
                        ? (m.win_rate * 100).toFixed(0) + "%"
                        : "—"}
                    </td>
                    <td
                      style={{ textAlign: "right" }}
                      className={m.recent_pnl >= 0 ? "hud-pos" : "hud-neg"}
                    >
                      {m.recent_pnl >= 0 ? "+" : ""}${m.recent_pnl.toFixed(0)}
                    </td>
                  </tr>
                ))}
                {matrix.length === 0 && (
                  <tr>
                    <td
                      colSpan={5}
                      style={{ textAlign: "center", color: "var(--ink-4)" }}
                    >
                      no strategy_matrix data
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="hud-grid" style={{ gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">05.03</span> Recent trades P&L distribution
            </div>
          </div>
          <div
            className="panel-body panel-body--chart"
            style={{ height: 180, position: "relative" }}
          >
            <TradeDistribution data={data} />
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">05.04</span> Days disabled (7d) · by cell
            </div>
            <span className="chip">{matrix.length} strategies</span>
          </div>
          <div className="panel-body hud-stack">
            {matrix.length === 0 && (
              <div style={{ color: "var(--ink-4)", fontSize: 12, padding: 8 }}>
                no strategy_matrix
              </div>
            )}
            {matrix
              .slice()
              .sort((a, b) => b.days_disabled_7d - a.days_disabled_7d)
              .slice(0, 10)
              .map((m, i) => {
                const pct = m.days_disabled_7d / 7;
                return (
                  <div key={i}>
                    <div
                      className="hud-between"
                      style={{
                        fontSize: 11,
                        color: "var(--ink-3)",
                        fontFamily: "var(--font-hud-mono)",
                      }}
                    >
                      <span>
                        {m.asset.toUpperCase()} · {m.strategy}
                      </span>
                      <span>{m.days_disabled_7d.toFixed(1)}d</span>
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
                          width: `${pct * 100}%`,
                          background:
                            "linear-gradient(90deg, var(--accent-soft), var(--hud-accent))",
                          boxShadow: "0 0 8px var(--accent-glow)",
                        }}
                      ></div>
                    </div>
                  </div>
                );
              })}
          </div>
        </div>
      </div>
    </>
  );
}

// Simple histogram of P&L per trade (net after fees)
function TradeDistribution({ data }: { data: TickData | null }) {
  const settled = (data?.trades ?? []).filter((t) => t.outcome !== "pending");
  if (settled.length < 3) {
    return (
      <div
        style={{
          display: "grid",
          placeItems: "center",
          height: "100%",
          color: "var(--ink-4)",
          fontSize: 12,
        }}
      >
        need at least 3 settled trades
      </div>
    );
  }
  const vals = settled.map((t) => t.profit_after_fees);
  const min = Math.min(...vals);
  const max = Math.max(...vals);
  const span = max - min || 1;
  const bins = 16;
  const counts = new Array(bins).fill(0) as number[];
  vals.forEach((v) => {
    const idx = Math.min(bins - 1, Math.floor(((v - min) / span) * bins));
    counts[idx] += 1;
  });
  const maxC = Math.max(...counts, 1);
  const W = 500;
  const H = 180;
  const bw = W / bins;
  return (
    <svg width="100%" height="100%" viewBox={`0 0 ${W} ${H}`} preserveAspectRatio="none">
      {counts.map((c, i) => {
        const h = (c / maxC) * (H - 20);
        const binLo = min + (i / bins) * span;
        const binHi = min + ((i + 1) / bins) * span;
        const isNeg = binHi < 0;
        const straddleZero = binLo < 0 && binHi >= 0;
        const color = isNeg
          ? "var(--neg)"
          : straddleZero
          ? "var(--ink-4)"
          : "var(--pos)";
        return (
          <rect
            key={i}
            x={i * bw + 2}
            y={H - h - 10}
            width={bw - 4}
            height={h}
            fill={color}
            opacity="0.6"
            stroke={color}
            strokeWidth="1"
          />
        );
      })}
      <line
        x1={((0 - min) / span) * W}
        y1="0"
        x2={((0 - min) / span) * W}
        y2={H}
        stroke="var(--line)"
        strokeDasharray="3 4"
      />
    </svg>
  );
}
