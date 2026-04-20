import React from "react";
import type { TickData } from "@shared/schema";
import { Signal, Sparkline } from "@/lib/hud";
import { postPause } from "@/lib/sse";

const ASSETS = ["BTC", "ETH", "SOL"] as const;

export default function RiskPage({ data }: { data: TickData | null }) {
  const stats = data?.stats;
  const enabled = stats ? !stats.bot_paused : true;
  const [busy, setBusy] = React.useState(false);
  const matrix = data?.strategy_matrix ?? [];
  const rrCfg = data?.rr_config;

  const togglePause = async () => {
    if (busy) return;
    setBusy(true);
    try {
      await postPause(enabled);
    } finally {
      setBusy(false);
    }
  };

  // Exposure: open/pending trades only
  const pending = (data?.trades ?? []).filter((t) => t.outcome === "pending");
  const exposureByAsset = ASSETS.map((t) => {
    const rows = pending.filter((p) => p.ticker.toUpperCase().includes(`KX${t}`));
    const stake = rows.reduce((s, r) => s + r.stake, 0);
    return { asset: t, stake, count: rows.length };
  });
  const totalExposure = exposureByAsset.reduce((s, x) => s + x.stake, 0);

  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">
            <span className="idx">04</span> Risk & Config
          </h1>
          <div className="page-sub">
            The cockpit: live balance, gate parameters, exposure, and a kill-switch.
          </div>
        </div>
        <button
          className="hud-btn hud-btn--primary"
          onClick={togglePause}
          disabled={busy}
          style={{
            background: enabled
              ? "oklch(0.72 0.18 25 / 0.2)"
              : "var(--accent-soft)",
            borderColor: enabled ? "var(--neg)" : "var(--hud-accent)",
            color: enabled ? "var(--neg)" : "var(--hud-accent)",
            opacity: busy ? 0.5 : 1,
          }}
        >
          <span
            className="hud-dot"
            style={{ background: enabled ? "var(--neg)" : "var(--hud-accent)" }}
          ></span>
          {enabled ? "KILL SWITCH" : "RESUME TRADING"}
        </button>
      </div>

      <div
        className="hud-grid"
        style={{ gridTemplateColumns: "1fr 1fr 1fr", gap: 16, marginBottom: 16 }}
      >
        <div className="panel panel--accent">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">04.01</span> Power
            </div>
          </div>
          <div
            className="panel-body"
            style={{
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              gap: 12,
              padding: 24,
            }}
          >
            <div
              style={{
                width: 110,
                height: 110,
                borderRadius: "50%",
                border: `2px solid ${enabled ? "var(--hud-accent)" : "var(--neg)"}`,
                display: "grid",
                placeItems: "center",
                boxShadow: enabled
                  ? "0 0 30px -5px var(--accent-glow), inset 0 0 20px var(--accent-soft)"
                  : "0 0 30px -5px oklch(0.72 0.18 25 / 0.35), inset 0 0 20px oklch(0.72 0.18 25 / 0.2)",
                position: "relative",
              }}
            >
              <div
                style={{
                  position: "absolute",
                  inset: 8,
                  borderRadius: "50%",
                  border: "1px dashed var(--line)",
                }}
              ></div>
              <div
                className="num num--xl"
                style={{ color: enabled ? "var(--hud-accent)" : "var(--neg)" }}
              >
                {enabled ? "ON" : "OFF"}
              </div>
            </div>
            <div
              className={`hud-toggle ${enabled ? "is-on" : ""}`}
              onClick={togglePause}
              style={{ pointerEvents: busy ? "none" : "auto", opacity: busy ? 0.5 : 1 }}
            >
              <span>Trading enabled</span>
              <span className="hud-toggle-track">
                <span className="hud-toggle-thumb"></span>
              </span>
            </div>
            <div style={{ fontSize: 11, color: "var(--ink-3)", textAlign: "center" }}>
              {stats?.is_paper ? "PAPER" : "LIVE"} · Balance $
              {(stats?.live_balance ?? stats?.paper_balance ?? 0).toFixed(2)}
            </div>
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">04.02</span> RR Defaults
            </div>
          </div>
          <div className="panel-body">
            {rrCfg ? (
              <table className="tbl">
                <tbody>
                  {(
                    [
                      ["min_contract_price", rrCfg.defaults.min_contract_price.toFixed(2)],
                      ["max_entry_price", rrCfg.defaults.max_entry_price.toFixed(2)],
                      ["min_seconds", String(rrCfg.defaults.min_seconds)],
                      ["max_seconds", String(rrCfg.defaults.max_seconds)],
                      [
                        "min_buffer_pct",
                        `${rrCfg.defaults.min_price_buffer_pct.toFixed(2)}%`,
                      ],
                      [
                        "max_adv_momentum",
                        rrCfg.defaults.max_adverse_momentum.toFixed(4),
                      ],
                      ["max_stake_usd", `$${rrCfg.defaults.max_stake_usd.toFixed(2)}`],
                    ] as const
                  ).map(([k, v]) => (
                    <tr key={k}>
                      <td style={{ color: "var(--ink-3)" }}>{k}</td>
                      <td
                        style={{ textAlign: "right", color: "var(--hud-accent)" }}
                      >
                        {v}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <div style={{ color: "var(--ink-4)", padding: 8, fontSize: 12 }}>
                awaiting rr_config…
              </div>
            )}
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">04.03</span> Exposure
            </div>
          </div>
          <div className="panel-body hud-stack">
            {exposureByAsset.map((e) => {
              const pct = totalExposure > 0 ? (e.stake / totalExposure) * 100 : 0;
              return (
                <div key={e.asset}>
                  <div className="hud-between" style={{ fontSize: 11, color: "var(--ink-3)" }}>
                    <span>{e.asset}</span>
                    <span className="num num--sm">
                      ${e.stake.toFixed(2)} · {e.count} ct
                    </span>
                  </div>
                  <div
                    style={{
                      height: 8,
                      background: "var(--bg-2)",
                      borderRadius: 4,
                      marginTop: 4,
                      overflow: "hidden",
                    }}
                  >
                    <div
                      style={{
                        height: "100%",
                        width: `${pct}%`,
                        background:
                          "linear-gradient(90deg, var(--accent-soft), var(--hud-accent))",
                        boxShadow: "inset 0 0 8px var(--accent-glow)",
                      }}
                    ></div>
                  </div>
                </div>
              );
            })}
            <div
              className="hud-between"
              style={{
                paddingTop: 12,
                borderTop: "1px solid var(--line-soft)",
                marginTop: 4,
              }}
            >
              <span className="kpi-label" style={{ margin: 0 }}>
                Total at risk
              </span>
              <div className="num num--lg">${totalExposure.toFixed(2)}</div>
            </div>
          </div>
        </div>
      </div>

      <div className="panel" style={{ marginBottom: 16 }}>
        <div className="panel-header">
          <div className="panel-title">
            <span className="idx">04.04</span> Strategy Matrix
          </div>
          <span className="chip">{matrix.length} rows</span>
        </div>
        <div className="panel-body">
          {matrix.length === 0 ? (
            <div style={{ color: "var(--ink-4)", fontSize: 12, padding: 8 }}>
              no strategy_matrix on the feed — bot hasn't published yet
            </div>
          ) : (
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "90px repeat(auto-fit, minmax(160px, 1fr))",
                gap: 1,
                background: "var(--line-soft)",
                border: "1px solid var(--line-soft)",
                borderRadius: 8,
                overflow: "hidden",
              }}
            >
              {(() => {
                const strategies = Array.from(
                  new Set(matrix.map((m) => m.strategy))
                );
                const assets = Array.from(new Set(matrix.map((m) => m.asset)));
                const header: React.ReactNode[] = [
                  <div
                    key="h0"
                    style={{
                      background: "var(--bg-1)",
                      padding: "10px 12px",
                      fontSize: 10,
                      letterSpacing: "0.14em",
                      color: "var(--ink-4)",
                      textTransform: "uppercase",
                    }}
                  ></div>,
                  ...strategies.map((s) => (
                    <div
                      key={s}
                      style={{
                        background: "var(--bg-1)",
                        padding: "10px 12px",
                        fontSize: 10,
                        letterSpacing: "0.14em",
                        color: "var(--ink-4)",
                        textTransform: "uppercase",
                      }}
                    >
                      {s}
                    </div>
                  )),
                ];
                const rows: React.ReactNode[] = assets.flatMap((a) => [
                  <div
                    key={`a-${a}`}
                    style={{
                      background: "var(--bg-1)",
                      padding: "14px 12px",
                      fontFamily: "var(--font-hud-mono)",
                      fontSize: 12,
                      color: "var(--ink-2)",
                      display: "flex",
                      alignItems: "center",
                    }}
                  >
                    {a.toUpperCase()}
                  </div>,
                  ...strategies.map((s) => {
                    const cell = matrix.find(
                      (m) => m.asset === a && m.strategy === s
                    );
                    const edge = cell?.edge ?? 0;
                    const status = cell?.status ?? "disabled";
                    const hue = edge > 0 ? "var(--pos)" : "var(--neg)";
                    const color =
                      status === "enabled"
                        ? hue
                        : status === "shadow"
                        ? "var(--warn)"
                        : "var(--line)";
                    const intensity = Math.min(1, Math.abs(edge) / 0.7);
                    return (
                      <div
                        key={`${a}-${s}`}
                        style={{
                          background: `linear-gradient(135deg, oklch(0.22 0.02 250) 0%, ${
                            edge > 0
                              ? "oklch(0.55 0.15 160"
                              : "oklch(0.55 0.18 25"
                          } / ${intensity * 0.3}) 100%)`,
                          padding: 14,
                          borderLeft: `3px solid ${color}`,
                          position: "relative",
                        }}
                      >
                        <div className="hud-between">
                          <span
                            style={{
                              fontSize: 10,
                              letterSpacing: "0.12em",
                              color: "var(--ink-4)",
                              textTransform: "uppercase",
                            }}
                          >
                            {status}
                          </span>
                          <span
                            className={`num num--sm ${
                              edge >= 0 ? "hud-pos" : "hud-neg"
                            }`}
                          >
                            {edge >= 0 ? "+" : ""}
                            {edge.toFixed(2)}
                          </span>
                        </div>
                        <div
                          style={{
                            fontFamily: "var(--font-hud-mono)",
                            fontSize: 10,
                            color: "var(--ink-3)",
                            marginTop: 6,
                          }}
                        >
                          WR{" "}
                          {cell?.win_rate != null
                            ? (cell.win_rate * 100).toFixed(0) + "%"
                            : "—"}{" "}
                          · {cell?.trades ?? 0}t · $
                          {(cell?.recent_pnl ?? 0).toFixed(0)}
                        </div>
                      </div>
                    );
                  }),
                ]);
                return [...header, ...rows];
              })()}
            </div>
          )}
        </div>
      </div>

      <div className="hud-grid" style={{ gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">04.05</span> Per-Cell Config
            </div>
            <span className="chip">
              {Object.keys(rrCfg?.per_cell ?? {}).length} cells
            </span>
          </div>
          <div
            className="panel-body"
            style={{ maxHeight: 360, overflow: "auto" }}
          >
            {rrCfg ? (
              <table className="tbl">
                <thead>
                  <tr>
                    <th>Cell</th>
                    <th>Price</th>
                    <th>Secs</th>
                    <th>Buffer</th>
                    <th>CV WR</th>
                    <th style={{ textAlign: "right" }}>Trades</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(rrCfg.per_cell)
                    .slice(0, 30)
                    .map(([k, cell]) => (
                      <tr key={k}>
                        <td style={{ color: "var(--ink-3)" }}>{k}</td>
                        <td>{cell.price}</td>
                        <td>{cell.max_secs}</td>
                        <td>{cell.buffer}</td>
                        <td>
                          {cell.cv_wr != null
                            ? (cell.cv_wr * 100).toFixed(0) + "%"
                            : "—"}
                        </td>
                        <td style={{ textAlign: "right" }}>{cell.cv_trades}</td>
                      </tr>
                    ))}
                </tbody>
              </table>
            ) : (
              <div style={{ color: "var(--ink-4)", fontSize: 12 }}>
                no rr_config published
              </div>
            )}
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">04.06</span> Reconciliation
            </div>
            <Signal kind="yes" label="SYNCED" />
          </div>
          <div className="panel-body">
            <table className="tbl">
              <tbody>
                {(
                  [
                    [
                      "Balance",
                      `$${(
                        stats?.live_balance ??
                        stats?.paper_balance ??
                        0
                      ).toFixed(2)}`,
                    ],
                    ["Total P&L (net)", `$${(stats?.total_pnl_after_fees ?? 0).toFixed(2)}`],
                    ["Alltime fees", `$${(stats?.alltime_fees ?? stats?.total_fees ?? 0).toFixed(2)}`],
                    ["Open positions", String(stats?.pending ?? 0)],
                    ["Total trades", String(stats?.total_trades ?? 0)],
                    ["Win rate", `${((stats?.win_rate ?? 0) * 100).toFixed(0)}%`],
                  ] as const
                ).map(([k, v]) => (
                  <tr key={k}>
                    <td style={{ color: "var(--ink-3)" }}>{k}</td>
                    <td style={{ textAlign: "right" }}>{v}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </>
  );
}
