import React, { useEffect, useState } from "react";
import type { TickData } from "@shared/schema";
import { AnimatedNumber, Annot, Signal, fmtSeconds } from "@/lib/hud";
import { MiniLadder } from "@/lib/mini-ladder";

type Asset = "btc" | "eth" | "sol";
const ASSETS: { key: Asset; t: string }[] = [
  { key: "btc", t: "BTC" },
  { key: "eth", t: "ETH" },
  { key: "sol", t: "SOL" },
];

// Six-dimension gate radar from real data
function GateRadar({ data, asset }: { data: TickData; asset: Asset }) {
  const mom = data.asset_momentum?.[asset];
  const sig = data.strategies_by_asset?.[asset]?.resolution_rider;
  const cx = 110,
    cy = 110,
    r = 80;
  const clamp = (v: number) => Math.max(0, Math.min(1, v));
  const dims = [
    { label: "mom1", v: clamp(Math.abs(data[`${asset}_momentum_1m` as const]) * 25) },
    { label: "mom5", v: clamp(Math.abs(data[`${asset}_momentum_5m` as const]) * 12) },
    { label: "vol", v: clamp(mom?.realized_vol ?? data.vol_reading) },
    { label: "ofi", v: clamp(Math.abs(data.ofi)) },
    { label: "edge", v: clamp(sig?.confidence ?? 0) },
    {
      label: "buf",
      v: clamp(
        data.current_market
          ? Math.abs(data.current_market.yes_ask - data.current_market.yes_bid) / 10
          : 0
      ),
    },
  ];
  const pt = (i: number, mag: number) => {
    const a = (i / dims.length) * Math.PI * 2 - Math.PI / 2;
    return [cx + Math.cos(a) * r * mag, cy + Math.sin(a) * r * mag] as const;
  };
  const poly = dims.map((d, i) => pt(i, d.v).join(",")).join(" ");
  return (
    <svg width="220" height="220" viewBox="0 0 220 220">
      {[0.25, 0.5, 0.75, 1].map((m) => (
        <polygon
          key={m}
          points={dims.map((_, i) => pt(i, m).join(",")).join(" ")}
          fill="none"
          stroke="var(--line-soft)"
          strokeWidth="0.8"
        />
      ))}
      {dims.map((d, i) => {
        const [x, y] = pt(i, 1);
        return (
          <line
            key={i}
            x1={cx}
            y1={cy}
            x2={x}
            y2={y}
            stroke="var(--line-soft)"
            strokeWidth="0.6"
          />
        );
      })}
      <polygon
        points={poly}
        fill="var(--accent-soft)"
        stroke="var(--hud-accent)"
        strokeWidth="1.4"
      />
      {dims.map((d, i) => {
        const [x, y] = pt(i, d.v);
        return <circle key={i} cx={x} cy={y} r="2.5" fill="var(--hud-accent)" />;
      })}
      {dims.map((d, i) => {
        const [x, y] = pt(i, 1.18);
        return (
          <text
            key={i}
            x={x}
            y={y}
            fill="var(--ink-3)"
            fontSize="9"
            fontFamily="var(--font-hud-mono)"
            textAnchor="middle"
            dominantBaseline="middle"
            letterSpacing="0.1em"
          >
            {d.label}
          </text>
        );
      })}
    </svg>
  );
}

export default function MarketPage({ data }: { data: TickData | null }) {
  const [asset, setAsset] = useState<Asset>("btc");
  const market = data?.markets[asset] ?? data?.current_market ?? null;
  const sig = data?.strategies_by_asset?.[asset]?.resolution_rider;
  const mom = data?.asset_momentum?.[asset];
  const [countdown, setCountdown] = useState(market?.seconds_remaining ?? 0);
  useEffect(() => {
    if (market) setCountdown(market.seconds_remaining);
  }, [market?.seconds_remaining]);
  useEffect(() => {
    const id = setInterval(() => setCountdown((c) => Math.max(0, c - 1)), 1000);
    return () => clearInterval(id);
  }, []);

  const rrCfg = data?.rr_config?.defaults;
  // Build entry-decision trace from real gates
  const decisionTrace: [string, string, boolean][] = [];
  if (market && rrCfg) {
    const yesMid = (market.yes_bid + market.yes_ask) / 2;
    decisionTrace.push([
      "min_contract_price",
      `${yesMid / 100} ≥ ${rrCfg.min_contract_price}`,
      yesMid / 100 >= rrCfg.min_contract_price,
    ]);
    decisionTrace.push([
      "max_entry_price",
      `${yesMid / 100} ≤ ${rrCfg.max_entry_price}`,
      yesMid / 100 <= rrCfg.max_entry_price,
    ]);
    decisionTrace.push([
      "seconds_remaining",
      `${market.seconds_remaining}s ∈ [${rrCfg.min_seconds}, ${rrCfg.max_seconds}]`,
      market.seconds_remaining >= rrCfg.min_seconds &&
        market.seconds_remaining <= rrCfg.max_seconds,
    ]);
    const vol = mom?.realized_vol;
    const volGate = mom?.vol_gate;
    if (vol != null && volGate != null) {
      decisionTrace.push([
        "volatility_gate",
        `${vol.toFixed(3)} ≤ ${volGate.toFixed(3)}`,
        vol <= volGate,
      ]);
    }
    const momCell = mom?.mom_cell;
    const momGate = mom?.mom_gate;
    if (momCell != null && momGate != null) {
      decisionTrace.push([
        "momentum_gate",
        `${momCell.toFixed(4)} ≥ ${momGate.toFixed(4)}`,
        momCell >= momGate,
      ]);
    }
  }

  if (!data || !market) {
    return (
      <>
        <HeaderBar asset={asset} setAsset={setAsset} market={null} countdown={0} />
        <div className="panel" style={{ padding: 32, textAlign: "center", color: "var(--ink-3)" }}>
          No active market for {asset.toUpperCase()}.
        </div>
      </>
    );
  }

  const prices = data[`${asset}_prices` as const];
  const ps = prices.length >= 2 ? prices.map(([, p]) => p) : [0, 0];
  const floor = market.floor_strike ?? null;
  const cap = market.cap_strike ?? null;
  const lo = Math.min(...ps, floor ?? Infinity);
  const hi = Math.max(...ps, cap ?? -Infinity);
  const range = hi - lo || 1;
  const pad = range * 0.1;
  const yMin = lo - pad;
  const yMax = hi + pad;
  const rng = yMax - yMin || 1;
  const scaleY = (p: number) => 240 - ((p - yMin) / rng) * 220 + 10;

  return (
    <>
      <HeaderBar asset={asset} setAsset={setAsset} market={market} countdown={countdown} />

      <div
        className="hud-grid"
        style={{ gridTemplateColumns: "1.2fr 1fr 1fr", gap: 16, marginBottom: 16 }}
      >
        <div className="panel panel--accent" style={{ position: "relative" }}>
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">03.01</span> Price vs Strike Band
            </div>
            <span
              className={`chip ${
                (floor != null && ps[ps.length - 1] > floor) ||
                (cap != null && ps[ps.length - 1] < cap)
                  ? "chip--pos"
                  : "chip--warn"
              }`}
            >
              {floor != null
                ? ps[ps.length - 1] > floor
                  ? "above floor"
                  : "below floor"
                : cap != null
                ? ps[ps.length - 1] < cap
                  ? "below cap"
                  : "above cap"
                : "—"}
            </span>
          </div>
          <div
            className="panel-body panel-body--chart"
            style={{ height: 260, position: "relative" }}
          >
            <svg
              width="100%"
              height="100%"
              viewBox="0 0 500 260"
              preserveAspectRatio="none"
              style={{ overflow: "visible" }}
            >
              {cap != null && (
                <>
                  <rect
                    x="0"
                    y={scaleY(cap)}
                    width="500"
                    height={floor != null ? scaleY(floor) - scaleY(cap) : 0}
                    fill="var(--accent-soft)"
                    opacity="0.25"
                  />
                  <line
                    x1="0"
                    y1={scaleY(cap)}
                    x2="500"
                    y2={scaleY(cap)}
                    stroke="var(--neg)"
                    strokeDasharray="4 6"
                  />
                  <text
                    x="6"
                    y={scaleY(cap) - 6}
                    fontSize="9"
                    fill="var(--neg)"
                    fontFamily="var(--font-hud-mono)"
                  >
                    {cap.toLocaleString()} CAP
                  </text>
                </>
              )}
              {floor != null && (
                <>
                  <line
                    x1="0"
                    y1={scaleY(floor)}
                    x2="500"
                    y2={scaleY(floor)}
                    stroke="var(--pos)"
                    strokeDasharray="4 6"
                  />
                  <text
                    x="6"
                    y={scaleY(floor) + 14}
                    fontSize="9"
                    fill="var(--pos)"
                    fontFamily="var(--font-hud-mono)"
                  >
                    {floor.toLocaleString()} FLR
                  </text>
                </>
              )}
              <path
                d={ps
                  .map((p, i) => {
                    const x = (i / (ps.length - 1)) * 500;
                    return `${i === 0 ? "M" : "L"}${x},${scaleY(p)}`;
                  })
                  .join(" ")}
                fill="none"
                stroke="var(--hud-accent)"
                strokeWidth="1.4"
                vectorEffect="non-scaling-stroke"
              />
            </svg>
            <div className="scan"></div>
          </div>
          {(floor != null || cap != null) && (
            <Annot x="52%" y={50} arrow="down">
              settlement band
            </Annot>
          )}
        </div>

        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">03.02</span> Momentum Stack
            </div>
          </div>
          <div className="panel-body hud-stack">
            {[
              ["mom_1m", data[`${asset}_momentum_1m` as const] * 100, "%"],
              ["mom_5m", data[`${asset}_momentum_5m` as const] * 100, "%"],
              ["mom_cell", (mom?.mom_cell ?? 0) * 1000, "bp"],
              ["mom_gate", (mom?.mom_gate ?? 0) * 1000, "bp"],
              ["realized_vol", (mom?.realized_vol ?? 0) * 1000, "bp"],
              ["vol_gate", -(mom?.vol_gate ?? 0) * 1000, "bp"],
            ].map(([k, vRaw]) => {
              const v = Number(vRaw) || 0;
              const pos = v >= 0;
              const maxAbs = 5;
              const normV = Math.max(-maxAbs, Math.min(maxAbs, v)) / maxAbs;
              return (
                <div key={String(k)}>
                  <div
                    className="hud-between"
                    style={{
                      fontSize: 10,
                      letterSpacing: "0.12em",
                      color: "var(--ink-3)",
                      textTransform: "uppercase",
                    }}
                  >
                    <span>{k}</span>
                    <span className={`num num--xs ${pos ? "hud-pos" : "hud-neg"}`}>
                      {pos ? "+" : ""}
                      {v.toFixed(2)}
                    </span>
                  </div>
                  <div
                    style={{
                      display: "flex",
                      height: 5,
                      background: "var(--bg-2)",
                      borderRadius: 3,
                      marginTop: 4,
                      position: "relative",
                    }}
                  >
                    <div
                      style={{
                        position: "absolute",
                        left: "50%",
                        top: -2,
                        bottom: -2,
                        width: 1,
                        background: "var(--line)",
                      }}
                    ></div>
                    <div
                      style={{
                        position: "absolute",
                        top: 0,
                        bottom: 0,
                        left: pos ? "50%" : `${50 + normV * 50}%`,
                        width: `${Math.abs(normV) * 50}%`,
                        background: pos ? "var(--pos)" : "var(--neg)",
                        borderRadius: 3,
                        boxShadow: "0 0 8px currentColor",
                      }}
                    ></div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">03.03</span> Gate Radar
            </div>
            <Signal
              kind={
                sig?.signal === "yes"
                  ? "yes"
                  : sig?.signal === "no"
                  ? "no"
                  : decisionTrace.every((x) => x[2]) && decisionTrace.length > 0
                  ? "yes"
                  : "none"
              }
              label={sig ? sig.signal.toUpperCase() : "HOLD"}
            />
          </div>
          <div
            className="panel-body"
            style={{ display: "flex", justifyContent: "center", padding: 12 }}
          >
            <GateRadar data={data} asset={asset} />
          </div>
        </div>
      </div>

      <div
        className="hud-grid"
        style={{ gridTemplateColumns: "1fr 1.2fr 1fr", gap: 16 }}
      >
        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">03.04</span> Order Book
            </div>
            <AnimatedNumber
              value={market.yes_bid}
              decimals={0}
              suffix="¢"
              className="num num--md"
            />
          </div>
          <MiniLadder market={market} />
        </div>

        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">03.05</span> Yes vs No
            </div>
          </div>
          <div
            className="panel-body panel-body--chart"
            style={{ height: 280, position: "relative" }}
          >
            <svg
              width="100%"
              height="100%"
              viewBox="0 0 500 280"
              preserveAspectRatio="none"
            >
              <defs>
                <linearGradient id="bidGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="var(--pos)" stopOpacity="0.4" />
                  <stop offset="100%" stopColor="var(--pos)" stopOpacity="0" />
                </linearGradient>
                <linearGradient id="askGrad" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="var(--neg)" stopOpacity="0.4" />
                  <stop offset="100%" stopColor="var(--neg)" stopOpacity="0" />
                </linearGradient>
              </defs>
              {(() => {
                const bid = market.yes_bid;
                const ask = market.yes_ask;
                const midPx = (bid + ask) / 2;
                const yScale = (c: number) => 260 - (c / 100) * 240;
                const midX = (midPx / 100) * 500;
                return (
                  <>
                    <line
                      x1={midX}
                      y1="0"
                      x2={midX}
                      y2="260"
                      stroke="var(--hud-accent)"
                      strokeDasharray="3 4"
                    />
                    <rect
                      x="0"
                      y={yScale(bid)}
                      width={midX}
                      height={260 - yScale(bid)}
                      fill="url(#bidGrad)"
                      stroke="var(--pos)"
                      strokeWidth="1"
                    />
                    <rect
                      x={midX}
                      y={yScale(100 - ask)}
                      width={500 - midX}
                      height={260 - yScale(100 - ask)}
                      fill="url(#askGrad)"
                      stroke="var(--neg)"
                      strokeWidth="1"
                    />
                    <text
                      x={midX + 4}
                      y="14"
                      fill="var(--hud-accent)"
                      fontSize="10"
                      fontFamily="var(--font-hud-mono)"
                    >
                      MID {midPx.toFixed(1)}¢
                    </text>
                  </>
                );
              })()}
            </svg>
          </div>
          <div
            className="panel-body"
            style={{ borderTop: "1px solid var(--line-soft)", display: "flex", gap: 16 }}
          >
            <div className="hud-flex-1">
              <div className="kpi-label">Yes</div>
              <div className="num num--lg hud-pos">{market.yes_bid}¢ / {market.yes_ask}¢</div>
            </div>
            <div className="hud-flex-1">
              <div className="kpi-label">No</div>
              <div className="num num--lg hud-neg">
                {100 - market.yes_ask}¢ / {100 - market.yes_bid}¢
              </div>
            </div>
          </div>
        </div>

        <div className="panel">
          <div className="panel-header">
            <div className="panel-title">
              <span className="idx">03.06</span> Entry Decision Trace
            </div>
          </div>
          <div className="panel-body">
            {decisionTrace.length === 0 ? (
              <div style={{ color: "var(--ink-4)", padding: 8, fontSize: 12 }}>
                awaiting rr_config from bot…
              </div>
            ) : (
              decisionTrace.map(([k, v, ok], i) => (
                <div
                  key={i}
                  className="hud-between"
                  style={{
                    padding: "8px 0",
                    borderBottom: "1px solid var(--line-soft)",
                    fontSize: 11,
                    fontFamily: "var(--font-hud-mono)",
                    color: "var(--ink-2)",
                  }}
                >
                  <span>{k}</span>
                  <span
                    className={ok ? "hud-pos" : "hud-neg"}
                    style={{ letterSpacing: "0.05em" }}
                  >
                    {ok ? "✓" : "✗"} {v}
                  </span>
                </div>
              ))
            )}
            <div
              style={{
                marginTop: 12,
                padding: 12,
                background: "var(--accent-soft)",
                border: "1px solid var(--hud-accent)",
                borderRadius: 8,
                textAlign: "center",
              }}
            >
              <div
                style={{
                  fontSize: 10,
                  letterSpacing: "0.2em",
                  color: "var(--ink-3)",
                  marginBottom: 4,
                }}
              >
                SIGNAL
              </div>
              <div className="num num--lg" style={{ color: "var(--hud-accent)" }}>
                {sig?.signal.toUpperCase() ?? "HOLD"} · {sig?.confidence.toFixed(2) ?? "—"}
              </div>
              {sig?.reason && (
                <div
                  style={{
                    fontSize: 10,
                    color: "var(--ink-3)",
                    marginTop: 6,
                    lineHeight: 1.4,
                  }}
                >
                  {sig.reason}
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </>
  );
}

function HeaderBar({
  asset,
  setAsset,
  market,
  countdown,
}: {
  asset: Asset;
  setAsset: (a: Asset) => void;
  market: TickData["current_market"] | null;
  countdown: number;
}) {
  return (
    <div className="page-header">
      <div>
        <h1 className="page-title">
          <span className="idx">03</span> Market Deep-dive{" "}
          <span className="chip">{market?.ticker ?? "—"}</span>
        </h1>
        <div className="page-sub">
          One market, everything. Live book, momentum decomposition, gate traces,
          and a decision trace.
        </div>
      </div>
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <div style={{ display: "flex", gap: 6 }}>
          {ASSETS.map((a) => (
            <span
              key={a.key}
              className={`chip chip--btn ${asset === a.key ? "chip--accent" : ""}`}
              onClick={() => setAsset(a.key)}
            >
              {a.t}
            </span>
          ))}
        </div>
        <Signal kind="live" />
        <span className="chip">resolves {fmtSeconds(countdown)}</span>
      </div>
    </div>
  );
}
