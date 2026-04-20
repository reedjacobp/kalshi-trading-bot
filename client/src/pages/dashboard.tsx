import { useEffect, useRef, useState } from "react";
import type { TickData, Trade, MarketData } from "@shared/schema";
import { postPause } from "@/lib/sse";
import "./dashboard.css";

// Asset key as the bot publishes it: "btc", "eth", "sol", "doge", "xrp",
// "bnb", "hype" for 15M markets, plus *_daily for the daily/hourly variants.
const COIN_ORDER = ["btc", "eth", "sol", "doge", "xrp", "bnb", "hype"];

// ─── primitives ─────────────────────────────────────────────────────────────

function FmtNum({
  value,
  decimals = 2,
  prefix = "",
  suffix = "",
  className = "",
  duration = 500,
}: {
  value: number;
  decimals?: number;
  prefix?: string;
  suffix?: string;
  className?: string;
  duration?: number;
}) {
  const [d, setD] = useState(value);
  const prev = useRef(value);
  useEffect(() => {
    const from = prev.current;
    const diff = value - from;
    if (Math.abs(diff) < 0.0001) {
      setD(value);
      prev.current = value;
      return;
    }
    const t0 = performance.now();
    let raf = 0;
    const step = (now: number) => {
      const p = Math.min((now - t0) / duration, 1);
      const e = 1 - Math.pow(1 - p, 3);
      setD(from + diff * e);
      if (p < 1) raf = requestAnimationFrame(step);
      else prev.current = value;
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, [value, duration]);
  const formatted = Math.abs(d).toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
  return <span className={className}>{prefix}{formatted}{suffix}</span>;
}

function PnLChart({ points }: { points: { i: number; v: number }[] }) {
  const w = 600;
  const h = 110;
  const pad = 4;
  if (points.length === 0) {
    return (
      <svg viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" style={{ width: "100%", height: 110, display: "block" }}>
        <line x1="0" y1={h / 2} x2={w} y2={h / 2} stroke="var(--r-line-soft)" strokeWidth="1" strokeDasharray="2 4" />
      </svg>
    );
  }
  const vs = points.map((p) => p.v);
  const min = Math.min(0, ...vs);
  const max = Math.max(0, ...vs);
  const range = max - min || 1;
  const x = (i: number) => pad + (i / Math.max(1, points.length - 1)) * (w - pad * 2);
  const y = (v: number) => pad + (1 - (v - min) / range) * (h - pad * 2);
  const zeroY = y(0);
  const path = points.map((p, i) => `${i === 0 ? "M" : "L"}${x(i).toFixed(2)},${y(p.v).toFixed(2)}`).join(" ");
  const area = path + ` L${x(points.length - 1).toFixed(2)},${zeroY.toFixed(2)} L${x(0).toFixed(2)},${zeroY.toFixed(2)} Z`;
  const finalPos = points[points.length - 1].v >= 0;
  const stroke = finalPos ? "var(--r-pos)" : "var(--r-neg)";
  return (
    <svg viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none" style={{ width: "100%", height: 110, display: "block" }}>
      <defs>
        <linearGradient id="rider-pnlfill" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={stroke} stopOpacity="0.30" />
          <stop offset="100%" stopColor={stroke} stopOpacity="0" />
        </linearGradient>
      </defs>
      <line x1="0" y1={zeroY} x2={w} y2={zeroY} stroke="var(--r-line-soft)" strokeWidth="1" strokeDasharray="2 4" />
      <path d={area} fill="url(#rider-pnlfill)" />
      <path d={path} fill="none" stroke={stroke} strokeWidth="1.4" vectorEffect="non-scaling-stroke" />
      <circle cx={x(points.length - 1)} cy={y(points[points.length - 1].v)} r="2.4" fill={stroke} />
    </svg>
  );
}

// ─── helpers ────────────────────────────────────────────────────────────────

const RANGES = [
  { k: "today", label: "Today" },
  { k: "24h", label: "24h" },
  { k: "7d", label: "7d" },
  { k: "30d", label: "30d" },
  { k: "all", label: "All" },
] as const;
type RangeKey = typeof RANGES[number]["k"] | "custom";

function rangeMs(range: RangeKey, customDays: number): number {
  switch (range) {
    case "today": return startOfDayLocal();
    case "24h":   return Date.now() - 24 * 3600_000;
    case "7d":    return Date.now() - 7 * 86400_000;
    case "30d":   return Date.now() - 30 * 86400_000;
    case "all":   return 0;
    case "custom": return Date.now() - customDays * 86400_000;
  }
}

function startOfDayLocal(): number {
  const d = new Date();
  d.setHours(0, 0, 0, 0);
  return d.getTime();
}

function pnlSeriesFromTrades(trades: Trade[], sinceMs: number): { i: number; v: number }[] {
  const filtered = trades.filter((t) => new Date(t.time).getTime() >= sinceMs);
  filtered.sort((a, b) => new Date(a.time).getTime() - new Date(b.time).getTime());
  let cum = 0;
  const pts: { i: number; v: number }[] = [];
  for (let i = 0; i < filtered.length; i++) {
    cum += filtered[i].profit_after_fees;
    pts.push({ i, v: +cum.toFixed(2) });
  }
  if (pts.length === 0) return [{ i: 0, v: 0 }];
  if (pts.length === 1) return [{ i: 0, v: 0 }, pts[0]];
  return pts;
}

// PDT == America/Los_Angeles. Labels include the timezone abbreviation.
const PDT_FMT = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/Los_Angeles",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});
const PDT_DATE_FMT = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/Los_Angeles",
  month: "2-digit",
  day: "2-digit",
});
const PDT_TZ_FMT = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/Los_Angeles",
  timeZoneName: "short",
});

function pdtClock(d: Date): string {
  return PDT_FMT.format(d);
}
function pdtDateAndClock(d: Date): { date: string; clock: string; tz: string } {
  const parts = PDT_TZ_FMT.formatToParts(d);
  const tz = parts.find((p) => p.type === "timeZoneName")?.value || "PT";
  return { date: PDT_DATE_FMT.format(d), clock: PDT_FMT.format(d), tz };
}

// Asset-key parsing. "btc" → BTC/15M, "btc_daily" → BTC/DAILY.
function splitAssetKey(key: string): { coin: string; daily: boolean; tf: string } {
  const daily = key.endsWith("_daily");
  const coin = daily ? key.slice(0, -6) : key;
  return { coin, daily, tf: daily ? "DAILY" : "15M" };
}

function sortAssetKeys(keys: string[]): string[] {
  return keys.slice().sort((a, b) => {
    const ai = splitAssetKey(a);
    const bi = splitAssetKey(b);
    const aCoin = COIN_ORDER.indexOf(ai.coin);
    const bCoin = COIN_ORDER.indexOf(bi.coin);
    if (aCoin !== bCoin) return (aCoin === -1 ? 99 : aCoin) - (bCoin === -1 ? 99 : bCoin);
    // 15M before DAILY for the same coin
    return Number(ai.daily) - Number(bi.daily);
  });
}

// Resolve a window/timeframe label from the ticker for display under each tab.
function tickerWindowLabel(ticker: string): string {
  // KX{ASSET}-{YYMMMDD}-{H|M|D}{HHMM} ...
  const m = ticker.match(/-(\d{2}[A-Z]{3}\d{2})-([HMD])(\d{0,4})/);
  if (!m) return ticker;
  const date = m[1];
  const kind = m[2];
  const t = m[3] || "";
  const fmtTime = (raw: string) => {
    if (raw.length < 3) return raw;
    const hh = parseInt(raw.slice(0, raw.length - 2), 10);
    const mm = raw.slice(-2);
    if (Number.isNaN(hh)) return raw;
    const h12 = ((hh + 11) % 12) + 1;
    const ap = hh >= 12 ? "PM" : "AM";
    return `${h12}:${mm} ${ap}`;
  };
  if (kind === "D") return `${date} EOD`;
  return `${fmtTime(t)} (${date})`;
}

type Gate = { k: string; v: string; ref: string; ok: boolean };

// Map an asset key (e.g. "btc", "btc_daily") to its rr_config.per_cell key.
// 15M markets → "<coin>_15m". Daily markets → "<coin>_hourly" (the bot
// uses the same cell name for both daily/hourly variants — bot.py:2839).
function cellNameFor(assetKey: string): string {
  if (assetKey.endsWith("_daily")) return assetKey.replace("_daily", "_hourly");
  return `${assetKey}_15m`;
}

function buildGates(
  assetKey: string,
  data: TickData,
  liveSecs?: number,
): Gate[] {
  const market = data.markets[assetKey];
  if (!market) return [];

  const cell = data.rr_config?.per_cell?.[cellNameFor(assetKey)];
  const defaults = data.rr_config?.defaults;
  const mom = data.asset_momentum?.[assetKey];

  // All price thresholds are in CENTS (matches yes_ask/yes_bid units).
  const askCents = market.yes_ask;
  const minPriceC = cell?.min_contract_price ?? defaults?.min_contract_price ?? 95;
  const maxPriceC = cell?.max_entry_price ?? defaults?.max_entry_price ?? 98;
  const minSec = cell?.min_seconds ?? defaults?.min_seconds ?? 15;
  const maxSec = cell?.max_seconds ?? defaults?.max_seconds ?? 60;

  // min_price_buffer_pct is a percent (e.g. 0.68 means 0.68%) — same unit
  // as buffer_pct = (price-strike)/strike*100.
  const minBufPct = cell?.min_price_buffer_pct ?? defaults?.min_price_buffer_pct ?? 0.15;

  // max_adverse_momentum is a NEGATIVE percent (e.g. -0.056 means -0.056%).
  // Block fires when momentum < threshold. mom_5m is also in percent.
  const maxAdvMom = cell?.mom_gate ?? defaults?.max_adverse_momentum ?? -0.05;

  const sec = liveSecs ?? market.seconds_remaining;
  // mom_cell is the smoothed momentum at this cell's (window, periods) —
  // the exact value the strategy compares against max_adverse_momentum.
  // Falls back to mom_5m if the bot couldn't compute the cell-specific one.
  const cellMom = mom?.mom_cell ?? mom?.mom_5m ?? 0;
  const realVol = mom?.realized_vol ?? null;
  const volGate = cell?.vol_gate ?? null;

  const price = mom?.price ?? 0;
  let bufferPct: number | null = null;
  if (market.floor_strike != null && market.floor_strike > 0 && price > 0) {
    bufferPct = ((price - market.floor_strike) / market.floor_strike) * 100;
  } else if (market.cap_strike != null && market.cap_strike > 0 && price > 0) {
    bufferPct = ((market.cap_strike - price) / market.cap_strike) * 100;
  }

  return [
    {
      k: "min_contract_price",
      v: `${askCents}¢`,
      ref: `≥ ${minPriceC}¢`,
      ok: askCents >= minPriceC,
    },
    {
      k: "max_entry_price",
      v: `${askCents}¢`,
      ref: `≤ ${maxPriceC}¢`,
      ok: askCents <= maxPriceC,
    },
    {
      k: "seconds_remaining",
      v: `${sec}s`,
      ref: `${minSec}–${maxSec}s`,
      ok: sec >= minSec && sec <= maxSec,
    },
    {
      k: "price_buffer_pct",
      v: bufferPct != null
        ? `${bufferPct >= 0 ? "+" : ""}${bufferPct.toFixed(3)}%`
        : "—",
      ref: `≥ ${minBufPct.toFixed(3)}%`,
      ok: bufferPct != null && Math.abs(bufferPct) >= minBufPct,
    },
    {
      k: "adverse_momentum",
      v: `${cellMom >= 0 ? "+" : ""}${cellMom.toFixed(4)}%`,
      ref: `≥ ${maxAdvMom.toFixed(4)}%`,
      ok: cellMom >= maxAdvMom,
    },
    {
      k: "realized_vol",
      v: realVol != null ? `${realVol.toFixed(3)}%` : "—",
      ref: volGate != null ? `≤ ${volGate.toFixed(3)}%` : "disabled",
      // Disabled gate is treated as PASS (matches strategy behavior — it
      // only blocks when max_realized_vol_pct is set).
      ok: volGate == null || (realVol != null && realVol <= volGate),
    },
  ];
}

type BookLevel = { p: number; sz: number };
function buildBook(market: MarketData): { bids: BookLevel[]; asks: BookLevel[] } {
  const bid = Math.max(1, Math.min(99, market.yes_bid));
  const ask = Math.max(1, Math.min(99, market.yes_ask));
  // ladder approximation — only top-of-book is on the SSE feed today
  const asks = [3, 2, 1, 0].map((i) => ({
    p: Math.min(99, ask + i),
    sz: 120 + i * 60,
  }));
  const bids = [0, 1, 2, 3].map((i) => ({
    p: Math.max(1, bid - i),
    sz: 120 + i * 60,
  }));
  return { asks, bids };
}

// ─── signal tape ────────────────────────────────────────────────────────────
// A real decisions log (not a trade list). Merges:
//   • recent_skips      → BLOCK rows (gate evaluated, order not placed)
//   • gate_matrix.rows  → BLOCK rows for tickers currently being evaluated
//   • trades            → FILL (pending) / WIN / LOSS rows
// Sorted reverse-chronological.

type TapeRow = {
  ts: number;
  t: string;
  msg: string;
  flag: "yes" | "no" | "fill" | "win" | "loss" | "—";
  px: string;
};

function buildTape(data: TickData): TapeRow[] {
  const rows: TapeRow[] = [];

  for (const tr of data.trades || []) {
    const ts = new Date(tr.time).getTime();
    if (Number.isNaN(ts)) continue;
    if (tr.outcome === "win" || tr.outcome === "loss") {
      const net = tr.profit_after_fees;
      rows.push({
        ts,
        t: pdtClock(new Date(ts)),
        msg: `${tr.ticker} · settled ${tr.outcome.toUpperCase()}`,
        flag: tr.outcome,
        px: `${net >= 0 ? "+" : ""}$${net.toFixed(2)}`,
      });
    } else {
      rows.push({
        ts,
        t: pdtClock(new Date(ts)),
        msg: `${tr.ticker} · ${tr.strategy} · ${tr.side.toUpperCase()} fill @ ${tr.price}¢ × ${tr.contracts}`,
        flag: "fill",
        px: `$${tr.stake.toFixed(0)}`,
      });
    }
  }

  for (const s of data.recent_skips || []) {
    const ts = new Date(s.timestamp).getTime();
    if (Number.isNaN(ts)) continue;
    rows.push({
      ts,
      t: pdtClock(new Date(ts)),
      msg: `${s.ticker} · ${s.reason}`,
      flag: "no",
      px: s.yes_ask != null ? `${s.yes_ask}¢` : "—",
    });
  }

  // gate_matrix rows are point-in-time (no timestamp); slot them at "now"
  // so they sit at the top until something more recent appears.
  const nowMs = Date.now();
  for (const r of data.gate_matrix?.rows || []) {
    if (r.blocked_at === "passed") continue;
    rows.push({
      ts: nowMs - Math.max(0, r.age_s) * 1000,
      t: pdtClock(new Date(nowMs - Math.max(0, r.age_s) * 1000)),
      msg: `${r.ticker} · ${r.blocked_at} blocking`,
      flag: "no",
      px: "—",
    });
  }

  rows.sort((a, b) => b.ts - a.ts);

  // De-dupe back-to-back identical rows (e.g. same ticker repeatedly blocked
  // on the same gate within a few seconds).
  const out: TapeRow[] = [];
  for (const r of rows) {
    const last = out[out.length - 1];
    if (last && last.msg === r.msg && last.flag === r.flag && Math.abs(last.ts - r.ts) < 3000) {
      continue;
    }
    out.push(r);
    if (out.length >= 14) break;
  }
  return out;
}

function flagText(f: TapeRow["flag"]): string {
  if (f === "yes")  return "ENTER";
  if (f === "no")   return "BLOCK";
  if (f === "fill") return "FILL";
  if (f === "win")  return "WIN";
  if (f === "loss") return "LOSS";
  return "—";
}
function flagCls(f: TapeRow["flag"]): string {
  if (f === "yes" || f === "fill" || f === "win") return "r-pos";
  if (f === "no" || f === "loss") return "r-neg";
  return "";
}

// ─── trade history ──────────────────────────────────────────────────────────

function outcomeClass(o: Trade["outcome"]): string {
  if (o === "win") return "r-pos";
  if (o === "loss") return "r-neg";
  return "";
}

const PAGE_SIZES = [25, 50, 100] as const;
type PageSize = typeof PAGE_SIZES[number];

function TradeHistory({
  trades,
  sinceMs,
  rangeLabel,
  pageSize,
  setPageSize,
  page,
  setPage,
}: {
  trades: Trade[];
  sinceMs: number;
  rangeLabel: string;
  pageSize: PageSize;
  setPageSize: (n: PageSize) => void;
  page: number;
  setPage: (n: number) => void;
}) {
  const filtered = trades
    .filter((t) => new Date(t.time).getTime() >= sinceMs)
    .sort((a, b) => new Date(b.time).getTime() - new Date(a.time).getTime());

  const total = filtered.length;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  const safePage = Math.min(Math.max(0, page), pageCount - 1);
  const startIdx = safePage * pageSize;
  const rows = filtered.slice(startIdx, startIdx + pageSize);

  // Pager + page-size selector live above the table on the right side
  // of the section. Always rendered (even when empty) so the controls
  // stay accessible.
  const pager = (
    <div className="r-history-controls">
      <div className="r-history-controls-info">
        {total === 0
          ? `0 trades in ${rangeLabel.toLowerCase()}`
          : `${startIdx + 1}–${Math.min(startIdx + pageSize, total)} of ${total}`}
      </div>
      <div className="r-history-controls-spacer" />
      <label className="r-history-controls-label">
        per page
        <select
          className="r-history-select"
          value={pageSize}
          onChange={(e) => {
            setPageSize(+e.target.value as PageSize);
            setPage(0);
          }}
        >
          {PAGE_SIZES.map((n) => (
            <option key={n} value={n}>{n}</option>
          ))}
        </select>
      </label>
      <div className="r-history-pager">
        <button
          onClick={() => setPage(Math.max(0, safePage - 1))}
          disabled={safePage === 0}
        >
          ‹ prev
        </button>
        <span className="r-history-pager-pos">
          {safePage + 1} / {pageCount}
        </span>
        <button
          onClick={() => setPage(Math.min(pageCount - 1, safePage + 1))}
          disabled={safePage >= pageCount - 1}
        >
          next ›
        </button>
      </div>
    </div>
  );

  if (rows.length === 0) {
    return (
      <>
        {pager}
        <div style={{ color: "var(--r-ink-3)", padding: "16px 0" }}>
          no trades in {rangeLabel.toLowerCase()}
        </div>
      </>
    );
  }

  // Use the most-recent trade's TZ label so it tracks PST/PDT correctly.
  const tzLabel = pdtDateAndClock(new Date(rows[0].time)).tz;

  return (
    <>
      {pager}
      <table className="r-history">
      <thead>
        <tr>
          <th>time · {tzLabel}</th>
          <th>ticker</th>
          <th>side</th>
          <th className="r-num-col">price</th>
          <th className="r-num-col">ct</th>
          <th>result</th>
          <th className="r-num-col">fees</th>
          <th className="r-num-col">net P&amp;L</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((t, i) => {
          const d = new Date(t.time);
          const { date, clock } = pdtDateAndClock(d);
          const netUsd = t.profit_after_fees;
          const feesUsd = t.fees;
          return (
            <tr key={`${t.time}-${t.ticker}-${i}`}>
              <td className="r-history-time">
                <span className="r-history-date">{date}</span> {clock}
              </td>
              <td className="r-history-ticker">{t.ticker}</td>
              <td className={t.side === "yes" ? "r-pos" : "r-neg"}>
                {t.side.toUpperCase()}
              </td>
              <td className="r-num-col">{t.price}¢</td>
              <td className="r-num-col">{t.contracts}</td>
              <td className={outcomeClass(t.outcome)}>
                {t.outcome.toUpperCase()}
              </td>
              <td className="r-num-col r-history-dim">
                ${feesUsd.toFixed(2)}
              </td>
              <td
                className={`r-num-col ${
                  t.outcome === "pending" ? "r-history-dim" :
                  netUsd >= 0 ? "r-pos" : "r-neg"
                }`}
              >
                {t.outcome === "pending" ? "—" :
                  `${netUsd >= 0 ? "+" : "-"}$${Math.abs(netUsd).toFixed(2)}`}
              </td>
            </tr>
          );
        })}
      </tbody>
      </table>
    </>
  );
}

// ─── page ───────────────────────────────────────────────────────────────────

export default function DashboardPage({
  data,
  connected,
}: {
  data: TickData | null;
  connected: boolean;
}) {
  const [pnlRange, setPnlRange] = useState<RangeKey>(() => {
    const raw = localStorage.getItem("rider-pnl-range");
    return (raw as RangeKey) || "today";
  });
  const [customDays, setCustomDays] = useState<number>(() => {
    return +(localStorage.getItem("rider-custom-days") || 14);
  });
  useEffect(() => { localStorage.setItem("rider-pnl-range", pnlRange); }, [pnlRange]);
  useEffect(() => { localStorage.setItem("rider-custom-days", String(customDays)); }, [customDays]);

  // Trade history pagination — page resets to 0 when the range or page
  // size changes so the user always lands on the most recent slice.
  const [historyPageSize, setHistoryPageSize] = useState<PageSize>(() => {
    const raw = +(localStorage.getItem("rider-history-page-size") || 25);
    return (PAGE_SIZES as readonly number[]).includes(raw) ? (raw as PageSize) : 25;
  });
  const [historyPage, setHistoryPage] = useState(0);
  useEffect(() => {
    localStorage.setItem("rider-history-page-size", String(historyPageSize));
  }, [historyPageSize]);
  useEffect(() => { setHistoryPage(0); }, [pnlRange, customDays, historyPageSize]);

  const [marketKey, setMarketKey] = useState<string>(() => {
    return localStorage.getItem("rider-market-key") || "btc";
  });
  useEffect(() => { localStorage.setItem("rider-market-key", marketKey); }, [marketKey]);

  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  // Track when each market's seconds_remaining was last published so the
  // countdown can tick smoothly between SSE updates instead of frozen
  // at the last broadcast value.
  const lastSeenRef = useRef<Map<string, { secs: number; ts: number }>>(new Map());
  if (data?.markets) {
    for (const [k, m] of Object.entries(data.markets)) {
      if (!m) continue;
      const prev = lastSeenRef.current.get(k);
      // Re-anchor whenever the bot broadcasts a new value (which only changes
      // every few seconds). Otherwise we'd reset every render.
      if (!prev || prev.secs !== m.seconds_remaining) {
        lastSeenRef.current.set(k, { secs: m.seconds_remaining, ts: Date.now() });
      }
    }
  }

  // Hide the global atmosphere bg while the rider page is mounted.
  useEffect(() => {
    const atmo = document.querySelector<HTMLDivElement>(".atmosphere");
    const prev = atmo?.style.display ?? "";
    if (atmo) atmo.style.display = "none";
    return () => {
      if (atmo) atmo.style.display = prev;
    };
  }, []);

  if (!data) {
    return <WaitingPage connected={connected} />;
  }

  const stats = data.stats;
  const trades = data.trades || [];

  const sinceMs = rangeMs(pnlRange, customDays);
  const series = pnlSeriesFromTrades(trades, sinceMs);
  const pnlValue = series[series.length - 1].v;
  const filteredCount = series.length > 1 ? series.length : 0;
  const winsInRange = trades
    .filter((t) => new Date(t.time).getTime() >= sinceMs && t.outcome === "win").length;
  const lossesInRange = trades
    .filter((t) => new Date(t.time).getTime() >= sinceMs && t.outcome === "loss").length;
  const feesInRange = trades
    .filter((t) => new Date(t.time).getTime() >= sinceMs)
    .reduce((acc, t) => acc + (t.fees || 0), 0);

  const balance = stats.live_balance ?? stats.paper_balance ?? 0;
  const killOn = !stats.bot_paused;

  const availableKeys = sortAssetKeys(
    Object.keys(data.markets || {}).filter((k) => data.markets[k]),
  );
  const activeKey = availableKeys.includes(marketKey)
    ? marketKey
    : (availableKeys[0] ?? "");
  const market = activeKey ? data.markets[activeKey] : null;

  // Real-time countdown: derive from the last broadcast value plus elapsed
  // wall-clock since it arrived. The `now` 1s tick drives re-renders.
  const seenAt = activeKey ? lastSeenRef.current.get(activeKey) : undefined;
  const elapsedSinceTick = seenAt ? Math.floor((now - seenAt.ts) / 1000) : 0;
  const secondsRemaining = seenAt
    ? Math.max(0, seenAt.secs - elapsedSinceTick)
    : (market?.seconds_remaining ?? 0);
  const mm = String(Math.floor(secondsRemaining / 60)).padStart(2, "0");
  const ss = String(secondsRemaining % 60).padStart(2, "0");

  const gates = market ? buildGates(activeKey, data, secondsRemaining) : [];
  const failedGates = gates.filter((g) => !g.ok).length;
  const book = market ? buildBook(market) : null;
  const maxBookSize = book
    ? Math.max(...book.asks.map((r) => r.sz), ...book.bids.map((r) => r.sz))
    : 1;
  const tape = buildTape(data);

  const mid = market ? (market.yes_bid + market.yes_ask) / 2 : 0;

  const onToggleKill = async () => {
    try {
      await postPause(killOn);
    } catch (e) {
      console.error("toggle failed", e);
    }
  };

  const rangeLabel = pnlRange === "custom"
    ? `${customDays}d`
    : RANGES.find((r) => r.k === pnlRange)?.label || "Today";

  const activeInfo = activeKey ? splitAssetKey(activeKey) : null;

  return (
    <div className="r-page" data-rider="1">
      <div className="r-shell">
        <div className="r-head">
          <div className="r-brand">
            rider <span className="r-dim">— resolution rider</span>
          </div>
          <div className="r-head-meta">
            <span>
              <span className={`r-dot ${connected ? "r-pulse" : ""}`}></span>
              {connected ? "kalshi" : "feed down"}
            </span>
            <span>
              mode<b>{stats.is_paper ? "paper" : "live"}</b>
            </span>
            <span>
              {killOn ? "trading" : "paused"}
              <b style={{ color: killOn ? "var(--r-pos)" : "var(--r-neg)" }}>
                {killOn ? "ON" : "OFF"}
              </b>
            </span>
          </div>
        </div>

        {/* Hero */}
        <div className="r-hero">
          <div>
            <div className="r-hero-label">P&amp;L · {rangeLabel}</div>
            <FmtNum
              value={pnlValue}
              decimals={2}
              prefix={pnlValue >= 0 ? "+$" : "-$"}
              className={`r-hero-value ${pnlValue >= 0 ? "r-pos" : "r-neg"}`}
            />
            <div className="r-hero-sub">
              {winsInRange}W · {lossesInRange}L · after ${feesInRange.toFixed(2)} fees
              {filteredCount === 0 && " · no trades in range"}
            </div>
            <div className="r-pnl-bar">
              {RANGES.map((r) => (
                <button
                  key={r.k}
                  className={pnlRange === r.k ? "r-on" : ""}
                  onClick={() => setPnlRange(r.k)}
                >
                  {r.label}
                </button>
              ))}
              <button
                className={pnlRange === "custom" ? "r-on" : ""}
                onClick={() => setPnlRange("custom")}
              >
                Custom
              </button>
              {pnlRange === "custom" && (
                <input
                  type="number"
                  min={1}
                  max={365}
                  value={customDays}
                  onChange={(e) =>
                    setCustomDays(Math.max(1, Math.min(365, +e.target.value || 1)))
                  }
                />
              )}
            </div>
            <div className="r-pnl-chart">
              <PnLChart points={series} />
            </div>
          </div>
          <div>
            <div className="r-hero-label">Kalshi Balance</div>
            <FmtNum value={balance} decimals={2} prefix="$" className="r-hero-value" />
            <div className="r-hero-sub">
              {stats.is_paper ? "paper account" : "live account"}
              {stats.pending ? ` · ${stats.pending} pending` : ""}
            </div>
          </div>
          <div>
            <div className="r-hero-label">Auto-trading</div>
            <div className="r-kill" style={{ marginTop: -2 }}>
              <div
                className={`r-kill-toggle ${killOn ? "" : "r-off"}`}
                onClick={onToggleKill}
                role="button"
                aria-pressed={killOn}
              >
                <span className="r-label-off">OFF</span>
                <span className="r-label-on">ON</span>
                <span className="r-knob"></span>
              </div>
              <div className="r-kill-status">{killOn ? "armed" : "halted"}</div>
            </div>
            <div className="r-hero-sub" style={{ marginTop: 18 }}>
              kill switch — click to {killOn ? "halt" : "arm"}
            </div>
          </div>
        </div>

        {/* Active markets */}
        <div className="r-section">
          <div className="r-section-label">
            <span className="r-num">01</span>Active markets
            <div style={{ marginTop: 18, color: "var(--r-ink-3)" }}>
              {availableKeys.length} live
            </div>
          </div>
          <div>
            <div className="r-mkt-tabs">
              {availableKeys.length === 0 && (
                <div className="r-mkt-tab" style={{ cursor: "default" }}>
                  <span className="r-mkt-tab-asset" style={{ color: "var(--r-ink-3)" }}>
                    no live markets
                  </span>
                </div>
              )}
              {availableKeys.map((k) => {
                const m = data.markets[k]!;
                const info = splitAssetKey(k);
                const fails = buildGates(k, data).filter((g) => !g.ok).length;
                return (
                  <div
                    key={k}
                    className={`r-mkt-tab ${k === activeKey ? "r-on" : ""}`}
                    onClick={() => setMarketKey(k)}
                    role="button"
                  >
                    <span className="r-mkt-tab-asset">
                      {info.coin.toUpperCase()}{" "}
                      <span style={{ color: "var(--r-ink-4)", fontSize: 11, letterSpacing: "0.14em" }}>
                        · {info.tf}
                      </span>
                    </span>
                    <span className="r-mkt-tab-id">{tickerWindowLabel(m.ticker)}</span>
                    <span className={`r-mkt-tab-flag ${fails ? "r-neg" : "r-pos"}`}>
                      {fails ? `${fails} BLOCK` : "PASS"}
                    </span>
                  </div>
                );
              })}
            </div>
            {market && activeInfo ? (
              <div className="r-market">
                <div>
                  <div className="r-market-id">{market.ticker.replace(/-/g, "‑")}</div>
                  <div className="r-market-q">
                    {market.floor_strike != null
                      ? `${activeInfo.coin.toUpperCase()} above $${market.floor_strike.toLocaleString()} · ${activeInfo.tf}`
                      : market.cap_strike != null
                        ? `${activeInfo.coin.toUpperCase()} below $${market.cap_strike.toLocaleString()} · ${activeInfo.tf}`
                        : `${activeInfo.coin.toUpperCase()} · ${activeInfo.tf}`}
                  </div>
                </div>
                <div>
                  <div className="r-market-time">
                    <span className="r-lbl">resolves in</span>
                    {mm}:{ss}
                  </div>
                </div>
              </div>
            ) : (
              <div className="r-market">
                <div className="r-market-q" style={{ color: "var(--r-ink-3)" }}>
                  Waiting for the bot to publish a market.
                </div>
              </div>
            )}
          </div>
        </div>

        {/* Gates */}
        <div className="r-section">
          <div className="r-section-label">
            <span className="r-num">02</span>Gates
            <div
              style={{
                marginTop: 18,
                color: failedGates ? "var(--r-neg)" : "var(--r-pos)",
                letterSpacing: "0.18em",
              }}
            >
              {gates.length === 0 ? "—" : failedGates ? `${failedGates} BLOCK` : "ALL PASS"}
            </div>
          </div>
          <div className="r-gates">
            {gates.length === 0 && (
              <div className="r-gate">
                <span className="r-gate-name" style={{ color: "var(--r-ink-3)" }}>
                  no gates available for selected market
                </span>
              </div>
            )}
            {gates.map((g) => (
              <div className="r-gate" key={g.k}>
                <span className="r-gate-name">{g.k}</span>
                <span className="r-gate-val">{g.v}</span>
                <span className="r-gate-ref">{g.ref}</span>
                <span className={`r-gate-flag ${g.ok ? "r-ok" : "r-no"}`}>
                  {g.ok ? "PASS" : "FAIL"}
                </span>
              </div>
            ))}
          </div>
        </div>

        {/* Order book */}
        <div className="r-section">
          <div className="r-section-label">
            <span className="r-num">03</span>Order book
            <div style={{ marginTop: 18, color: "var(--r-ink-3)" }}>
              {activeInfo ? `${activeInfo.coin.toUpperCase()} · ${activeInfo.tf}` : "—"}
            </div>
            <div style={{ marginTop: 6, color: "var(--r-ink-4)", fontSize: 10 }}>
              top-of-book only · depth approximated
            </div>
          </div>
          <div className="r-book">
            {book && book.asks.map((r) => (
              <Frag key={`a${r.p}`}>
                <div className="r-book-row" />
                <div className="r-book-px">{r.p}¢</div>
                <div className="r-book-row r-book-ask">
                  <span
                    className="r-bar"
                    style={{ width: `${(r.sz / maxBookSize) * 60}%` }}
                  ></span>
                  {r.sz}
                </div>
              </Frag>
            ))}
            {book && (
              <div className="r-book-mid">
                MID <FmtNum value={mid} decimals={1} suffix="¢" />
              </div>
            )}
            {book && book.bids.map((r) => (
              <Frag key={`b${r.p}`}>
                <div className="r-book-row r-book-bid">
                  <span
                    className="r-bar"
                    style={{ width: `${(r.sz / maxBookSize) * 60}%` }}
                  ></span>
                  {r.sz}
                </div>
                <div className="r-book-px">{r.p}¢</div>
                <div className="r-book-row" />
              </Frag>
            ))}
            {!book && (
              <div
                className="r-book-mid"
                style={{ gridColumn: "1 / -1", color: "var(--r-ink-3)" }}
              >
                no book on feed
              </div>
            )}
          </div>
        </div>

        {/* Signal tape */}
        <div className="r-section">
          <div className="r-section-label">
            <span className="r-num">04</span>Signal tape
            <div
              style={{
                marginTop: 14,
                color: "var(--r-ink-3)",
                lineHeight: 1.5,
                textTransform: "none",
                letterSpacing: 0,
                fontSize: 12,
              }}
            >
              live decisions log — every gate evaluation, fill, and settlement
              the bot makes across all markets
            </div>
            <div style={{ marginTop: 14, color: "var(--r-ink-3)" }}>
              <span className={`r-dot ${connected ? "r-pulse" : ""}`}></span>
              {connected ? "live" : "offline"}
            </div>
          </div>
          <div>
            {tape.length === 0 && (
              <div className="r-tape-row">
                <span className="r-tape-t">—</span>
                <span className="r-tape-msg" style={{ color: "var(--r-ink-3)" }}>
                  no events yet
                </span>
                <span className="r-tape-flag">—</span>
                <span className="r-tape-px">—</span>
              </div>
            )}
            {tape.map((r, i) => (
              <div className="r-tape-row" key={i}>
                <span className="r-tape-t">{r.t}</span>
                <span className="r-tape-msg">{r.msg}</span>
                <span className={`r-tape-flag ${flagCls(r.flag)}`}>
                  {flagText(r.flag)}
                </span>
                <span className="r-tape-px">{r.px}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Trade history — same range as the P&L hero above */}
        <div className="r-section">
          <div className="r-section-label">
            <span className="r-num">05</span>Trade history
            <div style={{ marginTop: 18, color: "var(--r-ink-3)" }}>
              {rangeLabel}
            </div>
            <div style={{ marginTop: 4, color: "var(--r-ink-4)", fontSize: 11 }}>
              follows P&amp;L range above
            </div>
          </div>
          <div>
            <TradeHistory
              trades={trades}
              sinceMs={sinceMs}
              rangeLabel={rangeLabel}
              pageSize={historyPageSize}
              setPageSize={setHistoryPageSize}
              page={historyPage}
              setPage={setHistoryPage}
            />
          </div>
        </div>

        <div className="r-foot">
          <div>rider // 0.1.4</div>
          <div>{new Date(now).toUTCString().slice(17, 25)} UTC</div>
        </div>
      </div>
    </div>
  );
}

function Frag({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}

function WaitingPage({ connected }: { connected: boolean }) {
  return (
    <div className="r-page">
      <div className="r-shell">
        <div className="r-head">
          <div className="r-brand">
            rider <span className="r-dim">— resolution rider</span>
          </div>
          <div className="r-head-meta">
            <span>
              <span className={`r-dot ${connected ? "r-pulse" : ""}`}></span>
              {connected ? "kalshi" : "feed down"}
            </span>
          </div>
        </div>
        <div
          style={{
            padding: "120px 0",
            textAlign: "center",
            color: "var(--r-ink-3)",
          }}
        >
          <div style={{ fontSize: 36, fontWeight: 200, marginBottom: 14 }}>
            {connected ? "waiting for bot…" : "feed down"}
          </div>
          <div style={{ fontSize: 13, color: "var(--r-ink-4)" }}>
            {connected
              ? "SSE connected. Waiting for first tick from python-bot."
              : "Can't reach /api/stream — check the server and python bot."}
          </div>
        </div>
      </div>
    </div>
  );
}
