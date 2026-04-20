import React, {
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type ReactNode,
} from "react";

// ── Tick hook: drives periodic re-renders for "live" feel ───────
export function useTick(intervalMs = 1400): number {
  const [t, setT] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setT((x) => x + 1), intervalMs);
    return () => clearInterval(id);
  }, [intervalMs]);
  return t;
}

// ── Deterministic pseudo-random walk (used for sparkline fills) ─
export function seededRandom(seed: number): () => number {
  let s = seed;
  return () => {
    s = (s * 9301 + 49297) % 233280;
    return s / 233280;
  };
}
export function makeWalk(n: number, seed: number, start = 100, vol = 0.4): number[] {
  const rand = seededRandom(seed);
  const out = [start];
  for (let i = 1; i < n; i++) out.push(out[i - 1] + (rand() - 0.5) * vol * 2);
  return out;
}

// ── Animated number: eases to target value ──────────────────────
export function AnimatedNumber({
  value,
  decimals = 2,
  prefix = "",
  suffix = "",
  className = "",
  showSign = false,
  duration = 450,
}: {
  value: number;
  decimals?: number;
  prefix?: string;
  suffix?: string;
  className?: string;
  showSign?: boolean;
  duration?: number;
}) {
  const [display, setDisplay] = useState(value);
  const prevRef = useRef(value);
  const rafRef = useRef<number | null>(null);

  useEffect(() => {
    const prev = prevRef.current;
    const diff = value - prev;
    if (Math.abs(diff) < 0.00001) {
      setDisplay(value);
      prevRef.current = value;
      return;
    }
    const start = performance.now();
    const step = (now: number) => {
      const p = Math.min((now - start) / duration, 1);
      const eased = 1 - Math.pow(1 - p, 3);
      const v = prev + diff * eased;
      setDisplay(v);
      if (p < 1) rafRef.current = requestAnimationFrame(step);
      else prevRef.current = value;
    };
    rafRef.current = requestAnimationFrame(step);
    return () => {
      if (rafRef.current != null) cancelAnimationFrame(rafRef.current);
    };
  }, [value, duration]);

  const sign = showSign && display > 0 ? "+" : "";
  return (
    <span className={`num ${className}`}>
      {prefix}
      {sign}
      {display.toFixed(decimals)}
      {suffix}
    </span>
  );
}

// ── Sparkline ───────────────────────────────────────────────────
export function Sparkline({
  data,
  color,
  height = 40,
  fill = true,
  showDot = true,
}: {
  data: number[];
  color?: string;
  height?: number;
  fill?: boolean;
  showDot?: boolean;
}) {
  const w = 200;
  const h = height;
  const safe = data.length >= 2 ? data : [0, 0];
  const min = Math.min(...safe);
  const max = Math.max(...safe);
  const range = max - min || 1;
  const pts = safe.map((v, i) => {
    const x = (i / (safe.length - 1)) * w;
    const y = h - ((v - min) / range) * (h - 4) - 2;
    return [x, y] as const;
  });
  const path = pts
    .map((p, i) => (i === 0 ? `M${p[0]},${p[1]}` : `L${p[0]},${p[1]}`))
    .join(" ");
  const area = `${path} L${w},${h} L0,${h} Z`;
  const last = pts[pts.length - 1];
  const gradId = useMemo(
    () => `g${Math.random().toString(36).slice(2)}`,
    []
  );
  const c = color || "var(--hud-accent)";
  return (
    <svg className="spark" viewBox={`0 0 ${w} ${h}`} preserveAspectRatio="none">
      <defs>
        <linearGradient id={gradId} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={c} stopOpacity="0.35" />
          <stop offset="100%" stopColor={c} stopOpacity="0" />
        </linearGradient>
      </defs>
      {fill && <path d={area} fill={`url(#${gradId})`} />}
      <path
        d={path}
        fill="none"
        stroke={c}
        strokeWidth="1.2"
        vectorEffect="non-scaling-stroke"
      />
      {showDot && (
        <circle cx={last[0]} cy={last[1]} r="2.5" fill={c}>
          <animate attributeName="r" values="2.5;4.5;2.5" dur="1.8s" repeatCount="indefinite" />
        </circle>
      )}
    </svg>
  );
}

// ── Radial gauge ────────────────────────────────────────────────
export function RadialGauge({
  value,
  min = -1,
  max = 1,
  label,
  unit = "",
  size = 120,
  decimals = 2,
}: {
  value: number;
  min?: number;
  max?: number;
  label: string;
  unit?: string;
  size?: number;
  decimals?: number;
}) {
  const pct = Math.max(0, Math.min(1, (value - min) / (max - min)));
  const r = size / 2 - 10;
  const cx = size / 2;
  const cy = size / 2;
  const startAngle = 135;
  const totalArc = 270;
  const arcTo = (a: number) => {
    const rad = (a - 90) * (Math.PI / 180);
    return [cx + r * Math.cos(rad), cy + r * Math.sin(rad)] as const;
  };
  const [sx, sy] = arcTo(startAngle);
  const [ex, ey] = arcTo(startAngle + totalArc);
  const bg = `M${sx},${sy} A${r},${r} 0 1 1 ${ex},${ey}`;
  const [fx, fy] = arcTo(startAngle + totalArc * pct);
  const largeArc = totalArc * pct > 180 ? 1 : 0;
  const fg = `M${sx},${sy} A${r},${r} 0 ${largeArc} 1 ${fx},${fy}`;
  const gradKey = `gg-${label.replace(/[^a-z0-9]/gi, "")}-${size}`;
  return (
    <div className="gauge" style={{ width: size, height: size }}>
      <svg width={size} height={size}>
        <defs>
          <linearGradient id={gradKey} x1="0" y1="0" x2="1" y2="0">
            <stop offset="0%" stopColor="var(--hud-accent)" stopOpacity="0.4" />
            <stop offset="100%" stopColor="var(--hud-accent)" stopOpacity="1" />
          </linearGradient>
        </defs>
        <path d={bg} stroke="var(--line)" strokeWidth="3" fill="none" strokeLinecap="round" />
        <path d={fg} stroke={`url(#${gradKey})`} strokeWidth="3" fill="none" strokeLinecap="round" />
        {[0, 0.25, 0.5, 0.75, 1].map((p) => {
          const a = startAngle + totalArc * p;
          const [x1, y1] = arcTo(a);
          const rad = (a - 90) * (Math.PI / 180);
          const x2 = cx + (r - 5) * Math.cos(rad);
          const y2 = cy + (r - 5) * Math.sin(rad);
          return (
            <line
              key={p}
              x1={x1}
              y1={y1}
              x2={x2}
              y2={y2}
              stroke="var(--ink-4)"
              strokeWidth="1"
              opacity="0.5"
            />
          );
        })}
        <circle cx={fx} cy={fy} r="3" fill="var(--hud-accent)">
          <animate attributeName="r" values="3;5;3" dur="1.4s" repeatCount="indefinite" />
        </circle>
      </svg>
      <div className="gauge-label">
        <div className="num num--lg">
          {value.toFixed(decimals)}
          {unit}
        </div>
        <div
          style={{
            fontSize: 9,
            letterSpacing: "0.18em",
            color: "var(--ink-4)",
            textTransform: "uppercase",
          }}
        >
          {label}
        </div>
      </div>
    </div>
  );
}

// ── Annotation (handwritten margin note w/ arrow) ───────────────
export function Annot({
  children,
  x,
  y,
  arrow = "left",
  width,
}: {
  children: ReactNode;
  x: string | number;
  y: string | number;
  arrow?: "left" | "right" | "up" | "down";
  width?: number | string;
}) {
  const style: CSSProperties = { left: x as any, top: y as any, width };
  const arrows = {
    left: (
      <svg className="annot-arrow" viewBox="0 0 32 20">
        <path d="M30,10 Q20,14 8,12" />
        <path d="M12,8 L6,12 L10,15" />
      </svg>
    ),
    right: (
      <svg className="annot-arrow" viewBox="0 0 32 20">
        <path d="M2,10 Q12,14 24,12" />
        <path d="M20,8 L26,12 L22,15" />
      </svg>
    ),
    down: (
      <svg className="annot-arrow" viewBox="0 0 20 32">
        <path d="M10,2 Q8,14 12,24" />
        <path d="M8,20 L12,26 L16,22" />
      </svg>
    ),
    up: (
      <svg className="annot-arrow" viewBox="0 0 20 32">
        <path d="M10,30 Q8,18 12,8" />
        <path d="M8,12 L12,6 L16,10" />
      </svg>
    ),
  };
  const showBefore = arrow === "left" || arrow === "up";
  return (
    <div className="annot-wrap" style={style}>
      {showBefore && arrows[arrow]}
      <span className="annot">{children}</span>
      {!showBefore && arrows[arrow]}
    </div>
  );
}

// ── Status pill for signals ─────────────────────────────────────
export function Signal({
  kind = "none",
  label,
}: {
  kind?: "yes" | "no" | "none" | "live" | "warn";
  label?: string;
}) {
  const map = {
    yes: { cls: "chip--pos", dotCls: "hud-dot--pos", text: label || "YES" },
    no: { cls: "chip--neg", dotCls: "hud-dot--neg", text: label || "NO" },
    none: { cls: "", dotCls: "", text: label || "HOLD" },
    live: { cls: "chip--accent", dotCls: "hud-dot--live", text: label || "LIVE" },
    warn: { cls: "chip--warn", dotCls: "", text: label || "WARN" },
  } as const;
  const m = map[kind];
  return (
    <span className={`chip ${m.cls}`}>
      <span className={`hud-dot ${m.dotCls}`}></span>
      {m.text}
    </span>
  );
}

// Small helper for readable seconds → mm:ss
export function fmtSeconds(s: number): string {
  const safe = Math.max(0, Math.floor(s));
  const m = Math.floor(safe / 60);
  const r = safe % 60;
  return `${String(m).padStart(2, "0")}:${String(r).padStart(2, "0")}`;
}
