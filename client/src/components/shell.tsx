import React, { useEffect, useMemo, useState, type ReactNode } from "react";
import { useLocation } from "wouter";
import type { TickData } from "@shared/schema";

export type PageKey = "dashboard" | "pnl" | "market" | "risk" | "backtest";

export const PAGES: {
  key: PageKey;
  num: string;
  label: string;
  sub: string;
  path: string;
}[] = [
  { key: "dashboard", num: "01", label: "Dashboard", sub: "live", path: "/" },
  { key: "pnl", num: "02", label: "Trade History & P&L", sub: "analytics", path: "/pnl" },
  { key: "market", num: "03", label: "Market Deep-dive", sub: "live", path: "/market" },
  { key: "risk", num: "04", label: "Risk & Config", sub: "control", path: "/risk" },
  { key: "backtest", num: "05", label: "Backtest · Analytics", sub: "research", path: "/backtest" },
];

type Tweaks = { nav: "sidebar" | "topbar"; theme: "dark" | "light" };
const DEFAULT_TWEAKS: Tweaks = { nav: "sidebar", theme: "dark" };

function Brand({ mini }: { mini?: boolean }) {
  return (
    <div className="brand">
      <div className="brand-mark"></div>
      {!mini && (
        <div>
          <div className="brand-text">Rider // HUD</div>
          <div className="brand-sub">wire v0.1</div>
        </div>
      )}
    </div>
  );
}

function NavItem({
  page,
  active,
  onClick,
  topbar,
}: {
  page: (typeof PAGES)[number];
  active: boolean;
  onClick: () => void;
  topbar?: boolean;
}) {
  return (
    <div
      className={`nav-item ${active ? "is-active" : ""}`}
      onClick={onClick}
    >
      <span
        style={{
          fontFamily: "var(--font-hud-mono)",
          fontSize: 10,
          color: "var(--ink-4)",
          letterSpacing: "0.1em",
        }}
      >
        {page.num}
      </span>
      <span>{page.label}</span>
      {!topbar && <span className="nav-num">{page.sub}</span>}
    </div>
  );
}

function SessionStats({ connected, stats }: { connected: boolean; stats?: TickData["stats"] }) {
  return (
    <div className="sidebar-session">
      <div
        className="hud-between"
        style={{ padding: "8px 12px", fontSize: 11, color: "var(--ink-3)" }}
      >
        <span>Feed</span>
        <span>
          <span
            className={`hud-dot ${connected ? "hud-dot--live" : "hud-dot--neg"}`}
            style={{ marginRight: 6 }}
          ></span>
          {connected ? "LIVE" : "DOWN"}
        </span>
      </div>
      <div
        className="hud-between"
        style={{ padding: "8px 12px", fontSize: 11, color: "var(--ink-3)" }}
      >
        <span>Mode</span>
        <span
          className="num num--xs"
          style={{ color: stats?.is_paper ? "var(--warn)" : "var(--hud-accent)" }}
        >
          {stats?.is_paper ? "PAPER" : "LIVE"}
        </span>
      </div>
      <div
        className="hud-between"
        style={{ padding: "8px 12px", fontSize: 11, color: "var(--ink-3)" }}
      >
        <span>{stats?.bot_paused ? "Paused" : "Trading"}</span>
        <span
          className="num num--xs"
          style={{ color: stats?.bot_paused ? "var(--neg)" : "var(--pos)" }}
        >
          {stats?.bot_paused ? "OFF" : "ON"}
        </span>
      </div>
    </div>
  );
}

function Sidebar({
  current,
  setCurrent,
  connected,
  stats,
}: {
  current: PageKey;
  setCurrent: (k: PageKey) => void;
  connected: boolean;
  stats?: TickData["stats"];
}) {
  return (
    <aside className="sidebar">
      <Brand />
      <div className="nav-section-label">Screens</div>
      {PAGES.map((p) => (
        <NavItem
          key={p.key}
          page={p}
          active={current === p.key}
          onClick={() => setCurrent(p.key)}
        />
      ))}
      <div className="sidebar-spacer" style={{ flex: 1 }}></div>
      <div className="nav-section-label sidebar-session-label">Session</div>
      <SessionStats connected={connected} stats={stats} />
    </aside>
  );
}

function Topbar({
  current,
  setCurrent,
  connected,
  stats,
}: {
  current: PageKey;
  setCurrent: (k: PageKey) => void;
  connected: boolean;
  stats?: TickData["stats"];
}) {
  return (
    <header className="topbar">
      <Brand mini />
      {PAGES.map((p) => (
        <NavItem
          key={p.key}
          page={p}
          active={current === p.key}
          onClick={() => setCurrent(p.key)}
          topbar
        />
      ))}
      <div className="topbar-spacer"></div>
      <div
        style={{
          fontSize: 11,
          color: "var(--ink-3)",
          gap: 16,
          display: "flex",
          alignItems: "center",
        }}
      >
        <span>
          <span
            className={`hud-dot ${connected ? "hud-dot--live" : "hud-dot--neg"}`}
            style={{ marginRight: 6 }}
          ></span>
          {connected ? "LIVE" : "DOWN"}
        </span>
        <span className="num num--xs" style={{ color: stats?.is_paper ? "var(--warn)" : "var(--hud-accent)" }}>
          {stats?.is_paper ? "PAPER" : "LIVE $"}
        </span>
      </div>
    </header>
  );
}

function TweaksPanel({
  show,
  tweaks,
  setTweaks,
  onHide,
}: {
  show: boolean;
  tweaks: Tweaks;
  setTweaks: (t: Tweaks) => void;
  onHide: () => void;
}) {
  const set = <K extends keyof Tweaks>(k: K, v: Tweaks[K]) =>
    setTweaks({ ...tweaks, [k]: v });
  return (
    <div className={`tweaks ${show ? "" : "is-hidden"}`}>
      <h4>
        <span>Tweaks</span>
        <button className="hud-btn hud-btn--ghost hud-btn--sm" onClick={onHide}>
          ×
        </button>
      </h4>
      <div className="tweaks-row">
        <span>Navigation</span>
        <div className="seg">
          <button
            className={tweaks.nav === "sidebar" ? "is-on" : ""}
            onClick={() => set("nav", "sidebar")}
          >
            sidebar
          </button>
          <button
            className={tweaks.nav === "topbar" ? "is-on" : ""}
            onClick={() => set("nav", "topbar")}
          >
            topbar
          </button>
        </div>
      </div>
      <div className="tweaks-row">
        <span>Theme</span>
        <div className="seg">
          <button
            className={tweaks.theme === "dark" ? "is-on" : ""}
            onClick={() => set("theme", "dark")}
          >
            dark
          </button>
          <button
            className={tweaks.theme === "light" ? "is-on" : ""}
            onClick={() => set("theme", "light")}
          >
            light
          </button>
        </div>
      </div>
      <div
        style={{
          marginTop: 10,
          fontSize: 10,
          color: "var(--ink-4)",
          lineHeight: 1.5,
          fontFamily: "var(--font-hud-mono)",
          letterSpacing: "0.04em",
        }}
      >
        // holographic glass · greyscale + cyan
        <br />
        // ticking numbers · pulsing signals
        <br />
        // shift + T to toggle this panel
      </div>
    </div>
  );
}

export function Shell({
  connected,
  stats,
  children,
}: {
  connected: boolean;
  stats?: TickData["stats"];
  children: ReactNode;
}) {
  const [location, setLocation] = useLocation();
  const [tweaks, setTweaks] = useState<Tweaks>(() => {
    try {
      const raw = localStorage.getItem("hud-tweaks");
      if (raw) return { ...DEFAULT_TWEAKS, ...JSON.parse(raw) };
    } catch {}
    return DEFAULT_TWEAKS;
  });
  const [showTweaks, setShowTweaks] = useState(false);

  const current = useMemo<PageKey>(() => {
    const hit = PAGES.find((p) => p.path === location);
    return hit?.key ?? "dashboard";
  }, [location]);

  const setCurrent = (key: PageKey) => {
    const hit = PAGES.find((p) => p.key === key);
    if (hit) setLocation(hit.path);
  };

  const currentPage = PAGES.find((p) => p.key === current)!;

  useEffect(() => {
    document.body.dataset.theme = tweaks.theme;
    localStorage.setItem("hud-tweaks", JSON.stringify(tweaks));
  }, [tweaks]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.shiftKey && (e.key === "T" || e.key === "t")) {
        setShowTweaks((s) => !s);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const navProps = { current, setCurrent, connected, stats };
  return (
    <div className={`shell ${tweaks.nav === "topbar" ? "shell--topbar" : ""}`}>
      {tweaks.nav === "sidebar" ? <Sidebar {...navProps} /> : <Topbar {...navProps} />}
      <main
        className="content"
        data-screen-label={`${currentPage.num} ${currentPage.label}`}
      >
        {children}
      </main>
      <TweaksPanel
        show={showTweaks}
        tweaks={tweaks}
        setTweaks={setTweaks}
        onHide={() => setShowTweaks(false)}
      />
    </div>
  );
}
