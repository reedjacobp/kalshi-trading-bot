import React from "react";
import type { TickData } from "@shared/schema";

// Mini orderbook ladder synthesized from top-of-book yes_bid/yes_ask.
// The Kalshi SSE feed only carries top-of-book today; this draws plausible
// ±2c levels around the spread for the HUD.
export function MiniLadder({ market }: { market: TickData["current_market"] }) {
  if (!market) {
    return (
      <div style={{ padding: 24, color: "var(--ink-4)", textAlign: "center" }}>
        no market selected
      </div>
    );
  }
  const bid = market.yes_bid;
  const ask = market.yes_ask;
  const mid = ((bid + ask) / 2).toFixed(1);
  const rows: { side: "bid" | "ask" | "mid"; p: number | string }[] = [
    { side: "ask", p: Math.min(99, ask + 2) },
    { side: "ask", p: Math.min(99, ask + 1) },
    { side: "ask", p: ask },
    { side: "mid", p: `${mid}¢` },
    { side: "bid", p: bid },
    { side: "bid", p: Math.max(1, bid - 1) },
    { side: "bid", p: Math.max(1, bid - 2) },
  ];
  return (
    <div className="ladder">
      <div className="ladder-head">BID</div>
      <div className="ladder-head" style={{ textAlign: "center" }}>PRICE</div>
      <div className="ladder-head" style={{ textAlign: "right" }}>ASK</div>
      {rows.map((r, i) => {
        if (r.side === "mid") {
          return (
            <div key={i} className="ladder-mid" style={{ gridColumn: "1 / -1" }}>
              ── MID {r.p} ──
            </div>
          );
        }
        return (
          <React.Fragment key={i}>
            <div
              className={`ladder-row ${r.side === "bid" ? "bid" : ""}`}
              style={{ color: r.side === "bid" ? "var(--pos)" : "var(--ink-3)" }}
            >
              {r.side === "bid" && r.p === bid && (
                <div className="ladder-bar" style={{ width: "60%" }}></div>
              )}
              <span>{r.side === "bid" && r.p === bid ? "top" : ""}</span>
            </div>
            <div className="ladder-row" style={{ textAlign: "center", justifyContent: "center" }}>
              <span className="ladder-price">{r.p}¢</span>
            </div>
            <div
              className={`ladder-row ${r.side === "ask" ? "ask" : ""}`}
              style={{
                color: r.side === "ask" ? "var(--neg)" : "var(--ink-3)",
                justifyContent: "flex-end",
              }}
            >
              {r.side === "ask" && r.p === ask && (
                <div className="ladder-bar" style={{ width: "60%" }}></div>
              )}
              <span>{r.side === "ask" && r.p === ask ? "top" : ""}</span>
            </div>
          </React.Fragment>
        );
      })}
    </div>
  );
}
