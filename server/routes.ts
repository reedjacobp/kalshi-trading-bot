import type { Express } from "express";
import { createServer, type Server, request as httpRequest } from "http";

/**
 * Routes for the dashboard server.
 *
 * All trading logic now lives in the Python bot (python-bot/bot.py).
 * This server only serves the React frontend and proxies the SSE
 * stream from the Python bot to the dashboard.
 */

const BOT_SSE_HOST = process.env.BOT_SSE_HOST || "127.0.0.1";
const BOT_SSE_PORT = parseInt(process.env.BOT_SSE_PORT || "5050", 10);

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  // Proxy SSE stream from the Python bot using Node's http module
  app.get("/api/stream", (req, res) => {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
      "X-Accel-Buffering": "no",
    });

    let aborted = false;
    req.on("close", () => {
      aborted = true;
    });

    const connectToBot = () => {
      if (aborted) return;

      const botReq = httpRequest(
        {
          hostname: BOT_SSE_HOST,
          port: BOT_SSE_PORT,
          path: "/api/stream",
          method: "GET",
          headers: { Accept: "text/event-stream" },
        },
        (botRes) => {
          // Pipe the bot's SSE stream directly to the client
          botRes.on("data", (chunk: Buffer) => {
            if (!aborted) {
              res.write(chunk);
            }
          });

          botRes.on("end", () => {
            // Bot closed the connection — retry after a delay
            if (!aborted) {
              setTimeout(connectToBot, 3000);
            }
          });
        }
      );

      botReq.on("error", () => {
        if (aborted) return;
        // Bot not reachable — send waiting message and retry
        const waitingData = JSON.stringify({
          timestamp: new Date().toISOString(),
          btc_price: 0,
          btc_momentum_1m: 0,
          btc_momentum_5m: 0,
          btc_prices: [],
          eth_price: 0,
          eth_momentum_1m: 0,
          eth_momentum_5m: 0,
          eth_prices: [],
          sol_price: 0,
          sol_momentum_1m: 0,
          sol_momentum_5m: 0,
          sol_prices: [],
          current_market: null,
          last_settled: null,
          strategies: {
            momentum: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
            mean_reversion: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
            consensus: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
            resolution_rider: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
            favorite_bias: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
          },
          markets: { btc: null, eth: null, sol: null },
          settled: { btc: null, eth: null, sol: null },
          strategies_by_asset: {
            btc: {
              momentum: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
              mean_reversion: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
              consensus: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
              resolution_rider: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
              favorite_bias: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
            },
            eth: {
              momentum: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
              mean_reversion: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
              consensus: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
              resolution_rider: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
              favorite_bias: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
            },
            sol: {
              momentum: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
              mean_reversion: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
              consensus: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
              resolution_rider: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
              favorite_bias: { signal: "none", confidence: 0, reason: "Waiting for bot..." },
            },
          },
          enabled_assets: { btc: true, eth: true, sol: true },
          trading_enabled: true,
          vol_regime: "medium",
          vol_reading: 0,
          ofi: 0,
          exchange_data: {
            btc: { divergence_pct: 0, exchange_lead: null, santiment: {} },
            eth: { divergence_pct: 0, exchange_lead: null, santiment: {} },
            sol: { divergence_pct: 0, exchange_lead: null, santiment: {} },
          },
          trades: [],
          stats: {
            total_trades: 0, pending: 0, wins: 0, losses: 0,
            win_rate: 0, total_pnl: 0, total_fees: 0, total_pnl_after_fees: 0,
            daily_pnl: 0, daily_pnl_after_fees: 0, bot_paused: false,
            paper_balance: null, live_balance: null, is_paper: true,
          },
        });
        res.write(`data: ${waitingData}\n\n`);
        setTimeout(connectToBot, 3000);
      });

      botReq.end();
    };

    connectToBot();
  });

  // Helper to proxy POST endpoints to the Python bot
  const proxyPost = (path: string) => {
    app.post(path, (req, res) => {
      const postData = JSON.stringify(req.body);
      const botReq = httpRequest(
        {
          hostname: BOT_SSE_HOST,
          port: BOT_SSE_PORT,
          path,
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Content-Length": Buffer.byteLength(postData),
          },
        },
        (botRes) => {
          let body = "";
          botRes.on("data", (chunk: Buffer) => { body += chunk.toString(); });
          botRes.on("end", () => {
            try {
              res.status(botRes.statusCode || 200).json(JSON.parse(body));
            } catch {
              res.status(502).json({ error: "Bad response from bot" });
            }
          });
        }
      );
      botReq.on("error", () => {
        res.status(502).json({ error: "Bot not reachable" });
      });
      botReq.write(postData);
      botReq.end();
    });
  };

  proxyPost("/api/toggle-asset");
  proxyPost("/api/toggle-trading");


  // Health check
  app.get("/api/health", (_req, res) => {
    res.json({ status: "ok", timestamp: new Date().toISOString() });
  });

  return httpServer;
}
