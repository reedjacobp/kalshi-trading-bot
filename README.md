# Kalshi 15-Minute Crypto Trading Bot

An algorithmic trading system for Kalshi's 15-minute Bitcoin prediction markets (KXBTC15M). Includes a real-time web dashboard and a standalone Python bot.

## What's Inside

```
├── dashboard/           # Web dashboard (Express + React + Tailwind)
│   ├── server/          # Backend: live data feeds, strategy engine, SSE
│   ├── client/          # Frontend: real-time charts, signals, trade history
│   └── shared/          # Shared types
├── python-bot/          # Standalone Python trading bot
│   ├── bot.py           # Main entry point
│   ├── kalshi_client.py # Kalshi API v2 client (RSA-PSS auth)
│   ├── strategies/      # Trading strategies
│   └── risk_manager.py  # Position sizing and risk controls
```

## Strategies

| Strategy | Edge Hypothesis | When It Fires |
|----------|----------------|---------------|
| **Consensus (4 signals)** | Multi-signal agreement reduces noise | 3+ of 4 signals agree: BTC momentum, previous market result, Kalshi orderbook skew, crypto exchange OFI |
| **Resolution Rider** | Final 90 seconds are dominated by settlement momentum | Last 90s of window, when market strongly favors one side and BTC momentum confirms |
| **Favorite-Longshot Bias** | Academic research shows favorites win more often than their price implies | When YES > 70c or NO > 70c, with 3+ minutes remaining |
| **Momentum** | Short-term crypto prices exhibit positive autocorrelation | 1-min and 5-min BTC momentum agree, with confirming price velocity |
| **Mean Reversion** | Sharp spikes tend to partially retrace | 1-min spike > 0.12% without 5-min confirmation, contract skewed to extreme |

## Key Features

- **Kelly Criterion sizing** — 0.25x fractional Kelly based on estimated edge, not flat dollar amounts
- **Order Flow Imbalance (OFI)** — reads Crypto.com BTC orderbook for buy/sell pressure as a leading indicator
- **Real settlement tracking** — paper trades stay pending until Kalshi actually settles the market
- **Daily loss limit** — auto-pauses at -$50 with a visual banner
- **Live data** — BTC from Coinbase, markets from Kalshi production API, orderbook from Crypto.com

## Quick Start

### Web Dashboard
```bash
cd dashboard
npm install
npm run dev
# Open http://localhost:5000
```

### Python Bot
```bash
cd python-bot
pip install -r requirements.txt
cp .env.example .env
python bot.py              # Paper trading (default)
python bot.py --live       # Live trading (needs API keys in .env)
```

## Based On

Strategies are informed by [academic research on Kalshi market microstructure](https://www2.gwu.edu/~forcpgm/2026-001.pdf), including the favorite-longshot bias, Kelly Criterion frameworks for binary contracts, and order flow imbalance as a leading indicator.

## Disclaimer

For educational purposes. Trading involves risk of loss. Start with paper trading.
