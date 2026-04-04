# Kalshi 15-Minute Crypto Trading Bot

An algorithmic trading bot for Kalshi's 15-minute Bitcoin and Ethereum prediction markets (`KXBTC15M` / `KXETH15M`). Supports paper trading out of the box and live trading with API keys.

## How Kalshi 15-Minute Crypto Markets Work

Every 15 minutes, Kalshi opens a new binary contract asking: **"Will BTC be higher at the end of this 15-minute window?"**

- **YES** contracts pay $1.00 if BTC finishes higher, $0.00 if not
- **NO** contracts pay $1.00 if BTC finishes lower, $0.00 if not
- Contracts trade between $0.01 and $0.99 (representing implied probability)
- YES price + NO price always equals $1.00

Example: If you buy YES at $0.45 and BTC goes up, you profit $0.55. If BTC goes down, you lose $0.45.

## Strategies

### 1. Consensus Strategy (Recommended)

The flagship strategy. Only trades when multiple independent signals agree, resulting in the highest win rate (~60-65%) at the cost of fewer trades.

**Signals combined:**
- **Momentum**: Is BTC trending up or down over the last 60 seconds?
- **Previous Result**: Did the last 15-min window settle YES or NO? (trend continuation)
- **Orderbook Skew**: Is the Kalshi contract price leaning bullish or bearish?

**Entry rules:**
- At least 2 of 3 signals must agree on direction
- Contract price must be 35-57 cents on our side (sweet spot for edge)
- At least 3 minutes remaining in the window
- Must pass all risk manager checks

**Why it works:** By requiring agreement across independent signal sources, the strategy filters out noise and only takes high-conviction trades. The 35-57 cent price range means we're getting meaningful odds while avoiding overpriced contracts.

### 2. Momentum Strategy

Pure trend-following. If BTC is moving in a clear direction across multiple timeframes, bet that it continues.

**Signals:**
- 1-minute momentum (primary, fast signal)
- 5-minute momentum (confirmation, slower signal)
- 30-second price velocity (acceleration check)

**Entry rules:**
- Both 1-min and 5-min momentum must agree
- Minimum 0.05% move on 1-min, 0.02% on 5-min
- Price velocity must confirm (not decelerating)
- Contract price in 30-60 cent range

### 3. Mean Reversion Strategy

Contrarian strategy that bets against overextended moves. When BTC spikes sharply in one direction but the broader trend doesn't support it, bet on a snapback.

**Signals:**
- Sharp 1-min spike (>0.12%) without 5-min confirmation
- Contract price skewed to an extreme (>60 or <40)
- Needs at least 5 minutes remaining for reversion to play out

## Architecture

```
bot.py                  # Main entry point and event loop
├── kalshi_client.py    # Kalshi API v2 client (RSA-PSS auth)
├── market_scanner.py   # Market discovery and tracking
├── price_feed.py       # Real-time BTC/ETH prices from Coinbase
├── risk_manager.py     # Position sizing, loss limits, risk controls
└── strategies/
    ├── base.py         # Strategy interface and Signal types
    ├── momentum.py     # Multi-timeframe momentum strategy
    ├── mean_reversion.py # Counter-trend spike reversion
    └── consensus.py    # Multi-signal consensus (recommended)
```

## Quick Start

### Paper Trading (No API Keys Needed)

```bash
# Install dependencies
pip install -r requirements.txt

# Copy config
cp .env.example .env

# Run with defaults (paper trading, consensus strategy, BTC)
python bot.py

# Run all strategies simultaneously
python bot.py --strategy all

# Trade ETH markets instead
python bot.py --series KXETH15M
```

### Live Trading

1. **Get API keys** from [Kalshi](https://kalshi.com/account/profile) → API Keys
2. **Configure `.env`**:
   ```
   KALSHI_API_KEY_ID=your-key-id
   KALSHI_PRIVATE_KEY_PATH=~/.key/kalshi/key.pem
   KALSHI_ENV=demo          # Start with demo!
   PAPER_TRADE=false
   ```
3. **Test on demo first:**
   ```bash
   python bot.py --live
   ```
4. **Switch to production** when confident:
   ```
   KALSHI_ENV=prod
   ```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KALSHI_API_KEY_ID` | — | Your Kalshi API key ID |
| `KALSHI_PRIVATE_KEY_PATH` | — | Path to RSA private key file |
| `KALSHI_ENV` | `demo` | `demo` or `prod` |
| `MARKET_SERIES` | `KXBTC15M` | `KXBTC15M` (Bitcoin) or `KXETH15M` (Ethereum) |
| `STAKE_USD` | `5.00` | Dollars per trade |
| `MAX_DAILY_LOSS_USD` | `25.00` | Stop trading after this daily loss |
| `MAX_CONCURRENT_POSITIONS` | `3` | Max simultaneous open positions |
| `POLL_INTERVAL_SECONDS` | `5` | Market polling interval |
| `STRATEGY` | `consensus` | `momentum`, `mean_reversion`, `consensus`, or `all` |
| `PAPER_TRADE` | `true` | Set `false` for real money |

## Risk Management

The risk manager enforces multiple layers of protection:

- **Daily loss cap**: Stops trading after reaching max daily loss ($25 default)
- **Weekly loss cap**: Additional weekly limit ($75 default)
- **Position sizing**: Max 2% of balance per trade
- **Concurrent positions**: Max 3 open at once
- **Cooldown**: 60-second pause after any loss
- **Hourly rate limit**: Max 20 trades per hour
- **Confidence filter**: Only trades above 0.3 confidence threshold

## Output

The bot logs all trades to CSV in the `data/` directory:

| Column | Description |
|--------|-------------|
| `time` | ISO timestamp |
| `strategy` | Which strategy triggered the trade |
| `ticker` | Kalshi market ticker |
| `side` | `yes` or `no` |
| `price_cents` | Entry price (1-99) |
| `contracts` | Number of contracts |
| `stake_usd` | Total stake |
| `outcome` | `win`, `loss`, or pending |
| `profit_usd` | Net profit/loss |

## API Reference

The bot uses the Kalshi Trading API v2 with RSA-PSS signed requests:

- **Base URLs:**
  - Demo: `https://demo-api.kalshi.co/trade-api/v2`
  - Production: `https://api.elections.kalshi.com/trade-api/v2`
- **Auth headers:** `KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-SIGNATURE`, `KALSHI-ACCESS-TIMESTAMP`
- **Market data** endpoints are public (no auth needed for paper trading)
- **Trading** endpoints require API key authentication

## Disclaimer

This software is for educational purposes. Trading on prediction markets involves risk of loss. Past performance of any strategy does not guarantee future results. Start with paper trading and the demo environment. Never risk money you can't afford to lose.
