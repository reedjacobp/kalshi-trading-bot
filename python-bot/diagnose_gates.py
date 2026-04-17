"""Live diagnostic: query Kalshi for current markets per cell, check each gate.
Intended as a one-shot diagnostic, not part of the normal bot flow.
"""
import json
import os
import sys
import csv
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv()

from kalshi_client import KalshiClient
from market_scanner import MarketScanner


def main():
    params = json.load(open('data/rr_params.json'))
    safe = {k: v for k, v in params.items() if v.get('cv_val_profit', 0) > 0}

    import data_paths
    prices = {}
    today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    price_dir = data_paths.resolve('prices')
    price_file = str(price_dir / f'{today}.csv')
    if not os.path.exists(price_file):
        # Fallback: pick the newest file in the resolved prices dir.
        existing = sorted(price_dir.glob('*.csv'))
        price_file = str(existing[-1]) if existing else price_file
    with open(price_file) as f:
        rows = list(csv.DictReader(f))
    if rows:
        last = rows[-1]
        for k, v in last.items():
            if k != 'timestamp' and v:
                try:
                    prices[k] = float(v)
                except ValueError:
                    pass
    print(f"Spot prices from {price_file}: {prices}")
    print()

    client = KalshiClient(
        key_id=os.getenv("KALSHI_API_KEY_ID"),
        private_key_path=os.path.expanduser(os.getenv("KALSHI_PRIVATE_KEY_PATH")),
        env=os.getenv("KALSHI_ENV", "prod"),
    )

    cell_info = {
        'bnb_15m':     ('KXBNB15M',  'bnb',  False),
        'btc_hourly':  ('KXBTCD',    'btc',  True),
        'doge_hourly': ('KXDOGED',   'doge', True),
        'eth_hourly':  ('KXETHD',    'eth',  True),
        'sol_15m':     ('KXSOL15M',  'sol',  False),
        'sol_hourly':  ('KXSOLD',    'sol',  True),
        'xrp_15m':     ('KXXRP15M',  'xrp',  False),
    }

    print("=" * 70)
    for cell in sorted(safe):
        p = safe[cell]
        series, coin, is_daily = cell_info[cell]
        spot = prices.get(coin)

        print(f"\n--- {cell}  ({series}, spot={spot}) ---")
        print(f"  Gates: price {p['min_contract_price']}-{p['max_entry_price']}c, "
              f"secs {p.get('min_seconds',10)}-{p['max_seconds']}, "
              f"buf>={p['min_price_buffer_pct']}%")

        scanner = MarketScanner(client, series=series)
        try:
            if is_daily:
                markets = scanner.get_near_certain_markets(max_hours=8/60) or []
            else:
                m = scanner.get_next_expiring_market()
                markets = [m] if m else []
        except Exception as e:
            print(f"  Fetch error: {e}")
            continue

        if not markets:
            print("  (no active markets)")
            continue

        now = datetime.now(timezone.utc)
        any_pass = False
        for m in markets[:20]:
            ticker = m.get('ticker', '?')
            close_time = m.get('close_time', '')
            strike = m.get('floor_strike')
            yes_bid, yes_ask = scanner.parse_yes_price(m)
            if yes_bid is None or yes_ask is None:
                continue
            yes_mid = (yes_bid + yes_ask) / 2
            fav_mid = max(yes_mid, 100 - yes_mid)
            side = 'yes' if yes_mid >= 50 else 'no'

            try:
                close_dt = datetime.fromisoformat(close_time.replace('Z', '+00:00'))
                secs_left = (close_dt - now).total_seconds()
            except Exception:
                secs_left = None

            try:
                strike_f = float(strike) if strike else None
            except (TypeError, ValueError):
                strike_f = None

            buffer_pct = None
            if strike_f and strike_f > 0 and spot:
                buffer_pct = (spot - strike_f) / strike_f * 100

            reasons = []
            if secs_left is None:
                reasons.append("no close_time")
            elif secs_left < 10:
                reasons.append("secs<10")
            elif secs_left > p['max_seconds']:
                reasons.append(f"secs={int(secs_left)}>{p['max_seconds']}")
            if not (p['min_contract_price'] <= fav_mid <= p['max_entry_price']):
                reasons.append(f"fav_mid={fav_mid:.0f} !in[{p['min_contract_price']},{p['max_entry_price']}]")
            if buffer_pct is None:
                reasons.append("no spot or strike")
            else:
                if side == 'yes' and buffer_pct < p['min_price_buffer_pct']:
                    reasons.append(f"yes buf {buffer_pct:+.2f}% < {p['min_price_buffer_pct']}%")
                elif side == 'no' and buffer_pct > -p['min_price_buffer_pct']:
                    reasons.append(f"no buf {buffer_pct:+.2f}% > -{p['min_price_buffer_pct']}%")

            status = "PASS" if not reasons else "; ".join(reasons)
            if not reasons:
                any_pass = True
            buf_str = f"{buffer_pct:+.2f}%" if buffer_pct is not None else "n/a"
            secs_str = f"{int(secs_left)}s" if secs_left is not None else "n/a"
            print(f"  {ticker[:42]:42s}  {yes_bid:>3}/{yes_ask:<3}  fav={fav_mid:.0f}  {secs_str:>5}  buf={buf_str:>7}  {side.upper():3s} → {status}")

        if not any_pass:
            print(f"  >>> NO market passes all gates for {cell}")


if __name__ == "__main__":
    main()
