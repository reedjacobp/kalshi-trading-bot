# DeltaBase BigQuery Setup

Goal: get read access to `deltabase-public.prediction_markets` (a public BigQuery dataset
with Kalshi + Polymarket history) so we can verify schemas and then wire it into
`optimize_rr.py` as an additional training source.

You don't need to pay anything — BigQuery's free tier includes 1 TB/month of query
scanning, and the diagnostic queries below scan well under 1 GB total.

---

## 1. Create a Google Cloud project (5 min)

1. Go to <https://console.cloud.google.com/>. Sign in with any Google account.
2. If this is your first time, accept the terms. You may be asked to start a free
   trial — you can click through it without attaching a payment method; the free
   tier alone is enough for our use.
3. Click the project selector in the top bar → **New Project**.
4. Name it `kalshi-bot-data` (or whatever). Leave org as "No organization". Click **Create**.
5. Wait ~30 seconds for the project to provision, then select it from the project selector.

## 2. Enable BigQuery (1 min)

1. In the left nav, go to **BigQuery** (or search for it in the top bar).
2. First-time visitors get a welcome screen — click through. The BigQuery API is
   enabled automatically the first time you open the console.
3. You should land in the BigQuery SQL workspace with an empty query editor.

## 3. Pin the public project in your Explorer (30 sec)

Per deltabase.tech's own instructions:

1. In the BigQuery console, look at the **Explorer** panel on the left side.
2. Click the **"+ ADD"** button at the top (or the "Add data" / three-dots menu).
3. Choose **"Star a project by name"** (sometimes labelled **"Pin a project"**
   or **"Add a project"**).
4. Type `deltabase-public` and confirm.
5. The project appears in your Explorer tree. Expand it → expand
   `prediction_markets` → you'll see a list of tables (things like `kalshi_trades`,
   `kalshi_markets`, `polymarket_trades`, etc. — the exact names aren't documented
   anywhere, we're about to discover them).

**Click on each table once** to see:
- Schema (column names + types) in the **Schema** tab
- Row count + size in the **Details** tab
- A sample of 100 rows in the **Preview** tab — no query cost

That visual browse alone will answer most of my questions. **Please screenshot
or transcribe** the table list and, for whichever table looks like the main
Kalshi trades table, its schema + row count.

## 4. Run the diagnostic queries (2 min)

Once you know the table names from step 3, run these queries in the editor.
Paste the full result of each back to me.

If the table names I guess don't match, just substitute whatever you saw in
Explorer.

### Query 1 — list all tables via INFORMATION_SCHEMA

`__TABLES__` is a legacy metadata endpoint that public datasets often disallow
(you hit that error earlier). `INFORMATION_SCHEMA.TABLES` is the standard-SQL
equivalent and works on public datasets:

```sql
SELECT table_name, table_type, creation_time
FROM `deltabase-public.prediction_markets.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name;
```

If `INFORMATION_SCHEMA.TABLES` *also* fails with a permission error, that means
deltabase locked down metadata access — in which case skip this query and rely
on the visual Explorer browse from step 3 to get the table names.

**Paste the full result.** I need to see the complete list of tables.

---

### Query 2 — schema of the main Kalshi trades table

Substitute `<kalshi_trades_table>` with whatever Query 1 (or the Explorer browse)
showed as the largest Kalshi table. Good guesses: `kalshi_trades`, `kalshi_fills`,
`trades`, `kalshi_markets`, etc.

```sql
SELECT column_name, data_type, is_nullable
FROM `deltabase-public.prediction_markets.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '<kalshi_trades_table>'
ORDER BY ordinal_position;
```

If `INFORMATION_SCHEMA.COLUMNS` also hits a permissions wall, fall back to this
one-liner instead — it reads one row from the table itself and shows you all the
columns in the result, which requires only read access (no metadata permissions):

```sql
SELECT * FROM `deltabase-public.prediction_markets.<kalshi_trades_table>` LIMIT 1;
```

The `LIMIT 1` keeps the cost near-zero even on huge tables.

**Paste the full column list.** I'm specifically looking for:

- A `ticker` / `market_ticker` column
- A `created_time` / `timestamp` column
- `yes_bid` / `yes_ask` or equivalent bid/ask columns (this is the critical one — if
  the table only has executed-trade prints without book context, it's less useful
  for our purposes)
- `count` / `size` / `quantity` column
- Any `fee` or `taker` columns

---

### Query 3 — coverage check for our series

Substitute `<kalshi_trades_table>` with the actual table name, AND substitute
`created_time` with whatever timestamp column Query 2 revealed (might be
`created_time`, `timestamp`, `trade_time`, `ts`, etc.). If in doubt, run
Query 2 first and check.

```sql
SELECT
  REGEXP_EXTRACT(ticker, r'^(KX[A-Z]+(?:15M|D))') AS series,
  COUNT(*) AS rows,
  MIN(created_time) AS earliest,
  MAX(created_time) AS latest
FROM `deltabase-public.prediction_markets.<kalshi_trades_table>`
WHERE ticker LIKE 'KXBTC%'
   OR ticker LIKE 'KXETH%'
   OR ticker LIKE 'KXSOL%'
   OR ticker LIKE 'KXDOGE%'
   OR ticker LIKE 'KXXRP%'
   OR ticker LIKE 'KXBNB%'
   OR ticker LIKE 'KXHYPE%'
GROUP BY series
ORDER BY rows DESC;
```

**Caution:** this query scans every row in the table where ticker starts with
`KX`. For a multi-GB `kalshi_trades` table, that could burn 1-5 GB of your
1 TB monthly quota. Still fine, but don't run it repeatedly.

If you want a cheaper pre-check, run this first to see approximate table size:

```sql
SELECT table_name, total_rows, ROUND(total_logical_bytes / 1e9, 2) AS gb
FROM `deltabase-public.prediction_markets.INFORMATION_SCHEMA.TABLE_STORAGE`
ORDER BY total_logical_bytes DESC;
```

If `TABLE_STORAGE` is permission-blocked too, just eyeball the "Details" tab
for each table in the Explorer from step 3 — it shows the table size directly.

**Paste the full result.** This tells us:

- Whether all 14 of our series are covered (7 coins × `15M` + `D`)
- How far back history goes for each
- How recent the most recent data is (freshness)

---

## 5. Watch your quota

After running the queries, check how much you've scanned:
<https://console.cloud.google.com/iam-admin/quotas> and filter for "BigQuery".

Expected usage for all queries above: **well under 10 GB total**, which is a
rounding error against your 1 TB/month free tier.

**Before hitting "Run" on any query, BigQuery shows an estimated bytes-to-scan
count in the bottom-right of the query editor** (e.g., "This query will process
423.1 MB when run"). If you ever see a number in the tens of GB or higher, stop
and ping me — something's off.

Tip: you can also dry-run a query without executing by clicking the 3-dot menu
next to "Run" → "Query settings" → "Dry run". That shows bytes-to-scan without
actually running it.

---

## What happens next

Once you paste the three results back, I'll know:

1. **If the schema has bid/ask** → I wire up a `load_deltabase_windows()` function
   in `optimize_rr.py` that pulls data straight from BigQuery into pandas and feeds
   it into the existing training pipeline. Should give us 10-100× more training
   samples per cell.
2. **If the schema is trade-prints-only** → still useful but limited. I'd use it as
   a validation pass ("do the current cell params also look profitable over this
   13-month history?") rather than as training input.
3. **If our series aren't covered** → we skip deltabase entirely and look at
   alternatives (synthesis.trade, historical Binance klines for spot-price backfill,
   or just waiting another week for our own recorder to accumulate more data).

You can run the three queries in parallel tabs if you want — they're independent
and together take <30 seconds of runtime. The biggest time sink is the GCP project
creation (step 1), which you only have to do once.
