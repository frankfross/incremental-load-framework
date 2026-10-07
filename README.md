# incremental-load-framework

Watermark-based incremental loads: remember the high-watermark per pipeline
(max `updated_at` or max `id` seen so far) and fetch only newer rows on each
run. A configurable lookback window re-fetches a slice of overlap so
late-arriving rows are still picked up, and upserts on the natural key make
every run safe to retry. SQLite by default, Postgres via `DATABASE_URL`.
Portfolio project.

## Layout

```
incrl/
  load.py        # IncrementalLoader: watermark -> fetch -> upsert -> advance
  sources.py     # Source protocol, CsvSource, MemorySource; watermark comparison
  watermarks.py  # persistent per-pipeline high-watermark store
  db.py          # SQLite/Postgres connection + upsert helper
  run.py         # CLI: run one pipeline or --all, prints a JSON report
config.yaml      # pipeline definitions (source, watermark column, lookback)
sample_data/     # two days of orders CSVs for the demo below
tests/           # pytest: watermarks, incremental runs, late arrivals, idempotency
```

## Quickstart

```bash
pip install -r requirements.txt

# day 1: initial load
mkdir -p data && cp sample_data/orders_day1.csv data/orders.csv
python3 -m incrl.run --config config.yaml --pipeline orders
```

```json
{
  "pipeline": "orders",
  "watermark_before": null,
  "watermark_after": "2026-10-05T18:31:00",
  "rows_seen": 6,
  "rows_inserted": 6,
  "rows_updated": 0,
  "rows_skipped_no_key": 0
}
```

```bash
# day 2: new rows arrive, plus a late correction for order 1004
# (timestamped yesterday -- inside the 24h lookback, so it is picked up)
tail -n +2 sample_data/orders_day2.csv >> data/orders.csv
python3 -m incrl.run --config config.yaml --pipeline orders
```

```json
{
  "pipeline": "orders",
  "watermark_before": "2026-10-05T18:31:00",
  "watermark_after": "2026-10-06T09:12:00",
  "rows_seen": 10,
  "rows_inserted": 3,
  "rows_updated": 7,
  "rows_skipped_no_key": 0
}
```

Only the 3 genuinely new orders were inserted; the 24h lookback re-fetched
yesterday's rows as upserts (no duplicates), and the late correction for
order 1004 landed (`89.00` -> `92.50`). Re-running with no new data changes
nothing: `rows_inserted: 0`.

Postgres instead of SQLite:

```bash
pip install "psycopg[binary]"
export DATABASE_URL="postgresql://user:pass@localhost:5432/analytics"
python3 -m incrl.run --config config.yaml --all
```

## Design notes

- **Watermarks, not full scans**: each pipeline stores one value -- the max
  watermark observed. Fetching is `watermark > bound`, so run cost is
  proportional to new data, not table size. Watermark comparison handles
  ints, floats, and ISO-8601 timestamps (mixed types in one pipeline are a
  config error, but comparisons stay total instead of crashing).
- **Lookback is an explicit tradeoff**: `lookback: "24h"` (or `"500"` rows
  for id-based watermarks) re-processes a slice of overlap on every run to
  catch late-arriving rows. That overlap is pure upsert, so it costs a
  re-read but can never duplicate data. Without a lookback you get minimal
  reads but silently miss late data -- the config forces you to choose.
- **Idempotency via natural key**: upserts keyed on `order_id` (Postgres and
  SQLite both support `ON CONFLICT ... DO UPDATE`). A crashed run is
  recovered by just re-running it; the report's inserted-vs-updated split
  shows what the re-run actually did.
- **Watermark advances to max observed, never to the fetch bound**: if a run
  fetches rows, the new watermark is the newest row's value. If a run sees
  nothing, the watermark is left alone.
- **Rows without a watermark or natural key are skipped and counted**, never
  inserted with a NULL key or an unorderable timestamp.
- State (watermarks + destination tables) lives in one SQLite file by
  default (`state/incremental.db`); the same code path talks to Postgres
  when `DATABASE_URL` is set -- only the placeholder style changes.
