# Architecture and Data Model

## Data Flow

### Ingestion

`src/extraction.py` requests grouped daily aggregates from Polygon.io (now
Massive.com) with `adjusted=true`. The ingestion code adds four operational
fields and keeps each source row as JSON in `RAW_PAYLOAD`:

- `API_DATE`: requested market date
- `RUN_ID`: ingestion run identifier
- `SOURCE`: historical source name, `polygon_grouped_daily`
- `INGESTED_AT`: landing timestamp

`src/load.py` writes these rows as gzip NDJSON to:

```text
s3://<bucket>/raw/polygon/grouped-daily/
  api_date=<date>/run_id=<run-id>/daily_stocks_raw.ndjson.gz
```

After archival succeeds, Snowflake loads the same object through the external
stage. The load atomically replaces the matching `API_DATE` in
`RAW.DAILY_STOCKS_RAW`, making retries idempotent. S3 remains the replayable file
archive.

### Snowflake Layers

| Schema | Responsibility | Main objects |
|---|---|---|
| `RAW` | Queryable source landing | `DAILY_STOCKS_RAW`, external stage, JSON file format |
| `STAGING` | Parse JSON, cast fields, normalize seeds, flag invalid rows | `STG_DAILY_STOCKS`, `STG_RUSSELL3000__CONSTITUENTS` |
| `INTERMEDIATE` | Join market rows to point-in-time Russell 3000 membership | `INT_RUSSELL3000__DAILY` |
| `MART_STAGING` | Prepare rolling measures and final fact rows | Four `PREP_*` models |
| `MARTS` | Conformed dimensions and analytics facts | Four dimensions and four facts |
| `SEEDS` | Russell 3000 constituent CSV snapshots | `RUSSELL3000_*` |
| `ADMIN` | Operational ingestion state | `INGESTION_CHECKPOINTS` |

This is a batch ELT design: extraction and landing occur before dbt executes
business transformations inside Snowflake. Pre-load processing is limited to
operational metadata and wrapping each source row as JSON.

## Load Behavior

| Target | Load type | Method | Write behavior |
|---|---|---|---|
| `RAW.DAILY_STOCKS_RAW` | Incremental | Partition-based on `API_DATE` | Overwrite one date partition |
| Staging models | No physical row load | Views | Create or replace view |
| `INT_RUSSELL3000__DAILY` | Incremental | Timestamp/date lookback | `MERGE` by ticker and trade date |
| `PREP_SECURITY_DAILY_MOMENTUM` | Incremental | Timestamp/date lookback | `MERGE` by ticker and trade date |
| Other `MART_STAGING` models | Full | Table rebuild | Overwrite table |
| `FCT_SECURITY_DAILY_MOMENTUM` | Incremental | Timestamp/date lookback | `MERGE` by security and date key |
| Other marts | Full | Table rebuild | Overwrite table |
| Russell 3000 seeds | Full | Seed reload | Replace seed tables |
| Ingestion checkpoints | Operational log | Per-state event | Append |

## Dimensional Model

The marts form a small fact constellation built around a primary security-day
star. Conformed dimensions allow facts at different grains to be analyzed
consistently.

### Dimensions

| Model | Grain | Purpose |
|---|---|---|
| `DIM_DATE` | One row per trading date | Calendar attributes shared by all dated facts |
| `DIM_SECURITY` | One row per ticker | Current descriptive security attributes |
| `DIM_SECURITY_HISTORY` | Ticker x validity period | SCD Type 2 history from constituent snapshots |
| `DIM_SECTOR` | One row per sector | Shared sector classification |

`DIM_SECURITY_HISTORY` uses `VALID_FROM`, `VALID_TO`, and `IS_CURRENT` to retain
attribute versions. It is implemented as a dbt table model rather than a dbt
snapshot.

### Facts

| Model | Grain | Measures |
|---|---|---|
| `FCT_SECURITY_DAILY_MOMENTUM` | Security x trading date | OHLCV, SMAs, RSI, relative volume, crossovers, 52-week levels |
| `FCT_MARKET_DAILY_BREADTH` | Trading date | Advances, declines, A/D metrics, breadth participation, highs/lows |
| `FCT_SECTOR_DAILY_BREADTH` | Sector x trading date | Sector breadth, RSI, participation, and momentum |
| `FCT_SECURITY_CURRENT_SNAPSHOT` | Security | Latest signals, returns, volatility, rankings, and screener flags |

## Orchestration

The Airflow DAG runs these six tasks in strict order:

1. Extract, archive to S3, and load Snowflake RAW
2. Build dbt staging views
3. Build the incremental intermediate model
4. Build mart-staging models
5. Publish marts
6. Run dbt tests

The cron schedule is `0 12 * * 1-5` in `America/New_York`. NYSE calendar logic
targets the latest completed trading date, and completed checkpoints prevent
duplicate work.

## Security Boundaries

- The ingestion IAM user can list the configured S3 prefix and read/write its
  objects but cannot delete them.
- Snowflake assumes a dedicated read-only IAM role for the same prefix.
- Docker Compose mounts the project AWS profile as read-only secrets.
- Snowflake clients and Streamlit authenticate with RSA private keys.

The source provider renamed Polygon.io to Massive.com in October 2025. Runtime
identifiers keep the original name for compatibility and an accurate history.
