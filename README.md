# Stock Market Data Analytics Pipeline

A batch ELT pipeline that ingests, transforms, and analyzes daily U.S. equity market data for the Russell 3000 universe using Polygon.io (now Massive.com), Amazon S3, Apache Airflow, Snowflake, dbt, and Streamlit.

The goal of this project is to build an end‑to‑end, production‑style analytics stack for equity market research: from raw OHLCV data to technical indicators, market breadth metrics, and interactive dashboards.

<p align="center">
  <img src="assets/streamlit_app.png" width="100%" alt="Russell 3000 Market Intelligence dashboard">
</p>

## Architecture Overview

<p align="center">
  <img src="assets/StockMarketELT-Arch.png" width="100%" alt="Stock market ELT architecture from the provider API through S3, Snowflake, dbt, and Streamlit">
</p>

The diagram groups `STAGING`, `INTERMEDIATE`, and `MART_STAGING` into one
transformation station for readability. They remain separate schemas in the
`MARKET` Snowflake database. Airflow orchestrates the workflow, Docker Compose
provides the local runtime, and AWS IAM controls access to the S3 raw archive.

## Technology Stack

- Orchestration: Apache Airflow (Dockerized, LocalExecutor)
- Data Warehouse: Snowflake (RSA private‑key authentication)
- Raw Archive: Amazon S3 (gzip NDJSON, partitioned by API date and run ID)
- Transformation: dbt Core + `dbt-snowflake`
- Data Source: Polygon.io grouped daily aggregates API (provider now named Massive.com)
- Language & Libraries: Python, pandas, pandas‑market‑calendars, pendulum
- Visualization: Streamlit app querying Snowflake
- Containerization: Docker & Docker Compose

> **Vendor naming:** The source provider rebranded from Polygon.io to
> [Massive.com](https://massive.com/blog/polygon-is-now-massive) on October 30,
> 2025. This project retains Polygon naming in historical metadata, environment
> variables, S3 paths, and the supported `api.polygon.io` endpoint because that
> was the provider name when the retained data was ingested.

## Key Capabilities

- Scheduled weekday ingestion design for Polygon.io/Massive.com grouped daily aggregates; ingestion is currently paused.
- Versioned, replayable S3 raw landing before warehouse loading.
- NYSE trading-calendar-aware date selection that excludes weekends and exchange holidays.
- Incremental dbt models for technical indicators, with market and sector breadth facts.
- Ingestion checkpoints in Snowflake (`ADMIN.INGESTION_CHECKPOINTS`) for restartability.
- Dimensional marts for security‑level, sector‑level, and market‑level analysis.
- Streamlit dashboards for market breadth, universe screening, and ticker momentum.

## Project Structure

```text
stock_market_data_pipeline/
├── airflow/
│   ├── dags/
│   │   └── daily_stock_pipeline_dag.py   # Airflow DAG: API → S3 → Snowflake → dbt
│   ├── config/                           # Airflow configuration
│   ├── logs/                             # Airflow logs (mounted volume)
│   └── plugins/                          # Placeholder for custom operators/plugins
├── dbt/
│   └── stock_analytics/
│       ├── dbt_project.yml               # dbt project config (Snowflake)
│       ├── profiles.yml                  # Local dbt profile (Snowflake connection)
│       ├── models/
│       │   ├── staging/                  # Raw data cleaning / typing
│       │   ├── intermediate/             # Russell 3000 enrichments
│       │   ├── mart_staging/             # Gold staging / pre-mart measure prep
│       │   └── marts/                    # Analytics‑ready fact/dimension tables
│       ├── macros/                       # Reusable SQL macros (SMA, returns, etc.)
│       ├── seeds/                        # Russell 3000 constituent snapshots
│       └── tests/                        # Data quality tests
├── src/
│   ├── config.py                         # Config loader (Airflow Variables / .env)
│   ├── extraction.py                     # Market-data API interface (grouped daily)
│   ├── load.py                           # Archive raw rows in S3, then load Snowflake
│   ├── extract_load_stocks.py            # Main ingestion orchestration logic
│   ├── s3_client.py                      # Gzip NDJSON raw archive client
│   └── snowflake_client.py               # Snowflake connection + tables + checkpoints
├── data-viz/
│   ├── streamlit_app.py                  # Streamlit entrypoint
│   ├── pages/                            # Individual dashboard pages
│   │   ├── 1_Market_Breadth.py
│   │   ├── 2_Universe_Screener.py
│   │   ├── 3_Ticker_Momentum.py
│   │   └── 4_Sector_Breadth.py
│   └── utilities/
│       └── snowflake_helper.py           # Helper for querying Snowflake from Streamlit
├── docker-compose.yaml                   # Airflow + Postgres + custom image
├── infra/                                # AWS IAM and Snowflake S3 integration definitions
├── scripts/backfill_raw_to_s3.py         # Restartable historical raw archive backfill
├── requirements.txt                      # Python dependencies for Airflow image
└── .env.example                          # Example environment configuration
```

## Data Flow

### 1. Ingestion: Polygon.io/Massive.com → S3 → Snowflake

- `src/extraction.py` fetches grouped daily aggregate data from Polygon.io/Massive.com:
  - Endpoint: `GET {API_BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{date}`
  - Parameters: `adjusted=true`, `apiKey=${POLYGON_API_KEY}`
  - Includes basic retry handling for rate limits and transient errors.
- `src/load.py` lands the response as raw provider row payloads:
  - Stores each source row in `RAW_PAYLOAD`.
  - Adds operational metadata: `API_DATE`, `RUN_ID`, `SOURCE`, and `INGESTED_AT`.
  - Archives gzip NDJSON under `api_date=<date>/run_id=<run-id>/` in S3.
  - Loads that exact object into Snowflake only after the archive write succeeds.
  - Leaves business typing and normalization to dbt staging models.
- `src/snowflake_client.py`:
  - Establishes a Snowflake connection using RSA private‑key auth.
  - Ensures the `RAW.DAILY_STOCKS_RAW` table and `ADMIN.INGESTION_CHECKPOINTS` table exist.
  - Uses a Snowflake external stage and `COPY INTO` to load the archived S3 object.
  - Atomically replaces one API-date partition so retries cannot duplicate rows.
  - Records status, row counts, and S3 object metadata in ingestion checkpoints.

### 2. Orchestration: Airflow DAG

- DAG definition: `airflow/dags/daily_stock_pipeline_dag.py`
- `dag_id`: `market_data_pipeline`
- Schedule: `0 12 * * 1-5` (Mon–Fri at noon ET; last completed trading day data).
- Steps:
  1. **Extract & Load**  
     `extract()` task calls `src.extract_load_stocks.extract_load_data(days_back_override=1)` to:
        - Determine valid NYSE trading days using `pandas-market-calendars`, with daily runs targeting the last completed trading day.
     - Skip dates already marked as `completed` in `ADMIN.INGESTION_CHECKPOINTS`.
     - Fetch market data, archive it in S3, then load that object into `RAW.DAILY_STOCKS_RAW`.
  2. **Transform**  
     Shell tasks run dbt models layer‑by‑layer:
     - `dbt run --select staging`
     - `dbt run --select intermediate`
     - `dbt run --select mart_staging`
     - `dbt run --select marts`
  3. **Test**  
     - `dbt test` for model‑level and custom tests.

The DAG enforces strict ordering: Extract → Staging → Intermediate → Mart Staging → Marts → Tests.

### 3. Transformation: dbt on Snowflake

The dbt project (`dbt/stock_analytics`) uses Snowflake as its target:

- Seeds (`SEEDS` schema) hold Russell 3000 constituent snapshots at multiple dates.
- Staging models (`STAGING` schema) clean and standardize raw Snowflake tables.
- Intermediate models (`INTERMEDIATE` schema) apply business logic and enrichments.
- Mart staging models (`MART_STAGING` schema) prepare final business measures.
- Marts (`MARTS` schema) expose dimensional facts and conformed dimensions.

#### Staging Layer

- `stg_daily_stocks`
  - Source: Snowflake table `RAW.DAILY_STOCKS_RAW`.
  - Responsibilities:
    - Type casting and basic sanity checks on OHLCV data.
    - Flags invalid records (e.g., negative prices or inconsistent high/low ranges).
    - Keeps ingestion timestamps for late‑arriving overrides.

- `stg_russell3000__constituents`
  - Source: Russell 3000 CSV seed files (`seeds/russell3000_*.csv`).
  - Responsibilities:
    - Normalize ticker, company name, sector, and index weights.
    - Track membership and validity dates across multiple snapshots.

#### Intermediate Layer

- `int_russell3000__daily`
  - Joins `stg_daily_stocks` with Russell 3000 constituents.
  - Filters universe down to index members.
  - Carries forward sector/company/security metadata and index weights.

#### Analytics Marts

<p align="center">
  <img src="assets/StockMarketELT_Model.png" width="100%" alt="Snowflake marts dimensional model with shared date, security, sector, and security history dimensions">
</p>

The primary security-day star shares conformed dimensions with market-day,
sector-day, and current-snapshot facts, forming a small fact constellation.
`DIM_SECURITY_HISTORY` implements SCD Type 2 history.

- The marts layer is modeled as a small dimensional fact constellation:
  - `dim_date`: conformed trading-date dimension.
  - `dim_security`: current Type 1-style security dimension, one row per ticker.
  - `dim_security_history`: Type 2 security history dimension, one row per ticker per validity period.
  - `dim_sector`: conformed sector dimension.
  - `fct_security_daily_momentum`: security-day fact table for OHLCV and technical momentum measures.
  - `fct_market_daily_breadth`: market-day aggregate fact table.
  - `fct_sector_daily_breadth`: sector-day aggregate fact table.
  - `fct_security_current_snapshot`: latest security snapshot fact for dashboard screening.

- Security dimensions:
  - `dim_security` provides the current/latest descriptive row for dashboard-friendly joins.
  - `dim_security_history` preserves historical Russell 3000 constituent attributes using `valid_from`, `valid_to`, and `is_current`.

- `fct_security_daily_momentum` (incremental fact table)
  Daily security-level signals and technical indicators:
  - Simple Moving Averages: 20, 50, 200‑day (`sma_20`, `sma_50`, `sma_200`)
  - Relative Strength Index (RSI, 14‑day)
  - Golden/Death cross signals
  - 52‑week highs/lows
  - Relative volume vs 20‑day average

- `fct_market_daily_breadth` (aggregate fact table)
  Market‑wide health indicators across the Russell 3000:
  - Advances, declines, unchanged counts and volumes
  - Advance/Decline ratios and cumulative A/D line
  - Percentage of stocks above key moving averages (20/50/200‑day)
  - 52‑week highs/lows counts and a high/low index
  - Aggregate market RSI and simple momentum classification (overbought/oversold/normal)

- `fct_sector_daily_breadth` (aggregate fact table)
  Sector-level breadth indicators using the conformed sector dimension:
  - Advances, declines, unchanged counts and volumes by sector
  - Percentage of sector constituents above key moving averages
  - Sector RSI and simple momentum classification

- `fct_security_current_snapshot` (snapshot fact table)
  Latest snapshot per ticker with:
  - Current technical indicators (RSI, SMAs, 52‑week high/low, relative volume)
  - Performance lookbacks (1W, 1M, 3M, YTD returns)
  - Sector average performance and percentile ranking
  - Volatility metrics (annualized 20‑day) and average volume
  - Flags for “golden cross active” and “over SMA 20/50/200”

### 4. Visualization: Streamlit Dashboards

- Location: `data-viz/`
- Uses `utilities/snowflake_helper.py` to:
  - Load a PEM‑encoded RSA private key from `st.secrets`.
  - Establish a Snowflake connection.
  - Run SQL and return `pandas` DataFrames.
- Dashboard overview highlighted at the top of this README.
- Example pages:
  - `streamlit_app.py`: Home page with the latest market breadth snapshot.
  - `1_Market_Breadth.py`: Market breadth trends and key signals.
  - `2_Universe_Screener.py`: Filterable Russell 3000 snapshot from `fct_security_current_snapshot` joined to `dim_security`.
  - `3_Ticker_Momentum.py`: Ticker‑level momentum and signal history from `fct_security_daily_momentum` joined to `dim_security`.
  - `4_Sector_Breadth.py`: Sector-level breadth, participation, and momentum comparisons.

## Getting Started

### Prerequisites

- Docker & Docker Compose
- AWS account with permission to provision a scoped S3 writer and Snowflake reader role
- Snowflake account with:
  - Database (e.g., `MARKET`)
  - Warehouse (e.g., `COMPUTE_WH`)
  - Role with privileges to create schemas/tables
- Massive.com/Polygon.io API key (required only to resume ingestion)
- Python 3 (for local runs) – optional but useful

### 1. Clone the repository

```bash
git clone https://github.com/KevDev7/stock_market_data_pipeline.git
cd stock_market_data_pipeline
```

### 2. Configure environment variables

Copy the example env file and edit it:

```bash
cp .env.example .env
```

Required values:

- Massive.com (formerly Polygon.io; historical variable names retained):
  - `POLYGON_API_KEY`
  - `API_BASE_URL` (the project retains the supported `https://api.polygon.io` endpoint)
- Snowflake:
  - `SNOWFLAKE_ACCOUNT`
  - `SNOWFLAKE_USER`
  - `SNOWFLAKE_ROLE`
  - `SNOWFLAKE_WAREHOUSE`
  - `SNOWFLAKE_DATABASE`
  - `SNOWFLAKE_SCHEMA`
  - `PRIVATE_KEY_PATH` (path to PEM private key in the Airflow container)
- AWS:
  - `AWS_REGION`
  - `AWS_PROFILE` (the dedicated `stock-market-ingestion` profile)
  - `AWS_S3_BUCKET`
  - `AWS_S3_PREFIX`
  - AWS credentials for the dedicated ingestion identity
- Optional:
  - `PYTHONPATH=.`
  - `DBT_PROFILES_DIR=dbt/stock_analytics`

### 3. Provision the S3 landing integration

The checked-in files under `infra/` document the current deployment and the IAM resources required to recreate its S3 connection. The portfolio environment is already provisioned; do not redeploy the template over its manually created resources.

For a new environment:

1. Create and version an S3 bucket; the CloudFormation template intentionally assumes the bucket already exists.
2. Replace the account, role, and bucket values in `infra/snowflake/s3_raw_landing.sql`, create the storage integration, then use `DESC INTEGRATION STOCK_MARKET_S3_INTEGRATION` to obtain Snowflake's IAM principal and external ID.
3. Deploy `infra/aws/stock-market-s3-iam.yaml` with an AWS administrator, passing the bucket name and Snowflake trust values. It creates a dedicated ingestion user and the read-only role Snowflake assumes.
4. Store only the ingestion profile in Git-ignored `keys/aws-credentials` and `keys/aws-config`; Docker Compose mounts them read-only as secrets. Set the non-secret bucket, prefix, profile, and stage values from `.env.example`.

The S3 bucket and Snowflake `RAW.DAILY_STOCKS_RAW` serve different purposes: S3 is the replayable source archive; the Snowflake table remains the queryable raw warehouse layer.

### 4. Configure Snowflake RSA key authentication

1. Generate an RSA private key and corresponding public key.
2. Upload the public key to your Snowflake user (per Snowflake docs).
3. Place the private key file on your host and ensure `PRIVATE_KEY_PATH` in `.env` matches where it will be mounted in the Airflow container (e.g., `/opt/airflow/keys/snowflake_key.pem`).

`src/snowflake_client.py` and `dbt/stock_analytics/profiles.yml` both rely on this key for authentication.

### 5. Start Airflow + Postgres

```bash
docker compose up -d
```

This builds the custom Airflow image using `requirements.txt` and starts:

- Postgres (Airflow metadata)
- Airflow API server (`airflow-apiserver`)
- Airflow scheduler and DAG processor

Access the Airflow UI at:

- `http://localhost:8080`

Once Airflow is initialized, you should see the `market_data_pipeline` DAG.

### 6. Initialize dbt

Inside an Airflow container or your local environment:

```bash
cd dbt/stock_analytics
dbt deps
dbt seed --profiles-dir .
```

This loads the Russell 3000 constituent snapshots into Snowflake (`SEEDS` schema).

### 7. Backfill the S3 raw archive (optional)

The retained Snowflake raw table can reconstruct the historical archive without
calling the provider API again. Preview the scope first, then run the restartable backfill:

```bash
docker compose run --rm --entrypoint python airflow-scheduler \
  /opt/airflow/scripts/backfill_raw_to_s3.py --dry-run
docker compose run --rm --entrypoint python airflow-scheduler \
  /opt/airflow/scripts/backfill_raw_to_s3.py
```

This reconstruction preserves the retained raw rows and operational metadata; it
does not recreate the provider's original HTTP response envelope.

### 8. Enable daily pipeline

In the Airflow UI:

- Unpause `market_data_pipeline`.
- It will run Monday–Friday at 12:00 ET and ingest the previous trading day’s data, run dbt models, and execute tests.

### 9. Run the Streamlit app

The dashboard is a historical portfolio snapshot backed by the retained
Snowflake marts. Scheduled provider ingestion is currently paused, so the
displayed data-through date advances only when ingestion is intentionally
resumed.

**Live dashboard:** [Russell 3000 Market Intelligence](https://russell3000-market-intelligence.streamlit.app/)

From `data-viz/` (local environment):

1. Create `.streamlit/secrets.toml` with your Snowflake connection info, for example:

   ```toml
   [snowflake]
   account = "your_account"
   user = "STREAMLIT_DASHBOARD_USER"
   role = "STREAMLIT_DASHBOARD_ROLE"
   warehouse = "STREAMLIT_DASHBOARD_WH"
   database = "MARKET"
   schema = "MARTS"
   mart_schema = "MARTS"
   private_key = """-----BEGIN PRIVATE KEY-----
   ... your PEM key here ...
   -----END PRIVATE KEY-----"""
   ```

2. Install dependencies (if not using the Docker image):

   ```bash
   pip install -r data-viz/requirements.txt
   ```

3. Run:

   ```bash
   streamlit run data-viz/streamlit_app.py
   ```

## Testing and Data Quality

### dbt Tests

From `dbt/stock_analytics`:

```bash
dbt test --profiles-dir .
```

Custom tests include:

- RSI range validation (0–100).
- Golden/Death cross mutual exclusivity.
- 52‑week high/low consistency vs closing prices.
- Market breadth reconciliations (advances + declines + unchanged = total).
- Freshness checks for key marts.

### Ingestion Checks

- `ADMIN.INGESTION_CHECKPOINTS` tracks each trading day’s status:
  - `started`, `archived`, `completed`, or `failed`
  - Total tickers and rows inserted
  - S3 bucket, key, ETag, and SHA-256 checksum
  - Error messages (if any)
- `src.extract_load_stocks.get_completed_dates()` uses this table to avoid duplicate loads.

## Example Snowflake Queries

Use these in the Snowflake UI or via `snowflake_helper.py`:

```sql
-- Most recent golden crosses
SELECT
  s.ticker,
  s.company,
  s.sector_name
FROM MARKET.MARTS.FCT_SECURITY_DAILY_MOMENTUM AS f
INNER JOIN MARKET.MARTS.DIM_SECURITY AS s
    ON s.security_key = f.security_key
WHERE f.trade_date = (
    SELECT MAX(trade_date) FROM MARKET.MARTS.FCT_SECURITY_DAILY_MOMENTUM
)
  AND f.golden_cross = 1;
```

```sql
-- Market breadth and sentiment over the last 30 trading days
SELECT 
  trade_date,
  ad_ratio,
  pct_market_over_sma50,
  market_rsi,
  CASE 
    WHEN pct_market_over_sma50 > 0.8 THEN 'Strong Bullish'
    WHEN pct_market_over_sma50 < 0.2 THEN 'Strong Bearish'
    ELSE 'Neutral'
  END AS market_sentiment
FROM MARKET.MARTS.FCT_MARKET_DAILY_BREADTH
ORDER BY trade_date DESC
LIMIT 30;
```

```sql
-- Top performers by sector in the latest snapshot
SELECT 
  s.sector_name,
  s.ticker,
  f.latest_close,
  f.return_1m,
  f.performance_percentile
FROM MARKET.MARTS.FCT_SECURITY_CURRENT_SNAPSHOT AS f
INNER JOIN MARKET.MARTS.DIM_SECURITY AS s
    ON s.security_key = f.security_key
WHERE f.performance_percentile > 0.9
ORDER BY s.sector_name, f.return_1m DESC;
```

## Known Limitations / Future Work

- **Corporate actions:**  
  The pipeline requests Polygon.io/Massive.com aggregates with `adjusted=true`, but it does not automatically re-ingest history when adjusted values change retroactively. Indicators can therefore remain based on the retained historical snapshot until a backfill is run.

- **Universe coverage:**  
  Focused on Russell 3000 constituents via seeded snapshots. Additional universes (e.g., sector ETFs, custom watchlists) would require new seeds and joins.

- **Cost awareness:**  
  Snowflake and Massive.com/Polygon.io usage can incur costs at scale. Query patterns and warehouse sizing should be tuned for your environment.

## License

This project is for educational and portfolio purposes. Adapt configuration, credentials, and resource sizing before using in any production environment.
