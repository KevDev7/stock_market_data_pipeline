# Stock Market Data Analytics Pipeline

A modern data pipeline that ingests, transforms, and analyzes daily U.S. equity market data for the Russell 3000 universe using Polygon.io, Apache Airflow, Snowflake, dbt, and Streamlit.

The goal of this project is to build an end‑to‑end, production‑style analytics stack for equity market research: from raw OHLCV data to technical indicators, market breadth metrics, and interactive dashboards.

![Market breadth dashboard](assets/market_breadth.png)

## Architecture Overview

```mermaid
graph LR
    A[Polygon API] -->|Daily Extract| B[Apache Airflow]
    B -->|Load| C[Snowflake RAW.DAILY_STOCKS]
    C -->|dbt Transform| D[Snowflake raw_staging]
    D --> E[Snowflake raw_intermediate]
    E --> F[Snowflake raw_marts]
    F --> G[Streamlit Dashboards]
```

## Technology Stack

- Orchestration: Apache Airflow (Dockerized, LocalExecutor)
- Data Warehouse: Snowflake (RSA private‑key authentication)
- Transformation: dbt Core + `dbt-snowflake`
- Data Source: Polygon.io grouped daily aggregates API
- Language & Libraries: Python, pandas, pandas‑market‑calendars, pendulum
- Visualization: Streamlit app querying Snowflake
- Containerization: Docker & Docker Compose

## Key Capabilities

- Automated daily ingestion of Polygon grouped daily aggregates into Snowflake.
- Trading‑calendar aware scheduling using NYSE market hours (no weekends/holidays).
- Incremental dbt models for technical indicators; market breadth is materialized as a table.
- Ingestion checkpoints in Snowflake (`ADMIN.INGESTION_CHECKPOINTS`) for restartability.
- Analytics‑ready marts for security‑level and market‑level analysis.
- Streamlit dashboards for market breadth, universe screening, and ticker momentum.

## Project Structure

```text
stock_market_data_pipeline/
├── airflow/
│   ├── dags/
│   │   └── daily_stock_pipeline_dag.py   # Airflow DAG: Polygon → Snowflake → dbt
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
│       │   └── marts/                    # Analytics‑ready fact/dimension tables
│       ├── macros/                       # Reusable SQL macros (SMA, returns, etc.)
│       ├── seeds/                        # Russell 3000 constituent snapshots
│       └── tests/                        # Data quality tests
├── src/
│   ├── config.py                         # Config loader (Airflow Variables / .env)
│   ├── extraction.py                     # Polygon API interface (grouped daily)
│   ├── load.py                           # Normalize + load data into Snowflake
│   ├── extract_load_stocks.py            # Main ETL orchestration logic
│   └── snowflake_client.py               # Snowflake connection + tables + checkpoints
├── data-viz/
│   ├── streamlit_app.py                  # Streamlit entrypoint
│   ├── pages/                            # Individual dashboard pages
│   │   ├── 1_Market_Breadth.py
│   │   ├── 2_Universe_Screener.py
│   │   └── 3_Ticker_Momentum.py
│   └── utilities/
│       └── snowflake_helper.py           # Helper for querying Snowflake from Streamlit
├── docker-compose.yaml                   # Airflow + Postgres + custom image
├── requirements.txt                      # Python dependencies for Airflow image
└── .env.example                          # Example environment configuration
```

## Data Flow

### 1. Ingestion: Polygon → Snowflake

- `src/extraction.py` fetches grouped daily aggregate data from Polygon:
  - Endpoint: `GET {API_BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{date}`
  - Parameters: `adjusted=true`, `apiKey=${POLYGON_API_KEY}`
  - Includes basic retry handling for rate limits and transient errors.
- `src/load.py` normalizes the response into a consistent schema:
  - Renames Polygon fields (`t`, `v`, `o`, `c`, `h`, `l`, `n`) to Snowflake columns.
  - Adds a `DATE` column (trading date) and `INGESTED_AT` timestamp.
  - Enforces type consistency for timestamps and numeric fields.
- `src/snowflake_client.py`:
  - Establishes a Snowflake connection using RSA private‑key auth.
  - Ensures the `RAW.DAILY_STOCKS` table and `ADMIN.INGESTION_CHECKPOINTS` table exist.
  - Writes pandas DataFrames into Snowflake using `write_pandas`.
  - Records ingestion checkpoints for each trading date (status, row count, timestamps).

### 2. Orchestration: Airflow DAG

- DAG definition: `airflow/dags/daily_stock_pipeline_dag.py`
- `dag_id`: `market_data_pipeline`
- Schedule: `0 12 * * 1-5` (Mon–Fri at noon ET; last completed trading day data).
- Steps:
  1. **Extract & Load**  
     `extract()` task calls `src.extract_load_stocks.extract_load_data(days_back_override=1)` to:
        - Determine valid NYSE trading days using `pandas-market-calendars`, with daily runs targeting the last completed trading day.
     - Skip dates already marked as `completed` in `ADMIN.INGESTION_CHECKPOINTS`.
     - Fetch Polygon data and load into `RAW.DAILY_STOCKS`.
  2. **Transform**  
     Shell tasks run dbt models layer‑by‑layer:
     - `dbt run --select staging`
     - `dbt run --select intermediate`
     - `dbt run --select marts`
  3. **Test**  
     - `dbt test` for model‑level and custom tests.

The DAG enforces strict ordering: Extract → Staging → Intermediate → Marts → Tests.

### 3. Transformation: dbt on Snowflake

The dbt project (`dbt/stock_analytics`) uses Snowflake as its target:

- Seeds (`raw_seeds` schema) hold Russell 3000 constituent snapshots at multiple dates.
- Staging models (`raw_staging` schema) clean and standardize raw Snowflake tables.
- Intermediate models (`raw_intermediate` schema) apply business logic and enrichments.
- Marts (`raw_marts` schema) expose analytics‑ready datasets.

#### Staging Layer

- `stg_daily_stocks`  
  - Source: Snowflake table `RAW.DAILY_STOCKS`.  
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
  - Carries forward sector/company metadata and index weights.

#### Analytics Marts

![Marts schema](assets/marts_schema.png)

- `fct_trading_momentum` (incremental fact table)  
  Daily trading signals and technical indicators:
  - Simple Moving Averages: 20, 50, 200‑day (`sma_20`, `sma_50`, `sma_200`)
  - Relative Strength Index (RSI, 14‑day)
  - Golden/Death cross signals
  - 52‑week highs/lows
  - Relative volume vs 20‑day average

- `agg_daily_market_breadth` (aggregated table)  
  Market‑wide health indicators across the Russell 3000:
  - Advances, declines, unchanged counts and volumes
  - Advance/Decline ratios and cumulative A/D line
  - Percentage of stocks above key moving averages (20/50/200‑day)
  - 52‑week highs/lows counts and a high/low index
  - Aggregate market RSI and simple momentum classification (overbought/oversold/normal)

- `dim_securities_current` (dimension table)  
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
- Market breadth dashboard highlighted at the top of this README.
- Streamlit entrypoint preview:

![Streamlit home dashboard](assets/streamlit_app.png)
- Example pages:
  - `streamlit_app.py`: Home page with the latest market breadth snapshot.
  - `1_Market_Breadth.py`: Market breadth trends and key signals.
  - `2_Universe_Screener.py`: Filterable Russell 3000 snapshot from `dim_securities_current`.
  - `3_Ticker_Momentum.py`: Ticker‑level momentum and signal history from `fct_trading_momentum`.

<details>
  <summary>More dashboard views</summary>

  ![Universe screener](assets/universe_screener.png)
  ![Ticker momentum (view 1)](assets/ticker_momentum_1.png)
  ![Ticker momentum (view 2)](assets/ticker_momentum_2.png)
</details>

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Snowflake account with:
  - Database (e.g., `MARKET`)
  - Warehouse (e.g., `COMPUTE_WH`)
  - Role with privileges to create schemas/tables
- Polygon.io API key
- Python 3 (for local runs) – optional but useful

### 1. Clone the repository

```bash
git clone https://github.com/your-username/stock_market_data_pipeline.git
cd stock_market_data_pipeline
```

### 2. Configure environment variables

Copy the example env file and edit it:

```bash
cp .env.example .env
```

Required values:

- Polygon:
  - `POLYGON_API_KEY`
  - `API_BASE_URL` (e.g., `https://api.polygon.io`)
- Snowflake:
  - `SNOWFLAKE_ACCOUNT`
  - `SNOWFLAKE_USER`
  - `SNOWFLAKE_ROLE`
  - `SNOWFLAKE_WAREHOUSE`
  - `SNOWFLAKE_DATABASE`
  - `SNOWFLAKE_SCHEMA`
  - `PRIVATE_KEY_PATH` (path to PEM private key in the Airflow container)
- Optional:
  - `PYTHONPATH=src`
  - `DBT_PROFILES_DIR=dbt/stock_analytics`

### 3. Configure Snowflake RSA key authentication

1. Generate an RSA private key and corresponding public key.
2. Upload the public key to your Snowflake user (per Snowflake docs).
3. Place the private key file on your host and ensure `PRIVATE_KEY_PATH` in `.env` matches where it will be mounted in the Airflow container (e.g., `/opt/airflow/keys/snowflake_key.pem`).

`src/snowflake_client.py` and `dbt/stock_analytics/profiles.yml` both rely on this key for authentication.

### 4. Start Airflow + Postgres

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

### 5. Initialize dbt

Inside the Airflow web container or your local environment:

```bash
cd dbt/stock_analytics
dbt deps
dbt seed --profiles-dir .
```

This loads the Russell 3000 constituent snapshots into Snowflake (`raw_seeds` schema).

### 6. Run a historical backfill (optional)

For an initial load spanning multiple years:

- Option A: Local Python (if you have the same env vars available):

  ```bash
  python -m src.extract_load_stocks  # default extracts ~2 years (configurable in script)
  ```

- Option B: Trigger via Airflow:
  - In the Airflow UI, trigger the `market_data_pipeline` DAG.
  - Temporarily configure `extract_load_data` to use a larger lookback (e.g., `years_back=2`).

The ingestion checkpoints in `ADMIN.INGESTION_CHECKPOINTS` ensure that re‑runs skip already completed dates.

### 7. Enable daily pipeline

In the Airflow UI:

- Unpause `market_data_pipeline`.
- It will run Monday–Friday at 12:00 ET and ingest the previous trading day’s data, run dbt models, and execute tests.

### 8. Run the Streamlit app

From `data-viz/` (local environment):

1. Create `.streamlit/secrets.toml` with your Snowflake connection info, for example:

   ```toml
   [snowflake]
   account = "your_account"
   user = "your_user"
   role = "SYSADMIN"
   warehouse = "COMPUTE_WH"
   database = "MARKET"
   schema = "RAW_MARTS"
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
  - `started`, `completed`, or `failed`
  - Total tickers and rows inserted
  - Error messages (if any)
- `src.extract_load_stocks.get_completed_dates()` uses this table to avoid duplicate loads.

## Example Snowflake Queries

Use these in the Snowflake UI or via `snowflake_helper.py`:

```sql
-- Most recent golden crosses
SELECT ticker, company, sector
FROM MARKET.RAW_MARTS.FCT_TRADING_MOMENTUM
WHERE trade_date = (
    SELECT MAX(trade_date) FROM MARKET.RAW_MARTS.FCT_TRADING_MOMENTUM
)
  AND golden_cross = 1;
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
FROM MARKET.RAW_MARTS.AGG_DAILY_MARKET_BREADTH
ORDER BY trade_date DESC
LIMIT 30;
```

```sql
-- Top performers by sector in the latest snapshot
SELECT 
  sector,
  ticker,
  latest_close,
  return_1m,
  performance_percentile
FROM MARKET.RAW_MARTS.DIM_SECURITIES_CURRENT
WHERE performance_percentile > 0.9
ORDER BY sector, return_1m DESC;
```

## Known Limitations / Future Work

- **Corporate actions:**  
  Polygon’s `adjusted=true` flag ensures splits/dividends are reflected for the requested date, but the pipeline does not currently re‑ingest historical data when new corporate actions are applied retroactively. This can temporarily distort indicators around split dates until history is refreshed.

- **Universe coverage:**  
  Focused on Russell 3000 constituents via seeded snapshots. Additional universes (e.g., sector ETFs, custom watchlists) would require new seeds and joins.

- **Cost awareness:**  
  Snowflake and Polygon usage can incur costs at scale. Query patterns and warehouse sizing should be tuned for your environment.

## License

This project is for educational and portfolio purposes. Adapt configuration, credentials, and resource sizing before using in any production environment.
