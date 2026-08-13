# Stock Market Data Analytics Pipeline

A production-style batch ELT pipeline for Russell 3000 market intelligence.
Daily OHLCV data flows from Polygon.io (now Massive.com) through an Amazon S3
raw archive into Snowflake, where dbt builds tested dimensional marts consumed
by a hosted Streamlit dashboard.

**[Open the live Russell 3000 Market Intelligence dashboard](https://russell3000-market-intelligence.streamlit.app/)**

> The dashboard is a retained historical snapshot because paid source ingestion
> is currently paused. The pipeline remains fully implemented and restartable.

<p align="center">
  <img src="assets/streamlit_app.png" width="100%" alt="Russell 3000 Market Intelligence dashboard">
</p>

## Architecture

<p align="center">
  <img src="assets/StockMarketELT-Arch.png" width="100%" alt="Stock market ELT architecture from the provider API through S3, Snowflake, dbt, and Streamlit">
</p>

The diagram groups `STAGING`, `INTERMEDIATE`, and `MART_STAGING` into one
transformation station for readability. They remain separate schemas in the
`MARKET` Snowflake database.

| Concern | Technology | Role |
|---|---|---|
| Source | Polygon.io / Massive.com | Grouped daily U.S. stock aggregates |
| Raw archive | Amazon S3 | Versioned gzip NDJSON partitioned by API date and run ID |
| Warehouse | Snowflake | Raw landing, transformation compute, and dimensional marts |
| Transformation | dbt Core | SQL models, incremental loads, SCD Type 2, and tests |
| Orchestration | Apache Airflow | Weekday ingestion, ordered dbt builds, and quality checks |
| Local runtime | Docker Compose | Reproducible Airflow and PostgreSQL environment |
| Analytics | Streamlit | Hosted market, sector, security, and momentum dashboards |

AWS IAM scopes the ingestion writer to the project S3 prefix and gives
Snowflake read-only access through an assumed role. Snowflake and Streamlit use
RSA key-pair authentication.

## What It Demonstrates

- **Replayable ingestion:** source rows are archived in S3 before Snowflake
  loading, with checksums and object metadata retained for auditability.
- **Idempotent daily loads:** one `API_DATE` partition is atomically replaced,
  while `ADMIN.INGESTION_CHECKPOINTS` supports retries and restartability.
- **Layered ELT:** `RAW -> STAGING -> INTERMEDIATE -> MART_STAGING -> MARTS`,
  with business transformations executed by dbt inside Snowflake.
- **Incremental processing:** security-day models use date lookbacks and dbt
  `MERGE` logic to safely recalculate rolling indicators.
- **Dimensional modeling:** conformed date, security, and sector dimensions are
  shared by a small fact constellation.
- **Historical dimensions:** `DIM_SECURITY_HISTORY` implements SCD Type 2 using
  validity windows, while `DIM_SECURITY` exposes current attributes.
- **Data quality:** schema tests and custom SQL tests validate keys, ranges,
  freshness, technical indicators, and breadth reconciliations.
- **Analytics delivery:** four Streamlit views expose market breadth, sector
  breadth, universe screening, and ticker momentum.

## Dimensional Marts

<p align="center">
  <img src="assets/StockMarketELT_Model.png" width="100%" alt="Snowflake marts dimensional model with shared date, security, sector, and security history dimensions">
</p>

The primary `FCT_SECURITY_DAILY_MOMENTUM` star shares conformed dimensions with
market-day, sector-day, and current-snapshot facts, making the marts both a star
schema and a small fact constellation.

| Model | Grain |
|---|---|
| `FCT_SECURITY_DAILY_MOMENTUM` | Security x trading date |
| `FCT_MARKET_DAILY_BREADTH` | Trading date |
| `FCT_SECTOR_DAILY_BREADTH` | Sector x trading date |
| `FCT_SECURITY_CURRENT_SNAPSHOT` | Security |
| `DIM_DATE` | Trading date |
| `DIM_SECURITY` | Ticker |
| `DIM_SECURITY_HISTORY` | Ticker x validity period |
| `DIM_SECTOR` | Sector |

## Pipeline Run

The Airflow DAG `market_data_pipeline` is designed for weekdays at noon Eastern
(`0 12 * * 1-5`) and contains six ordered tasks:

```text
Extract + S3 archive + Snowflake RAW load
  -> dbt STAGING
  -> dbt INTERMEDIATE
  -> dbt MART_STAGING
  -> dbt MARTS
  -> dbt tests
```

NYSE calendar logic selects the latest completed trading day and avoids
weekends and exchange holidays. Russell 3000 constituent CSV seeds supply
point-in-time membership and descriptive attributes.

## Reliability

- S3 objects use deterministic gzip encoding, server-side encryption, and
  SHA-256 verification.
- Checkpoints record `started`, `archived`, `completed`, or `failed` status,
  row counts, S3 location, ETag, checksum, and errors.
- dbt tests cover uniqueness, referential integrity, RSI bounds, crossover
  exclusivity, 52-week price consistency, freshness, and breadth totals.
- Historical S3 reconstruction is restartable and can be previewed with a dry
  run without calling the provider API.

## Quick Start

Prerequisites: Docker Compose, Snowflake, AWS credentials for the scoped
ingestion identity, an RSA private key, and a Massive.com/Polygon.io API key if
source ingestion will be resumed.

```bash
git clone https://github.com/KevDev7/stock_market_data_pipeline.git
cd stock_market_data_pipeline
cp .env.example .env
docker compose up -d
```

Airflow is available at `http://localhost:8080`. See the setup guide before
provisioning a new S3/Snowflake integration or enabling the DAG.

## Documentation

- [Architecture and data model](docs/architecture.md)
- [Environment and installation](docs/setup.md)
- [Operations, backfills, and testing](docs/operations.md)
- [Example Snowflake queries](examples/queries.sql)
- [Environment variable template](.env.example)

## Limitations

- Source ingestion is paused, so the hosted dashboard represents a historical
  portfolio snapshot rather than a real-time service.
- The universe is limited to the retained Russell 3000 constituent snapshots.
- Adjusted aggregates are requested, but history is not automatically reloaded
  when a provider applies retroactive corporate-action changes.
- This is an educational portfolio deployment, not a production SLA-backed
  service; Snowflake and market-data usage can incur costs when resumed.

## Vendor Naming

Polygon.io rebranded as [Massive.com](https://massive.com/blog/polygon-is-now-massive)
on October 30, 2025. Historical metadata, environment variables, S3 paths, and
the supported `api.polygon.io` endpoint retain their original names because the
retained data was ingested under the Polygon.io brand.

## License

This project is for educational and portfolio purposes.
