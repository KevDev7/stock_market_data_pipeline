# Operations, Backfills, and Testing

## Scheduled Run

The Airflow DAG `market_data_pipeline` runs Monday through Friday
at noon Eastern. It targets the latest completed NYSE trading date and runs:

```text
ingest_raw_stock_data -> run_dbt_staging -> run_dbt_intermediate
    -> run_dbt_mart_staging -> run_dbt_marts -> run_dbt_tests
```

Source ingestion is currently paused because the paid provider subscription is
inactive. To resume it, configure a valid Massive.com/Polygon.io API key and
unpause the DAG in Airflow.

## Checkpoints and Retries

`ADMIN.INGESTION_CHECKPOINTS` records each date's status:

- `started`: ingestion began
- `archived`: the S3 object was written and verified
- `completed`: Snowflake RAW loading succeeded
- `failed`: an error occurred, with its message saved

Rows also store source counts, inserted counts, S3 bucket and key, ETag, and
SHA-256 checksum. Completed dates are skipped by later runs.

## Historical S3 Reconstruction

The historical Snowflake raw table can reconstruct the S3 archive without
calling the provider API. Preview the dates and row counts first:

```bash
docker compose run --rm --entrypoint python airflow-scheduler \
  /opt/airflow/scripts/backfill_raw_to_s3.py --dry-run
```

Then run the restartable backfill:

```bash
docker compose run --rm --entrypoint python airflow-scheduler \
  /opt/airflow/scripts/backfill_raw_to_s3.py
```

Optional `--start-date` and `--end-date` arguments limit the date range.
Existing objects with matching row counts are skipped. Each newly archived
object is read back and checksum-verified. The backfill keeps the rows and
operational metadata but cannot recreate the original HTTP response metadata.

## Unit Tests

The ingestion and S3 tests use Python's standard `unittest` runner:

```bash
python -m unittest discover -s tests -v
```

They cover archive-before-load ordering, failed-load checkpoint behavior,
deterministic gzip output, checksum rejection, and restartable object metadata.

## dbt Validation

From `dbt/stock_analytics`:

```bash
dbt parse --profiles-dir .
dbt test --profiles-dir .
```

Tests include:

- uniqueness and not-null checks at every declared grain
- foreign-key relationships to conformed dimensions
- SCD Type 2 non-overlapping windows and one current row per ticker
- RSI range validation
- golden and death crosses cannot both be true
- 52-week high/low consistency
- yesterday-close reconciliation
- market and sector breadth totals
- relative freshness across key marts

Freshness checks compare related model dates; they do not require the historical
snapshot to be current with today's date.

## Useful Runtime Checks

```bash
docker compose config --quiet
docker compose ps
docker compose logs --tail=100 airflow-scheduler
```

The raw S3 archive and `RAW.DAILY_STOCKS_RAW` serve different purposes: S3 is
the durable replay source, while Snowflake RAW is the queryable warehouse
landing layer used by dbt.
