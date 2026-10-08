# Operations, Backfills, and Testing

## Scheduled Run

The Airflow DAG `market_data_pipeline` runs Monday through Friday
at noon Eastern. It targets the latest completed NYSE trading date and runs:

```text
extract -> extract_reference -> run_dbt_staging -> run_dbt_intermediate
    -> run_dbt_marts -> run_dbt_tests
```

The selected analytical dataset is a fixed 2024–2025 study. ANALYSIS_START,
ANALYSIS_END and WARMUP_START provide one shared window for all DAG dbt tasks
and reference ingestion; the defaults are the selected study dates. The paid Stocks
Starter subscription is active for this migration; the existing API key works.
Do not unpause the DAG merely to complete this historical migration. Extending
the reporting period requires new price/reference coverage and explicit dbt vars.
Runs are serialized to prevent overlapping raw-date replacement transactions.
Airflow stores its reference cache in the mounted logs/.cache directory.

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

For local SQL regression tests, install the test-only dependencies as well:

```bash
python -m pip install -r requirements-test.txt
python -m unittest discover -s tests -v
```

Tests cover archive-before-load ordering, failed-load checkpoint behavior,
deterministic gzip output, checksum rejection, and restartable object metadata.
They also render the actual dbt model SQL and execute translated queries in an
isolated DuckDB database: source duplicates, daily eligibility gaps, ticker reuse,
industry-observation boundaries, rejected-price history, sparse rolling
warmup, incremental/full-build equivalence, and removal of stale rows even when
an incremental replacement has no incoming rows. No warehouse credentials are
used. These fixtures do not replace Snowflake integration verification.

## dbt Validation

From `dbt/stock_analytics`:

```bash
dbt parse --profiles-dir .
dbt test --profiles-dir .
```

Tests include:

- uniqueness and not-null checks at every declared grain
- foreign-key relationships to conformed dimensions
- SCD Type 2 non-overlapping windows and one current row per security identity
- RSI range validation
- golden and death crosses cannot both be true
- 52-week high/low consistency
- yesterday-close reconciliation
- market and sector breadth totals
- relative freshness across key marts

Key uniqueness and accepted-history tests cover the stored dataset, including
historical snapshots. Freshness checks compare related model dates; they do not
require the historical snapshot to be current with today's date.

## Reference Migration

Completed on 2026-10-07: all 18 candidate models built, 126 tests passed both
before and after cutover, and all five local dashboard views rendered against
both schema sets. [Verified counts and recovery artifacts](reference-migration-results.json)
record the outcome. The commands below document the one-time procedure, not an
instruction to repeat the dated cutover. Subsequent isolated Docker checks passed
dependency validation, DAG import, seven-task ordering, and task-module imports.
No ingestion task or scheduler was started during those Docker checks. A separate
hosted dashboard deployment was subsequently verified as described below.

Run from the repository root using the configured project credentials:

```bash
python -m scripts.backfill_reference
python -m scripts.refresh_adjusted_prices --execute
python -m scripts.run_reference_models build --full-refresh
python -m scripts.verify_reference_migration
python -m scripts.cutover_reference_models
python -m scripts.cutover_reference_models --execute
```

The backfill covers 2022-12-29 through 2025-12-31: daily catalogs, missing price
dates, and issuer overviews at the initial reporting date and quarter ends, plus
first-observed issuers. Batches archive source JSON in S3 before COPY; exact
source/date counts must match before a transaction replaces reference rows.
The local SQLite cache is Git-ignored and keeps successful paid API requests for
restarts. Bounded concurrency and retries do not bypass provider rate limits.

The price refresh is necessary because later stock splits changed historical
adjusted prices. It first creates RAW.DAILY_STOCKS_RAW_BEFORE_REFERENCE_20261007,
then refreshes only retained overlap dates from 2023-11-20 through 2025-12-31.
Original S3 run objects are never deleted. PRICE_REFRESH_MANIFEST commits per-date
progress with the raw replacement, making retries skip completed refresh dates.
Previously retained January 2026 prices remain outside the analytical window.

Candidate models are built into CANDIDATE_STAGING, CANDIDATE_INTERMEDIATE,
and CANDIDATE_MARTS. RAW is unchanged. Completeness checks require every
expected price/catalog date, all nine quarterly/boundary observations, every
price refresh, exactly the reporting calendar in the marts, and complete SCD
coverage of fact rows. Passing tests on a partial dataset is not enough.

Only after a full build and verification may the candidate schemas replace the
active schemas. Preserve current grants and retain the old schemas under dated
BACKUP_ names. The MARTS swap switches all consumer tables together. This is a
warehouse cutover, not proof that the hosted Streamlit code has been deployed.
Legacy CSV files and SEEDS tables remain inactive provenance; they are not deleted.

After cutover, verify the active schemas with:

```bash
python -m scripts.verify_reference_migration --schema-prefix ''
python -m scripts.run_reference_models test --schema-prefix ''
```

Using the local dashboard dependencies and data-viz/.streamlit/secrets.toml,
run the read-only five-page application smoke test. It also exercises AAPL's
identity-selected momentum chart and requires charts or data tables to render:

```bash
python -m scripts.smoke_dashboard
python -m scripts.smoke_dashboard --mart-schema MARTS
```

The default now targets active MARTS. To check candidates explicitly, pass
`--mart-schema CANDIDATE_MARTS` before cutover. An optional in-memory role override
tests reader permissions without editing saved secrets:

```bash
python -m scripts.smoke_dashboard --role STREAMLIT_DASHBOARD_ROLE
```

The locally configured dashboard user currently lacks this role, so that check
is blocked; do not treat an ACCOUNTADMIN smoke test as reader-role verification.
Neither command deploys hosted application code or changes credentials.

## Pre-Push Integration Findings

[Pre-push evidence](prepush-verification-results.json) records the original
Snowflake mismatch and its verified fix. Floating moving averages caused
three bullish-crossover flags to change on an unchanged-source incremental run.
SMA20/SMA50/SMA200 now accumulate in fixed-point NUMBER(38,18), then publish
DOUBLE values as before. The crossover definitions and column contract are
unchanged; no tolerance threshold was added to the business rules.

Two unchanged-source retries passed, as did price corrections, removals, a new
security, another retry, and an empty recent-source window. Full/incremental
comparisons now require moving averages and every signal flag to match exactly.
After discarding fixtures and restoring unmodified source clones, all 18 models
and 126 warehouse tests passed, with identical mart column names/types/order.
All eight mart row counts are unchanged and all five dashboard views rendered
against these corrected isolated marts using the existing local ACCOUNTADMIN
credentials, not the still-unverified reader role.
The authorized full rebuild was subsequently applied to the live warehouse
on 2026-10-07; [deployment evidence](sma-fix-rebuild-results.json) records all
18 rebuilt models, 126 passing tests both before and after cutover, and five
passing dashboard views against each schema set. Do not assume an
incremental-only deployment repairs historical signals. The isolated tests did
not modify live data; the subsequent deployment updated only derived schemas.
RAW and S3 remain unchanged.
Across the reporting window, the corrected rebuild differs from the retained pre-fix data
in 236 bullish-crossover, four golden-cross, and three death-cross flag values.

The real replacement macro's rollback check passed: after DELETE removed 15,490
rows in an isolated test table, an intentional INSERT failure left its row count
and aggregate hash unchanged. All 42 local tests passed, including a new
flat-price/equality regression that failed before the fix and passed afterward.
The local dashboard reader-role check remains blocked as described above.

The integration harness creates an ownership-marked, uniquely named test
database and refuses to reuse or remove other databases:

```bash
python -m scripts.verify_incremental_snowflake --full-baseline
python -m scripts.verify_incremental_snowflake --database PREPUSH_TEST_<12HEX> --scenario corrections
python -m scripts.verify_incremental_snowflake --database PREPUSH_TEST_<12HEX> --scenario empty
python -m scripts.verify_incremental_snowflake --database PREPUSH_TEST_<12HEX> --scenario build
python -m scripts.verify_incremental_snowflake --database PREPUSH_TEST_<12HEX> --scenario rollback
python -m scripts.smoke_dashboard --database PREPUSH_TEST_<12HEX> --mart-schema MARTS
python -m scripts.verify_incremental_snowflake --database PREPUSH_TEST_<12HEX> --scenario cleanup
```

Replace the placeholder with the exact name printed by the first command.
The build scenario restores only that owned database's test source tables from
MARKET clones, discarding injected fixtures before validation. Capture evidence
before cleanup; cleanup never targets MARKET or its backups. Dashboard overrides
are in-memory only; no saved secrets or hosted deployment are changed.

## Applied Moving-Average Rebuild

The 2026-10-07 numerical fix was fully rebuilt into fresh
`SMA_FIX_20261007_` candidate schemas, validated, and switched into the four
active layer names. Existing schema, future-table/view, and object grants were
mirrored before cutover and verified afterward. All eight mart row counts and
column definitions remain unchanged. Old schemas are retained under:

```text
MARKET.BACKUP_STAGING_SMA_FIX_20261007
MARKET.BACKUP_INTERMEDIATE_SMA_FIX_20261007
MARKET.BACKUP_MART_STAGING_SMA_FIX_20261007
MARKET.BACKUP_MARTS_SMA_FIX_20261007
```

The earlier reference-migration backups are also preserved. No ingestion was
resumed and no additional API data was requested. The warehouse rebuild itself
did not deploy dashboard code. A separate dashboard-only commit, `d97f010`, was
subsequently pushed to the app's configured `codex/warehouse-marts-scd2` branch,
and Community Cloud's Reboot action reloaded the app and its in-memory cache.
All five hosted views were verified, including AAPL identity selection. Remote
`main` was not updated. See [hosted deployment evidence](hosted-dashboard-deployment-results.json).
Saved credentials and role assignments were not changed. A subsequent
[hosted access check](hosted-dashboard-access-results.json) confirmed the service
user is configured with `STREAMLIT_DASHBOARD_ROLE` and all 27 recent hosted
queries used that role successfully. Its project permissions are read-only:
database/schema/warehouse USAGE and SELECT on the eight active marts, with no
write, ownership, create, or grant-option permissions on MARKET. The earlier
local role override used a different user and is not a hosted access failure.
This is not a claim of the strict minimum possible privileges: read grants on
the retained mart backups remain, and shared PUBLIC capabilities include
Snowflake AI/compute features. PUBLIC inheritance adds no MARKET access.
Removing backup access or hardening account-wide PUBLIC grants is a separate
decision; this check did not change permissions.

The deployment used `scripts.cutover_reference_models` with
`--candidate-prefix SMA_FIX_20261007_`, `--backup-suffix SMA_FIX_20261007`, and
`--artifacts-path .cache/sma-fix-deploy/target`. These are completed deployment
identifiers, not instructions to rerun the same cutover. The helper rejects
existing backup names and mismatched or incomplete build artifacts.

If rollback is required, first stop warehouse writers and explicitly authorize
it. Swap the four active schemas with these pre-fix backups, switching MARTS
last so consumers keep one complete table set until the final switch; then
rerun completeness checks, dbt tests, and dashboard smoke checks. Do not drop
either set merely to undo the deployment.

## Four-Layer Consolidation

Applied on 2026-10-08: `RAW -> STAGING -> INTERMEDIATE -> MARTS`.
INTERMEDIATE retains shared eligibility, identity, enrichment, and record/history
preparation. MARTS owns analytical calculations and dimensional products.
`MARTS.CALC_SECURITY_DAILY_MOMENTUM` is an internal shared calculation table;
the eight published DIM_/FCT_ tables are unchanged. The other three preparation
models were folded into their corresponding fact models. Airflow now has six
tasks and no MART_STAGING task.

The complete candidate build passed with 15 models and 109 tests in 183.49
seconds. The smaller test count reflects removal of duplicate preparation-model
checks; published fact checks and shared-calculation checks remain. All 109
tests passed again after cutover, and all five local dashboard pages rendered.
Unchanged-source retries, corrections, removals, a new security, empty-window
replacement, and rollback were verified in an isolated database. See
[verification evidence](four-layer-refactor-results.json).

No API backfill, S3 change, RAW rewrite, credential change, or hosted code
deployment was needed. Rebuilding and testing derived models uses Snowflake
compute; elapsed build time is not a measurement of total billed credits.
Ingestion remains paused. The owned temporary test databases were removed.

The cutover preserved schema, future-table/view, and existing object grants and
kept the old schemas as:

```text
MARKET.BACKUP_STAGING_FOUR_LAYER_20261008
MARKET.BACKUP_INTERMEDIATE_FOUR_LAYER_20261008
MARKET.BACKUP_MARTS_FOUR_LAYER_20261008
MARKET.BACKUP_MART_STAGING_FOUR_LAYER_20261008
```

The completed deployment used:

```bash
python -m scripts.run_reference_models build --full-refresh \
  --schema-prefix FOUR_LAYER_20261008_ \
  --target-path .cache/four-layer/target --log-path .cache/four-layer/logs
python -m scripts.cutover_reference_models \
  --candidate-prefix FOUR_LAYER_20261008_ \
  --backup-suffix FOUR_LAYER_20261008 \
  --artifacts-path dbt/stock_analytics/.cache/four-layer/target \
  --require-identical-marts --retire-mart-staging --execute
```

These dated identifiers record the completed operation; do not rerun the same
cutover. The runner resolves relative target/log paths within the dbt project.
To check the current deployment without rebuilding it:

```bash
python -m scripts.run_reference_models test --schema-prefix ''
python -m scripts.verify_four_layer_refactor --candidate-prefix '' \
  --baseline-schema BACKUP_MARTS_FOUR_LAYER_20261008
python -m scripts.smoke_dashboard
```

The comparison requires exact columns, keys, prices, dates, and integer flags.
Derived floating measures allow absolute tolerance 1e-9 or relative tolerance
1e-10 for execution-plan rounding. Three fact tables differ only within that
tolerance; the dimensions and daily fact match aggregate fingerprints exactly.
The local application smoke test uses ACCOUNTADMIN; it does not authenticate as
the hosted service user. Hosted reader grants were checked separately.

Recovery requires stopping writers and explicitly authorizing rollback. Swap
STAGING and INTERMEDIATE with their backups, restore the retired MART_STAGING
schema name, and swap MARTS last. Restore the matching five-layer code before
resuming dbt/Airflow, then rerun completeness, quality, and dashboard checks.
Do not drop either schema set to undo the cutover.

## Corrections and Future Periods

Incremental models atomically replace the recent output window, including rows
that disappeared or became ineligible. The default correction window is four
calendar days before the target's latest date. Older corrections need a larger
lookback or full rebuild. Changed identity/classification history needs a full
rebuild because later windows and SCD versions can change.

To extend the study, fetch the requested prices and daily catalogs, then fetch
issuer overviews with matching --analysis-start and --analysis-end. Use the same
analysis_start, analysis_end and warmup_start vars in every dbt invocation. Fetch
at least 252 prior exchange sessions and refresh overlapping adjusted prices
when retrieval vintages differ. The default 2024–2025 vars intentionally do not
expand the displayed window just because newer raw prices exist.

## Useful Runtime Checks

```bash
docker compose config --quiet
docker compose ps
docker compose logs --tail=100 airflow-scheduler
```

The raw S3 archive and `RAW.DAILY_STOCKS_RAW` serve different purposes: S3 is
the durable replay source, while Snowflake RAW is the queryable warehouse
landing layer used by dbt.

## Logging

The ingestion code uses Python's standard `logging` module with three levels:

- `INFO` for run progress, dates, checkpoints, row counts, and successful loads
- `WARNING` for rate limits, retries, empty responses, and recoverable issues
- `ERROR` for terminal API failures and failed raw loads

Messages include identifiers such as `run_id` and `api_date` without logging API
keys, private keys, or raw payloads. Airflow automatically captures these module
logs for each task attempt and stores them under `airflow/logs/`, which is mounted
from `/opt/airflow/logs` by Docker Compose. Remote Airflow logging is disabled.
dbt writes its own detailed `logs/dbt.log` relative to the directory where dbt
is invoked, while its console output also appears in the corresponding Airflow
task log.
