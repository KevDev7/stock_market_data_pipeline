# Separate reference RAW tables

The active landing tables are `MARKET.RAW.DAILY_STOCKS_RAW`,
`MARKET.RAW.TICKER_CATALOG_RAW`, and `MARKET.RAW.COMPANY_OVERVIEW_RAW`.
Each preserves its endpoint's records. Staging cleans each dataset separately;
Intermediate joins the catalog and company overview. The shared
`ADMIN.REFERENCE_MANIFEST` still tracks both reference endpoints by source/date.

S3 filenames, stages, source labels, API requests, and the five landing columns
are unchanged. Ingestion setup owns the tables; Terraform does not manage them.

The live split is complete. See the [verification record](raw-reference-split-results.json)
for record counts, unchanged downstream fingerprints, tests, and access checks.

## Existing environment

Keep ingestion and refresh writers stopped and the DAG paused. Use the existing
table-owner role, without transferring ownership. Review code/tests and run dbt
parsing before executing the migration. A fresh environment uses normal setup,
not this migration.

Preview, without modifying Snowflake or creating a journal:

```bash
python -m scripts.migrate_reference_raw_tables
```

Execute each phase separately, inspecting its result before the next:

```bash
python -m scripts.migrate_reference_raw_tables --execute --idle-confirmed --phase prepare
python -m scripts.migrate_reference_raw_tables --execute --idle-confirmed --phase populate
python -m scripts.migrate_reference_raw_tables --execute --idle-confirmed --phase cutover
```

`prepare` captures the original records, access, view definitions, protected
relation fingerprints, and dashboard access. It creates empty destination tables
using `LIKE ... COPY GRANTS`. `populate` copies both source subsets in one
transaction and compares every column and duplicate count. `cutover` recreates
only the two active Staging views from their checked-in SQL, preserving grants.
It does not run Intermediate or Marts models.

Run the targeted dbt tests without storing failed rows:

```bash
DBT_STORE_FAILURES=false python -m scripts.run_reference_models test --schema-prefix '' \
  --select stg_market__catalog stg_market__issuer_observations stg_market__catalog__complete_manifest source:raw_reference
```

After tests and access checks pass:

```bash
python -m scripts.migrate_reference_raw_tables --execute --idle-confirmed --phase finalize
python -m scripts.migrate_reference_raw_tables --execute --idle-confirmed --phase verify
```

The private journal is `.cache/raw-reference-split/migration.json` (mode 0600,
Git-ignored). Preserve it for reruns and recovery. DDL is not one atomic
transaction: the journal and destination comments identify migration-owned
objects, and every resumed phase checks their actual state. A conflicting table,
unexpected source label, changed original data, or altered grants stops execution.
Never delete or overwrite a destination to bypass a failure.

`finalize` marks the original `RAW.REFERENCE_RAW` as a frozen recovery copy in its
comment. Four historical backup views still read it. It is retained under its
original name, with no new application writes; it is not a fourth active source.
Setup refuses an unmarked legacy table or missing/malformed destinations.
Do not rerun earlier branding/archive migrations against this retired table as
though it were the active landing destination.

The local Compose runtime mounts `src`, `scripts`, and `dbt` from this checkout.
Updated code is ready for a later authorized restart; this migration does not
start Docker, unpause the DAG, or activate ingestion.

## Recovery

Before any new ingestion, keep writers stopped and use the journal to restore
the two original Staging view definitions with `COPY GRANTS`, confirm their
saved privileges and baseline results, and restore matching pre-split loader,
setup, and dbt source code. Restore the original shared-table comment from the
journal. Preserve the new tables and journal for diagnosis; no automatic rollback
or table deletion is performed. A later retry must revalidate all phases.

After ingestion has resumed, the shared recovery table is stale. Reconcile the
new endpoint tables into a reviewed recovery target before rolling back; simply
switching readers back would discard access to newer records.

The migration verification phases are intended for the stopped-writer cutover.
After ingestion resumes, divergence from the frozen copy is expected; do not
overwrite new records to make the old migration baseline match.
