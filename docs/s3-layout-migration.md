# S3 dataset folders

This records the completed first layout migration. A subsequent
[date-only migration](date-archive-migration.md) removes the run folders shown
below. Do not rerun this historical procedure against the new layout.

The project bucket contains three datasets. The directories describe their
contents, not their refresh schedule or their future warehouse modeling role.

```text
stock-market-data-pipeline-645968351825-us-east-1/
├── daily-stock-prices/
│   └── api_date=<date>/run_id=<run-id>/daily_stocks_raw.ndjson.gz
├── ticker-catalog/
│   └── api_date=<date>/run_id=<run-id>/reference_raw.ndjson.gz
└── company-overview/
    └── api_date=<date>/run_id=<run-id>/reference_raw.ndjson.gz
```

## Ownership and mappings

| Previous prefix | Current prefix | Snowflake external stage |
|---|---|---|
| `raw/polygon/grouped-daily/` | `daily-stock-prices/` | `MARKET.RAW.POLYGON_RAW_STAGE` |
| `raw/polygon/reference/polygon_ticker_catalog/` | `ticker-catalog/` | `MARKET.RAW.POLYGON_TICKER_CATALOG_STAGE` |
| `raw/polygon/reference/polygon_ticker_overview/` | `company-overview/` | `MARKET.RAW.POLYGON_COMPANY_OVERVIEW_STAGE` |

Date directories, run IDs, filenames, compressed bytes, checksums, metadata,
tags, encryption, and operational source labels are preserved. The shared
reference stage is renamed in place for the catalog; Terraform creates one
additional stage for company overviews. No bucket-root access is introduced.
[Snowflake's integration rules](https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration)
require each stage URL to align with an allowed storage location.

Ingestion owns the three ADMIN tracking tables. Only their `S3_KEY` values
change: `INGESTION_CHECKPOINTS`, `REFERENCE_MANIFEST`, and
`PRICE_REFRESH_MANIFEST`. Raw payloads, model definitions, marts, and dashboard
results do not change. Batch reference loading reads both scoped stages into
one temporary table before its existing single atomic data/manifest transaction.
No new source extraction, archive serialization, or analytical rebuild is needed.

## Controlled rollout

1. Keep ingestion, refresh jobs, and Airflow stopped. Verify AWS account,
   bucket versioning, destination conflicts, stage grants/dependencies, and
   representative dashboard reads. Privately preserve the original Terraform
   inputs and `.env` prefix. Do not use administrator credentials for ingestion.
2. Preview with `python -m scripts.migrate_s3_layout preview`. This reads cloud
   resources without saving a migration journal or changing them. A source with
   additional versions/delete markers or a destination with existing version
   history stops for inspection; it is never silently overwritten.
3. Through reviewed saved Terraform plans, temporarily allow the three new
   prefixes alongside the old two prefixes. For Snowflake preparation set
   `manage_overview_stage=false`, keeping existing stage definitions unchanged;
   add the new folders using `additional_reference_prefixes`.
4. Run `python -m scripts.migrate_s3_layout copy --execute --idle-confirmed`.
   The private `.cache/s3-layout/migration.json` pins every source VersionId,
   ETag, checksum and mapping; records warehouse fingerprints and tracking
   columns/grants; and tracks verified copies. Source and destination bytes
   are compared with SHA-256. Existing conflicting targets stop the operation.
5. Stage the updated code while all writers remain stopped. Run the `metadata`
   phase with the same explicit flags. It changes the three tracking tables'
   paths in one transaction and validates normalized fingerprints before commit.
   A rerun after a lost commit response is safe; no raw/model data is written.
6. Apply a saved, narrowly reviewed Terraform cutover plan: point the existing
   price stage to `daily-stock-prices/`, rename/repoint the existing reference
   stage to the catalog, and create the JSON/AUTO overview stage. Keep the old
   locations temporarily allowed. Use `check_s3_layout_plan` with exact expected
   settings; ordinary adoption guards must continue rejecting all mutations.
7. Set the private `.env` price prefix to `daily-stock-prices`. Run the `verify`
   phase. Confirm the three stages can list/read their corresponding datasets,
   both reference prerequisites pass, tracking records point to verified
   objects, full normalized fingerprints match, and dashboard reads still work.
8. Run `hide` with the explicit flags. Each original is reverified before adding
   a versioned delete marker; original versions are never permanently deleted.
   Marker intent/IDs are journaled. Resume reconciles lost responses without
   creating duplicate markers. Unrelated prefixes and versions are untouched.
9. Contract IAM policies and integration locations to exactly the three new
   prefixes. Remove temporary `additional_reference_prefixes`. Require ordinary
   no-change AWS, Snowflake and bootstrap plans. Any computed-only refresh drift
   requires its own inspected refresh-only plan, never broader ignore rules.

Cloud actions use the authorized local administrator profile because the
connected AWS tool uses a different limited identity. Credentials, plans,
private inputs, journals, and logs remain Git-ignored. Ingestion stays stopped.
No dashboard deployment, dbt model rebuild, credential rotation, or diagram
change is part of this path-only migration.

Provider 2.22.0 reported the newly created overview stage's unset
`aws_access_point_arn` as `null` initially and `""` on the next read. The stage
definition used the ordinary bucket URL with no access point configured. This
specific empty-value normalization was inspected and recorded with a saved
refresh-only plan containing no resource actions. It was not hidden with an
ignore rule, treated as a configurable infrastructure update, or added as an
exception to the ordinary final no-change checker. Login/warehouse timestamps
and IAM's read-back of its separately managed inline policy were likewise
inspected before refresh-only state reconciliation.

## Recovery

Do not automatically roll back a partly completed cloud migration. Inspect the
private journal first. The previous paths remain recoverable as versioned files.

1. Stop all writers. Through reviewed Terraform plans temporarily permit both
   layouts. Use the recorded original tfvars/settings, preserving trust identities.
2. Verify each original VersionId and checksum. Remove only this migration's
   recorded delete-marker VersionIds to restore the old current paths. Do not
   remove the earlier provider-naming migration's markers under `raw/massive/`.
3. Transactionally restore each tracking table's `S3_KEY` using the journal's
   exact new-key → old-key mapping. Verify every other column and record against
   the baseline. Do not change payloads, checksums or completion states.
4. Restore the matching code/configuration and `.env`, repoint the price stage,
   reverse the catalog stage rename/URL, and restore old integration/IAM scopes
   through reviewed Terraform plans. Leave the additional overview stage intact
   until its disposal is explicitly reviewed: `prevent_destroy` is intentional.
5. Recheck access, columns/grants, data fingerprints and dashboard reads. Leave
   copied destinations preserved until cleanup is explicitly approved.

The earlier naming-migration journal/verification record and legacy archive
contents intentionally retain their historical paths. They are recovery
evidence, not current deployment defaults.
