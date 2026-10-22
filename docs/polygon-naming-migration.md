# Polygon naming migration

This records the earlier provider-naming migration. Its intermediate archive
paths were subsequently simplified by the [dataset-folder migration](s3-layout-migration.md).
Use the current setup guide for new environments; these historical mappings and
recovery instructions remain accurate for the earlier migration journal.

The existing project was migrated on 2026-10-22. See the
[verification record](polygon-naming-migration-results.json).

Active code, documentation, diagram labels, cache filenames, source identifiers,
and archive paths use Polygon. The API key and configured `api.polygon.io`
connection are unchanged. Both official API hosts remain accepted for pagination.

| Object | Current name |
|---|---|
| Reference client | `PolygonReferenceClient` |
| Catalog source | `polygon_ticker_catalog` |
| Company overview source | `polygon_ticker_overview` |
| Reference stage | `MARKET.RAW.POLYGON_REFERENCE_STAGE` |
| Reference archive root | `raw/polygon/reference/` |
| Local/reference DAG cache | `polygon-reference.sqlite3` |

Daily stock prices keep their existing `raw/polygon/grouped-daily/` path. Reference
archives keep the same date, run ID, filename, compressed bytes, metadata, tags,
and encryption. Their source folders use the corresponding Polygon identifiers.

## Ownership and execution

Keep orchestration and refresh jobs stopped throughout the cutover. Terraform
owns IAM policies, integration locations, and the stage name/URL. The migration
command owns copying/verifying archives and changing operational source labels
and manifest paths. It never changes Terraform-managed infrastructure itself.
Use an existing administrative AWS profile, not the application ingestion user,
for S3 copies and recoverable delete markers.

Preview without cloud writes:

```bash
python -m scripts.migrate_polygon_naming --phase preview --aws-profile EXISTING_ADMIN_PROFILE
```

Execution phases require `--execute --idle-confirmed`. The default private journal
is `.cache/polygon-naming/migration.json`, written atomically with mode 0600. Keep
the journal, full Terraform plans, old private inputs, and recovery records out of Git.

1. Inventory all source versions, destination conflicts, archive/manifest mappings,
   and warehouse fingerprints. Record original view definitions/grants and private
   Terraform inputs. The command rejects additional historical versions that its
   current one-version-per-key inventory does not cover; it does not discard them.
2. Review and apply saved Terraform plans adding the new prefix alongside the old
   one in the existing IAM policies. Set `additional_reference_prefixes` to
   `["raw/polygon/reference"]` while the reference stage still uses its old name/path.
3. Run the `cache` phase to back up existing local caches using SQLite's backup API.
   Logical cache fingerprints must match. Existing different destinations are
   conflicts. Original caches remain available for recovery.
4. Run `copy`. It pins original version IDs, copies without rewriting bytes, and
   verifies SHA-256, metadata, tags, encoding, and encryption. It records verified
   batches and verifies matching destinations on resume without copying again.
5. Run `bridge`. It replaces only the two reference staging views with filters
   accepting both sets of labels and uses `COPY GRANTS`. Results must remain equal.
6. Run `metadata`. One transaction changes only `SOURCE` in `RAW.REFERENCE_RAW`
   and `SOURCE`/`S3_KEY` in `ADMIN.REFERENCE_MANIFEST`. Payloads, timestamps,
   row counts, and archive checksums are preserved. A failed verification rolls
   back that transaction. A lost completion response is safe to retry.
7. Review/apply the Terraform stage rename and URL update in place. Set
   `reference_stage_name="POLYGON_REFERENCE_STAGE"`,
   `reference_prefix="raw/polygon/reference"`, and temporarily retain
   `additional_reference_prefixes=["raw/massive/reference"]`.
8. Run dbt only for `stg_market__catalog` and `stg_market__issuer_observations`,
   with the active schema prefix and grant preservation enabled. Run the relevant
   reference tests with failure-row storage disabled; do not rebuild marts.
9. Run `verify`. It checks normalized raw/manifest fingerprints, unchanged price
   data, operational tables, staging results, mart contents, stage replay access,
   grants, and existing dashboard reads.
10. Run `hide`. It verifies copies again before adding delete markers to old keys.
    Original version IDs remain recoverable. Recorded deletion intent lets a
    restart reconcile a lost response without adding another marker.
11. Review/apply final Terraform plans removing old-prefix access and clearing
    `additional_reference_prefixes`. Require clean final AWS/Snowflake/bootstrap
    plans and confirm no visible objects remain under `raw/massive/`.

Example invocation for an execution phase:

```bash
python -m scripts.migrate_polygon_naming --phase copy --execute --idle-confirmed \
  --aws-profile EXISTING_ADMIN_PROFILE
```

The Terraform plan checker in `scripts/check_polygon_rename_plan.py` permits only
explicitly reviewed in-place field values. It rejects replacements, imports,
unrelated mutations, unknown configurable settings, and unresolved drift. The
ordinary strict adoption checker remains unchanged. Audited computed-only drift
may be reconciled separately using a saved refresh-only plan.

## Recovery

Do not run automatic rollback across S3, Snowflake, and Terraform. Inspect the
private journal and actual completed phases first, while writers stay stopped.

- Restore old-prefix permissions and the old stage name/URL using reviewed
  Terraform plans and the recorded original inputs. Never manually edit state.
- If old keys were hidden, remove only the journal's recorded delete-marker
  versions. Do not delete original data versions or unrecorded markers.
- Restore old source labels and original manifest keys in one transaction using
  the journal's exact mappings. Verify unchanged normalized fingerprints before
  committing; restore the matching original staging definitions and code.
- Restore the original cache configuration if needed; original caches were kept.
- Retain the verified Polygon archive copies during investigation. Never overwrite
  a conflicting object to force a retry to succeed.

## Historical names that remain intentionally

Original archived bytes still contain legacy operational source labels. Python
archive reads and Snowflake COPY expressions translate those two labels at replay;
they do not change provider payloads or weaken checksum verification. Historical
verification records, retained backup schemas, old S3 versions, original cache
files, official provider URLs, and migration mappings retain accurate original
names. Retired infrastructure templates remain provenance rather than deployment
entrypoints. These exceptions do not change active provider naming.
