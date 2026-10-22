# Archives by API date

Each dataset has one current gzip NDJSON file per API date:

```text
stock-market-data-pipeline-645968351825-us-east-1/
├── daily-stock-prices/
│   └── api_date=<date>/daily_stocks_raw.ndjson.gz
├── ticker-catalog/
│   └── api_date=<date>/reference_raw.ndjson.gz
└── company-overview/
    └── api_date=<date>/reference_raw.ndjson.gz
```

Normal ingestion skips completed dates. A retry or deliberate refresh writes to
the same date's path. Bucket versioning retains replaced files. Run IDs remain in
the data, object metadata, logs, and tracking records. The scheduled DAG is
serialized; stop it before manual backfills or refreshes. Before publishing,
loaders check that staged records belong to the expected date, source, and run.

The three ADMIN tracking tables retain their columns, records, timestamps, and
checksums. Only their `S3_KEY` values change. A historical tracking record describes
the bytes loaded by that attempt; its path now identifies a date's current file,
which can change after a refresh. To recover that older download, inspect the
object's versions and match its recorded checksum/run metadata. Existing migrated
downloads also have exact original and destination VersionIds in the private
migration journal. Normal ingestion does not add version-ID tracking tables.

The completed migration is recorded in
[verification results](date-archive-migration-results.json). Its original tracking
rows and version inventory are retained privately in
`.cache/date-layout/discovery.json` alongside the migration journal.

## Migration

Keep Airflow and all manual ingestion/refresh writers stopped throughout. The
command defaults to a read-only preview:

```bash
python -m scripts.migrate_date_archives preview
python -m scripts.migrate_date_archives copy --execute --idle-confirmed
python -m scripts.migrate_date_archives metadata --execute --idle-confirmed
python -m scripts.migrate_date_archives verify --execute --idle-confirmed
python -m scripts.migrate_date_archives hide --execute --idle-confirmed
python -m scripts.migrate_date_archives verify --execute --idle-confirmed
```

The private `.cache/date-layout/migration.json` records the baseline, selected
current files, every old/new key, SHA-256, original and copied VersionIds, and
recoverable delete markers. Do not delete it or commit it. Inventory refuses
unexpected destination history. A copy retry verifies existing destination
versions and resumes without duplicating them, including after a lost response.

All original downloads are copied byte-for-byte into versions of the date-only
key. For dates with several downloads, the file matching the retained RAW run is
copied last so it becomes current. Legacy single-file exports are accepted only
after full stage-versus-RAW fingerprints match. Metadata cutover updates only
paths in the three ADMIN tables, in one transaction; payloads and model tables
are untouched. Both current archives and all retained historical copies are
checksum-verified. Existing run paths are hidden with delete markers only after
verification. Their original versions remain recoverable too.

S3 prefixes, stages, IAM permissions, and Terraform infrastructure do not change.
There are no source requests, warehouse payload writes, model rebuilds, dashboard
deployments, or diagram changes. Loader tests cover date paths, empty responses,
failed loads, run mismatches, and atomic publication. Live checks compare all
current archive records with RAW, protected warehouse fingerprints and grants,
and dashboard reads from all eight published marts.

## Recovery

Stop writers and inspect the private journal. Do not automatically undo a partial
migration or overwrite any newly ingested data.

1. Remove only the journaled delete markers from original run paths, checking
   that the original versions are still present.
2. Restore the matching pre-migration code. Restore ADMIN paths using the journal's
   old/new key mapping plus checksum. When multiple original keys share identical
   bytes, use the recorded run ID and original tracking record to disambiguate.
3. Recheck warehouse fingerprints, stage reads, and tracking records. If normal
   ingestion has occurred since migration, recover its newer date files separately;
   do not substitute an old baseline for new data.
4. If retiring date-only paths, add recoverable delete markers. Never permanently
   delete their object versions or the retained original archives.

The original dataset-folder and provider-naming migration journals remain valid
historical records. Their verification commands describe those earlier layouts;
use this migration's verification for the date-only layout.
