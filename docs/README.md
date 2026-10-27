# Documentation Index

## Guides

- [Architecture and data model](architecture.md)
- [Environment and installation](setup.md)
- [Infrastructure configuration and safe Terraform adoption](../infra/terraform/README.md)
- [Operations, backfills, and testing](operations.md)
- [Polygon naming and archive migration](polygon-naming-migration.md)
- [S3 dataset-folder layout and migration](s3-layout-migration.md)
- [Current API-date archive layout and recovery](date-archive-migration.md)
- [Endpoint-specific RAW tables and recovery](raw-reference-split.md)
- [Limitations](limitations.md)

## Verification Results

- [Verified split into endpoint-specific RAW tables](raw-reference-split-results.json)
- [Verified API-date archive simplification](date-archive-migration-results.json)
- [Verified Polygon naming and archive migration](polygon-naming-migration-results.json)
- [Verified S3 dataset-folder migration](s3-layout-migration-results.json)
- [Verified four-layer consolidation](four-layer-refactor-results.json)
- [Verified moving-average rebuild](sma-fix-rebuild-results.json)
- [Verified reference migration results](reference-migration-results.json)

## Reference Files

- [Example Snowflake queries](../examples/queries.sql)
- [Environment variable template](../.env.example)
