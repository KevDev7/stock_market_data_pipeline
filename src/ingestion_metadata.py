"""Application-owned ingestion metadata; source payloads remain in RAW."""
import re


MANIFEST_DDL = {
    'REFERENCE_MANIFEST': '''API_DATE DATE, SOURCE STRING, RUN_ID STRING, ROW_COUNT INTEGER,
        REQUESTED_COUNT INTEGER, S3_KEY STRING, SHA256 STRING, COMPLETED_AT TIMESTAMP_NTZ''',
    'PRICE_REFRESH_MANIFEST': '''CAMPAIGN STRING, API_DATE DATE, S3_KEY STRING,
        SHA256 STRING, REFRESHED_AT TIMESTAMP_NTZ''',
}


def validate_database(database):
    if not isinstance(database, str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', database):
        raise ValueError('Invalid database identifier')
    return database


def require_metadata_migrated(cursor, database):
    """Do not create empty completion tables alongside retained legacy records."""
    validate_database(database)
    cursor.execute(f'''SELECT TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='RAW'
        AND TABLE_NAME IN ('REFERENCE_MANIFEST','PRICE_REFRESH_MANIFEST')''')
    if cursor.fetchall():
        raise RuntimeError(
            'Ingestion manifests still exist in RAW. Keep ingestion paused and run '
            'python -m scripts.migrate_ingestion_metadata before ingestion setup.')


def ensure_manifest(cursor, database, table):
    validate_database(database)
    if table not in MANIFEST_DDL:
        raise ValueError('Unknown ingestion manifest')
    require_metadata_migrated(cursor, database)
    cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {database}.ADMIN')
    cursor.execute(f'CREATE TABLE IF NOT EXISTS {database}.ADMIN.{table} ({MANIFEST_DDL[table]})')
