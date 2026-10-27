"""Endpoint-specific reference landing tables, owned by ingestion setup."""
from src.ingestion_metadata import validate_database


REFERENCE_TABLES = {
    'polygon_ticker_catalog': 'TICKER_CATALOG_RAW',
    'polygon_ticker_overview': 'COMPANY_OVERVIEW_RAW',
}
COLUMNS = 'API_DATE,RUN_ID,SOURCE,RAW_PAYLOAD,INGESTED_AT'
LANDING_DDL = ('API_DATE DATE, RUN_ID STRING, SOURCE STRING, '
               'RAW_PAYLOAD STRING, INGESTED_AT TIMESTAMP_NTZ')
RETIRED_MARKER = 'raw-reference-split-v1: complete; frozen recovery copy'


def reference_table(database, source):
    validate_database(database)
    if source not in REFERENCE_TABLES:
        raise ValueError('Unknown reference source')
    return database + '.RAW.' + REFERENCE_TABLES[source]


def require_reference_split(cursor, database):
    """Never silently create empty replacements for an unmigrated shared table."""
    validate_database(database)
    names = ['REFERENCE_RAW', *REFERENCE_TABLES.values()]
    placeholders = ','.join(['%s'] * len(names))
    cursor.execute(f'''SELECT TABLE_NAME,COMMENT,TABLE_TYPE
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='RAW' AND TABLE_NAME IN ({placeholders})''', tuple(names))
    tables = {r[0]: (r[1], r[2]) for r in cursor.fetchall()}
    if 'REFERENCE_RAW' in tables:
        if (tables['REFERENCE_RAW'][0] != RETIRED_MARKER
                or not set(REFERENCE_TABLES.values()).issubset(tables)):
            raise RuntimeError('Keep ingestion paused and run python -m '
                               'scripts.migrate_reference_raw_tables before setup.')
    for name in REFERENCE_TABLES.values():
        if name in tables:
            if tables[name][1] != 'BASE TABLE':
                raise RuntimeError('Expected a landing table: ' + name)
            cursor.execute(f'DESC TABLE {database}.RAW.{name}')
            actual = [(r[0], r[1], r[3], r[4]) for r in cursor.fetchall()]
            expected = [('API_DATE', 'DATE', 'Y', None),
                        ('RUN_ID', 'VARCHAR(16777216)', 'Y', None),
                        ('SOURCE', 'VARCHAR(16777216)', 'Y', None),
                        ('RAW_PAYLOAD', 'VARCHAR(16777216)', 'Y', None),
                        ('INGESTED_AT', 'TIMESTAMP_NTZ(9)', 'Y', None)]
            if actual != expected:
                raise RuntimeError('Unexpected landing columns: ' + name)


def ensure_reference_tables(cursor, database):
    require_reference_split(cursor, database)
    for source in REFERENCE_TABLES:
        cursor.execute(f'CREATE TABLE IF NOT EXISTS {reference_table(database, source)} ({LANDING_DDL})')
