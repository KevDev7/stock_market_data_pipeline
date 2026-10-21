"""Preview or resume the two manifest moves. Never creates or copies a table.

Execution requires stopped ingestion and a private local journal. Snowflake DDL
commits separately, so the journal records the original objects before either move.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile

from scripts.inspect_infrastructure import query
from src.ingestion_metadata import MANIFEST_DDL, validate_database


def table_snapshot(cursor, database, schema, name):
    full_name = f'{database}.{schema}.{name}'
    records = query(cursor, f'''SELECT TABLE_TYPE,TABLE_OWNER FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{name}' ''')
    if not records:
        return None
    if len(records) != 1 or records[0]['table_type'] != 'BASE TABLE':
        raise RuntimeError('Expected an ordinary table: ' + full_name)
    cursor.execute(f'SELECT COUNT(*), HASH_AGG(*) FROM {full_name}')
    count, fingerprint = cursor.fetchone()
    columns = query(cursor, f'DESC TABLE {full_name}')
    grants = query(cursor, f'SHOW GRANTS ON TABLE {full_name}')
    # Names and grant timestamps describe the location, not the retained access.
    grants = sorted([
        {k: r.get(k) for k in ('privilege', 'granted_on', 'granted_to',
                               'grantee_name', 'grant_option', 'granted_by')}
        for r in grants
    ], key=lambda r: json.dumps(r, sort_keys=True, default=str))
    return json.loads(json.dumps(dict(owner=records[0]['table_owner'],
        columns=columns, grants=grants, rows=count, fingerprint=str(fingerprint)), default=str))


def locations(cursor, database):
    result = {}
    for name in MANIFEST_DDL:
        source = table_snapshot(cursor, database, 'RAW', name)
        destination = table_snapshot(cursor, database, 'ADMIN', name)
        if source is not None and destination is not None:
            raise RuntimeError('Both RAW and ADMIN contain ' + name + '; resolve without overwriting either.')
        if source is None and destination is None:
            raise RuntimeError('Neither schema contains ' + name + '; this migration does not create tables.')
        result[name] = dict(location='RAW' if source is not None else 'ADMIN',
                            snapshot=source if source is not None else destination)
    return result


def protected_snapshot(cursor, database):
    counts = query(cursor, f'''SELECT TABLE_SCHEMA,TABLE_NAME,ROW_COUNT
        FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA IN ('RAW','MARTS') AND TABLE_TYPE='BASE TABLE'
        AND TABLE_NAME NOT IN ('REFERENCE_MANIFEST','PRICE_REFRESH_MANIFEST')
        ORDER BY TABLE_SCHEMA,TABLE_NAME''')
    return dict(data_counts=counts,
                checkpoints=table_snapshot(cursor, database, 'ADMIN', 'INGESTION_CHECKPOINTS'))


def check_permissions(cursor, database, tables):
    schemas = query(cursor, f'SHOW SCHEMAS IN DATABASE {database}')
    admin = next((s for s in schemas if s['name'] == 'ADMIN'), None)
    if admin is None:
        raise RuntimeError('ADMIN must already exist before moving tables.')

    def active(role):
        cursor.execute('SELECT IS_ROLE_IN_SESSION(%s)', (role,))
        return cursor.fetchone()[0] is True

    schema_grants = query(cursor, f'SHOW GRANTS ON SCHEMA {database}.ADMIN')
    can_create = active(admin['owner']) or any(
        g['privilege'] == 'CREATE TABLE' and active(g['grantee_name'])
        for g in schema_grants)
    if not can_create:
        raise RuntimeError('The current role lacks CREATE TABLE access to ADMIN.')
    for name, entry in tables.items():
        if not active(entry['snapshot']['owner']):
            raise RuntimeError('The current role does not inherit table ownership for ' + name)
        if ('MANAGED ACCESS' in admin.get('options', '')
                and entry['snapshot']['owner'] != admin['owner']):
            raise RuntimeError('Managed-access ADMIN requires matching table and schema ownership.')
    return dict(owner=admin['owner'], options=admin.get('options', ''),
                grants=schema_grants, create_table_access=True)


def save_journal(path, record):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as handle:
            json.dump(record, handle, indent=2, default=str)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def migrate(cursor, database, *, execute=False, journal_path=None, idle_confirmed=False):
    database = validate_database(database)
    identity = query(cursor, 'SELECT CURRENT_ACCOUNT() AS ACCOUNT, CURRENT_USER() AS USER, '
                     'CURRENT_ROLE() AS ROLE')[0]
    tables = locations(cursor, database)
    permissions = check_permissions(cursor, database, tables)
    protected = protected_snapshot(cursor, database)
    if protected['checkpoints'] is None:
        raise RuntimeError('The existing ingestion checkpoint table is missing.')
    report = dict(database=database, identity=identity, tables=tables,
                  admin_permissions=permissions, protected=protected, executed=False)
    if not execute:
        return report
    if not idle_confirmed or journal_path is None:
        raise RuntimeError('Execution requires --idle-confirmed and a private --journal path.')
    path = Path(journal_path)
    if path.exists():
        journal = json.loads(path.read_text())
        if (journal['database'] != database or journal['account'] != identity['account']):
            raise RuntimeError('Journal belongs to a different account or database.')
        if set(journal['baseline']) != set(MANIFEST_DDL):
            raise RuntimeError('Journal does not cover both manifests.')
        for name, entry in tables.items():
            if entry['snapshot'] != journal['baseline'][name]:
                raise RuntimeError('Table differs from the recorded baseline: ' + name)
        if protected != journal['protected']:
            raise RuntimeError('Protected tables differ from the recorded baseline.')
    else:
        if any(e['location'] != 'RAW' for e in tables.values()):
            raise RuntimeError('A moved manifest needs its original journal for verified recovery.')
        journal = dict(database=database, account=identity['account'],
            started_at=datetime.now(timezone.utc).isoformat(),
            baseline={n: e['snapshot'] for n, e in tables.items()}, protected=protected,
            completed=[])
        save_journal(path, journal)
    # Both tables and permissions have been checked before the first DDL.
    for name, entry in tables.items():
        if entry['location'] == 'RAW':
            # Detect a concurrent writer before each separately committed rename.
            if table_snapshot(cursor, database, 'RAW', name) != journal['baseline'][name]:
                raise RuntimeError('Manifest changed during cutover: ' + name)
            cursor.execute(f'ALTER TABLE {database}.RAW.{name} RENAME TO {database}.ADMIN.{name}')
        after = table_snapshot(cursor, database, 'ADMIN', name)
        if after != journal['baseline'][name]:
            raise RuntimeError('Moved table failed content/column/owner/grant verification: ' + name)
        if name not in journal['completed']:
            journal['completed'].append(name)
        save_journal(path, journal)
    if protected_snapshot(cursor, database) != journal['protected']:
        raise RuntimeError('Protected source data, marts, or checkpoints changed during cutover.')
    final = locations(cursor, database)
    if any(e['location'] != 'ADMIN' for e in final.values()):
        raise RuntimeError('Manifest cutover incomplete.')
    journal['verified_at'] = datetime.now(timezone.utc).isoformat()
    save_journal(path, journal)
    report.update(tables=final, executed=True, verified=True)
    return report


def main():
    from scripts.inspect_infrastructure import snowflake_connection
    from src.config import SNOWFLAKE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', default=SNOWFLAKE['database'])
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--idle-confirmed', action='store_true',
                        help='Confirm ingestion/refresh jobs are stopped and the DAG will remain paused.')
    parser.add_argument('--journal', type=Path,
                        default=Path('.cache/ingestion-metadata/migration.json'))
    parser.add_argument('--report', type=Path,
                        help='Optional private JSON report containing the full inspected metadata.')
    args = parser.parse_args()
    connection = snowflake_connection()
    try:
        connection.cursor().execute("ALTER SESSION SET QUERY_TAG='stock_market_ingestion_metadata_migration'")
        result = migrate(connection.cursor(), args.database, execute=args.execute,
                         journal_path=args.journal, idle_confirmed=args.idle_confirmed)
        if args.report:
            save_journal(args.report, result)
        summary = dict(database=result['database'], executed=result['executed'],
            verified=result.get('verified', False),
            tables={name: dict(location=e['location'], rows=e['snapshot']['rows'],
                              fingerprint=e['snapshot']['fingerprint'], owner=e['snapshot']['owner'])
                    for name, e in result['tables'].items()},
            admin_create_table_access=result['admin_permissions']['create_table_access'])
        print(json.dumps(summary, indent=2))
    finally:
        connection.close()


if __name__ == '__main__':
    main()
