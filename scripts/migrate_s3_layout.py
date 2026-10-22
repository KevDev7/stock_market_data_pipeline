"""Safely flatten archive paths. Preview is read-only; Terraform rollout is separate.

Reuse the verified version-pinned copy/delete-marker machinery from the naming
migration. Only tracking-table S3_KEY values change; raw/model data never changes.
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from scripts.inspect_infrastructure import query, snowflake_connection
from scripts.migrate_polygon_naming import PolygonMigration, fingerprint
from scripts.verify_infrastructure_access import verify_archives, verify_dashboard
from src.archive_layout import PREFIX_MOVES, normalized_key_sql, simplified_archive_key

TRACKING = ('INGESTION_CHECKPOINTS', 'REFERENCE_MANIFEST', 'PRICE_REFRESH_MANIFEST')


def layout_snapshot(cursor, database):
    result = {}
    for table in TRACKING:
        relation = f'{database}.ADMIN.{table}'
        columns = query(cursor, f'DESC TABLE {relation}')
        projection = ','.join(normalized_key_sql('S3_KEY') if c['name'] == 'S3_KEY'
                              else '"' + c['name'] + '"' for c in columns)
        result['ADMIN.' + table] = fingerprint(cursor, relation, projection)
    relations = query(cursor, f"SELECT TABLE_SCHEMA,TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES "
                      "WHERE TABLE_SCHEMA IN ('RAW','STAGING','INTERMEDIATE','MARTS') "
                      "AND TABLE_NAME NOT LIKE 'TMP_%' ORDER BY 1,2")
    for row in relations:
        name = row['table_schema'] + '.' + row['table_name']
        result[name] = fingerprint(cursor, database + '.' + name)
    return result


def tracking_structure(cursor, database):
    result = {name: dict(
        columns=query(cursor, f'DESC TABLE {database}.ADMIN.{name}'),
        grants=query(cursor, f'SHOW GRANTS ON TABLE {database}.ADMIN.{name}')) for name in TRACKING}
    # SHOW GRANTS timestamps must compare identically after a journal reload.
    return json.loads(json.dumps(result, default=str))


class LayoutMigration(PolygonMigration):
    source_prefixes = list(PREFIX_MOVES)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.record and self.record.get('migration') != 'dataset-folders-v1':
            raise RuntimeError('Wrong migration journal')

    def snapshot(self):
        return layout_snapshot(self.cur, self.database)

    def versions(self):
        versions, markers = [], []
        for prefix in self.source_prefixes:
            for page in self.s3.get_paginator('list_object_versions').paginate(
                    Bucket=self.bucket, Prefix=prefix):
                versions.extend(page.get('Versions', []))
                markers.extend(page.get('DeleteMarkers', []))
        return versions, markers

    def inventory(self):
        if self.record:
            return self.summary()
        if self.s3.get_bucket_versioning(Bucket=self.bucket).get('Status') != 'Enabled':
            raise RuntimeError('Versioning must be enabled before migration')
        versions, markers = self.versions()
        if not versions or markers or any(not v['IsLatest'] for v in versions):
            raise RuntimeError('Unexpected source versions; inspect before migration')
        for prefix in PREFIX_MOVES.values():
            if self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1).get('Contents'):
                raise RuntimeError('Destination exists without this migration journal: ' + prefix)
            pages = self.s3.get_paginator('list_object_versions').paginate(Bucket=self.bucket, Prefix=prefix)
            if any(p.get('Versions') or p.get('DeleteMarkers') for p in pages):
                raise RuntimeError('Destination has version history; inspect before migration: ' + prefix)
        refs = {}
        for table in TRACKING:
            checksum = 'S3_SHA256' if table == 'INGESTION_CHECKPOINTS' else 'SHA256'
            for row in query(self.cur, f'SELECT S3_KEY,{checksum} AS SHA FROM {self.database}.ADMIN.{table} WHERE S3_KEY IS NOT NULL'):
                key, sha = row['s3_key'], row['sha']
                simplified_archive_key(key)
                if key in refs and refs[key] != sha:
                    raise RuntimeError('Tracking records disagree on archive checksum')
                refs[key] = sha
        entries = []
        for v in sorted(versions, key=lambda r: r['Key']):
            key = v['Key']
            sha = refs.get(key)
            if not sha:
                head = self.s3.head_object(Bucket=self.bucket, Key=key, VersionId=v['VersionId'])
                sha = head.get('Metadata', {}).get('sha256')
            if not sha:
                raise RuntimeError('Missing archive checksum: ' + key)
            entries.append(dict(old_key=key, new_key=simplified_archive_key(key),
                version=v['VersionId'], etag=v['ETag'], size=v['Size'], sha256=sha))
        if set(refs) - {e['old_key'] for e in entries}:
            raise RuntimeError('Tracking records reference missing source archives')
        self.cur.execute('SELECT CURRENT_ACCOUNT()')
        self.record = dict(migration='dataset-folders-v1', bucket=self.bucket, database=self.database,
            account=self.cur.fetchone()[0], started_at=datetime.now(timezone.utc).isoformat(),
            entries=entries, copied=[], hidden=[], phases=[], baseline=self.snapshot(),
            tracking_structure=tracking_structure(self.cur, self.database),
            dashboard_before=verify_dashboard())
        return self.summary()

    def assert_originals_unchanged(self):
        super().assert_originals_unchanged()
        versions, markers = self.versions()
        hidden = {e['old_key']: e.get('delete_marker') for e in self.record['entries']}
        for version in versions:
            if version['IsLatest'] == bool(hidden[version['Key']]):
                raise RuntimeError('Unexpected source visibility')
        if any(not m['IsLatest'] or hidden.get(m['Key']) != m['VersionId'] for m in markers):
            raise RuntimeError('Unexpected source delete-marker history')

    def metadata(self):
        if 'metadata' in self.record['phases']:
            if self.snapshot() != self.record['baseline']:
                raise RuntimeError('Warehouse changed after metadata cutover')
            return
        if set(self.record['copied']) != {e['old_key'] for e in self.record['entries']}:
            raise RuntimeError('Every archive must be copied and verified before cutover')
        self.assert_originals_unchanged()
        if self.snapshot() != self.record['baseline']:
            raise RuntimeError('Warehouse changed before metadata cutover')
        condition = ' OR '.join(f"STARTSWITH(S3_KEY,'{old}')" for old in PREFIX_MOVES)
        self.cur.execute('BEGIN')
        try:
            for table in TRACKING:
                self.cur.execute(f'UPDATE {self.database}.ADMIN.{table} '
                    f'SET S3_KEY={normalized_key_sql("S3_KEY")} WHERE {condition}')
            if self.snapshot() != self.record['baseline']:
                raise RuntimeError('Metadata cutover changed fields other than S3 paths')
            self.cur.execute('COMMIT')
        except BaseException:
            self.cur.execute('ROLLBACK')
            raise
        self.record['phases'].append('metadata')
        self.save()

    def verify(self):
        from types import SimpleNamespace
        from src.reference_load import ReferenceWarehouse
        if 'metadata' not in self.record['phases']:
            raise RuntimeError('Metadata cutover is required first')
        if self.snapshot() != self.record['baseline']:
            raise RuntimeError('Warehouse contents changed')
        if tracking_structure(self.cur, self.database) != self.record['tracking_structure']:
            raise RuntimeError('Tracking columns or grants changed')
        targets = {e['new_key']: e['sha256'] for e in self.record['entries']}
        for table in TRACKING:
            checksum = 'S3_SHA256' if table == 'INGESTION_CHECKPOINTS' else 'SHA256'
            rows = query(self.cur, f'SELECT S3_KEY,{checksum} AS SHA FROM {self.database}.ADMIN.{table} WHERE S3_KEY IS NOT NULL')
            if any(targets.get(r['s3_key']) != r['sha'] for r in rows):
                raise RuntimeError('Tracking paths differ from verified archives')
        ReferenceWarehouse(SimpleNamespace(cursor=self.cur)).check_infrastructure()
        self.record['stage_reads'] = verify_archives(self.cur.connection)
        self.record['dashboard_after'] = verify_dashboard()
        if self.record['dashboard_after']['status'] != 'passed':
            raise RuntimeError('Dashboard access verification failed')
        self.record['phases'] = sorted(set(self.record['phases']) | {'verified'})
        self.save()


def main():
    import boto3
    from src.config import AWS, SNOWFLAKE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['preview','copy','metadata','verify','hide'], default='preview', nargs='?')
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--idle-confirmed', action='store_true')
    parser.add_argument('--aws-profile', default='synthea-cdc')
    parser.add_argument('--journal', type=Path, default=Path('.cache/s3-layout/migration.json'))
    args = parser.parse_args()
    if args.phase != 'preview' and not (args.execute and args.idle_confirmed):
        parser.error('Mutating phases require --execute --idle-confirmed; keep ingestion stopped.')
    session = boto3.Session(profile_name=args.aws_profile, region_name=AWS['region'])
    if session.client('sts').get_caller_identity()['Account'] != '645968351825':
        raise RuntimeError('Wrong AWS account')
    connection = snowflake_connection()
    try:
        cur = connection.cursor()
        cur.execute("ALTER SESSION SET QUERY_TAG='stock_market_s3_layout_migration'")
        migration = LayoutMigration(session.client('s3'), cur, AWS['s3_bucket'], SNOWFLAKE['database'], args.journal)
        if args.phase == 'preview':
            print(json.dumps(migration.inventory(), indent=2))
            return
        if not migration.record:
            migration.inventory()
            migration.save()
        getattr(migration, args.phase)()
        print(json.dumps(migration.summary(), indent=2))
    finally:
        connection.close()


if __name__ == '__main__':
    main()
