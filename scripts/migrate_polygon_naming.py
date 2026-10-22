"""Phased Polygon naming migration. Preview is read-only against cloud resources.

Run from the repository root. Terraform changes are separate reviewed saved plans;
this command never changes IAM, integrations, stages, credentials, or Terraform state.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from botocore.exceptions import ClientError

from scripts.inspect_infrastructure import query
from scripts.migrate_ingestion_metadata import save_journal
from scripts.verify_infrastructure_access import PUBLISHED
from src.provider_names import (LEGACY_REFERENCE_PREFIX, REFERENCE_PREFIX,
    SOURCE_ALIASES, canonical_source_sql, polygon_archive_key)

VIEWS = ['STG_MARKET__CATALOG', 'STG_MARKET__ISSUER_OBSERVATIONS']


def sqlite_fingerprint(connection):
    digest = hashlib.sha256()
    count = 0
    for key, payload in connection.execute('SELECT cache_key,payload FROM responses ORDER BY cache_key'):
        digest.update(json.dumps([key, payload], separators=(',', ':')).encode())
        count += 1
    return dict(rows=count, sha256=digest.hexdigest())


def preserve_cache(source, destination):
    source, destination = Path(source), Path(destination)
    with sqlite3.connect(f'file:{source.resolve()}?mode=ro', uri=True) as original:
        expected = sqlite_fingerprint(original)
        if destination.exists():
            with sqlite3.connect(f'file:{destination.resolve()}?mode=ro', uri=True) as copied:
                if sqlite_fingerprint(copied) != expected:
                    raise RuntimeError('Destination cache conflicts with original: ' + str(destination))
            return expected
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(destination.name + '.migrating')
        if temporary.exists():
            raise RuntimeError('Inspect interrupted SQLite backup before retrying: ' + str(temporary))
        with sqlite3.connect(temporary) as copied:
            original.backup(copied)
            if sqlite_fingerprint(copied) != expected:
                raise RuntimeError('SQLite backup failed verification')
        temporary.chmod(0o600)
        temporary.rename(destination)
        return expected


def view_grants(cursor, database, name):
    rows = query(cursor, f'SHOW GRANTS ON VIEW {database}.STAGING.{name}')
    return sorted([{k: r.get(k) for k in ('privilege','granted_to','grantee_name','grant_option')}
                   for r in rows], key=lambda r: json.dumps(r, sort_keys=True))


def fingerprint(cursor, relation, projection='*'):
    cursor.execute(f'SELECT COUNT(*), TO_VARCHAR(HASH_AGG({projection})) FROM {relation}')
    count, digest = cursor.fetchone()
    return dict(rows=count, hash=digest)


def bridge_view_ddl(ddl, database, view):
    original = ddl
    for old,new in SOURCE_ALIASES.items():
        ddl = ddl.replace(f"SOURCE = '{old}'", f"SOURCE IN ('{old}','{new}')")
    if ddl == original:
        raise RuntimeError('Could not locate original source filter in ' + view)
    ddl,count = re.subn(r'^(create or replace view\s+)[^\s(]+',
        lambda m:m.group(1)+f'{database}.STAGING.{view}',ddl,count=1,flags=re.I)
    if count != 1:
        raise RuntimeError('Could not qualify bridge view')
    ddl,count = re.subn(r'\bas\s*\(', 'COPY GRANTS AS (', ddl, count=1, flags=re.I)
    if count != 1:
        raise RuntimeError('Could not preserve grants in view DDL')
    return ddl


def warehouse_snapshot(cursor, database):
    """Normalize only names/paths so pre/post values can be compared exactly."""
    raw = fingerprint(cursor, database + '.RAW.REFERENCE_RAW',
        'API_DATE,RUN_ID,' + canonical_source_sql('SOURCE') + ',RAW_PAYLOAD,INGESTED_AT')
    key_sql = 'S3_KEY'
    for old, new in [(LEGACY_REFERENCE_PREFIX, REFERENCE_PREFIX), *SOURCE_ALIASES.items()]:
        key_sql = f"REPLACE({key_sql},'{old}','{new}')"
    manifest = fingerprint(cursor, database + '.ADMIN.REFERENCE_MANIFEST',
        f'API_DATE,{canonical_source_sql("SOURCE")},RUN_ID,ROW_COUNT,REQUESTED_COUNT,'
        f'{key_sql},SHA256,COMPLETED_AT')
    relations = ['RAW.DAILY_STOCKS_RAW', 'ADMIN.INGESTION_CHECKPOINTS',
                 'ADMIN.PRICE_REFRESH_MANIFEST']
    relations += ['STAGING.' + v for v in VIEWS]
    relations += ['MARTS.' + t for t in [*PUBLISHED, 'CALC_SECURITY_DAILY_MOMENTUM']]
    return dict(reference=raw, manifest=manifest,
                protected={r: fingerprint(cursor, database + '.' + r) for r in relations})


class PolygonMigration:
    def __init__(self, s3, cursor, bucket, database, journal):
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', database):
            raise ValueError('Invalid database identifier')
        self.s3, self.cur = s3, cursor
        self.bucket, self.database, self.path = bucket, database, Path(journal)
        self.record = json.loads(self.path.read_text()) if self.path.exists() else None
        if self.record and (self.record['bucket'] != bucket or self.record['database'] != database):
            raise RuntimeError('Journal belongs to a different bucket/database')
        if self.record:
            cursor.execute('SELECT CURRENT_ACCOUNT()')
            if cursor.fetchone()[0] != self.record['account']:
                raise RuntimeError('Journal belongs to a different Snowflake account')

    def save(self):
        save_journal(self.path, self.record)

    def versions(self):
        versions, markers = [], []
        for page in self.s3.get_paginator('list_object_versions').paginate(
                Bucket=self.bucket, Prefix='raw/massive/'):
            versions.extend(page.get('Versions', []))
            markers.extend(page.get('DeleteMarkers', []))
        return versions, markers

    def current_object(self, key):
        try:
            return self.s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            if exc.response['Error']['Code'] in ('404','NoSuchKey','NotFound'):
                return None
            raise

    def inventory(self):
        if self.record:
            return self.summary()
        versions, markers = self.versions()
        if markers or any(not v['IsLatest'] for v in versions):
            raise RuntimeError('Additional historical versions need an expanded migration; nothing changed')
        if not versions:
            raise RuntimeError('No original reference archives found')
        destinations = {o['Key'] for page in self.s3.get_paginator('list_objects_v2').paginate(
            Bucket=self.bucket, Prefix=REFERENCE_PREFIX + '/') for o in page.get('Contents', [])}
        entries = []
        for version in sorted(versions, key=lambda v: v['Key']):
            old = version['Key']
            new = polygon_archive_key(old)
            if new in destinations:
                raise RuntimeError('Destination exists without a migration journal: ' + new)
            entries.append(dict(old_key=old, new_key=new, version=version['VersionId'],
                etag=version['ETag'], size=version['Size']))
        self.cur.execute('SELECT CURRENT_ACCOUNT()')
        account = self.cur.fetchone()[0]
        manifests = query(self.cur, f'SELECT SOURCE,API_DATE,S3_KEY,SHA256,ROW_COUNT '
                          f'FROM {self.database}.ADMIN.REFERENCE_MANIFEST')
        if {m['s3_key'] for m in manifests} != {e['old_key'] for e in entries} or len(manifests) != len(entries):
            raise RuntimeError('Archive inventory and reference manifest do not match one-to-one')
        by_key = {m['s3_key']: m for m in manifests}
        for entry in entries:
            m = by_key[entry['old_key']]
            if m['source'] not in SOURCE_ALIASES:
                raise RuntimeError('Unexpected manifest source')
            entry.update(sha256=m['sha256'], rows=m['row_count'], source=m['source'])
        definitions = {}
        for view in VIEWS:
            self.cur.execute('SELECT GET_DDL(%s,%s)', ('VIEW', f'{self.database}.STAGING.{view}'))
            definitions[view] = dict(ddl=self.cur.fetchone()[0], grants=view_grants(self.cur, self.database, view))
        self.record = dict(bucket=self.bucket, database=self.database, account=account,
            started_at=datetime.now(timezone.utc).isoformat(), entries=entries,
            baseline=warehouse_snapshot(self.cur, self.database), views=definitions,
            copied=[], hidden=[], phases=[])
        return self.summary()

    def assert_originals_unchanged(self):
        versions, markers = self.versions()
        expected = {(e['old_key'], e['version']) for e in self.record['entries']}
        actual = {(v['Key'], v['VersionId']) for v in versions}
        if actual != expected:
            raise RuntimeError('Original archive versions changed during migration')
        known = {e.get('delete_marker') for e in self.record['entries'] if e.get('delete_marker')}
        if any(m['VersionId'] not in known for m in markers):
            raise RuntimeError('Unexpected delete marker; inspect before proceeding')

    def verify_entry(self, entry, copy=False):
        original = self.s3.get_object(Bucket=self.bucket, Key=entry['old_key'], VersionId=entry['version'])
        body = original['Body'].read()
        if len(body) != entry['size'] or hashlib.sha256(body).hexdigest() != entry['sha256']:
            raise RuntimeError('Original archive checksum differs from manifest: ' + entry['old_key'])
        target = self.current_object(entry['new_key'])
        if not target:
            if not copy:
                raise RuntimeError('Verified destination is missing: ' + entry['new_key'])
            self.s3.copy_object(Bucket=self.bucket, Key=entry['new_key'],
                CopySource={'Bucket':self.bucket, 'Key':entry['old_key'], 'VersionId':entry['version']},
                CopySourceIfMatch=entry['etag'], MetadataDirective='COPY', TaggingDirective='COPY',
                ServerSideEncryption=original.get('ServerSideEncryption', 'AES256'))
        destination = self.s3.get_object(Bucket=self.bucket, Key=entry['new_key'])
        if hashlib.sha256(destination['Body'].read()).hexdigest() != entry['sha256']:
            raise RuntimeError('Destination conflicts with original: ' + entry['new_key'])
        for field in ('Metadata','ContentType','ContentEncoding','CacheControl','ContentDisposition',
                      'ContentLanguage','ServerSideEncryption'):
            if original.get(field) != destination.get(field):
                raise RuntimeError('Destination object attributes differ: ' + field)
        tags_old = self.s3.get_object_tagging(Bucket=self.bucket, Key=entry['old_key'], VersionId=entry['version'])['TagSet']
        tags_new = self.s3.get_object_tagging(Bucket=self.bucket, Key=entry['new_key'])['TagSet']
        if sorted(tags_old, key=lambda x:x['Key']) != sorted(tags_new, key=lambda x:x['Key']):
            raise RuntimeError('Destination object tags differ')
        return entry['old_key']

    def copy(self):
        self.assert_originals_unchanged()
        for start in range(0, len(self.record['entries']), 32):
            batch = self.record['entries'][start:start+32]
            with ThreadPoolExecutor(max_workers=8) as pool:
                verified = list(pool.map(lambda e:self.verify_entry(e, copy=True), batch))
            self.record['copied'] = sorted(set(self.record['copied']) | set(verified))
            self.save()
            print(f'Archives verified: {len(self.record["copied"])}/{len(self.record["entries"])}', flush=True)

    def bridge(self):
        if 'bridge' in self.record['phases']:
            return
        for view, saved in self.record['views'].items():
            ddl = bridge_view_ddl(saved['ddl'], self.database, view)
            self.cur.execute(ddl)
            if view_grants(self.cur,self.database,view) != saved['grants']:
                raise RuntimeError('Bridge view grants changed')
        if warehouse_snapshot(self.cur,self.database) != self.record['baseline']:
            raise RuntimeError('Bridge view results differ from baseline')
        self.record['phases'].append('bridge')
        self.save()

    def metadata(self):
        if 'metadata' in self.record['phases']:
            return
        if len(self.record['copied']) != len(self.record['entries']) or 'bridge' not in self.record['phases']:
            raise RuntimeError('Copy verification and bridge views are required first')
        self.assert_originals_unchanged()
        if warehouse_snapshot(self.cur,self.database) != self.record['baseline']:
            raise RuntimeError('Data changed before metadata cutover')
        self.cur.execute('BEGIN')
        try:
            self.cur.execute(f'''UPDATE {self.database}.RAW.REFERENCE_RAW
                SET SOURCE={canonical_source_sql('SOURCE')}
                WHERE SOURCE IN ('massive_ticker_catalog','massive_ticker_overview')''')
            expression = 'S3_KEY'
            for old,new in [(LEGACY_REFERENCE_PREFIX,REFERENCE_PREFIX),*SOURCE_ALIASES.items()]:
                expression = f"REPLACE({expression},'{old}','{new}')"
            self.cur.execute(f'''UPDATE {self.database}.ADMIN.REFERENCE_MANIFEST
                SET SOURCE={canonical_source_sql('SOURCE')},S3_KEY={expression}
                WHERE SOURCE IN ('massive_ticker_catalog','massive_ticker_overview')''')
            if warehouse_snapshot(self.cur,self.database) != self.record['baseline']:
                raise RuntimeError('Metadata cutover changed data beyond names')
            self.cur.execute('COMMIT')
        except Exception:
            self.cur.execute('ROLLBACK')
            raise
        self.record['phases'].append('metadata')
        self.save()

    def verify(self):
        from types import SimpleNamespace
        from src.reference_load import ReferenceWarehouse
        from scripts.verify_infrastructure_access import verify_dashboard
        ReferenceWarehouse(SimpleNamespace(cursor=self.cur)).check_infrastructure()
        if warehouse_snapshot(self.cur,self.database) != self.record['baseline']:
            raise RuntimeError('Warehouse contents changed beyond the naming migration')
        self.cur.execute(f"SELECT COUNT(*) FROM {self.database}.RAW.REFERENCE_RAW WHERE SOURCE LIKE 'massive_%'")
        if self.cur.fetchone()[0]:
            raise RuntimeError('Legacy source labels remain active')
        manifests = query(self.cur,f'SELECT SOURCE,S3_KEY,SHA256 FROM {self.database}.ADMIN.REFERENCE_MANIFEST')
        expected = {(SOURCE_ALIASES[e['source']],e['new_key'],e['sha256']) for e in self.record['entries']}
        if {(r['source'],r['s3_key'],r['sha256']) for r in manifests} != expected:
            raise RuntimeError('Manifest mappings differ from verified archives')
        for view,saved in self.record['views'].items():
            if view_grants(self.cur,self.database,view) != saved['grants']:
                raise RuntimeError('Final view grants changed')
        for source in SOURCE_ALIASES.values():
            path = f"{source}/api_date=2025-12-31/"
            self.cur.execute(f'LIST @{self.database}.RAW.POLYGON_REFERENCE_STAGE/{path}')
            if not self.cur.fetchall():
                raise RuntimeError('Renamed stage cannot list ' + source)
            self.cur.execute(f'SELECT {canonical_source_sql("$1:SOURCE::STRING")} '
                             f'FROM @{self.database}.RAW.POLYGON_REFERENCE_STAGE/{path} LIMIT 1')
            if self.cur.fetchone()[0] != source:
                raise RuntimeError('Original archive replay source translation failed')
        self.record['dashboard'] = verify_dashboard()
        if self.record['dashboard']['status'] != 'passed':
            raise RuntimeError('Dashboard verification did not pass')
        self.record['phases'] = sorted(set(self.record['phases']) | {'verified'})
        self.save()

    def hide(self):
        if 'verified' not in self.record['phases']:
            raise RuntimeError('Final verification is required before hiding originals')
        if warehouse_snapshot(self.cur,self.database) != self.record['baseline']:
            raise RuntimeError('Warehouse changed before hiding originals')
        # Reconcile a deletion whose response was lost, without creating another marker.
        versions, markers = self.versions()
        originals = {(e['old_key'],e['version']) for e in self.record['entries']}
        if {(v['Key'],v['VersionId']) for v in versions} != originals:
            raise RuntimeError('Original versions changed before hide')
        by_marker = {m['Key']:m for m in markers if m['IsLatest']}
        for entry in self.record['entries']:
            marker = by_marker.get(entry['old_key'])
            if marker and not entry.get('delete_marker'):
                if not entry.get('hide_intent'):
                    raise RuntimeError('Unexplained delete marker')
                entry['delete_marker'] = marker['VersionId']
        self.save()
        self.assert_originals_unchanged()
        def hide_entry(entry):
            head = self.current_object(entry['old_key'])
            if not head or head.get('VersionId') != entry['version']:
                raise RuntimeError('Original changed before hiding: ' + entry['old_key'])
            self.verify_entry(entry)
            response = self.s3.delete_object(Bucket=self.bucket, Key=entry['old_key'])
            if not response.get('DeleteMarker') or not response.get('VersionId'):
                raise RuntimeError('Expected a recoverable S3 delete marker')
            return response['VersionId']
        remaining = [e for e in self.record['entries'] if not e.get('delete_marker')]
        for start in range(0,len(remaining),32):
            batch = remaining[start:start+32]
            for entry in batch:
                entry['hide_intent'] = True
            self.save()
            failures = []
            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = {pool.submit(hide_entry,e):e for e in batch}
                for future in as_completed(futures):
                    entry = futures[future]
                    try:
                        entry['delete_marker'] = future.result()
                        self.record['hidden'].append(entry['old_key'])
                        self.save()
                    except Exception as exc:
                        failures.append(exc)
            if failures:
                raise failures[0]
            print(f'Original paths hidden: {sum(bool(e.get("delete_marker")) for e in self.record["entries"])}/{len(self.record["entries"])}',flush=True)
        self.assert_originals_unchanged()
        if self.s3.list_objects_v2(Bucket=self.bucket,Prefix='raw/massive/',MaxKeys=1).get('Contents'):
            raise RuntimeError('Old paths remain visible')
        self.record['phases'] = sorted(set(self.record['phases']) | {'hidden'})
        self.save()

    def summary(self):
        return dict(bucket=self.bucket, archives=len(self.record['entries']),
            bytes=sum(e['size'] for e in self.record['entries']),
            verified_copies=len(self.record['copied']),
            hidden=sum(bool(e.get('delete_marker')) for e in self.record['entries']),
            phases=self.record['phases'])


def main():
    import boto3
    from scripts.inspect_infrastructure import snowflake_connection
    from src.config import AWS,SNOWFLAKE,PROJECT_ROOT
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase',choices=['preview','copy','bridge','metadata','cache','verify','hide'],default='preview')
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--idle-confirmed',action='store_true')
    parser.add_argument('--aws-profile',default='synthea-cdc')
    parser.add_argument('--journal',type=Path,default=Path('.cache/polygon-naming/migration.json'))
    args=parser.parse_args()
    if args.phase!='preview' and not (args.execute and args.idle_confirmed):
        parser.error('Mutating phases require --execute --idle-confirmed; keep all ingestion stopped.')
    c=snowflake_connection()
    try:
        cur=c.cursor()
        cur.execute("ALTER SESSION SET QUERY_TAG='stock_market_polygon_naming_migration'")
        s3=boto3.Session(profile_name=args.aws_profile,region_name=AWS['region']).client('s3')
        migration=PolygonMigration(s3,cur,AWS['s3_bucket'],SNOWFLAKE['database'],args.journal)
        if args.phase=='preview':
            print(json.dumps(migration.inventory(),indent=2))
            return
        if not migration.record:
            migration.inventory()
            migration.save()
        if args.phase=='cache':
            results={}
            for directory in [PROJECT_ROOT/'.cache',PROJECT_ROOT/'logs/.cache',PROJECT_ROOT/'airflow/logs/.cache']:
                old=directory/'massive-reference.sqlite3'
                if old.is_file():
                    results[str(old)]=preserve_cache(old,directory/'polygon-reference.sqlite3')
            migration.record['caches']=results
            migration.save()
        else:
            getattr(migration,args.phase)()
        print(json.dumps(migration.summary(),indent=2))
    finally:
        c.close()


if __name__=='__main__':
    main()
