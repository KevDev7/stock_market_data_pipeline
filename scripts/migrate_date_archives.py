"""Adopt one current archive per dataset/date, preserving every previous download.

Preview is read-only. Execution requires stopped writers and an explicit phase.
All originals remain versioned; only recoverable delete markers hide run paths.
"""
import argparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re

from scripts.inspect_infrastructure import query, snowflake_connection
from scripts.migrate_polygon_naming import PolygonMigration, fingerprint
from scripts.migrate_s3_layout import TRACKING, tracking_structure
from scripts.verify_infrastructure_access import verify_archives, verify_dashboard
from src.archive_layout import REFERENCE_ARCHIVES
from src.provider_names import canonical_source_sql

PREFIXES = ('daily-stock-prices/', 'ticker-catalog/', 'company-overview/')
KEY = re.compile(r'^(daily-stock-prices|ticker-catalog|company-overview)/api_date=(\d{4}-\d{2}-\d{2})/run_id=([A-Za-z0-9_-]+)/([a-z_]+\.ndjson\.gz)$')


def date_key(key):
    match = KEY.fullmatch(key)
    if not match:
        raise ValueError('Unexpected run archive path: ' + key)
    prefix, day, _, filename = match.groups()
    date.fromisoformat(day)
    expected = 'daily_stocks_raw.ndjson.gz' if prefix == 'daily-stock-prices' else 'reference_raw.ndjson.gz'
    if filename != expected:
        raise ValueError('Unexpected archive filename')
    return f'{prefix}/api_date={day}/{filename}'


def normalized_path_sql():
    return "REGEXP_REPLACE(S3_KEY, '/run_id=[A-Za-z0-9_-]+/', '/')"


def choose_current(entries, prices, manifests):
    """Choose the warehouse's retained run, never an arbitrary last copy."""
    groups = defaultdict(list)
    for entry in entries:
        groups[entry['new_key']].append(entry['old_key'])
    selected = {}
    for row in prices:
        day, run = str(row['api_date']), row['run_id']
        key = f'daily-stock-prices/api_date={day}/daily_stocks_raw.ndjson.gz'
        candidates = groups.get(key, [])
        preferred = f'daily-stock-prices/api_date={day}/run_id={run}/daily_stocks_raw.ndjson.gz'
        if key in selected or not candidates:
            raise RuntimeError('Missing archives or multiple warehouse runs for ' + day)
        if preferred in candidates:
            selected[key] = preferred
        elif len(candidates) == 1:
            # Legacy exports retained the original RUN_ID inside the file.
            # Full stage/RAW fingerprints must still match before cutover.
            selected[key] = candidates[0]
        else:
            raise RuntimeError('Cannot determine current price archive for ' + day)
    for row in manifests:
        old = row['s3_key']
        key = date_key(old)
        if key in selected or old not in groups.get(key, []):
            raise RuntimeError('Reference manifest conflicts with archive inventory')
        selected[key] = old
    if set(selected) != set(groups):
        raise RuntimeError('Archive dates and warehouse dates do not match')
    return selected


class DateArchiveMigration(PolygonMigration):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.record and self.record.get('migration') != 'date-only-v1':
            raise RuntimeError('Wrong migration journal')

    def all_versions(self):
        versions, markers = [], []
        for prefix in PREFIXES:
            for page in self.s3.get_paginator('list_object_versions').paginate(Bucket=self.bucket, Prefix=prefix):
                versions.extend(page.get('Versions', []))
                markers.extend(page.get('DeleteMarkers', []))
        return versions, markers

    def versions(self):
        versions, markers = self.all_versions()
        return ([v for v in versions if KEY.fullmatch(v['Key'])],
                [m for m in markers if KEY.fullmatch(m['Key'])])

    def snapshot(self):
        result = {}
        for table in TRACKING:
            relation = f'{self.database}.ADMIN.{table}'
            columns = query(self.cur, 'DESC TABLE ' + relation)
            projection = ','.join(normalized_path_sql() if c['name'] == 'S3_KEY'
                                  else '"' + c['name'] + '"' for c in columns)
            result['ADMIN.' + table] = fingerprint(self.cur, relation, projection)
        for row in query(self.cur, f"SELECT TABLE_SCHEMA,TABLE_NAME FROM {self.database}.INFORMATION_SCHEMA.TABLES "
                         "WHERE TABLE_SCHEMA IN ('RAW','STAGING','INTERMEDIATE','MARTS') "
                         "AND TABLE_NAME NOT LIKE 'TMP_%' ORDER BY 1,2"):
            name = row['table_schema'] + '.' + row['table_name']
            result[name] = fingerprint(self.cur, self.database + '.' + name)
        return result

    def inventory(self):
        if self.record:
            return self.summary()
        if self.s3.get_bucket_versioning(Bucket=self.bucket).get('Status') != 'Enabled':
            raise RuntimeError('S3 versioning must be enabled')
        versions, markers = self.all_versions()
        if not versions or markers or any(not v['IsLatest'] or not KEY.fullmatch(v['Key']) for v in versions):
            raise RuntimeError('Unexpected destination or source history; inspect before migration')
        def inspect(v):
            head = self.s3.head_object(Bucket=self.bucket, Key=v['Key'], VersionId=v['VersionId'])
            sha = head.get('Metadata', {}).get('sha256')
            if not sha:
                raise RuntimeError('Missing original checksum')
            return dict(old_key=v['Key'], new_key=date_key(v['Key']), version=v['VersionId'],
                        etag=v['ETag'], size=v['Size'], sha256=sha)
        with ThreadPoolExecutor(max_workers=8) as pool:
            entries = list(pool.map(inspect,versions))
        prices = query(self.cur, f'SELECT API_DATE,RUN_ID,COUNT(*) AS N FROM {self.database}.RAW.DAILY_STOCKS_RAW GROUP BY 1,2')
        manifests = query(self.cur, f'SELECT * FROM {self.database}.ADMIN.REFERENCE_MANIFEST')
        selected = choose_current(entries, prices, manifests)
        refs = {e['old_key']:e['sha256'] for e in entries}
        for table in TRACKING:
            checksum = 'S3_SHA256' if table == 'INGESTION_CHECKPOINTS' else 'SHA256'
            for row in query(self.cur, f'SELECT S3_KEY,{checksum} AS SHA FROM {self.database}.ADMIN.{table} WHERE S3_KEY IS NOT NULL'):
                if refs.get(row['s3_key']) != row['sha']:
                    raise RuntimeError('Tracking checksum does not match an original archive')
        self.cur.execute('SELECT CURRENT_ACCOUNT()')
        self.record = dict(migration='date-only-v1', bucket=self.bucket, database=self.database,
            account=self.cur.fetchone()[0], started_at=datetime.now(timezone.utc).isoformat(),
            entries=entries, selected=selected, copied=[], hidden=[], phases=[], baseline=self.snapshot(),
            tracking_structure=tracking_structure(self.cur,self.database), dashboard_before=verify_dashboard())
        if self.record['dashboard_before']['status'] != 'passed':
            raise RuntimeError('Baseline dashboard read failed')
        return self.summary()

    def groups(self):
        groups = defaultdict(list)
        for entry in self.record['entries']:
            groups[entry['new_key']].append(entry)
        return [sorted(group, key=lambda e:(e['old_key'] == self.record['selected'][key], e['old_key']))
                for key,group in sorted(groups.items())]

    def verify_version(self, entry, version):
        old = self.s3.get_object(Bucket=self.bucket, Key=entry['old_key'], VersionId=entry['version'])
        new = self.s3.get_object(Bucket=self.bucket, Key=entry['new_key'], VersionId=version)
        for obj in (old, new):
            body = obj['Body'].read()
            if len(body) != entry['size'] or hashlib.sha256(body).hexdigest() != entry['sha256']:
                raise RuntimeError('Archive bytes differ: ' + entry['old_key'])
        for field in ('Metadata','ContentType','ContentEncoding','CacheControl','ContentDisposition',
                      'ContentLanguage','ServerSideEncryption','SSEKMSKeyId'):
            if old.get(field) != new.get(field):
                raise RuntimeError('Archive attributes differ: ' + field)
        tags = [self.s3.get_object_tagging(Bucket=self.bucket, Key=key, VersionId=v)['TagSet']
                for key,v in [(entry['old_key'],entry['version']), (entry['new_key'],version)]]
        if sorted(tags[0],key=lambda t:t['Key']) != sorted(tags[1],key=lambda t:t['Key']):
            raise RuntimeError('Archive tags differ')

    def copy_group(self, entries):
        key = entries[0]['new_key']
        versions = []
        for page in self.s3.get_paginator('list_object_versions').paginate(Bucket=self.bucket, Prefix=key):
            if any(m['Key']==key for m in page.get('DeleteMarkers', [])):
                raise RuntimeError('Unexpected destination delete marker')
            versions.extend(v for v in page.get('Versions', []) if v['Key']==key)
        # S3 returns versions newest first. Reconcile a copy whose response was lost.
        versions.reverse()
        if len(versions)>len(entries):
            raise RuntimeError('Unexpected extra destination versions')
        result = []
        for i, entry in enumerate(entries):
            if i < len(versions):
                version = versions[i]['VersionId']
                if entry.get('destination_version') not in (None,version):
                    raise RuntimeError('Destination version differs from journal')
            else:
                if entry.get('destination_version'):
                    raise RuntimeError('Previously copied version disappeared')
                response = self.s3.copy_object(Bucket=self.bucket, Key=key,
                    CopySource={'Bucket':self.bucket,'Key':entry['old_key'],'VersionId':entry['version']},
                    CopySourceIfMatch=entry['etag'], MetadataDirective='COPY', TaggingDirective='COPY')
                version = response.get('VersionId')
                if not version:
                    raise RuntimeError('Versioning is required for every copy')
            self.verify_version(entry, version)
            result.append((entry,version))
        return result

    def copy(self):
        self.assert_originals_unchanged()
        groups = self.groups()
        for offset in range(0,len(groups),16):
            batch = groups[offset:offset+16]
            with ThreadPoolExecutor(max_workers=8) as pool:
                results = list(pool.map(self.copy_group,batch))
            for group in results:
                for entry,version in group:
                    entry['destination_version'] = version
            self.record['copied'] = [e['old_key'] for e in self.record['entries'] if e.get('destination_version')]
            self.save()
            print(f'Verified archive versions: {len(self.record["copied"])}/{len(self.record["entries"])}',flush=True)

    def assert_destinations(self):
        versions, markers = self.all_versions()
        actual = {(v['Key'],v['VersionId']) for v in versions if not KEY.fullmatch(v['Key'])}
        expected = {(e['new_key'],e.get('destination_version')) for e in self.record['entries']}
        if actual != expected or any(not KEY.fullmatch(m['Key']) for m in markers):
            raise RuntimeError('Destination version history changed')
        current = {v['Key']:v['VersionId'] for v in versions if v['IsLatest'] and not KEY.fullmatch(v['Key'])}
        by_old = {e['old_key']:e for e in self.record['entries']}
        if current != {key:by_old[old]['destination_version'] for key,old in self.record['selected'].items()}:
            raise RuntimeError('Visible archive does not match selected warehouse run')

    def verify_stage_data(self):
        result = {}
        sources = [('polygon_grouped_daily','POLYGON_RAW_STAGE','DAILY_STOCKS_RAW'),
                   *[(source,stage,'REFERENCE_RAW') for source,(_,stage) in REFERENCE_ARCHIVES.items()]]
        projection = 'API_DATE,RUN_ID,SOURCE,RAW_PAYLOAD,INGESTED_AT'
        for source,stage,table in sources:
            # Retained January price exports carry a legacy source label. Their
            # rows must be compared too, without changing that historical label.
            where = '' if table == 'DAILY_STOCKS_RAW' else f" WHERE SOURCE='{source}'"
            raw = query(self.cur, f'SELECT API_DATE,COUNT(*) AS N,TO_VARCHAR(HASH_AGG({projection})) AS H '
                f"FROM {self.database}.RAW.{table}{where} GROUP BY 1")
            staged = query(self.cur, f'''SELECT API_DATE,COUNT(*) AS N,TO_VARCHAR(HASH_AGG({projection})) AS H FROM (
                SELECT TRY_TO_DATE($1:API_DATE::STRING) AS API_DATE,$1:RUN_ID::STRING AS RUN_ID,
                {canonical_source_sql('$1:SOURCE::STRING')} AS SOURCE,$1:RAW_PAYLOAD::STRING AS RAW_PAYLOAD,
                TRY_TO_TIMESTAMP_NTZ($1:INGESTED_AT::STRING) AS INGESTED_AT
                FROM @{self.database}.RAW.{stage} (PATTERN => '.*api_date=[0-9-]+/[a-z_]+[.]ndjson[.]gz')
                ) GROUP BY 1''')
            normalize = lambda rows:{str(r['api_date']):(r['n'],r['h']) for r in rows}
            if normalize(raw) != normalize(staged):
                raise RuntimeError('Date-only archives differ from current RAW data: ' + source)
            result[source] = dict(dates=len(raw),rows=sum(r['n'] for r in raw),full_content_match=True)
        return result

    def metadata(self):
        if len(self.record['copied']) != len(self.record['entries']):
            raise RuntimeError('Copy every version before cutover')
        self.assert_originals_unchanged()
        self.assert_destinations()
        if self.snapshot() != self.record['baseline']:
            raise RuntimeError('Warehouse changed before cutover')
        self.record['stage_data'] = self.verify_stage_data()
        if 'metadata' not in self.record['phases']:
            self.cur.execute('BEGIN')
            try:
                for table in TRACKING:
                    self.cur.execute(f'UPDATE {self.database}.ADMIN.{table} SET S3_KEY={normalized_path_sql()} '
                                     "WHERE S3_KEY LIKE '%/run_id=%'")
                if self.snapshot() != self.record['baseline']:
                    raise RuntimeError('Unexpected warehouse change during cutover')
                self.cur.execute('COMMIT')
            except BaseException:
                self.cur.execute('ROLLBACK')
                raise
            self.record['phases'].append('metadata')
        self.save()

    def verify(self):
        if 'metadata' not in self.record['phases']:
            raise RuntimeError('Metadata cutover required')
        self.assert_originals_unchanged()
        self.assert_destinations()
        if self.snapshot()!=self.record['baseline'] or tracking_structure(self.cur,self.database)!=self.record['tracking_structure']:
            raise RuntimeError('Warehouse data, columns, or grants changed')
        self.record['stage_data'] = self.verify_stage_data()
        self.record['stage_reads'] = verify_archives(self.cur.connection)
        self.record['dashboard_after'] = verify_dashboard()
        if self.record['dashboard_after']['status'] != 'passed':
            raise RuntimeError('Dashboard verification failed')
        self.record['phases'] = sorted(set(self.record['phases']) | {'verified'})
        self.save()

    def hide(self):
        if 'verified' not in self.record['phases'] or self.snapshot()!=self.record['baseline']:
            raise RuntimeError('Verify unchanged warehouse before hiding run paths')
        self.assert_destinations()
        versions, markers = self.versions()
        if {(v['Key'],v['VersionId']) for v in versions} != self.original_version_ids():
            raise RuntimeError('Original versions changed')
        by_key = {e['old_key']:e for e in self.record['entries']}
        for marker in markers:
            entry = by_key[marker['Key']]
            if not marker['IsLatest'] or not entry.get('hide_intent'):
                raise RuntimeError('Unexpected delete marker')
            if entry.get('delete_marker') not in (None,marker['VersionId']):
                raise RuntimeError('Delete marker differs from journal')
            entry['delete_marker'] = marker['VersionId']
        self.save()
        self.assert_originals_unchanged()
        remaining = [e for e in self.record['entries'] if not e.get('delete_marker')]
        for offset in range(0,len(remaining),32):
            batch = remaining[offset:offset+32]
            for entry in batch:
                entry['hide_intent'] = True
            self.save()
            def hide_one(entry):
                head = self.current_object(entry['old_key'])
                if not head or head.get('VersionId') != entry['version']:
                    raise RuntimeError('Original changed before hiding')
                # The exact destination version was byte-verified during copying.
                self.s3.head_object(Bucket=self.bucket,Key=entry['new_key'],VersionId=entry['destination_version'])
                response = self.s3.delete_object(Bucket=self.bucket,Key=entry['old_key'])
                if not response.get('DeleteMarker') or not response.get('VersionId'):
                    raise RuntimeError('Expected recoverable delete marker')
                return entry,response['VersionId']
            with ThreadPoolExecutor(max_workers=8) as pool:
                for entry,marker in pool.map(hide_one,batch):
                    entry['delete_marker'] = marker
            self.save()
            print(f'Run paths hidden: {sum(bool(e.get("delete_marker")) for e in self.record["entries"])}',flush=True)
        self.assert_originals_unchanged()
        self.assert_destinations()
        visible = {o['Key'] for prefix in PREFIXES
                   for page in self.s3.get_paginator('list_objects_v2').paginate(Bucket=self.bucket,Prefix=prefix)
                   for o in page.get('Contents',[])}
        if visible != set(self.record['selected']):
            raise RuntimeError('Unexpected visible objects after migration')
        self.record['phases'] = sorted(set(self.record['phases']) | {'hidden'})
        self.save()

    def summary(self):
        result = super().summary()
        result['current_date_files'] = len(self.record['selected'])
        return result


def main():
    import boto3
    from src.config import AWS,SNOWFLAKE
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['preview','copy','metadata','verify','hide'], default='preview',nargs='?')
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--idle-confirmed',action='store_true')
    parser.add_argument('--aws-profile',default='synthea-cdc')
    parser.add_argument('--journal',type=Path,default=Path('.cache/date-layout/migration.json'))
    args = parser.parse_args()
    if args.phase!='preview' and not(args.execute and args.idle_confirmed):
        parser.error('Execution requires --execute --idle-confirmed; stop ingestion and refresh writers.')
    session = boto3.Session(profile_name=args.aws_profile,region_name=AWS['region'])
    if session.client('sts').get_caller_identity()['Account']!='645968351825':
        raise RuntimeError('Unexpected AWS account')
    c = snowflake_connection()
    try:
        cur=c.cursor()
        cur.execute("ALTER SESSION SET QUERY_TAG='stock_market_date_layout_migration'")
        m=DateArchiveMigration(session.client('s3'),cur,AWS['s3_bucket'],SNOWFLAKE['database'],args.journal)
        m.inventory()
        if args.phase!='preview':
            m.save()
            getattr(m,args.phase)()
        print(json.dumps(m.summary(),indent=2))
    finally:
        c.close()


if __name__=='__main__':
    main()
