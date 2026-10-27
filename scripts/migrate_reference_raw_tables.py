"""Split reference landing without extraction or rebuilding analytical tables.

Default preview is read-only. Execution requires stopped writers and a private
journal. DDL phases are separately committed; reruns verify actual object state.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import uuid

from jinja2 import Environment, StrictUndefined

from scripts.inspect_infrastructure import query, snowflake_connection
from scripts.migrate_ingestion_metadata import save_journal, table_snapshot
from scripts.verify_infrastructure_access import verify_dashboard
from src.ingestion_metadata import validate_database
from src.raw_reference import COLUMNS, REFERENCE_TABLES, RETIRED_MARKER, reference_table

ROOT = Path(__file__).resolve().parents[1]
VIEWS = {'STG_MARKET__CATALOG': 'polygon_ticker_catalog',
         'STG_MARKET__ISSUER_OBSERVATIONS': 'polygon_ticker_overview'}
MIGRATION = 'raw-reference-split-v1'


def serial(value):
    return json.loads(json.dumps(value, default=str))


def fingerprint(cur, relation, where=''):
    cur.execute(f'SELECT COUNT(*), HASH_AGG(*) FROM {relation} {where}')
    count, digest = cur.fetchone()
    return {'rows': count, 'hash': str(digest)}


def destination_snapshot(cur, database, name):
    snapshot = table_snapshot(cur, database, 'RAW', name)
    if snapshot is not None:
        comment = query(cur, f"SELECT COMMENT FROM {database}.INFORMATION_SCHEMA.TABLES "
                        f"WHERE TABLE_SCHEMA='RAW' AND TABLE_NAME='{name}'")[0]['comment']
        snapshot['comment'] = comment
    return snapshot


def source_snapshot(cur, database):
    result = destination_snapshot(cur, database, 'REFERENCE_RAW')
    if result is None:
        raise RuntimeError('Existing RAW.REFERENCE_RAW is required for this migration.')
    sources = query(cur, f'SELECT SOURCE,COUNT(*) AS RECORD_COUNT FROM {database}.RAW.REFERENCE_RAW GROUP BY SOURCE')
    if any(r['source'] not in REFERENCE_TABLES for r in sources):
        raise RuntimeError('Unexpected or NULL source label; no records may be omitted.')
    result['sources'] = sorted(sources, key=lambda r: r['source'])
    result['dates'] = serial(query(cur, f'''SELECT SOURCE,API_DATE,COUNT(*) AS RECORD_COUNT,HASH_AGG(*) AS HASH
        FROM {database}.RAW.REFERENCE_RAW GROUP BY SOURCE,API_DATE ORDER BY SOURCE,API_DATE'''))
    return result


def render_views(database):
    env = Environment(undefined=StrictUndefined)
    result = {}
    for name in VIEWS:
        path = ROOT / 'dbt/stock_analytics/models/staging' / (name.lower()+'.sql')
        sql = env.from_string(path.read_text()).render(
            config=lambda **kwargs: '', source=lambda source, table: database+'.RAW.'+table)
        result[name] = f'CREATE OR REPLACE VIEW {database}.STAGING.{name} COPY GRANTS AS\n{sql}'
    return result


def protected_snapshot(cur, database):
    relations = query(cur, f'''SELECT TABLE_SCHEMA,TABLE_NAME FROM {database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA IN ('RAW','ADMIN','STAGING','INTERMEDIATE','MARTS')
        AND TABLE_NAME NOT LIKE 'TMP_%'
        AND NOT (TABLE_SCHEMA='RAW' AND TABLE_NAME IN ('TICKER_CATALOG_RAW','COMPANY_OVERVIEW_RAW'))
        ORDER BY TABLE_SCHEMA,TABLE_NAME''')
    historical = query(cur, f'''SELECT TABLE_SCHEMA,TABLE_NAME FROM {database}.INFORMATION_SCHEMA.VIEWS
        WHERE TABLE_SCHEMA LIKE 'BACKUP_%' AND VIEW_DEFINITION ILIKE '%REFERENCE_RAW%'
        ORDER BY TABLE_SCHEMA,TABLE_NAME''')
    return {r['table_schema']+'.'+r['table_name']:
            fingerprint(cur, database+'.'+r['table_schema']+'.'+r['table_name'])
            for r in relations+historical}


def view_baseline(cur, database):
    result = {}
    for name in VIEWS:
        full = database+'.STAGING.'+name
        cur.execute('SELECT GET_DDL(%s,%s)', ('VIEW', full))
        ddl = cur.fetchone()[0]
        grants = query(cur, 'SHOW GRANTS ON VIEW '+full)
        result[name] = {'ddl': ddl, 'grants': normalized_grants(grants)}
    return result


def normalized_grants(rows):
    return sorted([{k:r[k] for k in ('privilege','granted_to','grantee_name','grant_option')}
                   for r in rows], key=lambda r: json.dumps(r, sort_keys=True))


def exact_equal(cur, database, source):
    """Multiset equality: compares all five values and duplicate multiplicities."""
    old = database+'.RAW.REFERENCE_RAW'
    new = reference_table(database,source)
    cur.execute(f'''WITH old_rows AS (
        SELECT {COLUMNS},COUNT(*) AS MULTIPLICITY FROM {old} WHERE SOURCE=%s GROUP BY {COLUMNS}
    ), new_rows AS (
        SELECT {COLUMNS},COUNT(*) AS MULTIPLICITY FROM {new} GROUP BY {COLUMNS}
    ), missing AS (SELECT * FROM old_rows MINUS SELECT * FROM new_rows),
    extra AS (SELECT * FROM new_rows MINUS SELECT * FROM old_rows)
    SELECT (SELECT COUNT(*) FROM missing)+(SELECT COUNT(*) FROM extra)''', (source,))
    if cur.fetchone()[0] != 0:
        raise RuntimeError('Exact record comparison failed for '+new)


class SplitMigration:
    def __init__(self, cur, database, journal_path):
        self.cur = cur
        self.database = validate_database(database)
        self.path = Path(journal_path)
        self.record = json.loads(self.path.read_text()) if self.path.exists() else None

    def save(self):
        save_journal(self.path, self.record)

    def identity(self):
        return query(self.cur, 'SELECT CURRENT_ACCOUNT() AS ACCOUNT,CURRENT_ROLE() AS ROLE')[0]

    def preview(self):
        identity = self.identity()
        original = source_snapshot(self.cur,self.database)
        destinations = {s:destination_snapshot(self.cur,self.database,n) for s,n in REFERENCE_TABLES.items()}
        if identity['role'] != original['owner']:
            raise RuntimeError('Execute as the existing owner; do not transfer ownership.')
        # Check existing schema access and dependent objects without changing them.
        schema = query(self.cur, f'SHOW GRANTS ON SCHEMA {self.database}.RAW')
        dependencies = query(self.cur, f'''SELECT TABLE_SCHEMA,TABLE_NAME
            FROM {self.database}.INFORMATION_SCHEMA.VIEWS
            WHERE VIEW_DEFINITION ILIKE '%REFERENCE_RAW%' ORDER BY TABLE_SCHEMA,TABLE_NAME''')
        if self.record:
            if (self.record['migration'] != MIGRATION or self.record['database'] != self.database
                    or self.record['identity'] != identity):
                raise RuntimeError('Journal identity mismatch.')
        elif any(destinations.values()) or original['comment'] == RETIRED_MARKER:
            raise RuntimeError('Destination or retirement marker exists without this migration journal.')
        return dict(identity=identity, original=original, destinations=destinations,
                    schema_grants=normalized_grants(schema), dependencies=dependencies)

    def assert_original(self):
        current = source_snapshot(self.cur,self.database)
        expected = dict(self.record['original'])
        if current['comment'] == RETIRED_MARKER:
            current['comment'] = expected['comment']
        if current != expected:
            raise RuntimeError('Original table changed; keep writers stopped and investigate.')

    def check_destination(self, source, allow_missing=False):
        name = REFERENCE_TABLES[source]
        current = destination_snapshot(self.cur,self.database,name)
        if current is None:
            if allow_missing:
                return None
            raise RuntimeError('Migration destination missing: '+name)
        if current['comment'] != self.record['destination_marker']:
            raise RuntimeError('Destination is not owned by this journal: '+name)
        for field in ('owner','columns'):
            if current[field] != self.record['original'][field]:
                raise RuntimeError('Destination definition/owner differs: '+name)
        # COPY GRANTS records a new grantor/time, but privileges must stay identical.
        if normalized_grants(current['grants']) != normalized_grants(self.record['original']['grants']):
            raise RuntimeError('Destination grants differ: '+name)
        return current

    def verify_data(self):
        self.assert_original()
        for source in REFERENCE_TABLES:
            self.check_destination(source)
            exact_equal(self.cur,self.database,source)
            actual = serial(query(self.cur, f'''SELECT SOURCE,API_DATE,COUNT(*) AS RECORD_COUNT,HASH_AGG(*) AS HASH
                FROM {reference_table(self.database,source)} GROUP BY SOURCE,API_DATE ORDER BY SOURCE,API_DATE'''))
            expected = [r for r in self.record['original']['dates'] if r['source']==source]
            if actual != expected:
                raise RuntimeError('Per-date verification differs: '+source)

    def verify_views(self):
        for name,ddl in render_views(self.database).items():
            full=self.database+'.STAGING.'+name
            self.cur.execute('SELECT GET_DDL(%s,%s)',('VIEW',full))
            actual=self.cur.fetchone()[0]
            body=ddl.split(' AS\n',1)[1].strip().rstrip(';')
            if body not in actual:
                raise RuntimeError('Staging view does not use the approved definition: '+full)
            if normalized_grants(query(self.cur,'SHOW GRANTS ON VIEW '+full)) != self.record['views'][name]['grants']:
                raise RuntimeError('Staging view grants differ: '+full)

    def run(self, phase, *, execute=False, idle_confirmed=False):
        preview = self.preview()
        if not execute:
            return {'phase':'preview', **preview}
        if not idle_confirmed:
            raise RuntimeError('Execution requires stopped writers and --idle-confirmed.')
        definitions = render_views(self.database)
        code_hash = hashlib.sha256(json.dumps(definitions,sort_keys=True).encode()).hexdigest()
        if self.record is None:
            if phase != 'prepare':
                raise RuntimeError('Run prepare first.')
            self.record = dict(migration=MIGRATION, database=self.database, **preview,
                started_at=datetime.now(timezone.utc).isoformat(),
                destination_marker=MIGRATION+': '+uuid.uuid4().hex,
                protected=protected_snapshot(self.cur,self.database),
                views=view_baseline(self.cur,self.database),code_hash=code_hash,completed=[])
            dashboard=verify_dashboard()
            if dashboard.get('status') != 'passed':
                raise RuntimeError('Dashboard baseline access failed.')
            self.record['dashboard_before'] = dashboard
            self.save()
        if self.record['code_hash'] != code_hash:
            raise RuntimeError('Staging definitions changed since prepare.')
        self.assert_original()
        if phase == 'prepare':
            for source in REFERENCE_TABLES:
                if self.check_destination(source,allow_missing=True) is None:
                    self.cur.execute(f'''CREATE TABLE {reference_table(self.database,source)}
                        LIKE {self.database}.RAW.REFERENCE_RAW COPY GRANTS COMMENT=%s''',
                        (self.record['destination_marker'],))
                self.check_destination(source)
            self.complete('prepare')
        elif phase == 'populate':
            self.require('prepare')
            destinations=[self.check_destination(s) for s in REFERENCE_TABLES]
            if any(d['rows'] for d in destinations) or 'populate' in self.record['completed']:
                self.verify_data()  # A committed transaction may precede its journal update.
            else:
                try:
                    self.cur.execute('BEGIN')
                    for source in REFERENCE_TABLES:
                        self.cur.execute(f'''INSERT INTO {reference_table(self.database,source)} ({COLUMNS})
                            SELECT {COLUMNS} FROM {self.database}.RAW.REFERENCE_RAW WHERE SOURCE=%s''',(source,))
                    self.verify_data()
                    self.cur.execute('COMMIT')
                except BaseException:
                    self.cur.execute('ROLLBACK')
                    raise
            self.complete('populate')
        elif phase == 'cutover':
            self.require('populate')
            self.verify_data()
            for name,ddl in definitions.items():
                full = self.database+'.STAGING.'+name
                current = query(self.cur, 'SHOW GRANTS ON VIEW '+full)
                if normalized_grants(current) != self.record['views'][name]['grants']:
                    raise RuntimeError('View grants changed before cutover.')
                owners=[r['grantee_name'] for r in current if r['privilege']=='OWNERSHIP']
                if owners != [self.record['identity']['role']]:
                    raise RuntimeError('Cutover must preserve the existing view owner: '+full)
                self.cur.execute('SELECT GET_DDL(%s,%s)', ('VIEW',full))
                actual_ddl=self.cur.fetchone()[0]
                # A crash can follow CREATE before the phase is journaled.
                # Recognize the approved body and skip an already switched view.
                old=self.record['views'][name]['ddl']
                intended_body=ddl.split(' AS\n',1)[1].strip().rstrip(';')
                if actual_ddl != old and intended_body not in actual_ddl:
                    raise RuntimeError('Unexpected view definition; do not overwrite '+full)
                if actual_ddl == old:
                    self.cur.execute(ddl)
                if normalized_grants(query(self.cur,'SHOW GRANTS ON VIEW '+full)) != self.record['views'][name]['grants']:
                    raise RuntimeError('View privileges changed after cutover.')
            self.verify_views()
            self.verify_protected()
            self.complete('cutover')
        elif phase in ('finalize','verify'):
            self.require('cutover')
            self.verify_data()
            self.verify_views()
            self.verify_protected()
            dashboard=verify_dashboard()
            if dashboard.get('status') != 'passed':
                raise RuntimeError('Dashboard access failed.')
            if phase == 'finalize':
                self.cur.execute(f'ALTER TABLE {self.database}.RAW.REFERENCE_RAW SET COMMENT=%s',(RETIRED_MARKER,))
                self.complete('finalize')
            elif source_snapshot(self.cur,self.database)['comment'] != RETIRED_MARKER:
                raise RuntimeError('Finalization marker missing.')
            self.record['dashboard_after']=dashboard
            self.record['verified_at']=datetime.now(timezone.utc).isoformat()
            self.save()
        else:
            raise ValueError('Unknown phase')
        return {'phase':phase,'completed':self.record['completed'],
                'datasets':self.record['original']['sources'],
                'protected_relations':len(self.record['protected'])}

    def verify_protected(self):
        if protected_snapshot(self.cur,self.database) != self.record['protected']:
            raise RuntimeError('Existing data or view results changed; investigate before activation.')

    def require(self, phase):
        if phase not in self.record['completed']:
            raise RuntimeError('Complete '+phase+' first.')

    def complete(self, phase):
        if phase not in self.record['completed']:
            self.record['completed'].append(phase)
        self.save()


def main():
    from src.config import SNOWFLAKE
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',default=SNOWFLAKE['database'])
    parser.add_argument('--phase',choices=['prepare','populate','cutover','finalize','verify'],default='prepare')
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--idle-confirmed',action='store_true')
    parser.add_argument('--journal',type=Path,default=Path('.cache/raw-reference-split/migration.json'))
    args=parser.parse_args()
    connection=snowflake_connection()
    try:
        connection.cursor().execute("ALTER SESSION SET QUERY_TAG='stock_market_raw_reference_split'")
        result=SplitMigration(connection.cursor(),args.database,args.journal).run(
            args.phase,execute=args.execute,idle_confirmed=args.idle_confirmed)
        if not args.execute:
            result={k:result[k] for k in ('phase','identity','dependencies')}
        print(json.dumps(result,indent=2,default=str))
    finally:
        connection.close()


if __name__=='__main__':
    main()
