"""Exercise the actual dbt incremental path in a uniquely owned Snowflake test DB.

Never writes to MARKET. Test databases are retained for subsequent scenarios
and must be explicitly cleaned up after evidence is collected.
"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

from src.snowflake_client import SnowflakeClient

ROOT=Path(__file__).resolve().parents[1]
OWNER_MARKER='stock_market_pipeline_pre_push_incremental_test'
MODELS={
    'int_market__daily':'INTERMEDIATE.INT_MARKET__DAILY',
    'calc_security_daily_momentum':'MARTS.CALC_SECURITY_DAILY_MOMENTUM',
    'fct_security_daily_momentum':'MARTS.FCT_SECURITY_DAILY_MOMENTUM',
}
APPROXIMATE={'AVG_GAIN_14','AVG_LOSS_14','RSI','REL_VOL'}


def assert_owned(cur,database):
    if not re.fullmatch(r'PREPUSH_TEST_[0-9A-F]{12}',database):
        raise ValueError('Only a uniquely named pre-push test database is allowed')
    cur.execute('SHOW DATABASES LIKE %s',(database,))
    columns=[c[0].lower() for c in cur.description]
    records=[dict(zip(columns,row)) for row in cur.fetchall()]
    if len(records)!=1 or records[0]['name']!=database or records[0]['comment']!=OWNER_MARKER:
        raise ValueError('Database is not owned by this test harness')


def run_dbt(database,models,full_refresh=False,action='run'):
    env=dict(os.environ,SNOWFLAKE_DATABASE=database,SNOWFLAKE_SCHEMA='RAW')
    cache=ROOT/'.cache'/'prepush'/database
    command=[sys.executable,'-m','scripts.run_reference_models',action,'--schema-prefix','',
             '--target-path',str(cache/'target'),'--log-path',str(cache/'logs')]
    if models:
        command.extend(['--select',*models])
    if full_refresh:
        command.append('--full-refresh')
    result=subprocess.run(command,cwd=ROOT,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
    if result.returncode:
        print(result.stdout,flush=True)
        raise RuntimeError('Isolated dbt run failed')
    print('Isolated dbt '+action+' passed: '+(', '.join(models) or 'all models and tests'),flush=True)


def snapshot(cur,database,label,replace=True):
    create='CREATE OR REPLACE' if replace else 'CREATE'
    exists='' if replace else 'IF NOT EXISTS '
    for model,table in MODELS.items():
        cur.execute(f'{create} TRANSIENT TABLE {exists}{database}.CHECKS.{label}_{model} CLONE {database}.{table}')


def compare(cur,database,label):
    results={}
    for model,table in MODELS.items():
        target=database+'.'+table
        expected=database+'.CHECKS.'+label+'_'+model
        cur.execute('DESC TABLE '+target)
        columns=[r[0] for r in cur.fetchall()]
        predicates=[]
        for name in columns:
            a='a."'+name+'"'; e='e."'+name+'"'
            equal=f'EQUAL_NULL({a},{e})'
            if name in APPROXIMATE:
                equal+=f' OR ({a} IS NOT NULL AND {e} IS NOT NULL AND ABS({a}-{e})<=GREATEST(1e-9,ABS({e})*1e-10))'
            predicates.append('NOT ('+equal+')')
        cur.execute(f'''SELECT COUNT(*) FROM {target} a FULL OUTER JOIN {expected} e
            ON a.SECURITY_KEY=e.SECURITY_KEY AND a.TRADE_DATE=e.TRADE_DATE
            WHERE a.SECURITY_KEY IS NULL OR e.SECURITY_KEY IS NULL OR {' OR '.join(predicates)}''')
        differences=cur.fetchone()[0]
        cur.execute(f'SELECT COUNT(*),COUNT(DISTINCT SECURITY_KEY||TO_VARCHAR(TRADE_DATE)) FROM {target}')
        total,unique=cur.fetchone()
        results[model]={'rows':total,'duplicate_keys':total-unique,'different_rows':differences}
    print(json.dumps({'comparison':label,'models':results}),flush=True)
    if any(v['different_rows'] or v['duplicate_keys'] for v in results.values()):
        raise AssertionError('Incremental results differ from the expected output')
    return results


def prepare(cur):
    database='PREPUSH_TEST_'+uuid.uuid4().hex[:12].upper()
    cur.execute(f"CREATE DATABASE {database} COMMENT='{OWNER_MARKER}'")
    assert_owned(cur,database)
    print('Created isolated database '+database,flush=True)
    for schema in ['RAW','INTERMEDIATE','MARTS']:
        cur.execute(f'CREATE SCHEMA {database}.{schema} CLONE MARKET.{schema}')
    cur.execute(f"SHOW TABLES LIKE 'CALC_SECURITY_DAILY_MOMENTUM' IN SCHEMA {database}.MARTS")
    if not cur.fetchall():
        # Pre-cutover verification starts from the existing five-layer warehouse.
        cur.execute(f'''CREATE TRANSIENT TABLE {database}.MARTS.CALC_SECURITY_DAILY_MOMENTUM
            CLONE MARKET.MART_STAGING.PREP_SECURITY_DAILY_MOMENTUM''')
    cur.execute(f'CREATE SCHEMA {database}.STAGING')
    cur.execute(f'CREATE SCHEMA {database}.CHECKS')
    run_dbt(database,['stg_daily_stocks','stg_market__catalog','stg_market__issuer_observations'])
    return database


def verify(database=None,full_baseline=False):
    w=SnowflakeClient()
    try:
        database=database or prepare(w.cursor)
        assert_owned(w.cursor,database)
        if full_baseline:
            run_dbt(database,list(MODELS),full_refresh=True)
        # Reusing a failed test database must not overwrite its original baseline.
        snapshot(w.cursor,database,'BASELINE',replace=False)
        for attempt in range(2):
            run_dbt(database,list(MODELS))
            compare(w.cursor,database,'BASELINE')
            print('Unchanged-source incremental retry passed:',attempt+1,flush=True)
        print(json.dumps({'status':'retries_verified','test_database':database}),flush=True)
    finally:
        w.close()


def verify_rollback(database):
    w=SnowflakeClient()
    try:
        assert_owned(w.cursor,database)
        target=database+'.CHECKS.ROLLBACK_TARGET'
        incoming=database+'.CHECKS.ROLLBACK_INCOMING'
        w.cursor.execute(f'CREATE OR REPLACE TRANSIENT TABLE {target} CLONE {database}.INTERMEDIATE.INT_MARKET__DAILY')
        w.cursor.execute(f'CREATE OR REPLACE TRANSIENT TABLE {incoming} LIKE {target}')
        w.cursor.execute(f'ALTER TABLE {incoming} DROP COLUMN CLOSE')
        w.cursor.execute(f'SELECT COUNT(*),HASH_AGG(*) FROM {target}')
        before=w.cursor.fetchone()
        cache=ROOT/'.cache'/'prepush'/database/'rollback'
        env=dict(os.environ,SNOWFLAKE_DATABASE=database,SNOWFLAKE_SCHEMA='RAW')
        result=subprocess.run([sys.executable,'-m','scripts.run_reference_models',
            'run-operation','verify_incremental_rollback','--schema-prefix','',
            '--target-path',str(cache/'target'),'--log-path',str(cache/'logs')],
            cwd=ROOT,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
        session=re.search(r'ROLLBACK_TEST_SESSION=(\d+)',result.stdout)
        if result.returncode==0 or session is None or "invalid identifier 'CLOSE'" not in result.stdout:
            print(result.stdout,flush=True)
            raise AssertionError('Rollback probe did not fail at the intended INSERT')
        w.cursor.execute(f'''SELECT QUERY_ID,EXECUTION_STATUS
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION(
                SESSION_ID=>{session.group(1)},RESULT_LIMIT=>100))
            WHERE QUERY_TYPE='DELETE' AND QUERY_TEXT ILIKE '%ROLLBACK_TARGET%' ''')
        deletes=w.cursor.fetchall()
        if len(deletes)!=1 or deletes[0][1]!='SUCCESS':
            raise AssertionError('Could not confirm successful DELETE before INSERT failure')
        w.cursor.execute('SELECT * FROM TABLE(RESULT_SCAN(%s))',(deletes[0][0],))
        deleted_rows=w.cursor.fetchone()[0]
        if deleted_rows<=0:
            raise AssertionError('Rollback probe must delete a nonempty window')
        w.cursor.execute(f'SELECT COUNT(*),HASH_AGG(*) FROM {target}')
        after=w.cursor.fetchone()
        if before!=after:
            raise AssertionError('Failed INSERT left the destination changed')
        print(json.dumps({'rollback':'passed','rows_unchanged':after[0],
                          'rows_deleted_before_failure':deleted_rows}),flush=True)
    finally:
        w.close()


def verify_corrections(database):
    """Recent source corrections/removals/additions must reconcile to a full build."""
    w=SnowflakeClient()
    try:
        assert_owned(w.cursor,database)
        compare(w.cursor,database,'BASELINE')
        prices=database+'.RAW.DAILY_STOCKS_RAW'
        reference=database+'.RAW.REFERENCE_RAW'
        manifest=database+'.RAW.REFERENCE_MANIFEST'
        w.cursor.execute(f'''SELECT COUNT(*) FROM {reference}
            WHERE TRY_PARSE_JSON(RAW_PAYLOAD):ticker::STRING='ZZPREPUSH' ''')
        if w.cursor.fetchone()[0]:
            raise AssertionError('Correction fixture already exists; use a fresh test database')
        w.cursor.execute(f'''DELETE FROM {prices} WHERE API_DATE='2025-12-30'
            AND TRY_PARSE_JSON(RAW_PAYLOAD):T::STRING='AAPL' ''')
        if w.cursor.rowcount<=0:
            raise AssertionError('Expected source price to remove is absent')
        w.cursor.execute(f'''SELECT TRY_PARSE_JSON(RAW_PAYLOAD):c::DOUBLE,
            TRY_PARSE_JSON(RAW_PAYLOAD):h::DOUBLE,TRY_PARSE_JSON(RAW_PAYLOAD):l::DOUBLE
            FROM {prices} WHERE API_DATE='2025-12-30'
            AND TRY_PARSE_JSON(RAW_PAYLOAD):T::STRING='MSFT' LIMIT 1''')
        old_close,high,low=w.cursor.fetchone()
        corrected_close=high if high!=old_close else low
        if corrected_close==old_close:
            raise AssertionError('Correction fixture must change the price')
        w.cursor.execute(f'''UPDATE {prices} SET RAW_PAYLOAD=TO_JSON(OBJECT_INSERT(
            TRY_PARSE_JSON(RAW_PAYLOAD),'c',TO_VARIANT(%s::DOUBLE),TRUE))
            WHERE API_DATE='2025-12-30' AND TRY_PARSE_JSON(RAW_PAYLOAD):T::STRING='MSFT' ''',
            (corrected_close,))
        catalog={'ticker':'ZZPREPUSH','name':'Isolated regression fixture','type':'CS',
                 'locale':'us','active':True,'primary_exchange':'XNAS','currency_name':'usd',
                 'share_class_figi':'PREPUSH_SHARE_TEST','cik':'PREPUSH-CIK'}
        overview={'ticker':'ZZPREPUSH','name':catalog['name'],'cik':catalog['cik'],
                  'sic_code':'3571','sic_description':'Fixture industry'}
        for source,payload in [('massive_ticker_catalog',catalog),('massive_ticker_overview',overview)]:
            w.cursor.execute(f'''INSERT INTO {reference}
                (API_DATE,RUN_ID,SOURCE,RAW_PAYLOAD,INGESTED_AT)
                VALUES ('2025-12-31','prepush-fixture',%s,%s,CURRENT_TIMESTAMP())''',
                (source,json.dumps(payload)))
            w.cursor.execute(f'''UPDATE {manifest} SET ROW_COUNT=ROW_COUNT+1,
                REQUESTED_COUNT=REQUESTED_COUNT+1 WHERE API_DATE='2025-12-31' AND SOURCE=%s''',(source,))
            if w.cursor.rowcount!=1:
                raise AssertionError('Expected dated source manifest is absent')
        payload={'T':'ZZPREPUSH','v':1000,'vw':10.2,'o':10,'c':10.5,'h':11,'l':9,'n':10}
        w.cursor.execute(f'''INSERT INTO {prices} (API_DATE,RUN_ID,SOURCE,RAW_PAYLOAD,INGESTED_AT)
            VALUES ('2025-12-31','prepush-fixture','test',%s,CURRENT_TIMESTAMP())''',(json.dumps(payload),))
        run_dbt(database,['int_market__reference_daily'])
        run_dbt(database,list(MODELS))
        target=database+'.INTERMEDIATE.INT_MARKET__DAILY'
        w.cursor.execute(f"SELECT COUNT(*) FROM {target} WHERE TICKER='AAPL' AND TRADE_DATE='2025-12-30'")
        if w.cursor.fetchone()[0]!=0:
            raise AssertionError('Removed source record remains in accepted history')
        w.cursor.execute(f"SELECT CLOSE FROM {target} WHERE TICKER='MSFT' AND TRADE_DATE='2025-12-30'")
        if w.cursor.fetchone()[0]!=corrected_close:
            raise AssertionError('Corrected price did not reach accepted history')
        w.cursor.execute(f"SELECT YESTERDAY_CLOSE,OBSERVATION_COUNT,IS_FIRST_OBSERVATION FROM {target} WHERE TICKER='ZZPREPUSH'")
        if w.cursor.fetchall()!=[(None,1,1)]:
            raise AssertionError('New security inherited another security history')
        snapshot(w.cursor,database,'CORRECTED_INCREMENTAL')
        run_dbt(database,list(MODELS),full_refresh=True)
        compare(w.cursor,database,'CORRECTED_INCREMENTAL')
        run_dbt(database,list(MODELS))
        compare(w.cursor,database,'CORRECTED_INCREMENTAL')
        print(json.dumps({'corrections_removals_new_security_and_retry':'passed',
                          'msft_corrected_close':corrected_close}),flush=True)
    finally:
        w.close()


def cleanup(database):
    w=SnowflakeClient()
    try:
        assert_owned(w.cursor,database)
        w.cursor.execute(f'DROP DATABASE {database}')
        print('Removed only isolated test database '+database,flush=True)
    finally:
        w.close()


def verify_empty_window(database):
    w=SnowflakeClient()
    try:
        assert_owned(w.cursor,database)
        w.cursor.execute(f'''DELETE FROM {database}.RAW.DAILY_STOCKS_RAW
            WHERE API_DATE BETWEEN '2025-12-27' AND '2025-12-31' ''')
        if w.cursor.rowcount<=0:
            raise AssertionError('Empty-window fixture must remove existing source rows')
        run_dbt(database,list(MODELS))
        for table in MODELS.values():
            w.cursor.execute(f"SELECT COUNT(*) FROM {database}.{table} WHERE TRADE_DATE>='2025-12-27'")
            if w.cursor.fetchone()[0]:
                raise AssertionError('Empty replacement left stale output rows')
        snapshot(w.cursor,database,'EMPTY_INCREMENTAL')
        run_dbt(database,list(MODELS),full_refresh=True)
        compare(w.cursor,database,'EMPTY_INCREMENTAL')
        print(json.dumps({'empty_replacement_window':'passed'}),flush=True)
    finally:
        w.close()


def verify_full_build(database):
    """Discard test fixtures in this owned DB, then validate every updated model."""
    w=SnowflakeClient()
    try:
        assert_owned(w.cursor,database)
        for table in ['DAILY_STOCKS_RAW','REFERENCE_RAW','REFERENCE_MANIFEST']:
            w.cursor.execute(f'CREATE OR REPLACE TABLE {database}.RAW.{table} CLONE MARKET.RAW.{table}')
        run_dbt(database,[],full_refresh=True,action='build')
        results=json.loads((ROOT/'.cache'/'prepush'/database/'target'/'run_results.json').read_text())['results']
        summary={'models':sum(r['unique_id'].startswith('model.') for r in results),
                 'tests':sum(r['unique_id'].startswith('test.') for r in results),
                 'errors_warnings_skips':sum(r['status'] not in ('success','pass') for r in results)}
        manifest=json.loads((ROOT/'.cache'/'prepush'/database/'target'/'manifest.json').read_text())
        expected={key for key,node in manifest['nodes'].items()
                  if node['resource_type'] in ('model','test') and node['config'].get('enabled',True)}
        passed={r['unique_id'] for r in results if r['status'] in ('success','pass')}
        if summary['models']!=15 or passed!=expected or summary['errors_warnings_skips']:
            raise AssertionError('Complete expected build/test coverage is required')
        columns="TABLE_NAME,COLUMN_NAME,DATA_TYPE,NUMERIC_PRECISION,NUMERIC_SCALE,ORDINAL_POSITION"
        snapshots=[]
        for db in ['MARKET',database]:
            w.cursor.execute(f'''SELECT {columns} FROM {db}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA='MARTS' AND SUBSTR(TABLE_NAME,1,4) IN ('FCT_','DIM_')
                ORDER BY TABLE_NAME,ORDINAL_POSITION''')
            snapshots.append(w.cursor.fetchall())
        if snapshots[0]!=snapshots[1]:
            raise AssertionError('The four-layer refactor changed the consumer column contract')
        print(json.dumps({'full_build':summary,'mart_column_contract_unchanged':True}),flush=True)
    finally:
        w.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database')
    parser.add_argument('--full-baseline',action='store_true',
                        help='Build the updated models fully in the test DB before incremental comparisons')
    parser.add_argument('--scenario',choices=['retries','corrections','empty','build','rollback','cleanup'],default='retries')
    args=parser.parse_args()
    if args.scenario in ('corrections','empty','build','rollback','cleanup'):
        if not args.database:
            parser.error('--database is required for this scenario')
        {'corrections':verify_corrections,'empty':verify_empty_window,'build':verify_full_build,
         'rollback':verify_rollback,'cleanup':cleanup}[args.scenario](args.database)
    else:
        verify(args.database,args.full_baseline)
