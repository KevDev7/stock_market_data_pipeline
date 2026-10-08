"""Recoverable schema cutover after a complete candidate dbt build and verification.

Preserves existing grants and old layer schemas. The final MARTS swap switches
the consumer dataset atomically. This does not deploy hosted dashboard code.
"""
import argparse
import json
from pathlib import Path
import re

from scripts.verify_reference_migration import verify
from scripts.verify_four_layer_refactor import compare
from src.snowflake_client import SnowflakeClient

ROOT=Path(__file__).resolve().parents[1]
PROJECT=ROOT/'dbt'/'stock_analytics'
LAYERS=['STAGING','INTERMEDIATE','MARTS']


def quoted(name):
    return '"'+name.replace('"','""')+'"'


def rows(cur,sql):
    cur.execute(sql)
    columns=[c[0].lower() for c in cur.description]
    return [dict(zip(columns,row)) for row in cur.fetchall()]


def mirror_grants(cur,old,new,objects):
    def grant(record,target,future=False):
        privilege=record['privilege']
        if privilege=='OWNERSHIP':
            return
        kind=record.get('granted_to',record.get('grant_to'))
        if kind!='ROLE' or not re.fullmatch(r'[A-Z_ ]+',privilege):
            raise ValueError('Grant requires explicit review before cutover')
        option=' WITH GRANT OPTION' if str(record['grant_option']).lower()=='true' else ''
        cur.execute(f'GRANT {privilege} ON {target} TO ROLE {quoted(record["grantee_name"])}{option}')
    for record in rows(cur,'SHOW GRANTS ON SCHEMA '+old):
        grant(record,'SCHEMA '+new)
    for record in rows(cur,'SHOW FUTURE GRANTS IN SCHEMA '+old):
        kind=record['grant_on']
        if kind not in ['TABLE','VIEW']:
            raise ValueError('Unsupported future grant requires review')
        grant(record,'FUTURE '+kind+'S IN SCHEMA '+new,True)
    for name,kind in objects:
        for record in rows(cur,f'SHOW GRANTS ON {kind} {old}.{quoted(name)}'):
            grant(record,f'{kind} {new}.{quoted(name)}')


def cutover(execute=False,candidate_prefix='CANDIDATE_',backup_suffix='20261007',artifacts_path=None,
            require_identical_marts=False,retire_mart_staging=False):
    if not re.fullmatch(r'[A-Z][A-Z0-9_]*_',candidate_prefix):
        raise ValueError('Invalid candidate schema prefix')
    if not re.fullmatch(r'[A-Z0-9_]+',backup_suffix):
        raise ValueError('Invalid backup suffix')
    artifacts=Path(artifacts_path) if artifacts_path else PROJECT/'target'
    # Require the last dbt invocation to cover every active model and test.
    manifest=json.loads((artifacts/'manifest.json').read_text())
    result=json.loads((artifacts/'run_results.json').read_text())
    if manifest['metadata']['invocation_id']!=result['metadata']['invocation_id']:
        raise ValueError('Build manifest and results belong to different invocations')
    expected={k for k,v in manifest['nodes'].items() if v['resource_type'] in ['model','test']
              and v['config'].get('enabled',True)}
    passed={r['unique_id'] for r in result['results'] if r['status'] in ['success','pass']}
    if not expected.issubset(passed):
        raise ValueError('Run a complete successful candidate dbt build before cutover')
    models=[v for v in manifest['nodes'].values() if v['resource_type']=='model'
            and v['config'].get('enabled',True)]
    if any(v['database']!='MARKET' or v['schema'] not in
           {candidate_prefix+layer for layer in LAYERS} for v in models):
        raise ValueError('Build artifacts do not target the selected MARKET candidate schemas')
    allowed={p.stem.upper() for p in (PROJECT/'models').glob('**/*.sql')}
    w=SnowflakeClient()
    try:
        verify(w,prefix=candidate_prefix)
        if require_identical_marts:
            compare(w,candidate_prefix)
        cur=w.cursor
        inventory=rows(cur,"SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_TYPE FROM MARKET.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN ('STAGING','INTERMEDIATE','MARTS')")
        if any(r['table_name'] not in allowed for r in inventory):
            raise ValueError('Active layer contains objects outside this project; do not swap')
        names=[f'BACKUP_{layer}_{backup_suffix}' for layer in LAYERS]
        if retire_mart_staging:
            legacy=rows(cur,"SELECT TABLE_NAME FROM MARKET.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='MART_STAGING'")
            legacy_names={'PREP_SECURITY_DAILY_MOMENTUM','PREP_SECURITY_CURRENT_SNAPSHOT',
                          'PREP_MARKET_DAILY_BREADTH','PREP_SECTOR_DAILY_BREADTH'}
            if {r['table_name'] for r in legacy}!=legacy_names:
                raise ValueError('Retired schema inventory differs from the four known preparation models')
            names.append(f'BACKUP_MART_STAGING_{backup_suffix}')
        cur.execute('SELECT SCHEMA_NAME FROM MARKET.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME IN ('+
                    ','.join(['%s']*len(names))+')',tuple(names))
        if cur.fetchall():
            raise ValueError('A dated layer backup already exists; inspect migration state before retrying')
        print('Cutover preview: three transformed-layer candidate schemas; RAW unchanged; existing grants preserved; old schemas retained.')
        if not execute:
            return
        for layer in LAYERS:
            old='MARKET.'+layer
            new='MARKET.'+candidate_prefix+layer
            objects=[(r['table_name'],'VIEW' if r['table_type']=='VIEW' else 'TABLE')
                     for r in inventory if r['table_schema']==layer]
            mirror_grants(cur,old,new,objects)
        for layer in LAYERS:
            cur.execute(f'ALTER SCHEMA MARKET.{layer} SWAP WITH MARKET.{candidate_prefix}{layer}')
            backup=f'BACKUP_{layer}_{backup_suffix}'
            cur.execute(f'ALTER SCHEMA MARKET.{candidate_prefix}{layer} RENAME TO {backup}')
            print('Switched',layer,'; retained '+backup,flush=True)
        if retire_mart_staging:
            backup=f'BACKUP_MART_STAGING_{backup_suffix}'
            cur.execute(f'ALTER SCHEMA MARKET.MART_STAGING RENAME TO {backup}')
            print('Retired MART_STAGING; retained '+backup,flush=True)
        verify(w,prefix='')
        if require_identical_marts:
            compare(w,'',baseline_schema=f'BACKUP_MARTS_{backup_suffix}')
    finally:
        w.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--candidate-prefix',default='CANDIDATE_')
    parser.add_argument('--backup-suffix',default='20261007')
    parser.add_argument('--artifacts-path',type=Path)
    parser.add_argument('--require-identical-marts',action='store_true')
    parser.add_argument('--retire-mart-staging',action='store_true')
    args=parser.parse_args()
    cutover(args.execute,args.candidate_prefix,args.backup_suffix,args.artifacts_path,
            args.require_identical_marts,args.retire_mart_staging)
