"""Bounded bulk landing for historical backfills, with per-date archive manifests.

The API payload is unchanged. One COPY validates a batch of archived dates before
any raw-table transaction. Missing-price backfills preserve retained dates;
an explicit refresh campaign replaces only verified, archived batch dates.
"""
import concurrent.futures
import re
import uuid

import pandas as pd

from src.config import AWS, SNOWFLAKE
from src.load import build_raw_landing_dataframe
from src.reference import landing_rows
from src.reference_load import REFERENCE_PREFIX, SOURCES
from src.s3_client import S3RawClient


def archive_item(item):
    records,date,source,requested = item
    run_id = 'backfill-' + uuid.uuid4().hex[:12]
    if source == 'polygon_grouped_daily':
        archive = S3RawClient()
        frame = build_raw_landing_dataframe(pd.DataFrame(records),date,run_id)
    elif source in SOURCES:
        archive = S3RawClient(prefix=REFERENCE_PREFIX+'/'+source,filename='reference_raw.ndjson.gz')
        frame = pd.DataFrame(landing_rows(records,date,source,run_id),
                             columns=['API_DATE','SOURCE','RUN_ID','RAW_PAYLOAD','INGESTED_AT'])
    else:
        raise ValueError('Unknown backfill source')
    result = archive.archive_dataframe(frame,date,run_id)
    result.update(date=date,source=source,run_id=run_id,requested=requested)
    return result


def land_batch(warehouse, items, prices=False, workers=8, refresh_campaign=None):
    """Archive a bounded batch, verify date/source counts, then commit atomically."""
    if not items:
        return []
    if refresh_campaign is not None and (not prices or not re.fullmatch(r'[A-Za-z0-9_]+',refresh_campaign)):
        raise ValueError('Invalid price refresh campaign')
    if len(items)>100 or len({(i[1],i[2]) for i in items})!=len(items):
        raise ValueError('Backfill batch must contain <=100 distinct source/dates')
    for records,date,source,requested in items:
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',date):
            raise ValueError('Invalid source date')
        if prices != (source=='polygon_grouped_daily') or (prices and not records):
            raise ValueError('Mixed or empty price batch')
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        archived=list(pool.map(archive_item,items))
    database=SNOWFLAKE['database']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',database):
        raise ValueError('Invalid database')
    table=database+'.RAW.'+('DAILY_STOCKS_RAW' if prices else 'REFERENCE_RAW')
    stage=SNOWFLAKE['s3_stage'] if prices else database+'.RAW.MASSIVE_REFERENCE_STAGE'
    if not all(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',part) for part in stage.split('.')):
        raise ValueError('Invalid stage')
    prefix=AWS['s3_prefix'].strip('/') if prices else REFERENCE_PREFIX
    paths=[]
    for entry in archived:
        if not entry['key'].startswith(prefix+'/'):
            raise ValueError('Unexpected archive prefix')
        path=entry['key'][len(prefix)+1:]
        if not re.fullmatch(r'[A-Za-z0-9_./=-]+',path):
            raise ValueError('Invalid archive path')
        if entry['row_count']:
            paths.append("'"+path+"'")
    temp=database+'.RAW.TMP_BATCH_'+uuid.uuid4().hex[:12].upper()
    cur=warehouse.cursor
    cur.execute('CREATE TEMPORARY TABLE '+temp+' LIKE '+table)
    try:
        if paths:
            cur.execute(f'''COPY INTO {temp} (API_DATE,RUN_ID,SOURCE,RAW_PAYLOAD,INGESTED_AT)
                FROM (SELECT TRY_TO_DATE($1:API_DATE::STRING),$1:RUN_ID::STRING,
                $1:SOURCE::STRING,$1:RAW_PAYLOAD::STRING,TRY_TO_TIMESTAMP_NTZ($1:INGESTED_AT::STRING)
                FROM @{stage}) FILES=({','.join(paths)})
                FILE_FORMAT=(TYPE=JSON COMPRESSION=AUTO) ON_ERROR=ABORT_STATEMENT''')
        cur.execute(f'SELECT API_DATE,SOURCE,COUNT(*) FROM {temp} GROUP BY 1,2')
        actual={(str(r[0]),r[1]):r[2] for r in cur.fetchall()}
        expected={(e['date'],e['source']):e['row_count'] for e in archived if e['row_count']}
        if actual!=expected:
            raise ValueError('Batch archive/warehouse source-date count mismatch')
        cur.execute('BEGIN')
        if prices:
            if refresh_campaign:
                cur.execute(f'DELETE FROM {table} WHERE API_DATE IN (SELECT DISTINCT API_DATE FROM {temp})')
                cur.execute(f'INSERT INTO {table} SELECT * FROM {temp}')
                placeholders=','.join(['(%s,%s,%s,%s,CURRENT_TIMESTAMP())']*len(archived))
                params=[v for e in archived for v in [refresh_campaign,e['date'],e['key'],e['sha256']]]
                cur.execute(f'''INSERT INTO {database}.RAW.PRICE_REFRESH_MANIFEST
                    (CAMPAIGN,API_DATE,S3_KEY,SHA256,REFRESHED_AT) VALUES {placeholders}''',params)
            else:
                cur.execute(f'''INSERT INTO {table} SELECT t.* FROM {temp} t
                    WHERE NOT EXISTS (SELECT 1 FROM {table} p WHERE p.API_DATE=t.API_DATE)''')
            placeholders=','.join(['(%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP())']*len(archived))
            params=[]
            for e in archived:
                params.extend([e['run_id'],e['date'],'completed',e['row_count'],e['bucket'],
                               e['key'],e['etag'],e['sha256']])
            cur.execute(f'''INSERT INTO {database}.ADMIN.INGESTION_CHECKPOINTS
                (RUN_ID,API_DATE,STATUS,ROWS_INSERTED,S3_BUCKET,S3_KEY,S3_ETAG,S3_SHA256,COMPLETED_AT)
                VALUES {placeholders}''',params)
        else:
            # Include empty successful observations in the manifest replacement.
            condition=' OR '.join(['(API_DATE=%s AND SOURCE=%s)']*len(archived))
            pairs=[v for e in archived for v in [e['date'],e['source']]]
            cur.execute(f'DELETE FROM {table} WHERE {condition}',pairs)
            cur.execute(f'DELETE FROM {database}.RAW.REFERENCE_MANIFEST WHERE {condition}',pairs)
            cur.execute(f'INSERT INTO {table} SELECT * FROM {temp}')
            placeholders=','.join(['(%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP())']*len(archived))
            params=[v for e in archived for v in [e['date'],e['source'],e['run_id'],e['row_count'],
                                                e['requested'],e['key'],e['sha256']]]
            cur.execute(f'INSERT INTO {database}.RAW.REFERENCE_MANIFEST VALUES {placeholders}',params)
        cur.execute('COMMIT')
    except BaseException:
        cur.execute('ROLLBACK')
        raise
    finally:
        cur.execute('DROP TABLE IF EXISTS '+temp)
    return archived
