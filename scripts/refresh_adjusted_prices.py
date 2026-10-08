"""Refresh retained adjusted prices to match newly fetched warmup prices.

Only the original overlap dates are refreshed. A zero-copy Snowflake backup and
all prior S3 archive versions are preserved. Use --execute for this migration.
"""
import argparse
import concurrent.futures
import logging

from src.backfill_batches import land_batch
from src.config import SNOWFLAKE
from src.reference import MassiveReferenceClient
from src.snowflake_client import SnowflakeClient

logger=logging.getLogger(__name__)
CAMPAIGN='reference_migration_20261007'


def refresh(execute=False):
    api=MassiveReferenceClient('.cache/massive-reference.sqlite3',interval=0.02)
    w=SnowflakeClient()
    try:
        database=SNOWFLAKE['database']
        w._validate_identifier(database)
        w.cursor.execute(f'''SELECT DISTINCT API_DATE FROM {database}.RAW.DAILY_STOCKS_RAW
            WHERE API_DATE BETWEEN '2023-11-20' AND '2025-12-31' ORDER BY 1''')
        days=[str(r[0]) for r in w.cursor.fetchall()]
        logger.info('Adjusted-price refresh: %s retained dates; preview=%s',len(days),not execute)
        if not execute:
            return
        backup=database+'.RAW.DAILY_STOCKS_RAW_BEFORE_REFERENCE_20261007'
        w.cursor.execute(f'CREATE TABLE IF NOT EXISTS {backup} CLONE {database}.RAW.DAILY_STOCKS_RAW')
        logger.info('Preserved pre-refresh source table in %s',backup)
        w.cursor.execute(f'''CREATE TABLE IF NOT EXISTS {database}.RAW.PRICE_REFRESH_MANIFEST
            (CAMPAIGN STRING,API_DATE DATE,S3_KEY STRING,SHA256 STRING,REFRESHED_AT TIMESTAMP_NTZ)''')
        w.cursor.execute(f'SELECT API_DATE FROM {database}.RAW.PRICE_REFRESH_MANIFEST WHERE CAMPAIGN=%s',(CAMPAIGN,))
        completed={str(r[0]) for r in w.cursor.fetchall()}
        pending=[day for day in days if day not in completed]
        def extract(day):
            result=api.get('/v2/aggs/grouped/locale/us/market/stocks/'+day,
                           {'adjusted':'true','include_otc':'false'})
            rows=result.get('results',[])
            if not rows:
                raise ValueError('Empty price refresh for '+day)
            return rows,day,'polygon_grouped_daily',len(rows)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            for offset in range(0,len(pending),24):
                items=list(pool.map(extract,pending[offset:offset+24]))
                land_batch(w,items,prices=True,refresh_campaign=CAMPAIGN)
                logger.info('Refreshed adjusted-price batch through=%s progress=%s/%s',
                            items[-1][1],len(completed)+min(offset+24,len(pending)),len(days))
        logger.info('Adjusted-price refresh complete; original S3 versions and backup retained')
    finally:
        api.close(); w.close()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    refresh(args.execute)
