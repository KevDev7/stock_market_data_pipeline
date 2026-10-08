"""Resume Massive metadata and missing warmup prices without replacing retained prices.

Run from repository root: python -m scripts.backfill_reference
Catalogs are daily; issuer overviews are quarterly plus first observed issuers.
"""
import argparse
import concurrent.futures
import logging

import pandas as pd
import pandas_market_calendars as mcal

from src.backfill_batches import land_batch
from src.reference import MassiveReferenceClient, reference_dates
from src.reference_load import ReferenceWarehouse
from src.snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)


def issuer_key(row):
    return row.get('cik') or 'TICKER:' + row['ticker']


def backfill(start_date='2022-12-29', end_date='2025-12-31', analysis_start='2024-01-01',
             analysis_end='2025-12-31', workers=8, interval=0.02, cache_path='.cache/massive-reference.sqlite3'):
    api = MassiveReferenceClient(cache_path,interval=interval)
    warehouse = SnowflakeClient()
    reference = ReferenceWarehouse(warehouse)
    try:
        warehouse.ensure_objects_exist()
        reference.setup()
        catalog_done = reference.completed('massive_ticker_catalog')
        overview_done = reference.completed('massive_ticker_overview')
        known_issuers = reference.known_issuers()
        warehouse.cursor.execute('SELECT DISTINCT API_DATE FROM RAW.DAILY_STOCKS_RAW')
        price_done = {str(r[0]) for r in warehouse.cursor.fetchall()}
        snapshots = {str(x) for x in reference_dates(analysis_start,analysis_end)}
        sessions = mcal.get_calendar('NYSE').schedule(start_date=start_date,end_date=end_date).index
        logger.info('Reference backfill sessions=%s quarterly_and_boundary_dates=%s',len(sessions),sorted(snapshots))
        def missing_price(day):
            result = api.get('/v2/aggs/grouped/locale/us/market/stocks/'+day,
                             {'adjusted':'true','include_otc':'false'})
            prices = result.get('results',[])
            if not prices:
                raise ValueError('Missing grouped prices for trading date '+day)
            return prices,day,'polygon_grouped_daily',len(prices)

        days = [str(session.date()) for session in sessions]
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
            for offset in range(0,len(days),24):
                batch_days = days[offset:offset+24]
                catalogs = dict(zip(batch_days,pool.map(api.catalog,batch_days)))
                missing = [day for day in batch_days if day not in price_done]
                price_items = list(pool.map(missing_price,missing))
                if price_items:
                    land_batch(warehouse,price_items,prices=True,workers=workers)
                    logger.info('Added missing price batch (including warmup) dates=%s rows=%s',
                                len(price_items),sum(len(i[0]) for i in price_items))
                catalog_items = [(catalogs[day],day,'massive_ticker_catalog',len(catalogs[day]))
                                 for day in batch_days if day not in catalog_done]
                land_batch(warehouse,catalog_items,workers=workers)
                logger.info('Archived complete catalog batch through=%s progress=%s/%s',
                            batch_days[-1],min(offset+24,len(days)),len(days))
                overview_items = []
                for day in batch_days:
                    if not (analysis_start <= day <= analysis_end) or day in overview_done:
                        continue
                    issuers = {}
                    for row in catalogs[day]:
                        issuers.setdefault(issuer_key(row),row['ticker'])
                    wanted = issuers if day in snapshots else {
                        k:v for k,v in issuers.items() if known_issuers.get(k, '9999-12-31') > day}
                    if wanted:
                        futures = {pool.submit(api.overview,ticker,day):key for key,ticker in wanted.items()}
                        rows = []
                        for done,future in enumerate(concurrent.futures.as_completed(futures),1):
                            record = future.result()
                            if record:
                                rows.append(record)
                            if done % 250 == 0:
                                logger.info('Issuer overview progress date=%s completed=%s/%s',day,done,len(wanted))
                        overview_items.append((rows,day,'massive_ticker_overview',len(wanted)))
                        for key in wanted:
                            known_issuers[key] = min(day, known_issuers.get(key, day))
                        logger.info('Extracted issuer observations date=%s requested=%s returned=%s quarterly=%s',
                                    day,len(wanted),len(rows),day in snapshots)
                land_batch(warehouse,overview_items,workers=workers)
        logger.info('Reference backfill completed; no marts modified')
    finally:
        api.close()
        warehouse.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-date',default='2022-12-29')
    parser.add_argument('--end-date',default='2025-12-31')
    parser.add_argument('--workers',type=int,default=8)
    parser.add_argument('--interval',type=float,default=0.02)
    parser.add_argument('--cache-path',default='.cache/massive-reference.sqlite3')
    parser.add_argument('--analysis-start',default='2024-01-01')
    parser.add_argument('--analysis-end',default='2025-12-31')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(name)s: %(message)s')
    backfill(args.start_date,args.end_date,analysis_start=args.analysis_start,analysis_end=args.analysis_end,
             workers=args.workers,interval=args.interval,cache_path=args.cache_path)


if __name__ == '__main__':
    main()
