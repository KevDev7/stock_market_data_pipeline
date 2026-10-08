"""Read-only completeness and analytical checks; never cut over partial history."""
import argparse
import json
import re

import pandas_market_calendars as mcal

from src.reference import reference_dates
from src.snowflake_client import SnowflakeClient


def verify(w,prefix='CANDIDATE_'):
    if not re.fullmatch(r'[A-Za-z0-9_]*',prefix):
        raise ValueError('Invalid verification schema prefix')
    cur=w.cursor
    def query(sql):
        cur.execute(sql)
        return cur.fetchall()
    expected={str(d.date()) for d in mcal.get_calendar('NYSE').schedule(
        start_date='2022-12-29',end_date='2025-12-31').index}
    report_dates={d for d in expected if d>='2024-01-01'}
    raw={str(r[0]) for r in query('SELECT DISTINCT API_DATE FROM MARKET.RAW.DAILY_STOCKS_RAW')}
    catalogs={str(r[0]) for r in query("SELECT API_DATE FROM MARKET.RAW.REFERENCE_MANIFEST WHERE SOURCE='massive_ticker_catalog'")}
    observed={str(r[0]) for r in query("SELECT API_DATE FROM MARKET.RAW.REFERENCE_MANIFEST WHERE SOURCE='massive_ticker_overview'")}
    quarterly={str(d) for d in reference_dates('2024-01-01','2025-12-31')}
    cur.execute("SELECT API_DATE FROM MARKET.RAW.PRICE_REFRESH_MANIFEST WHERE CAMPAIGN='reference_migration_20261007'")
    refreshed={str(r[0]) for r in cur.fetchall()}
    overlap={str(r[0]) for r in query("SELECT DISTINCT API_DATE FROM MARKET.RAW.DAILY_STOCKS_RAW_BEFORE_REFERENCE_20261007 WHERE API_DATE BETWEEN '2023-11-20' AND '2025-12-31'")}
    missing={
        'price_dates':sorted(expected-raw),'catalog_dates':sorted(expected-catalogs),
        'quarterly_observations':sorted(quarterly-observed),'adjusted_refresh_dates':sorted(overlap-refreshed)}
    summary={'expected_source_dates':len(expected),'expected_reporting_dates':len(report_dates),
             'quarterly_and_boundary_snapshots':sorted(quarterly),'missing':missing}
    if any(missing.values()):
        print(json.dumps(summary,indent=2))
        raise ValueError('Source history is incomplete; do not cut over')
    marts='MARKET.'+prefix+'MARTS.'
    intermediate='MARKET.'+prefix+'INTERMEDIATE.'
    actual={str(r[0]) for r in query('SELECT TRADE_DATE FROM '+marts+'FCT_MARKET_DAILY_BREADTH')}
    if actual!=report_dates:
        raise ValueError('Mart dates do not exactly match the reporting calendar')
    counts={name:query('SELECT COUNT(*) FROM '+marts+name)[0][0] for name in [
        'DIM_DATE','DIM_SECURITY','DIM_SECURITY_HISTORY','DIM_SECTOR','FCT_SECURITY_DAILY_MOMENTUM',
        'FCT_SECURITY_CURRENT_SNAPSHOT','FCT_MARKET_DAILY_BREADTH','FCT_SECTOR_DAILY_BREADTH']}
    if any(count==0 for count in counts.values()):
        raise ValueError('A consumer dataset is empty')
    gaps=query(f'''SELECT COUNT(*) FROM {marts}FCT_SECURITY_DAILY_MOMENTUM f
        LEFT JOIN {marts}DIM_SECURITY_HISTORY h ON f.SECURITY_KEY=h.SECURITY_KEY
        AND f.TRADE_DATE BETWEEN h.VALID_FROM AND h.VALID_TO
        WHERE h.SECURITY_KEY IS NULL''')[0][0]
    if gaps:
        raise ValueError('History dimension does not cover every fact row')
    quality=query(f'''SELECT COUNT(*),COUNT_IF(SECTOR='Unknown'),
        COUNT_IF(IDENTITY_QUALITY='issuer_ticker_fallback'),COUNT_IF(REFERENCE_TYPE_REVIEW_REQUIRED=1),
        COUNT_IF(TRADING_LINE<>'regular')
        FROM {intermediate}INT_MARKET__REFERENCE_DAILY
        WHERE TRADE_DATE BETWEEN '2024-01-01' AND '2025-12-31' ''')[0]
    published_quality=query(f'''SELECT COUNT(*),COUNT_IF(p.SECTOR='Unknown'),
        COUNT_IF(r.IDENTITY_QUALITY='issuer_ticker_fallback'),COUNT_IF(r.REFERENCE_TYPE_REVIEW_REQUIRED=1),
        COUNT_IF(r.TRADING_LINE<>'regular'),COUNT_IF(r.CLASSIFICATION_OBSERVED_ON IS NULL)
        FROM {intermediate}INT_MARKET__DAILY p JOIN {intermediate}INT_MARKET__REFERENCE_DAILY r
        ON p.TICKER=r.TICKER AND p.TRADE_DATE=r.TRADE_DATE
        WHERE p.TRADE_DATE BETWEEN '2024-01-01' AND '2025-12-31' ''')[0]
    summary.update(status='verified',row_counts=counts,
        reference_quality=dict(zip(['catalog_security_days','unknown_industry_days',
                                   'fallback_identity_days','type_review_days','temporary_trading_line_days'],quality)),
        published_reference_quality=dict(zip(['accepted_security_days','unknown_industry_days',
            'fallback_identity_days','type_review_days','temporary_trading_line_days',
            'missing_issuer_observation_days'],published_quality)),
        reporting_period=['2024-01-01','2025-12-31'],warmup_start='2022-12-29',
        raw_price_rows=query('SELECT COUNT(*) FROM MARKET.RAW.DAILY_STOCKS_RAW')[0][0])
    print(json.dumps(summary,indent=2))
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--schema-prefix',default='CANDIDATE_')
    args=parser.parse_args()
    w=SnowflakeClient()
    try:
        verify(w,args.schema_prefix)
    finally:
        w.close()
