"""Read existing archived records and marts without writing pipeline data.

May resume the existing Snowflake warehouses and incur small query costs.
No source API requests, DDL, ingestion, dbt builds, or dashboard deployments.
"""
import json
from pathlib import Path
import re

from scripts.inspect_infrastructure import query, snowflake_connection


PUBLISHED = ['DIM_DATE', 'DIM_SECTOR', 'DIM_SECURITY', 'DIM_SECURITY_HISTORY',
             'FCT_MARKET_DAILY_BREADTH', 'FCT_SECTOR_DAILY_BREADTH',
             'FCT_SECURITY_CURRENT_SNAPSHOT', 'FCT_SECURITY_DAILY_MOMENTUM']


def verify_archives(connection):
    from src.config import AWS
    cursor = connection.cursor()
    results = {}
    for stage, prefix, sample in [
        ('POLYGON_RAW_STAGE', AWS['s3_prefix'], 'api_date=2025-12-31/'),
        ('POLYGON_REFERENCE_STAGE', 'raw/polygon/reference',
         'polygon_ticker_catalog/api_date=2025-12-31/'),
    ]:
        listing = query(cursor, f'LIST @MARKET.RAW.{stage}/{sample}')
        if not listing:
            raise ValueError('Expected retained archive sample is missing: ' + stage)
        url = f"s3://{AWS['s3_bucket']}/{prefix.strip('/')}/"
        name = listing[0]['name']
        if not name.startswith(url):
            raise ValueError('Unexpected archive location')
        suffix = name[len(url):]
        if not re.fullmatch(r'[A-Za-z0-9_./=-]+', suffix):
            raise ValueError('Unexpected archive filename')
        cursor.execute(f'SELECT $1 FROM @MARKET.RAW.{stage}/{suffix} LIMIT 1')
        if cursor.fetchone() is None:
            raise ValueError('Archive sample has no records: ' + stage)
        results[stage] = {'listing': 'passed', 'sample_record_read': 'passed'}
    return results


def verify_dashboard():
    # Import only here: Python 3.11+ is needed for the optional local-secret check.
    import tomllib
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    root = Path(__file__).resolve().parents[1]
    path = root / 'data-viz/.streamlit/secrets.toml'
    config = {}
    if path.is_file():
        with path.open('rb') as handle:
            config = tomllib.load(handle)['snowflake']
    credential_source = 'local_dashboard_reader_secrets'
    if config.get('role') == 'STREAMLIT_DASHBOARD_ROLE':
        pem = config['private_key'].encode()
    else:
        dedicated_key = root / 'keys/streamlit_dashboard_key.pem'
        if not dedicated_key.is_file():
            return {'status': 'not_checked', 'reason': 'Dedicated dashboard reader credentials unavailable'}
        from src.config import SNOWFLAKE
        config = dict(account=SNOWFLAKE['account'], user='STREAMLIT_DASHBOARD_USER',
                      role='STREAMLIT_DASHBOARD_ROLE', warehouse='STREAMLIT_DASHBOARD_WH',
                      database='MARKET', schema='MARTS')
        pem = dedicated_key.read_bytes()
        credential_source = 'existing_dedicated_project_key'
    key = serialization.load_pem_private_key(pem, password=None)
    connection = snowflake.connector.connect(
        **{key: config[key] for key in ['account','user','role','warehouse','database','schema']},
        private_key=key.private_bytes(serialization.Encoding.DER, serialization.PrivateFormat.PKCS8,
                                     serialization.NoEncryption()), login_timeout=20,
        session_parameters={'QUERY_TAG': 'stock_market_iac_read_only_dashboard_check'},
    )
    try:
        cursor = connection.cursor()
        role = query(cursor, 'SELECT CURRENT_ROLE() AS ROLE')[0]['role']
        if role != 'STREAMLIT_DASHBOARD_ROLE':
            raise ValueError('Dashboard connection did not use the expected reader role')
        for table in PUBLISHED:
            cursor.execute(f'SELECT * FROM MARKET.MARTS.{table} LIMIT 1')
            if cursor.fetchone() is None:
                raise ValueError('Published mart is empty: ' + table)
        return {'status':'passed', 'role':role, 'published_marts_read':len(PUBLISHED),
                'credential_source':credential_source}
    finally:
        connection.close()


def main():
    connection = snowflake_connection()
    try:
        from types import SimpleNamespace
        from src.reference_load import ReferenceWarehouse
        ReferenceWarehouse(SimpleNamespace(cursor=connection.cursor())).check_infrastructure()
        archives = verify_archives(connection)
        counts = query(connection.cursor(), "SELECT TABLE_SCHEMA,TABLE_NAME,ROW_COUNT "
                       "FROM MARKET.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA IN ('RAW','MARTS') "
                       "ORDER BY TABLE_SCHEMA,TABLE_NAME")
    finally:
        connection.close()
    print(json.dumps({'reference_infrastructure_checks':'passed', 'archive_access':archives, 'dashboard':verify_dashboard(),
                      'metadata_row_counts':counts, 'data_writes_performed':False}, indent=2))


if __name__ == '__main__':
    main()
