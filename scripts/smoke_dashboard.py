"""Read-only local dashboard smoke test; never deploys code or prints secrets."""
import argparse
import json
import logging
import os
from pathlib import Path
import re
import sys

import toml
from streamlit.testing.v1 import AppTest

from src.snowflake_client import SnowflakeClient


def smoke(schema='MARTS', role=None, database=None):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',schema):
        raise ValueError('Invalid mart schema')
    site=Path(__file__).resolve().parents[1]/'data-viz'
    credentials=toml.load(site/'.streamlit'/'secrets.toml')['snowflake']
    credentials=dict(credentials,mart_schema=schema)
    database=database or credentials['database']
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',database):
        raise ValueError('Invalid Snowflake database')
    credentials['database']=database
    if role is not None:
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',role):
            raise ValueError('Invalid Snowflake role')
        credentials['role']=role
    w=SnowflakeClient()
    try:
        w.cursor.execute(f"SELECT SECURITY_KEY FROM {database}.{schema}.DIM_SECURITY WHERE TICKER='AAPL' AND IS_CURRENT_SNAPSHOT=1")
        record=w.cursor.fetchone()
        if record is None:
            raise ValueError('Expected reference security is absent')
        reference_key=record[0]
    finally:
        w.close()
    logging.getLogger('streamlit').setLevel(logging.ERROR)
    previous_directory=Path.cwd()
    previous_import_paths=list(sys.path)
    results=[]
    try:
        os.chdir(site)
        sys.path.insert(0,str(site))
        for path in [site/'streamlit_app.py',*sorted((site/'pages').glob('*.py'))]:
            app=AppTest.from_file(str(path),default_timeout=90)
            app.secrets['snowflake']=credentials
            app.run()
            if path.name=='3_Ticker_Momentum.py' and not app.exception:
                app.sidebar.selectbox[0].set_value(reference_key).run()
            result={'page':path.name,'database':database,'role':credentials['role'],'exceptions':len(app.exception),
                    'charts':len(app.get('plotly_chart')),'dataframes':len(app.dataframe),
                    'metrics':len(app.metric)}
            print(json.dumps(result),flush=True)
            if result['exceptions'] or not (result['charts'] or result['dataframes']):
                raise RuntimeError('Dashboard smoke test failed for '+path.name)
            results.append(result)
    finally:
        os.chdir(previous_directory)
        sys.path[:]=previous_import_paths
    return results


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mart-schema',default='MARTS')
    parser.add_argument('--role',help='Override the role in memory; never edits saved credentials')
    parser.add_argument('--database',help='Override the database in memory for isolated model verification')
    args=parser.parse_args()
    smoke(args.mart_schema,args.role,args.database)
