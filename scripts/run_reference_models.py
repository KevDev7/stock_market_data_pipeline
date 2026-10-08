"""Run dbt in isolated candidate schemas using the configured local credentials.

Examples: python -m scripts.run_reference_models parse
          python -m scripts.run_reference_models build --full-refresh
"""
import argparse
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import yaml

from dotenv import load_dotenv


def main():
    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / '.env')
    private_key = Path(os.environ['PRIVATE_KEY_PATH'])
    if not private_key.exists():
        private_key = root / 'keys' / private_key.name
    if not private_key.is_file():
        raise FileNotFoundError('Configured local Snowflake private key is unavailable')
    os.environ['PRIVATE_KEY_PATH'] = str(private_key)
    project = root / 'dbt' / 'stock_analytics'
    parser=argparse.ArgumentParser(add_help=False)
    parser.add_argument('--schema-prefix',default='CANDIDATE_')
    parser.add_argument('--vars',default='{}')
    options,args=parser.parse_known_args()
    if not re.fullmatch(r'[A-Za-z0-9_]*',options.schema_prefix):
        raise ValueError('Invalid schema prefix')
    args=args or ['build','--full-refresh']
    variables=yaml.safe_load(options.vars) or {}
    if not isinstance(variables,dict):
        raise ValueError('dbt vars must be a mapping')
    variables['schema_prefix']=options.schema_prefix
    command = [str(Path(sys.executable).parent / 'dbt'), *args,
               '--project-dir',str(project),'--profiles-dir',str(project),
               '--vars',json.dumps(variables)]
    raise SystemExit(subprocess.call(command,cwd=str(project)))


if __name__ == '__main__':
    main()
