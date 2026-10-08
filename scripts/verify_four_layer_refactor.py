"""Read-only comparison of all eight published marts before a layer cutover."""
import argparse
import json
from pathlib import Path
import re

from src.snowflake_client import SnowflakeClient


CONTRACTS = {
    'DIM_DATE': ['DATE_KEY'],
    'DIM_SECTOR': ['SECTOR_KEY'],
    'DIM_SECURITY': ['SECURITY_KEY'],
    'DIM_SECURITY_HISTORY': ['SECURITY_HISTORY_KEY'],
    'FCT_SECURITY_DAILY_MOMENTUM': ['SECURITY_KEY', 'DATE_KEY'],
    'FCT_SECURITY_CURRENT_SNAPSHOT': ['SECURITY_KEY'],
    'FCT_MARKET_DAILY_BREADTH': ['DATE_KEY'],
    'FCT_SECTOR_DAILY_BREADTH': ['SECTOR_KEY', 'DATE_KEY'],
}


def compare(w, candidate_prefix='FOUR_LAYER_20261008_', baseline_schema='MARTS'):
    if not re.fullmatch(r'(?:[A-Z][A-Z0-9_]*)?', candidate_prefix):
        raise ValueError('Invalid verification schema prefix')
    if not re.fullmatch(r'[A-Z][A-Z0-9_]*', baseline_schema):
        raise ValueError('Invalid verification baseline schema')
    cur = w.cursor
    results = {}
    for table, keys in CONTRACTS.items():
        baseline = f'MARKET.{baseline_schema}.{table}'
        candidate = f'MARKET.{candidate_prefix}MARTS.{table}'
        columns = []
        for schema in (baseline_schema, candidate_prefix + 'MARTS'):
            cur.execute('''SELECT COLUMN_NAME,DATA_TYPE,NUMERIC_PRECISION,NUMERIC_SCALE,
                CHARACTER_MAXIMUM_LENGTH,DATETIME_PRECISION,ORDINAL_POSITION
                FROM MARKET.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION''',
                (schema, table))
            columns.append(cur.fetchall())
        if not columns[0] or columns[0] != columns[1]:
            raise AssertionError('Published column contract changed: ' + table)
        statistics = []
        key_sql = ','.join('"' + key + '"' for key in keys)
        for relation in (baseline, candidate):
            cur.execute(f'SELECT COUNT(*),HASH_AGG(*) FROM {relation}')
            count, fingerprint = cur.fetchone()
            cur.execute(f'''SELECT COUNT(*) FROM (
                SELECT {key_sql} FROM {relation} GROUP BY {key_sql} HAVING COUNT(*)>1)''')
            duplicates = cur.fetchone()[0]
            statistics.append((count, fingerprint, duplicates))
        if statistics[0][0] != statistics[1][0] or any(s[2] for s in statistics):
            raise AssertionError('Published row count or grain changed: ' + table)
        exact = statistics[0][1] == statistics[1][1]
        different_rows = 0
        if not exact:
            predicates = []
            for name, dtype, *_ in columns[0]:
                a = 'a."' + name + '"'
                b = 'b."' + name + '"'
                equality = f'EQUAL_NULL({a},{b})'
                # Float aggregates can differ in final bits across execution plans.
                # Integer flags, identifiers, source prices, and dates stay exact.
                if dtype == 'FLOAT' and name not in {
                    'OPEN', 'HIGH', 'LOW', 'CLOSE', 'YESTERDAY_CLOSE',
                    'LATEST_OPEN', 'LATEST_HIGH', 'LATEST_LOW', 'LATEST_CLOSE', 'LATEST_PREV_CLOSE',
                }:
                    equality += (f' OR ({a} IS NOT NULL AND {b} IS NOT NULL AND '
                                 f'ABS({a}-{b})<=GREATEST(1e-9,ABS({b})*1e-10))')
                predicates.append('NOT (' + equality + ')')
            joins = ' AND '.join(f'a."{key}"=b."{key}"' for key in keys)
            cur.execute(f'''SELECT COUNT(*) FROM {candidate} a FULL OUTER JOIN {baseline} b
                ON {joins} WHERE a."{keys[0]}" IS NULL OR b."{keys[0]}" IS NULL
                OR {' OR '.join(predicates)}''')
            different_rows = cur.fetchone()[0]
            if different_rows:
                raise AssertionError(f'Published values changed: {table}: {different_rows} rows')
        results[table] = {'rows': statistics[1][0], 'duplicate_keys': 0,
                          'column_contract_unchanged': True, 'exact_fingerprint_match': exact,
                          'different_rows': different_rows}
    summary = {'status': 'verified', 'baseline_schema': baseline_schema,
               'candidate_prefix': candidate_prefix, 'published_marts': results}
    print(json.dumps(summary, indent=2), flush=True)
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate-prefix', default='FOUR_LAYER_20261008_')
    parser.add_argument('--baseline-schema', default='MARTS')
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    w = SnowflakeClient()
    try:
        result = compare(w, args.candidate_prefix, args.baseline_schema)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(result, indent=2) + '\n')
    finally:
        w.close()
