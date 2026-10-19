"""Durable reference archive and atomic, source/date-scoped Snowflake landing."""
import json
import re
import uuid

import pandas as pd

from src.config import AWS, SNOWFLAKE
from src.reference import landing_rows
from src.s3_client import S3RawClient

REFERENCE_PREFIX = 'raw/massive/reference'
SOURCES = {'massive_ticker_catalog', 'massive_ticker_overview'}


class ReferenceWarehouse:
    def __init__(self, warehouse):
        self.warehouse = warehouse
        self.cursor = warehouse.cursor
        self.database = SNOWFLAKE['database']
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',self.database):
            raise ValueError('Invalid database identifier')
        self.stage = self.database + '.RAW.MASSIVE_REFERENCE_STAGE'

    def check_infrastructure(self):
        """Inspect Terraform-owned prerequisites without creating or altering them."""
        guidance = 'Review/apply infra/terraform before running reference ingestion.'

        def fail(problem):
            raise RuntimeError('Reference infrastructure: ' + problem + '. ' + guidance)

        if not AWS.get('s3_bucket'):
            fail('AWS_S3_BUCKET is not configured')
        location = 's3://' + AWS['s3_bucket'] + '/' + REFERENCE_PREFIX + '/'
        try:
            self.cursor.execute('DESC INTEGRATION STOCK_MARKET_S3_INTEGRATION')
            properties = {r[0]: r[2] for r in self.cursor.fetchall()}
        except Exception as exc:
            raise RuntimeError('Cannot inspect the reference storage integration. ' + guidance) from exc
        if str(properties.get('ENABLED', '')).lower() != 'true':
            fail('storage integration is not enabled')
        if properties.get('STORAGE_PROVIDER') != 'S3':
            fail('storage integration is not configured for S3')

        def covers(value):
            return value == '*' or location.startswith(value.rstrip('/') + '/')

        locations = [x.strip() for x in properties.get('STORAGE_ALLOWED_LOCATIONS', '').split(',') if x.strip()]
        blocked = [x.strip() for x in properties.get('STORAGE_BLOCKED_LOCATIONS', '').split(',') if x.strip()]
        if not any(covers(value) for value in locations) or any(covers(value) for value in blocked):
            fail('reference archive location is not permitted by the integration')
        try:
            self.cursor.execute(f'DESC STAGE {self.stage}')
            stage = {(r[0], r[1]): r[3] for r in self.cursor.fetchall()}
        except Exception as exc:
            raise RuntimeError('Cannot inspect the reference external stage. ' + guidance) from exc
        try:
            urls = json.loads(stage.get(('STAGE_LOCATION', 'URL'), '[]'))
        except (ValueError, TypeError):
            fail('reference stage location is malformed')
        if urls != [location]:
            fail('reference stage points to a different archive location')
        if stage.get(('STAGE_INTEGRATION', 'STORAGE_INTEGRATION')) != 'STOCK_MARKET_S3_INTEGRATION':
            fail('reference stage uses a different storage integration')
        if (stage.get(('STAGE_FILE_FORMAT', 'TYPE')) != 'JSON'
                or stage.get(('STAGE_FILE_FORMAT', 'COMPRESSION')) != 'AUTO'):
            fail('reference stage must use the Terraform-managed JSON/AUTO format')

    def setup(self):
        self.check_infrastructure()
        self.cursor.execute(f'''CREATE TABLE IF NOT EXISTS {self.database}.RAW.REFERENCE_RAW (
            API_DATE DATE, RUN_ID STRING, SOURCE STRING, RAW_PAYLOAD STRING, INGESTED_AT TIMESTAMP_NTZ)''')
        self.cursor.execute(f'''CREATE TABLE IF NOT EXISTS {self.database}.RAW.REFERENCE_MANIFEST (
            API_DATE DATE, SOURCE STRING, RUN_ID STRING, ROW_COUNT INTEGER,
            REQUESTED_COUNT INTEGER, S3_KEY STRING, SHA256 STRING, COMPLETED_AT TIMESTAMP_NTZ)''')

    def completed(self, source):
        self.cursor.execute(f'SELECT API_DATE FROM {self.database}.RAW.REFERENCE_MANIFEST WHERE SOURCE=%s',(source,))
        return {str(r[0]) for r in self.cursor.fetchall()}

    def known_issuers(self):
        self.cursor.execute(f'''SELECT COALESCE(NULLIF(TRY_PARSE_JSON(RAW_PAYLOAD):cik::STRING,''),
            'TICKER:' || TRY_PARSE_JSON(RAW_PAYLOAD):ticker::STRING), MIN(API_DATE)
            FROM {self.database}.RAW.REFERENCE_RAW WHERE SOURCE='massive_ticker_overview'
            GROUP BY 1''')
        return {r[0]: str(r[1]) for r in self.cursor.fetchall()}

    def load(self, records, date, source, requested_count=None):
        if source not in SOURCES:
            raise ValueError('Unknown reference source')
        run_id = 'reference-' + uuid.uuid4().hex[:12]
        rows = landing_rows(records, date, source, run_id)
        columns = ['API_DATE','SOURCE','RUN_ID','RAW_PAYLOAD','INGESTED_AT']
        archive = S3RawClient(prefix=REFERENCE_PREFIX + '/' + source,
                              filename='reference_raw.ndjson.gz')
        archived = archive.archive_dataframe(pd.DataFrame(rows,columns=columns),str(date),run_id)
        stage_path = archived['key'][len(REFERENCE_PREFIX)+1:]
        if not re.fullmatch(r'[A-Za-z0-9_./=-]+',stage_path):
            raise ValueError('Unexpected archive key')
        temp = self.database + '.RAW.TMP_REFERENCE_' + uuid.uuid4().hex[:12].upper()
        self.cursor.execute(f'CREATE TEMPORARY TABLE {temp} LIKE {self.database}.RAW.REFERENCE_RAW')
        try:
            if rows:
                self.cursor.execute(f'''COPY INTO {temp} (API_DATE,RUN_ID,SOURCE,RAW_PAYLOAD,INGESTED_AT)
                    FROM (SELECT TRY_TO_DATE($1:API_DATE::STRING),$1:RUN_ID::STRING,
                    $1:SOURCE::STRING,$1:RAW_PAYLOAD::STRING,TRY_TO_TIMESTAMP_NTZ($1:INGESTED_AT::STRING)
                    FROM @{self.stage}/{stage_path})
                    FILE_FORMAT=(TYPE=JSON COMPRESSION=AUTO) ON_ERROR=ABORT_STATEMENT''')
            self.cursor.execute(f'SELECT COUNT(*) FROM {temp}')
            if self.cursor.fetchone()[0] != len(rows):
                raise ValueError('Reference archive/warehouse row-count mismatch')
            self.cursor.execute('BEGIN')
            for table in ['REFERENCE_RAW','REFERENCE_MANIFEST']:
                self.cursor.execute(f'DELETE FROM {self.database}.RAW.{table} WHERE API_DATE=%s AND SOURCE=%s',(str(date),source))
            self.cursor.execute(f'INSERT INTO {self.database}.RAW.REFERENCE_RAW SELECT * FROM {temp}')
            self.cursor.execute(f'''INSERT INTO {self.database}.RAW.REFERENCE_MANIFEST
                VALUES (%s,%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP())''',
                (str(date),source,run_id,len(rows),requested_count if requested_count is not None else len(rows),
                 archived['key'],archived['sha256']))
            self.cursor.execute('COMMIT')
        except Exception:
            self.cursor.execute('ROLLBACK')
            raise
        finally:
            self.cursor.execute('DROP TABLE IF EXISTS '+temp)
        return archived
