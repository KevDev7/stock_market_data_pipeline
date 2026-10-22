import hashlib
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from scripts.migrate_date_archives import DateArchiveMigration, choose_current, date_key
from src.s3_client import S3RawClient
from src.snowflake_client import SnowflakeClient
from src.reference_load import ReferenceWarehouse
from src.backfill_batches import land_batch


class DatePathTest(unittest.TestCase):
    def test_new_runs_share_one_date_path_but_dates_and_datasets_stay_separate(self):
        for prefix,filename in [('daily-stock-prices','daily_stocks_raw.ndjson.gz'),
                                ('ticker-catalog','reference_raw.ndjson.gz'),
                                ('company-overview','reference_raw.ndjson.gz')]:
            client=S3RawClient(bucket='fixture',prefix=prefix,filename=filename,client=Mock())
            self.assertEqual(client.object_key('2025-12-31','one'),f'{prefix}/api_date=2025-12-31/{filename}')
            self.assertEqual(client.object_key('2025-12-31','one'),client.object_key('2025-12-31','two'))
            self.assertNotEqual(client.object_key('2025-12-31','one'),client.object_key('2025-12-30','one'))

    def test_mapping_rejects_unrelated_paths_and_wrong_filenames(self):
        for key in ['other/api_date=2025-01-01/run_id=x/reference_raw.ndjson.gz',
                    'ticker-catalog/api_date=2025-01-01/run_id=x/daily_stocks_raw.ndjson.gz',
                    'ticker-catalog/api_date=2025-13-01/run_id=x/reference_raw.ndjson.gz']:
            with self.assertRaises(ValueError): date_key(key)

    def test_selected_price_is_warehouse_run_not_latest_lexical_run(self):
        old=lambda run:f'daily-stock-prices/api_date=2025-01-02/run_id={run}/daily_stocks_raw.ndjson.gz'
        entries=[dict(old_key=old(run),new_key=date_key(old(run))) for run in ['zzz','aaa']]
        selected=choose_current(entries,[dict(api_date='2025-01-02',run_id='aaa')],[])
        self.assertEqual(selected[date_key(old('aaa'))],old('aaa'))
        with self.assertRaisesRegex(RuntimeError,'Cannot determine'):
            choose_current(entries,[dict(api_date='2025-01-02',run_id='missing')],[])


class ChangedFileTest(unittest.TestCase):
    def test_price_overwrite_detected_before_raw_transaction(self):
        warehouse=SnowflakeClient.__new__(SnowflakeClient)
        warehouse.cursor=Mock()
        warehouse.cursor.fetchone.side_effect=[(2,),(2,)]
        with patch('src.snowflake_client.SNOWFLAKE',dict(database='MARKET',schema='RAW',s3_stage='MARKET.RAW.PRICES')), \
             patch('src.snowflake_client.AWS',dict(s3_prefix='daily-stock-prices')):
            with self.assertRaisesRegex(ValueError,'changed before loading'):
                warehouse.replace_s3_object_for_date('daily-stock-prices/api_date=2025-01-02/daily_stocks_raw.ndjson.gz',
                                                    '2025-01-02',2,'expected-run')
        sql=[c.args[0] for c in warehouse.cursor.execute.call_args_list]
        self.assertNotIn('BEGIN',sql)
        self.assertFalse(any(s.startswith('DELETE') for s in sql))

    def test_reference_overwrite_detected_before_raw_transaction(self):
        cursor=Mock();cursor.fetchone.side_effect=[(1,),(1,)]
        with patch('src.reference_load.SNOWFLAKE',dict(database='MARKET')),patch('src.reference_load.S3RawClient') as archive:
            archive.return_value.archive_dataframe.return_value=dict(
                key='ticker-catalog/api_date=2025-01-02/reference_raw.ndjson.gz',sha256='sha')
            with self.assertRaisesRegex(ValueError,'changed before loading'):
                ReferenceWarehouse(SimpleNamespace(cursor=cursor)).load([{'ticker':'AAA'}],'2025-01-02','polygon_ticker_catalog')
        self.assertNotIn('BEGIN',[c.args[0] for c in cursor.execute.call_args_list])

    def test_batch_rejects_same_count_from_wrong_run(self):
        cursor=Mock();cursor.fetchall.return_value=[('2025-01-02','polygon_ticker_catalog','wrong',1)]
        with patch('src.backfill_batches.archive_item',return_value=dict(date='2025-01-02',source='polygon_ticker_catalog',
                run_id='expected',row_count=1,key='ticker-catalog/api_date=2025-01-02/reference_raw.ndjson.gz')):
            with self.assertRaisesRegex(ValueError,'count mismatch'):
                land_batch(SimpleNamespace(cursor=cursor),[([{}],'2025-01-02','polygon_ticker_catalog',1)])
        self.assertNotIn('BEGIN',[c.args[0] for c in cursor.execute.call_args_list])


class VersionedS3:
    def __init__(self):
        self.objects={}
        self.copies=0
        self.fail_after_copy=False

    def add(self,key,body):
        v='v'+str(sum(len(v) for v in self.objects.values())+1)
        self.objects.setdefault(key,[]).append((v,body))
        return v

    def get_paginator(self,name):
        return self

    def paginate(self,Bucket,Prefix):
        yield {'Versions':[dict(Key=k,VersionId=v,IsLatest=i==len(values)-1)
                           for k,values in self.objects.items() if k.startswith(Prefix)
                           for i,(v,b) in reversed(list(enumerate(values)))]}

    def copy_object(self,**kw):
        src=kw['CopySource']
        body=next(b for v,b in self.objects[src['Key']] if v==src['VersionId'])
        version=self.add(kw['Key'],body);self.copies+=1
        if self.fail_after_copy:
            self.fail_after_copy=False
            raise RuntimeError('lost response')
        return {'VersionId':version}

    def get_object(self,Bucket,Key,VersionId):
        body=next(b for v,b in self.objects[Key] if v==VersionId)
        return dict(Body=io.BytesIO(body),Metadata={'sha256':hashlib.sha256(body).hexdigest()},ServerSideEncryption='AES256')

    def get_object_tagging(self,**kwargs):
        return {'TagSet':[]}


class DateMigrationTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.s3=VersionedS3()
        self.m=DateArchiveMigration(self.s3,Mock(),'fixture','MARKET',Path(self.temp.name)/'journal.json')
        self.entries=[]
        for run,body in [('old',b'old'),('current',b'current')]:
            key=f'daily-stock-prices/api_date=2025-01-02/run_id={run}/daily_stocks_raw.ndjson.gz'
            version=self.s3.add(key,body)
            self.entries.append(dict(old_key=key,new_key=date_key(key),version=version,etag='etag',size=len(body),sha256=hashlib.sha256(body).hexdigest()))

    def test_copy_keeps_all_versions_with_current_last_and_rerun_is_noop(self):
        first=self.m.copy_group(self.entries)
        self.assertEqual(self.s3.copies,2)
        for entry,version in first: entry['destination_version']=version
        self.assertEqual(self.m.copy_group(self.entries),first)
        self.assertEqual(self.s3.copies,2)
        self.assertEqual(self.s3.objects[self.entries[0]['new_key']][-1][1],b'current')

    def test_lost_copy_response_is_reconciled_without_duplicate_version(self):
        self.s3.fail_after_copy=True
        with self.assertRaisesRegex(RuntimeError,'lost response'): self.m.copy_group(self.entries)
        self.m.copy_group(self.entries)
        self.assertEqual(self.s3.copies,2)

    def test_conflicting_destination_is_never_overwritten(self):
        self.s3.add(self.entries[0]['new_key'],b'conflict')
        with self.assertRaisesRegex(RuntimeError,'bytes differ'): self.m.copy_group(self.entries)
        self.assertEqual(self.s3.copies,0)

    def test_corrupt_source_is_detected(self):
        self.entries[0]['sha256']='incorrect'
        with self.assertRaisesRegex(RuntimeError,'bytes differ'): self.m.copy_group(self.entries)

    def test_wrong_visible_version_fails(self):
        results=self.m.copy_group(self.entries)
        for entry,version in results: entry['destination_version']=version
        self.m.record=dict(entries=self.entries,selected={self.entries[0]['new_key']:self.entries[0]['old_key']})
        with self.assertRaisesRegex(RuntimeError,'Visible archive'): self.m.assert_destinations()

    def test_metadata_rolls_back_if_non_path_data_changes(self):
        self.m.record=dict(entries=self.entries,copied=['a','b'],baseline={'rows':1},phases=[])
        with patch.object(self.m,'assert_originals_unchanged'),patch.object(self.m,'assert_destinations'), \
             patch.object(self.m,'verify_stage_data',return_value={}), \
             patch.object(self.m,'snapshot',side_effect=[{'rows':1},{'rows':2}]):
            with self.assertRaisesRegex(RuntimeError,'Unexpected warehouse change'): self.m.metadata()
        sql=[c.args[0] for c in self.m.cur.execute.call_args_list]
        self.assertIn('ROLLBACK',sql)
        self.assertNotIn('COMMIT',sql)

    def test_hide_requires_prior_verification(self):
        self.m.record=dict(phases=[])
        with self.assertRaisesRegex(RuntimeError,'Verify unchanged'): self.m.hide()

    def test_stage_comparison_includes_retained_legacy_price_sources(self):
        rows=[dict(api_date='2026-01-02',n=1,h='same')]
        self.m.record={}
        with patch('scripts.migrate_date_archives.query',return_value=rows) as query:
            self.m.verify_stage_data()
        raw_sql=query.call_args_list[0].args[1]
        self.assertIn('RAW.DAILY_STOCKS_RAW GROUP BY',raw_sql)
        self.assertNotIn('WHERE SOURCE=',raw_sql)


if __name__=='__main__': unittest.main()
