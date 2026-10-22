import copy
import datetime
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts.check_s3_layout_plan import check_layout_plan
from scripts.migrate_s3_layout import LayoutMigration, tracking_structure
from src.archive_layout import PREFIX_MOVES, normalized_key_sql, simplified_archive_key
from src.reference_load import ReferenceWarehouse


class LayoutMappingTest(unittest.TestCase):
    def test_compose_and_airflow_use_the_same_price_prefix(self):
        root=Path(__file__).resolve().parents[1]
        text=(root/'docker-compose.yaml').read_text()
        self.assertIn('AWS_S3_PREFIX: ${AWS_S3_PREFIX:-daily-stock-prices}',text)
        self.assertIn('AIRFLOW_VAR_AWS_S3_PREFIX: ${AWS_S3_PREFIX:-daily-stock-prices}',text)

    def test_all_sources_preserve_date_run_and_filename(self):
        for old,new in PREFIX_MOVES.items():
            suffix='api_date=2025-12-31/run_id=backfill-123/file.ndjson.gz'
            self.assertEqual(simplified_archive_key(old+suffix),new+suffix)

    def test_unknown_or_empty_prefix_is_not_rewritten(self):
        for key in ['raw/polygon/unrelated/file','raw/polygon/grouped-daily/',
                    'daily-stock-prices/file','elsewhere/raw/polygon/grouped-daily/file']:
            with self.subTest(key=key), self.assertRaises(ValueError):
                simplified_archive_key(key)

    def test_sql_mapping_is_anchored_and_preserves_nulls(self):
        import duckdb
        connection=duckdb.connect()
        self.addCleanup(connection.close)
        connection.execute('CREATE MACRO STARTSWITH(a,b) AS starts_with(a,b)')
        for key in [old+'api_date=2025-12-31/run_id=test/archive' for old in PREFIX_MOVES]:
            expression=normalized_key_sql('k')
            actual=connection.sql(f"SELECT {expression} FROM (SELECT '{key}' AS k)").fetchone()[0]
            self.assertEqual(actual,simplified_archive_key(key))
        for key in ['other/raw/polygon/grouped-daily/file',None]:
            value='NULL::VARCHAR' if key is None else "'"+key+"'"
            self.assertEqual(connection.sql(f'SELECT {normalized_key_sql("k")} FROM (SELECT {value} AS k)').fetchone()[0],key)

    def test_single_reference_load_routes_each_source_to_its_own_folder_and_stage(self):
        from src.archive_layout import REFERENCE_ARCHIVES
        for source,(prefix,stage) in REFERENCE_ARCHIVES.items():
            cursor=Mock();cursor.fetchone.side_effect=[(1,), (0,)]
            with patch('src.reference_load.SNOWFLAKE',{'database':'MARKET'}), \
                 patch('src.reference_load.S3RawClient') as archive:
                archive.return_value.archive_dataframe.return_value=dict(
                    key=prefix+'/api_date=2025-12-31/reference_raw.ndjson.gz',sha256='fixture')
                warehouse=ReferenceWarehouse(Mock(cursor=cursor))
                warehouse.load([{'ticker':'AAA'}],'2025-12-31',source)
            archive.assert_called_once_with(prefix=prefix,filename='reference_raw.ndjson.gz')
            copies=[c.args[0] for c in cursor.execute.call_args_list if c.args[0].startswith('COPY')]
            self.assertEqual(len(copies),1)
            self.assertIn('@MARKET.RAW.'+stage+'/api_date=2025-12-31/',copies[0])


class LayoutMigrationTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.s3=Mock();self.cur=Mock()
        self.m=LayoutMigration(self.s3,self.cur,'fixture','MARKET',Path(self.temp.name)/'journal.json')
        self.entry=dict(old_key='raw/polygon/grouped-daily/file',new_key='daily-stock-prices/file',
                        version='v1',etag='etag',size=3,sha256='sha')
        self.m.record=dict(migration='dataset-folders-v1',entries=[self.entry],copied=[],hidden=[],
                           phases=[],baseline={})

    def test_preview_refuses_disabled_versioning(self):
        self.m.record=None;self.s3.get_bucket_versioning.return_value={}
        with self.assertRaisesRegex(RuntimeError,'Versioning'):self.m.inventory()
        self.s3.copy_object.assert_not_called()
        self.cur.execute.assert_not_called()

    def test_tracking_grants_compare_after_journal_reload(self):
        rows=[{'created_on':datetime.datetime(2026,10,22,tzinfo=datetime.timezone.utc)}]
        with patch('scripts.migrate_s3_layout.query',return_value=rows):
            before=tracking_structure(self.cur,'MARKET')
            restored=json.loads(json.dumps(before))
            self.assertEqual(tracking_structure(self.cur,'MARKET'),restored)

    def test_preview_refuses_destination_and_does_not_write_journal(self):
        self.m.record=None;self.s3.get_bucket_versioning.return_value={'Status':'Enabled'}
        self.s3.list_objects_v2.return_value={'Contents':[{'Key':'daily-stock-prices/conflict'}]}
        with patch.object(self.m,'versions',return_value=([{'IsLatest':True}],[])):
            with self.assertRaisesRegex(RuntimeError,'Destination exists'):self.m.inventory()
        self.assertFalse(self.m.path.exists())
        self.s3.copy_object.assert_not_called()

    def test_preview_refuses_historical_versions_without_assuming_they_are_current(self):
        self.m.record=None;self.s3.get_bucket_versioning.return_value={'Status':'Enabled'}
        with patch.object(self.m,'versions',return_value=([{'IsLatest':False}],[])):
            with self.assertRaisesRegex(RuntimeError,'Unexpected source versions'):self.m.inventory()

    def test_metadata_requires_all_verified_copies(self):
        with self.assertRaisesRegex(RuntimeError,'Every archive'):self.m.metadata()
        self.cur.execute.assert_not_called()

    def test_metadata_changes_only_paths_in_three_admin_tables_in_one_transaction(self):
        self.m.record['copied']=[self.entry['old_key']]
        with patch.object(self.m,'assert_originals_unchanged'),patch.object(self.m,'snapshot',return_value={}):
            self.m.metadata();self.m.metadata()
        sql=[c.args[0] for c in self.cur.execute.call_args_list]
        self.assertEqual(sql.count('BEGIN'),1);self.assertEqual(sql.count('COMMIT'),1)
        updates=[s for s in sql if s.startswith('UPDATE')]
        self.assertEqual(len(updates),3)
        self.assertTrue(all('.ADMIN.' in s and 'SET S3_KEY=CASE' in s for s in updates))
        self.assertFalse(any('RAW.' in s or 'SOURCE=' in s for s in updates))

    def test_metadata_rolls_back_if_normalized_contents_change(self):
        self.m.record['copied']=[self.entry['old_key']]
        with patch.object(self.m,'assert_originals_unchanged'), \
             patch.object(self.m,'snapshot',side_effect=[{}, {'changed':True}]):
            with self.assertRaisesRegex(RuntimeError,'other than S3 paths'):self.m.metadata()
        sql=[c.args[0] for c in self.cur.execute.call_args_list]
        self.assertIn('ROLLBACK',sql);self.assertNotIn('COMMIT',sql)

    def test_metadata_resume_after_lost_commit_response_is_idempotent(self):
        self.m.record['copied']=[self.entry['old_key']]
        with patch.object(self.m,'assert_originals_unchanged'),patch.object(self.m,'snapshot',return_value={}), \
             patch.object(self.m,'save',side_effect=[RuntimeError('lost journal'),None]):
            with self.assertRaises(RuntimeError):self.m.metadata()
            self.m.record['phases']=[]
            self.m.metadata()
        self.assertEqual(sum(c.args[0]=='COMMIT' for c in self.cur.execute.call_args_list),2)

    def test_hide_never_deletes_without_final_verification(self):
        with self.assertRaisesRegex(RuntimeError,'Final verification'):self.m.hide()
        self.s3.delete_object.assert_not_called()

    def test_hide_rechecks_snapshot_and_uses_only_recoverable_marker(self):
        self.m.record['phases']=['verified']
        self.s3.delete_object.return_value={'DeleteMarker':True,'VersionId':'marker'}
        self.s3.list_objects_v2.return_value={}
        versions=[dict(Key=self.entry['old_key'],VersionId='v1',IsLatest=True)]
        with patch.object(self.m,'snapshot',return_value={}), \
             patch.object(self.m,'versions',return_value=(versions,[])), \
             patch.object(self.m,'assert_originals_unchanged'), \
             patch.object(self.m,'current_object',return_value={'VersionId':'v1'}), \
             patch.object(self.m,'verify_entry'):
            self.m.hide();self.m.hide()
        self.s3.delete_object.assert_called_once_with(Bucket='fixture',Key=self.entry['old_key'])
        self.assertEqual(self.entry['delete_marker'],'marker')


class LayoutPlanTest(unittest.TestCase):
    def setUp(self):
        self.overview=dict(database='MARKET',schema='RAW',name='POLYGON_COMPANY_OVERVIEW_STAGE',
            url='s3://fixture/company-overview/',storage_integration='same',file_format=[{'json':[]}])
        self.plan=dict(complete=True,resource_changes=[dict(address='snowflake_stage_external_s3.overview[0]',
            type='snowflake_stage_external_s3',change=dict(actions=['create'],before=None,
                after=self.overview,after_unknown={'id':True,'show_output':True}))])

    def test_explicitly_reviewed_stage_creation_passes(self):
        self.assertEqual(check_layout_plan(self.plan,{},self.overview)['creations'],
                         ['snowflake_stage_external_s3.overview[0]'])

    def test_creation_is_rejected_by_default(self):
        with self.assertRaises(ValueError):check_layout_plan(self.plan,{})

    def test_wrong_stage_configuration_unknown_settings_and_additional_creation_rejected(self):
        cases=[]
        p=copy.deepcopy(self.plan);p['resource_changes'][0]['address']='snowflake_database.market';cases.append(p)
        p=copy.deepcopy(self.plan);p['resource_changes'][0]['change']['after']['url']='s3://fixture/';cases.append(p)
        p=copy.deepcopy(self.plan);p['resource_changes'][0]['change']['after_unknown']['url']=True;cases.append(p)
        p=copy.deepcopy(self.plan);p['resource_changes'].append(copy.deepcopy(p['resource_changes'][0]));cases.append(p)
        p=copy.deepcopy(self.plan);p['resource_drift']=[{}];cases.append(p)
        for p in cases:
            with self.subTest(plan=p),self.assertRaises(ValueError):check_layout_plan(p,{},self.overview)


if __name__=='__main__':unittest.main()
