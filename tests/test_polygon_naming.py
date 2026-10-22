import copy
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock,patch

import pandas as pd

from src.provider_names import SOURCE_ALIASES,canonical_source,canonical_source_sql,polygon_archive_key
from src.s3_client import S3RawClient
from scripts.migrate_polygon_naming import PolygonMigration,preserve_cache,bridge_view_ddl
from scripts.check_polygon_rename_plan import check_rename_plan


class NamingTest(unittest.TestCase):
    def test_only_operational_source_names_are_translated(self):
        for old,new in SOURCE_ALIASES.items():
            self.assertEqual(canonical_source(old),new)
            self.assertEqual(canonical_source(new),new)
        self.assertEqual(canonical_source('polygon_grouped_daily'),'polygon_grouped_daily')
        self.assertEqual(canonical_source('other-provider'),'other-provider')

    def test_key_mapping_preserves_date_run_and_filename(self):
        suffix='/api_date=2025-12-31/run_id=reference-123/reference_raw.ndjson.gz'
        for old,new in SOURCE_ALIASES.items():
            self.assertEqual(polygon_archive_key('raw/massive/reference/'+old+suffix),
                             'raw/polygon/reference/'+new+suffix)
        with self.assertRaises(ValueError):polygon_archive_key('unrelated/archive')

    def test_preserved_archive_replays_with_original_checksum(self):
        frame=pd.DataFrame([dict(API_DATE='2025-12-31',INGESTED_AT='2026-01-01',
            SOURCE='massive_ticker_catalog',RAW_PAYLOAD='{"name":"Original company"}')])
        body=S3RawClient._encode_dataframe(frame)
        s3=Mock()
        s3.get_object.return_value={'Body':io.BytesIO(body)}
        archive=S3RawClient(bucket='fixture',client=s3)
        restored=archive.read_dataframe('new-key',hashlib.sha256(body).hexdigest())
        self.assertEqual(restored.loc[0,'SOURCE'],'polygon_ticker_catalog')
        self.assertEqual(restored.loc[0,'RAW_PAYLOAD'],frame.loc[0,'RAW_PAYLOAD'])

    def test_copy_translation_is_executable_for_both_legacy_sources(self):
        import duckdb
        for old,new in SOURCE_ALIASES.items():
            sql=canonical_source_sql('source')
            result=duckdb.sql(f"SELECT {sql} FROM (SELECT '{old}' AS source)").fetchone()[0]
            self.assertEqual(result,new)

    def test_bridge_qualifies_real_get_ddl_format_and_preserves_grants(self):
        ddl="create or replace view STG_MARKET__CATALOG(\n TICKER\n) as (\n SELECT ticker FROM MARKET.RAW.REFERENCE_RAW WHERE SOURCE = 'massive_ticker_catalog');"
        bridge=bridge_view_ddl(ddl,'MARKET','STG_MARKET__CATALOG')
        self.assertIn('view MARKET.STAGING.STG_MARKET__CATALOG(',bridge)
        self.assertIn('COPY GRANTS AS (',bridge)
        self.assertIn("SOURCE IN ('massive_ticker_catalog','polygon_ticker_catalog')",bridge)


class CacheTest(unittest.TestCase):
    def test_sqlite_backup_resume_and_conflict_preserve_original(self):
        with tempfile.TemporaryDirectory() as directory:
            old=Path(directory)/'massive-reference.sqlite3';new=Path(directory)/'polygon-reference.sqlite3'
            with sqlite3.connect(old) as c:
                c.execute('CREATE TABLE responses (cache_key TEXT PRIMARY KEY,payload TEXT)')
                c.execute('INSERT INTO responses VALUES (?,?)',('cached-url','{"results":[1]}'))
            first=preserve_cache(old,new)
            self.assertEqual(first['rows'],1)
            self.assertEqual(preserve_cache(old,new),first)
            self.assertTrue(old.is_file())
            with sqlite3.connect(new) as c:c.execute("UPDATE responses SET payload='different'")
            with self.assertRaisesRegex(RuntimeError,'conflicts'):preserve_cache(old,new)
            with sqlite3.connect(old) as c:
                self.assertEqual(c.execute('SELECT payload FROM responses').fetchone()[0],'{"results":[1]}')


class CopyResumeTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.s3=Mock();self.cursor=Mock()
        self.m=PolygonMigration(self.s3,self.cursor,'fixture','MARKET',Path(self.temp.name)/'journal.json')
        self.entry=dict(old_key='raw/massive/reference/massive_ticker_catalog/file',
            new_key='raw/polygon/reference/polygon_ticker_catalog/file',version='original',
            etag='etag',size=3,sha256=hashlib.sha256(b'abc').hexdigest())
        self.m.record=dict(entries=[self.entry],copied=[],hidden=[],phases=[],baseline={})

    def test_copy_resumes_without_recopying_verified_objects(self):
        with patch.object(self.m,'assert_originals_unchanged'), \
             patch.object(self.m,'verify_entry',return_value=self.entry['old_key']) as verify:
            self.m.copy();self.m.copy()
        self.assertEqual(len(self.m.record['copied']),1)
        self.assertTrue(all(c.kwargs['copy'] for c in verify.call_args_list))

    def test_existing_conflicting_destination_is_never_overwritten(self):
        self.s3.get_object.side_effect=[{'Body':io.BytesIO(b'abc')},{'Body':io.BytesIO(b'wrong')}]
        with patch.object(self.m,'current_object',return_value={'VersionId':'destination'}):
            with self.assertRaisesRegex(RuntimeError,'conflicts'):self.m.verify_entry(self.entry,copy=True)
        self.s3.copy_object.assert_not_called()

    def test_matching_destination_is_verified_without_copy(self):
        self.s3.get_object.side_effect=[{'Body':io.BytesIO(b'abc')},{'Body':io.BytesIO(b'abc')}]
        self.s3.get_object_tagging.return_value={'TagSet':[]}
        with patch.object(self.m,'current_object',return_value={'VersionId':'destination'}):
            self.m.verify_entry(self.entry,copy=True)
        self.s3.copy_object.assert_not_called()

    def test_hide_requires_verification(self):
        with self.assertRaisesRegex(RuntimeError,'Final verification'):self.m.hide()
        self.s3.delete_object.assert_not_called()

    def test_hide_stops_if_warehouse_changed_since_verification(self):
        self.m.record['phases']=['verified']
        with patch('scripts.migrate_polygon_naming.warehouse_snapshot',return_value={'changed':True}):
            with self.assertRaisesRegex(RuntimeError,'Warehouse changed'):self.m.hide()
        self.s3.delete_object.assert_not_called()

    def test_journal_cannot_be_replayed_in_another_snowflake_account(self):
        self.m.record.update(bucket='fixture',database='MARKET',account='original-account')
        self.m.save()
        self.cursor.fetchone.return_value=('different-account',)
        with self.assertRaisesRegex(RuntimeError,'different Snowflake account'):
            PolygonMigration(self.s3,self.cursor,'fixture','MARKET',self.m.path)

    def test_metadata_requires_copy_and_bridge(self):
        with self.assertRaisesRegex(RuntimeError,'Copy verification'):self.m.metadata()
        self.cursor.execute.assert_not_called()

    def test_hide_adds_only_one_recoverable_marker_and_rerun_is_safe(self):
        self.m.record['phases']=['verified']
        self.s3.delete_object.return_value={'DeleteMarker':True,'VersionId':'marker-1'}
        self.s3.list_objects_v2.return_value={}
        versions=[{'Key':self.entry['old_key'],'VersionId':'original','IsLatest':True}]
        with patch.object(self.m,'versions',return_value=(versions,[])), \
             patch('scripts.migrate_polygon_naming.warehouse_snapshot',return_value={}), \
             patch.object(self.m,'assert_originals_unchanged'), \
             patch.object(self.m,'current_object',return_value={'VersionId':'original'}), \
             patch.object(self.m,'verify_entry'):
            self.m.hide();self.m.hide()
        self.assertEqual(self.s3.delete_object.call_count,1)
        self.assertEqual(self.s3.delete_object.call_args.kwargs,{'Bucket':'fixture','Key':self.entry['old_key']})
        self.assertEqual(self.entry['delete_marker'],'marker-1')

    def test_lost_delete_response_is_reconciled_without_deleting_again(self):
        self.m.record['phases']=['verified'];self.entry['hide_intent']=True
        self.s3.list_objects_v2.return_value={}
        versions=[{'Key':self.entry['old_key'],'VersionId':'original','IsLatest':False}]
        markers=[{'Key':self.entry['old_key'],'VersionId':'recovered-marker','IsLatest':True}]
        with patch.object(self.m,'versions',return_value=(versions,markers)), \
             patch('scripts.migrate_polygon_naming.warehouse_snapshot',return_value={}), \
             patch.object(self.m,'assert_originals_unchanged'):
            self.m.hide()
        self.s3.delete_object.assert_not_called()
        self.assertEqual(self.entry['delete_marker'],'recovered-marker')

    def test_interrupted_copy_retains_completed_batch_for_resume(self):
        self.m.record['entries']=[dict(self.entry,old_key=str(i)) for i in range(33)]
        def verify(entry,copy):
            if entry['old_key']=='32':raise RuntimeError('interrupted')
            return entry['old_key']
        with patch.object(self.m,'assert_originals_unchanged'),patch.object(self.m,'verify_entry',side_effect=verify):
            with self.assertRaisesRegex(RuntimeError,'interrupted'):self.m.copy()
        self.assertEqual(len(json.loads(self.m.path.read_text())['copied']),32)


class RenamePlanTest(unittest.TestCase):
    def setUp(self):
        self.address='snowflake_stage_external_s3.reference'
        self.expected={self.address:{'name':'POLYGON_REFERENCE_STAGE','url':'s3://fixture/raw/polygon/reference/'}}
        self.plan=dict(complete=True,resource_changes=[dict(address=self.address,type='snowflake_stage_external_s3',
            change=dict(actions=['update'],before=dict(name='MASSIVE_REFERENCE_STAGE',url='s3://fixture/raw/massive/reference/',storage_integration='same'),
                after=dict(name='POLYGON_REFERENCE_STAGE',url='s3://fixture/raw/polygon/reference/',storage_integration='same'),after_unknown={}))])

    def test_expected_in_place_update_passes(self):
        self.assertEqual(len(check_rename_plan(self.plan,self.expected)),1)

    def test_rejects_replacement_unrelated_mutation_drift_and_import(self):
        cases=[('actions',['delete','create']),('replace_paths',[['name']]),('importing',{'id':'stage'})]
        for field,value in cases:
            p=copy.deepcopy(self.plan);p['resource_changes'][0]['change'][field]=value
            with self.subTest(field=field),self.assertRaises(ValueError):check_rename_plan(p,self.expected)
        with self.assertRaises(ValueError):check_rename_plan(dict(self.plan,resource_drift=[{}]),self.expected)
        p=copy.deepcopy(self.plan);p['resource_changes'][0]['address']='snowflake_database.market'
        with self.assertRaises(ValueError):check_rename_plan(p,self.expected)

    def test_rejects_changed_or_unknown_connection_settings(self):
        for value in ('other-integration',None):
            p=copy.deepcopy(self.plan)
            c=p['resource_changes'][0]['change'];c['after']['storage_integration']=value
            if value is None:c['after_unknown']['storage_integration']=True
            with self.subTest(value=value),self.assertRaises(ValueError):check_rename_plan(p,self.expected)


if __name__=='__main__':unittest.main()
