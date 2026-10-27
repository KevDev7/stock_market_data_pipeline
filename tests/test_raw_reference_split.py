import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import duckdb
import sqlglot

from src.raw_reference import (REFERENCE_TABLES, RETIRED_MARKER, ensure_reference_tables,
                               reference_table, require_reference_split)
from src.reference_load import ReferenceWarehouse
from scripts import migrate_reference_raw_tables as migration


class SetupTest(unittest.TestCase):
    def test_fresh_setup_creates_only_two_endpoint_tables(self):
        cur=Mock(); cur.fetchall.return_value=[]
        ensure_reference_tables(cur,'MARKET')
        ddl=[c.args[0] for c in cur.execute.call_args_list if c.args[0].startswith('CREATE')]
        self.assertEqual(len(ddl),2)
        self.assertTrue(any('.RAW.TICKER_CATALOG_RAW' in s for s in ddl))
        self.assertTrue(any('.RAW.COMPANY_OVERVIEW_RAW' in s for s in ddl))

    def test_unmigrated_and_partially_created_tables_are_rejected_before_ddl(self):
        for extras in ([], [('TICKER_CATALOG_RAW',None,'BASE TABLE')]):
            cur=Mock();cur.fetchall.return_value=[('REFERENCE_RAW',None,'BASE TABLE')]+extras
            with self.assertRaisesRegex(RuntimeError,'migrate_reference_raw_tables'):
                ensure_reference_tables(cur,'MARKET')
            self.assertFalse(any(c.args[0].startswith('CREATE') for c in cur.execute.call_args_list))

    def test_retirement_marker_requires_both_valid_destinations(self):
        cur=Mock();cur.fetchall.return_value=[('REFERENCE_RAW',RETIRED_MARKER,'BASE TABLE')]
        with self.assertRaises(RuntimeError): require_reference_split(cur,'MARKET')
        rows=[('REFERENCE_RAW',RETIRED_MARKER,'BASE TABLE')]+[(t,None,'BASE TABLE') for t in REFERENCE_TABLES.values()]
        columns=[('API_DATE','DATE','COLUMN','Y',None),('RUN_ID','VARCHAR(16777216)','COLUMN','Y',None),
                 ('SOURCE','VARCHAR(16777216)','COLUMN','Y',None),('RAW_PAYLOAD','VARCHAR(16777216)','COLUMN','Y',None),
                 ('INGESTED_AT','TIMESTAMP_NTZ(9)','COLUMN','Y',None)]
        cur.fetchall.side_effect=[rows,columns,columns]
        require_reference_split(cur,'MARKET')
        cur.fetchall.side_effect=[rows,columns[:-1]]
        with self.assertRaisesRegex(RuntimeError,'columns'): require_reference_split(cur,'MARKET')

    def test_source_mapping_rejects_unknown_inputs(self):
        self.assertEqual(reference_table('MARKET','polygon_ticker_overview'),'MARKET.RAW.COMPANY_OVERVIEW_RAW')
        with self.assertRaises(ValueError): reference_table('MARKET','unknown')
        with self.assertRaises(ValueError): reference_table('MARKET; DROP DATABASE X','polygon_ticker_catalog')


class ExactComparisonTest(unittest.TestCase):
    def test_multiset_comparison_detects_duplicate_loss_and_changed_payload(self):
        db=duckdb.connect(':memory:')
        self.addCleanup(db.close)
        for name in ('old_data','new_data'):
            db.execute(f'CREATE TABLE {name}(API_DATE DATE,RUN_ID VARCHAR,SOURCE VARCHAR,RAW_PAYLOAD VARCHAR,INGESTED_AT TIMESTAMP)')
        db.execute("INSERT INTO old_data VALUES ('2025-01-01','r','polygon_ticker_catalog','{}','2025-01-02')")
        db.execute('INSERT INTO old_data SELECT * FROM old_data')
        db.execute('INSERT INTO new_data SELECT * FROM old_data')
        class Cursor:
            def execute(self,sql,params):
                sql=sql.replace('MARKET.RAW.REFERENCE_RAW','old_data').replace('MARKET.RAW.TICKER_CATALOG_RAW','new_data')
                sql=sql.replace('%s',"'"+params[0]+"'")
                db.execute(sqlglot.transpile(sql,read='snowflake',write='duckdb')[0])
            def fetchone(self): return db.fetchone()
        migration.exact_equal(Cursor(),'MARKET','polygon_ticker_catalog')
        db.execute('DELETE FROM new_data')
        db.execute('INSERT INTO new_data SELECT DISTINCT * FROM old_data')
        with self.assertRaisesRegex(RuntimeError,'Exact record'): migration.exact_equal(Cursor(),'MARKET','polygon_ticker_catalog')
        db.execute("UPDATE new_data SET RAW_PAYLOAD='changed'")
        with self.assertRaises(RuntimeError): migration.exact_equal(Cursor(),'MARKET','polygon_ticker_catalog')


class ReferenceRoutingTest(unittest.TestCase):
    def load(self, source, checks):
        cur=Mock();cur.fetchone.side_effect=checks
        warehouse=ReferenceWarehouse(Mock(cursor=cur))
        from src.archive_layout import REFERENCE_ARCHIVES
        prefix=REFERENCE_ARCHIVES[source][0]
        with patch('src.reference_load.S3RawClient') as archive:
            archive.return_value.archive_dataframe.return_value={'key':prefix+'/api_date=2025-01-02/reference_raw.ndjson.gz','sha256':'fixture'}
            warehouse.load([{'ticker':'AAA'}],'2025-01-02',source)
        return cur

    def test_each_endpoint_uses_only_its_own_destination(self):
        for source,table in REFERENCE_TABLES.items():
            cur=self.load(source,[(1,),(0,)])
            sql=[c.args[0] for c in cur.execute.call_args_list]
            self.assertTrue(any('LIKE MARKET.RAW.'+table in s for s in sql))
            self.assertTrue(any(s.startswith('INSERT INTO MARKET.RAW.'+table) for s in sql))
            self.assertFalse(any('MARKET.RAW.REFERENCE_RAW' in s for s in sql))
            self.assertIn('COMMIT',sql)

    def test_count_and_source_date_run_mismatches_stop_before_replacement(self):
        for checks in ([(0,)],[(1,),(1,)]):
            cur=Mock();cur.fetchone.side_effect=checks
            warehouse=ReferenceWarehouse(Mock(cursor=cur))
            with patch('src.reference_load.S3RawClient') as archive:
                archive.return_value.archive_dataframe.return_value={'key':'ticker-catalog/api_date=2025-01-02/reference_raw.ndjson.gz','sha256':'fixture'}
                with self.assertRaises(ValueError): warehouse.load([{'ticker':'AAA'}],'2025-01-02','polygon_ticker_catalog')
            sql=[c.args[0] for c in cur.execute.call_args_list]
            self.assertNotIn('BEGIN',sql)
            self.assertFalse(any(s.startswith('DELETE') for s in sql))

    def test_known_issuers_reads_overview_table(self):
        cur=Mock();cur.fetchall.return_value=[('123','2024-01-02')]
        self.assertEqual(ReferenceWarehouse(Mock(cursor=cur)).known_issuers(),{'123':'2024-01-02'})
        self.assertIn('MARKET.RAW.COMPANY_OVERVIEW_RAW',cur.execute.call_args.args[0])


class MigrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.cur=Mock();self.path=Path(self.tmp.name)/'journal.json'
        self.m=migration.SplitMigration(self.cur,'MARKET',self.path)
        self.original=dict(owner='ACCOUNTADMIN',comment=None,columns=[],grants=[],rows=2,fingerprint='1',sources=[],dates=[])
        self.preview=dict(identity={'account':'a','role':'ACCOUNTADMIN'},original=self.original,
                          destinations={s:None for s in REFERENCE_TABLES},schema_grants=[],dependencies=[])

    def record(self, completed):
        import hashlib
        definitions=migration.render_views('MARKET')
        self.m.record=dict(migration=migration.MIGRATION,database='MARKET',**copy.deepcopy(self.preview),
            destination_marker='owned-marker',completed=completed,protected={},views={},
            code_hash=hashlib.sha256(json.dumps(definitions,sort_keys=True).encode()).hexdigest())

    def test_preview_does_not_write_journal_or_execute_ddl(self):
        with patch.object(self.m,'preview',return_value=self.preview):
            self.assertEqual(self.m.run('prepare')['phase'],'preview')
        self.assertFalse(self.path.exists());self.cur.execute.assert_not_called()

    def test_execution_requires_idle_and_prepare(self):
        with patch.object(self.m,'preview',return_value=self.preview):
            with self.assertRaisesRegex(RuntimeError,'stopped writers'): self.m.run('prepare',execute=True)
            with self.assertRaisesRegex(RuntimeError,'prepare first'):
                self.m.run('populate',execute=True,idle_confirmed=True)

    def test_unjournaled_destination_conflict_and_wrong_owner(self):
        with patch.object(self.m,'identity',return_value=self.preview['identity']), \
             patch.object(migration,'source_snapshot',return_value=self.original), \
             patch.object(migration,'destination_snapshot',return_value={'rows':1}), \
             patch.object(migration,'query',return_value=[]):
            with self.assertRaisesRegex(RuntimeError,'without this migration journal'): self.m.preview()
        with patch.object(self.m,'identity',return_value={'role':'OTHER'}), \
             patch.object(migration,'source_snapshot',return_value=self.original), \
             patch.object(migration,'destination_snapshot',return_value=None):
            with self.assertRaisesRegex(RuntimeError,'existing owner'): self.m.preview()

    def test_destination_requires_marker_owner_columns_and_grants(self):
        self.record(['prepare'])
        good={**self.original,'comment':'owned-marker'}
        for changed in ({'comment':'foreign'}, {'owner':'OTHER'}, {'columns':['unexpected']}):
            with patch.object(migration,'destination_snapshot',return_value={**good,**changed}):
                with self.assertRaises(RuntimeError): self.m.check_destination('polygon_ticker_catalog')
        with patch.object(migration,'destination_snapshot',return_value=good):
            self.m.check_destination('polygon_ticker_catalog')

    def test_atomic_population_rolls_back_second_insert_failure(self):
        self.record(['prepare'])
        def execute(sql,*args):
            if sql.startswith('INSERT INTO MARKET.RAW.COMPANY_OVERVIEW_RAW'): raise RuntimeError('second insert')
        self.cur.execute.side_effect=execute
        with patch.object(self.m,'preview',return_value=self.preview),patch.object(self.m,'assert_original'), \
             patch.object(self.m,'check_destination',return_value={'rows':0}),patch.object(self.m,'verify_data'):
            with self.assertRaisesRegex(RuntimeError,'second insert'):
                self.m.run('populate',execute=True,idle_confirmed=True)
        sql=[c.args[0] for c in self.cur.execute.call_args_list]
        self.assertIn('BEGIN',sql);self.assertIn('ROLLBACK',sql);self.assertNotIn('COMMIT',sql)

    def test_success_and_interrupted_commit_rerun_do_not_duplicate_records(self):
        self.record(['prepare'])
        with patch.object(self.m,'preview',return_value=self.preview),patch.object(self.m,'assert_original'), \
             patch.object(self.m,'check_destination',return_value={'rows':0}),patch.object(self.m,'verify_data'):
            self.m.run('populate',execute=True,idle_confirmed=True)
        self.assertIn('populate',self.m.record['completed'])
        self.assertEqual(sum(c.args[0].startswith('INSERT') for c in self.cur.execute.call_args_list),2)
        self.m.record['completed']=['prepare']  # COMMIT succeeded but journal save was interrupted.
        self.cur.reset_mock()
        with patch.object(self.m,'preview',return_value=self.preview),patch.object(self.m,'assert_original'), \
             patch.object(self.m,'check_destination',return_value={'rows':1}),patch.object(self.m,'verify_data') as check:
            self.m.run('populate',execute=True,idle_confirmed=True)
            check.assert_called_once()
        self.assertFalse(any(c.args[0].startswith('INSERT') for c in self.cur.execute.call_args_list))

    def test_partially_populated_destination_cannot_be_overwritten(self):
        self.record(['prepare'])
        with patch.object(self.m,'preview',return_value=self.preview),patch.object(self.m,'assert_original'), \
             patch.object(self.m,'check_destination',side_effect=[{'rows':1},{'rows':0}]), \
             patch.object(self.m,'verify_data',side_effect=RuntimeError('partial content')):
            with self.assertRaisesRegex(RuntimeError,'partial content'):
                self.m.run('populate',execute=True,idle_confirmed=True)
        self.cur.execute.assert_not_called()

    def test_prepare_resumes_missing_second_table_without_replacing_first(self):
        self.record([])
        with patch.object(self.m,'preview',return_value=self.preview),patch.object(self.m,'assert_original'), \
             patch.object(self.m,'check_destination',side_effect=[{'rows':0},{'rows':0},None,{'rows':0}]):
            self.m.run('prepare',execute=True,idle_confirmed=True)
        creates=[c.args[0] for c in self.cur.execute.call_args_list]
        self.assertEqual(len(creates),1)
        self.assertIn('CREATE TABLE MARKET.RAW.COMPANY_OVERVIEW_RAW',creates[0])
        self.assertNotIn('REPLACE',creates[0])

    def test_cutover_resumes_after_second_view_failure_and_preserves_first(self):
        self.record(['prepare','populate'])
        definitions=migration.render_views('MARKET')
        grants=[{'privilege':'OWNERSHIP','granted_to':'ROLE','grantee_name':'ACCOUNTADMIN','grant_option':'true'}]
        self.m.record['views']={name:{'ddl':'old '+name,'grants':grants} for name in definitions}
        actual={name:'old '+name for name in definitions}
        fetched=[None];fail=[True];created=[]
        def execute(sql,params=None):
            if sql.startswith('SELECT GET_DDL'):
                fetched[0]=actual[params[1].split('.')[-1]]
            elif sql.startswith('CREATE OR REPLACE VIEW'):
                name=sql.split()[4].split('.')[-1]
                if name=='STG_MARKET__ISSUER_OBSERVATIONS' and fail[0]:
                    raise RuntimeError('interrupted second view')
                actual[name]=sql;created.append(name)
        self.cur.execute.side_effect=execute
        self.cur.fetchone.side_effect=lambda:(fetched[0],)
        with patch.object(self.m,'preview',return_value=self.preview),patch.object(self.m,'assert_original'), \
             patch.object(self.m,'verify_data'),patch.object(self.m,'verify_protected'), \
             patch.object(migration,'query',return_value=grants):
            with self.assertRaisesRegex(RuntimeError,'interrupted second view'):
                self.m.run('cutover',execute=True,idle_confirmed=True)
            self.assertNotIn('cutover',self.m.record['completed'])
            fail[0]=False
            self.m.run('cutover',execute=True,idle_confirmed=True)
            self.m.run('cutover',execute=True,idle_confirmed=True)
        self.assertEqual(created,list(definitions))
        self.assertIn('cutover',self.m.record['completed'])

    def test_changed_original_and_unexpected_source_stop_migration(self):
        self.record(['prepare'])
        with patch.object(migration,'source_snapshot',return_value={**self.original,'rows':3}):
            with self.assertRaisesRegex(RuntimeError,'Original table changed'): self.m.assert_original()
        with patch.object(migration,'destination_snapshot',return_value=copy.deepcopy(self.original)), \
             patch.object(migration,'query',return_value=[{'source':'unrecognized','record_count':1}]):
            with self.assertRaisesRegex(RuntimeError,'Unexpected or NULL source'):
                migration.source_snapshot(self.cur,'MARKET')


if __name__=='__main__': unittest.main()
