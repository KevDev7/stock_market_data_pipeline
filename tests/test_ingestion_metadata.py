import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts import migrate_ingestion_metadata as migration
from src.ingestion_metadata import ensure_manifest, require_metadata_migrated
from src.reference_load import ReferenceWarehouse


class MetadataSetupTest(unittest.TestCase):
    def test_legacy_manifest_stops_setup_before_any_create(self):
        for name in ('REFERENCE_MANIFEST', 'PRICE_REFRESH_MANIFEST'):
            with self.subTest(name=name):
                cursor = Mock()
                cursor.fetchall.return_value = [(name,)]
                with self.assertRaisesRegex(RuntimeError, 'migrate_ingestion_metadata'):
                    ensure_manifest(cursor, 'MARKET', 'REFERENCE_MANIFEST')
                self.assertTrue(all(c.args[0].startswith('SELECT') for c in cursor.execute.call_args_list))

    def test_fresh_environment_creates_manifest_in_admin(self):
        cursor = Mock()
        cursor.fetchall.return_value = []
        ensure_manifest(cursor, 'MARKET', 'REFERENCE_MANIFEST')
        statements = [c.args[0] for c in cursor.execute.call_args_list]
        self.assertIn('CREATE SCHEMA IF NOT EXISTS MARKET.ADMIN', statements)
        self.assertTrue(statements[-1].startswith('CREATE TABLE IF NOT EXISTS MARKET.ADMIN.REFERENCE_MANIFEST'))

    def test_reference_completed_dates_read_admin_by_source(self):
        cursor = Mock()
        cursor.fetchall.return_value = [('2025-03-31',)]
        with patch('src.reference_load.SNOWFLAKE', {'database':'MARKET'}):
            warehouse = ReferenceWarehouse(Mock(cursor=cursor))
            self.assertEqual(warehouse.completed('polygon_ticker_catalog'), {'2025-03-31'})
        sql, params = cursor.execute.call_args.args
        self.assertIn('MARKET.ADMIN.REFERENCE_MANIFEST', sql)
        self.assertEqual(params, ('polygon_ticker_catalog',))

    def test_empty_reference_load_commits_raw_and_admin_manifest_together(self):
        cursor = Mock()
        cursor.fetchone.return_value = (0,)
        with patch('src.reference_load.SNOWFLAKE', {'database':'MARKET'}), \
             patch('src.reference_load.S3RawClient') as archive:
            archive.return_value.archive_dataframe.return_value = {
                'key':'raw/polygon/reference/polygon_ticker_overview/api_date=2025-01-02/test.ndjson.gz',
                'sha256':'fixture'}
            warehouse = ReferenceWarehouse(Mock(cursor=cursor))
            warehouse.load([], '2025-01-02', 'polygon_ticker_overview', requested_count=1)
        sql = [c.args[0] for c in cursor.execute.call_args_list]
        self.assertFalse(any(s.startswith('COPY') for s in sql))
        deletes = [s for s in sql if s.startswith('DELETE')]
        self.assertEqual(len(deletes), 2)
        self.assertIn('MARKET.RAW.REFERENCE_RAW', deletes[0])
        self.assertIn('MARKET.ADMIN.REFERENCE_MANIFEST', deletes[1])
        self.assertLess(sql.index('BEGIN'), sql.index(deletes[0]))
        self.assertGreater(sql.index('COMMIT'), sql.index(deletes[1]))

    def test_reference_manifest_write_failure_rolls_back(self):
        cursor = Mock()
        cursor.fetchone.return_value = (0,)
        def execute(sql, *args):
            if sql.startswith('INSERT INTO MARKET.ADMIN.REFERENCE_MANIFEST'):
                raise RuntimeError('manifest write failed')
        cursor.execute.side_effect = execute
        with patch('src.reference_load.SNOWFLAKE', {'database':'MARKET'}), \
             patch('src.reference_load.S3RawClient') as archive:
            archive.return_value.archive_dataframe.return_value = {
                'key':'raw/polygon/reference/polygon_ticker_overview/test.ndjson.gz', 'sha256':'fixture'}
            warehouse = ReferenceWarehouse(Mock(cursor=cursor))
            with self.assertRaisesRegex(RuntimeError, 'manifest write failed'):
                warehouse.load([], '2025-01-02', 'polygon_ticker_overview')
        sql = [c.args[0] for c in cursor.execute.call_args_list]
        self.assertIn('ROLLBACK', sql)
        self.assertNotIn('COMMIT', sql)


class MetadataMigrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.journal = Path(self.temp.name) / 'journal.json'
        self.cursor = Mock()
        self.tables = {name: dict(location='RAW', snapshot=dict(owner='ACCOUNTADMIN',
            rows=3, fingerprint='123', columns=[{'name':'API_DATE'}], grants=[]))
            for name in migration.MANIFEST_DDL}
        self.protected = dict(checkpoints={'rows':5, 'fingerprint':'456'}, data_counts=[])
        self.failure_on_second = False
        self.moves = 0

        def execute(sql, *args):
            if sql.startswith('ALTER TABLE'):
                self.moves += 1
                if self.failure_on_second and self.moves == 2:
                    raise RuntimeError('interrupted second move')
                name = sql.split()[2].split('.')[-1]
                self.tables[name]['location'] = 'ADMIN'
        self.cursor.execute.side_effect = execute
        for name, replacement in [
            ('locations', lambda *a: copy.deepcopy(self.tables)),
            ('protected_snapshot', lambda *a: copy.deepcopy(self.protected)),
            ('check_permissions', lambda *a: {'create_table_access':True}),
            ('query', lambda *a: [dict(account='test-account',user='loader',role='ACCOUNTADMIN')]),
            ('table_snapshot', lambda c,d,s,n: copy.deepcopy(self.tables[n]['snapshot'])
             if self.tables[n]['location'] == s else None),
        ]:
            p = patch.object(migration, name, side_effect=replacement)
            p.start()
            self.addCleanup(p.stop)

    def run_migration(self, execute=True):
        return migration.migrate(self.cursor, 'MARKET', execute=execute,
            journal_path=self.journal, idle_confirmed=True)

    def test_preview_has_no_ddl_or_journal(self):
        report = self.run_migration(execute=False)
        self.assertFalse(report['executed'])
        self.cursor.execute.assert_not_called()
        self.assertFalse(self.journal.exists())

    def test_first_execution_and_verified_rerun(self):
        self.assertTrue(self.run_migration()['verified'])
        self.assertEqual(self.moves, 2)
        self.assertEqual(self.journal.stat().st_mode & 0o777, 0o600)
        self.assertTrue(self.run_migration()['verified'])
        self.assertEqual(self.moves, 2)

    def test_partial_completion_resumes_without_repeating_first_move(self):
        self.failure_on_second = True
        with self.assertRaisesRegex(RuntimeError, 'interrupted'):
            self.run_migration()
        self.assertEqual(len(json.loads(self.journal.read_text())['completed']), 1)
        self.failure_on_second = False
        self.assertTrue(self.run_migration()['verified'])
        self.assertEqual(self.moves, 3)

    def test_rejects_changed_contents_on_resume(self):
        self.run_migration()
        self.tables['REFERENCE_MANIFEST']['snapshot']['rows'] += 1
        with self.assertRaisesRegex(RuntimeError, 'differs from the recorded baseline'):
            self.run_migration()
        self.assertEqual(self.moves, 2)

    def test_destination_without_journal_is_not_assumed_verified(self):
        self.tables['REFERENCE_MANIFEST']['location'] = 'ADMIN'
        with self.assertRaisesRegex(RuntimeError, 'original journal'):
            self.run_migration()
        self.assertEqual(self.moves, 0)

    def test_execution_requires_idle_confirmation(self):
        with self.assertRaisesRegex(RuntimeError, 'idle-confirmed'):
            migration.migrate(self.cursor, 'MARKET', execute=True, journal_path=self.journal)
        self.assertEqual(self.moves, 0)

    def test_both_tables_checked_before_rename(self):
        with patch.object(migration, 'locations', side_effect=RuntimeError('destination conflict')):
            with self.assertRaisesRegex(RuntimeError, 'destination conflict'):
                self.run_migration()
        self.assertEqual(self.moves, 0)

    def test_permission_failure_prevents_both_moves(self):
        with patch.object(migration, 'check_permissions', side_effect=RuntimeError('no ownership')):
            with self.assertRaisesRegex(RuntimeError, 'no ownership'):
                self.run_migration()
        self.assertEqual(self.moves, 0)
        self.assertFalse(self.journal.exists())


class LocationConflictTest(unittest.TestCase):
    def test_source_and_destination_conflict(self):
        with patch.object(migration, 'table_snapshot', return_value={'rows':1}):
            with self.assertRaisesRegex(RuntimeError, 'Both RAW and ADMIN'):
                migration.locations(Mock(), 'MARKET')

    def test_no_table_is_not_recreated(self):
        with patch.object(migration, 'table_snapshot', return_value=None):
            with self.assertRaisesRegex(RuntimeError, 'does not create tables'):
                migration.locations(Mock(), 'MARKET')


if __name__ == '__main__':
    unittest.main()
