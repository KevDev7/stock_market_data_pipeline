import copy
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.reference_load import ReferenceWarehouse


class ReferenceSetupTest(unittest.TestCase):
    def setUp(self):
        self.cursor = Mock()
        self.config = patch('src.reference_load.SNOWFLAKE', {'database':'MARKET'})
        self.aws = patch('src.reference_load.AWS', {'s3_bucket':'stock-bucket'})
        self.config.start()
        self.aws.start()
        self.addCleanup(self.config.stop)
        self.addCleanup(self.aws.stop)
        self.warehouse = ReferenceWarehouse(SimpleNamespace(cursor=self.cursor))
        self.integration = [
            ['ENABLED','Boolean','true','true'],
            ['STORAGE_PROVIDER','String','S3',''],
            ['STORAGE_ALLOWED_LOCATIONS','List','s3://stock-bucket/raw/massive/reference/','[]'],
            ['STORAGE_BLOCKED_LOCATIONS','List','','[]'],
        ]
        self.stage = [
            ['STAGE_LOCATION','URL','String',json.dumps(['s3://stock-bucket/raw/massive/reference/']), ''],
            ['STAGE_INTEGRATION','STORAGE_INTEGRATION','String','STOCK_MARKET_S3_INTEGRATION',''],
            ['STAGE_FILE_FORMAT','TYPE','String','JSON','CSV'],
            ['STAGE_FILE_FORMAT','COMPRESSION','String','AUTO','AUTO'],
        ]

    def configure(self):
        self.cursor.fetchall.side_effect = [self.integration, self.stage]

    def statements(self):
        return [call.args[0] for call in self.cursor.execute.call_args_list]

    def assert_no_infrastructure_mutations(self):
        for sql in self.statements():
            self.assertNotIn('CREATE STAGE', sql)
            self.assertNotIn('ALTER STORAGE INTEGRATION', sql)
            self.assertNotIn('CREATE STORAGE INTEGRATION', sql)

    def test_setup_creates_only_application_tables_after_checks(self):
        self.configure()
        self.warehouse.setup()
        self.assertTrue(self.statements()[0].startswith('DESC INTEGRATION'))
        self.assertTrue(self.statements()[1].startswith('DESC STAGE'))
        self.assertEqual(sum(sql.startswith('CREATE TABLE IF NOT EXISTS') for sql in self.statements()), 2)
        self.assert_no_infrastructure_mutations()

    def test_infrastructure_check_alone_is_read_only(self):
        self.configure()
        self.warehouse.check_infrastructure()
        self.assertTrue(all(sql.startswith('DESC ') for sql in self.statements()))

    def test_parent_allowed_location_is_valid(self):
        self.integration[2][2] = 's3://stock-bucket/raw/'
        self.configure()
        self.warehouse.check_infrastructure()

    def test_invalid_integration_fails_before_table_setup(self):
        for row, value in [(0,'false'), (1,'GCS'), (2,'s3://wrong-bucket/'),
                           (3,'s3://stock-bucket/raw/')]:
            with self.subTest(row=row):
                original = copy.deepcopy(self.integration)
                self.integration[row][2] = value
                self.cursor.reset_mock()
                self.configure()
                with self.assertRaisesRegex(RuntimeError, 'infra/terraform'):
                    self.warehouse.setup()
                self.assertTrue(all(sql.startswith('DESC ') for sql in self.statements()))
                self.integration = original

    def test_invalid_stage_fails_before_table_setup(self):
        for row, value in [(0,'["s3://wrong-bucket/"]'), (0,'not-json'),
                           (1,'OTHER_INTEGRATION'), (2,'CSV'), (3,'NONE')]:
            with self.subTest(row=row, value=value):
                original = copy.deepcopy(self.stage)
                self.stage[row][3] = value
                self.cursor.reset_mock()
                self.configure()
                with self.assertRaisesRegex(RuntimeError, 'infra/terraform'):
                    self.warehouse.setup()
                self.assertTrue(all(sql.startswith('DESC ') for sql in self.statements()))
                self.stage = original

    def test_missing_integration_does_not_create_one(self):
        self.cursor.execute.side_effect = RuntimeError('missing integration')
        with self.assertRaisesRegex(RuntimeError, 'Cannot inspect.*integration'):
            self.warehouse.setup()
        self.assert_no_infrastructure_mutations()

    def test_missing_stage_does_not_create_one(self):
        self.configure()
        def execute(sql):
            if sql.startswith('DESC STAGE'):
                raise RuntimeError('missing stage')
        self.cursor.execute.side_effect = execute
        with self.assertRaisesRegex(RuntimeError, 'Cannot inspect.*stage'):
            self.warehouse.setup()
        self.assert_no_infrastructure_mutations()


if __name__ == '__main__':
    unittest.main()
