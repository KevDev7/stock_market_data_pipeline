import unittest
from unittest.mock import Mock, patch

from src.snowflake_client import SnowflakeClient


class SnowflakeClientInitializationTest(unittest.TestCase):
    def test_missing_raw_namespace_fails_with_infrastructure_guidance(self):
        connection = Mock()
        connection.cursor.return_value.execute.side_effect = RuntimeError('missing schema')
        with patch.object(SnowflakeClient, "_connect", return_value=connection):
            client = SnowflakeClient()
        with patch('src.snowflake_client.SNOWFLAKE', {'schema':'RAW','database':'MARKET'}):
            with self.assertRaisesRegex(RuntimeError, 'infra/terraform'):
                client.ensure_objects_exist()
        self.assertEqual(connection.cursor.return_value.execute.call_count, 1)
        connection.commit.assert_not_called()
        client.close()

    def test_landing_setup_does_not_create_the_terraform_owned_namespace(self):
        connection = Mock()
        connection.cursor.return_value.fetchall.return_value = []
        with patch.object(SnowflakeClient, "_connect", return_value=connection):
            client = SnowflakeClient()
        with patch('src.snowflake_client.SNOWFLAKE', {'schema':'RAW','database':'MARKET'}):
            client.ensure_objects_exist()
        statements = [call.args[0] for call in connection.cursor.return_value.execute.call_args_list]
        self.assertEqual(statements[0], 'DESC SCHEMA RAW;')
        self.assertFalse(any('CREATE SCHEMA IF NOT EXISTS RAW' in sql for sql in statements))
        self.assertTrue(any('CREATE SCHEMA IF NOT EXISTS MARKET.ADMIN' in sql for sql in statements))
        client.close()

    def test_constructor_only_connects_and_creates_cursor(self):
        connection = Mock()

        with patch.object(SnowflakeClient, "_connect", return_value=connection):
            client = SnowflakeClient()

        connection.cursor.assert_called_once_with()
        connection.cursor.return_value.execute.assert_not_called()
        client.close()

    def test_legacy_manifest_blocks_landing_setup(self):
        connection = Mock()
        connection.cursor.return_value.fetchall.return_value = [('REFERENCE_MANIFEST',)]
        with patch.object(SnowflakeClient, '_connect', return_value=connection), \
             patch('src.snowflake_client.SNOWFLAKE', {'schema':'RAW','database':'MARKET'}):
            client = SnowflakeClient()
            with self.assertRaisesRegex(RuntimeError, 'migrate_ingestion_metadata'):
                client.ensure_objects_exist()
        statements = [c.args[0] for c in connection.cursor.return_value.execute.call_args_list]
        self.assertFalse(any(s.lstrip().startswith('CREATE') for s in statements))
        connection.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
