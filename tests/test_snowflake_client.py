import unittest
from unittest.mock import Mock, patch

from src.snowflake_client import SnowflakeClient


class SnowflakeClientInitializationTest(unittest.TestCase):
    def test_constructor_only_connects_and_creates_cursor(self):
        connection = Mock()

        with patch.object(SnowflakeClient, "_connect", return_value=connection):
            client = SnowflakeClient()

        connection.cursor.assert_called_once_with()
        connection.cursor.return_value.execute.assert_not_called()
        client.close()


if __name__ == "__main__":
    unittest.main()
