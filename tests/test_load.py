import unittest

import pandas as pd

from src.load import load_data


class FakeArchive:
    def archive_dataframe(self, df, date_str, run_id):
        return {
            "bucket": "stock-bucket",
            "key": (
                "raw/polygon/grouped-daily/api_date=2026-01-14/"
                "run_id=run-1/daily_stocks_raw.ndjson.gz"
            ),
            "etag": "etag",
            "sha256": "checksum",
            "row_count": len(df),
        }


class FakeSnowflake:
    def __init__(self, fail_load=False):
        self.fail_load = fail_load
        self.checkpoints = []
        self.load_call = None

    def record_checkpoint(self, **kwargs):
        self.checkpoints.append(kwargs)

    def replace_s3_object_for_date(self, key, date_str, expected_rows):
        self.load_call = (key, date_str, expected_rows)
        if self.fail_load:
            raise RuntimeError("warehouse unavailable")
        return True, expected_rows


class LoadDataTest(unittest.TestCase):
    def setUp(self):
        self.source = pd.DataFrame(
            [{"T": "AAPL", "c": 234.4}, {"T": "MSFT", "c": 418.8}]
        )

    def test_completion_requires_archive_and_snowflake_load(self):
        warehouse = FakeSnowflake()
        load_data(
            self.source,
            "2026-01-14",
            "run-1",
            snowflake_client=warehouse,
            s3_client=FakeArchive(),
        )

        self.assertEqual(
            [row["status"] for row in warehouse.checkpoints],
            ["started", "archived", "completed"],
        )
        self.assertEqual(warehouse.load_call[1:], ("2026-01-14", 2))
        self.assertTrue(warehouse.checkpoints[-1]["s3_key"].endswith(".ndjson.gz"))

    def test_failed_snowflake_load_preserves_archive_location(self):
        warehouse = FakeSnowflake(fail_load=True)
        with self.assertRaisesRegex(RuntimeError, "warehouse unavailable"):
            load_data(
                self.source,
                "2026-01-14",
                "run-1",
                snowflake_client=warehouse,
                s3_client=FakeArchive(),
            )

        self.assertEqual(
            [row["status"] for row in warehouse.checkpoints],
            ["started", "archived", "failed"],
        )
        self.assertIsNotNone(warehouse.checkpoints[-1]["s3_key"])


if __name__ == "__main__":
    unittest.main()
