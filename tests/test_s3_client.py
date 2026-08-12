import hashlib
import io
import unittest

import pandas as pd
from botocore.exceptions import ClientError

from src.s3_client import S3RawClient


class FakeBody:
    def __init__(self, value):
        self.value = value

    def read(self):
        return self.value


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.last_put = None

    def put_object(self, **kwargs):
        self.last_put = kwargs
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]
        return {"ETag": '"test-etag"'}

    def get_object(self, Bucket, Key):
        return {"Body": FakeBody(self.objects[(Bucket, Key)])}

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {
            "ETag": '"test-etag"',
            "Metadata": self.last_put["Metadata"],
        }


class S3RawClientTest(unittest.TestCase):
    def setUp(self):
        self.fake = FakeS3()
        self.client = S3RawClient(
            bucket="test-bucket", prefix="raw/test", client=self.fake
        )
        self.df = pd.DataFrame(
            {
                "API_DATE": ["2026-01-14"],
                "RUN_ID": ["run-1"],
                "SOURCE": ["polygon_grouped_daily"],
                "RAW_PAYLOAD": ['{"T":"AAPL","c":234.4}'],
                "INGESTED_AT": [pd.Timestamp("2026-01-15 12:00:00")],
            }
        )

    def test_archive_and_read_round_trip(self):
        archived = self.client.archive_dataframe(self.df, "2026-01-14", "run-1")
        restored = self.client.read_dataframe(
            archived["key"], expected_sha256=archived["sha256"]
        )

        self.assertEqual(archived["row_count"], 1)
        self.assertEqual(archived["etag"], "test-etag")
        self.assertEqual(restored.loc[0, "RAW_PAYLOAD"], self.df.loc[0, "RAW_PAYLOAD"])
        self.assertEqual(str(restored.loc[0, "API_DATE"]), "2026-01-14")
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(restored["INGESTED_AT"]))
        self.assertEqual(self.fake.last_put["ServerSideEncryption"], "AES256")

    def test_checksum_mismatch_is_rejected(self):
        archived = self.client.archive_dataframe(self.df, "2026-01-14", "run-1")
        with self.assertRaisesRegex(ValueError, "Checksum mismatch"):
            self.client.read_dataframe(archived["key"], expected_sha256="wrong")

    def test_object_metadata_supports_restartable_backfills(self):
        key = self.client.object_key("2026-01-14", "run-1")
        self.assertIsNone(self.client.object_metadata(key))
        archived = self.client.archive_dataframe(self.df, "2026-01-14", "run-1")
        metadata = self.client.object_metadata(key)
        self.assertEqual(metadata["row_count"], 1)
        self.assertEqual(metadata["sha256"], archived["sha256"])

    def test_gzip_output_is_deterministic(self):
        first = self.client._encode_dataframe(self.df)
        second = self.client._encode_dataframe(self.df)
        self.assertEqual(hashlib.sha256(first).hexdigest(), hashlib.sha256(second).hexdigest())
        self.assertTrue(first.startswith(b"\x1f\x8b"))


if __name__ == "__main__":
    unittest.main()
