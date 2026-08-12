import gzip
import hashlib
import io
import json

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from src.config import AWS


class S3RawClient:
    """Archives and retrieves immutable raw landing records in Amazon S3."""

    def __init__(self, bucket=None, prefix=None, region=None, client=None):
        self.bucket = bucket or AWS["s3_bucket"]
        self.prefix = (prefix or AWS["s3_prefix"]).strip("/")
        if not self.bucket:
            raise ValueError("AWS_S3_BUCKET is required")
        self.client = client or boto3.client(
            "s3", region_name=region or AWS["region"]
        )

    def archive_dataframe(self, df, date_str, run_id):
        """Write one immutable gzip NDJSON object and return its metadata."""
        object_key = self.object_key(date_str, run_id)
        body = self._encode_dataframe(df)
        checksum = hashlib.sha256(body).hexdigest()

        response = self.client.put_object(
            Bucket=self.bucket,
            Key=object_key,
            Body=body,
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
            ServerSideEncryption="AES256",
            Metadata={
                "api-date": date_str,
                "run-id": run_id,
                "row-count": str(len(df)),
                "sha256": checksum,
            },
        )
        return {
            "bucket": self.bucket,
            "key": object_key,
            "etag": response.get("ETag", "").strip('"'),
            "sha256": checksum,
            "row_count": len(df),
        }

    def read_dataframe(self, object_key, expected_sha256=None):
        """Read an archived landing object and verify its compressed checksum."""
        response = self.client.get_object(Bucket=self.bucket, Key=object_key)
        body = response["Body"].read()
        checksum = hashlib.sha256(body).hexdigest()
        if expected_sha256 and checksum != expected_sha256:
            raise ValueError(f"Checksum mismatch for s3://{self.bucket}/{object_key}")

        with gzip.GzipFile(fileobj=io.BytesIO(body), mode="rb") as stream:
            records = [json.loads(line) for line in stream if line.strip()]
        df = pd.DataFrame.from_records(records)
        if not df.empty:
            df["API_DATE"] = pd.to_datetime(df["API_DATE"]).dt.date
            df["INGESTED_AT"] = pd.to_datetime(df["INGESTED_AT"])
        return df

    def object_metadata(self, object_key):
        """Return archive metadata, or None when the object does not exist."""
        try:
            response = self.client.head_object(Bucket=self.bucket, Key=object_key)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey"}:
                return None
            raise
        metadata = response.get("Metadata", {})
        return {
            "etag": response.get("ETag", "").strip('"'),
            "sha256": metadata.get("sha256"),
            "row_count": int(metadata["row-count"]) if metadata.get("row-count") else None,
        }

    def object_key(self, date_str, run_id):
        return (
            f"{self.prefix}/api_date={date_str}/run_id={run_id}/"
            "daily_stocks_raw.ndjson.gz"
        )

    @staticmethod
    def _encode_dataframe(df):
        output = io.BytesIO()
        with gzip.GzipFile(fileobj=output, mode="wb", mtime=0) as stream:
            for record in df.to_dict("records"):
                line = json.dumps(
                    record, separators=(",", ":"), default=str, allow_nan=False
                )
                stream.write(line.encode("utf-8") + b"\n")
        return output.getvalue()
