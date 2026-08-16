# src/snowflake_client.py
# Manages Snowflake connections, S3-backed raw loads, and ingestion checkpoints.

import pendulum
from snowflake.connector import connect
from src.config import AWS, SNOWFLAKE
import os
import re
import uuid


class SnowflakeClient:
    """Handles connection, S3-backed raw loads, and ingestion checkpoints."""

    def __init__(self):
        """Initialize the Snowflake connection and cursor."""
        self.conn = self._connect()
        self.cursor = self.conn.cursor()

    def _connect(self):
        """Establish a secure RSA-based connection to Snowflake."""
        private_key_path = SNOWFLAKE.get("private_key_path")

        if private_key_path and os.path.exists(private_key_path):
            import snowflake.connector
            import cryptography.hazmat.primitives.serialization as serialization
            from cryptography.hazmat.backends import default_backend

            with open(private_key_path, "rb") as key:
                p_key = serialization.load_pem_private_key(
                    key.read(), password=None, backend=default_backend()
                )
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            conn = connect(
                account=SNOWFLAKE["account"],
                user=SNOWFLAKE["user"],
                role=SNOWFLAKE["role"],
                warehouse=SNOWFLAKE["warehouse"],
                database=SNOWFLAKE["database"],
                schema=SNOWFLAKE["schema"],
                private_key=pkb,
            )
        else:
            raise FileNotFoundError(f"Private key not found: {private_key_path}")

        print("Connected to Snowflake successfully.")
        return conn
    
    def ensure_objects_exist(self):
        """Ensure database tables exist in configured schema and admin schema."""
        print("Checking or creating necessary tables...")

        # Create the configured schema if it doesn’t exist
        self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE['schema']};")
        self.cursor.execute("CREATE SCHEMA IF NOT EXISTS ADMIN;")  # keep for checkpoints

        # Raw Polygon.io/Massive.com landing table. dbt owns parsing and typing.
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SNOWFLAKE['schema']}.DAILY_STOCKS_RAW (
                API_DATE DATE,
                RUN_ID STRING,
                SOURCE STRING,
                RAW_PAYLOAD STRING,
                INGESTED_AT TIMESTAMP_NTZ
            );
        """)

        # Checkpoints table (still lives in ADMIN schema)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS ADMIN.INGESTION_CHECKPOINTS (
                RUN_ID STRING,
                API_DATE DATE,
                STATUS STRING,
                TOTAL_TICKERS INT,
                ROWS_INSERTED INT,
                STARTED_AT TIMESTAMP_NTZ,
                COMPLETED_AT TIMESTAMP_NTZ,
                ERROR_MESSAGE STRING,
                S3_BUCKET STRING,
                S3_KEY STRING,
                S3_ETAG STRING,
                S3_SHA256 STRING
            );
        """)

        for column in ("S3_BUCKET", "S3_KEY", "S3_ETAG", "S3_SHA256"):
            self.cursor.execute(
                f"ALTER TABLE ADMIN.INGESTION_CHECKPOINTS "
                f"ADD COLUMN IF NOT EXISTS {column} STRING"
            )

        self.conn.commit()
        print("Verified table existence.")

    def _validate_identifier(self, identifier: str):
        """Validate simple Snowflake identifiers used by internal SQL templates."""
        if not identifier.replace("_", "").isalnum():
            raise ValueError(f"Invalid Snowflake identifier: {identifier}")

    def _qualified_table(self, table_name: str) -> str:
        """Return a fully qualified table name for known internal table identifiers."""
        self._validate_identifier(table_name)
        return f"{SNOWFLAKE['database']}.{SNOWFLAKE['schema']}.{table_name}"

    def replace_s3_object_for_date(self, object_key, date_str, expected_rows):
        """Load one archived S3 object and atomically replace its raw date."""
        if not re.fullmatch(r"[A-Za-z0-9_./=-]+", object_key):
            raise ValueError(f"Invalid S3 object key: {object_key}")

        prefix = AWS["s3_prefix"].strip("/")
        prefix_with_separator = f"{prefix}/"
        if not object_key.startswith(prefix_with_separator):
            raise ValueError(f"S3 object is outside the configured prefix: {object_key}")
        stage_path = object_key[len(prefix_with_separator):]

        stage = SNOWFLAKE["s3_stage"]
        for identifier in stage.split("."):
            self._validate_identifier(identifier)

        qualified_table = self._qualified_table("DAILY_STOCKS_RAW")
        temp_table = f"TMP_DAILY_STOCKS_RAW_{uuid.uuid4().hex[:12].upper()}"
        self._validate_identifier(temp_table)
        temp_qualified = f"{SNOWFLAKE['schema']}.{temp_table}"

        try:
            self.cursor.execute(
                f"CREATE TEMPORARY TABLE {temp_qualified} LIKE {qualified_table}"
            )
            self.cursor.execute(
                f"""
                COPY INTO {temp_qualified} (
                    API_DATE, RUN_ID, SOURCE, RAW_PAYLOAD, INGESTED_AT
                )
                FROM (
                    SELECT
                        TRY_TO_DATE($1:API_DATE::STRING),
                        $1:RUN_ID::STRING,
                        $1:SOURCE::STRING,
                        $1:RAW_PAYLOAD::STRING,
                        TRY_TO_TIMESTAMP_NTZ($1:INGESTED_AT::STRING)
                    FROM @{stage}/{stage_path}
                )
                FILE_FORMAT = (TYPE = JSON COMPRESSION = AUTO)
                ON_ERROR = ABORT_STATEMENT
                """
            )
            self.cursor.execute(f"SELECT COUNT(*) FROM {temp_qualified}")
            staged_rows = self.cursor.fetchone()[0]
            if staged_rows != expected_rows:
                raise ValueError(
                    f"S3 row-count mismatch for {date_str}: "
                    f"expected {expected_rows}, staged {staged_rows}"
                )

            self.cursor.execute("BEGIN")
            self.cursor.execute(
                f"DELETE FROM {qualified_table} WHERE API_DATE = %s", (date_str,)
            )
            self.cursor.execute(
                f"""
                INSERT INTO {qualified_table} (
                    API_DATE, RUN_ID, SOURCE, RAW_PAYLOAD, INGESTED_AT
                )
                SELECT API_DATE, RUN_ID, SOURCE, RAW_PAYLOAD, INGESTED_AT
                FROM {temp_qualified}
                """
            )
            self.cursor.execute("COMMIT")
            return True, staged_rows
        except Exception:
            try:
                self.cursor.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            try:
                self.cursor.execute(f"DROP TABLE IF EXISTS {temp_qualified}")
            except Exception:
                pass

    def record_checkpoint(self, run_id, api_date, status, total_tickers=None,
                          rows_inserted=None, error_message=None,
                          s3_bucket=None, s3_key=None, s3_etag=None,
                          s3_sha256=None):
        """Insert a checkpoint record into ADMIN.INGESTION_CHECKPOINTS."""
        now = pendulum.now()
        started_at = now if status == "started" else None
        completed_at = now if status in ["completed", "failed"] else None

        query = f"""
            INSERT INTO ADMIN.INGESTION_CHECKPOINTS (
                RUN_ID, API_DATE, STATUS, TOTAL_TICKERS,
                ROWS_INSERTED, STARTED_AT, COMPLETED_AT, ERROR_MESSAGE,
                S3_BUCKET, S3_KEY, S3_ETAG, S3_SHA256
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.cursor.execute(query, (
            run_id, api_date, status, total_tickers,
            rows_inserted, started_at, completed_at, error_message,
            s3_bucket, s3_key, s3_etag, s3_sha256
        ))
        self.conn.commit()
        print(f"Checkpoint recorded for {api_date} — {status}")

    def get_completed_dates(self):
        """Return all API_DATE values where status='completed'."""
        query = """
            SELECT DISTINCT API_DATE
            FROM ADMIN.INGESTION_CHECKPOINTS
            WHERE STATUS = 'completed'
        """
        try:
            self.cursor.execute(query)
            dates = {row[0].strftime("%Y-%m-%d") for row in self.cursor.fetchall()}
            print(f"Found {len(dates)} completed dates.")
            return dates
        except Exception as e:
            print(f"Error reading checkpoint table: {e}")
            return set()

    def close(self):
        """Close Snowflake connection."""
        try:
            self.cursor.close()
            self.conn.close()
            print("Connection closed.")
        except Exception:
            pass
