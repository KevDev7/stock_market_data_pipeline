# src/snowflake_client.py
# Manages Snowflake connections, table creation, data writes, and ingestion checkpoints.

import pandas as pd
import pendulum
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from src.config import SNOWFLAKE
import os
import uuid


class SnowflakeClient:
    """Handles connection, table setup, data writes, and checkpoints in Snowflake."""

    def __init__(self):
        """Initialize connection, cursor, and ensure required Snowflake objects exist."""
        self.conn = self._connect()
        self.cursor = self.conn.cursor()
        self._ensure_objects_exist()

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
    
    def _ensure_objects_exist(self):
        """Ensure database tables exist in configured schema and admin schema."""
        print("Checking or creating necessary tables...")

        # Create the configured schema if it doesn’t exist
        self.cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE['schema']};")
        self.cursor.execute("CREATE SCHEMA IF NOT EXISTS ADMIN;")  # keep for checkpoints

        # Raw Polygon row landing table. dbt owns market-data parsing and typing.
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
                ERROR_MESSAGE STRING
            );
        """)

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

    def write_dataframe(self, df: pd.DataFrame, table_name: str):
        """Write a pandas DataFrame into Snowflake using write_pandas()."""
        if df is None or df.empty:
            print("DataFrame is empty; skipping load.")
            return False, 0

        success, nchunks, nrows, _ = write_pandas(
            conn=self.conn,
            df=df,
            table_name=table_name,
            database=SNOWFLAKE["database"],
            schema=SNOWFLAKE["schema"],
            quote_identifiers=False,
            use_logical_type=True
        )

        if success:
            print(f"Successfully loaded {nrows} rows into {table_name}.")
            return True, nrows
        else:
            print(f"Failed to load data into {table_name}.")
            return False, 0

    def replace_dataframe_for_date(
        self,
        df: pd.DataFrame,
        table_name: str,
        date_str: str,
        date_column: str = "DATE"
    ):
        """
        Replace one trading date in the target table.

        Polygon grouped daily aggregates are a full-day snapshot, so replacing the
        date keeps retries idempotent instead of appending duplicate rows.
        """
        if df is None or df.empty:
            print("DataFrame is empty; skipping load.")
            return False, 0

        qualified_table = self._qualified_table(table_name)
        self._validate_identifier(date_column)
        temp_table = f"TMP_{table_name}_{uuid.uuid4().hex[:12].upper()}"
        self._validate_identifier(temp_table)

        for col in df.columns:
            self._validate_identifier(col)
        columns = ", ".join(df.columns)

        try:
            self.cursor.execute(
                f"CREATE TEMPORARY TABLE {SNOWFLAKE['schema']}.{temp_table} "
                f"LIKE {qualified_table}"
            )

            success, _, staged_rows, _ = write_pandas(
                conn=self.conn,
                df=df,
                table_name=temp_table,
                database=SNOWFLAKE["database"],
                schema=SNOWFLAKE["schema"],
                quote_identifiers=False,
                use_logical_type=True
            )

            if not success:
                print(f"Failed to stage data for {date_str} into {temp_table}.")
                return False, 0

            self.cursor.execute("BEGIN")
            self.cursor.execute(
                f"DELETE FROM {qualified_table} WHERE {date_column} = %s",
                (date_str,)
            )
            self.cursor.execute(
                f"""
                INSERT INTO {qualified_table} ({columns})
                SELECT {columns}
                FROM {SNOWFLAKE['schema']}.{temp_table}
                """
            )
            self.cursor.execute("COMMIT")

            print(
                f"Atomically replaced {staged_rows} rows for {date_str} "
                f"in {table_name}."
            )
            return True, staged_rows
        except Exception:
            try:
                self.cursor.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            try:
                self.cursor.execute(
                    f"DROP TABLE IF EXISTS {SNOWFLAKE['schema']}.{temp_table}"
                )
            except Exception:
                pass

    def record_checkpoint(self, run_id, api_date, status, total_tickers=None,
                          rows_inserted=None, error_message=None):
        """Insert a checkpoint record into ADMIN.INGESTION_CHECKPOINTS."""
        now = pendulum.now()
        started_at = now if status == "started" else None
        completed_at = now if status in ["completed", "failed"] else None

        query = f"""
            INSERT INTO ADMIN.INGESTION_CHECKPOINTS (
                RUN_ID, API_DATE, STATUS, TOTAL_TICKERS,
                ROWS_INSERTED, STARTED_AT, COMPLETED_AT, ERROR_MESSAGE
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        self.cursor.execute(query, (
            run_id, api_date, status, total_tickers,
            rows_inserted, started_at, completed_at, error_message
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
