# src/snowflake_client.py

import pandas as pd
import pendulum
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
from src.config import SNOWFLAKE
import os


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

        print("✅ Connected to Snowflake successfully.")
        return conn

    def _ensure_objects_exist(self):
        """Ensure database tables exist: RAW.DAILY_STOCKS and ADMIN.INGESTION_CHECKPOINTS."""
        print("Checking or creating necessary tables...")

        self.cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS RAW;
        """)
        self.cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS ADMIN;
        """)

        # Table for stock data
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS RAW.DAILY_STOCKS (
                T STRING,
                V FLOAT,
                VW FLOAT,
                O FLOAT,
                C FLOAT,
                H FLOAT,
                L FLOAT,
                N INT,
                DATE DATE,
                INGESTED_AT TIMESTAMP_NTZ
            );
        """)

        # Table for checkpoints
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
        print("✅ Verified table existence.")

    def write_dataframe(self, df: pd.DataFrame, table_name: str):
        """Write a pandas DataFrame into Snowflake using write_pandas()."""
        if df is None or df.empty:
            print("⚠️ DataFrame is empty; skipping load.")
            return False, 0

        success, nchunks, nrows, _ = write_pandas(
            conn=self.conn,
            df=df,
            table_name=table_name,
            database=SNOWFLAKE["database"],
            schema=SNOWFLAKE["schema"],
            quote_identifiers=False
        )

        if success:
            print(f"✅ Successfully loaded {nrows} rows into {table_name}.")
            return True, nrows
        else:
            print(f"❌ Failed to load data into {table_name}.")
            return False, 0

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
        print(f"🪵 Checkpoint recorded for {api_date} — {status}")

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
        
    # --------------------------------------------------------------------
    # 🚧 For Phase 3+ (Airflow Monitoring / Analytics)
    # --------------------------------------------------------------------
    # def get_ingestion_stats(self):
    #     """
    #     Summarize ingestion stats for Airflow dashboards or monitoring.
    #     Returns:
    #         dict: counts, totals, averages, and failed runs summary.
    #     """
    #     query = """
    #         SELECT
    #             COUNT(DISTINCT API_DATE) AS DAYS_PROCESSED,
    #             SUM(ROWS_INSERTED) AS TOTAL_ROWS,
    #             AVG(TOTAL_TICKERS) AS AVG_TICKERS_PER_DAY,
    #             MIN(API_DATE) AS EARLIEST_DATE,
    #             MAX(API_DATE) AS LATEST_DATE,
    #             COUNT_IF(STATUS = 'failed') AS FAILED_RUNS
    #         FROM ADMIN.INGESTION_CHECKPOINTS
    #         WHERE STATUS = 'completed'
    #     """
    #
    #     try:
    #         self.cursor.execute(query)
    #         result = self.cursor.fetchone()
    #         return {
    #             "days_processed": result[0],
    #             "total_rows": result[1],
    #             "avg_tickers_per_day": result[2],
    #             "earliest_date": result[3],
    #             "latest_date": result[4],
    #             "failed_runs": result[5],
    #         }
    #     except Exception as e:
    #         print(f"Error getting ingestion stats: {e}")
    #         return None
    #
    # --------------------------------------------------------------------

    def close(self):
        """Close Snowflake connection."""
        try:
            self.cursor.close()
            self.conn.close()
            print("🔒 Connection closed.")
        except Exception:
            pass
