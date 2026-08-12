# src/load.py
# Lands grouped daily Polygon data into Snowflake with checkpoint tracking.

import json
import pandas as pd
from pendulum import parse
from src.s3_client import S3RawClient
from src.snowflake_client import SnowflakeClient

SOURCE_NAME = "polygon_grouped_daily"


def load_data(df, date_str, run_id, snowflake_client=None, s3_client=None):
    """
    Load extracted Polygon data into Snowflake and record checkpoints.

    Args:
        df (pd.DataFrame): DataFrame returned by the Polygon API.
        date_str (str): Trading date being processed (YYYY-MM-DD).
        run_id (str): Pipeline execution identifier.
        snowflake_client (SnowflakeClient | None): Optional existing client.
    """
    owns_client = snowflake_client is None
    client = snowflake_client or SnowflakeClient()
    archive = s3_client or S3RawClient()
    archive_result = None

    try:
        if df is None or df.empty:
            error_message = f"No data returned for {date_str}"
            raise ValueError(error_message)

        total_tickers = len(df["T"].unique()) if "T" in df.columns else 0

        # Record "started" checkpoint
        client.record_checkpoint(
            run_id=run_id,
            api_date=parse(date_str),
            status="started",
            total_tickers=total_tickers
        )

        landing_df = _build_raw_landing_dataframe(df, date_str, run_id)

        archive_result = archive.archive_dataframe(landing_df, date_str, run_id)
        client.record_checkpoint(
            run_id=run_id,
            api_date=parse(date_str),
            status="archived",
            total_tickers=total_tickers,
            rows_inserted=0,
            s3_bucket=archive_result["bucket"],
            s3_key=archive_result["key"],
            s3_etag=archive_result["etag"],
            s3_sha256=archive_result["sha256"],
        )

        # Snowflake loads the durable S3 object through its external stage.
        success, rows_inserted = client.replace_s3_object_for_date(
            archive_result["key"],
            date_str,
            expected_rows=archive_result["row_count"],
        )

        # Record checkpoint status
        if success:
            client.record_checkpoint(
                run_id=run_id,
                api_date=parse(date_str),
                status="completed",
                total_tickers=total_tickers,
                rows_inserted=rows_inserted,
                s3_bucket=archive_result["bucket"],
                s3_key=archive_result["key"],
                s3_etag=archive_result["etag"],
                s3_sha256=archive_result["sha256"],
            )
            print(f"Successfully saved {rows_inserted} records for {date_str}")
        else:
            error_message = "Failed to insert data into Snowflake"
            raise RuntimeError(error_message)
    except Exception as exc:
        client.record_checkpoint(
            run_id=run_id,
            api_date=parse(date_str),
            status="failed",
            total_tickers=locals().get("total_tickers", 0),
            rows_inserted=0,
            error_message=str(exc),
            s3_bucket=archive_result["bucket"] if archive_result else None,
            s3_key=archive_result["key"] if archive_result else None,
            s3_etag=archive_result["etag"] if archive_result else None,
            s3_sha256=archive_result["sha256"] if archive_result else None,
        )
        raise
    finally:
        if owns_client:
            client.close()


def _build_raw_landing_dataframe(df: pd.DataFrame, date_str: str, run_id: str) -> pd.DataFrame:
    """Create a raw landing dataframe with only operational metadata added."""
    clean_df = df.astype(object).where(pd.notnull(df), None)
    records = clean_df.to_dict("records")
    ingested_at = pd.Timestamp.utcnow().tz_localize(None)

    return pd.DataFrame({
        "API_DATE": [date_str] * len(records),
        "RUN_ID": [run_id] * len(records),
        "SOURCE": [SOURCE_NAME] * len(records),
        "RAW_PAYLOAD": [
            json.dumps(record, separators=(",", ":"), default=str, allow_nan=False)
            for record in records
        ],
        "INGESTED_AT": [ingested_at] * len(records),
    })
