# src/load.py
# Archives Polygon.io (now Massive.com) grouped daily data, then loads Snowflake RAW.

import json
import logging

import pandas as pd
from pendulum import parse

from src.s3_client import S3RawClient
from src.snowflake_client import SnowflakeClient

SOURCE_NAME = "polygon_grouped_daily"
logger = logging.getLogger(__name__)


def archive_and_load_raw_data(
    df, date_str, run_id, snowflake_client=None, s3_client=None
):
    """
    Archive extracted Polygon.io/Massive.com data, load it, and checkpoint it.

    Args:
        df (pd.DataFrame): DataFrame returned by the Polygon.io/Massive.com API.
        date_str (str): Trading date being processed (YYYY-MM-DD).
        run_id (str): Pipeline execution identifier.
        snowflake_client (SnowflakeClient | None): Optional existing client.
    """
    owns_snowflake_client = snowflake_client is None
    snowflake = snowflake_client or SnowflakeClient()
    if owns_snowflake_client:
        snowflake.ensure_objects_exist()

    s3_archive = s3_client or S3RawClient()
    archive_result = None

    try:
        if df is None or df.empty:
            error_message = f"No data returned for {date_str}"
            raise ValueError(error_message)

        total_tickers = len(df["T"].unique()) if "T" in df.columns else 0

        # Record "started" checkpoint
        snowflake.record_checkpoint(
            run_id=run_id,
            api_date=parse(date_str),
            status="started",
            total_tickers=total_tickers
        )

        landing_df = build_raw_landing_dataframe(df, date_str, run_id)

        archive_result = s3_archive.archive_dataframe(landing_df, date_str, run_id)
        snowflake.record_checkpoint(
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
        success, rows_inserted = snowflake.replace_s3_object_for_date(
            archive_result["key"],
            date_str,
            expected_rows=archive_result["row_count"],
        )

        # Record checkpoint status
        if success:
            snowflake.record_checkpoint(
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
            logger.info(
                "Loaded raw stock data run_id=%s api_date=%s rows=%s "
                "table=DAILY_STOCKS_RAW",
                run_id,
                date_str,
                rows_inserted,
            )
        else:
            error_message = "Failed to insert data into Snowflake"
            raise RuntimeError(error_message)
    except Exception as exc:
        logger.exception(
            "Raw stock load failed run_id=%s api_date=%s",
            run_id,
            date_str,
        )
        snowflake.record_checkpoint(
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
        if owns_snowflake_client:
            snowflake.close()


def build_raw_landing_dataframe(
    df: pd.DataFrame, date_str: str, run_id: str
) -> pd.DataFrame:
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


def load_data(df, date_str, run_id, snowflake_client=None, s3_client=None):
    """Backward-compatible name for archive_and_load_raw_data()."""
    return archive_and_load_raw_data(
        df,
        date_str,
        run_id,
        snowflake_client=snowflake_client,
        s3_client=s3_client,
    )
