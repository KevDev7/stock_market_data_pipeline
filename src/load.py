# src/load.py
# Normalizes grouped daily Polygon data and loads it into Snowflake with checkpoint tracking.

import pandas as pd
from pendulum import parse
from src.snowflake_client import SnowflakeClient

# Shared client for load operations
snowflake_client = SnowflakeClient()


def load_data(df, date_str, run_id):
    """
    Load extracted Polygon data into Snowflake and record checkpoints.

    Args:
        df (pd.DataFrame): DataFrame returned by the Polygon API.
        date_str (str): Trading date being processed (YYYY-MM-DD).
        run_id (str): Pipeline execution identifier.
    """
    if df is None or df.empty:
        print(f"No data to load for {date_str}")
        return

    total_tickers = len(df["T"].unique()) if "T" in df.columns else 0

    # Record "started" checkpoint
    snowflake_client.record_checkpoint(
        run_id=run_id,
        api_date=parse(date_str),
        status="started",
        total_tickers=total_tickers
    )

    # Normalize and enrich DataFrame before writing
    # Rename timestamp column from Polygon ("t") to Snowflake ("TS")
    df = df.rename(columns={"t": "TS"})

    # Convert TS (milliseconds since epoch) → pandas datetime (tz-naive for TIMESTAMP_NTZ)
    if "TS" in df.columns:
        df["TS"] = pd.to_datetime(df["TS"], unit="ms")

    # Add DATE column as ISO string so Snowflake casts safely to DATE
    df["DATE"] = date_str

    # Add ingestion timestamp (UTC, tz-naive fits TIMESTAMP_NTZ)
    df["INGESTED_AT"] = pd.Timestamp.utcnow()

    # Map Polygon lowercase fields to Snowflake expected uppercase schema
    rename_map = {
        "v": "V",   # volume
        "vw": "VW", # volume-weighted price
        "o": "O",   # open
        "c": "C",   # close
        "h": "H",   # high
        "l": "L",   # low
        "n": "N",   # number of transactions
        "t": "TS"   # ensure consistency even if not renamed yet
    }
    df.rename(columns=rename_map, inplace=True)

    # Restrict to target table columns to avoid column-order/type mismatches during write
    target_cols = [
        "T", "V", "VW", "O", "C", "H", "L", "N", "TS", "DATE", "INGESTED_AT"
    ]
    present_cols = [c for c in target_cols if c in df.columns]
    if present_cols:
        df = df[present_cols]

    # Ensure datetime columns are tz-naive (TIMESTAMP_NTZ safe)
    for col in ["TS", "INGESTED_AT"]:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = df[col].dt.tz_localize(None)
            except (TypeError, AttributeError):
                pass

    # Write to Snowflake
    success, rows_inserted = snowflake_client.write_dataframe(df, "DAILY_STOCKS")

    # Record checkpoint status
    if success:
        snowflake_client.record_checkpoint(
            run_id=run_id,
            api_date=parse(date_str),
            status="completed",
            total_tickers=total_tickers,
            rows_inserted=rows_inserted
        )
        print(f"Successfully saved {rows_inserted} records for {date_str}")
    else:
        snowflake_client.record_checkpoint(
            run_id=run_id,
            api_date=parse(date_str),
            status="failed",
            total_tickers=total_tickers,
            error_message="Failed to insert data into Snowflake"
        )
        print(f"Failed to save data for {date_str}")
