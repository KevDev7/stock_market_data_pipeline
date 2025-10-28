# src/load.py

from pendulum import parse
from src.snowflake_client import SnowflakeClient


# Initialize one shared Snowflake client for all loads
snowflake_client = SnowflakeClient()


def load_data(df, date_str, run_id):
    """
    Load extracted Polygon data into Snowflake and record checkpoints.

    Args:
        df (pd.DataFrame): DataFrame of stock data from Polygon API.
        date_str (str): The trading date being processed (YYYY-MM-DD).
        run_id (str): Unique run identifier (e.g., timestamped ID).
    """
    if df is None or df.empty:
        print(f"⚠️ No data to load for {date_str}")
        return

    total_tickers = len(df["T"].unique()) if "T" in df.columns else 0

    # Record "started" checkpoint
    snowflake_client.record_checkpoint(
        run_id=run_id,
        api_date=parse(date_str),
        status="started",
        total_tickers=total_tickers
    )

    # Normalize dataframe column names to avoid duplicates (Snowflake uppercases names)
    df = df.rename(columns={"t": "TS"})  # rename timestamp column

    # Attempt to load data
    success, rows_inserted = snowflake_client.write_dataframe(df, "DAILY_STOCKS")

    if success:
        snowflake_client.record_checkpoint(
            run_id=run_id,
            api_date=parse(date_str),
            status="completed",
            total_tickers=total_tickers,
            rows_inserted=rows_inserted
        )
        print(f"✅ Successfully saved {rows_inserted} records for {date_str}")
    else:
        snowflake_client.record_checkpoint(
            run_id=run_id,
            api_date=parse(date_str),
            status="failed",
            total_tickers=total_tickers,
            error_message="Failed to insert data into Snowflake"
        )
        print(f"❌ Failed to save data for {date_str}")
