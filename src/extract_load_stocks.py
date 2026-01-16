# src/extract_load_stocks.py
# Pipeline entrypoint for extracting grouped daily data from Polygon and loading it into Snowflake with ingestion checkpoints.

import time
import pendulum
import pandas_market_calendars as mcal
from pendulum import duration
from src.extraction import fetch_grouped_daily
from src.load import load_data
from src.snowflake_client import SnowflakeClient


def get_trading_days(start_date, end_date, calendar_name="NYSE"):
    """Return all valid trading days between two dates for a given market calendar."""
    calendar = mcal.get_calendar(calendar_name)
    schedule = calendar.schedule(start_date=start_date, end_date=end_date)
    trading_days = schedule.index
    return trading_days


def get_completed_dates():
    """Retrieve dates already loaded into Snowflake."""
    client = SnowflakeClient()
    completed = client.get_completed_dates()
    client.close()
    return completed


def extract_load_data(years_back=2, days_back_override=None):
    """
    Main pipeline entrypoint: fetch grouped daily data from Polygon,
    load it into Snowflake, and record ingestion checkpoints.
    """
    run_id = pendulum.now().strftime("%Y%m%d_%H%M%S")
    print(f"\nStarting historical stock data load | run_id = {run_id}")

    today = pendulum.now("America/New_York").date()
    end_date = today - duration(days=1)

    if days_back_override == 1:
        calendar = mcal.get_calendar("NYSE")
        schedule = calendar.schedule(
            start_date=today.subtract(days=10),
            end_date=today
        )
        last_trading_day = schedule.index[schedule.index < today][-1].date()
        start_date = end_date = last_trading_day
    elif days_back_override:
        start_date = end_date - duration(days=days_back_override)
    else:
        start_date = end_date - duration(years=years_back)

    completed_dates = get_completed_dates()
    trading_days = get_trading_days(start_date, end_date)
    total_days = len(trading_days)
    remaining_days = len(
        [d for d in trading_days if d.strftime("%Y-%m-%d") not in completed_dates]
    )

    print(f"Total trading days: {total_days}")
    print(f"Already completed: {len(completed_dates)}")
    print(f"Remaining to process: {remaining_days}\n")

    for i, date in enumerate(trading_days, 1):
        date_str = date.strftime("%Y-%m-%d")

        if date_str in completed_dates:
            print(f"Skipping {date_str} (already completed). Progress: {i}/{total_days}")
            continue

        print(f"Processing {date_str} | Progress {i}/{total_days} (Remaining: {remaining_days})")

        df = fetch_grouped_daily(date_str)
        load_data(df, date_str, run_id)

        # Prevent API throttling
        time.sleep(20)
        remaining_days -= 1

    print("\nFinished processing all trading days.")


if __name__ == "__main__":
    # Override for short local runs during development
    # extract_load_data(years_back=2)
    extract_load_data(days_back_override=3)
