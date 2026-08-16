# src/extract_load_stocks.py
# Pipeline entrypoint for Polygon.io/Massive.com extraction and raw loading.

import logging
import time

import pendulum
import pandas_market_calendars as mcal
from pendulum import duration

from src.extraction import fetch_grouped_daily
from src.load import archive_and_load_raw_data
from src.snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)


def get_trading_days(start_date, end_date, calendar_name="NYSE"):
    """Return all valid trading days between two dates for a given market calendar."""
    calendar = mcal.get_calendar(calendar_name)
    schedule = calendar.schedule(start_date=start_date, end_date=end_date)
    trading_days = schedule.index
    return trading_days


def get_completed_dates():
    """Retrieve dates already completed through S3 and Snowflake loading."""
    client = SnowflakeClient()
    client.ensure_objects_exist()
    completed = client.get_completed_dates()
    client.close()
    return completed


def ingest_raw_stock_data(years_back=2, days_back_override=None):
    """
    Fetch Polygon.io/Massive.com grouped daily data, archive it in S3, load it into
    Snowflake RAW, and record ingestion checkpoints.
    """
    run_id = pendulum.now().strftime("%Y%m%d_%H%M%S")
    logger.info("Starting raw stock ingestion run_id=%s", run_id)

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

    logger.info(
        "Prepared ingestion run_id=%s start_date=%s end_date=%s "
        "trading_days=%s known_completed_dates=%s remaining_days=%s",
        run_id,
        start_date,
        end_date,
        total_days,
        len(completed_dates),
        remaining_days,
    )

    for i, date in enumerate(trading_days, 1):
        date_str = date.strftime("%Y-%m-%d")

        if date_str in completed_dates:
            logger.info(
                "Skipping completed date run_id=%s api_date=%s progress=%s/%s",
                run_id,
                date_str,
                i,
                total_days,
            )
            continue

        logger.info(
            "Processing date run_id=%s api_date=%s progress=%s/%s remaining_days=%s",
            run_id,
            date_str,
            i,
            total_days,
            remaining_days,
        )

        df = fetch_grouped_daily(date_str)
        archive_and_load_raw_data(df, date_str, run_id)

        # Prevent API throttling
        time.sleep(20)
        remaining_days -= 1

    logger.info("Finished raw stock ingestion run_id=%s", run_id)


def extract_load_data(years_back=2, days_back_override=None):
    """Backward-compatible name for ingest_raw_stock_data()."""
    return ingest_raw_stock_data(
        years_back=years_back,
        days_back_override=days_back_override,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    # Override for short local runs during development
    # ingest_raw_stock_data(years_back=2)
    ingest_raw_stock_data(days_back_override=3)
