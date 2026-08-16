# src/extraction.py
# Fetches grouped daily aggregates from Polygon.io (now Massive.com).

import logging
import time

import pandas as pd
import requests
from requests import RequestException

from src.config import POLYGON_API_KEY, API_BASE_URL

logger = logging.getLogger(__name__)


def fetch_grouped_daily(date_str: str) -> pd.DataFrame:
    """
    Fetch grouped daily data from Polygon.io (now Massive.com) for a given date.

    Args:
        date_str (str): Date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame | None: DataFrame of results or None if request failed.
    """

    # Build the full URL path dynamically
    # Even if API_BASE_URL is just 'https://api.polygon.io'
    url = f"{API_BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{date_str}"

    params = {
        "adjusted": "true",
        "apiKey": POLYGON_API_KEY
    }

    data = _make_request_with_retry(url, params=params, api_date=date_str)

    if data is None:
        return None

    if "results" not in data:
        logger.warning("API response missing results api_date=%s", date_str)
        return None

    df = pd.DataFrame(data["results"])

    if df.empty:
        logger.warning("Empty API results api_date=%s", date_str)
        return None

    logger.info("Fetched stock data api_date=%s rows=%s", date_str, len(df))
    return df


def _make_request_with_retry(
    url: str, params: dict, api_date: str, max_retries: int = 3
):
    """
    Helper: retry HTTP requests for transient errors or rate limits.

    Args:
        url (str): API endpoint URL.
        params (dict): Query parameters.
        api_date (str): Trading date used to identify request logs.
        max_retries (int): Maximum number of retry attempts.

    Returns:
        dict | None: JSON response as dict, or None on failure.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=10)
            status = response.status_code

            if status == 200:
                return response.json()
            elif status == 429:
                logger.warning(
                    "API rate limited api_date=%s attempt=%s/%s retry_in_seconds=60",
                    api_date,
                    attempt,
                    max_retries,
                )
                time.sleep(60)
            elif 500 <= status < 600:
                logger.warning(
                    "API server error api_date=%s status=%s attempt=%s/%s "
                    "retry_in_seconds=5",
                    api_date,
                    status,
                    attempt,
                    max_retries,
                )
                time.sleep(5)
            else:
                logger.error(
                    "API client error api_date=%s status=%s response=%s",
                    api_date,
                    status,
                    response.text[:100],
                )
                return None

        except RequestException as exc:
            logger.warning(
                "API request failed api_date=%s attempt=%s/%s error=%s",
                api_date,
                attempt,
                max_retries,
                exc,
            )
            time.sleep(5)

    logger.error(
        "API request did not complete api_date=%s max_attempts=%s",
        api_date,
        max_retries,
    )
    return None
