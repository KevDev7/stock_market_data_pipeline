# src/extraction.py
# Fetches grouped daily aggregate data from the Polygon API with retry handling.

import requests
import pandas as pd
import time
from requests import RequestException
from src.config import POLYGON_API_KEY, API_BASE_URL


def fetch_grouped_daily(date_str: str) -> pd.DataFrame:
    """
    Fetch grouped daily aggregate data from the Polygon API for a given date.

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

    data = _make_request_with_retry(url, params=params)

    if not data or "results" not in data:
        print(f"No data returned for {date_str}")
        return None

    df = pd.DataFrame(data["results"])

    if df.empty:
        print(f"Empty results for {date_str}")
        return None

    print(f"Successfully fetched {len(df)} records for {date_str}")
    return df


def _make_request_with_retry(url: str, params: dict, max_retries: int = 3):
    """
    Helper: retry HTTP requests for transient errors or rate limits.

    Args:
        url (str): API endpoint URL.
        params (dict): Query parameters.
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
                print("Rate limited. Waiting 60 seconds before retry...")
                time.sleep(60)
            elif 500 <= status < 600:
                print(f"Server error {status}. Retrying in 5s (attempt {attempt}/{max_retries})...")
                time.sleep(5)
            else:
                print(f"Client error {status}: {response.text[:100]}")
                break

        except RequestException as e:
            print(f"Request failed ({attempt}/{max_retries}): {e}")
            time.sleep(5)

    print("All retries exhausted. Returning None.")
    return None
