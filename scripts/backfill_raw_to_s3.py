"""Backfill retained Snowflake raw rows into partitioned S3 landing objects."""

import argparse
import json

import pandas as pd

from src.s3_client import S3RawClient
from src.snowflake_client import SnowflakeClient


def backfill(start_date=None, end_date=None, dry_run=False):
    warehouse = SnowflakeClient()
    warehouse.ensure_objects_exist()
    archive = S3RawClient()
    try:
        warehouse.cursor.execute(
            """
            SELECT API_DATE, COUNT(*)
            FROM RAW.DAILY_STOCKS_RAW
            WHERE (%s IS NULL OR API_DATE >= %s)
              AND (%s IS NULL OR API_DATE <= %s)
            GROUP BY API_DATE
            ORDER BY API_DATE
            """,
            (start_date, start_date, end_date, end_date),
        )
        dates = warehouse.cursor.fetchall()
        print(f"Dates selected: {len(dates)}")
        if dry_run:
            print(f"Rows selected: {sum(row_count for _, row_count in dates)}")
            return

        for api_date, expected_rows in dates:
            date_str = api_date.isoformat()
            run_id = f"legacy-backfill-{date_str}"
            object_key = archive.object_key(date_str, run_id)
            existing = archive.object_metadata(object_key)
            if existing and existing["row_count"] == expected_rows:
                print(f"Skipping verified existing object for {date_str}")
                continue

            warehouse.cursor.execute(
                """
                SELECT API_DATE, RUN_ID, SOURCE, RAW_PAYLOAD, INGESTED_AT
                FROM RAW.DAILY_STOCKS_RAW
                WHERE API_DATE = %s
                ORDER BY TRY_PARSE_JSON(RAW_PAYLOAD):T::STRING
                """,
                (date_str,),
            )
            columns = [item[0] for item in warehouse.cursor.description]
            df = pd.DataFrame(warehouse.cursor.fetchall(), columns=columns)
            if len(df) != expected_rows:
                raise ValueError(
                    f"Snowflake row-count mismatch for {date_str}: "
                    f"expected {expected_rows}, fetched {len(df)}"
                )

            result = archive.archive_dataframe(df, date_str, run_id)
            restored = archive.read_dataframe(
                result["key"], expected_sha256=result["sha256"]
            )
            if len(restored) != expected_rows:
                raise ValueError(f"S3 row-count mismatch for {date_str}")
            print(json.dumps({"api_date": date_str, **result}, sort_keys=True))
    finally:
        warehouse.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    backfill(args.start_date, args.end_date, args.dry_run)


if __name__ == "__main__":
    main()
