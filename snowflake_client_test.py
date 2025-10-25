# snowflake_client_test.py
import pandas as pd
import pendulum
from src.snowflake_client import SnowflakeClient

def main():
    print("🚀 Starting Snowflake client test...")

    # 1️⃣ Initialize client (this connects & ensures schemas/tables)
    client = SnowflakeClient()

    # 2️⃣ Create dummy dataframe
    df = pd.DataFrame([{
        "T": "AAPL",
        "V": 1234567,
        "VW": 182.75,
        "O": 181.2,
        "C": 183.4,
        "H": 184.1,
        "L": 180.5,
        "N": 4200,
        "DATE": pendulum.now().to_date_string(),
        "INGESTED_AT": pendulum.now()
    }])

    print("\n✅ Created dummy DataFrame:")
    print(df.head())

    # 3️⃣ Write dataframe to RAW.DAILY_STOCKS
    success, nrows = client.write_dataframe(df, "DAILY_STOCKS")
    print(f"Write success={success}, nrows={nrows}")

    # 4️⃣ Record a checkpoint
    run_id = f"test_run_{pendulum.now().int_timestamp}"
    api_date = pendulum.now().to_date_string()
    client.record_checkpoint(
        run_id=run_id,
        api_date=api_date,
        status="completed",
        total_tickers=1,
        rows_inserted=nrows,
        error_message=None
    )

    # 5️⃣ Get completed dates
    completed = client.get_completed_dates()
    print(f"\n📅 Completed dates found: {completed}")

    # 6️⃣ Close connection
    client.close()
    print("\n🏁 Test finished successfully!")

if __name__ == "__main__":
    main()
