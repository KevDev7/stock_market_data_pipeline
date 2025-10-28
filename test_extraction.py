# test_extraction.py
from src.extraction import fetch_grouped_daily

if __name__ == "__main__":
    df = fetch_grouped_daily("2024-10-15")
    if df is not None:
        print(df.head())
        print(f"✅ Rows returned: {len(df)}")
    else:
        print("❌ No data returned.")
