from dotenv import load_dotenv
import os
import requests
from datetime import date

load_dotenv()
print("🔹 Testing Polygon API connection (Paid tier mode)...")

API_KEY = os.getenv("POLYGON_API_KEY")
today = date.today().strftime("%Y-%m-%d")
url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{today}?apiKey={API_KEY}"

try:
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        print("✅ Success! Polygon Aggregates endpoint working.")
        print(f"Results: {len(data.get('results', []))} tickers returned.")
        print(f"First ticker: {data['results'][0].get('T', 'Unknown')}")
    else:
        print(f"❌ Failed. Status {response.status_code}")
        print(response.text[:300])
except Exception as e:
    print("❌ Error during API call:")
    print(e)

