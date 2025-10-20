# file is test_keypair_conn.py
from dotenv import load_dotenv
import os
import snowflake.connector

load_dotenv()

print("🔹 Minimal keypair connection test...")

try:
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        region=os.getenv("SNOWFLAKE_REGION"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key_file=os.getenv("PRIVATE_KEY_PATH"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    print("✅ Connection succeeded!")
    print(conn.cursor().execute("SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_USER();").fetchone())
    conn.close()

except Exception as e:
    print("❌ Connection failed:")
    print(e)
