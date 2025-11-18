# src/config.py
# Centralized configuration loader for local execution and Airflow deployments.
from dotenv import load_dotenv
import os
from pathlib import Path

# Detect whether the code is running inside Airflow; fallback to .env locally.
try:
    from airflow.models import Variable
    IS_AIRFLOW = True
except ImportError:
    IS_AIRFLOW = False
    load_dotenv()

def get_config_value(key, default=None):
    """Fetch a configuration value from Airflow Variables or environment vars."""
    if IS_AIRFLOW:
        return Variable.get(key, default_var=os.getenv(key, default))
    return os.getenv(key, default)

# Repository root
PROJECT_ROOT = Path(__file__).parent.parent

# Polygon API Configuration
POLYGON_API_KEY = get_config_value("POLYGON_API_KEY")
API_BASE_URL = get_config_value("API_BASE_URL")

# Snowflake configuration settings
SNOWFLAKE = {
    "account": get_config_value("SNOWFLAKE_ACCOUNT"),
    "user": get_config_value("SNOWFLAKE_USER"),
    "role": get_config_value("SNOWFLAKE_ROLE"),
    "warehouse": get_config_value("SNOWFLAKE_WAREHOUSE"),
    "database": get_config_value("SNOWFLAKE_DATABASE"),
    "schema": get_config_value("SNOWFLAKE_SCHEMA"),
    "private_key_path": get_config_value("PRIVATE_KEY_PATH"),
}