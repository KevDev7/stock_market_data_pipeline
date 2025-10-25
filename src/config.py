# src/config.py

from dotenv import load_dotenv
import os
from pathlib import Path

# Airflow fallback detection
try:
    from airflow.models import Variable
    IS_AIRFLOW = True
except ImportError:
    IS_AIRFLOW = False
    load_dotenv()

def get_config_value(key, default=None):
    """Return config from Airflow Variable if available, else from .env."""
    if IS_AIRFLOW:
        return Variable.get(key, default_var=os.getenv(key, default))
    return os.getenv(key, default)

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Polygon API Configuration
POLYGON_API_KEY = get_config_value("POLYGON_API_KEY")
API_BASE_URL = get_config_value("API_BASE_URL")

# Snowflake Configuration
SNOWFLAKE = {
    "account": get_config_value("SNOWFLAKE_ACCOUNT"),
    # "region": get_config_value("SNOWFLAKE_REGION"),
    "user": get_config_value("SNOWFLAKE_USER"),
    "role": get_config_value("SNOWFLAKE_ROLE"),
    "warehouse": get_config_value("SNOWFLAKE_WAREHOUSE"),
    "database": get_config_value("SNOWFLAKE_DATABASE"),
    "schema": get_config_value("SNOWFLAKE_SCHEMA"),
    "private_key_path": get_config_value("PRIVATE_KEY_PATH"),
}

# For later when we move to Docker
# KEYS_DIR = PROJECT_ROOT / "keys"
# PRIVATE_KEY_FILE = KEYS_DIR / "rsa_key.pem"