import streamlit as st
import pandas as pd
import snowflake.connector
from cryptography.hazmat.primitives import serialization


def _load_private_key():
    """Load RSA private key from Streamlit secrets and convert to DER bytes."""

    pem_text = st.secrets["snowflake"]["private_key"]

    private_key_obj = serialization.load_pem_private_key(
        pem_text.encode("utf-8"),
        password=None
    )

    # Convert Snowflake needs: PKCS8 DER binary
    private_key_der = private_key_obj.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return private_key_der


def get_snowflake_connection():
    """Create a Snowflake connection using private-key authentication."""
    private_key_der = _load_private_key()

    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        role=st.secrets["snowflake"]["role"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        private_key=private_key_der,
    )


def qualified_table(table_name: str) -> str:
    """Build a fully qualified table name from Streamlit Snowflake secrets."""
    if not table_name.replace("_", "").isalnum():
        raise ValueError(f"Invalid table name: {table_name}")

    snowflake_secrets = st.secrets["snowflake"]
    database = snowflake_secrets["database"]
    schema = snowflake_secrets.get("mart_schema", "MARTS")
    return f"{database}.{schema}.{table_name}"


@st.cache_data(ttl=86400, show_spinner=False)
def query_snowflake(sql: str) -> pd.DataFrame:
    """Run SQL query against Snowflake and return pandas DataFrame."""
    conn = get_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        cur.close()
        conn.close()
