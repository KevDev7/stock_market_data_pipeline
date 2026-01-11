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
