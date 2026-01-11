import streamlit as st

from utilities.dashboard_helpers import (
    apply_dashboard_style,
    format_count,
    format_date,
    format_return,
    format_rsi,
    render_data_freshness,
    render_page_intro,
)
from utilities.snowflake_helper import query_snowflake

st.set_page_config(page_title="Home", layout="wide")

apply_dashboard_style()

render_page_intro(
    "Russell 3000 Market Intelligence",
    "High-level snapshot of market breadth and universe coverage.",
)
st.sidebar.success("Use the sidebar to navigate the marts")

breadth_query = """
    SELECT *
    FROM MARKET.RAW_MARTS.AGG_DAILY_MARKET_BREADTH
    ORDER BY TRADE_DATE DESC
    LIMIT 1
"""
count_query = """
    SELECT COUNT(*) AS TICKER_COUNT
    FROM MARKET.RAW_MARTS.DIM_SECURITIES_CURRENT
"""

breadth_df = query_snowflake(breadth_query)
count_df = query_snowflake(count_query)

if breadth_df.empty:
    st.warning("No market breadth data available in the marts yet.")
else:
    breadth_df.columns = breadth_df.columns.str.lower()
    latest = breadth_df.iloc[0]
    ticker_count = None
    if not count_df.empty:
        ticker_count = count_df.iloc[0]["TICKER_COUNT"]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Latest Trade Date", format_date(latest["trade_date"]))
    col2.metric("Stocks Traded", format_count(latest["stocks_traded"]))
    col3.metric("Advances", format_count(latest["advances"]))
    col4.metric("Declines", format_count(latest["declines"]))

    col5, col6, col7, col8 = st.columns(4)
    col5.metric("Unchanged", format_count(latest["unchanged_stocks"]))
    col6.metric("Market RSI", format_rsi(latest["market_rsi"]))
    col7.metric("% Market Over SMA50", format_return(latest["pct_market_over_sma50"]))
    col8.metric("Record High %", format_return(latest["record_high_pct"]))

    st.markdown("---")
    render_data_freshness(
        data_through=latest["trade_date"],
        ticker_count=ticker_count,
    )

st.caption("Returns and percent metrics are stored as decimals in the marts.")
