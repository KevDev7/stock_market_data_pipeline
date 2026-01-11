import pandas as pd
import streamlit as st

from utilities.dashboard_helpers import (
    apply_dashboard_style,
    format_date,
    format_price,
    format_ratio,
    format_rsi,
    render_data_freshness,
    render_page_intro,
)
from utilities.snowflake_helper import query_snowflake

st.set_page_config(page_title="Ticker Momentum", layout="wide")

apply_dashboard_style()

render_page_intro(
    "Ticker Momentum",
    "Time-series signals for a single ticker from the trading momentum mart.",
)

ticker_query = """
    SELECT DISTINCT TICKER
    FROM MARKET.RAW_MARTS.DIM_SECURITIES_CURRENT
    ORDER BY TICKER
"""
date_query = """
    SELECT MIN(TRADE_DATE) AS MIN_DATE, MAX(TRADE_DATE) AS MAX_DATE
    FROM MARKET.RAW_MARTS.FCT_TRADING_MOMENTUM
"""

tickers_df = query_snowflake(ticker_query)
dates_df = query_snowflake(date_query)

tickers = tickers_df["TICKER"].dropna().tolist()

if dates_df.empty:
    st.warning("No momentum data available in the marts yet.")
    st.stop()

min_date = dates_df.iloc[0]["MIN_DATE"]
max_date = dates_df.iloc[0]["MAX_DATE"]

st.sidebar.header("Filters")

selected_ticker = st.sidebar.selectbox("Ticker", options=tickers)

default_start = pd.to_datetime(max_date) - pd.Timedelta(days=90)
date_range = st.sidebar.date_input(
    "Trade Date Range",
    value=(default_start.date(), pd.to_datetime(max_date).date()),
    min_value=pd.to_datetime(min_date).date(),
    max_value=pd.to_datetime(max_date).date(),
)

if isinstance(date_range, tuple):
    start_date, end_date = date_range
else:
    start_date = date_range
    end_date = date_range

row_limit = st.sidebar.number_input(
    "Max Rows",
    min_value=50,
    max_value=2000,
    value=200,
    step=50,
)

query = f"""
    SELECT
        TICKER,
        TRADE_DATE,
        OPEN,
        HIGH,
        LOW,
        CLOSE,
        YESTERDAY_CLOSE,
        VOLUME,
        SMA_20,
        SMA_50,
        SMA_200,
        RSI,
        REL_VOL,
        HIGH_52WEEK,
        LOW_52WEEK,
        BULLISH_CROSSOVER,
        GOLDEN_CROSS,
        DEATH_CROSS
    FROM MARKET.RAW_MARTS.FCT_TRADING_MOMENTUM
    WHERE TICKER = '{selected_ticker}'
      AND TRADE_DATE BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY TRADE_DATE DESC
    LIMIT {row_limit}
"""

df = query_snowflake(query)

if df.empty:
    st.warning("No rows returned for the selected ticker/date range.")
    st.stop()

df.columns = df.columns.str.lower()

latest = df.iloc[0]
signal_label = "None"
if pd.notna(latest["golden_cross"]) and latest["golden_cross"] == 1:
    signal_label = "Golden Cross"
elif pd.notna(latest["death_cross"]) and latest["death_cross"] == 1:
    signal_label = "Death Cross"

st.markdown("**Latest Signal Snapshot**")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Latest Trade Date", format_date(latest["trade_date"]))
col2.metric("Close", format_price(latest["close"]))
col3.metric("RSI", format_rsi(latest["rsi"]))
col4.metric("Rel Vol", format_ratio(latest["rel_vol"]))
col5.metric("Signal", signal_label)

st.markdown("---")
st.markdown("**Price + SMA Trends**")

trend_df = df.sort_values("trade_date").set_index("trade_date")
st.line_chart(trend_df[["close", "sma_20", "sma_50", "sma_200"]])

st.markdown("---")
st.markdown("**Latest Rows**")

format_map = {
    "trade_date": "{:%Y-%m-%d}",
    "open": "${:,.2f}",
    "high": "${:,.2f}",
    "low": "${:,.2f}",
    "close": "${:,.2f}",
    "yesterday_close": "${:,.2f}",
    "volume": "{:,.0f}",
    "sma_20": "${:,.2f}",
    "sma_50": "${:,.2f}",
    "sma_200": "${:,.2f}",
    "rsi": "{:.1f}",
    "rel_vol": "{:.2f}",
    "high_52week": "${:,.2f}",
    "low_52week": "${:,.2f}",
}

st.dataframe(df.style.format(format_map), use_container_width=True)
render_data_freshness()
st.caption("Returns and percent metrics are stored as decimals in the marts.")
