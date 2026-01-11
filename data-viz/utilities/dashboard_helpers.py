import pandas as pd
import streamlit as st

from utilities.snowflake_helper import query_snowflake


def apply_dashboard_style():
    st.markdown(
        """
        <style>
        .dashboard-subtitle {
            color: #475467;
            margin-top: -0.75rem;
            margin-bottom: 1rem;
            font-size: 0.95rem;
        }
        div[data-testid="stMetricDelta"] > div {
            color: #667085 !important;
        }
        hr {
            border: 0;
            border-top: 1px solid #E4E7EC;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_page_intro(title: str, description: str):
    st.title(title)
    st.markdown(
        f"<div class='dashboard-subtitle'>{description}</div>",
        unsafe_allow_html=True,
    )


def format_count(value):
    if pd.isna(value):
        return "N/A"
    return f"{value:,.0f}"


def format_price(value):
    if pd.isna(value):
        return "N/A"
    return f"${value:,.2f}"


def format_return(value):
    if pd.isna(value):
        return "N/A"
    return f"{value * 100:.2f}%"


def format_ratio(value):
    if pd.isna(value):
        return "N/A"
    return f"{value:.2f}"


def format_rsi(value):
    if pd.isna(value):
        return "N/A"
    return f"{value:.1f}"


def format_date(value):
    if pd.isna(value):
        return "N/A"
    return pd.to_datetime(value).strftime("%Y-%m-%d")


def get_data_freshness():
    date_query = """
        SELECT MAX(TRADE_DATE) AS DATA_THROUGH
        FROM MARKET.RAW_MARTS.AGG_DAILY_MARKET_BREADTH
    """
    count_query = """
        SELECT COUNT(*) AS TICKER_COUNT
        FROM MARKET.RAW_MARTS.DIM_SECURITIES_CURRENT
    """

    date_df = query_snowflake(date_query)
    count_df = query_snowflake(count_query)

    data_through = None
    if not date_df.empty:
        data_through = date_df.iloc[0]["DATA_THROUGH"]

    ticker_count = None
    if not count_df.empty:
        ticker_count = count_df.iloc[0]["TICKER_COUNT"]

    return data_through, ticker_count


def render_data_freshness(data_through=None, ticker_count=None):
    if data_through is None or ticker_count is None:
        data_through, ticker_count = get_data_freshness()
    if pd.isna(data_through) and pd.isna(ticker_count):
        return
    st.caption(
        "Data through: "
        f"{format_date(data_through)}"
        " \u00b7 Coverage: "
        f"{format_count(ticker_count)} tickers"
    )
