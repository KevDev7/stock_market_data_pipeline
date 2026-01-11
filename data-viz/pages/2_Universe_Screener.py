import streamlit as st

from utilities.dashboard_helpers import (
    apply_dashboard_style,
    format_count,
    format_return,
    render_data_freshness,
    render_page_intro,
)
from utilities.snowflake_helper import query_snowflake

st.set_page_config(page_title="Universe Screener", layout="wide")

apply_dashboard_style()

render_page_intro(
    "Universe Screener",
    "Latest snapshot across tickers from the current securities mart.",
)

sector_query = """
    SELECT DISTINCT SECTOR
    FROM MARKET.RAW_MARTS.DIM_SECURITIES_CURRENT
    ORDER BY SECTOR
"""
sectors_df = query_snowflake(sector_query)
sectors = sectors_df["SECTOR"].dropna().tolist()

st.sidebar.header("Filters")

selected_sectors = st.sidebar.multiselect("Sectors", options=sectors)
rsi_min, rsi_max = st.sidebar.slider("Latest RSI Range", 0, 100, (20, 80))

apply_return_filter = st.sidebar.checkbox(
    "Filter by Min 1M Return (%)",
    value=False,
)
min_return_1m_pct = st.sidebar.number_input(
    "Min 1M Return (%)",
    value=-10.0,
    step=1.0,
    format="%.2f",
)

only_over_sma50 = st.sidebar.checkbox("Only Over SMA50", value=False)
only_golden_cross = st.sidebar.checkbox("Only Golden Cross Active", value=False)

ticker_search = st.sidebar.text_input("Ticker Contains", value="")

row_limit = st.sidebar.number_input(
    "Max Rows",
    min_value=100,
    max_value=5000,
    value=500,
    step=100,
)

conditions = [f"LATEST_RSI BETWEEN {rsi_min} AND {rsi_max}"]

if selected_sectors:
    sector_list = ", ".join(f"'{s}'" for s in selected_sectors)
    conditions.append(f"SECTOR IN ({sector_list})")

if apply_return_filter:
    conditions.append(f"RETURN_1M >= {min_return_1m_pct / 100}")

if only_over_sma50:
    conditions.append("OVER_SMA50 = 1")

if only_golden_cross:
    conditions.append("HAS_GOLDEN_CROSS_ACTIVE = 1")

if ticker_search.strip():
    safe_search = ticker_search.replace("'", "''")
    conditions.append(f"TICKER ILIKE '%{safe_search}%'")

where_clause = "WHERE " + " AND ".join(conditions)

query = f"""
    SELECT
        TICKER,
        COMPANY,
        SECTOR,
        LATEST_TRADE_DATE,
        LATEST_CLOSE,
        PRICE_CHANGE_1D,
        RETURN_1D,
        RETURN_1W,
        RETURN_1M,
        RETURN_3M,
        RETURN_YTD,
        LATEST_RSI,
        LATEST_SMA20,
        LATEST_SMA50,
        LATEST_SMA200,
        OVER_SMA50,
        HAS_GOLDEN_CROSS_ACTIVE,
        DAYS_SINCE_LAST_GOLDEN_CROSS,
        PCT_DISTANCE_FROM_52WEEK_HIGH,
        PCT_DISTANCE_FROM_52WEEK_LOW,
        AVG_VOLUME_20D,
        VOLATILITY_20D
    FROM MARKET.RAW_MARTS.DIM_SECURITIES_CURRENT
    {where_clause}
    ORDER BY RETURN_1M DESC
    LIMIT {row_limit}
"""

df = query_snowflake(query)

if df.empty:
    st.warning("No rows match the current filters.")
    st.stop()

df.columns = df.columns.str.lower()

st.markdown("**Summary**")
summary_col1, summary_col2, summary_col3 = st.columns(3)
summary_col1.metric("Rows Returned", format_count(len(df)))
summary_col2.metric(
    "Median 1M Return",
    format_return(df["return_1m"].median()),
)
summary_col3.metric(
    "% Over SMA50",
    format_return(df["over_sma50"].mean()),
)

st.markdown("---")
st.markdown("**Latest Snapshot**")

display_columns = [
    "ticker",
    "company",
    "sector",
    "latest_trade_date",
    "latest_close",
    "price_change_1d",
    "return_1d",
    "return_1w",
    "return_1m",
    "return_3m",
    "return_ytd",
    "latest_rsi",
    "latest_sma20",
    "latest_sma50",
    "latest_sma200",
    "over_sma50",
    "has_golden_cross_active",
    "days_since_last_golden_cross",
    "pct_distance_from_52week_high",
    "pct_distance_from_52week_low",
    "avg_volume_20d",
    "volatility_20d",
]

format_map = {
    "latest_trade_date": "{:%Y-%m-%d}",
    "latest_close": "${:,.2f}",
    "price_change_1d": "${:,.2f}",
    "return_1d": "{:.2%}",
    "return_1w": "{:.2%}",
    "return_1m": "{:.2%}",
    "return_3m": "{:.2%}",
    "return_ytd": "{:.2%}",
    "latest_rsi": "{:.1f}",
    "latest_sma20": "${:,.2f}",
    "latest_sma50": "${:,.2f}",
    "latest_sma200": "${:,.2f}",
    "days_since_last_golden_cross": "{:,.0f}",
    "pct_distance_from_52week_high": "{:.2%}",
    "pct_distance_from_52week_low": "{:.2%}",
    "avg_volume_20d": "{:,.0f}",
    "volatility_20d": "{:.2f}",
}

st.dataframe(
    df[display_columns].style.format(format_map),
    use_container_width=True,
)

render_data_freshness()
st.caption("Returns and percent fields are stored as decimals in the marts.")
