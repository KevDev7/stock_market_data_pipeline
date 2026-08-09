import streamlit as st

from utilities.dashboard_helpers import (
    apply_dashboard_style,
    format_count,
    format_return,
    render_data_freshness,
    render_page_intro,
)
from utilities.snowflake_helper import qualified_table, query_snowflake

st.set_page_config(page_title="Universe Screener", layout="wide")

apply_dashboard_style()

render_page_intro(
    "Universe Screener",
    "Latest snapshot across tickers from the current securities mart.",
)

sector_query = """
    SELECT DISTINCT s.SECTOR_NAME AS SECTOR
    FROM {snapshot_table} AS f
    INNER JOIN {securities_table} AS s
        ON s.SECURITY_KEY = f.SECURITY_KEY
    ORDER BY SECTOR_NAME
""".format(
    snapshot_table=qualified_table("FCT_SECURITY_CURRENT_SNAPSHOT"),
    securities_table=qualified_table("DIM_SECURITY"),
)
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

conditions = [f"f.LATEST_RSI BETWEEN {rsi_min} AND {rsi_max}"]

if selected_sectors:
    sector_list = ", ".join(f"'{s}'" for s in selected_sectors)
    conditions.append(f"s.SECTOR_NAME IN ({sector_list})")

if apply_return_filter:
    conditions.append(f"f.RETURN_1M >= {min_return_1m_pct / 100}")

if only_over_sma50:
    conditions.append("f.OVER_SMA50 = 1")

if only_golden_cross:
    conditions.append("f.HAS_GOLDEN_CROSS_ACTIVE = 1")

if ticker_search.strip():
    safe_search = ticker_search.replace("'", "''")
    conditions.append(f"s.TICKER ILIKE '%{safe_search}%'")

where_clause = "WHERE " + " AND ".join(conditions)

query = f"""
    SELECT
        s.TICKER,
        s.COMPANY,
        s.SECTOR_NAME AS SECTOR,
        f.LATEST_TRADE_DATE,
        f.LATEST_CLOSE,
        f.PRICE_CHANGE_1D,
        f.RETURN_1D,
        f.RETURN_1W,
        f.RETURN_1M,
        f.RETURN_3M,
        f.RETURN_YTD,
        f.LATEST_RSI,
        f.LATEST_SMA20,
        f.LATEST_SMA50,
        f.LATEST_SMA200,
        f.OVER_SMA50,
        f.HAS_GOLDEN_CROSS_ACTIVE,
        f.DAYS_SINCE_LAST_GOLDEN_CROSS,
        f.PCT_DISTANCE_FROM_52WEEK_HIGH,
        f.PCT_DISTANCE_FROM_52WEEK_LOW,
        f.AVG_VOLUME_20D,
        f.VOLATILITY_20D
    FROM {qualified_table("FCT_SECURITY_CURRENT_SNAPSHOT")} AS f
    INNER JOIN {qualified_table("DIM_SECURITY")} AS s
        ON s.SECURITY_KEY = f.SECURITY_KEY
    {where_clause}
    ORDER BY f.RETURN_1M DESC
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
