import streamlit as st

from utilities.dashboard_helpers import (
    apply_dashboard_style,
    format_count,
    format_date,
    format_ratio,
    format_return,
    format_rsi,
    render_data_freshness,
    render_page_intro,
)
from utilities.snowflake_helper import query_snowflake

st.set_page_config(page_title="Market Breadth", layout="wide")

apply_dashboard_style()

render_page_intro(
    "Market Breadth",
    "Aggregated market signals from the daily breadth mart.",
)

query = """
    SELECT *
    FROM MARKET.RAW_MARTS.AGG_DAILY_MARKET_BREADTH
    ORDER BY TRADE_DATE DESC
    LIMIT 30
"""

df = query_snowflake(query)

if df.empty:
    st.warning("No market breadth rows returned.")
    st.stop()

df.columns = df.columns.str.lower()

latest = df.iloc[0]
prev = df.iloc[1] if len(df) > 1 else None

st.markdown("**Key Signals**")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest Trade Date", format_date(latest["trade_date"]))
col2.metric(
    "Stocks Traded",
    format_count(latest["stocks_traded"]),
    format_count(latest["stocks_traded"] - prev["stocks_traded"])
    if prev is not None else None,
)
col3.metric(
    "A/D Ratio",
    format_ratio(latest["ad_ratio"]),
    format_ratio(latest["ad_ratio"] - prev["ad_ratio"])
    if prev is not None else None,
)
col4.metric(
    "% Market Over SMA50",
    format_return(latest["pct_market_over_sma50"]),
    format_return(latest["pct_market_over_sma50"] - prev["pct_market_over_sma50"])
    if prev is not None else None,
)

col5, col6, col7, col8 = st.columns(4)
col5.metric(
    "Market RSI",
    format_rsi(latest["market_rsi"]),
    format_rsi(latest["market_rsi"] - prev["market_rsi"])
    if prev is not None else None,
)
col6.metric(
    "Up/Down Vol Ratio",
    format_ratio(latest["up_down_volume_ratio"]),
    format_ratio(latest["up_down_volume_ratio"] - prev["up_down_volume_ratio"])
    if prev is not None else None,
)
col7.metric(
    "New Highs",
    format_count(latest["new_highs"]),
    format_count(latest["new_highs"] - prev["new_highs"])
    if prev is not None else None,
)
col8.metric(
    "New Lows",
    format_count(latest["new_lows"]),
    format_count(latest["new_lows"] - prev["new_lows"])
    if prev is not None else None,
)

st.markdown("---")
st.markdown("**Signal Trends**")

trend_df = df.sort_values("trade_date").copy()
trend_df["pct_market_over_sma50"] = trend_df["pct_market_over_sma50"] * 100
trend_df = trend_df.set_index("trade_date")

col_trend_1, col_trend_2 = st.columns(2)
with col_trend_1:
    st.line_chart(trend_df[["pct_market_over_sma50"]])
    st.caption("% Market Over SMA50")

with col_trend_2:
    st.line_chart(trend_df[["ad_line"]])
    st.caption("Advance/Decline Line")

st.markdown("---")
st.markdown("**Latest Rows**")

display_columns = [
    "trade_date",
    "stocks_traded",
    "advances",
    "declines",
    "unchanged_stocks",
    "pct_market_over_sma20",
    "pct_market_over_sma50",
    "pct_market_over_sma200",
    "market_rsi",
    "ad_line",
    "ad_ratio",
    "ad_percentage",
    "up_down_volume_ratio",
    "new_highs",
    "new_lows",
    "record_high_pct",
    "high_low_index",
    "market_momentum",
]

format_map = {
    "trade_date": "{:%Y-%m-%d}",
    "stocks_traded": "{:,.0f}",
    "advances": "{:,.0f}",
    "declines": "{:,.0f}",
    "unchanged_stocks": "{:,.0f}",
    "pct_market_over_sma20": "{:.2%}",
    "pct_market_over_sma50": "{:.2%}",
    "pct_market_over_sma200": "{:.2%}",
    "market_rsi": "{:.1f}",
    "ad_line": "{:,.0f}",
    "ad_ratio": "{:.2f}",
    "ad_percentage": "{:.2%}",
    "up_down_volume_ratio": "{:.2f}",
    "new_highs": "{:,.0f}",
    "new_lows": "{:,.0f}",
    "record_high_pct": "{:.2%}",
    "high_low_index": "{:.2f}",
}

st.dataframe(
    df[display_columns].style.format(format_map),
    use_container_width=True,
)

render_data_freshness(data_through=latest["trade_date"])
st.caption("Returns and percent metrics are stored as decimals in the marts.")
