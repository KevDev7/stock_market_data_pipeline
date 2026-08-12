import plotly.graph_objects as go
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
from utilities.snowflake_helper import qualified_table, query_snowflake

st.set_page_config(page_title="Market Breadth", layout="wide")

apply_dashboard_style()

render_page_intro(
    "Market Breadth",
    "Market-wide participation, trend, and breadth health across the Russell 3000.",
)

query = """
    SELECT
        TRADE_DATE,
        STOCKS_TRADED,
        ADVANCES,
        DECLINES,
        UNCHANGED_STOCKS,
        PCT_MARKET_OVER_SMA20,
        PCT_MARKET_OVER_SMA50,
        PCT_MARKET_OVER_SMA200,
        MARKET_RSI,
        AD_LINE,
        AD_RATIO,
        AD_PERCENTAGE,
        UP_DOWN_VOLUME_RATIO,
        NEW_HIGHS,
        NEW_LOWS,
        RECORD_HIGH_PCT,
        HIGH_LOW_INDEX,
        MARKET_MOMENTUM
    FROM {breadth_table}
    ORDER BY TRADE_DATE DESC
    LIMIT 120
""".format(breadth_table=qualified_table("FCT_MARKET_DAILY_BREADTH"))

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
st.markdown("**Breadth Trends**")

trend_df = df.sort_values("trade_date").copy()
for col in ["pct_market_over_sma20", "pct_market_over_sma50", "pct_market_over_sma200"]:
    trend_df[col] = trend_df[col] * 100

left, right = st.columns(2)

with left:
    fig = go.Figure()
    for col, label in [
        ("pct_market_over_sma20", "SMA20"),
        ("pct_market_over_sma50", "SMA50"),
        ("pct_market_over_sma200", "SMA200"),
    ]:
        fig.add_trace(go.Scatter(
            x=trend_df["trade_date"],
            y=trend_df[col],
            mode="lines",
            name=label,
        ))
    fig.update_layout(
        title="% of Stocks Above Moving Averages",
        yaxis_title="Percent",
        xaxis_title=None,
        height=360,
        legend_title=None,
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

with right:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend_df["trade_date"],
        y=trend_df["ad_line"],
        mode="lines",
        name="A/D Line",
    ))
    fig.update_layout(
        title="Advance/Decline Line",
        yaxis_title="Cumulative breadth",
        xaxis_title=None,
        height=360,
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

left2, right2 = st.columns(2)

with left2:
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=trend_df["trade_date"],
        y=trend_df["new_highs"],
        name="New Highs",
    ))
    fig.add_trace(go.Bar(
        x=trend_df["trade_date"],
        y=trend_df["new_lows"],
        name="New Lows",
    ))
    fig.update_layout(
        title="New Highs vs New Lows",
        barmode="group",
        yaxis_title="Count",
        xaxis_title=None,
        height=340,
        legend_title=None,
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

with right2:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend_df["trade_date"],
        y=trend_df["market_rsi"],
        mode="lines",
        name="Market RSI",
    ))
    fig.add_hline(y=70, line_dash="dot", line_color="#D92D20")
    fig.add_hline(y=30, line_dash="dot", line_color="#039855")
    fig.update_layout(
        title="Market RSI",
        yaxis_title="RSI",
        xaxis_title=None,
        height=340,
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

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
    width="stretch",
)

render_data_freshness(data_through=latest["trade_date"])
st.caption("Returns and percent metrics are stored as decimals in the marts.")
