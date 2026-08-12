import plotly.express as px
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

st.set_page_config(page_title="Sector Breadth", layout="wide")

apply_dashboard_style()

render_page_intro(
    "Sector Breadth",
    "Sector-level participation and momentum from the sector fact mart.",
)

latest_query = """
    SELECT
        s.SECTOR_NAME,
        f.TRADE_DATE,
        f.STOCKS_TRADED,
        f.ADVANCES,
        f.DECLINES,
        f.UNCHANGED_STOCKS,
        f.PCT_SECTOR_OVER_SMA20,
        f.PCT_SECTOR_OVER_SMA50,
        f.PCT_SECTOR_OVER_SMA200,
        f.SECTOR_RSI,
        f.AD_PERCENTAGE,
        f.AD_RATIO,
        f.UP_DOWN_VOLUME_RATIO,
        f.SECTOR_MOMENTUM,
        f.NEW_HIGHS,
        f.NEW_LOWS,
        f.RECORD_HIGH_PCT
    FROM {sector_fact} AS f
    INNER JOIN {sector_dim} AS s
        ON s.SECTOR_KEY = f.SECTOR_KEY
    WHERE f.TRADE_DATE = (
        SELECT MAX(TRADE_DATE)
        FROM {sector_fact}
    )
    ORDER BY f.PCT_SECTOR_OVER_SMA50 DESC
""".format(
    sector_fact=qualified_table("FCT_SECTOR_DAILY_BREADTH"),
    sector_dim=qualified_table("DIM_SECTOR"),
)

trend_query = """
    SELECT
        s.SECTOR_NAME,
        f.TRADE_DATE,
        f.PCT_SECTOR_OVER_SMA50,
        f.SECTOR_RSI,
        f.AD_PERCENTAGE
    FROM {sector_fact} AS f
    INNER JOIN {sector_dim} AS s
        ON s.SECTOR_KEY = f.SECTOR_KEY
    WHERE f.TRADE_DATE >= DATEADD(day, -60, (
        SELECT MAX(TRADE_DATE)
        FROM {sector_fact}
    ))
    ORDER BY f.TRADE_DATE, s.SECTOR_NAME
""".format(
    sector_fact=qualified_table("FCT_SECTOR_DAILY_BREADTH"),
    sector_dim=qualified_table("DIM_SECTOR"),
)

latest_df = query_snowflake(latest_query)
trend_df = query_snowflake(trend_query)

if latest_df.empty:
    st.warning("No sector breadth rows returned.")
    st.stop()

latest_df.columns = latest_df.columns.str.lower()
trend_df.columns = trend_df.columns.str.lower()

latest_date = latest_df.iloc[0]["trade_date"]
strongest = latest_df.iloc[0]
weakest = latest_df.iloc[-1]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest Trade Date", format_date(latest_date))
col2.metric("Strongest Sector", strongest["sector_name"])
col3.metric("Strongest % Over SMA50", format_return(strongest["pct_sector_over_sma50"]))
col4.metric("Weakest % Over SMA50", format_return(weakest["pct_sector_over_sma50"]))

st.markdown("---")

chart_df = latest_df.copy()
chart_df["pct_sector_over_sma50"] = chart_df["pct_sector_over_sma50"] * 100
chart_df["ad_percentage"] = chart_df["ad_percentage"] * 100

left, right = st.columns(2)

with left:
    fig = px.bar(
        chart_df,
        x="pct_sector_over_sma50",
        y="sector_name",
        orientation="h",
        color="sector_rsi",
        color_continuous_scale="RdYlGn",
        title="Latest % of Sector Above SMA50",
        labels={
            "pct_sector_over_sma50": "% Over SMA50",
            "sector_name": "",
            "sector_rsi": "RSI",
        },
    )
    fig.update_layout(
        height=430,
        yaxis={"categoryorder": "total ascending"},
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

with right:
    fig = px.scatter(
        chart_df,
        x="ad_percentage",
        y="sector_rsi",
        size="stocks_traded",
        color="sector_name",
        hover_name="sector_name",
        title="Sector Momentum vs Advance/Decline %",
        labels={
            "ad_percentage": "A/D %",
            "sector_rsi": "Sector RSI",
            "stocks_traded": "Stocks traded",
        },
    )
    fig.add_hline(y=70, line_dash="dot", line_color="#D92D20")
    fig.add_hline(y=30, line_dash="dot", line_color="#039855")
    fig.add_vline(x=0, line_dash="dot", line_color="#667085")
    fig.update_layout(
        height=430,
        legend_title=None,
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

if not trend_df.empty:
    st.markdown("---")
    st.markdown("**60-Day Sector Trend**")

    selected_sector = st.selectbox(
        "Sector",
        options=sorted(trend_df["sector_name"].dropna().unique()),
    )
    selected_trend = trend_df[trend_df["sector_name"] == selected_sector].copy()
    selected_trend["pct_sector_over_sma50"] = selected_trend["pct_sector_over_sma50"] * 100

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=selected_trend["trade_date"],
        y=selected_trend["pct_sector_over_sma50"],
        mode="lines",
        name="% Over SMA50",
    ))
    fig.add_trace(go.Scatter(
        x=selected_trend["trade_date"],
        y=selected_trend["sector_rsi"],
        mode="lines",
        name="Sector RSI",
        yaxis="y2",
    ))
    fig.update_layout(
        title=f"{selected_sector}: Participation and RSI",
        yaxis=dict(title="% Over SMA50"),
        yaxis2=dict(title="RSI", overlaying="y", side="right", range=[0, 100]),
        xaxis_title=None,
        height=380,
        legend_title=None,
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
st.markdown("**Latest Sector Rows**")

display_columns = [
    "sector_name",
    "stocks_traded",
    "advances",
    "declines",
    "unchanged_stocks",
    "pct_sector_over_sma20",
    "pct_sector_over_sma50",
    "pct_sector_over_sma200",
    "sector_rsi",
    "ad_percentage",
    "ad_ratio",
    "up_down_volume_ratio",
    "new_highs",
    "new_lows",
    "record_high_pct",
    "sector_momentum",
]

format_map = {
    "stocks_traded": "{:,.0f}",
    "advances": "{:,.0f}",
    "declines": "{:,.0f}",
    "unchanged_stocks": "{:,.0f}",
    "pct_sector_over_sma20": "{:.2%}",
    "pct_sector_over_sma50": "{:.2%}",
    "pct_sector_over_sma200": "{:.2%}",
    "sector_rsi": "{:.1f}",
    "ad_percentage": "{:.2%}",
    "ad_ratio": "{:.2f}",
    "up_down_volume_ratio": "{:.2f}",
    "new_highs": "{:,.0f}",
    "new_lows": "{:,.0f}",
    "record_high_pct": "{:.2%}",
}

st.dataframe(
    latest_df[display_columns].style.format(format_map),
    width="stretch",
)

render_data_freshness(data_through=latest_date)
st.caption("Sector rows are built from the sector-day fact and conformed sector dimension.")
