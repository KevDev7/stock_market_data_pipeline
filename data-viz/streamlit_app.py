import plotly.express as px
import plotly.graph_objects as go
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
from utilities.snowflake_helper import qualified_table, query_snowflake

st.set_page_config(page_title="Home", layout="wide")

apply_dashboard_style()

render_page_intro(
    "Russell 3000 Market Intelligence",
    "Executive snapshot from Snowflake dimensional marts.",
)
st.caption("Historical portfolio snapshot. Scheduled market-data ingestion is currently paused.")
st.sidebar.success("Use the sidebar to navigate the marts")

breadth_query = """
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
        RECORD_HIGH_PCT,
        NEW_HIGHS,
        NEW_LOWS,
        AD_LINE
    FROM {breadth_table}
    ORDER BY TRADE_DATE DESC
    LIMIT 60
""".format(breadth_table=qualified_table("FCT_MARKET_DAILY_BREADTH"))

snapshot_query = """
    SELECT COUNT(*) AS TICKER_COUNT
    FROM {snapshot_table}
""".format(snapshot_table=qualified_table("FCT_SECURITY_CURRENT_SNAPSHOT"))

sector_query = """
    SELECT
        s.SECTOR_NAME,
        f.STOCKS_TRADED,
        f.PCT_SECTOR_OVER_SMA50,
        f.SECTOR_RSI,
        f.AD_PERCENTAGE,
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

breadth_df = query_snowflake(breadth_query)
snapshot_df = query_snowflake(snapshot_query)
sector_df = query_snowflake(sector_query)

if breadth_df.empty:
    st.warning("No market breadth data available in the marts yet.")
    st.stop()

breadth_df.columns = breadth_df.columns.str.lower()
sector_df.columns = sector_df.columns.str.lower()

latest = breadth_df.iloc[0]
ticker_count = None
if not snapshot_df.empty:
    ticker_count = snapshot_df.iloc[0]["TICKER_COUNT"]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Latest Trade Date", format_date(latest["trade_date"]))
col2.metric("Universe Coverage", format_count(ticker_count))
col3.metric("Market RSI", format_rsi(latest["market_rsi"]))
col4.metric("% Over SMA50", format_return(latest["pct_market_over_sma50"]))

col5, col6, col7, col8 = st.columns(4)
col5.metric("Advances", format_count(latest["advances"]))
col6.metric("Declines", format_count(latest["declines"]))
col7.metric("Record High %", format_return(latest["record_high_pct"]))
col8.metric("New Highs / Lows", f"{format_count(latest['new_highs'])} / {format_count(latest['new_lows'])}")

st.markdown("---")

trend_df = breadth_df.sort_values("trade_date").copy()
trend_df["pct_market_over_sma20"] = trend_df["pct_market_over_sma20"] * 100
trend_df["pct_market_over_sma50"] = trend_df["pct_market_over_sma50"] * 100
trend_df["pct_market_over_sma200"] = trend_df["pct_market_over_sma200"] * 100

left, right = st.columns([1.2, 1])

with left:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend_df["trade_date"],
        y=trend_df["pct_market_over_sma20"],
        mode="lines",
        name="SMA20",
    ))
    fig.add_trace(go.Scatter(
        x=trend_df["trade_date"],
        y=trend_df["pct_market_over_sma50"],
        mode="lines",
        name="SMA50",
    ))
    fig.add_trace(go.Scatter(
        x=trend_df["trade_date"],
        y=trend_df["pct_market_over_sma200"],
        mode="lines",
        name="SMA200",
    ))
    fig.update_layout(
        title="% of Market Above Moving Averages",
        yaxis_title="Percent",
        xaxis_title=None,
        height=360,
        legend_title=None,
        margin=dict(l=10, r=10, t=50, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)

with right:
    if not sector_df.empty:
        sector_plot = sector_df.copy()
        sector_plot["pct_sector_over_sma50"] = sector_plot["pct_sector_over_sma50"] * 100
        fig = px.bar(
            sector_plot,
            x="pct_sector_over_sma50",
            y="sector_name",
            orientation="h",
            title="Latest Sector Strength",
            labels={
                "pct_sector_over_sma50": "% Over SMA50",
                "sector_name": "",
            },
            color="sector_rsi",
            color_continuous_scale="RdYlGn",
        )
        fig.update_layout(
            height=360,
            yaxis={"categoryorder": "total ascending"},
            margin=dict(l=10, r=10, t=50, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
render_data_freshness(
    data_through=latest["trade_date"],
    ticker_count=ticker_count,
)
st.caption("Returns and percent metrics are stored as decimals in the marts.")
