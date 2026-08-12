-- Market-day aggregate fact table for Russell 3000 breadth.
{{ config(materialized = 'table') }}

SELECT
    TO_NUMBER(TO_CHAR(trade_date, 'YYYYMMDD')) AS date_key,
    trade_date,
    stocks_traded,
    unchanged_stocks,
    advances,
    declines,
    up_volume,
    down_volume,
    pct_market_over_sma20,
    pct_market_over_sma50,
    pct_market_over_sma200,
    market_rsi,
    ad_line,
    ad_percentage,
    ad_ratio,
    up_down_volume_ratio,
    market_momentum,
    new_highs,
    new_lows,
    record_high_pct,
    high_low_index
FROM {{ ref('prep_market_daily_breadth') }}
ORDER BY trade_date
