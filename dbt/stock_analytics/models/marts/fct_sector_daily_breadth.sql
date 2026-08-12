-- Sector-day aggregate fact table for Russell 3000 breadth.
{{ config(materialized = 'table') }}

SELECT
    TO_NUMBER(TO_CHAR(trade_date, 'YYYYMMDD')) AS date_key,
    MD5(COALESCE(sector, 'Unknown')) AS sector_key,
    trade_date,
    stocks_traded,
    unchanged_stocks,
    advances,
    declines,
    up_volume,
    down_volume,
    pct_sector_over_sma20,
    pct_sector_over_sma50,
    pct_sector_over_sma200,
    sector_rsi,
    ad_percentage,
    ad_ratio,
    up_down_volume_ratio,
    sector_momentum,
    new_highs,
    new_lows,
    record_high_pct
FROM {{ ref('prep_sector_daily_breadth') }}
ORDER BY trade_date, sector_key
