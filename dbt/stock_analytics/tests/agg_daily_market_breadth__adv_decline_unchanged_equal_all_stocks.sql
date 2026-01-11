-- Checks for market breadth rows where component counts don't reconcile.
SELECT 
    *
FROM {{ ref('agg_daily_market_breadth') }}
WHERE 
    (advances + declines + unchanged_stocks) != stocks_traded
    AND trade_date >= DATEADD(day, -7, CURRENT_DATE())
