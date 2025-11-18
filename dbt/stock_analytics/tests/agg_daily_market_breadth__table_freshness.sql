-- Checks whether any market breadth data has been loaded in the past 4 days.
WITH dates AS (
    SELECT 
        COUNT(DISTINCT trade_date) AS recent_dates
    FROM {{ ref('agg_daily_market_breadth') }}
    WHERE trade_date >= DATEADD(day, -4, CURRENT_DATE())
)
SELECT * 
FROM dates
WHERE recent_dates = 0
