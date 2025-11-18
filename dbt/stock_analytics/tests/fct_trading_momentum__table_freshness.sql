-- Checks whether fct_trading_momentum has any data in the past 4 days.
WITH dates AS (
    SELECT COUNT(DISTINCT trade_date) AS recent_dates
    FROM {{ ref('fct_trading_momentum') }}
    WHERE trade_date >= DATEADD(day, -4, CURRENT_DATE())
)
SELECT * 
FROM dates
WHERE recent_dates = 0
