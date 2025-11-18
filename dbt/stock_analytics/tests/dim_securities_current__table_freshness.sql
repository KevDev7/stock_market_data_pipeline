-- Checks whether dim_securities_current has any recent trade dates in the past 4 days.
WITH dates AS (
    SELECT 
        COUNT(DISTINCT latest_trade_date) AS recent_dates
    FROM {{ ref('dim_securities_current') }}
    WHERE latest_trade_date >= DATEADD(day, -4, CURRENT_DATE())
)

SELECT *
FROM dates
WHERE recent_dates = 0
