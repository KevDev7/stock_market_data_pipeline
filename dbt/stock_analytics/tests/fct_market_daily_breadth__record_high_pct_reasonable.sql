-- Flags days where record-high percentage exceeds a realistic range.
SELECT
    *
FROM {{ ref('fct_market_daily_breadth') }}
WHERE
    record_high_pct > 0.3
    AND trade_date >= DATEADD(day, -7, CURRENT_DATE())
