-- Flags days where record-high percentage exceeds a realistic range.
SELECT 
    *
FROM {{ ref('agg_daily_market_breadth') }}
WHERE 
    record_high_pct > 0.3    -- >30% of market hitting record highs is implausible
    AND trade_date >= DATEADD(day, -7, CURRENT_DATE())
