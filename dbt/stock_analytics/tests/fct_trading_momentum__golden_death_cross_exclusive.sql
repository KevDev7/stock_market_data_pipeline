-- Flags rows where both golden_cross and death_cross are simultaneously true.
SELECT *
FROM {{ ref('fct_trading_momentum') }}
WHERE 
    golden_cross = 1 
    AND death_cross = 1
    AND trade_date >= DATEADD(day, -7, CURRENT_DATE())
