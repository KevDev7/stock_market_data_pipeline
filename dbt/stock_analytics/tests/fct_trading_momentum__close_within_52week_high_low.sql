-- Flags rows where close price falls outside the 52-week range.
SELECT 
    *
FROM {{ ref('fct_trading_momentum') }}
WHERE 
    (close > high_52week OR close < low_52week)
    AND trade_date >= DATEADD(day, -7, CURRENT_DATE())
