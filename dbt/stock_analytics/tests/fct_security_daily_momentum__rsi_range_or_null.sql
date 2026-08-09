-- Flags rows where RSI falls outside the valid 0-100 range.
SELECT *
FROM {{ ref('fct_security_daily_momentum') }}
WHERE
    rsi IS NOT NULL
    AND (rsi < 0 OR rsi > 100)
    AND trade_date >= DATEADD(day, -7, CURRENT_DATE())
