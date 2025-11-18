-- Flags rows where yesterday_close does not match the prior day's close.
WITH agg AS (
    SELECT 
        *,
        LAG(close, 1) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
        ) AS lag_close
    FROM {{ ref('fct_trading_momentum') }}
)
SELECT 
    *
FROM agg
WHERE 
    yesterday_close IS NOT NULL
    AND yesterday_close != lag_close
    AND trade_date >= DATEADD(day, -7, CURRENT_DATE())
