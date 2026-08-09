-- Flags rows where yesterday_close does not match the prior trading row's close.
WITH rows_with_lag AS (
    SELECT
        *,
        LAG(close, 1) OVER (
            PARTITION BY security_key
            ORDER BY trade_date
        ) AS lag_close
    FROM {{ ref('fct_security_daily_momentum') }}
)
SELECT
    *
FROM rows_with_lag
WHERE
    yesterday_close IS NOT NULL
    AND yesterday_close != lag_close
    AND trade_date >= DATEADD(day, -7, CURRENT_DATE())
