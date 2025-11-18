-- Flags rows where SMA indicators are inconsistently populated.
SELECT *
FROM {{ ref('fct_trading_momentum') }}
WHERE 
    ((sma_200 IS NOT NULL AND sma_50 IS NULL)
     OR (sma_200 IS NOT NULL AND sma_20 IS NULL)
     OR (sma_50 IS NOT NULL AND sma_20 IS NULL))
    AND trade_date >= DATEADD(day, -7, CURRENT_DATE())
