-- Flags tickers with more than one current Type 2 security history row.
SELECT
    ticker,
    COUNT(*) AS current_rows
FROM {{ ref('dim_security_history') }}
WHERE is_current = 1
GROUP BY ticker
HAVING COUNT(*) > 1
