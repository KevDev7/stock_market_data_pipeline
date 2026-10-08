-- Flags tickers with more than one current Type 2 security history row.
SELECT
    security_key,
    COUNT(*) AS current_rows
FROM {{ ref('dim_security_history') }}
WHERE is_current = 1
GROUP BY security_key
HAVING COUNT(*) > 1
