SELECT f.security_key,f.trade_date,f.sector,h.sector_name
FROM {{ ref('int_market__daily') }} f
LEFT JOIN {{ ref('dim_security_history') }} h ON f.security_key=h.security_key
    AND f.trade_date BETWEEN h.valid_from AND h.valid_to
WHERE f.trade_date BETWEEN TO_DATE('{{ var("analysis_start", "2024-01-01") }}')
                         AND TO_DATE('{{ var("analysis_end", "2025-12-31") }}')
  AND (h.security_key IS NULL OR NOT EQUAL_NULL(f.sector,h.sector_name)
       OR NOT EQUAL_NULL(f.ticker,h.ticker) OR NOT EQUAL_NULL(f.company,h.company))
