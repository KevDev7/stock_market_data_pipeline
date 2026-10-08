SELECT * FROM {{ ref('fct_security_daily_momentum') }}
WHERE trade_date<TO_DATE('{{ var("analysis_start", "2024-01-01") }}')
   OR trade_date>TO_DATE('{{ var("analysis_end", "2025-12-31") }}')
