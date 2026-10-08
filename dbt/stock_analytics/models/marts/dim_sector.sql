-- Conformed sector dimension shared by security, sector, and market facts.
{{ config(materialized = 'table') }}

WITH sectors AS (
    SELECT DISTINCT
        COALESCE(sector, 'Unknown') AS sector_name
    FROM {{ ref('calc_security_daily_momentum') }}
    WHERE trade_date BETWEEN TO_DATE('{{ var("analysis_start", "2024-01-01") }}')
                         AND TO_DATE('{{ var("analysis_end", "2025-12-31") }}')
)

SELECT
    MD5(sector_name) AS sector_key,
    sector_name
FROM sectors
ORDER BY sector_name
