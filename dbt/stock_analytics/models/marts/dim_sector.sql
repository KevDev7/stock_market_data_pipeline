-- Conformed sector dimension shared by security, sector, and market facts.
{{ config(materialized = 'table') }}

WITH sectors AS (
    SELECT DISTINCT
        COALESCE(sector, 'Unknown') AS sector_name
    FROM {{ ref('prep_security_daily_momentum') }}
)

SELECT
    MD5(sector_name) AS sector_key,
    sector_name
FROM sectors
ORDER BY sector_name
