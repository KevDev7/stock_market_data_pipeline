-- Observed attribute history, not a claim of exact historical change dates.
{{ config(materialized='table') }}
SELECT
    MD5(security_key || '|' || TO_VARCHAR(valid_from)) AS security_history_key,
    security_key, ticker, company, cik, sic_code, sic_description,
    MD5(sector) AS sector_key, sector AS sector_name,
    asset_class, location, exchange, currency, market_currency,
    CAST(NULL AS DOUBLE) AS index_weight,
    valid_from, valid_to, last_observed,
    IFF(valid_to=TO_DATE('3000-01-01'),1,0) AS is_current
FROM {{ ref('int_market__security_history') }}
