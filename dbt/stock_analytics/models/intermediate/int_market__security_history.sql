-- Compress consecutive daily reference observations only when attributes change.
{{ config(materialized='table') }}
WITH observations AS (
    SELECT *, MD5(CONCAT_WS('|',ticker,COALESCE(company,''),sector,
           COALESCE(cik,''),COALESCE(sic_code,''),COALESCE(exchange,''),COALESCE(currency,''),COALESCE(location,''))) AS attributes_hash
    FROM {{ ref('int_market__reference_daily') }} r
    WHERE EXISTS (SELECT 1 FROM {{ ref('int_market__daily') }} p
                  WHERE r.security_key=p.security_key AND r.ticker=p.ticker AND r.trade_date=p.trade_date)
      AND trade_date BETWEEN TO_DATE('{{ var("analysis_start", "2024-01-01") }}')
                         AND TO_DATE('{{ var("analysis_end", "2025-12-31") }}')
), changes AS (
    SELECT *, IFF(EQUAL_NULL(attributes_hash,
        LAG(attributes_hash) OVER(PARTITION BY security_key ORDER BY trade_date)),0,1) AS new_version
    FROM observations
), numbered AS (
    SELECT *, SUM(new_version) OVER(PARTITION BY security_key ORDER BY trade_date) AS version_number
    FROM changes
), versions AS (
    SELECT security_key,version_number, MIN(trade_date) AS valid_from, MAX(trade_date) AS last_observed,
           MIN(ticker) AS ticker, MIN(company) AS company, MIN(sector) AS sector,
           MIN(cik) AS cik, MIN(sic_code) AS sic_code, MIN(sic_description) AS sic_description,
           MIN(asset_class) AS asset_class, MIN(location) AS location,
           MIN(exchange) AS exchange, MIN(currency) AS currency, MIN(market_currency) AS market_currency
    FROM numbered GROUP BY security_key,version_number
), bounds AS (
    SELECT MAX(trade_date) AS catalog_through FROM {{ ref('int_market__reference_daily') }}
)
SELECT v.*,
    IFF(LEAD(valid_from) OVER(PARTITION BY security_key ORDER BY valid_from) IS NOT NULL,
        DATEADD(day,-1,LEAD(valid_from) OVER(PARTITION BY security_key ORDER BY valid_from)),
        IFF(last_observed=b.catalog_through,TO_DATE('3000-01-01'),last_observed)) AS valid_to
FROM versions v CROSS JOIN bounds b
