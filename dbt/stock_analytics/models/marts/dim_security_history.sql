-- Type 2 security history dimension from Russell 3000 constituent snapshots.
{{ config(materialized = 'table') }}

WITH source_rows AS (
    SELECT
        ticker,
        company,
        COALESCE(sector, 'Unknown') AS sector_name,
        asset_class,
        location,
        exchange,
        currency,
        market_currency,
        market_value,
        market_weight AS index_weight,
        valid_from,
        valid_to
    FROM {{ ref('stg_russell3000__constituents') }}
    WHERE ticker IS NOT NULL
),

deduped AS (
    SELECT *
    FROM source_rows
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ticker, valid_from, valid_to
        ORDER BY
            market_value DESC NULLS LAST,
            company ASC NULLS LAST,
            sector_name ASC NULLS LAST
    ) = 1
)

SELECT
    MD5(
        ticker
        || '|' || TO_VARCHAR(valid_from)
        || '|' || TO_VARCHAR(valid_to)
        || '|' || COALESCE(company, '')
        || '|' || COALESCE(sector_name, '')
    ) AS security_history_key,
    MD5(ticker) AS security_key,
    ticker,
    company,
    MD5(sector_name) AS sector_key,
    sector_name,
    asset_class,
    location,
    exchange,
    currency,
    market_currency,
    index_weight,
    valid_from,
    valid_to,
    IFF(valid_to = TO_DATE('3000-01-01'), 1, 0) AS is_current
FROM deduped
ORDER BY ticker, valid_from
