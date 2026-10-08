-- Standardizes and deduplicates source snapshots; membership windows belong to INTERMEDIATE.
{{ config(
    materialized = 'view'
) }}

WITH russell_snapshots AS (

    SELECT 
        Ticker       AS ticker,
        Name         AS company, 
        Sector       AS sector,
        Asset_Class  AS asset_class,
        Location     AS location,
        Exchange     AS exchange,
        Currency     AS currency,
        Market_Currency AS market_currency,
        TRY_TO_DOUBLE(REPLACE(Market_Value, ',', '')) AS market_value,
        TRY_TO_DOUBLE(REPLACE(Weight, ',', ''))       AS market_weight,
        TO_DATE('2024-12-31') AS snapshot_date
    FROM {{ ref('russell3000_2024_1231') }}

    UNION ALL

    SELECT 
        Ticker       AS ticker,
        Name         AS company, 
        Sector       AS sector,
        Asset_Class  AS asset_class,
        Location     AS location,
        Exchange     AS exchange,
        Currency     AS currency,
        Market_Currency AS market_currency,
        TRY_TO_DOUBLE(REPLACE(Market_Value, ',', '')) AS market_value,
        TRY_TO_DOUBLE(REPLACE(Weight, ',', ''))       AS market_weight,
        TO_DATE('2025-06-30') AS snapshot_date
    FROM {{ ref('russell3000_2025_0630') }}

    UNION ALL

    SELECT 
        Ticker       AS ticker,
        Name         AS company, 
        Sector       AS sector,
        Asset_Class  AS asset_class,
        Location     AS location,
        Exchange     AS exchange,
        Currency     AS currency,
        Market_Currency AS market_currency,
        TRY_TO_DOUBLE(REPLACE(Market_Value, ',', '')) AS market_value,
        TRY_TO_DOUBLE(REPLACE(Weight, ',', ''))       AS market_weight,
        TO_DATE('2025-08-29') AS snapshot_date
    FROM {{ ref('russell3000_2025_0829') }}

    UNION ALL

    SELECT 
        Ticker       AS ticker,
        Name         AS company, 
        Sector       AS sector,
        Asset_Class  AS asset_class,
        Location     AS location,
        Exchange     AS exchange,
        Currency     AS currency,
        Market_Currency AS market_currency,
        TRY_TO_DOUBLE(REPLACE(Market_Value, ',', '')) AS market_value,
        TRY_TO_DOUBLE(REPLACE(Weight, ',', ''))       AS market_weight,
        TO_DATE('2025-09-16') AS snapshot_date
    FROM {{ ref('russell3000_2025_0916') }}
)

SELECT 
    *
FROM russell_snapshots
-- Preserve the history dimension's existing highest-market-value preference,
-- but apply it once for every downstream consumer of the snapshot.
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ticker, snapshot_date
    ORDER BY
        market_value DESC NULLS LAST,
        company ASC NULLS LAST,
        sector ASC NULLS LAST,
        asset_class ASC NULLS LAST,
        location ASC NULLS LAST,
        exchange ASC NULLS LAST,
        currency ASC NULLS LAST,
        market_currency ASC NULLS LAST,
        market_weight DESC NULLS LAST
) = 1
