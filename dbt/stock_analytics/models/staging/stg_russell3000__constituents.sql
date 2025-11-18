-- Combines Russell 3000 seed files into a unified constituents table with validity ranges.
{{ config(
    materialized = 'view'
) }}

WITH russell_snapshots AS (

    SELECT 
        Ticker       AS ticker,
        Name         AS company, 
        Sector       AS sector,
        Market_Value AS market_value,
        Weight       AS market_weight,
        TO_DATE('2023-01-01') AS valid_from,
        TO_DATE('2025-06-29') AS valid_to
    FROM {{ ref('russell3000_2024_1231') }}

    UNION ALL

    SELECT 
        Ticker       AS ticker,
        Name         AS company, 
        Sector       AS sector,
        Market_Value AS market_value,
        Weight       AS market_weight,
        TO_DATE('2025-06-30') AS valid_from,
        TO_DATE('2025-08-28') AS valid_to
    FROM {{ ref('russell3000_2025_0630') }}

    UNION ALL

    SELECT 
        Ticker       AS ticker,
        Name         AS company, 
        Sector       AS sector,
        Market_Value AS market_value,
        Weight       AS market_weight,
        TO_DATE('2025-08-29') AS valid_from,
        TO_DATE('2025-09-15') AS valid_to
    FROM {{ ref('russell3000_2025_0829') }}

    UNION ALL

    SELECT 
        Ticker       AS ticker,
        Name         AS company, 
        Sector       AS sector,
        Market_Value AS market_value,
        Weight       AS market_weight,
        TO_DATE('2025-09-16') AS valid_from,
        TO_DATE('3000-01-01') AS valid_to
    FROM {{ ref('russell3000_2025_0916') }}
)

SELECT 
    *,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS ingested_at
FROM russell_snapshots
