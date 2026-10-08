-- Quarterly and first-observed issuer responses; no validity-window inference here.
WITH raw AS (
    SELECT API_DATE AS observation_date, INGESTED_AT AS ingested_at, RUN_ID AS run_id,
           RAW_PAYLOAD AS source_payload, TRY_PARSE_JSON(RAW_PAYLOAD) AS payload
    FROM {{ source('raw_reference', 'REFERENCE_RAW') }}
    WHERE SOURCE = 'massive_ticker_overview'
)
SELECT
    observation_date,
    NULLIF(payload:"ticker"::STRING, '') AS ticker,
    NULLIF(payload:"cik"::STRING, '') AS cik,
    NULLIF(payload:"sic_code"::STRING, '') AS sic_code,
    NULLIF(payload:"sic_description"::STRING, '') AS sic_description,
    NULLIF(payload:"address":"country"::STRING, '') AS location,
    ingested_at
FROM raw
WHERE payload IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY observation_date,
                 COALESCE(payload:"cik"::STRING, 'TICKER:' || payload:"ticker"::STRING)
    ORDER BY ingested_at DESC, run_id DESC, source_payload
) = 1
