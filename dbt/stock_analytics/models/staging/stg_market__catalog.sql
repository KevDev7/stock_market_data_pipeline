-- Daily source-standardized security catalog. Business eligibility belongs downstream.
WITH raw AS (
    SELECT API_DATE AS trade_date, INGESTED_AT AS ingested_at, RUN_ID AS run_id,
           RAW_PAYLOAD AS source_payload, TRY_PARSE_JSON(RAW_PAYLOAD) AS payload
    FROM {{ source('raw_reference', 'REFERENCE_RAW') }}
    WHERE SOURCE = 'massive_ticker_catalog'
)
SELECT
    trade_date,
    NULLIF(payload:"ticker"::STRING, '') AS ticker,
    NULLIF(payload:"name"::STRING, '') AS company,
    NULLIF(payload:"cik"::STRING, '') AS cik,
    NULLIF(payload:"share_class_figi"::STRING, '') AS share_class_figi,
    NULLIF(payload:"composite_figi"::STRING, '') AS composite_figi,
    payload:"type"::STRING AS security_type,
    payload:"locale"::STRING AS locale,
    payload:"primary_exchange"::STRING AS exchange,
    UPPER(payload:"currency_name"::STRING) AS currency,
    payload:"active"::BOOLEAN AS active,
    ingested_at
FROM raw
WHERE payload IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY trade_date, payload:"ticker"::STRING
    ORDER BY ingested_at DESC, run_id DESC, source_payload
) = 1
