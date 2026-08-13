-- Standardizes raw Polygon.io/Massive.com daily payloads into staging format.
WITH raw_rows AS (
    SELECT
        API_DATE AS trade_date,
        INGESTED_AT AS ingested_at,
        TRY_PARSE_JSON(RAW_PAYLOAD) AS payload
    FROM {{ source('raw_market', 'DAILY_STOCKS_RAW') }}
    WHERE API_DATE IS NOT NULL
),

typed AS (
    SELECT
        payload:"T"::STRING AS ticker,
        TRY_TO_NUMBER(payload:"v"::STRING) AS volume,
        TRY_TO_DOUBLE(payload:"vw"::STRING) AS volume_weighted_avg,
        TRY_TO_DOUBLE(payload:"o"::STRING) AS open,
        TRY_TO_DOUBLE(payload:"c"::STRING) AS close,
        TRY_TO_DOUBLE(payload:"h"::STRING) AS high,
        TRY_TO_DOUBLE(payload:"l"::STRING) AS low,
        TRY_TO_NUMBER(payload:"n"::STRING) AS num_transactions,
        trade_date,
        ingested_at
    FROM raw_rows
    WHERE payload IS NOT NULL
)

SELECT
    ticker,
    CAST(volume AS INTEGER) AS volume,
    volume_weighted_avg,
    open,
    close,
    high,
    low,
    CAST(num_transactions AS INTEGER) AS num_transactions,
    trade_date,
    ingested_at,
    IFF(volume > 0, 1, 0) AS has_volume,
    IFF(
        open > 0
        AND close > 0
        AND high > 0
        AND low > 0
        AND close <= high
        AND close >= low
        AND low <= high,
        1, 0
    ) AS is_valid_record
FROM typed
WHERE trade_date IS NOT NULL
