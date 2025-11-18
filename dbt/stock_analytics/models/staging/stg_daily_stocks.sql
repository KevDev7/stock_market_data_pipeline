-- Standardizes raw Polygon daily stock data into staging format.
SELECT 
    T                               AS ticker,
    CAST(V AS INTEGER)              AS volume,
    VW                              AS volume_weighted_avg,
    O                               AS open,
    C                               AS close,
    H                               AS high,
    L                               AS low,
    N                               AS num_transactions,
    DATE                            AS trade_date,
    INGESTED_AT                     AS ingested_at,
    IFF(V > 0, 1, 0)                AS has_volume,
    IFF(
        O > 0
        AND C > 0
        AND H > 0
        AND L > 0
        AND C <= H
        AND C >= L
        AND L <= H,
        1, 0
    ) AS is_valid_record
FROM {{ source('raw_market', 'DAILY_STOCKS') }}
WHERE DATE IS NOT NULL

