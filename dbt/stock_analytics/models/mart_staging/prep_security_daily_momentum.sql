-- Prepares daily security-level momentum measures before loading the dimensional marts.
{{ config(
    materialized = 'incremental',
    unique_key = ['ticker', 'trade_date'],
    cluster_by = ['ticker'],
    on_schema_change = 'fail'
) }}

WITH
{% if is_incremental() %}
incremental_bounds AS (
    SELECT
        COALESCE(
            DATEADD(day, -4, MAX(trade_date)),
            TO_DATE('1900-01-01')
        ) AS output_start_date,
        COALESCE(
            DATEADD(day, -400, DATEADD(day, -4, MAX(trade_date))),
            TO_DATE('1900-01-01')
        ) AS calculation_start_date
    FROM {{ this }}
),
{% endif %}

source_rows AS (
    SELECT *
    FROM {{ ref('int_russell3000__daily') }}
    {% if is_incremental() %}
    WHERE trade_date >= (
        SELECT calculation_start_date
        FROM incremental_bounds
    )
    {% endif %}
),

base_metrics AS (
    SELECT
        ticker,
        volume,
        open,
        close,
        yesterday_close,
        high,
        low,
        trade_date,
        sector,
        company,
        asset_class,
        location,
        exchange,
        currency,
        market_currency,
        index_weight,
        is_new_to_index,
        is_valid_record,

        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) >= 20
            THEN AVG(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS sma_20,

        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            ) >= 50
            THEN AVG(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS sma_50,

        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            ) >= 200
            THEN AVG(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS sma_200,

        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) >= 252
            THEN MAX(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS high_52week,

        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) >= 252
            THEN MIN(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS low_52week,

        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) >= 14
            THEN
                SUM(
                    CASE
                        WHEN close > yesterday_close THEN (close - yesterday_close)
                        ELSE 0
                    END
                ) OVER (
                    PARTITION BY ticker
                    ORDER BY trade_date
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) / 14
            ELSE NULL
        END AS avg_gain_14,

        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) >= 14
            THEN
                SUM(
                    CASE
                        WHEN close < yesterday_close THEN (yesterday_close - close)
                        ELSE 0
                    END
                ) OVER (
                    PARTITION BY ticker
                    ORDER BY trade_date
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) / 14
            ELSE NULL
        END AS avg_loss_14

    FROM source_rows
),

signal_flags AS (
    SELECT
        *,

        CASE
            WHEN close > sma_20
             AND LAG(close) OVER (PARTITION BY ticker ORDER BY trade_date)
                 <= LAG(sma_20) OVER (PARTITION BY ticker ORDER BY trade_date)
            THEN 1 ELSE 0
        END AS bullish_crossover,

        CASE
            WHEN sma_50 > sma_200
             AND LAG(sma_50) OVER (PARTITION BY ticker ORDER BY trade_date)
                 <= LAG(sma_200) OVER (PARTITION BY ticker ORDER BY trade_date)
            THEN 1 ELSE 0
        END AS golden_cross,

        CASE
            WHEN sma_50 < sma_200
             AND LAG(sma_50) OVER (PARTITION BY ticker ORDER BY trade_date)
                 >= LAG(sma_200) OVER (PARTITION BY ticker ORDER BY trade_date)
            THEN 1 ELSE 0
        END AS death_cross,

        CASE
            WHEN COUNT(volume) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) >= 20
            THEN volume / NULLIF(
                AVG(volume) OVER (
                    PARTITION BY ticker
                    ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ),
                0
            )
            ELSE NULL
        END AS rel_vol,

        CASE
            WHEN avg_gain_14 IS NULL OR avg_loss_14 IS NULL THEN NULL
            WHEN GREATEST(avg_gain_14, 0) = 0
                AND GREATEST(avg_loss_14, 0) = 0 THEN 50
            WHEN GREATEST(avg_loss_14, 0) = 0 THEN 100
            WHEN GREATEST(avg_gain_14, 0) = 0 THEN 0
            ELSE
                100 - (
                    100 / (
                        1 + (GREATEST(avg_gain_14, 0) / GREATEST(avg_loss_14, 0))
                    )
                )
        END AS rsi

    FROM base_metrics
)

SELECT *
FROM signal_flags
WHERE is_valid_record = 1
{% if is_incremental() %}
  AND trade_date >= (
      SELECT output_start_date
      FROM incremental_bounds
  )
{% endif %}
