-- Computes trading momentum indicators (SMA, RSI, crosses, volatility signals) for Russell 3000 constituents.
{{ config(
    materialized = 'incremental',
    unique_key = ['ticker', 'trade_date'],
    cluster_by = ['ticker'],
    on_schema_change = 'fail'
) }}

WITH base_metrics AS (
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
        index_weight,
        is_new_to_index,
        is_valid_record,

        /* SMA-20 (NULL until 20 rows exist for ticker) */
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

        /* SMA-50 (NULL until 50 rows exist for ticker) */
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

        /* SMA-200 (NULL until 200 rows exist for ticker) */
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

        /* 52-week high/low (252 trading days) */
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

        /* Avg gain/loss (14-day RSI components) */
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

    FROM {{ ref('int_russell3000__daily') }}
),

signal_flags AS (
    SELECT
        *,

        /* Price crosses above SMA-20 today */
        CASE 
            WHEN close > sma_20
             AND LAG(close) OVER (PARTITION BY ticker ORDER BY trade_date)
                 <= LAG(sma_20) OVER (PARTITION BY ticker ORDER BY trade_date)
            THEN 1 ELSE 0
        END AS bullish_crossover,

        /* SMA-50 crosses above SMA-200 today */
        CASE 
            WHEN sma_50 > sma_200
             AND LAG(sma_50) OVER (PARTITION BY ticker ORDER BY trade_date)
                 <= LAG(sma_200) OVER (PARTITION BY ticker ORDER BY trade_date)
            THEN 1 ELSE 0
        END AS golden_cross,

        /* SMA-50 crosses below SMA-200 today */
        CASE
            WHEN sma_50 < sma_200
             AND LAG(sma_50) OVER (PARTITION BY ticker ORDER BY trade_date)
                 >= LAG(sma_200) OVER (PARTITION BY ticker ORDER BY trade_date)
            THEN 1 ELSE 0
        END AS death_cross,

        /* Relative volume vs 20-day avg (NULL until 20 rows) */
        CASE 
            WHEN COUNT(volume) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) >= 20
            THEN volume / (
                AVG(volume) OVER (
                    PARTITION BY ticker
                    ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                )
            )
            ELSE NULL
        END AS rel_vol,

        /* RSI from avg_gain_14 / avg_loss_14 */
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
{% if is_incremental() %}
WHERE trade_date >= (
    SELECT DATEADD(day, -4, MAX(trade_date)) FROM {{ this }}
)
AND is_valid_record = 1
{% endif %}

