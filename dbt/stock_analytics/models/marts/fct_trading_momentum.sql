-- Computes trading momentum indicators (SMA, RSI, crosses, volatility signals) for Russell 3000 constituents.
{{ config(
    materialized = 'incremental',
    unique_key = ['ticker', 'trade_date'],
    on_schema_change = 'fail'
) }}

WITH base AS (
    SELECT 
        ticker,
        trade_date,
        volume,
        open,
        close,
        high,
        low,
        yesterday_close,
        sector,
        company,
        index_weight,
        is_valid_record,
        ingested_at
    FROM {{ ref('int_russell3000__daily') }}
    {% if is_incremental() %}
      WHERE trade_date >= (
          SELECT DATEADD(day, -4, MAX(trade_date))
          FROM {{ this }}
      )
    {% endif %}
),

with_sma AS (
    SELECT
        *,
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS sma_20,
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS sma_50,
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS sma_200
    FROM base
),

with_rsi AS (
    SELECT
        *,
        CASE 
            WHEN COUNT(close) OVER (
                PARTITION BY ticker
                ORDER BY trade_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) >= 14
            THEN 100 - (
                100 / (
                    1 + (
                        SUM(CASE WHEN close > yesterday_close THEN close - yesterday_close ELSE 0 END)
                        OVER (PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
                        /
                        NULLIF(SUM(CASE WHEN close < yesterday_close THEN yesterday_close - close ELSE 0 END)
                        OVER (PARTITION BY ticker ORDER BY trade_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW), 0)
                    )
                )
            )
            ELSE NULL
        END AS rsi
    FROM with_sma
),

enriched AS (
    SELECT
        *,
        MAX(close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS high_52week,
        MIN(close) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS low_52week,
        volume / NULLIF(AVG(volume) OVER (
            PARTITION BY ticker
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ), 0) AS rel_vol
    FROM with_rsi
),

ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, trade_date 
            ORDER BY ingested_at DESC NULLS LAST
        ) AS rn
    FROM enriched
),

with_crosses AS (
    SELECT
        *,
        -- golden cross: 50-day SMA crossing above 200-day SMA
        CASE 
            WHEN LAG(sma_50) OVER (PARTITION BY ticker ORDER BY trade_date) < 
                 LAG(sma_200) OVER (PARTITION BY ticker ORDER BY trade_date)
             AND sma_50 >= sma_200 THEN 1
            ELSE 0
        END AS golden_cross,

        -- death cross: 50-day SMA crossing below 200-day SMA
        CASE 
            WHEN LAG(sma_50) OVER (PARTITION BY ticker ORDER BY trade_date) > 
                 LAG(sma_200) OVER (PARTITION BY ticker ORDER BY trade_date)
             AND sma_50 <= sma_200 THEN 1
            ELSE 0
        END AS death_cross
    FROM ranked
)

SELECT *
FROM with_crosses
WHERE rn = 1
  AND is_valid_record = 1
