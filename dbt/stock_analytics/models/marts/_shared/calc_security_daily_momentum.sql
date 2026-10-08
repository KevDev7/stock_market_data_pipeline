-- Shared MARTS calculation: compute momentum once for facts and dimensions.
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'replace_recent',
    tmp_relation_type = 'table',
    unique_key = ['security_key', 'trade_date'],
    cluster_by = ['ticker'],
    on_schema_change = 'fail'
) }}

WITH
{% if is_incremental() %}
incremental_bounds AS (
    SELECT
        {{ recent_rebuild_start(this) }} AS output_start_date
),
{% endif %}

source_rows AS (
    SELECT *
    FROM {{ ref('int_market__daily') }}
    {% if is_incremental() %}
    WHERE trade_date >= (
        SELECT output_start_date
        FROM incremental_bounds
    )
    UNION ALL
    -- Warm up by accepted observations, not an estimated number of calendar days.
    -- 252 earlier rows cover the longest window and preceding crossover state.
    SELECT *
    FROM {{ ref('int_market__daily') }}
    WHERE trade_date < (SELECT output_start_date FROM incremental_bounds)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY security_key ORDER BY trade_date DESC
    ) <= 252
    {% endif %}
),

base_metrics AS (
    SELECT
        security_key,
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
        is_first_observation,
        is_valid_record,

        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) >= 20
            -- Fixed-point accumulation prevents floating-window cancellation
            -- from turning price/average equality into a false crossover.
            -- Publish DOUBLE as before: no consumer column type change.
            THEN (AVG(close::NUMBER(38,18)) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ))::DOUBLE
            ELSE NULL
        END AS sma_20,

        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            ) >= 50
            THEN (AVG(close::NUMBER(38,18)) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            ))::DOUBLE
            ELSE NULL
        END AS sma_50,

        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            ) >= 200
            THEN (AVG(close::NUMBER(38,18)) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            ))::DOUBLE
            ELSE NULL
        END AS sma_200,

        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) >= 252
            THEN MAX(close) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS high_52week,

        CASE
            WHEN COUNT(close) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) >= 252
            THEN MIN(close) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            )
            ELSE NULL
        END AS low_52week,

        -- Both sums contain only nonnegative terms. Clamp floating-window
        -- cancellation residue to their mathematical lower bound, zero.
        CASE
            WHEN COUNT(yesterday_close) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) >= 14
            THEN
                GREATEST(SUM(
                    CASE
                        WHEN close > yesterday_close THEN (close - yesterday_close)
                        ELSE 0
                    END
                ) OVER (
                    PARTITION BY security_key
                    ORDER BY trade_date
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) / 14, 0)
            ELSE NULL
        END AS avg_gain_14,

        CASE
            WHEN COUNT(yesterday_close) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) >= 14
            THEN
                GREATEST(SUM(
                    CASE
                        WHEN close < yesterday_close THEN (yesterday_close - close)
                        ELSE 0
                    END
                ) OVER (
                    PARTITION BY security_key
                    ORDER BY trade_date
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) / 14, 0)
            ELSE NULL
        END AS avg_loss_14

    FROM source_rows
),

signal_flags AS (
    SELECT
        *,

        CASE
            WHEN close > sma_20
             AND LAG(close) OVER (PARTITION BY security_key ORDER BY trade_date)
                 <= LAG(sma_20) OVER (PARTITION BY security_key ORDER BY trade_date)
            THEN 1 ELSE 0
        END AS bullish_crossover,

        CASE
            WHEN sma_50 > sma_200
             AND LAG(sma_50) OVER (PARTITION BY security_key ORDER BY trade_date)
                 <= LAG(sma_200) OVER (PARTITION BY security_key ORDER BY trade_date)
            THEN 1 ELSE 0
        END AS golden_cross,

        CASE
            WHEN sma_50 < sma_200
             AND LAG(sma_50) OVER (PARTITION BY security_key ORDER BY trade_date)
                 >= LAG(sma_200) OVER (PARTITION BY security_key ORDER BY trade_date)
            THEN 1 ELSE 0
        END AS death_cross,

        CASE
            WHEN COUNT(volume) OVER (
                PARTITION BY security_key
                ORDER BY trade_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) >= 20
            -- Floating division preserves very small positive ratios; NUMBER
            -- division can round legitimate low-volume observations to zero.
            THEN volume::DOUBLE / NULLIF(
                AVG(volume::DOUBLE) OVER (
                    PARTITION BY security_key
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
{% if is_incremental() %}
WHERE trade_date >= (
      SELECT output_start_date
      FROM incremental_bounds
  )
{% endif %}
